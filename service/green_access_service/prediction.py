from typing import Annotated
import random
import json
import redis
from multiprocessing import Process, Event
import pandas as pd
import asyncio
from io import BytesIO
import time

from fastapi import APIRouter
import sqlalchemy as sa
from sqlalchemy import desc, select
from fancyimpute import KNN
from fancyimpute.scaler import Scaler

from .messages import *
from .common import database, redis_db, redis_db_raw, REDIS_URL, DATABASE_URL
from .models import jobs, functions

router = APIRouter(
    prefix="/predict",
    responses={404: {"description": "Not found"}},
)
predictions = dict()

# Prediction is computation heavy, so we put it in a new process
kill_event = Event()
prediction_pooling_process = None


@router.on_event("startup")
def startup():
    global prediction_pooling_process
    prediction_pooling_process = Process(target=prediction_loop, args=(kill_event, REDIS_URL, DATABASE_URL, 60))
    prediction_pooling_process.start()

@router.on_event("shutdown")
def shutdown():
    kill_event.set()
    prediction_pooling_process.terminate()
    prediction_pooling_process.join()

@router.get("/")
async def predict_function(function_id: uuid.UUID, endpoint_id: uuid.UUID | None = None) -> PredictFnRep:
    resp = PredictFnRep(function_id=function_id, predictions=[])
    endpoints = redis_db.smembers("endpoints")

    # TODO: Somehow cache this so every call doesn't have to read from redis
    try:
        prediction_matrix = pd.read_pickle(BytesIO(redis_db_raw.get("prediction_matrix")))
    except Exception as e:
        # No prediction matrix. Must not be any data in database
        if endpoint_id is not None:
            endpoint_info = json.loads(redis_db.get(f"endpoint_{endpoint_id}"))
            resp.predictions.append(Resources(endpoint_id=endpoint_id,
                                              endpoint_name=endpoint_info["display_name"], 
                                              runtime=0,
                                              energy=0,
                                              credit=0,
                                              endpoint_carbon_intensity=endpoint_info["carbon_intensity"]))
            return resp
        for eid in endpoints:
            endpoint_info = json.loads(redis_db.get(f"endpoint_{eid}"))
            resp.predictions.append(Resources(endpoint_id=eid,
                                              endpoint_name=endpoint_info["display_name"],
                                              runtime=0,
                                              energy=0,
                                              credit=0,
                                              endpoint_carbon_intensity=endpoint_info["carbon_intensity"]))
        return resp


    query = functions.select().where(functions.c.function_id == str(function_id))
    func_record = await database.fetch_one(query)
    func_name = func_record.name
    
    predictions[function_id] = dict()
    for eid in endpoints:
        endpoint_info = json.loads(redis_db.get(f"endpoint_{eid}"))
        if eid not in prediction_matrix["running_duration"].columns:
            # Special case for adding a new endpoint.
            # Predict 0 to implore the user to run here :)
            prediction = Resources(endpoint_id=eid,
                                   endpoint_name=endpoint_info["display_name"],
                                   runtime=0,
                                   energy=0,
                                   credit=0,
                                   endpoint_carbon_intensity=endpoint_info["carbon_intensity"])
            predictions[function_id][eid] = prediction 
            continue

        
        if func_name in prediction_matrix.index:
            runtime = prediction_matrix.loc[func_name, ("running_duration", eid)]
            power = prediction_matrix.loc[func_name, ("power", eid)]
        else:
            runtime = prediction_matrix.loc[:, ("running_duration", eid)].mean()
            power = prediction_matrix.loc[:, ("power", eid)].mean()

        energy = runtime * (power + endpoint_info["idle_power"]/endpoint_info["cores_per_node"])
        adujusted_energy = (energy + (runtime * (endpoint_info["per_node_tdp"]/endpoint_info["cores_per_node"]))) / 2
        adujusted_energy = adujusted_energy  / (1000 * 3600) # Convert to kWh
        carbon_intensity = endpoint_info["carbon_intensity"] # TODO: Fetch from electricity maps with caching
        credit = adujusted_energy * (carbon_intensity + endpoint_info["embodied_carbon_rate"])

        prediction = Resources(endpoint_id=eid,
                               endpoint_name=endpoint_info["display_name"],
                               runtime=runtime,
                               energy=energy,
                               credit=credit,
                               endpoint_carbon_intensity=endpoint_info["carbon_intensity"])
        predictions[function_id][eid] = prediction 

    if endpoint_id is None:
        resp.predictions = list(predictions[function_id].values())
    else:
        resp.predictions.append(predictions[function_id][str(endpoint_id)])

    return resp

def prediction_loop(kill_event, redis_url, database_url, polling_interval=60):
    redis_db = redis.Redis.from_url(redis_url, 
                                    decode_responses=True,
                                    health_check_interval=30)
    engine = sa.create_engine(database_url)
    while not kill_event.is_set():

        query = select(jobs, functions.c.name).join(
            functions, functions.c.function_id == jobs.c.function_id,
            isouter=True).where(jobs.c.task_status == "finished").order_by(desc(jobs.c.time_completed))
        with engine.connect() as connection:
            task_stats = pd.read_sql(query, connection)
        if len(task_stats) == 0:
            time.sleep(polling_interval)
            continue

        task_stats["llc_misses"] = task_stats["llc_misses"] / 1e6
        task_stats["instructions_retired"] = task_stats["instructions_retired"] / 1e8
        task_stats["core_cycles"] = task_stats["core_cycles"] / 1e8

        task_stats["power"] = task_stats["energy_consumed"] / task_stats["running_duration"]
        task_stats["llc_misses_per_sec"] = task_stats["llc_misses"]/task_stats["running_duration"]
        task_stats["instructions_per_sec"] = task_stats["instructions_retired"]/task_stats["running_duration"]
        task_stats["core_cycles_per_sec"] = task_stats["core_cycles"] / task_stats["running_duration"]

        df = pd.pivot_table(task_stats, 
                            values=["power", "running_duration", "instructions_per_sec", "llc_misses_per_sec", "core_cycles_per_sec", "energy_consumed"],
                            index=["name"],
                            columns=["endpoint_id"],
                            aggfunc="mean")

        imputer = KNN(k=3, normalizer=Scaler())
        perf_counters = df[["instructions_per_sec", "llc_misses_per_sec", "core_cycles_per_sec"]]
        perf_counters_pred = imputer.fit_transform(perf_counters.values)
        df = df.fillna(pd.DataFrame(perf_counters_pred, index=perf_counters.index, columns=perf_counters.columns))
        
        imputer = KNN(k=3, normalizer=Scaler())
        runtime_df = df
        runtime_pred = imputer.fit_transform(runtime_df.values)
        df = df.fillna(pd.DataFrame(runtime_pred, index=runtime_df.index, columns=runtime_df.columns))
        
        # Write prediction matrix to redis
        bio = BytesIO()
        df.to_pickle(bio)
        redis_db.set("prediction_matrix", bio.getvalue())
        time.sleep(polling_interval)
