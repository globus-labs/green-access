import asyncio
import ssl
import datetime
import os
import json
from collections import defaultdict

import faust
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from faust.auth import OAuthCredentials
from sklearn.linear_model import ElasticNet
from scipy import integrate
import numpy as np
import pandas as pd
from diaspora_event_sdk import Client as GlobusClient
import httpx

from green_access_service.records import Energy, Resource, Task, Workflow
from green_access_service.common import redis_db, database, APP_NAME, PREDICTION_TOPIC, RESOURCE_TOPIC
from green_access_service.models import jobs

# TODO: Performance counters to use need to be endpoint specific?
PERF_COUTERS = ["perf_instructions_retired", "perf_llc_misses", "perf_unhalted_core_cycles", "perf_unhalted_reference_cycles"]

class MSKTokenProvider(AbstractTokenProvider):
    async def token(self):
        return await asyncio.get_running_loop().run_in_executor(None, self._token)

    def _token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return token

c = GlobusClient()
try:
    keys = c.retrieve_key()
    brokers = keys['endpoint']
    os.environ["AWS_ACCESS_KEY_ID"] = keys["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = keys["secret_key"]
except Exception as e:
    raise RuntimeError("Failed to retrieve Kafka keys") from e

min_training_threshold = 5

app = faust.App(
    APP_NAME,
    broker=brokers,
    ssl_context=ssl.create_default_context(),
    broker_credentials=OAuthCredentials(
        oauth_cb=MSKTokenProvider(), ssl_context=ssl.create_default_context()
    ),
    store='rocksdb://',
    producer_acks=1
)
resource_topic = app.topic(RESOURCE_TOPIC)
prediction_topic = app.topic(PREDICTION_TOPIC)   

def create_node_id(msg):
    return f"{msg.block_id}:{msg.run_id}"

def validate_input_records(value):
    key = value["msg_type"]
    if key == "WORKFLOW_INFO":
        try:
            return "WORKFLOW_INFO", Workflow(**value)
        except TypeError as e:
            # print(f"Recived invalid workflow message: {e}, ignoring")
            return None, value
    elif key == "ENERGY_INFO":
        try:
            return "ENERGY_INFO", Energy(**value)
        except TypeError as e:
            # print(f"Recived invalid energy message: {e}, ignoring")
            return None, value
    elif key == "RESOURCE_INFO":
        try:
            return "RESOURCE_INFO", Resource(**value)
        except TypeError as e:
            # print(f"Recived invalid resource message: {e}, ignoring")
            return None, value
    else:
        raise ValueError(f"Unknown key type: {key}")  

def get_key(msg):
    t, v = validate_input_records(msg)
    if t is None:
        return "0000000"
    return create_node_id(v)

# train the model periodically (~ every 60 seconds)
def split(df):
    gb = df.groupby(['pid'])
    return [gb.get_group(x) for x in gb.groups]

def sum_resources(df1, df2, columns=["perf_unhalted_core_cycles", "perf_unhalted_reference_cycles", "perf_llc_misses", "perf_instructions_retired"]):
    df = pd.merge_asof(df1[columns], df2[columns], on="timestamp", direction="forward", tolerance=pd.Timedelta(3, unit="s")).fillna(0)
    for col in columns:
        df[col] = df[f"{col}_x"] + df[f"{col}_y"]
        df = df.drop([f"{col}_x", f"{col}_y"], axis=1)
    df = df.set_index("timestamp")
    return df

def send_data(producer, key, msg):
    fut = producer.send(topic=PREDICTION_TOPIC, key=run_id, value=message)
    return fut.result()

async def train_and_send(energy_records, resource_records):
    loop = asyncio.get_running_loop()
    if len(energy_records) < min_training_threshold:
        return

    run_id = energy_records[0].run_id
    block_id = energy_records[0].block_id

    # This could be blocking IO, but redis is in-memory, so should be fast?
    endpoint_id = redis_db.get(f"run_{run_id}")
    try:
        endpoint_info = json.loads(redis_db.get(f"endpoint_{endpoint_id}"))
    except:
        raise Exception(f"Could not find endpoint id {endpoint_id} in redis")

    try:
        carbon_intensity = endpoint_info["carbon_intensity"] # TODO: Fetch from electricity maps with caching
    except:
        carbon_intensity = 502

    resource_df = pd.DataFrame.from_records([v.to_representation() for v in resource_records])
    resource_df["timestamp"] = pd.to_datetime(resource_df["timestamp"])
    df_split = split(resource_df)
    resources_dict = dict()
    for i in range(len(df_split)):
        df_new = df_split[i][["timestamp", "perf_unhalted_core_cycles", "perf_unhalted_reference_cycles", "perf_instructions_retired", "perf_llc_misses"]].diff()
        df_new["timestamp"] = (df_new["timestamp"] / np.timedelta64(1, "s"))
        df_new = df_new.div(df_new["timestamp"], axis='index')
        df_new["pid"] = df_split[i]["pid"]
        df_new["psutil_process_ppid"] = df_split[i]["psutil_process_ppid"].astype(int)
        df_new["timestamp"] = df_split[i]["timestamp"]
        df_new = df_new.set_index("timestamp")
        df_new = df_new.dropna() # Drop first row of diff
        if len(df_new) > 0:
            resources_dict[df_new["pid"].iloc[0]] = df_new

    process_preds = {}
    resources_dict = {k: v for k, v in sorted(resources_dict.items(), key=lambda item: item[1].index[-1] - item[1].index[0], reverse=True)}
    node_resource_df = None 
    for process_df in resources_dict.values():
        if node_resource_df  is None:
            node_resource_df  = process_df
        else:
            node_resource_df = sum_resources(node_resource_df , process_df)

    energy_df = pd.DataFrame.from_records([v.to_representation() for v in energy_records])
    energy_df["timestamp"] = pd.to_datetime(energy_df["timestamp"])
    energy_df["power"] = energy_df["total_energy"] / energy_df["duration"]
    df_combined = pd.merge_asof(energy_df, node_resource_df, on="timestamp", direction="backward")
    df_combined.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_combined = df_combined.dropna()
    df_combined["power"] = df_combined["power"] / 1e6 # Covert uW to W
    regr = ElasticNet(random_state=0, positive=True)
    regr.fit(df_combined[PERF_COUTERS], df_combined["power"])
    regr.intercept_ = max(regr.intercept_, 0)
    endpoint_info["idle_power"] = (0.5 * endpoint_info["idle_power"]) + (0.5 * regr.intercept_)
    redis_db.set(f"endpoint_{endpoint_id}", json.dumps(endpoint_info))

    df_combined["total_pred_power"] = regr.predict(df_combined[PERF_COUTERS]) - regr.intercept_
    df_combined["total_pred_power"] = df_combined["total_pred_power"].clip(lower=1e-7)
    df_combined["true_incremental_power"] = df_combined["power"] - regr.intercept_

    for process_df in resources_dict.values():
        try:
            if process_df["psutil_process_ppid"].iloc[0] in resources_dict:
                ppid = process_df["psutil_process_ppid"].iloc[0]
                resources_dict[ppid] = sum_resources(resources_dict[ppid], process_df)
        except Exception as e:
            # Not all processes have parent
            continue

    for pid, worker_df in resources_dict.items():
        worker_df = pd.merge_asof(df_combined[["total_pred_power", "true_incremental_power", "timestamp"]], worker_df, on="timestamp", direction="backward").dropna()
        if len(worker_df) == 0:
            continue

        worker_df["inferenced_power_prior"] = regr.predict(worker_df[PERF_COUTERS]) - regr.intercept_
        worker_df["inferenced_power"] = (worker_df["inferenced_power_prior"]  / worker_df["total_pred_power"]) * worker_df["true_incremental_power"]
        worker_df["inferenced_power"] = worker_df["inferenced_power"].clip(0, endpoint_info["per_node_tdp"]/endpoint_info["cores_per_node"])
        power_used = worker_df["inferenced_power"] + (regr.intercept_ / endpoint_info["cores_per_node"])
        adujusted_power = (power_used + (endpoint_info["per_node_tdp"]/endpoint_info["cores_per_node"])) / 2
        worker_df["credit"] = adujusted_power * (carbon_intensity + endpoint_info["embodied_carbon_rate"])
        worker_df["block_id"] = block_id
        worker_df["run_id"] = run_id
        worker_df["pid"] = pid
        worker_df = worker_df[["block_id", "run_id", "timestamp", "pid", "perf_unhalted_core_cycles", "perf_unhalted_reference_cycles", "perf_llc_misses", "perf_instructions_retired", "inferenced_power", "credit"]]
        if worker_df["inferenced_power"].max() > 1e-10:
            message = {
                "msg_type": "RESOURCE_INFO",
                "resource_df": worker_df.to_dict('records')
            }
            await prediction_topic.send(key=run_id, value=message)

@app.agent(resource_topic)
async def topic_processor(stream, concurrency=4):
    async for values in stream.take(1000000, within=30):
        print("Recieved message!!")
        resource_table = defaultdict(lambda : ([], []))
        for value in values:
            if value is None:
                continue
            msg_type, v = validate_input_records(value)
            if msg_type is None:
                continue
            
            if msg_type == "WORKFLOW_INFO":
                # TODO: This needs to be a table instead of redis for resilience
                redis_db.set(f"run_{v.run_id}", v.workflow_name)

            else:
                node_id = create_node_id(v)
                if msg_type == "ENERGY_INFO":
                    resource_table[node_id][0].append(v)
                elif msg_type == "RESOURCE_INFO":
                    resource_table[node_id][1].append(v)

        await asyncio.gather(*[train_and_send(e_msgs, r_msgs) for (e_msgs, r_msgs) in resource_table.values()])     

######### Deals with prediction ################

# Might eventually need for resilience
# task_status_table = app.Table(
#     "task_status_table",
#     default=lambda: {},
#     partitions=1,
# )
# task_resource_table = app.Table(
#     "task_resource_table",
#     default=lambda: {},
#     partitions=1,
# )

# task_predictor -> completed_task_channel -> allocation_updator
completed_task_channel = app.channel()

def create_node_id_pid(block_id, run_id, pid):
    return f"{block_id}:{run_id}:{pid}"

def create_try_task_id(run_id, try_id, task_id):
    return f"{try_id}:{task_id}:{run_id}"

def validate_prediction_records(value):
    key = value["msg_type"]
    if key == "RESOURCE_INFO":
        try:
            df = pd.DataFrame.from_records(value["resource_df"])
            return "RESOURCE_INFO", df
        except TypeError as e:
            print(f"Recieved invalid resource message: {e}, ignoring")
            return None, value
    elif key == "TASK_INFO":
        try:
            return "TASK_INFO", Task(**value)
        except TypeError as e:
            print(f"Recived invalid task message: {e}, ignoring")
            return None, value
    else:
        raise ValueError(f"Unknown key type: {key}")
        
def calculate_task_values(resource_list, task_list):
    resource_df = pd.concat(resource_list)
    resource_df["timestamp"] = pd.to_datetime(resource_df["timestamp"])
    resource_df = resource_df.set_index("timestamp")
    timestamps = resource_df.index.astype(np.int64) / 10**9

    pending_tasks = []
    completed_tasks = []
    last_timestamp = 0
    is_first = True
    for task_record in task_list:
        start_time = pd.to_datetime(task_record["task_try_time_running"]).asm8.astype(np.int64) / 10**9 # TODO: Check this conversion?
        end_time = pd.to_datetime(task_record["task_try_time_running_ended"]).asm8.astype(np.int64) / 10**9

        if end_time - 0.05  < timestamps[-1]:
            if is_first:
                resource_df["energy"] = integrate.cumtrapz(resource_df["inferenced_power"], 
                                                           x=resource_df.index.astype(np.int64) / 10**9, # Convert to seconds
                                                           initial=0)
                resource_df["cumulative_credits"] = integrate.cumtrapz(resource_df["credit"],
                                                           x=resource_df.index.astype(np.int64) / (10**9), # Convert to hours
                                                           initial=0)
                for perf_counter in PERF_COUTERS:
                    resource_df[perf_counter] = integrate.cumtrapz(resource_df[perf_counter], x=resource_df.index.astype(np.int64) / 10**9, initial=0)

                is_first = False
            # Deals with worker exiting too fast
            end_time = min(end_time, timestamps[-1])

            last_timestamp = end_time
            pred_energy = np.interp(np.array([start_time, end_time]), timestamps, resource_df["energy"])
            carbon_credits = np.interp(np.array([start_time, end_time]), timestamps, resource_df["cumulative_credits"])
            task_record["energy_consumed"] = pred_energy[1] - pred_energy[0]
            task_record["credits_consumed"] = carbon_credits[1] - carbon_credits[0]
            for perf_counter in PERF_COUTERS:
                interp_counters = np.interp(np.array([start_time, end_time]), timestamps, resource_df[perf_counter])
                task_record[perf_counter] = interp_counters[1] - interp_counters[0]
            completed_tasks.append(task_record)
            print(f"Infered energy for task {task_record['task_id']}")
        else:
            pending_tasks.append(task_record)
    
    resource_df = resource_df[timestamps >= last_timestamp]
    return completed_tasks, [resource_df,], pending_tasks


@app.agent(prediction_topic)
async def task_predictor(stream):
    task_resource_dict = {}
    task_status_dict = {}
    async for run_id, value in stream.items():
        #task_resource_dict = task_resource_table[run_id]
        #task_status_dict = task_status_table[run_id]

        k, v = validate_prediction_records(value)
        if k is None:
            continue

        elif k == "RESOURCE_INFO":
            print("Recieved resource info!")
            node_id_pid = create_node_id_pid(v.iloc[0]["block_id"], v.iloc[0]["run_id"], v.iloc[0]["pid"])
            cur_record = task_resource_dict.get(node_id_pid, [[], [], []])
            # We only need to save records for processes that have pending tasks
            if len(cur_record[1]) > 0 or len(cur_record[2]) > 0:
                cur_record[0].append(v)
                if len(cur_record[2]) > 0:
                    task_records, remaining_records, pending_tasks = calculate_task_values(cur_record[0], cur_record[2])
                    for task in task_records:
                        await completed_task_channel.send(value=task)
                    cur_record[0] = remaining_records
                    cur_record[2] = pending_tasks

                task_resource_dict[node_id_pid] = cur_record
                    
        elif k == "TASK_INFO":
            if v.task_try_time_running is not None:
                print("Received task start message")

                # Save task record 
                try_task_id = create_try_task_id(v.run_id, v.try_id, v.task_id)
                cur_record = task_status_dict.get(try_task_id, [[], []])
                cur_record[0].append(v.to_representation())
                task_status_dict[try_task_id] = cur_record

                if len(cur_record[0]) == len(cur_record[1]):
                    task_record = cur_record[0][-1]
                    task_record["task_try_time_running_ended"] = cur_record[1]["task_try_time_running_ended"] 

                    # Add to pending tasks in resource table
                    node_id_pid = create_node_id_pid(
                        task_record["block_id"], task_record["run_id"], task_record["pid"])
                    cur_record = task_resource_dict.get(node_id_pid, [[], [], []])
                    if isinstance(cur_record, list):
                        cur_record[1] = set(cur_record[1])
                    cur_record[1].discard(try_task_id)
                    cur_record[2].append(task_record)
                    task_resource_dict[node_id_pid] = cur_record
                else:
                    task_record = cur_record[0][-1]
                    node_id_pid = create_node_id_pid(
                        task_record["block_id"], task_record["run_id"], task_record["pid"])
                    cur_record = task_resource_dict.get(node_id_pid, [[], [], []])
                    if isinstance(cur_record[1], list):
                        cur_record[1] = set(cur_record[1])
                    cur_record[1].add(try_task_id)
                    task_resource_dict[node_id_pid] = cur_record
            
            if v.task_try_time_running_ended is not None:
                print("Receive task finished message")

                try_task_id = create_try_task_id(v.run_id, v.try_id, v.task_id)
                cur_record = task_status_dict.get(try_task_id, [[], []])
                cur_record[1].append(v.to_representation())
                task_status_dict[try_task_id] = cur_record
                
                if len(cur_record[0]) == len(cur_record[1]):
                    task_record = cur_record[0][-1]
                    task_record["task_try_time_running_ended"] = v.task_try_time_running_ended 

                    # Add to pending tasks in resource table
                    node_id_pid = create_node_id_pid(
                        task_record["block_id"], task_record["run_id"], task_record["pid"])

                    cur_record = task_resource_dict.get(node_id_pid, [[], [], []])
                    if isinstance(cur_record[1], list):
                        cur_record[1] = set(cur_record[1])
                    cur_record[1].discard(try_task_id)
                    cur_record[2].append(task_record)
                    task_resource_dict[node_id_pid] = cur_record
                else:
                    task_record = cur_record[0][-1]
                    node_id_pid = create_node_id_pid(
                        task_record["block_id"], task_record["run_id"], task_record["pid"])
                    cur_record = task_resource_dict.get(node_id_pid, [[], [], []])
                    if isinstance(cur_record[1], list):
                        cur_record[1] = set(cur_record[1])
                    cur_record[1].add(try_task_id)
                    task_resource_dict[node_id_pid] = cur_record
                
        #task_resource_table[run_id] = task_resource_dict
        #task_status_table[run_id] = task_status_dict 

@app.agent(completed_task_channel)
async def inference_worker(tasks):
    await database.connect()
    async for task in tasks:
        print(f"Received completed task {task['task_id']}")
        payload = json.loads(redis_db.get(f"task_{task['task_id']}"))
        start_time = pd.to_datetime(task["task_try_time_running"])
        end_time = pd.to_datetime(task["task_try_time_running_ended"])
        runtime = (end_time - start_time)  / np.timedelta64(1, 'h')
        
        async with httpx.AsyncClient() as http_client:
            params = {"username": payload["user_id"]}
            req_payload = {
                "credits_predicted": payload["predicted_credits"],
                "credits_consumed": task["credits_consumed"] / (3600 * 1000),
                "energy_consumed": task["energy_consumed"] / (3600 * 1000),
                "core_hours_consumed": runtime
            }
            resp = await http_client.post(url="http://127.0.0.1:8000/allocation/deduct", params=params, json=req_payload)
        
        
        # TODO: Do we need to batch inserts heres?
        query = jobs.update().where(jobs.c.task_id==payload["task_id"]).values(
            task_status="finished",
            running_duration=(end_time - start_time)  / np.timedelta64(1, 's'),
            energy_consumed=task["energy_consumed"],
            credits_consumed=task["credits_consumed"] / (3600 * 1000),
            llc_misses=task["perf_llc_misses"],
            instructions_retired=task["perf_instructions_retired"],
            core_cycles=task["perf_unhalted_core_cycles"],
            ref_cycles=task["perf_unhalted_reference_cycles"],
            time_completed=datetime.datetime.now()) #pd.to_datetime(task["task_try_time_running_ended"]))
        await database.execute(query)
