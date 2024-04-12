from typing import Annotated, List
import json
import threading
import asyncio
import uuid
import queue
import time
from collections import defaultdict

from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
from globus_compute_sdk import Client
from globus_compute_sdk.sdk.monitoring import monitor_wrapper

from green_access_service import access_control
from green_access_service import prediction
from green_access_service import  admin
from green_access_service.messages import *
from green_access_service.common import database, redis_db, get_current_user, DATABASE_URL
from green_access_service.models import init_database, functions, jobs

# FuncX Client
client = Client()
kill_event = threading.Event()
task_queue = queue.Queue()
func_submit_thread = None

app = FastAPI()
app.include_router(access_control.router)
app.include_router(prediction.router)
app.include_router(admin.router)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_headers=["*"]
)

@app.on_event("startup")
async def startup():
    global func_submit_thread
    func_submit_thread = threading.Thread(target=function_submit_loop, 
                                          args=(kill_event, task_queue))
    init_database(DATABASE_URL)
    func_submit_thread.start()
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    kill_event.set()
    await database.disconnect()
    func_submit_thread.join()

@app.get("/")
async def liveness(user: Annotated[User, Depends(get_current_user)]):
    return f"Alive"
    
@app.post("/register_function")
async def register_function(user: Annotated[User, Depends(get_current_user)], register_req: RegisterFn) -> RegisterFnRep:
    function = client.fx_serializer.deserialize(register_req.payload)
    function = monitor_wrapper(function)
    fn_id = client.register_function(function, function_name=register_req.function_name)
    query = functions.insert().values(function_id=fn_id,
                                      user_id=user.username, 
                                      name=register_req.function_name)
    await database.execute(query)
    return RegisterFnRep(function_id=fn_id)

@app.post("/execute_function")
async def execute_function(user: Annotated[User, Depends(get_current_user)], execute_req: ExecuteFnReq) -> ExecuteFnRep:    
    async with httpx.AsyncClient() as http_client:
        pred_req = {"function_id": execute_req.function_id, "endpoint_id": execute_req.endpoint_id}
        resp = await http_client.get(url="http://127.0.0.1:8000/predict/", params=pred_req)
        predicted_resources = resp.json()["predictions"][0]
        resp = await http_client.post(url="http://127.0.0.1:8000/allocation/execute", params=user.dict(), json=predicted_resources)
        
        if not resp.status_code == httpx.codes.OK:
            raise HTTPException(status_code=440, detail="Insufficient credits")
    
    task_id = uuid.uuid4()
    query = jobs.insert().values(task_id=str(task_id),
                                 endpoint_id=str(execute_req.endpoint_id),
                                 user_id=user.username,
                                 function_id=str(execute_req.function_id),
                                 task_status="received",
                                 predicted_runtime=predicted_resources["runtime"],
                                 predicted_energy=predicted_resources["energy"],
                                 predicted_credits=predicted_resources["credit"])
    await database.execute(query)
    task_queue.put((user, task_id, execute_req, predicted_resources["credit"]))
    return ExecuteFnRep(task_id=task_id)

@app.get("/status")
async def get_status(user: Annotated[User, Depends(get_current_user)], task_id: str):
    # TODO: Limit polling interval for task status and result
    funcx_task_id = redis_db.get(f"access_{task_id}")
    if funcx_task_id is None:
        return {"task_id": task_id, "status": "waiting", "reason": "Waiting for batch submission to funcX"}
    
    funcx_results = await asyncio.to_thread(client.web_client.get_task, funcx_task_id)
    return json.loads(funcx_results.text)

@app.get("/batch/status")
async def get_batch_status(user: Annotated[User, Depends(get_current_user)], task_ids: list[str] | None = Query(default=None)):
    # TODO: Limit polling interval for task status and result
    batch = []
    response = {"task_ids": task_ids, "results": {}}
    submitted_tasks = dict()
    for task_id in task_ids:
        funcx_task_id = redis_db.get(f"access_{task_id}")
        if funcx_task_id is not None:
            submitted_tasks[funcx_task_id] = task_id
            batch.append(funcx_task_id)
        else:
            response["results"][task_id] = {"task_id": task_id, "status": "waiting", "reason": "Waiting for batch submission to funcX"}
    
    funcx_results = await asyncio.to_thread(client.web_client.get_batch_status, batch)
    for funcx_task_id, result in funcx_results["results"].items():
        result["funcx_task_id"] = funcx_task_id
        response["results"][submitted_tasks[funcx_task_id]] = result
    return response

@app.get("/functions")
async def get_functions(user: Annotated[User, Depends(get_current_user)]) -> List[FuncInfo] :
    query = functions.select().where(functions.c.user_id==user.username)
    return await database.fetch_all(query)

def function_submit_loop(kill_event, task_queue, submit_interval=0.5, max_batch_size=20):
    cur_batch = defaultdict(client.create_batch)
    batch_functions = defaultdict(list)
    batch_start_time = time.time()

    while not kill_event.is_set():
        try:
            (user, task_id, execute_req, predicted_credits) = task_queue.get(block=False)
            (args, kwargs) = client.fx_serializer.deserialize(execute_req.payload)
            cur_batch[execute_req.endpoint_id].add(str(execute_req.function_id), args, kwargs)
            batch_functions[execute_req.endpoint_id].append({
                "function_id": str(execute_req.function_id),
                "task_id": str(task_id), 
                "user_id": user.username,
                "predicted_credits": predicted_credits})
            
        except queue.Empty:
            time.sleep(0.1)

        if time.time() - batch_start_time > submit_interval:
            for endpoint_id, batch in cur_batch.items():
                if len(batch_functions[endpoint_id]) > 0:
                    funcx_resp = client.batch_run(endpoint_id, batch)
                    func_idx = defaultdict(int)
                    for payload in batch_functions[endpoint_id]:
                        funcx_task_id = funcx_resp['tasks'][payload['function_id']][func_idx[payload['function_id']]]
                        func_idx[payload['function_id']] += 1
                        redis_db.set(f"task_{funcx_task_id}", json.dumps(payload))
                        redis_db.set(f"access_{payload['task_id']}", funcx_task_id)

            # Update new batch
            cur_batch = defaultdict(client.create_batch)
            batch_functions = defaultdict(list)
            batch_start_time = time.time()
