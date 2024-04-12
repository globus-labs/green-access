from pydantic import BaseModel
from typing import List, Dict
import uuid

class User(BaseModel):
    username: str

class RegisterFn(BaseModel):
    function_name: str
    payload: str

class RegisterFnRep(BaseModel):
    function_id: uuid.UUID

class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    endpoint_id: uuid.UUID
    payload: str

class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID

class Resources(BaseModel):
    endpoint_id: uuid.UUID
    endpoint_name: str
    runtime: float
    energy: float
    credit: float
    endpoint_carbon_intensity: float

class PredictFnRep(BaseModel):
    function_id: uuid.UUID
    predictions: List[Resources]

class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str

class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str | None = None
    exception: str | None = None
    resources: Resources | None = None

class AllocationRep(BaseModel):
    credits_remaining: float
    credits_consumed: float
    credits_pending: float
    energy_consumed: float
    core_hours_consumed: float
    jobs_completed: int
    jobs_running: int

class RegisterEndpointReq(BaseModel):
    endpoint_id: uuid.UUID
    display_name: str
    nodes: int
    cores_per_node: int
    per_node_tdp: float
    idle_power: float
    embodied_carbon_rate: float = 0
    carbon_intensity: float | None = None
    lat: float | None = None
    long: float | None = None

class RegisterUserReq(BaseModel):
    new_user: User
    init_credits: float

class SetAllocationReq(BaseModel):
    user: User
    credit: float

class JobExecutedReq(BaseModel):
    credits_predicted: float
    credits_consumed: float
    energy_consumed: float
    core_hours_consumed: float

class FuncInfo(BaseModel):
    function_id: uuid.UUID
    name: str
    user_id: str