import faust
from datetime import datetime
from typing import Optional

class Energy(faust.Record, serializer="json", isodates=True):
    msg_type: str
    block_id: str
    run_id: str
    timestamp: datetime
    duration: int
    total_energy: float
    devices: str
    hostname: str
    resource_monitoring_interval: int

class Resource(faust.Record, serializer="json", isodates=True):
    msg_type: str
    block_id: str
    run_id: str
    timestamp: datetime
    pid: int
    perf_unhalted_core_cycles: int
    perf_unhalted_reference_cycles: int
    perf_llc_misses: int
    perf_instructions_retired: int
    psutil_process_ppid: Optional[int] = None
    psutil_process_name: Optional[str] = None
    hostname: Optional[str] = None
    inferenced_power: Optional[float] = None
    credit: Optional[float] = None
    first_msg: bool = False,
    last_msg: bool = False,    

class Task(faust.Record, serializer="json", isodates=True):
    msg_type: str
    try_id: int
    task_id: str
    run_id: str
    block_id: str
    hostname: str
    pid: int
    task_try_time_running: Optional[datetime] = None
    task_try_time_running_ended: Optional[datetime] = None
    first_msg: bool = False,
    last_msg: bool = False,
    timestamp: Optional[datetime] = None

class Workflow(faust.Record, serializer="json", isodates=True):
    msg_type: str
    run_id: str
    workflow_name: str
    workflow_version: str
    time_began: datetime
    tasks_failed_count: int
    tasks_completed_count: int
    parsl_version: str
    python_version: str
    time_completed: Optional[datetime] = None
    host: Optional[str] = None
    user: Optional[str] = None
    rundir: Optional[str] = None