import datetime

import sqlalchemy as sa
from sqlalchemy import Column, String, Text, Float, Boolean, BigInteger, Integer, DateTime, PrimaryKeyConstraint, Table
import databases

from .messages import *

metadata = sa.MetaData()

users = sa.Table(
    "users",
    metadata,
    Column("user_id", String(256), primary_key=True),
    Column("credits_remaining", Float, nullable=True),
    Column("credits_consumed", Float, nullable=True),
    Column("credits_pending", Float, nullable=True),
    Column("energy_consumed", Float, nullable=True),
    Column("core_hours_consumed", Float, nullable=True),
    Column("jobs_completed", Integer, nullable=True),
    Column("jobs_running", Integer, nullable=True),
    Column("job_limit", Integer, nullable=True, default=16)
)

jobs = sa.Table(
    "jobs",
    metadata,
    Column("task_id", String(256), primary_key=True),
    Column("endpoint_id", String(256), nullable=False),
    Column("user_id", String(256), nullable=False),
    Column("function_id", String(256), nullable=False),
    Column("task_status", String(64), nullable=False),
    Column("predicted_runtime", Float, nullable=True),
    Column("predicted_energy", Float, nullable=True),
    Column("predicted_credits", Float, nullable=True),

    Column("running_duration", Float, nullable=True),
    Column("energy_consumed", Float, nullable=True),
    Column("credits_consumed", Float, nullable=True),
    Column("llc_misses", Float, nullable=True),
    Column("instructions_retired", Float, nullable=True),
    Column("core_cycles", Float, nullable=True),
    Column("ref_cycles", Float, nullable=True),
    Column("time_completed", DateTime, default=datetime.datetime.fromtimestamp(0))
)

functions = sa.Table(
    "functions",
    metadata,
    Column("function_id", String(256), primary_key=True),
    Column("user_id", String(256), nullable=False),
    Column("name", Text, nullable=False)
)

def init_database(url):
    engine = sa.create_engine(
        url
    )
    metadata.create_all(engine)
