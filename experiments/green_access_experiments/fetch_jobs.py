import sqlalchemy as sa
from sqlalchemy import Column, String, Text, Float, Boolean, BigInteger, Integer, DateTime, PrimaryKeyConstraint, Table
from sqlalchemy import desc, select
import pandas as pd
import datetime
import argparse

metadata = sa.MetaData()

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

def fetch_jobs(url):
    engine = sa.create_engine(url)
    query = select(jobs, functions.c.name).join(
            functions, functions.c.function_id == jobs.c.function_id,
            isouter=True).where(jobs.c.task_status == "finished").order_by(desc(jobs.c.time_completed))
    with engine.connect() as connection:
        task_stats = pd.read_sql(query, connection)

    task_stats.to_csv("job_log.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='Connection string to connect to database')
    args = parser.parse_args()
    db_url= args.database
    fetch_jobs(db_url)
