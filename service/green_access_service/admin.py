from typing import Annotated
import json

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
import databases

from .common import database, redis_db, get_admin_user
from .messages import *
from .models import users

router = APIRouter(
    prefix="/admin",
    dependencies=[Depends(get_admin_user)],
)

@router.post("/register_user")
async def create_user(register_req: RegisterUserReq):
    return await insert_user(database, register_req.new_user, register_req.init_credits)

@router.post("/register_endpoint")
async def create_endpoint(endpoint:  RegisterEndpointReq):
    if not redis_db.set(f"endpoint_{endpoint.endpoint_id}", endpoint.json()):
        raise HTTPException(status_code=406, detail="Could not set endpoint value")
    assert redis_db.get(f"endpoint_{endpoint.endpoint_id}") == endpoint.json()

    redis_db.sadd("endpoints", f"{endpoint.endpoint_id}")
    return

@router.post("/set_allocation")
async def set_allocation(set_req: SetAllocationReq):
    query = users.update().where(users.c.user_id == set_req.user.username).values(
        credits_remaining=set_req.credit,
        credits_consumed=0,
        credits_pending=0,
        energy_consumed=0,
        core_hours_consumed=0,
        jobs_completed=0,
        jobs_running=0,
        job_limit=16)
    await database.execute(query)
    return

async def insert_user(db: databases.Database, user: User, start_credits: float):
    query = users.insert().values(user_id=user.username,
                                  credits_remaining=start_credits,
                                  credits_consumed=0,
                                  credits_pending=0,
                                  energy_consumed=0,
                                  core_hours_consumed=0,
                                  jobs_completed=0,
                                  jobs_running=0,
                                  job_limit=16)
    try:
        await db.execute(query)
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_201_CREATED, content="Previously created object")

    return

