from typing import Annotated, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException
import databases

from .common import get_current_user, database
from .messages import *
from .models import users

router = APIRouter(
    prefix="/allocation",
    responses={404: {"description": "Not found"}},
)

@router.get("/")
async def get_allocation(
    user: Annotated[User, Depends(get_current_user)]
) -> AllocationRep:
    allocation_dict =  await fetch_allocation(database, user)
    if allocation_dict is None:
        raise HTTPException(status_code=404, detail="User not found")
    return AllocationRep(**allocation_dict)

@router.post("/execute")
async def allocate_pending_credits(
    user: Annotated[User, Depends(get_current_user)],
    resources: Resources
):
    return await update_allocation(database, user, False, resources.credit)

@router.post("/deduct")
async def deduct_pending_credits(
    user: Annotated[User, Depends(get_current_user)],
    job_info: JobExecutedReq
):
    return await update_allocation(database, 
                                   user,
                                   True,
                                   job_info.credits_predicted,
                                   job_info.credits_consumed,
                                   job_info.energy_consumed,
                                   job_info.core_hours_consumed)

async def fetch_allocation(db: databases.Database, user: User):
    query = users.select().where(users.c.user_id == user.username)
    return await database.fetch_one(query)

async def update_allocation(db: databases.Database,
                            user: User,
                            job_finished: bool,
                            credits_predicted: float,
                            credits_consumed: Optional[float] = None,
                            energy_consumed: Optional[float] = None,
                            core_hours_consumed: Optional[float] = None):
    async with database.transaction():
        query = users.select().where(users.c.user_id == user.username)
        allocation_info = await database.fetch_one(query)
        allocation_info = dict(allocation_info)
        if not job_finished:
            if allocation_info["credits_remaining"] > credits_predicted:
                allocation_info["credits_remaining"] -= credits_predicted
                allocation_info["credits_pending"] += credits_predicted
                allocation_info["jobs_running"] += 1
            else:
                raise HTTPException(status_code=405, detail="Insufficient credits")
        else:
            credits_returned = credits_predicted - credits_consumed
            allocation_info["credits_remaining"] += credits_returned
            allocation_info["credits_pending"] -= credits_predicted
            allocation_info["credits_consumed"] += credits_consumed
            allocation_info["energy_consumed"] += energy_consumed
            allocation_info["core_hours_consumed"] += core_hours_consumed
            allocation_info["jobs_running"] -= 1
            allocation_info["jobs_completed"] += 1

        query = users.update().where(users.c.user_id == user.username).values(**allocation_info)
        await db.execute(query)

    return
