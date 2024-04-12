from typing import Annotated

from fastapi import Depends, HTTPException
import databases
import redis

from .messages import User

# TODO: Update variables
DATABASE_URL = "sqlite:///./test.db"
REDIS_URL = "redis://127.0.0.1:6379"
APP_NAME = "application-green-faas"
PREDICTION_TOPIC =  'green-faas-prediction'
RESOURCE_TOPIC =  'green-faas-resources'

database = databases.Database(DATABASE_URL)

redis_db = redis.Redis.from_url(REDIS_URL, 
                                decode_responses=True,
                                health_check_interval=30)

redis_db_raw = redis.Redis.from_url(REDIS_URL, 
                                    decode_responses=False,
                                    health_check_interval=30)

async def get_current_user(username: str):
    return User(username=username)

async def get_admin_user(user: Annotated[User, Depends(get_current_user)]):
    if user.username != "admin":
        raise HTTPException(status_code=401, detail="Required to be an admin user to perform operation")
    return user