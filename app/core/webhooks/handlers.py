import logging

from fastapi import APIRouter, status
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1", tags=["v1"])
logger = logging.getLogger(__name__)


class UserUpdateData(BaseModel):
    timstamp: int
    geo_id: int
    domain_id: int
    path_id: int
    user_agent: str


class UserPredictionsResponse(BaseModel):
    gender: int
    age: int


@router.get("/user/{user_id}/predictions", response_model=UserPredictionsResponse)
async def get_predictions(user_id: int):
    return {"gender": 1, "age": 42}


@router.put("/user/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def update_user(user_id: int, body: UserUpdateData):
    return 
    