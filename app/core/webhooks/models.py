from pydantic import BaseModel


class UserUpdateData(BaseModel):
    timstamp: int
    geo_id: int
    domain_id: int
    path_id: int
    user_agent: str


class UserPredictionsResponse(BaseModel):
    gender: int
    age: int
    