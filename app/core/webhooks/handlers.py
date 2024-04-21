import json
import logging
from typing import Any

import user_agents
from core.redis import delete_by_key, get_by_key, set_by_key
from core.webhooks.models import UserPredictionsResponse, UserUpdateData
from fastapi import APIRouter, status
from core.ml import Model_Predictor

router = APIRouter(prefix="/api/v1", tags=["v1"])
logger = logging.getLogger(__name__)

predictor = Model_Predictor()

@router.get("/user/{user_id}/predictions", response_model=UserPredictionsResponse)
async def get_predictions(user_id: int):
    user_data = json.loads(await get_by_key(f"user:{user_id}"))
    await delete_by_key(f"user:{user_id}")
    logger.info(f"User data for id={user_id}: {user_data}")
    result = predictor.predict(user_data)
    return result


@router.put("/user/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def update_user(user_id: int, body: UserUpdateData):
    parsed = user_agents.parse(body.user_agent)
    browser, os, device, is_mobile = parsed.browser.family, parsed.os.family, parsed.device.family, parsed.is_mobile
    data: dict[str, Any] = body.model_dump()
    # add browser, os, device, is_mobile
    data["browser"] = browser
    data["os"] = os
    data["device"] = device
    data["is_mobile"] = is_mobile
    # save to redis
    data_list = await get_by_key(f"user:{user_id}")
    if data_list:
        data_list = json.loads(data_list)
        data_list.append(data)
    else:
        data_list = [data]
    await set_by_key(
        f"user:{user_id}",
        json.dumps(data_list),
    )

