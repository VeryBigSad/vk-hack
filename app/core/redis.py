from datetime import timedelta
from typing import Optional

import redis.asyncio as aioredis
from configs.settings import env_parameters

pool = aioredis.ConnectionPool.from_url(
    f"redis://{env_parameters.REDIS_DB_HOST}:{env_parameters.REDIS_DB_PORT}/{env_parameters.REDIS_DB_DATABASE}",
    decode_responses=True,
)
redis_client = aioredis.Redis(connection_pool=pool)


async def get_by_key(key: str) -> Optional[str]:
    return await redis_client.get(key)


async def set_by_key(key: str, value: str, ttl: Optional[int | timedelta] = None) -> None:
    await redis_client.set(key, value, ex=ttl)


async def delete_by_key(key: str) -> None:
    return await redis_client.delete(key)

