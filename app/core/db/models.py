import logging
from datetime import datetime
from typing import Optional

from tortoise import fields
from tortoise.models import Model

logger = logging.getLogger(__name__)


class User(Model):
    class Meta:
        table = "users"
        ordering = ["created_at"]

    id = fields.BigIntField(pk=True)
    user_id = fields.BigIntField(unique=True, index=True)
    username = fields.CharField(max_length=32, index=True, null=True)
    fio = fields.TextField(null=True)
    phone = fields.TextField(null=True)
    city = fields.TextField(null=True)
    status = fields.CharField(max_length=32, default="member")  # clamper or member
    stage = fields.TextField(null=True)
    registered_at = fields.DatetimeField(null=True)
    referer_id = fields.BigIntField(null=True)
    email = fields.TextField(null=True)
    platform_password = fields.TextField(null=True)
    first_name = fields.CharField(max_length=64)
    last_name = fields.CharField(max_length=64, null=True)
    language_code = fields.CharField(max_length=2, null=True)
    is_premium = fields.BooleanField(null=True)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    @classmethod
    async def update_data(
        cls,
        user_id: int,
        first_name: str,
        last_name: str,
        username: str,
        language_code: str,
        is_premium: bool,
        referer_id: int | None,
    ):
        user = await cls.filter(user_id=user_id).first()
        if user is None:
            await cls.create(
                user_id=user_id,
                first_name=first_name,
                last_name=last_name,
                username=username,
                language_code=language_code,
                is_premium=is_premium,
                referer_id=referer_id,
            )
        else:
            if not user.referer_id:
                await cls.filter(user_id=user_id).update(
                    first_name=first_name,
                    last_name=last_name,
                    username=username,
                    language_code=language_code,
                    is_premium=is_premium,
                    referer_id=referer_id,
                    updated_at=datetime.now(),
                )
            else:  # don't erase refer_id
                await cls.filter(user_id=user_id).update(
                    first_name=first_name,
                    last_name=last_name,
                    username=username,
                    language_code=language_code,
                    is_premium=is_premium,
                    updated_at=datetime.now(),
                )

    @classmethod
    async def update_vk_data(
        cls,
        user_id: int,
        fio: str,
        phone: str,
        city: str,
        status: str,
        email: str,
        platform_password: str,
    ):
        await cls.filter(user_id=user_id).update(
            fio=fio,
            phone=phone,
            city=city,
            status=status,
            email=email,
            platform_password=platform_password,
        )

    @classmethod
    async def set_status(cls, user_id: int, status: Optional[str]):
        await cls.filter(user_id=user_id).update(status=status)

    @classmethod
    async def check_status(cls, user_id: int):
        if user_id is not None:
            user = await cls.get(user_id=user_id)
            if user is None:
                return None
            return user.status

    @classmethod
    async def update_stage(cls, user_id: int, stage: str):
        if stage == "registered":
            await cls.filter(user_id=user_id).update(
                stage=stage, registered_at=datetime.now()
            )
        await cls.filter(user_id=user_id).update(stage=stage)
