from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault

from configs.settings import env_parameters

if env_parameters.PROD_MODE:
    from core.wi18n import gettext_async
else:
    from aiogram.utils.i18n import gettext

available_roles = ["admin", "clamper", "member"]


# i18n function
async def _(text: str, **kwargs):
    return gettext(text).format(**kwargs)
