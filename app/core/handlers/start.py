import logging
from aiogram import Bot, types, Router, F
import tortoise.expressions as t_exp
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from core.db.models import User
from core.utils.texts import _


logger = logging.getLogger(__name__)
router = Router(name="Welcome router")


@router.message(Command(commands=["start"]), F.chat.type == "private")
async def start_command(
    message: types.Message, command: CommandObject
):
    await message.answer("yo")