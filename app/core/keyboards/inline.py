from aiogram.types import InlineKeyboardMarkup, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder
from core.utils.texts import _
from configs.settings import env_parameters


async def pick_status_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=await _("MEMBER_BUTTON"), callback_data="member")
    kb.button(text=await _("CLAMPER_BUTTON"), callback_data="clamper")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


async def get_verification_code_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=await _("RESEND_EMAIL"), callback_data="resend_email")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


def start_webapp(button_text: str, url: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=button_text, web_app=WebAppInfo(url=url))
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)
