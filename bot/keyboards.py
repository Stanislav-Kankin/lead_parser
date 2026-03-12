from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Найти компании")],
            [KeyboardButton(text="📋 Новые"), KeyboardButton(text="🛠 В работе")],
            [KeyboardButton(text="☎ Контакт был"), KeyboardButton(text="✅ Готово")],
        ],
        resize_keyboard=True
    )


def pagination_keyboard(prefix: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    buttons = []

    if page > 0:
        buttons.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"{prefix}:{page - 1}"
            )
        )

    buttons.append(
        InlineKeyboardButton(
            text=f"{page + 1}/{total_pages}",
            callback_data="page_info"
        )
    )

    if page < total_pages - 1:
        buttons.append(
            InlineKeyboardButton(
                text="Вперёд ➡️",
                callback_data=f"{prefix}:{page + 1}"
            )
        )

    return InlineKeyboardMarkup(inline_keyboard=[buttons])