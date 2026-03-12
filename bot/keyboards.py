from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Найти компании", callback_data="find_companies")],
            [InlineKeyboardButton(text="📋 Последние лиды", callback_data="last_leads")],
        ]
    )
