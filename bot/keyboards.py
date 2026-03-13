from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

PAGE_SIZE = 3


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Найти компании", callback_data="find_companies")],
            [InlineKeyboardButton(text="📡 Telegram сигналы", callback_data="tg_signals_menu")],
            [InlineKeyboardButton(text="📋 Последние лиды", callback_data="last_leads:0")],
        ]
    )


def telegram_signals_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 WB / Ozon боль", callback_data="tg_collect:ecom_marketplace_pain")],
            [InlineKeyboardButton(text="🌐 Свой сайт / Direct", callback_data="tg_collect:ecom_direct_growth")],
            [InlineKeyboardButton(text="🏭 Производители", callback_data="tg_collect:manufacturer_secondary")],
            [InlineKeyboardButton(text="🔥 Последние сигналы", callback_data="tg_list:0:all")],
            [InlineKeyboardButton(text="🎯 Актуальные лиды", callback_data="tg_actionable:0:all")],
            [
                InlineKeyboardButton(text="📤 Excel лиды", callback_data="tg_export:actionable"),
                InlineKeyboardButton(text="📦 Excel raw", callback_data="tg_export:raw"),
            ],
        ]
    )


def pagination_keyboard(prefix: str, page: int, total_pages: int, extra: str | None = None) -> InlineKeyboardMarkup | None:
    if total_pages <= 1:
        return None

    row = []
    suffix = f":{extra}" if extra else ""
    if page > 0:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{prefix}:{page - 1}{suffix}"))
    row.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="page_info"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"{prefix}:{page + 1}{suffix}"))
    return InlineKeyboardMarkup(inline_keyboard=[row])
