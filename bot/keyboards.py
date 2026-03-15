from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

PAGE_SIZE = 1


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
            [InlineKeyboardButton(text="Обновить WB / Ozon", callback_data="tg_collect:ecom_marketplace_pain")],
            [InlineKeyboardButton(text="Обновить сайт / direct", callback_data="tg_collect:ecom_direct_growth")],
            [InlineKeyboardButton(text="Обновить производители", callback_data="tg_collect:manufacturer_secondary")],
            [InlineKeyboardButton(text="🎯 Лиды: писать сейчас", callback_data="tg_targets:0:all")],
            [InlineKeyboardButton(text="🟡 Лиды: проверить", callback_data="tg_review:0:all")],
            [InlineKeyboardButton(text="📤 Excel: target + review", callback_data="tg_export:actionable")],
            [InlineKeyboardButton(text="⚙️ Debug / сырьё", callback_data="tg_debug_menu")],
        ]
    )


def telegram_signals_debug_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Все сигналы", callback_data="tg_list:0:all")],
            [InlineKeyboardButton(text="💬 Обсуждения с болью", callback_data="tg_discussions:0:all")],
            [InlineKeyboardButton(text="🏢 Похожи на бизнес", callback_data="tg_business:0:all")],
            [InlineKeyboardButton(text="📡 Рынок / гипотезы", callback_data="tg_market:0:all")],
            [
                InlineKeyboardButton(text="📤 Excel лиды", callback_data="tg_export:actionable"),
                InlineKeyboardButton(text="📤 Excel рынок", callback_data="tg_export:market"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="tg_signals_menu")],
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
