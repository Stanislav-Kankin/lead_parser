from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from parsers.wb_parser import parse_wb

router = Router()


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Найти бренды WB", callback_data="find_wb")],
        ]
    )


@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Lead Parser\n\n"
        "Нажми кнопку ниже или просто отправь запрос.\n"
        "Например: кроссовки",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "find_wb")
async def find_wb(callback: CallbackQuery):
    await callback.message.answer("Отправь запрос для поиска брендов на Wildberries.\nНапример: кроссовки")
    await callback.answer()


@router.message()
async def handle_query(message: Message):
    query = (message.text or "").strip()
    if not query:
        await message.answer("Пришли текстовый запрос. Например: кроссовки")
        return

    await message.answer("Ищу бренды...")

    try:
        data = await parse_wb(query)
    except Exception as e:
        await message.answer(
            "WB сейчас не отдал карточки или изменилась верстка.\n"
            f"Текст ошибки: {e}"
        )
        return

    brands = []
    seen = set()
    for item in data:
        brand = (item.get("brand") or "").strip()
        if brand and brand not in seen:
            seen.add(brand)
            brands.append(brand)

    if not brands:
        await message.answer("Бренды не найдены. Попробуй другой запрос.")
        return

    text = "Найденные бренды:\n\n"
    for idx, brand in enumerate(brands[:10], start=1):
        text += f"{idx}. {brand}\n"

    await message.answer(text)
