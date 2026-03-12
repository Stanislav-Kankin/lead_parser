from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command

from parsers.wb_parser import parse_wb

router = Router()


@router.message(Command("start"))
async def start(message: Message):

    await message.answer(
        "Lead Parser\n\n"
        "Введите запрос для поиска брендов на WB\n"
        "Например:\n"
        "`кроссовки`",
        parse_mode="Markdown"
    )


@router.message()
async def handle_query(message: Message):

    query = message.text

    await message.answer("Ищу бренды...")

    data = await parse_wb(query)

    brands = list(set([x["brand"] for x in data if x["brand"]]))

    text = "Найденные бренды:\n\n"

    for b in brands[:10]:
        text += f"• {b}\n"

    await message.answer(text)
