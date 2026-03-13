from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.types import InputMessagesFilterEmpty

from .client import get_client
from .signal_classifier import classify_signal
from .repository import save_signal


SEARCH_QUERIES = [
    "wildberries комиссия",
    "ozon комиссия",
    "маркетплейс комиссия",
    "как развивать интернет магазин",
    "нужен трафик на сайт",
]


async def search_signals(limit=50):

    client = get_client()

    await client.start()

    results = []

    for query in SEARCH_QUERIES:

        response = await client(
            SearchGlobalRequest(
                q=query,
                filter=InputMessagesFilterEmpty(),
                min_date=None,
                max_date=None,
                offset_rate=0,
                offset_peer=None,
                offset_id=0,
                limit=limit,
            )
        )

        for msg in response.messages:

            if not msg.message:
                continue

            signal = classify_signal(msg.message)

            if signal["level"] == "low":
                continue

            chat = msg.peer_id

            data = {
                "chat_title": str(chat),
                "chat_username": None,
                "chat_url": None,
                "message_id": msg.id,
                "message_date": msg.date,
                "author_username": None,
                "message_text": msg.message,
                "text_excerpt": msg.message[:200],
                "matched_keywords": ",".join(signal["matches"]),
                "signal_score": signal["score"],
                "signal_level": signal["level"],
            }

            save_signal(data)

            results.append(data)

    await client.disconnect()

    return results