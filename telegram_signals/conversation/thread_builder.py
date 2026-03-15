from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from telethon.tl.types import Message


@dataclass(slots=True)
class ThreadMessageView:
    message: Message
    root_message_id: int
    parent_message_id: int | None
    reply_depth: int
    conversation_key: str
    context_text: str
    conversation_text: str
    chain_size: int


def _message_text(message: Message) -> str:
    return (getattr(message, "message", None) or "").strip()


def _build_index(messages: Iterable[Message]) -> dict[int, Message]:
    index: dict[int, Message] = {}
    for message in messages:
        message_id = getattr(message, "id", None)
        if message_id:
            index[int(message_id)] = message
    return index


def _resolve_chain(message: Message, message_index: dict[int, Message]) -> tuple[int, list[Message]]:
    chain: list[Message] = [message]
    seen: set[int] = {int(getattr(message, "id", 0) or 0)}
    current = message

    while True:
        parent_id = getattr(current, "reply_to_msg_id", None)
        if not parent_id:
            break
        parent = message_index.get(int(parent_id))
        if not parent:
            break
        parent_message_id = int(getattr(parent, "id", 0) or 0)
        if not parent_message_id or parent_message_id in seen:
            break
        chain.append(parent)
        seen.add(parent_message_id)
        current = parent

    chain.reverse()
    root_id = int(getattr(chain[0], "id", 0) or 0)
    return root_id, chain


def build_thread_views(messages: list[Message]) -> list[ThreadMessageView]:
    if not messages:
        return []

    ordered_messages = sorted(
        messages,
        key=lambda item: (
            getattr(item, "date", None) or 0,
            getattr(item, "id", 0) or 0,
        ),
    )
    message_index = _build_index(ordered_messages)
    thread_map: dict[int, list[Message]] = {}
    chain_cache: dict[int, tuple[int, list[Message]]] = {}

    for message in ordered_messages:
        message_id = int(getattr(message, "id", 0) or 0)
        root_id, chain = _resolve_chain(message, message_index)
        chain_cache[message_id] = (root_id, chain)
        thread_map.setdefault(root_id, []).append(message)

    result: list[ThreadMessageView] = []
    for message in ordered_messages:
        message_id = int(getattr(message, "id", 0) or 0)
        root_id, chain = chain_cache[message_id]
        thread_messages = thread_map.get(root_id, [message])
        thread_messages = sorted(
            thread_messages,
            key=lambda item: (
                getattr(item, "date", None) or 0,
                getattr(item, "id", 0) or 0,
            ),
        )

        current_text = _message_text(message)
        context_parts: list[str] = []
        for item in thread_messages:
            item_id = int(getattr(item, "id", 0) or 0)
            text = _message_text(item)
            if not text or item_id == message_id:
                continue
            context_parts.append(text)

        conversation_parts: list[str] = []
        for item in thread_messages:
            text = _message_text(item)
            if text:
                conversation_parts.append(text)

        result.append(
            ThreadMessageView(
                message=message,
                root_message_id=root_id,
                parent_message_id=getattr(message, "reply_to_msg_id", None),
                reply_depth=max(len(chain) - 1, 0),
                conversation_key=f"{root_id}:{len(thread_messages)}",
                context_text="\n".join(context_parts),
                conversation_text="\n".join(conversation_parts),
                chain_size=len(thread_messages),
            )
        )

    return result
