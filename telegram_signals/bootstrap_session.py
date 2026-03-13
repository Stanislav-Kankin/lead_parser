from __future__ import annotations

import asyncio

from .client import get_client


async def main():
    client = get_client()
    await client.start()
    me = await client.get_me()
    print(f"Telegram session created for: {getattr(me, 'username', None) or getattr(me, 'id', 'unknown')}")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
