import asyncio
from .client import get_client


async def main():

    client = get_client()

    await client.start()

    print("Telegram session created")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())