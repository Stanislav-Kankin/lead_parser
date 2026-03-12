import asyncio
from playwright.async_api import async_playwright


async def run_debug(query):

    url = f"https://www.wildberries.ru/catalog/0/search.aspx?search={query}"

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=False)

        page = await browser.new_page()

        requests = []

        page.on(
            "request",
            lambda req: requests.append(f"{req.resource_type} | {req.url}")
        )

        await page.goto(url)

        await page.wait_for_timeout(10000)

        with open("wb_network_requests.txt", "w", encoding="utf-8") as f:
            for r in requests:
                f.write(r + "\n")

        print("Saved requests to wb_network_requests.txt")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run_debug("кроссовки"))
