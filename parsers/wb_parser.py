from playwright.async_api import async_playwright


async def parse_wb(query: str):

    results = []

    url = f"https://www.wildberries.ru/catalog/0/search.aspx?search={query}"

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url)
        await page.wait_for_selector(".product-card")

        cards = await page.query_selector_all(".product-card")

        for card in cards[:20]:

            brand = await card.get_attribute("data-brand")

            link_el = await card.query_selector("a")

            link = None
            if link_el:
                link = await link_el.get_attribute("href")

            results.append({
                "brand": brand,
                "url": link
            })

        await browser.close()

    return results
