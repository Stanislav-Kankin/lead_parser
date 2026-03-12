import asyncio
from typing import Any

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


SEARCH_SELECTORS = [
    "[data-nm-id]",
    ".product-card",
    ".j-card-item",
    "article",
]


async def _extract_from_cards(page) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for selector in SEARCH_SELECTORS:
        locator = page.locator(selector)
        count = await locator.count()
        if count == 0:
            continue

        limit = min(count, 20)
        for i in range(limit):
            item = locator.nth(i)

            brand = None
            url = None

            brand_candidates = [
                ".product-card__brand",
                ".brand-name",
                "[class*='brand']",
            ]
            for brand_selector in brand_candidates:
                inner = item.locator(brand_selector).first
                if await inner.count():
                    text = (await inner.text_content() or "").strip()
                    if text:
                        brand = text
                        break

            link_candidates = [
                "a[href*='/catalog/']",
                "a[href]",
            ]
            for link_selector in link_candidates:
                link_el = item.locator(link_selector).first
                if await link_el.count():
                    href = await link_el.get_attribute("href")
                    if href:
                        if href.startswith("/"):
                            href = f"https://www.wildberries.ru{href}"
                        url = href
                        break

            if url and url not in seen_urls:
                seen_urls.add(url)
                results.append({
                    "brand": brand,
                    "url": url,
                })

        if results:
            return results

    return results


async def parse_wb(query: str) -> list[dict[str, Any]]:
    url = f"https://www.wildberries.ru/catalog/0/search.aspx?search={query}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2200},
            locale="ru-RU",
        )

        page = await context.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeoutError:
            # Даже если часть ресурсов не догрузилась, пробуем читать страницу дальше.
            pass

        # Небольшая пауза на гидратацию фронта
        await asyncio.sleep(3)

        results = await _extract_from_cards(page)

        if not results:
            html_path = "wb_debug_last.html"
            screenshot_path = "wb_debug_last.png"
            await page.screenshot(path=screenshot_path, full_page=True)
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(await page.content())
            await browser.close()
            raise RuntimeError(
                "Не удалось найти карточки товаров WB. "
                f"Сохранены debug-файлы: {html_path}, {screenshot_path}"
            )

        await browser.close()
        return results
