import httpx
from bs4 import BeautifulSoup


async def fetch_site_title(domain: str) -> str | None:
    variants = [f"https://{domain}", f"http://{domain}"]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    async with httpx.AsyncClient(timeout=12, follow_redirects=True, headers=headers, verify=False) as client:
        for url in variants:
            try:
                response = await client.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "lxml")
                title = soup.title.get_text(" ", strip=True) if soup.title else None
                if title:
                    return title[:200]
            except Exception:
                continue

    return None
