from urllib.parse import urlparse


def normalize_domain(value: str | None) -> str | None:
    if not value:
        return None

    raw = value.strip().lower()
    if not raw:
        return None

    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if not host:
        return None

    if ":" in host:
        host = host.split(":", 1)[0]

    if host.startswith("www."):
        host = host[4:]

    return host.rstrip("./") or None
