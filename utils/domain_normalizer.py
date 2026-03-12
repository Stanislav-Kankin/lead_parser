from urllib.parse import urlparse


def normalize_domain(url_or_domain: str | None) -> str | None:
    if not url_or_domain:
        return None

    value = url_or_domain.strip().lower()
    if not value:
        return None

    if "://" not in value:
        value = f"https://{value}"

    parsed = urlparse(value)
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if not host:
        return None

    if "@" in host:
        host = host.split("@", 1)[-1]
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    if host.endswith("."):
        host = host[:-1]

    return host or None
