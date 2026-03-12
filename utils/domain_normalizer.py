from urllib.parse import urlparse

COMMON_PUBLIC_SUFFIXES = {
    "co.uk",
    "com.au",
    "com.tr",
    "com.br",
    "com.ua",
    "co.il",
    "co.jp",
}


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

    host = host.strip("/ .")
    return host or None


def get_root_domain(value: str | None) -> str | None:
    host = normalize_domain(value)
    if not host:
        return None

    parts = host.split(".")
    if len(parts) <= 2:
        return host

    tail2 = ".".join(parts[-2:])
    tail3 = ".".join(parts[-3:])

    if tail2 in COMMON_PUBLIC_SUFFIXES:
        return ".".join(parts[-3:]) if len(parts) >= 3 else host

    if parts[-2] in {"co", "com", "net", "org", "gov"} and len(parts[-1]) == 2:
        return tail3

    return tail2


def domains_for_lookup(value: str | None) -> list[str]:
    normalized = normalize_domain(value)
    if not normalized:
        return []

    root = get_root_domain(normalized)
    variants: list[str] = []
    for item in [normalized, root]:
        if item and item not in variants:
            variants.append(item)
    return variants
