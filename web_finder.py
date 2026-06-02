from __future__ import annotations

import asyncio
import logging

from enrichment.domain_analyzer import analyze_domain
from scoring.icp_classifier import classify_icp
from sources.domain_search import search_domains_multi
from sources.query_builder import build_queries
from storage.lead_repository import save_leads

logger = logging.getLogger(__name__)


async def collect_web_icp_leads(
    *,
    preset: str = "all",
    custom_queries: str | None = None,
    total_limit: int = 40,
    per_query_limit: int = 8,
    concurrency: int = 8,
    search_category: str | None = None,
) -> dict:
    queries = build_queries(custom_queries, preset=preset)
    candidates = await search_domains_multi(
        queries,
        per_query_limit=per_query_limit,
        total_limit=total_limit,
    )

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def enrich(candidate: dict) -> dict | None:
        async with semaphore:
            domain = candidate.get("domain")
            if not domain:
                return None
            try:
                site = await analyze_domain(domain)
            except Exception as exc:
                logger.warning("[web_finder] analyze_failed domain=%s error=%s", domain, exc)
                site = {}

            has_contacts = bool(site.get("email") or site.get("phone"))
            classification = classify_icp(
                title=site.get("title") or candidate.get("company_name"),
                description=site.get("description"),
                h1=site.get("h1"),
                text=site.get("text"),
                company_name=candidate.get("company_name"),
                domain=domain,
                has_contacts=has_contacts,
                has_catalog=bool(site.get("has_catalog")),
                has_cart=bool(site.get("has_cart")),
                ecommerce_score=int(site.get("ecommerce_score") or 0),
                site_type=site.get("site_type"),
                site_assessment=site.get("site_assessment"),
            )

            return {
                "query": candidate.get("source_query") or (queries[0] if queries else preset),
                "search_category": search_category or preset,
                "company_name": candidate.get("company_name"),
                "domain": domain,
                "source": candidate.get("source", "ddgs"),
                "source_url": candidate.get("url"),
                "title": site.get("title") or candidate.get("company_name"),
                "company_email": site.get("email"),
                "company_phone": site.get("phone"),
                "company_inn": site.get("company_inn"),
                "company_ogrn": site.get("company_ogrn"),
                "company_legal_name": site.get("company_legal_name"),
                "legal_form": site.get("legal_form"),
                "inn_source": site.get("inn_source"),
                "has_contacts": has_contacts,
                "has_catalog": bool(site.get("has_catalog")),
                "has_cart": bool(site.get("has_cart")),
                "ecommerce_score": int(site.get("ecommerce_score") or 0),
                "site_type": site.get("site_type"),
                "site_assessment": site.get("site_assessment"),
                "sales_ready": bool(classification["is_icp"] and has_contacts),
                "status": "new",
                **classification,
            }

    enriched = [item for item in await asyncio.gather(*(enrich(candidate) for candidate in candidates)) if item]
    # Keep weak rows for learning, but avoid filling the CRM with obvious non-targets.
    filtered = [
        item
        for item in enriched
        if int(item.get("icp_score") or 0) >= 35
        or (item.get("has_contacts") and int(item.get("icp_score") or 0) >= 25)
    ]
    save_stats = save_leads(filtered)

    return {
        "queries": len(queries),
        "candidates": len(candidates),
        "analyzed": len(enriched),
        "kept": len(filtered),
        "created": save_stats.get("created", 0),
        "updated": save_stats.get("updated", 0),
        "skipped": save_stats.get("skipped", 0),
    }
