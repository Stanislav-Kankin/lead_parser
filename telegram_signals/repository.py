from __future__ import annotations

from typing import Iterable

from datetime import datetime

from sqlalchemy import case, desc, func, or_, select

from storage.db import SessionLocal
from telegram_signals.signal_classifier import classify_signal
from .models import SearchProfile, TelegramSignal, TelegramSignalComment

WORKING_LEAD_FITS = ["hot_outreach", "warm_reply", "warm_hypothesis", "target", "review"]
OUTREACH_LEAD_FITS = ["hot_outreach", "target"]
REVIEW_LEAD_FITS = ["warm_reply", "nurture", "review"]

AD_TEXT_EXCLUDE_PATTERNS = (
    "наклейки-замки",
    "на ваши зип-пакеты",
    "ваш идеальный помощник",
    "нужен товар с рынка",
    "мы выкупим",
    "упакуем",
    "доставим в любую точку",
    "этапы сотрудничества",
    "поиск одежды по фото",
    "показ товаров по видеосвязи",
)


def _exclude_obvious_ads(stmt):
    for pattern in AD_TEXT_EXCLUDE_PATTERNS:
        stmt = stmt.where(~TelegramSignal.message_text.ilike(f"%{pattern}%"))
    return stmt


def _author_identity_filter(item: dict):
    author_id = str(item.get("author_id") or "").strip()
    author_username = str(item.get("author_username") or "").strip()
    clauses = []
    if author_id:
        clauses.append(TelegramSignal.author_id == author_id)
    if author_username:
        clauses.append(TelegramSignal.author_username.ilike(author_username))
    if not clauses:
        return None
    return or_(*clauses)


def _apply_author_history(session, item: dict) -> None:
    identity_filter = _author_identity_filter(item)
    if identity_filter is None:
        return

    contacted = session.execute(
        select(TelegramSignal.id).where(
            identity_filter,
            TelegramSignal.status == "contacted",
        ).limit(1)
    ).first()
    if contacted:
        item["status"] = "contacted"
        item["review_status"] = "ok"
        item["reject_reason"] = None
        return

    not_ok = session.execute(
        select(TelegramSignal.id).where(
            identity_filter,
            TelegramSignal.review_status == "not_ok",
        ).limit(1)
    ).first()
    if not_ok:
        item["review_status"] = "not_ok"


def save_signals(items: Iterable[dict]) -> dict:
    created = 0
    updated = 0

    with SessionLocal() as session:
        for item in items:
            exists = session.execute(
                select(TelegramSignal).where(
                    TelegramSignal.chat_id == item.get("chat_id"),
                    TelegramSignal.message_id == item.get("message_id"),
                )
            ).scalar_one_or_none()

            if exists:
                for k, v in item.items():
                    if k in {"review_status", "reject_reason", "reviewed_at", "status"}:
                        continue
                    setattr(exists, k, v)
                updated += 1
            else:
                _apply_author_history(session, item)
                session.add(TelegramSignal(**item))
                created += 1
        session.commit()

    return {"created": created, "updated": updated}


RECLASSIFY_FIELDS = [
    "matched_keywords",
    "signal_score",
    "signal_level",
    "recommended_opener",
    "conversation_score",
    "pain_detected",
    "icp_detected",
    "message_type",
    "conversation_type",
    "author_type_guess",
    "icp_score",
    "pain_score",
    "intent_score",
    "context_score",
    "owner_likelihood_score",
    "promo_penalty",
    "contractor_penalty",
    "final_lead_score",
    "contactability_score",
    "contact_entity_type",
    "contact_entity_score",
    "is_person_reachable",
    "lead_fit",
    "next_step",
    "why_actionable",
    "company_hint",
    "website_hint",
    "contact_hint",
    "outreach_segment",
    "outreach_stage",
    "cjm_stage",
    "outreach_angle",
    "bridge_to_offer",
    "best_reply_draft",
    "next_question",
    "reply_tone",
    "lead_category",
    "lead_score_100",
    "likely_icp",
    "marketplace",
    "niche",
    "budget_hint",
    "urgency",
    "opener_soft",
    "opener_expert",
    "opener_sales",
    "is_actionable",
]


def reclassify_existing_signals(limit: int | None = None) -> dict:
    updated = 0
    with SessionLocal() as session:
        stmt = select(TelegramSignal).order_by(desc(TelegramSignal.created_at), desc(TelegramSignal.id))
        if limit:
            stmt = stmt.limit(limit)
        items = list(session.execute(stmt).scalars().all())
        for item in items:
            signal = classify_signal(
                item.message_text or "",
                item.segment or "ecom_marketplace_pain",
                context_text="",
                conversation_text="",
                author_username=item.author_username,
                author_name=item.author_name,
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                reply_depth=item.reply_depth or 0,
            )
            matched = signal.get("matched_keywords")
            if isinstance(matched, list):
                signal["matched_keywords"] = ",".join(str(value) for value in matched if value)
            for field in RECLASSIFY_FIELDS:
                if field in signal:
                    setattr(item, field, signal[field])
            if signal.get("lead_fit") in {"not_icp", "noise", "market_insight", "contractor"}:
                if item.review_status == "unchecked":
                    item.reject_reason = item.reject_reason or _auto_reject_reason(signal)
            updated += 1
        session.commit()
    return {"updated": updated}


def _auto_reject_reason(signal: dict) -> str:
    message_type = str(signal.get("message_type") or "")
    lead_category = str(signal.get("lead_category") or "")
    if message_type == "vacancy":
        return "not_icp"
    if message_type in {"service_ad", "supplier_ad"}:
        return "supplier_or_ad"
    if lead_category in {"taxes", "certification", "returns_logistics"}:
        return "operations_only"
    if message_type in {"market_intelligence", "expert_content"}:
        return "soft_opinion"
    return "not_icp"


def get_signals(
    segment: str | None = None,
    limit: int | None = None,
    *,
    only_actionable: bool = False,
    conversation_type: str | None = None,
    business_only: bool = False,
    lead_fit: str | None = None,
    lead_fit_in: list[str] | None = None,
    review_status: str | None = None,
    review_status_in: list[str] | None = None,
    reject_reason: str | None = None,
    status: str | None = None,
    status_not: str | None = None,
    crm_tag: str | None = None,
    cjm_stage: str | None = None,
    min_score: int | None = None,
    marketplace: str | None = None,
    niche: str | None = None,
    lead_category: str | None = None,
    offset: int | None = None,
) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        if only_actionable:
            stmt = stmt.where(TelegramSignal.is_actionable == True)  # noqa: E712
        if conversation_type:
            stmt = stmt.where(TelegramSignal.conversation_type == conversation_type)
        if business_only:
            stmt = stmt.where(TelegramSignal.author_type_guess == "business")
        if lead_fit:
            stmt = stmt.where(TelegramSignal.lead_fit == lead_fit)
        if lead_fit_in:
            stmt = stmt.where(TelegramSignal.lead_fit.in_(lead_fit_in))
        if review_status:
            stmt = stmt.where(TelegramSignal.review_status == review_status)
        if review_status_in:
            stmt = stmt.where(TelegramSignal.review_status.in_(review_status_in))
        if reject_reason:
            stmt = stmt.where(TelegramSignal.reject_reason == reject_reason)
        if status:
            stmt = stmt.where(TelegramSignal.status == status)
        if status_not:
            stmt = stmt.where(TelegramSignal.status != status_not)
        if crm_tag:
            stmt = stmt.where(
                or_(
                    TelegramSignal.crm_tag == crm_tag,
                    TelegramSignal.crm_tag.like(f"{crm_tag},%"),
                    TelegramSignal.crm_tag.like(f"%,{crm_tag}"),
                    TelegramSignal.crm_tag.like(f"%,{crm_tag},%"),
                )
            )
        if cjm_stage:
            stmt = stmt.where(TelegramSignal.cjm_stage == cjm_stage)
        if min_score is not None:
            stmt = stmt.where(TelegramSignal.lead_score_100 >= min_score)
        if marketplace:
            stmt = stmt.where(TelegramSignal.marketplace == marketplace)
        if niche:
            stmt = stmt.where(TelegramSignal.niche == niche)
        if lead_category:
            stmt = stmt.where(TelegramSignal.lead_category == lead_category)
        if only_actionable or lead_fit or lead_fit_in or review_status or review_status_in:
            stmt = _exclude_obvious_ads(stmt)

        level_order = case(
            (TelegramSignal.signal_level == "high", 3),
            (TelegramSignal.signal_level == "medium", 2),
            else_=1,
        )
        stmt = stmt.order_by(
            desc(TelegramSignal.is_person_reachable),
            desc(TelegramSignal.lead_score_100),
            desc(TelegramSignal.contact_entity_score),
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.conversation_score),
            desc(TelegramSignal.reply_depth),
            desc(TelegramSignal.contactability_score),
            desc(level_order),
            desc(TelegramSignal.signal_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            if offset:
                stmt = stmt.offset(offset)
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def count_signals(
    *,
    lead_fit_in: list[str] | None = None,
    review_status: str | None = None,
    reject_reason: str | None = None,
    status: str | None = None,
    crm_tag: str | None = None,
    cjm_stage: str | None = None,
    min_score: int | None = None,
    marketplace: str | None = None,
    niche: str | None = None,
    lead_category: str | None = None,
) -> int:
    with SessionLocal() as session:
        stmt = select(func.count(TelegramSignal.id))
        if lead_fit_in:
            stmt = stmt.where(TelegramSignal.lead_fit.in_(lead_fit_in))
        if review_status:
            stmt = stmt.where(TelegramSignal.review_status == review_status)
        if reject_reason:
            stmt = stmt.where(TelegramSignal.reject_reason == reject_reason)
        if status:
            stmt = stmt.where(TelegramSignal.status == status)
        if crm_tag:
            stmt = stmt.where(
                or_(
                    TelegramSignal.crm_tag == crm_tag,
                    TelegramSignal.crm_tag.like(f"{crm_tag},%"),
                    TelegramSignal.crm_tag.like(f"%,{crm_tag}"),
                    TelegramSignal.crm_tag.like(f"%,{crm_tag},%"),
                )
            )
        if cjm_stage:
            stmt = stmt.where(TelegramSignal.cjm_stage == cjm_stage)
        if min_score is not None:
            stmt = stmt.where(TelegramSignal.lead_score_100 >= min_score)
        if marketplace:
            stmt = stmt.where(TelegramSignal.marketplace == marketplace)
        if niche:
            stmt = stmt.where(TelegramSignal.niche == niche)
        if lead_category:
            stmt = stmt.where(TelegramSignal.lead_category == lead_category)
        return int(session.execute(stmt).scalar_one() or 0)


def get_reject_reason_stats() -> list[dict]:
    with SessionLocal() as session:
        reason = func.coalesce(TelegramSignal.reject_reason, "unknown")
        stmt = (
            select(
                reason.label("reason"),
                func.count(TelegramSignal.id).label("total"),
                func.avg(TelegramSignal.lead_score_100).label("avg_score"),
            )
            .where(TelegramSignal.review_status == "not_ok")
            .group_by(reason)
            .order_by(desc(func.count(TelegramSignal.id)))
        )
        return [
            {
                "reason": row.reason,
                "total": int(row.total or 0),
                "avg_score": round(float(row.avg_score or 0)),
            }
            for row in session.execute(stmt).all()
        ]


def get_source_quality_stats(limit: int = 40, min_total: int = 1) -> list[dict]:
    with SessionLocal() as session:
        total_count = func.count(TelegramSignal.id)
        ok_count = func.sum(case((TelegramSignal.review_status == "ok", 1), else_=0))
        not_ok_count = func.sum(case((TelegramSignal.review_status == "not_ok", 1), else_=0))
        unchecked_count = func.sum(case((TelegramSignal.review_status == "unchecked", 1), else_=0))
        working_count = func.sum(case((TelegramSignal.lead_fit.in_(WORKING_LEAD_FITS), 1), else_=0))
        hot_count = func.sum(case((TelegramSignal.lead_score_100 >= 80, 1), else_=0))
        reachable_count = func.sum(case((TelegramSignal.is_person_reachable == True, 1), else_=0))  # noqa: E712
        avg_score = func.avg(TelegramSignal.lead_score_100)

        stmt = (
            select(
                TelegramSignal.chat_title.label("chat_title"),
                TelegramSignal.chat_username.label("chat_username"),
                total_count.label("total"),
                ok_count.label("ok"),
                not_ok_count.label("not_ok"),
                unchecked_count.label("unchecked"),
                working_count.label("working"),
                hot_count.label("hot"),
                reachable_count.label("reachable"),
                avg_score.label("avg_score"),
            )
            .where(TelegramSignal.chat_title.is_not(None))
            .group_by(TelegramSignal.chat_title, TelegramSignal.chat_username)
            .having(total_count >= min_total)
            .order_by(desc(ok_count), desc(hot_count), desc(avg_score), desc(total_count))
            .limit(limit)
        )
        rows = []
        for row in session.execute(stmt).all():
            total = int(row.total or 0)
            ok = int(row.ok or 0)
            not_ok = int(row.not_ok or 0)
            rows.append(
                {
                    "chat_title": row.chat_title or "Без названия",
                    "chat_username": row.chat_username or "",
                    "total": total,
                    "ok": ok,
                    "not_ok": not_ok,
                    "unchecked": int(row.unchecked or 0),
                    "working": int(row.working or 0),
                    "hot": int(row.hot or 0),
                    "reachable": int(row.reachable or 0),
                    "avg_score": round(float(row.avg_score or 0)),
                    "ok_rate": round(ok * 100 / total) if total else 0,
                    "reject_rate": round(not_ok * 100 / total) if total else 0,
                }
            )
        return rows


def get_target_leads(segment: str | None = None, limit: int | None = None, *, include_reviewed: bool = False) -> list[TelegramSignal]:
    kwargs = {} if include_reviewed else {"review_status": "unchecked"}
    return get_signals(segment=segment, limit=limit, lead_fit_in=OUTREACH_LEAD_FITS, **kwargs)


def get_review_leads(segment: str | None = None, limit: int | None = None, *, include_reviewed: bool = False) -> list[TelegramSignal]:
    kwargs = {} if include_reviewed else {"review_status": "unchecked"}
    return get_signals(segment=segment, limit=limit, lead_fit_in=REVIEW_LEAD_FITS, **kwargs)


def get_discussion_leads(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        stmt = stmt.where(
            TelegramSignal.lead_fit.in_(WORKING_LEAD_FITS + ["nurture"]),
            TelegramSignal.author_type_guess != "contractor",
            or_(
                TelegramSignal.context_score >= 2,
                TelegramSignal.conversation_score >= 2,
                TelegramSignal.conversation_type.in_(["discussion", "complaint", "help_request", "question"]),
                TelegramSignal.pain_score >= 3,
                TelegramSignal.intent_score >= 4,
            ),
        )
        stmt = stmt.order_by(
            desc(TelegramSignal.conversation_score),
            desc(TelegramSignal.context_score),
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_business_like_messages(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, business_only=True, lead_fit_in=WORKING_LEAD_FITS)


def get_market_intelligence(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    with SessionLocal() as session:
        stmt = select(TelegramSignal)
        if segment:
            stmt = stmt.where(TelegramSignal.segment == segment)
        stmt = stmt.where(TelegramSignal.message_type.in_(["expert_content", "market_intelligence"]))
        stmt = stmt.order_by(
            desc(TelegramSignal.final_lead_score),
            desc(TelegramSignal.message_date),
            desc(TelegramSignal.created_at),
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_reviewed_leads(review_status: str, segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, lead_fit_in=WORKING_LEAD_FITS, review_status=review_status, status_not="contacted")


def get_contacted_leads(segment: str | None = None, limit: int | None = None) -> list[TelegramSignal]:
    return get_signals(segment=segment, limit=limit, lead_fit_in=WORKING_LEAD_FITS, review_status="ok", status="contacted")


def get_hot_leads(limit: int | None = 10) -> list[TelegramSignal]:
    return get_signals(limit=limit, lead_fit_in=OUTREACH_LEAD_FITS, review_status="unchecked", status_not="contacted", min_score=80)


def get_signal_by_id(signal_id: int) -> TelegramSignal | None:
    with SessionLocal() as session:
        return session.get(TelegramSignal, signal_id)


def add_signal_comment(signal_id: int, comment: str, author: str = "dashboard") -> bool:
    text = (comment or "").strip()
    if not text:
        return False
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.comment = text
        session.add(
            TelegramSignalComment(
                signal_id=signal_id,
                comment=text,
                author=author,
            )
        )
        session.commit()
        return True


def get_signal_comments(signal_id: int, limit: int | None = None) -> list[TelegramSignalComment]:
    with SessionLocal() as session:
        stmt = (
            select(TelegramSignalComment)
            .where(TelegramSignalComment.signal_id == signal_id)
            .order_by(desc(TelegramSignalComment.created_at), desc(TelegramSignalComment.id))
        )
        if limit:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def get_signal_comments_map(signal_ids: list[int], limit_per_signal: int = 3) -> dict[int, list[TelegramSignalComment]]:
    if not signal_ids:
        return {}
    with SessionLocal() as session:
        stmt = (
            select(TelegramSignalComment)
            .where(TelegramSignalComment.signal_id.in_(signal_ids))
            .order_by(
                TelegramSignalComment.signal_id,
                desc(TelegramSignalComment.created_at),
                desc(TelegramSignalComment.id),
            )
        )
        result: dict[int, list[TelegramSignalComment]] = {signal_id: [] for signal_id in signal_ids}
        for comment in session.execute(stmt).scalars().all():
            bucket = result.setdefault(comment.signal_id, [])
            if len(bucket) < limit_per_signal:
                bucket.append(comment)
        return result


def set_signal_review_status(signal_id: int, review_status: str, *, reject_reason: str | None = None) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.review_status = review_status
        item.reject_reason = reject_reason if review_status == "not_ok" else None
        item.reviewed_at = datetime.utcnow()
        session.commit()
        return True


def set_signal_status(signal_id: int, status: str, *, review_status: str | None = None, reject_reason: str | None = None) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        item.status = status
        if review_status:
            item.review_status = review_status
            item.reject_reason = reject_reason if review_status == "not_ok" else None
            item.reviewed_at = datetime.utcnow()
        session.commit()
        return True


def update_signal_crm(
    signal_id: int,
    *,
    status: str | None = None,
    crm_tag: str | None = None,
    comment: str | None = None,
    review_status: str | None = None,
    reject_reason: str | None = None,
) -> bool:
    with SessionLocal() as session:
        item = session.get(TelegramSignal, signal_id)
        if item is None:
            return False
        if status is not None:
            item.status = status
        if crm_tag is not None:
            item.crm_tag = crm_tag or None
        if comment is not None:
            comment_text = comment.strip()
            if comment_text:
                item.comment = comment_text
                session.add(
                    TelegramSignalComment(
                        signal_id=signal_id,
                        comment=comment_text,
                        author="dashboard",
                    )
                )
        if review_status:
            item.review_status = review_status
            item.reject_reason = reject_reason if review_status == "not_ok" else None
            item.reviewed_at = datetime.utcnow()
        session.commit()
        return True


def list_search_profiles(active_only: bool = False) -> list[SearchProfile]:
    with SessionLocal() as session:
        stmt = select(SearchProfile).order_by(desc(SearchProfile.is_active), SearchProfile.id)
        if active_only:
            stmt = stmt.where(SearchProfile.is_active == True)  # noqa: E712
        return list(session.execute(stmt).scalars().all())


def get_search_profile(profile_id: int) -> SearchProfile | None:
    with SessionLocal() as session:
        return session.get(SearchProfile, profile_id)


def save_search_profile(data: dict, profile_id: int | None = None) -> SearchProfile | None:
    with SessionLocal() as session:
        item = session.get(SearchProfile, profile_id) if profile_id else SearchProfile()
        if item is None:
            return None
        for key in [
            "name",
            "segment",
            "queries_text",
            "source_chats_text",
            "stop_words_text",
            "good_chat_hints_text",
            "bad_chat_hints_text",
            "max_age_hours",
            "limit_chats",
            "limit_messages_per_chat",
            "min_score",
            "is_active",
        ]:
            if key in data:
                setattr(item, key, data[key])
        item.updated_at = datetime.utcnow()
        session.add(item)
        session.commit()
        session.refresh(item)
        return item
