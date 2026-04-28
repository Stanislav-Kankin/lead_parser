from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from html import escape
from threading import Lock
from urllib.parse import parse_qs, urlencode

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from storage.db import init_db
from telegram_signals.exporter import export_signals_to_xlsx
from telegram_signals.keywords import SEGMENT_LABELS
from telegram_signals.repository import (
    count_signals,
    get_search_profile,
    get_signal_comments_map,
    get_signals,
    list_search_profiles,
    save_search_profile,
    set_signal_review_status,
    set_signal_status,
    update_signal_crm,
)
from telegram_signals.service import collect_signals
from utils.time_format import format_msk

app = FastAPI(title="Telegram Signals")
logger = logging.getLogger(__name__)

DASHBOARD_JOB = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": None,
    "last_result": None,
}
JOB_LOCK = Lock()

STATUS_LABELS = {
    "new": "Новый",
    "reviewed": "Прочитал",
    "contacted": "Написал",
    "replied": "Ответил",
    "warm": "Теплый",
    "meeting_booked": "Встреча",
    "sale": "Продажа",
    "dead": "Архив",
}

CRM_TAG_LABELS = {
    "hot": "Горячий",
    "warm": "Теплый",
    "cold": "Холодный",
    "meeting": "Назначена встреча",
    "paused": "Пауза",
    "stopped": "Прекратили общение",
}

REVIEW_LABELS = {
    "unchecked": "Не разобран",
    "ok": "ОК",
    "not_ok": "Не ОК",
}

CATEGORY_LABELS = {
    "returns_logistics": "Возвраты / логистика",
    "unit_economics": "Экономика",
    "sales_growth": "Рост продаж",
    "marketplace_complaint": "Боль MP",
    "ads_complaint": "Реклама",
    "direct_channel": "Сайт / direct",
    "consultation_request": "Просит совет",
    "contractor_search": "Ищет подрядчика",
    "marketer_search": "Ищет маркетолога",
    "not_target": "Нецелевой",
}


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return RedirectResponse("/telegram-signals", status_code=302)


def _selected(current: str, value: str) -> str:
    return "selected" if current == value else ""


def _checked(value: bool) -> str:
    return "checked" if value else ""


def _short(value: str | None, limit: int) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _message_link(item) -> str:
    if item.chat_username and item.message_id:
        return f"https://t.me/{str(item.chat_username).lstrip('@')}/{item.message_id}"
    if item.chat_url and item.message_id:
        return f"{str(item.chat_url).rstrip('/')}/{item.message_id}"
    chat_id = str(item.chat_id or "").strip()
    if chat_id and item.message_id:
        internal_id = chat_id.removeprefix("-100").lstrip("-")
        if internal_id.isdigit():
            return f"https://t.me/c/{internal_id}/{item.message_id}"
    return ""


def _contact_link(item) -> str:
    if item.author_username:
        return f"https://t.me/{str(item.author_username).lstrip('@')}"
    return ""


def _return_url(request: Request) -> str:
    referer = request.headers.get("referer") or ""
    if "/telegram-signals" in referer:
        return referer
    return "/telegram-signals"


def _action_form(signal_id: int, label: str, status: str | None = None, review_status: str | None = None, tone: str = "ghost") -> str:
    params = {}
    if status:
        params["status"] = status
    if review_status:
        params["review_status"] = review_status
    action = f"/telegram-signals/{signal_id}/status"
    if params:
        action += "?" + urlencode(params)
    return (
        f"<form method='post' action='{escape(action)}' class='inline-form'>"
        f"<button type='submit' class='btn {tone}'>{escape(label)}</button>"
        "</form>"
    )


def _count_signals(**kwargs) -> int:
    return count_signals(lead_fit_in=["target", "review"], **kwargs)


def _lines(value: str | None) -> list[str]:
    return [line.strip() for line in (value or "").splitlines() if line.strip()]


def _profile_config(profile) -> dict:
    return {
        "queries": _lines(profile.queries_text),
        "stop_words": _lines(profile.stop_words_text),
        "good_chat_hints": _lines(profile.good_chat_hints_text),
        "bad_chat_hints": _lines(profile.bad_chat_hints_text),
        "min_score": profile.min_score or 0,
    }


def _run_collect_job(profile_id: int | None = None) -> None:
    with JOB_LOCK:
        if DASHBOARD_JOB["running"]:
            return
        DASHBOARD_JOB.update(
            {
                "running": True,
                "last_started_at": format_msk(datetime.utcnow()),
                "last_finished_at": None,
                "last_error": None,
                "last_result": None,
            }
        )

    async def runner() -> dict:
        totals = {"created": 0, "updated": 0, "scanned_chats": 0, "scanned_messages": 0, "kept_signals": 0}
        profiles = [get_search_profile(profile_id)] if profile_id else list_search_profiles(active_only=True)
        profiles = [profile for profile in profiles if profile is not None]
        for profile in profiles:
            result = await collect_signals(
                profile.segment,
                limit_chats=profile.limit_chats,
                limit_messages_per_chat=profile.limit_messages_per_chat,
                max_age_hours=profile.max_age_hours,
                profile=_profile_config(profile),
            )
            for key in totals:
                totals[key] += int(result.get(key, 0) or 0)
        return totals

    try:
        result = asyncio.run(runner())
        with JOB_LOCK:
            DASHBOARD_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": None,
                    "last_result": result,
                }
            )
    except Exception as exc:
        logger.exception("Dashboard Telegram collection failed")
        with JOB_LOCK:
            DASHBOARD_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": str(exc),
                    "last_result": None,
                }
            )


@app.post("/telegram-signals/collect")
async def collect_from_dashboard(request: Request, background_tasks: BackgroundTasks):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    profile_id = int(form.get("profile_id") or 0) or None
    with JOB_LOCK:
        running = bool(DASHBOARD_JOB["running"])
    if not running:
        background_tasks.add_task(_run_collect_job, profile_id)
    return RedirectResponse("/telegram-signals?review_status=unchecked", status_code=303)


@app.post("/telegram-signals/{signal_id}/status")
def update_signal_status(signal_id: int, request: Request, status: str | None = None, review_status: str | None = None):
    if status:
        set_signal_status(signal_id, status, review_status=review_status)
    elif review_status:
        set_signal_review_status(signal_id, review_status)
    return RedirectResponse(_return_url(request), status_code=303)


@app.post("/telegram-signals/{signal_id}/crm")
async def update_signal_crm_from_dashboard(signal_id: int, request: Request):
    raw_form = parse_qs((await request.body()).decode("utf-8"))
    form = {key: values[-1] for key, values in raw_form.items()}
    crm_tags = ",".join(tag for tag in raw_form.get("crm_tag", []) if tag)
    update_signal_crm(
        signal_id,
        status=str(form.get("status") or ""),
        crm_tag=crm_tags,
        comment=str(form.get("comment") or ""),
        review_status=str(form.get("review_status") or "") or None,
    )
    return RedirectResponse(_return_url(request), status_code=303)


@app.get("/telegram-signals/export")
def export_telegram_signals(kind: str = "all"):
    allowed = {"all", "ok", "not_ok", "review", "target", "raw", "actionable"}
    file_path = export_signals_to_xlsx(kind if kind in allowed else "all")
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.name,
    )


def _profile_form_value(form: dict, key: str, default: str = "") -> str:
    return str(form.get(key) or default).strip()


def _split_tags(value: str | None) -> list[str]:
    return [tag.strip() for tag in (value or "").split(",") if tag.strip()]


@app.post("/telegram-signals/settings")
async def save_settings_profile(request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    profile_id = int(form.get("profile_id") or 0) or None
    save_search_profile(
        {
            "name": _profile_form_value(form, "name", "Новый профиль"),
            "segment": _profile_form_value(form, "segment", "ecom_marketplace_pain"),
            "queries_text": str(form.get("queries_text") or "").strip(),
            "stop_words_text": str(form.get("stop_words_text") or "").strip(),
            "good_chat_hints_text": str(form.get("good_chat_hints_text") or "").strip(),
            "bad_chat_hints_text": str(form.get("bad_chat_hints_text") or "").strip(),
            "max_age_hours": max(1, int(form.get("max_age_hours") or 96)),
            "limit_chats": max(1, int(form.get("limit_chats") or 12)),
            "limit_messages_per_chat": max(1, int(form.get("limit_messages_per_chat") or 80)),
            "min_score": max(0, min(100, int(form.get("min_score") or 0))),
            "is_active": str(form.get("is_active") or "") == "on",
        },
        profile_id=profile_id,
    )
    return RedirectResponse("/telegram-signals/settings", status_code=303)


@app.get("/telegram-signals/settings", response_class=HTMLResponse)
def search_settings_dashboard():
    profiles = list_search_profiles()
    segment_options = "".join(
        f"<option value='{escape(value)}'>{escape(label)}</option>"
        for value, label in SEGMENT_LABELS.items()
    )
    profile_cards = []
    for profile in profiles:
        profile_cards.append(
            f"""
            <article class="settings-card">
              <form method="post" action="/telegram-signals/settings">
                <input type="hidden" name="profile_id" value="{profile.id}">
                <div class="settings-grid">
                  <label>Название <input name="name" value="{escape(profile.name or '')}"></label>
                  <label>Сегмент <select name="segment">
                    {"".join(f"<option value='{escape(value)}' {_selected(profile.segment, value)}>{escape(label)}</option>" for value, label in SEGMENT_LABELS.items())}
                  </select></label>
                  <label>Свежесть, часов <input type="number" name="max_age_hours" value="{profile.max_age_hours}" min="1"></label>
                  <label>Чатов на запрос <input type="number" name="limit_chats" value="{profile.limit_chats}" min="1"></label>
                  <label>Сообщений на чат <input type="number" name="limit_messages_per_chat" value="{profile.limit_messages_per_chat}" min="1"></label>
                  <label>Мин. score <input type="number" name="min_score" value="{profile.min_score}" min="0" max="100"></label>
                  <label class="check"><input type="checkbox" name="is_active" {_checked(bool(profile.is_active))}> Активен</label>
                </div>
                <div class="textarea-grid">
                  <label>Запросы Telegram <textarea name="queries_text">{escape(profile.queries_text or '')}</textarea></label>
                  <label>Стоп-слова сообщений <textarea name="stop_words_text">{escape(profile.stop_words_text or '')}</textarea></label>
                  <label>Хорошие слова в названии чата <textarea name="good_chat_hints_text">{escape(profile.good_chat_hints_text or '')}</textarea></label>
                  <label>Плохие слова в названии чата <textarea name="bad_chat_hints_text">{escape(profile.bad_chat_hints_text or '')}</textarea></label>
                </div>
                <div class="settings-actions">
                  <button class="filter-btn" type="submit">Сохранить профиль</button>
                </div>
              </form>
            </article>
            """
        )

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Настройки поиска</title>
      <style>
        body {{ margin: 0; background: #eef3f7; color: #17202a; font: 14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
        .page {{ max-width: 1240px; margin: 0 auto; padding: 28px 24px 48px; }}
        a {{ color: #2563eb; }}
        h1 {{ margin: 0 0 8px; font-size: 32px; }}
        .subtitle {{ color: #667085; margin-bottom: 18px; }}
        .settings-card {{ background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 16px; margin-bottom: 14px; }}
        .settings-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; align-items: end; }}
        .textarea-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-top: 12px; }}
        label {{ display: grid; gap: 5px; color: #667085; font-size: 12px; }}
        input, select, textarea {{ width: 100%; border: 1px solid #cbd5e1; border-radius: 7px; padding: 8px 10px; font: inherit; color: #17202a; background: #fff; }}
        input, select {{ height: 38px; }}
        textarea {{ min-height: 120px; resize: vertical; }}
        .check {{ display: flex; align-items: center; gap: 8px; height: 38px; color: #17202a; }}
        .check input {{ width: 16px; height: 16px; }}
        .filter-btn {{ border: 0; border-radius: 7px; background: #2563eb; color: #fff; min-height: 38px; padding: 0 14px; cursor: pointer; font: inherit; font-weight: 700; }}
        .settings-actions {{ margin-top: 12px; }}
        .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: center; margin-bottom: 18px; }}
        @media (max-width: 900px) {{ .settings-grid, .textarea-grid {{ grid-template-columns: 1fr; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="topbar">
          <div>
            <h1>Настройки поиска</h1>
            <div class="subtitle">Профили управляют тем, кого искать, по каким запросам, за какой период и с какими стоп-словами.</div>
          </div>
          <a href="/telegram-signals">Вернуться к базе лидов</a>
        </div>
        {"".join(profile_cards)}
        <article class="settings-card">
          <h2>Новый профиль</h2>
          <form method="post" action="/telegram-signals/settings">
            <div class="settings-grid">
              <label>Название <input name="name" value="Новый профиль"></label>
              <label>Сегмент <select name="segment">{segment_options}</select></label>
              <label>Свежесть, часов <input type="number" name="max_age_hours" value="96" min="1"></label>
              <label>Чатов на запрос <input type="number" name="limit_chats" value="12" min="1"></label>
              <label>Сообщений на чат <input type="number" name="limit_messages_per_chat" value="80" min="1"></label>
              <label>Мин. score <input type="number" name="min_score" value="0" min="0" max="100"></label>
              <label class="check"><input type="checkbox" name="is_active" checked> Активен</label>
            </div>
            <div class="textarea-grid">
              <label>Запросы Telegram <textarea name="queries_text"></textarea></label>
              <label>Стоп-слова сообщений <textarea name="stop_words_text"></textarea></label>
              <label>Хорошие слова в названии чата <textarea name="good_chat_hints_text"></textarea></label>
              <label>Плохие слова в названии чата <textarea name="bad_chat_hints_text"></textarea></label>
            </div>
            <div class="settings-actions"><button class="filter-btn" type="submit">Создать профиль</button></div>
          </form>
        </article>
      </main>
    </body>
    </html>
    """


@app.get("/telegram-signals", response_class=HTMLResponse)
def telegram_signals_dashboard(
    min_score: int = 0,
    marketplace: str = "",
    niche: str = "",
    status: str = "",
    crm_tag: str = "",
    review_status: str = "",
    lead_category: str = "",
    hot: bool = False,
    page: int = 1,
    per_page: int = 50,
):
    score = 80 if hot and min_score < 80 else min_score
    per_page = per_page if per_page in {10, 50, 200} else 50
    page = max(1, page)
    filter_kwargs = {
        "lead_fit_in": ["target", "review"],
        "min_score": score or None,
        "marketplace": marketplace or None,
        "niche": niche or None,
        "status": status or None,
        "crm_tag": crm_tag or None,
        "review_status": review_status or None,
        "lead_category": lead_category or None,
    }
    total_items = count_signals(**filter_kwargs)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = min(page, total_pages)
    items = get_signals(
        limit=per_page,
        offset=(page - 1) * per_page,
        **filter_kwargs,
    )
    comments_by_signal = get_signal_comments_map([item.id for item in items], limit_per_signal=5)

    contacted_count = sum(1 for item in items if item.status == "contacted")
    active_count = sum(1 for item in items if item.status in {"new", "reviewed", None})
    hot_count = sum(1 for item in items if (item.lead_score_100 or 0) >= 80)
    avg_score = round(sum((item.lead_score_100 or 0) for item in items) / len(items)) if items else 0

    cards = []
    for item in items:
        score_value = item.lead_score_100 or 0
        score_class = "score-hot" if score_value >= 80 else "score-mid" if score_value >= 60 else "score-low"
        message_link = _message_link(item)
        contact_link = _contact_link(item)
        text = escape(_short(item.text_excerpt or item.message_text, 520))
        opener = escape(_short(item.opener_expert or item.opener_soft or item.recommended_opener, 520))
        category = CATEGORY_LABELS.get(item.lead_category or "", item.lead_category or "Не определено")
        status_label = STATUS_LABELS.get(item.status or "new", item.status or "Новый")
        review_label = REVIEW_LABELS.get(item.review_status or "unchecked", item.review_status or "Не разобран")
        selected_tags = set(_split_tags(item.crm_tag))
        tag_labels = [CRM_TAG_LABELS.get(tag, tag) for tag in selected_tags]
        tag_label = ", ".join(tag_labels) if tag_labels else "Без тега"
        author = item.author_name or item.author_username or "Без имени"

        comment_items = comments_by_signal.get(item.id, [])
        comment_history = ""
        if comment_items:
            comment_history = (
                "<section class='comment-history'>"
                "<div class='section-title'>История комментариев</div>"
                + "".join(
                    "<div class='comment-item'>"
                    f"<time>{escape(format_msk(comment.created_at))}</time>"
                    f"<p>{escape(comment.comment)}</p>"
                    "</div>"
                    for comment in comment_items
                )
                + "</section>"
            )

        actions = [
            _action_form(item.id, "ОК", review_status="ok", tone="ok"),
            _action_form(item.id, "Не ОК", review_status="not_ok", tone="danger"),
            _action_form(item.id, "Прочитал", status="reviewed", review_status="ok"),
            _action_form(item.id, "Написал", status="contacted", review_status="ok", tone="primary"),
            _action_form(item.id, "Ответил", status="replied", review_status="ok"),
            _action_form(item.id, "Теплый", status="warm", review_status="ok"),
            _action_form(item.id, "Встреча", status="meeting_booked", review_status="ok"),
            _action_form(item.id, "Продажа", status="sale", review_status="ok"),
            _action_form(item.id, "Архив", status="dead", review_status="not_ok"),
        ]
        links = []
        if message_link:
            links.append(f"<a class='link-btn' target='_blank' href='{escape(message_link)}'>Сообщение</a>")
        if contact_link:
            links.append(f"<a class='link-btn' target='_blank' href='{escape(contact_link)}'>Профиль</a>")

        cards.append(
            f"""
            <article class="lead-card">
              <div class="lead-top">
                <div>
                  <div class="meta">{escape(format_msk(item.message_date))} · {escape(item.chat_title or "")}</div>
                  <h2>{escape(author)}</h2>
                </div>
                <div class="score {score_class}">{score_value}</div>
              </div>

              <div class="badges">
                <span>{escape(status_label)}</span>
                <span>{escape(tag_label)}</span>
                <span>{escape(review_label)}</span>
                <span>{escape(item.marketplace or "MP не указан")}</span>
                <span>{escape(item.niche or "ниша не указана")}</span>
                <span>{escape(item.likely_icp or "ICP unknown")}</span>
                <span>{escape(category)}</span>
              </div>

              <div class="columns">
                <section>
                  <div class="section-title">Сигнал</div>
                  <p>{text}</p>
                </section>
                <section>
                  <div class="section-title">Черновик захода</div>
                  <p>{opener}</p>
                  <button type="button" class="copy-btn" data-copy="{escape(opener, quote=True)}">Скопировать черновик</button>
                </section>
              </div>

              <div class="card-footer">
                <div class="actions">{"".join(actions)}</div>
                <div class="links">{"".join(links)}</div>
              </div>
              <form method="post" action="/telegram-signals/{item.id}/crm" class="crm-form">
                <label>Статус
                  <select name="status">
                    {"".join(f"<option value='{escape(value)}' {_selected(item.status or 'new', value)}>{escape(label)}</option>" for value, label in STATUS_LABELS.items())}
                  </select>
                </label>
                <label>Теги
                  <select name="crm_tag" multiple class="tag-select">
                    {"".join(f"<option value='{escape(value)}' {'selected' if value in selected_tags else ''}>{escape(label)}</option>" for value, label in CRM_TAG_LABELS.items())}
                  </select>
                </label>
                <label class="comment-field">Комментарий
                  <textarea name="comment" placeholder="Добавить новую заметку по лиду"></textarea>
                </label>
                <button type="submit" class="btn primary">Сохранить CRM</button>
              </form>
              {comment_history}
            </article>
            """
        )

    marketplace_options = "".join(
        f"<option value='{escape(value)}' {_selected(marketplace, value)}>{escape(label)}</option>"
        for value, label in [
            ("", "Все"),
            ("WB", "WB"),
            ("Ozon", "Ozon"),
            ("WB/Ozon", "WB/Ozon"),
            ("marketplaces", "Маркетплейсы"),
        ]
    )
    status_options = "".join(
        f"<option value='{escape(value)}' {_selected(status, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *STATUS_LABELS.items()]
    )
    review_options = "".join(
        f"<option value='{escape(value)}' {_selected(review_status, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *REVIEW_LABELS.items()]
    )
    category_options = "".join(
        f"<option value='{escape(value)}' {_selected(lead_category, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *CATEGORY_LABELS.items()]
    )
    tag_options = "".join(
        f"<option value='{escape(value)}' {_selected(crm_tag, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *CRM_TAG_LABELS.items()]
    )
    per_page_options = "".join(
        f"<option value='{value}' {_selected(str(per_page), str(value))}>{value}</option>"
        for value in [10, 50, 200]
    )
    base_params = {
        "min_score": score,
        "marketplace": marketplace,
        "niche": niche,
        "status": status,
        "crm_tag": crm_tag,
        "review_status": review_status,
        "lead_category": lead_category,
        "hot": "true" if hot else "",
        "per_page": per_page,
    }
    prev_params = {**base_params, "page": max(1, page - 1)}
    next_params = {**base_params, "page": min(total_pages, page + 1)}
    prev_url = "/telegram-signals?" + urlencode(prev_params)
    next_url = "/telegram-signals?" + urlencode(next_params)
    job = dict(DASHBOARD_JOB)
    if job["running"]:
        job_text = "Идет сбор сигналов. Можно обновить страницу через минуту."
        job_class = "job-running"
    elif job["last_error"]:
        job_text = f"Последний сбор упал: {job['last_error']}"
        job_class = "job-error"
    elif job["last_result"]:
        result = job["last_result"]
        job_text = (
            f"Последний сбор: +{result.get('created', 0)} новых, "
            f"{result.get('updated', 0)} обновлено, "
            f"{result.get('scanned_messages', 0)} сообщений. "
            f"Завершен: {job.get('last_finished_at') or '-'}"
        )
        job_class = "job-ok"
    else:
        job_text = "Сбор еще не запускался из dashboard."
        job_class = "job-idle"

    quick_links = [
        ("На проверку", "/telegram-signals?review_status=unchecked", _count_signals(review_status="unchecked")),
        ("ОК написать", "/telegram-signals?review_status=ok&status=new", _count_signals(review_status="ok", status="new")),
        ("Прочитал", "/telegram-signals?status=reviewed", _count_signals(status="reviewed")),
        ("Написал", "/telegram-signals?status=contacted", _count_signals(status="contacted")),
        ("Ответили", "/telegram-signals?status=replied", _count_signals(status="replied")),
        ("Теплые", "/telegram-signals?status=warm", _count_signals(status="warm")),
        ("Встречи", "/telegram-signals?status=meeting_booked", _count_signals(status="meeting_booked")),
        ("Архив", "/telegram-signals?status=dead", _count_signals(status="dead")),
        ("Горячие", "/telegram-signals?hot=true", _count_signals(min_score=80)),
    ]
    quick_nav = "".join(
        f"<a class='quick-link' href='{escape(url)}'><span>{escape(label)}</span><b>{count}</b></a>"
        for label, url, count in quick_links
    )
    sidebar_groups = [
        (
            "Качество лида",
            [
                ("На проверку", "/telegram-signals?review_status=unchecked", _count_signals(review_status="unchecked")),
                ("ОК", "/telegram-signals?review_status=ok", _count_signals(review_status="ok")),
                ("Не ОК", "/telegram-signals?review_status=not_ok", _count_signals(review_status="not_ok")),
            ],
        ),
        (
            "Контакт",
            [
                ("Прочитал", "/telegram-signals?status=reviewed", _count_signals(status="reviewed")),
                ("Написал", "/telegram-signals?status=contacted", _count_signals(status="contacted")),
                ("Ответили", "/telegram-signals?status=replied", _count_signals(status="replied")),
            ],
        ),
        (
            "Сделка и температура",
            [
                ("Горячие", "/telegram-signals?crm_tag=hot", _count_signals(crm_tag="hot")),
                ("Теплые", "/telegram-signals?crm_tag=warm", _count_signals(crm_tag="warm")),
                ("Холодные", "/telegram-signals?crm_tag=cold", _count_signals(crm_tag="cold")),
                ("Встречи", "/telegram-signals?status=meeting_booked", _count_signals(status="meeting_booked")),
                ("Продажи", "/telegram-signals?status=sale", _count_signals(status="sale")),
                ("Архив", "/telegram-signals?status=dead", _count_signals(status="dead")),
            ],
        ),
    ]
    sidebar_nav = "".join(
        f"<div class='side-title'>{escape(title)}</div>"
        + "".join(f"<a class='side-link' href='{escape(url)}'><span>{escape(label)}</span><b>{count}</b></a>" for label, url, count in links)
        for title, links in sidebar_groups
    )
    export_links = "".join(
        f"<a class='export-link' href='/telegram-signals/export?kind={escape(kind)}'>{escape(label)}</a>"
        for kind, label in [("all", "Excel: вся база"), ("ok", "Excel: только ОК"), ("not_ok", "Excel: только не ОК")]
    )
    profiles = list_search_profiles(active_only=True)
    profile_options = "<option value=''>Все активные профили</option>" + "".join(
        f"<option value='{profile.id}'>{escape(profile.name)}</option>" for profile in profiles
    )

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Telegram Signals</title>
      <style>
        :root {{
          --bg: #eef3f7;
          --panel: #ffffff;
          --text: #17202a;
          --muted: #667085;
          --line: #d9e2ec;
          --blue: #2563eb;
          --cyan: #0891b2;
          --green: #0f9f6e;
          --red: #d92d20;
          --amber: #b7791f;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          background: var(--bg);
          color: var(--text);
          font: 14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
        }}
        .shell {{ display: grid; grid-template-columns: 248px minmax(0, 1fr); min-height: 100vh; }}
        .sidebar {{
          position: sticky;
          top: 0;
          height: 100vh;
          padding: 22px 16px;
          background: #10202f;
          color: #d9e7f2;
        }}
        .brand {{ font-size: 20px; font-weight: 800; margin-bottom: 4px; }}
        .brand-subtitle {{ color: #8fb0c7; font-size: 12px; margin-bottom: 22px; }}
        .side-title {{ color: #8fb0c7; font-size: 11px; font-weight: 800; text-transform: uppercase; margin: 20px 10px 8px; }}
        .side-link {{
          display: flex;
          justify-content: space-between;
          align-items: center;
          min-height: 38px;
          padding: 0 10px;
          color: #d9e7f2;
          text-decoration: none;
          border-radius: 7px;
        }}
        .side-link:hover {{ background: rgba(255,255,255,.08); }}
        .side-link b {{
          min-width: 28px;
          padding: 2px 7px;
          border-radius: 999px;
          background: rgba(255,255,255,.12);
          text-align: center;
          font-size: 12px;
        }}
        .side-note {{
          margin-top: 24px;
          padding: 12px;
          border: 1px solid rgba(255,255,255,.12);
          border-radius: 8px;
          color: #bdd1df;
          font-size: 12px;
        }}
        .side-settings {{
          display: block;
          margin-top: 14px;
          padding: 10px;
          border-radius: 7px;
          background: rgba(255,255,255,.08);
          color: #fff;
          text-decoration: none;
        }}
        .content {{ min-width: 0; }}
        .page {{ max-width: 1480px; margin: 0 auto; padding: 28px 24px 48px; }}
        .hero {{
          display: flex;
          justify-content: space-between;
          gap: 24px;
          align-items: flex-end;
          margin-bottom: 20px;
        }}
        h1 {{ margin: 0; font-size: 34px; letter-spacing: 0; }}
        .subtitle {{ margin-top: 8px; color: var(--muted); max-width: 760px; }}
        .stats {{ display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 10px; min-width: 520px; }}
        .stat {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }}
        .stat b {{ display: block; font-size: 22px; }}
        .stat span {{ color: var(--muted); font-size: 12px; }}
        .filters {{
          position: sticky;
          top: 0;
          z-index: 5;
          display: grid;
          grid-template-columns: 100px 140px 140px 150px 150px 170px 1fr 130px auto auto;
          gap: 10px;
          align-items: end;
          background: rgba(246, 248, 251, .94);
          backdrop-filter: blur(10px);
          border-bottom: 1px solid var(--line);
          padding: 12px 0 16px;
          margin-bottom: 18px;
        }}
        label {{ display: grid; gap: 5px; color: var(--muted); font-size: 12px; }}
        input, select {{
          width: 100%;
          height: 38px;
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          background: #fff;
          color: var(--text);
          padding: 0 10px;
          font: inherit;
        }}
        .check {{ display: flex; align-items: center; gap: 8px; height: 38px; color: var(--text); }}
        .check input {{ width: 16px; height: 16px; }}
        .btn, .filter-btn, .link-btn {{
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          background: #fff;
          color: var(--text);
          min-height: 34px;
          padding: 7px 10px;
          cursor: pointer;
          text-decoration: none;
          font: inherit;
          white-space: nowrap;
        }}
        .filter-btn {{ height: 38px; background: var(--text); color: #fff; border-color: var(--text); }}
        .btn.primary {{ background: var(--blue); color: #fff; border-color: var(--blue); }}
        .btn.ok {{ background: #ecfdf3; color: #067647; border-color: #abefc6; }}
        .btn.danger {{ background: #fff1f3; color: var(--red); border-color: #fecdd3; }}
        .btn.ghost {{ background: #fff; color: #344054; }}
        .toolbar {{
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 14px;
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 8px;
          padding: 12px;
          margin-bottom: 14px;
        }}
        .collect-form {{ margin: 0; }}
        .collect-form select {{
          width: 240px;
          margin-right: 8px;
        }}
        .collect-btn {{
          height: 40px;
          border: 0;
          border-radius: 7px;
          background: var(--blue);
          color: #fff;
          padding: 0 14px;
          font: inherit;
          font-weight: 700;
          cursor: pointer;
        }}
        .collect-btn:disabled {{ background: #98a2b3; cursor: wait; }}
        .job-status {{ flex: 1; color: var(--muted); }}
        .job-error {{ color: var(--red); }}
        .job-running {{ color: var(--blue); }}
        .job-ok {{ color: var(--green); }}
        .quick-nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }}
        .quick-link {{
          display: inline-flex;
          gap: 8px;
          align-items: center;
          justify-content: space-between;
          min-height: 34px;
          border: 1px solid var(--line);
          border-radius: 7px;
          background: #fff;
          color: #344054;
          padding: 0 12px;
          text-decoration: none;
        }}
        .quick-link b {{
          min-width: 24px;
          padding: 1px 7px;
          border-radius: 999px;
          background: #eef2f7;
          text-align: center;
          color: #475467;
        }}
        .exports {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 0 0 14px; }}
        .export-link {{
          display: inline-flex;
          align-items: center;
          min-height: 34px;
          border: 1px solid #bbf7d0;
          border-radius: 7px;
          background: #f0fdf4;
          color: #166534;
          padding: 0 12px;
          text-decoration: none;
        }}
        .lead-list {{ display: grid; gap: 14px; }}
        .pagination {{
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 12px;
          margin-bottom: 14px;
          color: var(--muted);
        }}
        .pager-links {{ display: flex; gap: 8px; align-items: center; }}
        .pager-links a {{
          min-height: 34px;
          display: inline-flex;
          align-items: center;
          border: 1px solid var(--line);
          border-radius: 7px;
          background: #fff;
          color: #344054;
          padding: 0 12px;
          text-decoration: none;
        }}
        .lead-card {{
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 8px;
          padding: 16px;
          box-shadow: 0 1px 2px rgba(16, 24, 40, .04);
        }}
        .lead-top {{ display: flex; justify-content: space-between; gap: 16px; }}
        .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 4px; }}
        h2 {{ margin: 0; font-size: 20px; }}
        .score {{
          width: 54px;
          height: 42px;
          display: grid;
          place-items: center;
          border-radius: 8px;
          font-weight: 800;
          font-size: 20px;
        }}
        .score-hot {{ background: #fef3c7; color: #92400e; }}
        .score-mid {{ background: #dbeafe; color: #1d4ed8; }}
        .score-low {{ background: #eef2f7; color: #475467; }}
        .badges {{ display: flex; flex-wrap: wrap; gap: 6px; margin: 12px 0; }}
        .badges span {{
          border: 1px solid #d0d5dd;
          background: #f8fafc;
          border-radius: 999px;
          padding: 4px 8px;
          font-size: 12px;
          color: #344054;
        }}
        .columns {{ display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, .9fr); gap: 16px; }}
        .section-title {{ color: var(--muted); font-size: 12px; font-weight: 700; text-transform: uppercase; margin-bottom: 6px; }}
        p {{ margin: 0; white-space: pre-wrap; }}
        .copy-btn {{
          margin-top: 12px;
          min-height: 34px;
          border: 1px solid #bae6fd;
          border-radius: 7px;
          background: #ecfeff;
          color: #155e75;
          padding: 7px 10px;
          cursor: pointer;
          font: inherit;
        }}
        .copy-btn.done {{ background: #ecfdf3; border-color: #abefc6; color: #067647; }}
        .card-footer {{
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
          border-top: 1px solid var(--line);
          margin-top: 14px;
          padding-top: 12px;
        }}
        .actions, .links {{ display: flex; flex-wrap: wrap; gap: 8px; }}
        .inline-form {{ display: inline; margin: 0; }}
        .crm-form {{
          display: grid;
          grid-template-columns: 160px 180px minmax(260px, 1fr) auto;
          gap: 10px;
          align-items: end;
          border-top: 1px solid var(--line);
          margin-top: 12px;
          padding-top: 12px;
        }}
        textarea {{
          width: 100%;
          min-height: 38px;
          resize: vertical;
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          padding: 8px 10px;
          font: inherit;
        }}
        .tag-select {{
          height: 38px;
          overflow: hidden;
        }}
        .tag-select:focus {{
          height: 132px;
          overflow: auto;
        }}
        .comment-history {{
          border-top: 1px solid var(--line);
          margin-top: 12px;
          padding-top: 12px;
        }}
        .comment-item {{
          border-left: 3px solid #cbd5e1;
          margin-top: 8px;
          padding: 2px 0 2px 10px;
        }}
        .comment-item time {{
          display: block;
          color: var(--muted);
          font-size: 12px;
          margin-bottom: 3px;
        }}
        .empty {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 28px; color: var(--muted); }}
        @media (max-width: 1100px) {{
          .shell {{ display: block; }}
          .sidebar {{ position: static; height: auto; }}
          .hero {{ display: block; }}
          .stats {{ min-width: 0; margin-top: 16px; }}
          .toolbar {{ align-items: flex-start; flex-direction: column; }}
          .filters {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
          .columns {{ grid-template-columns: 1fr; }}
          .crm-form {{ grid-template-columns: 1fr; }}
          .card-footer {{ align-items: flex-start; flex-direction: column; }}
        }}
      </style>
    </head>
    <body>
      <div class="shell">
      <aside class="sidebar">
        <div class="brand">Telegram Signals</div>
        <div class="brand-subtitle">поиск B2B-сигналов в Telegram</div>
        {sidebar_nav}
        <a class="side-settings" href="/telegram-signals/settings">Настройки поиска</a>
        <div class="side-note">Логика работы: собрать сигналы, проверить людей, написать по черновику, вести статус до ответа или встречи.</div>
      </aside>
      <main class="page content">
        <header class="hero">
          <div>
            <h1>База лидов</h1>
            <div class="subtitle">Рабочая база лидов: фильтруем сигналы, помечаем контакт, ведем статус до ответа и встречи.</div>
          </div>
          <div class="stats">
            <div class="stat"><b>{total_items}</b><span>найдено по фильтру</span></div>
            <div class="stat"><b>{hot_count}</b><span>горячих 80+</span></div>
            <div class="stat"><b>{active_count}</b><span>не связались</span></div>
            <div class="stat"><b>{avg_score}</b><span>средний score</span></div>
          </div>
        </header>

        <section class="toolbar">
          <form method="post" action="/telegram-signals/collect" class="collect-form">
            <select name="profile_id">{profile_options}</select>
            <button type="submit" class="collect-btn" {"disabled" if job["running"] else ""}>Запустить сбор лидов</button>
          </form>
          <div class="job-status {job_class}">{escape(job_text)}</div>
        </section>

        <nav class="quick-nav">{quick_nav}</nav>
        <div class="exports">{export_links}</div>

        <form class="filters">
          <label>Score от <input name="min_score" type="number" value="{score}" min="0" max="100"></label>
          <label>Площадка <select name="marketplace">{marketplace_options}</select></label>
          <label>Статус <select name="status">{status_options}</select></label>
          <label>Тег <select name="crm_tag">{tag_options}</select></label>
          <label>Разбор <select name="review_status">{review_options}</select></label>
          <label>Боль <select name="lead_category">{category_options}</select></label>
          <label>Ниша <input name="niche" value="{escape(niche)}" placeholder="одежда, электроника..."></label>
          <label>На странице <select name="per_page">{per_page_options}</select></label>
          <label class="check"><input name="hot" type="checkbox" value="true" {_checked(hot)}> Hot 80+</label>
          <button class="filter-btn">Фильтр</button>
        </form>

        <div class="pagination">
          <div>Страница {page} из {total_pages}. Показано {len(items)} из {total_items}.</div>
          <div class="pager-links">
            <a href="{escape(prev_url)}">Назад</a>
            <a href="{escape(next_url)}">Вперед</a>
          </div>
        </div>

        <section class="lead-list">
          {"".join(cards) if cards else '<div class="empty">Под эти фильтры лидов нет.</div>'}
        </section>
      </main>
      </div>
      <script>
        document.querySelectorAll('.copy-btn').forEach((button) => {{
          button.addEventListener('click', async () => {{
            const text = button.dataset.copy || '';
            try {{
              await navigator.clipboard.writeText(text);
              button.textContent = 'Скопировано';
              button.classList.add('done');
            }} catch (e) {{
              button.textContent = 'Не скопировалось';
            }}
          }});
        }});
      </script>
    </body>
    </html>
    """
