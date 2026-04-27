from __future__ import annotations

from html import escape
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from storage.db import init_db
from telegram_signals.repository import get_signals, set_signal_review_status, set_signal_status

app = FastAPI(title="Telegram Signals")

STATUS_LABELS = {
    "new": "Новый",
    "contacted": "Связались",
    "replied": "Ответил",
    "warm": "Теплый",
    "meeting_booked": "Встреча",
    "dead": "Архив",
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
        f"<button class='btn {tone}'>{escape(label)}</button>"
        "</form>"
    )


@app.post("/telegram-signals/{signal_id}/status")
def update_signal_status(signal_id: int, request: Request, status: str | None = None, review_status: str | None = None):
    if status:
        set_signal_status(signal_id, status, review_status=review_status)
    elif review_status:
        set_signal_review_status(signal_id, review_status)
    return RedirectResponse(_return_url(request), status_code=303)


@app.get("/telegram-signals", response_class=HTMLResponse)
def telegram_signals_dashboard(
    min_score: int = 0,
    marketplace: str = "",
    niche: str = "",
    status: str = "",
    review_status: str = "",
    lead_category: str = "",
    hot: bool = False,
):
    score = 80 if hot and min_score < 80 else min_score
    items = get_signals(
        limit=300,
        lead_fit_in=["target", "review"],
        min_score=score or None,
        marketplace=marketplace or None,
        niche=niche or None,
        status=status or None,
        review_status=review_status or None,
        lead_category=lead_category or None,
    )

    contacted_count = sum(1 for item in items if item.status == "contacted")
    active_count = sum(1 for item in items if item.status in {"new", None})
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
        author = item.author_name or item.author_username or "Без имени"

        actions = [
            _action_form(item.id, "ОК", review_status="ok", tone="ok"),
            _action_form(item.id, "Не ОК", review_status="not_ok", tone="danger"),
            _action_form(item.id, "Связался", status="contacted", review_status="ok", tone="primary"),
            _action_form(item.id, "Ответил", status="replied", review_status="ok"),
            _action_form(item.id, "Теплый", status="warm", review_status="ok"),
            _action_form(item.id, "Встреча", status="meeting_booked", review_status="ok"),
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
                  <div class="meta">{escape(str(item.message_date)[:16] if item.message_date else "")} · {escape(item.chat_title or "")}</div>
                  <h2>{escape(author)}</h2>
                </div>
                <div class="score {score_class}">{score_value}</div>
              </div>

              <div class="badges">
                <span>{escape(status_label)}</span>
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
                </section>
              </div>

              <div class="card-footer">
                <div class="actions">{"".join(actions)}</div>
                <div class="links">{"".join(links)}</div>
              </div>
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

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Telegram Signals</title>
      <style>
        :root {{
          --bg: #f6f8fb;
          --panel: #ffffff;
          --text: #17202a;
          --muted: #667085;
          --line: #d9e2ec;
          --blue: #2563eb;
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
          grid-template-columns: 100px 160px 160px 180px 180px 1fr auto auto;
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
        .lead-list {{ display: grid; gap: 14px; }}
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
        .empty {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 28px; color: var(--muted); }}
        @media (max-width: 1100px) {{
          .hero {{ display: block; }}
          .stats {{ min-width: 0; margin-top: 16px; }}
          .filters {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
          .columns {{ grid-template-columns: 1fr; }}
          .card-footer {{ align-items: flex-start; flex-direction: column; }}
        }}
      </style>
    </head>
    <body>
      <main class="page">
        <header class="hero">
          <div>
            <h1>Telegram Signals</h1>
            <div class="subtitle">Рабочая база лидов: фильтруем сигналы, помечаем контакт, ведем статус до ответа и встречи.</div>
          </div>
          <div class="stats">
            <div class="stat"><b>{len(items)}</b><span>показано</span></div>
            <div class="stat"><b>{hot_count}</b><span>горячих 80+</span></div>
            <div class="stat"><b>{active_count}</b><span>не связались</span></div>
            <div class="stat"><b>{avg_score}</b><span>средний score</span></div>
          </div>
        </header>

        <form class="filters">
          <label>Score от <input name="min_score" type="number" value="{score}" min="0" max="100"></label>
          <label>Marketplace <select name="marketplace">{marketplace_options}</select></label>
          <label>Статус <select name="status">{status_options}</select></label>
          <label>Разбор <select name="review_status">{review_options}</select></label>
          <label>Боль <select name="lead_category">{category_options}</select></label>
          <label>Ниша <input name="niche" value="{escape(niche)}" placeholder="одежда, электроника..."></label>
          <label class="check"><input name="hot" type="checkbox" value="true" {_checked(hot)}> Hot 80+</label>
          <button class="filter-btn">Фильтр</button>
        </form>

        <section class="lead-list">
          {"".join(cards) if cards else '<div class="empty">Под эти фильтры лидов нет.</div>'}
        </section>
      </main>
    </body>
    </html>
    """
