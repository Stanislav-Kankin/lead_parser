from html import escape

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from storage.db import init_db
from telegram_signals.repository import get_signals

app = FastAPI(title="Lead Parser MVP v1.2.1")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok", "service": "lead_parser_google_mvp_v1_2_1_fix"}


@app.get("/telegram-signals", response_class=HTMLResponse)
def telegram_signals_dashboard(
    min_score: int = Query(0, ge=0, le=100),
    marketplace: str = "",
    niche: str = "",
    status: str = "",
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
    )
    rows = []
    for item in items:
        message_link = ""
        if item.chat_username and item.message_id:
            message_link = f"https://t.me/{str(item.chat_username).lstrip('@')}/{item.message_id}"
        elif item.chat_url and item.message_id:
            message_link = f"{item.chat_url.rstrip('/')}/{item.message_id}"
        text = escape((item.text_excerpt or item.message_text or "")[:240])
        opener = escape((item.opener_soft or item.recommended_opener or "")[:260])
        rows.append(
            "<tr>"
            f"<td>{escape(str(item.message_date)[:16] if item.message_date else '')}</td>"
            f"<td><b>{item.lead_score_100 or 0}</b></td>"
            f"<td>{escape(item.review_status or '')}<br>{escape(item.status or '')}</td>"
            f"<td>{escape(item.marketplace or '')}</td>"
            f"<td>{escape(item.niche or '')}</td>"
            f"<td>{escape(item.likely_icp or '')}</td>"
            f"<td>{escape(item.lead_category or '')}</td>"
            f"<td>{escape(item.chat_title or '')}</td>"
            f"<td>{text}</td>"
            f"<td>{opener}</td>"
            f"<td><a href='{escape(message_link)}' target='_blank'>open</a></td>"
            "</tr>"
        )

    return """
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Telegram Signals</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 24px; color: #1f2933; }
        form { display: flex; gap: 8px; align-items: end; margin-bottom: 16px; flex-wrap: wrap; }
        label { display: grid; gap: 4px; font-size: 12px; color: #52606d; }
        input, select, button { padding: 8px; border: 1px solid #bcccdc; border-radius: 6px; }
        table { border-collapse: collapse; width: 100%; font-size: 13px; }
        th, td { border-bottom: 1px solid #d9e2ec; padding: 8px; vertical-align: top; }
        th { position: sticky; top: 0; background: #f0f4f8; text-align: left; }
        td:nth-child(9), td:nth-child(10) { max-width: 340px; }
      </style>
    </head>
    <body>
      <h1>Telegram Signals</h1>
      <form>
        <label>Score от <input name="min_score" type="number" value="%s" min="0" max="100"></label>
        <label>Marketplace <select name="marketplace">
          <option value="">Все</option><option>WB</option><option>Ozon</option><option>WB/Ozon</option><option>marketplaces</option>
        </select></label>
        <label>Ниша <input name="niche" value="%s"></label>
        <label>Статус <select name="status">
          <option value="">Все</option><option>new</option><option>contacted</option>
        </select></label>
        <label>Hot <input name="hot" type="checkbox" value="true" %s></label>
        <button>Фильтр</button>
      </form>
      <p>Показано: %s</p>
      <table>
        <thead><tr><th>Дата</th><th>Score</th><th>Статус</th><th>MP</th><th>Ниша</th><th>ICP</th><th>Боль</th><th>Чат</th><th>Текст</th><th>Opener</th><th>Link</th></tr></thead>
        <tbody>%s</tbody>
      </table>
    </body>
    </html>
    """ % (score, escape(niche), "checked" if hot else "", len(items), "".join(rows))
