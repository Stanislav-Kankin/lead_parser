from __future__ import annotations

from collections import defaultdict
from html import escape
from urllib.parse import urlencode

from storage.lead_repository import get_project, list_projects
from storage.social_lead_repository import get_social_leads


def render_social_focus_dashboard(
    *,
    project_id: int,
    q: str,
    company_status: str,
    page: int,
    import_result: dict,
) -> str:
    project_id = int(project_id or 0)
    page = max(1, int(page or 1))
    per_page = 25
    projects = list_projects()
    project = get_project(project_id) if project_id else None
    all_people = get_social_leads(limit=10000, project_id=project_id or None)
    enriched_people = [item for item in all_people if item.focus_loaded_at is not None]

    grouped: dict[str, list] = defaultdict(list)
    for item in enriched_people:
        grouped[str(item.company_inn or f"lead-{item.id}")].append(item)
    companies = list(grouped.values())
    companies.sort(key=_company_sort_key)

    total_companies = len(companies)
    active_companies = sum(1 for group in companies if _is_active(group[0].focus_status))
    with_contacts = sum(1 for group in companies if group[0].focus_phone or group[0].focus_email)
    with_revenue = sum(1 for group in companies if group[0].focus_revenue)

    query = (q or "").strip().lower()
    if company_status == "active":
        companies = [group for group in companies if _is_active(group[0].focus_status)]
    if query:
        companies = [group for group in companies if query in _company_search_text(group)]

    total_filtered = len(companies)
    start = (page - 1) * per_page
    page_companies = companies[start : start + per_page]
    project_options = _project_options(projects, project_id)
    import_banner = _import_banner(import_result)
    cards = "".join(_company_card(group) for group in page_companies)
    if not cards:
        cards = _empty_state(project_id, bool(enriched_people))

    base_params = {
        "project_id": project_id,
        "q": q or "",
        "company_status": company_status or "all",
    }
    prev_url = "/people-leads/finance?" + urlencode({**base_params, "page": max(1, page - 1)})
    next_url = "/people-leads/finance?" + urlencode({**base_params, "page": page + 1})
    project_name = project.name if project else "Общий пул"
    upload_disabled = "disabled" if not project_id else ""

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>TenChat + Компас</title>
      <style>
        :root {{ --bg:#eef3f7; --panel:#fff; --text:#0f172a; --muted:#64748b; --line:#d8e1ea; --blue:#2563eb; --green:#07875f; --green-bg:#ecfdf3; --amber:#9a5b00; --amber-bg:#fff7d6; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Arial,sans-serif; }}
        a {{ color:inherit; }}
        .page {{ max-width:1380px; margin:0 auto; padding:22px; }}
        .topbar {{ display:flex; justify-content:space-between; gap:20px; align-items:flex-start; margin-bottom:16px; }}
        h1 {{ margin:0 0 4px; font-size:30px; letter-spacing:0; }}
        .subtitle,.muted {{ color:var(--muted); }}
        .nav,.actions {{ display:flex; flex-wrap:wrap; gap:8px; }}
        .nav a,.button {{ display:inline-flex; min-height:38px; align-items:center; justify-content:center; padding:8px 13px; border:1px solid var(--line); border-radius:7px; background:#fff; text-decoration:none; cursor:pointer; }}
        .nav a.active,.button.primary {{ border-color:var(--blue); background:var(--blue); color:#fff; font-weight:700; }}
        .layout {{ display:grid; grid-template-columns:300px minmax(0,1fr); gap:14px; align-items:start; }}
        .sidebar {{ position:sticky; top:14px; }}
        .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }}
        .panel + .panel {{ margin-top:12px; }}
        .panel h2 {{ margin:0 0 12px; font-size:16px; }}
        label {{ display:grid; gap:5px; color:var(--muted); font-size:13px; }}
        input,select {{ width:100%; min-height:38px; border:1px solid #cbd5e1; border-radius:6px; background:#fff; padding:8px 10px; color:var(--text); }}
        form.stack {{ display:grid; gap:11px; }}
        .hint {{ margin-top:10px; color:var(--muted); font-size:12px; }}
        .metrics {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; margin-bottom:12px; }}
        .metric {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:15px; }}
        .metric b {{ display:block; font-size:25px; margin-bottom:3px; }}
        .metric span {{ color:var(--muted); }}
        .toolbar {{ display:flex; justify-content:space-between; gap:12px; align-items:end; margin-bottom:12px; }}
        .filters {{ display:grid; grid-template-columns:180px minmax(260px,1fr) 110px; gap:9px; flex:1; align-items:end; }}
        .banner {{ margin-bottom:12px; padding:11px 13px; border:1px solid #94e5b7; border-radius:7px; background:var(--green-bg); color:#08643f; }}
        .banner.error {{ border-color:#f0c967; background:var(--amber-bg); color:#744500; }}
        .pager {{ display:flex; justify-content:space-between; align-items:center; margin:10px 0; color:var(--muted); }}
        .company {{ display:grid; grid-template-columns:minmax(0,1fr) 260px; gap:16px; margin-bottom:12px; background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; }}
        .company-head {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; padding-bottom:12px; border-bottom:1px solid var(--line); }}
        .company h2 {{ margin:0; font-size:20px; }}
        .identity {{ margin-top:4px; color:var(--muted); }}
        .status {{ display:inline-flex; max-width:320px; padding:5px 8px; border-radius:6px; background:var(--amber-bg); color:var(--amber); font-size:12px; font-weight:700; text-align:center; overflow-wrap:anywhere; }}
        .status.active {{ background:var(--green-bg); color:var(--green); }}
        .finance {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); margin:13px 0; border-top:1px solid var(--line); border-bottom:1px solid var(--line); }}
        .finance div {{ padding:11px 12px 11px 0; }}
        .finance div + div {{ border-left:1px solid var(--line); padding-left:12px; }}
        .finance span,.section-title {{ display:block; margin-bottom:3px; color:var(--muted); font-size:12px; text-transform:uppercase; font-weight:700; }}
        .finance b {{ font-size:18px; }}
        .details-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; }}
        .details-grid p {{ margin:0; white-space:pre-line; }}
        details {{ margin-top:10px; }}
        summary {{ cursor:pointer; color:var(--blue); font-weight:700; }}
        .contacts {{ border-left:1px solid var(--line); padding-left:16px; }}
        .contact-block {{ margin-bottom:13px; overflow-wrap:anywhere; }}
        .contact-block a {{ color:var(--blue); }}
        .people {{ display:grid; gap:8px; }}
        .person {{ padding-top:9px; border-top:1px solid var(--line); }}
        .person:first-child {{ padding-top:0; border-top:0; }}
        .person b {{ display:block; }}
        .person .button {{ width:100%; margin-top:7px; min-height:34px; }}
        .empty {{ padding:32px; background:#fff; border:1px solid var(--line); border-radius:8px; color:var(--muted); }}
        @media(max-width:1000px) {{ .layout,.company {{ grid-template-columns:1fr; }} .sidebar {{ position:static; }} .metrics {{ grid-template-columns:1fr 1fr; }} .toolbar {{ display:block; }} .filters {{ margin-top:10px; }} .contacts {{ border-left:0; border-top:1px solid var(--line); padding:14px 0 0; }} }}
        @media(max-width:640px) {{ .page {{ padding:12px; }} .topbar,.company-head {{ display:block; }} .nav {{ margin-top:12px; }} .status {{ max-width:100%; margin-top:8px; }} .metrics,.filters,.finance,.details-grid {{ grid-template-columns:1fr; }} .finance div + div {{ border-left:0; border-top:1px solid var(--line); padding-left:0; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <header class="topbar">
          <div>
            <h1>TenChat + Компас</h1>
            <div class="subtitle">Финансовая проверка найденных ЛПР и быстрый переход к диалогу.</div>
          </div>
          <nav class="nav">
            <a href="/web-leads">Web</a>
            <a class="active" href="/people-leads">TenChat</a>
            <a href="/telegram-signals">Telegram</a>
          </nav>
        </header>
        <div class="layout">
          <aside class="sidebar">
            <section class="panel">
              <h2>{escape(project_name)}</h2>
              <form class="stack" method="get" action="/people-leads/finance">
                <label>Проект
                  <select name="project_id">{project_options}</select>
                </label>
                <button class="button primary" type="submit">Открыть проект</button>
              </form>
            </section>
            <section class="panel">
              <h2>Загрузить Контур Компас</h2>
              <form class="stack" method="post" action="/people-leads/import-focus?project_id={project_id}" enctype="multipart/form-data">
                <label>Excel из Компаса
                  <input type="file" name="file" accept=".xlsx,.xlsm,.csv" required {upload_disabled}>
                </label>
                <button class="button primary" type="submit" {upload_disabled}>Соединить по ИНН</button>
              </form>
              <div class="hint">Загрузку нужно делать внутри проекта. Совпадения определяются только по ИНН.</div>
            </section>
            <section class="panel">
              <h2>Действия</h2>
              <div class="actions">
                <a class="button" href="/people-leads?project_id={project_id}">Люди TenChat</a>
                {f'<a class="button primary" href="/people-leads/export-focus?project_id={project_id}">Excel: люди + Компас</a>' if project_id else ''}
              </div>
            </section>
          </aside>
          <section>
            {import_banner}
            <section class="metrics">
              {_metric(total_companies, "компаний с данными")}
              {_metric(active_companies, "действующих")}
              {_metric(with_contacts, "с телефоном или почтой")}
              {_metric(with_revenue, "с раскрытой выручкой")}
            </section>
            <section class="panel toolbar">
              <b>Финансовый срез проекта</b>
              <form class="filters" method="get" action="/people-leads/finance">
                <input type="hidden" name="project_id" value="{project_id}">
                <label>Статус
                  <select name="company_status">
                    <option value="all" {'selected' if company_status != 'active' else ''}>Все</option>
                    <option value="active" {'selected' if company_status == 'active' else ''}>Только действующие</option>
                  </select>
                </label>
                <label>Поиск
                  <input name="q" value="{escape(q or '')}" placeholder="человек, организация, ИНН, ОКВЭД">
                </label>
                <button class="button primary" type="submit">Фильтр</button>
              </form>
            </section>
            <div class="pager">
              <span>Найдено компаний: {total_filtered}. Страница {page}.</span>
              <div class="actions">
                <a class="button" href="{escape(prev_url)}">Назад</a>
                <a class="button" href="{escape(next_url)}">Вперёд</a>
              </div>
            </div>
            {cards}
          </section>
        </div>
      </main>
    </body>
    </html>
    """


def _company_card(group: list) -> str:
    item = group[0]
    company = item.focus_legal_name or item.company_legal_name or item.company_name or "Организация"
    status = item.focus_status or "Статус не указан"
    status_class = "status active" if _is_active(status) else "status"
    website = _external_url(item.focus_website)
    website_html = f'<a href="{escape(website)}" target="_blank" rel="noreferrer">{escape(item.focus_website)}</a>' if website else "—"
    email_html = _multiline(item.focus_email)
    phone_html = _multiline(item.focus_phone)
    people_html = "".join(_person_row(person) for person in group)
    other_okved = escape(item.focus_other_okved or "").replace("\n", "<br>")

    return f"""
    <article class="company">
      <div>
        <header class="company-head">
          <div>
            <h2>{escape(company)}</h2>
            <div class="identity">ИНН {escape(item.company_inn or '—')} · {escape(item.focus_region or 'регион не указан')}</div>
          </div>
          <span class="{status_class}">{escape(status)}</span>
        </header>
        <div class="finance">
          <div><span>Выручка</span><b>{escape(item.focus_revenue or 'нет данных')}</b></div>
          <div><span>Баланс</span><b>{escape(item.focus_balance or 'нет данных')}</b></div>
          <div><span>Чистая прибыль / убыток</span><b>{escape(item.focus_profit or 'нет данных')}</b></div>
        </div>
        <div class="details-grid">
          <section>
            <span class="section-title">Основной вид деятельности</span>
            <p>{escape(item.focus_okved or 'не указан')}</p>
          </section>
          <section>
            <span class="section-title">Руководитель и команда</span>
            <p>{escape(item.focus_director or 'не указан')}{' · ' + escape(item.focus_employees) if item.focus_employees else ''}</p>
          </section>
          <section>
            <span class="section-title">Адрес</span>
            <p>{escape(item.focus_address or 'не указан')}</p>
          </section>
          <section>
            <span class="section-title">МСП</span>
            <p>{escape(item.focus_msp or 'не указано')}</p>
          </section>
        </div>
        {f'<details><summary>Другие виды деятельности</summary><p>{other_okved}</p></details>' if other_okved else ''}
      </div>
      <aside class="contacts">
        <div class="contact-block"><span class="section-title">Телефон</span>{phone_html}</div>
        <div class="contact-block"><span class="section-title">Почта</span>{email_html}</div>
        <div class="contact-block"><span class="section-title">Сайт</span>{website_html}</div>
        <div class="contact-block">
          <span class="section-title">Люди в TenChat</span>
          <div class="people">{people_html}</div>
        </div>
      </aside>
    </article>
    """


def _person_row(item) -> str:
    profile = _external_url(item.profile_url or item.source_url)
    link = f'<a class="button primary" href="{escape(profile)}" target="_blank" rel="noreferrer">Написать в TenChat</a>' if profile else ""
    return f"""
    <div class="person">
      <b>{escape(item.person_name or 'Имя не указано')}</b>
      <div class="muted">{escape(item.role_title or 'роль не указана')} · score {int(item.lead_score or 0)}</div>
      {link}
    </div>
    """


def _project_options(projects: list[dict], current_id: int) -> str:
    options = ['<option value="0">Выберите проект</option>']
    for project in projects:
        project_id = int(project["id"])
        selected = " selected" if project_id == current_id else ""
        options.append(f'<option value="{project_id}"{selected}>{escape(project["name"])}</option>')
    return "".join(options)


def _import_banner(result: dict) -> str:
    if result.get("error") == "project":
        return '<div class="banner error">Сначала выберите проект, затем загрузите файл Компаса.</div>'
    if result.get("error") == "import":
        return '<div class="banner error">Не удалось прочитать файл Компаса. Проверьте формат Excel и журнал контейнера.</div>'
    if not result.get("imported"):
        return ""
    return (
        '<div class="banner">'
        f'Файл обработан: {int(result.get("rows") or 0)} строк. '
        f'Совпало компаний по ИНН: <b>{int(result.get("matched") or 0)}</b>, '
        f'обновлено людей: <b>{int(result.get("people") or 0)}</b>, '
        f'действующих: <b>{int(result.get("active") or 0)}</b>, '
        f'без совпадения: {int(result.get("unmatched") or 0)}.'
        '</div>'
    )


def _metric(value: int, label: str) -> str:
    return f'<div class="metric"><b>{int(value)}</b><span>{escape(label)}</span></div>'


def _company_sort_key(group: list) -> tuple:
    item = group[0]
    return (
        0 if _is_active(item.focus_status) else 1,
        -max(int(person.lead_score or 0) for person in group),
        (item.focus_legal_name or item.company_legal_name or item.company_name or "").lower(),
    )


def _company_search_text(group: list) -> str:
    item = group[0]
    values = [
        item.company_inn,
        item.focus_legal_name,
        item.company_legal_name,
        item.company_name,
        item.focus_okved,
        item.focus_other_okved,
        item.focus_region,
        item.focus_director,
    ]
    for person in group:
        values.extend([person.person_name, person.role_title])
    return " ".join(str(value or "") for value in values).lower()


def _is_active(value: str | None) -> bool:
    return "действующее предприятие" in str(value or "").lower()


def _multiline(value: str | None) -> str:
    if not value:
        return "—"
    return "<br>".join(escape(line.strip()) for line in str(value).splitlines() if line.strip())


def _external_url(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    return "https://" + raw.lstrip("/")


def _empty_state(project_id: int, has_enriched: bool) -> str:
    if not project_id:
        return '<div class="empty">Выберите проект слева. Данные Компаса всегда загружаются в конкретный проект.</div>'
    if has_enriched:
        return '<div class="empty">По текущим фильтрам компаний нет.</div>'
    return '<div class="empty">В этом проекте ещё нет объединённых данных. Загрузите Excel из Контур Компаса слева.</div>'
