from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from datetime import datetime
from html import escape
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qs, urlencode

from fastapi import BackgroundTasks, FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from enrichment.domain_analyzer import analyze_domain
from focus_importer import import_focus_file
from scoring.icp_classifier import classify_icp
from social_leads.exporter import export_social_leads_to_xlsx
from social_leads.tenchat_finder import DEFAULT_TENCHAT_PRESET, TENCHAT_SEARCH_PRESETS, collect_people_leads
from storage.db import init_db
from storage.lead_repository import (
    clear_web_leads,
    count_web_leads,
    get_or_create_project,
    get_project,
    get_web_lead,
    get_web_leads,
    list_projects,
    save_leads,
    update_web_lead,
)
from storage.social_lead_repository import (
    clear_social_leads,
    count_social_leads,
    count_social_leads_with_inn,
    get_social_leads,
    update_social_lead,
)
from telegram_signals.exporter import export_signals_to_xlsx
from telegram_signals.keywords import CHAT_BAD_HINTS, CHAT_DISCOVERY_KEYWORDS, CHAT_GOOD_HINTS, SEGMENT_LABELS
from telegram_signals.repository import (
    WORKING_LEAD_FITS,
    count_signals,
    get_reject_reason_stats,
    get_search_profile,
    get_signal_comments_map,
    get_signals,
    get_source_quality_stats,
    list_search_profiles,
    reclassify_existing_signals,
    save_search_profile,
    set_signal_review_status,
    set_signal_status,
    update_signal_crm,
)
from telegram_signals.service import collect_signals
from utils.time_format import format_msk
from web_finder import collect_web_icp_leads
from web_exporter import export_compact_merged_leads_to_xlsx, export_inns_to_txt, export_web_leads_to_xlsx
from sources.web_query_templates import load_query_templates, save_query_templates

app = FastAPI(title="AdBeam ICP Finder")
logger = logging.getLogger(__name__)

WEB_JOB = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": None,
    "last_result": None,
    "last_preset": "all",
    "last_custom_queries": "",
    "last_template_category": "косметика",
    "last_total_limit": 40,
    "last_project_id": None,
    "last_project_name": "",
}

PEOPLE_JOB = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": None,
    "last_result": None,
    "last_preset": DEFAULT_TENCHAT_PRESET,
    "last_custom_queries": "",
    "last_total_limit": 40,
    "last_project_id": None,
    "last_project_name": "",
    "last_project_limit": 25,
}

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

REJECT_REASON_LABELS = {
    "no_own_business": "Нет своего бизнеса",
    "not_icp": "Не ICP",
    "product_research": "Кастдев / продукт",
    "supplier_or_ad": "Поставщик / реклама",
    "operations_only": "Операционка MP",
    "soft_opinion": "Мнение / обсуждение",
    "no_contact": "Нет контакта",
    "duplicate": "Дубль",
}

CJM_STAGE_LABELS = {
    "hot_outreach": "🔥 Ищет сейчас",
    "consideration": "🤔 Изучает варианты",
    "awareness": "💡 Осознает проблему",
    "signal_only": "📡 Сигнал",
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
    "taxes": "Налоги / отчеты",
    "certification": "Сертификация",
    "not_target": "Нецелевой",
}

LEAD_FIT_LABELS = {
    "hot_outreach": "Горячий",
    "warm_hypothesis": "Гипотеза",
    "warm_reply": "Теплый",
    "target": "Целевой",
    "review": "Проверить",
    "nurture": "Наблюдать",
    "market_insight": "Контекст",
    "not_icp": "Не ICP",
    "noise": "Шум",
    "contractor": "Подрядчик",
}


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return RedirectResponse("/web-leads", status_code=302)


def _run_web_collect_job(
    preset: str = "all",
    custom_queries: str | None = None,
    total_limit: int = 40,
    search_category: str | None = None,
    project_id: int | None = None,
    project_name: str | None = None,
) -> None:
    with JOB_LOCK:
        if WEB_JOB["running"]:
            return
        WEB_JOB.update(
            {
                "running": True,
                "last_started_at": format_msk(datetime.utcnow()),
                "last_finished_at": None,
                "last_error": None,
                "last_result": None,
                "last_preset": preset,
                "last_custom_queries": custom_queries or "",
                "last_template_category": search_category or WEB_JOB.get("last_template_category") or "",
                "last_total_limit": max(5, min(300, int(total_limit or 40))),
                "last_project_id": project_id,
                "last_project_name": project_name or "",
            }
        )

    try:
        result = asyncio.run(
            collect_web_icp_leads(
                preset=preset,
                custom_queries=custom_queries,
                total_limit=max(5, min(120, int(total_limit or 40))),
                search_category=search_category or preset,
                project_id=project_id,
                project_name=project_name,
            )
        )
        with JOB_LOCK:
            WEB_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": None,
                    "last_result": result,
                }
            )
    except Exception as exc:
        logger.exception("Web ICP collection failed")
        with JOB_LOCK:
            WEB_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": str(exc),
                    "last_result": None,
                }
            )


@app.post("/web-leads/search")
async def start_web_leads_search(request: Request, background_tasks: BackgroundTasks):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    preset = str(form.get("preset") or "all")
    custom_queries = str(form.get("custom_queries") or "").strip()
    template_category = str(form.get("template_category") or "").strip()
    try:
        project_id = int(form.get("project_id") or 0) or None
    except ValueError:
        project_id = None
    project = get_project(project_id)
    project_name = project.name if project else ""
    try:
        total_limit = max(5, min(120, int(form.get("total_limit") or 40)))
    except ValueError:
        total_limit = 40
    with JOB_LOCK:
        running = bool(WEB_JOB["running"])
        WEB_JOB["last_preset"] = preset
        WEB_JOB["last_custom_queries"] = custom_queries
        WEB_JOB["last_template_category"] = template_category or WEB_JOB.get("last_template_category") or "косметика"
        WEB_JOB["last_total_limit"] = total_limit
        WEB_JOB["last_project_id"] = project_id
        WEB_JOB["last_project_name"] = project_name
    if not running:
        background_tasks.add_task(
            _run_web_collect_job,
            preset,
            custom_queries or None,
            total_limit,
            template_category or preset,
            project_id,
            project_name,
        )
    redirect_url = f"/web-leads?project_id={project_id}" if project_id else "/web-leads"
    return RedirectResponse(redirect_url, status_code=303)


@app.post("/web-leads/projects")
async def create_web_project(request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    project = get_or_create_project(str(form.get("project_name") or "Новый проект"))
    with JOB_LOCK:
        WEB_JOB["last_project_id"] = project.id
        WEB_JOB["last_project_name"] = project.name
    return RedirectResponse(f"/web-leads?project_id={project.id}", status_code=303)


@app.get("/web-leads/job-status")
def web_leads_job_status():
    with JOB_LOCK:
        return JSONResponse(dict(WEB_JOB))


@app.post("/web-leads/clear")
def clear_web_leads_from_dashboard():
    deleted = clear_web_leads()
    with JOB_LOCK:
        WEB_JOB.update(
            {
                "last_result": {"created": 0, "updated": 0, "analyzed": 0, "deleted": deleted},
                "last_error": None,
                "last_finished_at": format_msk(datetime.utcnow()),
            }
        )
    return RedirectResponse("/web-leads", status_code=303)


@app.post("/web-leads/query-templates")
async def save_web_query_templates_from_dashboard(request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    save_query_templates(
        exhibition_templates_text=str(form.get("exhibition_templates") or ""),
        category_templates_text=str(form.get("category_templates") or ""),
    )
    return RedirectResponse("/web-leads", status_code=303)


@app.get("/web-leads/export")
def export_web_leads(project_id: int = 0):
    file_path = export_web_leads_to_xlsx(project_id=project_id or None)
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.name,
    )


@app.get("/web-leads/export-merged")
def export_merged_web_leads(project_id: int = 0):
    file_path = export_compact_merged_leads_to_xlsx(project_id=project_id or None)
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.name,
    )


@app.get("/web-leads/export-inn")
def export_web_lead_inns(project_id: int = 0):
    file_path = export_inns_to_txt(project_id=project_id or None)
    return FileResponse(file_path, media_type="text/plain; charset=utf-8", filename=file_path.name)


@app.post("/web-leads/import-focus")
async def import_focus_from_dashboard(file: UploadFile = File(...), project_id: int = 0):
    suffix = Path(file.filename or "").suffix or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = Path(tmp.name)
    try:
        result = import_focus_file(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass
    with JOB_LOCK:
        WEB_JOB.update(
            {
                "last_result": {"focus_import": True, **result},
                "last_error": None,
                "last_finished_at": format_msk(datetime.utcnow()),
            }
        )
    export_path = export_compact_merged_leads_to_xlsx(project_id=project_id or None)
    return FileResponse(
        export_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"merged_{export_path.name}",
    )


def _run_people_collect_job(
    custom_queries: str | None = None,
    total_limit: int = 40,
    preset: str = DEFAULT_TENCHAT_PRESET,
    project_id: int | None = None,
    project_name: str | None = None,
    project_limit: int = 25,
) -> None:
    with JOB_LOCK:
        if PEOPLE_JOB["running"]:
            return
        PEOPLE_JOB.update(
            {
                "running": True,
                "last_started_at": format_msk(datetime.utcnow()),
                "last_finished_at": None,
                "last_error": None,
                "last_result": None,
                "last_preset": preset if preset in TENCHAT_SEARCH_PRESETS else DEFAULT_TENCHAT_PRESET,
                "last_custom_queries": custom_queries or "",
                "last_total_limit": max(5, min(120, int(total_limit or 40))),
                "last_project_id": project_id,
                "last_project_name": project_name or "",
                "last_project_limit": max(1, min(150, int(project_limit or 25))),
            }
        )

    try:
        result = asyncio.run(
            collect_people_leads(
                custom_queries=custom_queries,
                preset=preset,
                total_limit=max(5, min(300, int(total_limit or 40))),
                project_id=project_id,
                project_name=project_name,
                project_limit=max(1, min(150, int(project_limit or 25))),
            )
        )
        with JOB_LOCK:
            PEOPLE_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": None,
                    "last_result": result,
                }
            )
    except Exception as exc:
        logger.exception("People ICP collection failed")
        with JOB_LOCK:
            PEOPLE_JOB.update(
                {
                    "running": False,
                    "last_finished_at": format_msk(datetime.utcnow()),
                    "last_error": str(exc),
                    "last_result": None,
                }
            )


@app.post("/people-leads/search")
async def start_people_leads_search(request: Request, background_tasks: BackgroundTasks):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    preset = str(form.get("preset") or DEFAULT_TENCHAT_PRESET)
    if preset not in TENCHAT_SEARCH_PRESETS:
        preset = DEFAULT_TENCHAT_PRESET
    custom_queries = str(form.get("custom_queries") or "").strip()
    try:
        project_id = int(form.get("project_id") or 0) or None
    except ValueError:
        project_id = None
    project = get_project(project_id)
    project_name = project.name if project else ""
    try:
        total_limit = max(5, min(300, int(form.get("total_limit") or 40)))
    except ValueError:
        total_limit = 40
    try:
        project_limit = max(1, min(150, int(form.get("project_limit") or 25)))
    except ValueError:
        project_limit = 25
    with JOB_LOCK:
        running = bool(PEOPLE_JOB["running"])
        PEOPLE_JOB["last_preset"] = preset
        PEOPLE_JOB["last_custom_queries"] = custom_queries
        PEOPLE_JOB["last_total_limit"] = total_limit
        PEOPLE_JOB["last_project_id"] = project_id
        PEOPLE_JOB["last_project_name"] = project_name
        PEOPLE_JOB["last_project_limit"] = project_limit
    if not running:
        background_tasks.add_task(
            _run_people_collect_job,
            custom_queries or None,
            total_limit,
            preset,
            project_id,
            project_name,
            project_limit,
        )
    redirect_url = f"/people-leads?project_id={project_id}" if project_id else "/people-leads"
    return RedirectResponse(redirect_url, status_code=303)


@app.post("/people-leads/projects")
async def create_people_project(request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    project = get_or_create_project(str(form.get("project_name") or "Новый проект"))
    with JOB_LOCK:
        PEOPLE_JOB["last_project_id"] = project.id
        PEOPLE_JOB["last_project_name"] = project.name
    return RedirectResponse(f"/people-leads?project_id={project.id}", status_code=303)


@app.get("/people-leads/job-status")
def people_leads_job_status():
    with JOB_LOCK:
        return JSONResponse(dict(PEOPLE_JOB))


@app.post("/people-leads/clear")
def clear_people_leads_from_dashboard(project_id: int = 0):
    deleted = clear_social_leads(project_id=project_id or None)
    with JOB_LOCK:
        PEOPLE_JOB.update(
            {
                "last_result": {"created": 0, "updated": 0, "analyzed": 0, "deleted": deleted},
                "last_error": None,
                "last_finished_at": format_msk(datetime.utcnow()),
            }
        )
    redirect_url = f"/people-leads?project_id={project_id}" if project_id else "/people-leads"
    return RedirectResponse(redirect_url, status_code=303)


@app.get("/people-leads/export")
def export_people_leads(project_id: int = 0):
    file_path = export_social_leads_to_xlsx(project_id=project_id or None)
    return FileResponse(
        file_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=file_path.name,
    )


@app.post("/people-leads/{lead_id}/crm")
async def update_people_lead_from_dashboard(lead_id: int, request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    update_social_lead(
        lead_id,
        status=str(form.get("status") or "new"),
        owner=str(form.get("owner") or ""),
        comment=str(form.get("comment") or ""),
    )
    return RedirectResponse(_return_url(request), status_code=303)


def _people_leads_dashboard_v2(
    *,
    request: Request,
    status: str = "",
    min_score: int = 0,
    q: str = "",
    project_id: int = 0,
    page: int = 1,
    per_page: int = 40,
) -> str:
    page = max(1, page)
    per_page = max(10, min(100, per_page))
    offset = (page - 1) * per_page
    selected_project_id = int(project_id or 0)
    items = get_social_leads(
        limit=per_page,
        offset=offset,
        status=status or None,
        min_score=min_score or None,
        query=q or None,
        project_id=selected_project_id or None,
    )
    with JOB_LOCK:
        people_job = dict(PEOPLE_JOB)

    projects = list_projects()
    selected_project = get_project(selected_project_id) if selected_project_id else None
    selected_project_web_count = next((int(project["count"] or 0) for project in projects if int(project["id"]) == selected_project_id), 0)
    search_project_id = selected_project_id or int(people_job.get("last_project_id") or 0)
    selected_people_preset = str(people_job.get("last_preset") or DEFAULT_TENCHAT_PRESET)
    if selected_people_preset not in TENCHAT_SEARCH_PRESETS:
        selected_people_preset = DEFAULT_TENCHAT_PRESET
    form_custom_queries = str(people_job.get("last_custom_queries") or "")
    preset_queries = "\n".join(TENCHAT_SEARCH_PRESETS[selected_people_preset]["queries"])
    preset_options = "".join(
        f"<option value='{escape(value)}' {_selected(selected_people_preset, value)}>{escape(config['label'])}</option>"
        for value, config in TENCHAT_SEARCH_PRESETS.items()
    )
    form_total_limit = max(5, min(300, int(people_job.get("last_total_limit") or 40)))
    form_project_limit = max(1, min(150, int(people_job.get("last_project_limit") or 25)))

    def nav_button(label: str, href: str, active: bool = False) -> str:
        cls = "nav-pill active" if active else "nav-pill"
        return f'<a class="{cls}" href="{escape(href)}">{escape(label)}</a>'

    def metric(label: str, value: int) -> str:
        return f"<div class='metric'><b>{value}</b><span>{escape(label)}</span></div>"

    def badge(text: str, tone: str = "") -> str:
        return f"<span class='badge {tone}'>{escape(text or '')}</span>"

    def project_options(current_id: int, include_all: bool = False) -> str:
        options = []
        if include_all:
            options.append(f"<option value='0' {_selected(str(current_id), '0')}>Общий пул</option>")
        else:
            options.append(f"<option value='' {_selected(str(current_id), '0')}>Без проекта</option>")
        for project in projects:
            value = str(project["id"])
            label = f'{project["name"]} ({project["count"]} web)'
            options.append(f"<option value='{escape(value)}' {_selected(str(current_id), value)}>{escape(label)}</option>")
        return "".join(options)

    def card(item) -> str:
        score = int(item.lead_score or 0)
        score_tone = "hot" if score >= 70 else "warm" if score >= 45 else "cold"
        title = escape(item.person_name or item.title or "Профиль / кандидат")
        role = escape(item.role_title or "роль не определена")
        company = escape(item.company_name or "компания не определена")
        inn = escape(item.company_inn or "")
        legal = escape(item.company_legal_name or "")
        matched = escape(item.matched_web_title or item.matched_web_domain or "")
        matched_domain = escape(item.matched_web_domain or "")
        url = escape(item.profile_url or item.source_url or "#")
        why = escape(item.why_relevant or "").replace("\n", "<br>")
        pain = escape(item.pain_detected or "")
        angle = escape(item.outreach_angle or "")
        opener = escape(item.opener or "")
        snippet = escape(_short(item.snippet or "", 260))
        status_value = escape(item.status or "new")
        return f"""
        <article class="lead-card">
          <div class="lead-main">
            <div class="lead-topline">
              <div>
                <a class="lead-title" href="{url}" target="_blank" rel="noreferrer">{title}</a>
                <div class="domain">{role} · {company}</div>
              </div>
              <div class="score {score_tone}">{score}</div>
            </div>
            <div class="badges">
              {badge(item.source or "tenchat")}
              {badge(item.lead_fit or "signal")}
              {badge(item.cjm_stage or "manual_check")}
              {badge("ИНН есть", "ok") if inn else badge("ИНН не найден")}
              {badge("связан с web", "ok") if matched else ""}
              {badge("score 70+", "ok") if score >= 70 else ""}
            </div>
            <div class="identity-row">
              <span><b>ИНН:</b> {inn or "не найден"}</span>
              <span><b>Юр. лицо:</b> {legal or "не найдено"}</span>
              <span><b>Web:</b> {matched or "не привязан"} {f"({matched_domain})" if matched_domain and matched_domain != matched else ""}</span>
            </div>
            <div class="site-check">
              <b>Почему такой рейтинг</b>
              <span>{why or "нужна ручная проверка"}</span>
            </div>
            <div class="columns">
              <section><h3>Контекст</h3><p>{pain}</p></section>
              <section><h3>Заход</h3><p>{angle}</p></section>
              <section><h3>Фрагмент</h3><p>{snippet}</p></section>
            </div>
            <details>
              <summary>Черновик первого сообщения</summary>
              <p>{opener}</p>
            </details>
          </div>
          <aside class="lead-side">
            <form method="post" action="/people-leads/{item.id}/crm">
              <label>Статус
                <select name="status">
                  <option value="new" {_selected(status_value, "new")}>Новый</option>
                  <option value="reviewed" {_selected(status_value, "reviewed")}>Проверил</option>
                  <option value="contacted" {_selected(status_value, "contacted")}>Написал</option>
                  <option value="replied" {_selected(status_value, "replied")}>Ответил</option>
                  <option value="dead" {_selected(status_value, "dead")}>Архив</option>
                </select>
              </label>
              <label>Ответственный <input name="owner" value="{escape(item.owner or '')}"></label>
              <label>Комментарий <textarea name="comment">{escape(item.comment or '')}</textarea></label>
              <button class="primary-btn" type="submit">Сохранить</button>
            </form>
          </aside>
        </article>
        """

    result = people_job.get("last_result") or {}
    if people_job.get("running"):
        job_text = "Идет поиск людей в TenChat..."
    elif people_job.get("last_error"):
        job_text = f"Ошибка: {people_job['last_error']}"
    elif result and "deleted" in result:
        job_text = f"Очищено связей/лидов: {result.get('deleted', 0)}."
    elif result:
        job_text = (
            f"Последний сбор: {result.get('created', 0)} новых, {result.get('updated', 0)} обновлено, "
            f"{result.get('kept', 0)} оставлено из {result.get('candidates', 0)} кандидатов. "
            f"Проект: {result.get('project_name') or 'общий пул'}. Завершен: {people_job.get('last_finished_at')}"
        )
    else:
        job_text = "TenChat-поиск еще не запускался."

    query_params = {"status": status, "min_score": min_score, "q": q, "project_id": selected_project_id, "per_page": per_page}
    prev_params = {**query_params, "page": max(1, page - 1)}
    next_params = {**query_params, "page": page + 1}
    cards = "".join(card(item) for item in items) or "<div class='empty'>Под эти фильтры людей пока нет.</div>"
    selected_project_title = "Общий пул" if not selected_project_id else (selected_project.name if selected_project else "Проект")
    project_warning = (
        "<div class='warning'>В этом проекте пока 0 web-компаний. Для поиска по конкретным компаниям сначала собери Web ICP базу в этот проект.</div>"
        if selected_project_id and selected_project_web_count == 0
        else ""
    )

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>TenChat People ICP</title>
      <style>
        :root {{ --bg:#eef3f7; --panel:#fff; --text:#0f172a; --muted:#64748b; --line:#d8e1ea; --blue:#2563eb; --green:#059669; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
        .page {{ max-width:1280px; margin:0 auto; padding:24px 22px 56px; }}
        a {{ color:inherit; text-decoration:none; }}
        h1 {{ margin:0; font-size:30px; letter-spacing:0; }}
        h3 {{ margin:0 0 6px; font-size:12px; color:var(--muted); text-transform:uppercase; }}
        p {{ margin:0; }}
        .topbar {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; margin-bottom:16px; }}
        .subtitle {{ color:var(--muted); max-width:760px; margin-top:6px; }}
        .nav, .actions {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
        .nav-pill, .link-btn, .primary-btn, .danger-btn {{ min-height:36px; display:inline-flex; align-items:center; justify-content:center; border-radius:7px; padding:0 12px; border:1px solid var(--line); background:#fff; cursor:pointer; }}
        .nav-pill.active, .primary-btn {{ background:var(--blue); border-color:var(--blue); color:#fff; font-weight:700; }}
        .danger-btn {{ border-color:#fecaca; color:#b91c1c; }}
        .layout {{ display:grid; grid-template-columns:320px minmax(0, 1fr); gap:14px; align-items:start; }}
        .sidebar {{ position:sticky; top:14px; display:grid; gap:10px; }}
        .metrics {{ display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:8px; margin-bottom:10px; }}
        .metric, .panel, .lead-card, .empty {{ background:#fff; border:1px solid var(--line); border-radius:8px; }}
        .metric {{ padding:10px 12px; min-height:68px; }}
        .metric b {{ display:block; font-size:22px; }}
        .metric span, .domain {{ color:var(--muted); }}
        .panel {{ padding:12px; margin-bottom:10px; }}
        .side-form {{ display:grid; gap:10px; }}
        .inline-form {{ display:grid; grid-template-columns:1fr auto; gap:8px; align-items:end; }}
        label {{ display:grid; gap:5px; color:var(--muted); font-size:12px; }}
        input, select, textarea {{ width:100%; border:1px solid #cbd5e1; border-radius:7px; padding:9px 10px; background:#fff; color:#0f172a; font:inherit; }}
        textarea {{ min-height:68px; resize:vertical; }}
        .hint {{ color:var(--muted); font-size:12px; margin:8px 0; }}
        .warning {{ margin:8px 0; padding:8px 10px; border:1px solid #fde68a; background:#fffbeb; color:#92400e; border-radius:7px; font-size:12px; }}
        .query-details {{ margin-top:10px; border:1px solid var(--line); border-radius:8px; padding:10px; background:#f8fafc; }}
        .query-details summary {{ border:0; padding:0; color:var(--blue); }}
        .preset-preview {{ margin-top:8px; color:var(--muted); font-size:12px; white-space:pre-wrap; }}
        .panel-head {{ display:flex; justify-content:space-between; gap:10px; align-items:center; margin-bottom:10px; }}
        .job {{ color:#059669; margin-top:10px; }}
        .filters {{ display:grid; grid-template-columns:120px 130px 1fr 120px; gap:10px; align-items:end; border-top:1px solid var(--line); padding-top:12px; }}
        .pager {{ display:flex; justify-content:space-between; align-items:center; color:var(--muted); margin:10px 0; }}
        .pager-actions {{ display:flex; gap:8px; }}
        .lead-card {{ display:grid; grid-template-columns:minmax(0,1fr) 230px; gap:12px; padding:12px; margin-bottom:10px; }}
        .lead-topline {{ display:flex; justify-content:space-between; gap:12px; }}
        .lead-title {{ display:block; font-size:20px; font-weight:800; }}
        .score {{ width:52px; height:52px; display:grid; place-items:center; border-radius:8px; font-size:22px; font-weight:900; background:#e2e8f0; color:#475569; flex:0 0 auto; }}
        .score.hot {{ background:#dcfce7; color:#047857; }}
        .score.warm {{ background:#fef3c7; color:#92400e; }}
        .badges {{ display:flex; flex-wrap:wrap; gap:6px; margin:10px 0; }}
        .badge {{ border:1px solid var(--line); border-radius:999px; padding:4px 9px; font-size:12px; color:#334155; }}
        .badge.ok {{ background:#ecfdf5; border-color:#bbf7d0; color:#047857; }}
        .identity-row {{ display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:8px; color:#334155; margin:8px 0; }}
        .identity-row span {{ padding:7px 8px; border:1px solid var(--line); border-radius:7px; background:#f8fafc; overflow-wrap:anywhere; }}
        .site-check {{ border:1px solid #bbf7d0; background:#f0fdf4; color:#065f46; border-radius:7px; padding:8px 10px; margin:8px 0 12px; display:grid; gap:2px; }}
        .columns {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; border-top:1px solid var(--line); padding-top:10px; }}
        .columns section:last-child {{ display:none; }}
        details {{ margin-top:12px; border-top:1px solid var(--line); padding-top:10px; color:#334155; }}
        summary {{ cursor:pointer; color:var(--blue); font-weight:700; }}
        .lead-side {{ border-left:1px solid var(--line); padding-left:14px; }}
        .lead-side form {{ display:grid; gap:8px; }}
        .empty {{ padding:28px; color:var(--muted); }}
        @media (max-width:980px) {{ .topbar, .lead-card, .layout {{ display:block; }} .sidebar {{ position:static; }} .nav {{ justify-content:flex-start; margin-top:12px; }} .metrics, .filters, .columns, .identity-row {{ grid-template-columns:1fr; }} .lead-side {{ border-left:0; border-top:1px solid var(--line); padding:12px 0 0; margin-top:12px; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="topbar">
          <div>
            <h1>TenChat люди</h1>
            <div class="subtitle">Ищем не статьи, а потенциальных ЛПР: собственников, директоров, маркетинг и e-commerce у компаний из ICP. ИНН подтягиваем из web-базы или из открытого текста профиля.</div>
          </div>
          <nav class="nav">
            {nav_button("Web", "/web-leads")}
            {nav_button("TenChat", "/people-leads", True)}
            {nav_button("Telegram", "/telegram-signals")}
          </nav>
        </div>
        <div class="layout">
          <aside class="sidebar">
            <section class="panel">
              <div class="panel-head"><b>{escape(selected_project_title)}</b></div>
              <form id="new-people-project-form" method="post" action="/people-leads/projects" class="inline-form">
                <label>Новый проект <input name="project_name" placeholder="Например: Текстиль июнь"></label>
                <button class="primary-btn" type="submit">Создать</button>
              </form>
              <form id="people-search-form" method="post" action="/people-leads/search" class="side-form">
                <label>Проект для поиска
                  <select name="project_id">{project_options(search_project_id, include_all=False)}</select>
                </label>
                <label>Сценарий
                  <select name="preset">{preset_options}</select>
                </label>
                <label>Компаний из web-проекта <input type="number" name="project_limit" min="1" max="150" value="{form_project_limit}"></label>
                <label>Лимит TenChat-кандидатов <input type="number" name="total_limit" min="5" max="300" value="{form_total_limit}"></label>
                <label>Свои запросы
                  <textarea name="custom_queries" placeholder="По одному запросу на строку. Можно без site:tenchat.ru">{escape(form_custom_queries)}</textarea>
                </label>
                <button class="primary-btn" type="submit">Найти людей</button>
              </form>
              <details class="query-details">
                <summary>Шаблон текущего сценария</summary>
                <div class="preset-preview">{escape(preset_queries)}</div>
              </details>
              {project_warning}
              <div class="hint">Лучший режим: сначала собрать Web ICP проект, потом тут выбрать его и искать ЛПР по найденным компаниям.</div>
              <form method="post" action="/people-leads/clear?project_id={selected_project_id}" onsubmit="return confirm('Очистить выбранный TenChat-срез?')">
                <button class="danger-btn" type="submit">{'Очистить проект' if selected_project_id else 'Очистить всю TenChat-базу'}</button>
              </form>
              <div class="job" id="people-job-text">{escape(job_text)}</div>
            </section>
          </aside>
          <section>
            <section class="metrics">
              {metric("вся база", count_social_leads())}
              {metric("в проекте", count_social_leads(project_id=selected_project_id or None))}
              {metric("score 70+ проект", count_social_leads(min_score=70, project_id=selected_project_id or None))}
              {metric("с ИНН проект", count_social_leads_with_inn(project_id=selected_project_id or None))}
            </section>
            <section class="panel">
              <div class="panel-head">
                <b>Выгрузки и фильтр</b>
                <div class="actions">
                  <a class="link-btn" href="/people-leads/export">Excel: вся база</a>
                  <a class="primary-btn" href="/people-leads/export?project_id={selected_project_id}">Excel: проект</a>
                </div>
              </div>
              <form method="get" action="/people-leads" class="filters">
                <input type="hidden" name="project_id" value="{selected_project_id}">
                <label>Score от <input type="number" name="min_score" value="{int(min_score or 0)}" min="0" max="100"></label>
                <label>Статус <select name="status">
                  <option value="">Все</option>
                  <option value="new" {_selected(status, "new")}>Новые</option>
                  <option value="reviewed" {_selected(status, "reviewed")}>Проверил</option>
                  <option value="contacted" {_selected(status, "contacted")}>Написал</option>
                  <option value="replied" {_selected(status, "replied")}>Ответил</option>
                  <option value="dead" {_selected(status, "dead")}>Архив</option>
                </select></label>
                <label>Поиск <input name="q" value="{escape(q or '')}" placeholder="человек, роль, компания, ИНН, причина"></label>
                <button class="primary-btn" type="submit">Фильтр</button>
              </form>
            </section>
            <div class="pager">
              <span>Страница {page}. Показано {len(items)}.</span>
              <div class="pager-actions">
                <a class="link-btn" href="/people-leads?{escape(urlencode(prev_params))}">Назад</a>
                <a class="link-btn" href="/people-leads?{escape(urlencode(next_params))}">Вперед</a>
              </div>
            </div>
            {cards}
          </section>
        </div>
      </main>
      <script>
        let peopleJobWasRunning = false;
        async function pollPeopleJob() {{
          try {{
            const response = await fetch('/people-leads/job-status', {{ cache: 'no-store' }});
            const job = await response.json();
            if (job.running) {{
              peopleJobWasRunning = true;
              const el = document.getElementById('people-job-text');
              if (el) el.textContent = 'Идет поиск людей в TenChat...';
              setTimeout(pollPeopleJob, 2500);
              return;
            }}
            if (peopleJobWasRunning) window.location.reload();
          }} catch (e) {{}}
        }}
        pollPeopleJob();
      </script>
    </body>
    </html>
    """


@app.get("/people-leads", response_class=HTMLResponse)
def people_leads_dashboard(
    request: Request,
    status: str = "",
    min_score: int = 0,
    q: str = "",
    project_id: int = 0,
    page: int = 1,
    per_page: int = 40,
):
    return _people_leads_dashboard_v2(
        request=request,
        status=status,
        min_score=min_score,
        q=q,
        project_id=project_id,
        page=page,
        per_page=per_page,
    )
    page = max(1, page)
    per_page = max(10, min(100, per_page))
    offset = (page - 1) * per_page
    items = get_social_leads(
        limit=per_page,
        offset=offset,
        status=status or None,
        min_score=min_score or None,
        query=q or None,
    )
    with JOB_LOCK:
        people_job = dict(PEOPLE_JOB)

    selected_people_preset = str(people_job.get("last_preset") or DEFAULT_TENCHAT_PRESET)
    if selected_people_preset not in TENCHAT_SEARCH_PRESETS:
        selected_people_preset = DEFAULT_TENCHAT_PRESET
    form_custom_queries = str(people_job.get("last_custom_queries") or "")
    preset_queries = "\n".join(TENCHAT_SEARCH_PRESETS[selected_people_preset]["queries"])
    preset_options = "".join(
        f"<option value='{escape(value)}' {_selected(selected_people_preset, value)}>{escape(config['label'])}</option>"
        for value, config in TENCHAT_SEARCH_PRESETS.items()
    )
    try:
        form_total_limit = max(5, min(120, int(people_job.get("last_total_limit") or 40)))
    except ValueError:
        form_total_limit = 40

    def nav_button(label: str, href: str, active: bool = False) -> str:
        cls = "nav-pill active" if active else "nav-pill"
        return f'<a class="{cls}" href="{escape(href)}">{escape(label)}</a>'

    def metric(label: str, value: int) -> str:
        return f"<div class='metric'><b>{value}</b><span>{escape(label)}</span></div>"

    def badge(text: str, tone: str = "") -> str:
        return f"<span class='badge {tone}'>{escape(text or '')}</span>"

    def card(item) -> str:
        score = int(item.lead_score or 0)
        score_tone = "hot" if score >= 70 else "warm" if score >= 45 else "cold"
        title = escape(item.person_name or item.title or "Профиль / сигнал")
        role = escape(item.role_title or "роль не определена")
        company = escape(item.company_name or "компания не определена")
        url = escape(item.source_url or item.profile_url or "#")
        why = escape(item.why_relevant or "").replace("\n", "<br>")
        pain = escape(item.pain_detected or "")
        angle = escape(item.outreach_angle or "")
        opener = escape(item.opener or "")
        snippet = escape(item.snippet or "")
        status_value = escape(item.status or "new")
        return f"""
        <article class="lead-card">
          <div class="lead-main">
            <div class="lead-topline">
              <div>
                <a class="lead-title" href="{url}" target="_blank" rel="noreferrer">{title}</a>
                <div class="domain">{role} · {company}</div>
              </div>
              <div class="score {score_tone}">{score}</div>
            </div>
            <div class="badges">
              {badge(item.source or "tenchat")}
              {badge(item.lead_fit or "signal")}
              {badge(item.cjm_stage or "signal_only")}
              {badge("score 70+", "ok") if score >= 70 else ""}
            </div>
            <div class="site-check">
              <b>Почему подходит</b>
              <span>{why or "нужно проверить вручную"}</span>
            </div>
            <div class="columns">
              <section><h3>Боль / триггер</h3><p>{pain}</p></section>
              <section><h3>Заход</h3><p>{angle}</p></section>
              <section><h3>Фрагмент</h3><p>{snippet}</p></section>
            </div>
            <details>
              <summary>Черновик первого сообщения</summary>
              <p>{opener}</p>
            </details>
          </div>
          <aside class="lead-side">
            <form method="post" action="/people-leads/{item.id}/crm">
              <label>Статус
                <select name="status">
                  <option value="new" {_selected(status_value, "new")}>Новый</option>
                  <option value="reviewed" {_selected(status_value, "reviewed")}>Проверил</option>
                  <option value="contacted" {_selected(status_value, "contacted")}>Написал</option>
                  <option value="replied" {_selected(status_value, "replied")}>Ответил</option>
                  <option value="dead" {_selected(status_value, "dead")}>Архив</option>
                </select>
              </label>
              <label>Ответственный <input name="owner" value="{escape(item.owner or '')}"></label>
              <label>Комментарий <textarea name="comment">{escape(item.comment or '')}</textarea></label>
              <button class="primary-btn" type="submit">Сохранить</button>
            </form>
          </aside>
        </article>
        """

    result = people_job.get("last_result") or {}
    if people_job.get("running"):
        job_text = "Идет поиск прямого спроса в TenChat..."
    elif people_job.get("last_error"):
        job_text = f"Ошибка: {people_job['last_error']}"
    elif result and "deleted" in result:
        job_text = f"TenChat-результаты очищены: удалено {result.get('deleted', 0)}."
    elif result:
        job_text = (
            f"Последний сбор: {result.get('created', 0)} новых, {result.get('updated', 0)} обновлено, "
            f"{result.get('kept', 0)} оставлено из {result.get('candidates', 0)} кандидатов. "
            f"Завершен: {people_job.get('last_finished_at')}"
        )
    else:
        job_text = "TenChat-поиск еще не запускался."

    query_params = {"status": status, "min_score": min_score, "q": q, "per_page": per_page}
    prev_params = {**query_params, "page": max(1, page - 1)}
    next_params = {**query_params, "page": page + 1}
    cards = "".join(card(item) for item in items) or "<div class='empty'>Под эти фильтры TenChat-лидов пока нет.</div>"

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>TenChat ICP</title>
      <style>
        :root {{ --bg:#eef3f7; --panel:#fff; --text:#0f172a; --muted:#64748b; --line:#d8e1ea; --blue:#2563eb; --green:#059669; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
        .page {{ max-width:1260px; margin:0 auto; padding:24px 22px 56px; }}
        a {{ color:inherit; text-decoration:none; }}
        h1 {{ margin:0; font-size:30px; letter-spacing:0; }}
        h3 {{ margin:0 0 6px; font-size:12px; color:var(--muted); text-transform:uppercase; }}
        p {{ margin:0; }}
        .topbar {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; margin-bottom:16px; }}
        .subtitle {{ color:var(--muted); max-width:760px; margin-top:6px; }}
        .nav, .actions {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
        .nav-pill, .link-btn, .primary-btn, .danger-btn {{ min-height:36px; display:inline-flex; align-items:center; border-radius:7px; padding:0 12px; border:1px solid var(--line); background:#fff; cursor:pointer; }}
        .nav-pill.active, .primary-btn {{ background:var(--blue); border-color:var(--blue); color:#fff; font-weight:700; }}
        .danger-btn {{ border-color:#fecaca; color:#b91c1c; }}
        .metrics {{ display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:8px; margin-bottom:10px; }}
        .metric, .panel, .lead-card, .empty {{ background:#fff; border:1px solid var(--line); border-radius:8px; }}
        .metric {{ padding:10px 12px; min-height:68px; }}
        .metric b {{ display:block; font-size:22px; }}
        .metric span, .domain {{ color:var(--muted); }}
        .panel {{ padding:12px; margin-bottom:10px; }}
        .search-form {{ display:grid; grid-template-columns:260px 100px 132px; gap:10px; align-items:end; }}
        label {{ display:grid; gap:5px; color:var(--muted); font-size:12px; }}
        input, select, textarea {{ width:100%; border:1px solid #cbd5e1; border-radius:7px; padding:9px 10px; background:#fff; color:#0f172a; font:inherit; }}
        textarea {{ min-height:68px; resize:vertical; }}
        .search-note {{ color:var(--muted); margin-top:8px; }}
        .query-details {{ margin-top:10px; border:1px solid var(--line); border-radius:8px; padding:10px; background:#f8fafc; }}
        .query-details summary {{ border:0; padding:0; color:var(--blue); }}
        .preset-preview {{ margin-top:8px; color:var(--muted); font-size:12px; white-space:pre-wrap; }}
        .panel-head {{ display:flex; justify-content:space-between; gap:10px; align-items:center; margin-bottom:10px; }}
        .job {{ color:#059669; margin-top:10px; }}
        .filters {{ display:grid; grid-template-columns:120px 120px 1fr 120px; gap:10px; align-items:end; border-bottom:1px solid var(--line); padding-bottom:12px; margin-bottom:12px; }}
        .pager {{ display:flex; justify-content:space-between; align-items:center; color:var(--muted); margin:10px 0; }}
        .pager-actions {{ display:flex; gap:8px; }}
        .lead-card {{ display:grid; grid-template-columns:minmax(0,1fr) 230px; gap:12px; padding:12px; margin-bottom:10px; }}
        .lead-topline {{ display:flex; justify-content:space-between; gap:12px; }}
        .lead-title {{ display:block; font-size:20px; font-weight:800; }}
        .score {{ width:52px; height:52px; display:grid; place-items:center; border-radius:8px; font-size:22px; font-weight:900; background:#e2e8f0; color:#475569; flex:0 0 auto; }}
        .score.hot {{ background:#dcfce7; color:#047857; }}
        .score.warm {{ background:#fef3c7; color:#92400e; }}
        .badges {{ display:flex; flex-wrap:wrap; gap:6px; margin:10px 0; }}
        .badge {{ border:1px solid var(--line); border-radius:999px; padding:4px 9px; font-size:12px; color:#334155; }}
        .badge.ok {{ background:#ecfdf5; border-color:#bbf7d0; color:#047857; }}
        .site-check {{ border:1px solid #bbf7d0; background:#f0fdf4; color:#065f46; border-radius:7px; padding:8px 10px; margin:8px 0 12px; display:grid; gap:2px; }}
        .columns {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; border-top:1px solid var(--line); padding-top:10px; }}
        .columns section:last-child {{ display:none; }}
        details {{ margin-top:12px; border-top:1px solid var(--line); padding-top:10px; color:#334155; }}
        summary {{ cursor:pointer; color:var(--blue); font-weight:700; }}
        .lead-side {{ border-left:1px solid var(--line); padding-left:14px; }}
        .lead-side form {{ display:grid; gap:8px; }}
        .empty {{ padding:28px; color:var(--muted); }}
        @media (max-width:900px) {{ .topbar, .lead-card {{ display:block; }} .nav {{ justify-content:flex-start; margin-top:12px; }} .metrics, .search-form, .filters, .columns {{ grid-template-columns:1fr; }} .lead-side {{ border-left:0; border-top:1px solid var(--line); padding:12px 0 0; margin-top:12px; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="topbar">
          <div>
            <h1>TenChat спрос</h1>
            <div class="subtitle">Ищем прямые запросы на рекламу и подрядчиков, затем отсекаем не-ICP: агентства, вакансии, обучение и общий контент без покупательского намерения.</div>
          </div>
          <nav class="nav">
            {nav_button("Web", "/web-leads")}
            {nav_button("TenChat", "/people-leads", True)}
            {nav_button("Telegram", "/telegram-signals")}
          </nav>
        </div>
        <section class="metrics">
          {metric("всего лидов", count_social_leads())}
          {metric("score 70+", count_social_leads(min_score=70))}
          {metric("score 45+", count_social_leads(min_score=45))}
          {metric("новые", count_social_leads(status="new"))}
        </section>
        <section class="panel">
          <div class="panel-head">
            <b>Поиск спроса</b>
            <a class="primary-btn" href="/people-leads/export">Скачать Excel</a>
          </div>
          <form id="people-search-form" method="post" action="/people-leads/search" class="search-form">
            <label>Сценарий поиска
              <select name="preset">{preset_options}</select>
            </label>
            <label>Лимит <input type="number" name="total_limit" min="5" max="120" value="{form_total_limit}"></label>
            <button class="primary-btn" type="submit">Запустить</button>
          </form>
          <details class="query-details">
            <summary>Свои запросы и текущий пресет</summary>
            <label>Дополнительные запросы
              <textarea name="custom_queries" form="people-search-form" placeholder="Можно оставить пустым. По одному запросу на строку, site:tenchat.ru можно не писать.">{escape(form_custom_queries)}</textarea>
            </label>
            <div class="preset-preview">{escape(preset_queries)}</div>
          </details>
          <div class="search-note">Ищем прямой коммерческий сигнал: “ищу / нужен / посоветуйте” + рекламный канал + ICP-контекст. Просто статьи и агентства, которые продают себя, режутся.</div>
          <div class="actions" style="justify-content:flex-start;margin-top:10px;">
            <form method="post" action="/people-leads/clear" onsubmit="return confirm('Очистить people-результаты?')">
              <button class="danger-btn" type="submit">Очистить</button>
            </form>
          </div>
          <div class="job">{escape(job_text)}</div>
        </section>
        <form method="get" action="/people-leads" class="filters">
          <label>Score от <input type="number" name="min_score" value="{int(min_score or 0)}" min="0" max="100"></label>
          <label>Статус <select name="status">
            <option value="">Все</option>
            <option value="new" {_selected(status, "new")}>Новые</option>
            <option value="reviewed" {_selected(status, "reviewed")}>Проверил</option>
            <option value="contacted" {_selected(status, "contacted")}>Написал</option>
            <option value="replied" {_selected(status, "replied")}>Ответил</option>
            <option value="dead" {_selected(status, "dead")}>Архив</option>
          </select></label>
          <label>Поиск <input name="q" value="{escape(q or '')}" placeholder="человек, роль, компания, боль"></label>
          <button class="primary-btn" type="submit">Фильтр</button>
        </form>
        <div class="pager">
          <span>Страница {page}. Показано {len(items)}.</span>
          <div class="pager-actions">
            <a class="link-btn" href="/people-leads?{escape(urlencode(prev_params))}">Назад</a>
            <a class="link-btn" href="/people-leads?{escape(urlencode(next_params))}">Вперед</a>
          </div>
        </div>
        {cards}
      </main>
      <script>
        let peopleJobWasRunning = false;
        async function pollPeopleJob() {{
          try {{
            const response = await fetch('/people-leads/job-status', {{ cache: 'no-store' }});
            const job = await response.json();
            if (job.running) {{
              peopleJobWasRunning = true;
              setTimeout(pollPeopleJob, 2500);
              return;
            }}
            if (peopleJobWasRunning) window.location.reload();
          }} catch (e) {{}}
        }}
        pollPeopleJob();
      </script>
    </body>
    </html>
    """


@app.post("/web-leads/{lead_id}/crm")
async def update_web_lead_from_dashboard(lead_id: int, request: Request):
    form = {key: values[-1] for key, values in parse_qs((await request.body()).decode("utf-8")).items()}
    update_web_lead(
        lead_id,
        status=str(form.get("status") or "new"),
        owner=str(form.get("owner") or ""),
        comment=str(form.get("comment") or ""),
    )
    return RedirectResponse(_return_url(request), status_code=303)


@app.post("/web-leads/{lead_id}/refresh")
def refresh_web_lead_from_dashboard(lead_id: int, request: Request):
    item = get_web_lead(lead_id)
    if item is None:
        return RedirectResponse(_return_url(request), status_code=303)

    site = asyncio.run(analyze_domain(item.domain_normalized or item.domain))
    has_contacts = bool(site.get("email") or site.get("phone"))
    classification = classify_icp(
        title=site.get("title") or item.title or item.company_name,
        description=site.get("description"),
        h1=site.get("h1"),
        text=site.get("text"),
        company_name=item.company_name,
        domain=item.domain,
        has_contacts=has_contacts,
        has_catalog=bool(site.get("has_catalog")),
        has_cart=bool(site.get("has_cart")),
        ecommerce_score=int(site.get("ecommerce_score") or 0),
        site_type=site.get("site_type"),
        site_assessment=site.get("site_assessment"),
    )
    save_leads(
        [
            {
                "query": item.query or "manual_refresh",
                "company_name": item.company_name,
                "domain": item.domain,
                "source": item.source or "refresh",
                "source_url": item.source_url,
                "search_category": item.search_category,
                "title": site.get("title") or item.title or item.company_name,
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
                "status": item.status or "new",
                **classification,
            }
        ]
    )
    return RedirectResponse(_return_url(request), status_code=303)


@app.get("/web-leads", response_class=HTMLResponse)
def web_leads_dashboard(
    request: Request,
    status: str = "",
    min_score: int = 0,
    q: str = "",
    project_id: int = 0,
    page: int = 1,
    per_page: int = 40,
):
    page = max(1, page)
    per_page = max(10, min(100, per_page))
    offset = (page - 1) * per_page
    items = get_web_leads(
        limit=per_page,
        offset=offset,
        status=status or None,
        min_score=min_score or None,
        query=q or None,
        project_id=project_id or None,
    )

    with JOB_LOCK:
        web_job = dict(WEB_JOB)
    projects = list_projects()
    selected_project_id = int(project_id or 0)
    selected_project = get_project(selected_project_id) if selected_project_id else None
    search_project_id = selected_project_id or int(web_job.get("last_project_id") or 0)
    selected_preset = str(web_job.get("last_preset") or "all")
    form_custom_queries = str(web_job.get("last_custom_queries") or "")
    template_category = str(web_job.get("last_template_category") or "косметика")
    query_templates = load_query_templates()
    exhibition_templates_text = "\n".join(query_templates["exhibition_templates"])
    category_templates_text = "\n".join(query_templates["category_templates"])
    query_templates_json = json.dumps(query_templates, ensure_ascii=False).replace("</", "<\\/")
    try:
        form_total_limit = max(5, min(120, int(web_job.get("last_total_limit") or 40)))
    except ValueError:
        form_total_limit = 40

    def nav_button(label: str, href: str, active: bool = False) -> str:
        cls = "nav-pill active" if active else "nav-pill"
        return f'<a class="{cls}" href="{escape(href)}">{escape(label)}</a>'

    def metric(label: str, value: int) -> str:
        return f"<div class='metric'><b>{value}</b><span>{escape(label)}</span></div>"

    def badge(text: str, tone: str = "") -> str:
        return f"<span class='badge {tone}'>{escape(text)}</span>"

    def project_options(current_id: int, include_all: bool = False, all_label: str = "Общий пул") -> str:
        options = []
        if include_all:
            options.append(f"<option value='0' {_selected(str(current_id), '0')}>{escape(all_label)}</option>")
        else:
            options.append(f"<option value='' {_selected(str(current_id), '0')}>Без проекта</option>")
        for project in projects:
            value = str(project["id"])
            label = f'{project["name"]} ({project["count"]})'
            options.append(f"<option value='{escape(value)}' {_selected(str(current_id), value)}>{escape(label)}</option>")
        return "".join(options)

    def lead_card(item) -> str:
        site = f"https://{escape(item.domain_normalized or item.domain)}"
        score_tone = "hot" if int(item.icp_score or 0) >= 70 else "warm" if int(item.icp_score or 0) >= 45 else "cold"
        contacts = []
        if item.company_email:
            contacts.append(f"<a href='mailto:{escape(item.company_email)}'>{escape(item.company_email)}</a>")
        if item.company_phone:
            contacts.append(escape(item.company_phone))
        if item.company_inn:
            contacts.append(f"ИНН: {escape(item.company_inn)}")
        if not contacts:
            contacts.append("контакты не найдены")
        evidence = escape(item.evidence or item.icp_reason or "нет доказательств").replace("\n", "<br>")
        site_assessment = escape(item.site_assessment or "оценка сайта не проводилась")
        opener = escape(item.opener or "")
        hypothesis = escape(item.hypothesis or "")
        angle = escape(item.outreach_angle or "")
        title = escape(item.title or item.company_name or item.domain)
        legal = escape(item.company_legal_name or "")
        status_value = escape(item.status or "new")
        return f"""
        <article class="lead-card">
          <div class="lead-main">
            <div class="lead-topline">
              <div>
                <a class="lead-title" href="{site}" target="_blank" rel="noreferrer">{title}</a>
                <div class="domain">{escape(item.domain_normalized or item.domain)} {f" · {legal}" if legal else ""}</div>
              </div>
              <div class="score {score_tone}">{int(item.icp_score or 0)}</div>
            </div>
            <div class="badges">
              {badge("ICP" if item.is_icp else "проверить", "ok" if item.is_icp else "")}
              {badge(item.search_category or "без категории")}
              {badge(item.lead_type or "unknown")}
              {badge(item.priority or "low")}
              {badge(item.cjm_stage or "signal_only")}
              {badge("контакты есть", "ok") if item.has_contacts else badge("нет контактов")}
              {badge("каталог", "ok") if item.has_catalog else badge("без каталога")}
              {badge("корзина", "ok") if item.has_cart else badge("без корзины")}
            </div>
            <div class="site-check">
              <b>Сайт: {escape(item.site_type or "unknown")} · ecom {int(item.ecommerce_score or 0)}</b>
              <span>{site_assessment}</span>
            </div>
            <div class="columns">
              <section>
                <h3>Почему подходит</h3>
                <p>{evidence}</p>
              </section>
              <section>
                <h3>Гипотеза</h3>
                <p>{hypothesis}</p>
              </section>
              <section>
                <h3>Заход</h3>
                <p>{angle}</p>
              </section>
            </div>
            <details>
              <summary>Черновик первого сообщения</summary>
              <p>{opener}</p>
            </details>
          </div>
          <aside class="lead-side">
            <div class="side-block">
              <b>Контакты</b>
              <p>{"<br>".join(contacts)}</p>
            </div>
            <form method="post" action="/web-leads/{item.id}/refresh">
              <button class="link-btn" type="submit">Обновить сайт</button>
            </form>
            <form method="post" action="/web-leads/{item.id}/crm">
              <label>Статус
                <select name="status">
                  {"".join(f"<option value='{escape(value)}' {_selected(status_value, value)}>{escape(label)}</option>" for value, label in STATUS_LABELS.items())}
                </select>
              </label>
              <label>Ответственный <input name="owner" value="{escape(item.owner or '')}"></label>
              <label>Комментарий <textarea name="comment">{escape(item.comment or '')}</textarea></label>
              <button class="primary-btn" type="submit">Сохранить</button>
            </form>
          </aside>
        </article>
        """

    result = web_job.get("last_result") or {}
    if web_job.get("running"):
        job_text = "Идет web-поиск и анализ сайтов..."
    elif web_job.get("last_error"):
        job_text = f"Ошибка: {web_job['last_error']}"
    elif result and result.get("focus_import"):
        job_text = (
            f"Фокус импортирован: совпало по ИНН {result.get('matched', 0)}, "
            f"из них ИНН {result.get('matched_by_inn', 0)}, сайт {result.get('matched_by_domain', 0)}, "
            f"email {result.get('matched_by_email', 0)}, телефон {result.get('matched_by_phone', 0)}. "
            f"Не найдено в базе {result.get('unmatched', 0)}, строк пропущено {result.get('skipped', 0)}."
        )
    elif result and "deleted" in result:
        job_text = f"Результаты очищены: удалено {result.get('deleted', 0)} компаний."
    elif result:
        job_text = (
            f"Последний сбор: {result.get('created', 0)} новых, {result.get('updated', 0)} обновлено, "
            f"{result.get('analyzed', 0)} сайтов проанализировано. Завершен: {web_job.get('last_finished_at')}"
        )
    else:
        job_text = "Web-поиск еще не запускался."

    query_params = {"status": status, "min_score": min_score, "q": q, "project_id": selected_project_id, "per_page": per_page}
    prev_params = {**query_params, "page": max(1, page - 1)}
    next_params = {**query_params, "page": page + 1}

    cards = "".join(lead_card(item) for item in items) or "<div class='empty'>Компаний под эти фильтры пока нет.</div>"

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>AdBeam ICP Finder</title>
      <style>
        :root {{ --bg:#eef3f7; --panel:#fff; --text:#0f172a; --muted:#64748b; --line:#d8e1ea; --blue:#2563eb; --green:#059669; --red:#dc2626; --amber:#b7791f; }}
        * {{ box-sizing:border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
        .page {{ max-width:1280px; margin:0 auto; padding:24px 22px 56px; }}
        a {{ color:inherit; text-decoration:none; }}
        h1 {{ margin:0; font-size:30px; letter-spacing:0; }}
        h3 {{ margin:0 0 6px; font-size:12px; color:var(--muted); text-transform:uppercase; }}
        p {{ margin:0; }}
        .topbar {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; margin-bottom:16px; }}
        .subtitle {{ color:var(--muted); max-width:760px; margin-top:6px; }}
        .nav {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
        .nav-pill, .link-btn, .primary-btn {{ min-height:36px; display:inline-flex; align-items:center; border-radius:7px; padding:0 12px; border:1px solid var(--line); background:#fff; cursor:pointer; }}
        .nav-pill.active, .primary-btn {{ background:var(--blue); border-color:var(--blue); color:#fff; font-weight:700; }}
        .metrics {{ display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:10px; margin-bottom:12px; }}
        .metric {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:12px; min-height:78px; }}
        .metric b {{ display:block; font-size:24px; }}
        .metric span {{ color:var(--muted); }}
        .search-panel {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:12px; margin-bottom:12px; }}
        .project-panel {{ display:grid; grid-template-columns:1fr 220px 150px; gap:10px; align-items:end; margin-bottom:12px; padding-bottom:12px; border-bottom:1px solid var(--line); }}
        .project-caption {{ color:var(--muted); }}
        .search-form {{ display:grid; grid-template-columns:190px 180px 1fr 96px 132px; gap:10px; align-items:end; }}
        .search-actions {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; }}
        .inline-form {{ margin:0; }}
        .template-panel {{ border-top:1px solid var(--line); margin-top:12px; padding-top:12px; }}
        .template-controls {{ display:grid; grid-template-columns:220px repeat(3, max-content); gap:8px; align-items:end; }}
        .template-editor {{ display:grid; grid-template-columns:1fr 1fr 132px; gap:10px; align-items:end; margin-top:10px; }}
        .template-editor textarea {{ min-height:130px; }}
        .template-panel details {{ border-top:0; margin-top:10px; padding-top:0; }}
        label {{ display:grid; gap:5px; color:var(--muted); font-size:12px; }}
        input, select, textarea {{ width:100%; border:1px solid #cbd5e1; border-radius:7px; padding:8px 10px; font:inherit; color:var(--text); background:#fff; }}
        input, select {{ height:36px; }}
        textarea {{ min-height:70px; resize:vertical; }}
        .danger-btn {{ min-height:36px; border-radius:7px; padding:0 12px; border:1px solid #fecaca; color:var(--red); background:#fff7f7; cursor:pointer; font:inherit; }}
        .job {{ margin-top:8px; color:var(--green); }}
        .filters {{ display:grid; grid-template-columns:190px 120px 120px 1fr 110px; gap:8px; align-items:end; margin:12px 0; }}
        .pager {{ display:flex; justify-content:space-between; align-items:center; color:var(--muted); margin:10px 0; }}
        .pager-actions {{ display:flex; gap:8px; }}
        .lead-card {{ display:grid; grid-template-columns:minmax(0,1fr) 260px; gap:14px; background:#fff; border:1px solid var(--line); border-radius:8px; padding:14px; margin-bottom:12px; }}
        .lead-topline {{ display:flex; justify-content:space-between; gap:12px; }}
        .lead-title {{ display:block; font-size:20px; font-weight:800; }}
        .domain {{ color:var(--muted); margin-top:2px; }}
        .score {{ width:52px; height:52px; display:grid; place-items:center; border-radius:8px; font-size:22px; font-weight:900; background:#eef2ff; color:#1d4ed8; flex:0 0 auto; }}
        .score.hot {{ background:#dcfce7; color:#047857; }}
        .score.warm {{ background:#fef3c7; color:#92400e; }}
        .score.cold {{ background:#e2e8f0; color:#475569; }}
        .badges {{ display:flex; flex-wrap:wrap; gap:6px; margin:10px 0; }}
        .badge {{ border:1px solid var(--line); border-radius:999px; padding:4px 9px; font-size:12px; color:#334155; }}
        .badge.ok {{ background:#ecfdf5; border-color:#bbf7d0; color:#047857; }}
        .site-check {{ border:1px solid #bbf7d0; background:#f0fdf4; color:#065f46; border-radius:7px; padding:8px 10px; margin:8px 0 12px; display:grid; gap:2px; }}
        .site-check span {{ color:#166534; }}
        .columns {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; border-top:1px solid var(--line); padding-top:12px; }}
        .columns section {{ min-width:0; }}
        details {{ margin-top:12px; border-top:1px solid var(--line); padding-top:10px; color:#334155; }}
        summary {{ cursor:pointer; color:var(--blue); font-weight:700; }}
        .lead-side {{ border-left:1px solid var(--line); padding-left:14px; }}
        .lead-side form {{ display:grid; gap:8px; }}
        .side-block {{ margin-bottom:10px; }}
        .side-block p {{ color:#334155; margin-top:4px; }}
        .empty {{ background:#fff; border:1px solid var(--line); border-radius:8px; padding:28px; color:var(--muted); }}
        .modal-overlay {{ position:fixed; inset:0; z-index:50; display:none; align-items:center; justify-content:center; padding:20px; background:rgba(15,23,42,.46); }}
        .modal-overlay.show {{ display:flex; }}
        .result-modal {{ width:min(520px,100%); background:#fff; border-radius:10px; border:1px solid var(--line); box-shadow:0 24px 70px rgba(15,23,42,.24); overflow:hidden; }}
        .modal-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:16px; padding:18px 18px 8px; }}
        .modal-head h2 {{ margin:0; font-size:22px; }}
        .modal-head p {{ color:var(--muted); margin-top:4px; }}
        .modal-close {{ width:34px; height:34px; border-radius:7px; border:1px solid var(--line); background:#fff; cursor:pointer; font-size:22px; line-height:1; }}
        .modal-stats {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; padding:12px 18px 18px; }}
        .modal-stat {{ border:1px solid var(--line); border-radius:8px; padding:10px; }}
        .modal-stat b {{ display:block; font-size:24px; }}
        .modal-stat span {{ color:var(--muted); }}
        .modal-foot {{ display:flex; justify-content:flex-end; gap:8px; border-top:1px solid var(--line); padding:12px 18px; background:#f8fafc; }}
        @media (max-width:980px) {{ .topbar, .lead-card {{ display:block; }} .nav {{ justify-content:flex-start; margin-top:12px; }} .metrics, .columns, .project-panel, .search-form, .filters, .template-controls, .template-editor {{ grid-template-columns:1fr; }} .lead-side {{ border-left:0; border-top:1px solid var(--line); padding:12px 0 0; margin-top:12px; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="topbar">
          <div>
            <h1>ICP Finder</h1>
            <div class="subtitle">Поиск в сети ICP 1 по запросам. Полуавтоматический режим.</div>
          </div>
          <nav class="nav">
            {nav_button("Web", "/web-leads", True)}
            {nav_button("TenChat", "/people-leads")}
            {nav_button("Telegram", "/telegram-signals")}
            {nav_button("Настройки TG", "/telegram-signals/settings")}
          </nav>
        </div>

        <section class="metrics">
          {metric("компаний в срезе", count_web_leads(project_id=selected_project_id or None))}
          {metric("ICP score 70+", count_web_leads(min_score=70, project_id=selected_project_id or None))}
          {metric("ICP score 45+", count_web_leads(min_score=45, project_id=selected_project_id or None))}
          {metric("готовы к контакту", count_web_leads(only_icp=True, status="new", project_id=selected_project_id or None))}
        </section>

        <section class="search-panel">
          <div class="project-panel">
            <div>
              <b>{escape("Общий пул" if not selected_project_id else (selected_project.name if selected_project else "Проект"))}</b>
              <div class="project-caption">Проект группирует поиски. Общий пул показывает всю базу без вкладок.</div>
            </div>
            <form id="new-project-form" method="post" action="/web-leads/projects" class="inline-form">
              <label>Новый проект <input name="project_name" placeholder="Проект А"></label>
            </form>
            <button class="primary-btn" type="submit" form="new-project-form">Начать новый проект</button>
          </div>
          <form id="web-search-form" method="post" action="/web-leads/search" class="search-form">
            <label>Проект
              <select name="project_id">{project_options(search_project_id, include_all=False)}</select>
            </label>
            <label>Сегмент
              <select name="preset">
                <option value="all" {_selected(selected_preset, "all")}>Все ICP1</option>
                <option value="fmcg" {_selected(selected_preset, "fmcg")}>FMCG / еда</option>
                <option value="beauty" {_selected(selected_preset, "beauty")}>Beauty / household</option>
                <option value="household" {_selected(selected_preset, "household")}>Дом / быт</option>
                <option value="kids" {_selected(selected_preset, "kids")}>Детские товары</option>
                <option value="fashion" {_selected(selected_preset, "fashion")}>Одежда</option>
                <option value="marketplace_brand" {_selected(selected_preset, "marketplace_brand")}>Бренды на MP</option>
                <option value="exhibitors" {_selected(selected_preset, "exhibitors")}>Выставки</option>
              </select>
            </label>
            <label>Свои поисковые запросы <textarea name="custom_queries" placeholder="Можно оставить пустым. По одному запросу на строку.">{escape(form_custom_queries)}</textarea></label>
            <label>Лимит <input type="number" name="total_limit" min="5" max="120" value="{form_total_limit}"></label>
            <button class="primary-btn" type="submit">Запустить поиск</button>
          </form>
          <div class="template-panel">
            <div class="template-controls">
              <label>Категория для шаблонов
                <input id="template-category" name="template_category" form="web-search-form" value="{escape(template_category)}" placeholder="косметика, снеки, посуда">
              </label>
              <button class="link-btn" type="button" data-template-mode="exhibition">Выставки</button>
              <button class="link-btn" type="button" data-template-mode="category">Категория</button>
              <button class="link-btn" type="button" data-template-mode="both">Категория + выставки</button>
            </div>
            <details>
              <summary>Редактировать шаблоны поиска</summary>
              <form method="post" action="/web-leads/query-templates" class="template-editor">
                <label>Выставочные запросы
                  <textarea name="exhibition_templates">{escape(exhibition_templates_text)}</textarea>
                </label>
                <label>Шаблоны с категорией
                  <textarea name="category_templates">{escape(category_templates_text)}</textarea>
                </label>
                <button class="primary-btn" type="submit">Сохранить</button>
              </form>
            </details>
          </div>
          <div class="search-actions">
            <a class="link-btn" href="/web-leads/export?project_id={selected_project_id}">Excel: текущий срез</a>
            <a class="link-btn" href="/web-leads/export">Excel: общий пул</a>
            <a class="link-btn" href="/web-leads/export-merged?project_id={selected_project_id}">Excel: объединенный короткий</a>
            <a class="link-btn" href="/web-leads/export-inn?project_id={selected_project_id}">ИНН: текущий срез</a>
            <a class="link-btn" href="/web-leads/export-inn">ИНН: общий пул</a>
            <form method="post" action="/web-leads/import-focus?project_id={selected_project_id}" enctype="multipart/form-data" class="inline-form">
              <label>Фокус/Компас Excel <input type="file" name="file" accept=".xlsx,.xlsm,.csv"></label>
              <button class="link-btn" type="submit">Объединить</button>
            </form>
            <form method="post" action="/web-leads/clear" class="inline-form" onsubmit="return confirm('Очистить все web-результаты?')">
              <button class="danger-btn" type="submit">Очистить результаты</button>
            </form>
          </div>
          <div class="job" id="job-text">{escape(job_text)}</div>
        </section>

        <form method="get" action="/web-leads" class="filters">
          <label>Проект <select name="project_id">
            {project_options(selected_project_id, include_all=True)}
          </select></label>
          <label>Score от <input type="number" name="min_score" value="{int(min_score or 0)}" min="0" max="100"></label>
          <label>Статус <select name="status">
            <option value="">Все</option>
            {"".join(f"<option value='{escape(value)}' {_selected(status, value)}>{escape(label)}</option>" for value, label in STATUS_LABELS.items())}
          </select></label>
          <label>Поиск <input name="q" value="{escape(q or '')}" placeholder="домен, название, причина"></label>
          <button class="primary-btn" type="submit">Фильтр</button>
        </form>

        <div class="pager">
          <span>Страница {page}. Показано {len(items)}.</span>
          <div class="pager-actions">
            <a class="link-btn" href="/web-leads?{escape(urlencode(prev_params))}">Назад</a>
            <a class="link-btn" href="/web-leads?{escape(urlencode(next_params))}">Вперед</a>
          </div>
        </div>
        {cards}
      </main>
      <div class="modal-overlay" id="job-modal" aria-hidden="true">
        <section class="result-modal" role="dialog" aria-modal="true" aria-labelledby="job-modal-title">
          <div class="modal-head">
            <div>
              <h2 id="job-modal-title">Поиск закончен</h2>
              <p id="job-modal-subtitle">Свежие результаты уже сохранены в базе.</p>
            </div>
            <button class="modal-close" type="button" aria-label="Закрыть" id="job-modal-close">×</button>
          </div>
          <div class="modal-stats" id="job-modal-stats"></div>
          <div class="modal-foot">
            <a class="link-btn" href="/web-leads/export?project_id={selected_project_id}">Excel</a>
            <button class="primary-btn" type="button" id="job-modal-refresh">Показать результаты</button>
          </div>
        </section>
      </div>
      <script>
        const queryTemplates = {query_templates_json};
        function uniqueLines(lines) {{
          const seen = new Set();
          const result = [];
          for (const raw of lines) {{
            const value = String(raw || '').trim();
            const key = value.toLowerCase();
            if (!value || seen.has(key)) continue;
            seen.add(key);
            result.push(value);
          }}
          return result;
        }}
        function renderCategoryQueries() {{
          const category = (document.getElementById('template-category')?.value || '').trim();
          if (!category) return [];
          return (queryTemplates.category_templates || [])
            .map((template) => String(template || '').replaceAll('[категория]', category).trim())
            .filter(Boolean);
        }}
        document.querySelectorAll('[data-template-mode]').forEach((button) => {{
          button.addEventListener('click', () => {{
            const textarea = document.querySelector('textarea[name="custom_queries"]');
            if (!textarea) return;
            const mode = button.getAttribute('data-template-mode');
            let lines = [];
            if (mode === 'exhibition') lines = queryTemplates.exhibition_templates || [];
            if (mode === 'category') lines = renderCategoryQueries();
            if (mode === 'both') lines = [...renderCategoryQueries(), ...(queryTemplates.exhibition_templates || [])];
            textarea.value = uniqueLines(lines).join('\\n');
            textarea.focus();
          }});
        }});
        function showJobModal(job) {{
          const modal = document.getElementById('job-modal');
          const title = document.getElementById('job-modal-title');
          const subtitle = document.getElementById('job-modal-subtitle');
          const stats = document.getElementById('job-modal-stats');
          if (!modal || !title || !subtitle || !stats) return;
          const r = job.last_result || {{}};
          if (job.last_error) {{
            title.textContent = 'Поиск остановился';
            subtitle.textContent = job.last_error || 'Ошибка во время сбора.';
            stats.innerHTML = '';
          }} else {{
            title.textContent = 'Поиск закончен';
            subtitle.textContent = `Готово: ${{job.last_finished_at || 'только что'}}. Закрой окно, чтобы обновить выдачу.`;
            const rows = [
              ['Новых', r.created || 0],
              ['Обновлено', r.updated || 0],
              ['Проанализировано сайтов', r.analyzed || 0],
              ['Оставлено в базе', r.kept || 0],
              ['Кандидатов из поиска', r.candidates || 0],
              ['Пропущено', r.skipped || 0],
            ];
            stats.innerHTML = rows.map(([label, value]) => `<div class="modal-stat"><b>${{value}}</b><span>${{label}}</span></div>`).join('');
          }}
          modal.classList.add('show');
          modal.setAttribute('aria-hidden', 'false');
        }}
        function closeJobModal() {{
          window.location.reload();
        }}
        document.getElementById('job-modal-close')?.addEventListener('click', closeJobModal);
        document.getElementById('job-modal-refresh')?.addEventListener('click', closeJobModal);
        document.getElementById('job-modal')?.addEventListener('click', (event) => {{
          if (event.target && event.target.id === 'job-modal') closeJobModal();
        }});
        document.addEventListener('keydown', (event) => {{
          if (event.key === 'Escape' && document.getElementById('job-modal')?.classList.contains('show')) {{
            closeJobModal();
          }}
        }});
        let webJobWasRunning = false;
        async function pollJob() {{
          try {{
            const response = await fetch('/web-leads/job-status', {{ cache: 'no-store' }});
            const job = await response.json();
            const el = document.getElementById('job-text');
            if (!el) return;
            if (job.running) {{
              webJobWasRunning = true;
              el.textContent = 'Идет web-поиск и анализ сайтов...';
              setTimeout(pollJob, 2500);
              return;
            }}
            if (webJobWasRunning) {{
              showJobModal(job);
              webJobWasRunning = false;
              return;
            }}
            if (job.last_result) {{
              const r = job.last_result;
              el.textContent = `Последний сбор: ${{r.created || 0}} новых, ${{r.updated || 0}} обновлено, ${{r.analyzed || 0}} сайтов проанализировано.`;
            }}
          }} catch (e) {{}}
        }}
        pollJob();
      </script>
    </body>
    </html>
    """


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
    if "/web-leads" in referer or "/people-leads" in referer or "/telegram-signals" in referer:
        return referer
    return "/web-leads"


def _action_form(
    signal_id: int,
    label: str,
    status: str | None = None,
    review_status: str | None = None,
    reject_reason: str | None = None,
    tone: str = "ghost",
) -> str:
    params = {}
    if status:
        params["status"] = status
    if review_status:
        params["review_status"] = review_status
    if reject_reason:
        params["reject_reason"] = reject_reason
    action = f"/telegram-signals/{signal_id}/status"
    if params:
        action += "?" + urlencode(params)
    return (
        f"<form method='post' action='{escape(action)}' class='inline-form'>"
        f"<button type='submit' class='btn {tone}'>{escape(label)}</button>"
        "</form>"
    )


def _count_signals(**kwargs) -> int:
    return count_signals(lead_fit_in=WORKING_LEAD_FITS, **kwargs)


def _lines(value: str | None) -> list[str]:
    return [line.strip() for line in (value or "").splitlines() if line.strip()]


def _profile_config(profile) -> dict:
    return {
        "queries": _lines(profile.queries_text),
        "source_chats": _lines(getattr(profile, "source_chats_text", "")),
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

    working_before = count_signals(lead_fit_in=WORKING_LEAD_FITS)
    raw_before = count_signals()

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
        working_after = count_signals(lead_fit_in=WORKING_LEAD_FITS)
        raw_after = count_signals()
        result["created_working"] = max(0, working_after - working_before)
        result["created_raw"] = max(0, raw_after - raw_before)
        result["total_working"] = working_after
        result["total_raw"] = raw_after
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


@app.post("/telegram-signals/reclassify")
def reclassify_from_dashboard(request: Request):
    result = reclassify_existing_signals()
    with JOB_LOCK:
        DASHBOARD_JOB.update(
            {
                "running": False,
                "last_started_at": None,
                "last_finished_at": format_msk(datetime.utcnow()),
                "last_error": None,
                "last_result": {
                    "created": 0,
                    "created_working": 0,
                    "updated": result.get("updated", 0),
                    "scanned_messages": 0,
                    "total_raw": count_signals(),
                },
            }
        )
    return RedirectResponse(_return_url(request), status_code=303)


@app.get("/telegram-signals/job-status")
def telegram_signals_job_status():
    with JOB_LOCK:
        job = dict(DASHBOARD_JOB)
    return JSONResponse(job)


@app.post("/telegram-signals/{signal_id}/status")
def update_signal_status(
    signal_id: int,
    request: Request,
    status: str | None = None,
    review_status: str | None = None,
    reject_reason: str | None = None,
):
    if status:
        set_signal_status(signal_id, status, review_status=review_status, reject_reason=reject_reason)
    elif review_status:
        set_signal_review_status(signal_id, review_status, reject_reason=reject_reason)
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
        reject_reason=str(form.get("reject_reason") or "") or None,
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
            "source_chats_text": str(form.get("source_chats_text") or "").strip(),
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

    def render_profile_form(profile=None) -> str:
        is_new = profile is None
        profile_id = "" if is_new else f'<input type="hidden" name="profile_id" value="{profile.id}">'
        current_segment = "ecom_marketplace_pain" if is_new else profile.segment
        name = "Маркетплейсы: явная боль" if is_new else profile.name or ""
        queries = "\n".join(CHAT_DISCOVERY_KEYWORDS.get(current_segment, [])) if is_new else profile.queries_text or ""
        source_chats = "" if is_new else getattr(profile, "source_chats_text", None) or ""
        stop_words = "" if is_new else profile.stop_words_text or ""
        good_hints = "\n".join(CHAT_GOOD_HINTS) if is_new else profile.good_chat_hints_text or ""
        bad_hints = "\n".join(CHAT_BAD_HINTS) if is_new else profile.bad_chat_hints_text or ""
        max_age_hours = 96 if is_new else profile.max_age_hours
        limit_chats = 10 if is_new else profile.limit_chats
        limit_messages = 80 if is_new else profile.limit_messages_per_chat
        min_score = 35 if is_new else profile.min_score
        is_active = True if is_new else bool(profile.is_active)
        title = "Новый профиль" if is_new else escape(name)
        submit_label = "Создать профиль" if is_new else "Сохранить"
        active_label = "Включен" if is_active else "Выключен"

        return f"""
        <article class="profile-card">
          <form method="post" action="/telegram-signals/settings">
            {profile_id}
            <div class="profile-head">
              <div>
                <div class="eyebrow">{escape(active_label)}</div>
                <h2>{title}</h2>
              </div>
              <button class="primary-btn" type="submit">{submit_label}</button>
            </div>

            <div class="compact-grid">
              <label>Название <input name="name" value="{escape(name)}"></label>
              <label>Сегмент <select name="segment">
                {"".join(f"<option value='{escape(value)}' {_selected(current_segment, value)}>{escape(label)}</option>" for value, label in SEGMENT_LABELS.items())}
              </select></label>
              <label>Мин. score <input type="number" name="min_score" value="{min_score}" min="0" max="100"></label>
              <label>Период, ч <input type="number" name="max_age_hours" value="{max_age_hours}" min="1"></label>
              <label>Чатов <input type="number" name="limit_chats" value="{limit_chats}" min="1"></label>
              <label>Сообщений <input type="number" name="limit_messages_per_chat" value="{limit_messages}" min="1"></label>
              <label class="check"><input type="checkbox" name="is_active" {_checked(is_active)}> Активен</label>
            </div>

            <div class="signal-rule">
              <b>Рабочий сигнал:</b> ICP + потолок текущей модели / ухудшение экономики / безопасный следующий шаг.
              <span>Сбор ищет и источники, и сами сообщения по болевым фразам: CAC, ДРР, потолок MP, direct, внешний трафик, сайт.</span>
            </div>

            <div class="textarea-grid">
              <label>Темы для поиска источников <textarea name="queries_text" spellcheck="false">{escape(queries)}</textarea></label>
              <label>Конкретные чаты / каналы <textarea name="source_chats_text" spellcheck="false" placeholder="@chatname&#10;https://t.me/chatname&#10;chatname">{escape(source_chats)}</textarea></label>
              <label>Минус-слова сообщений <textarea name="stop_words_text" spellcheck="false" placeholder="новость&#10;вебинар&#10;вакансия&#10;мне кажется&#10;кейсы по выбору ниши">{escape(stop_words)}</textarea></label>
              <label>Плюс-слова в названии чата <textarea name="good_chat_hints_text" spellcheck="false">{escape(good_hints)}</textarea></label>
              <label>Минус-слова в названии чата <textarea name="bad_chat_hints_text" spellcheck="false">{escape(bad_hints)}</textarea></label>
            </div>
          </form>
        </article>
        """

    profile_cards = []
    for profile in profiles:
        profile_cards.append(render_profile_form(profile))

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Настройки поиска</title>
      <style>
        :root {{ --bg: #f4f7fb; --panel: #fff; --text: #17202a; --muted: #667085; --line: #d9e2ec; --blue: #2563eb; --green: #0f9f6e; }}
        * {{ box-sizing: border-box; }}
        body {{ margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; }}
        .page {{ max-width: 1320px; margin: 0 auto; padding: 22px 20px 44px; }}
        a {{ color: var(--blue); text-decoration: none; }}
        h1 {{ margin: 0; font-size: 28px; letter-spacing: 0; }}
        h2 {{ margin: 2px 0 0; font-size: 18px; letter-spacing: 0; }}
        .subtitle {{ color: var(--muted); margin-top: 6px; max-width: 820px; }}
        .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 14px; }}
        .top-actions {{ display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }}
        .link-btn, .primary-btn {{ min-height: 36px; display: inline-flex; align-items: center; border-radius: 7px; padding: 0 12px; font: inherit; cursor: pointer; white-space: nowrap; }}
        .link-btn {{ border: 1px solid var(--line); background: #fff; color: #344054; }}
        .primary-btn {{ border: 0; background: var(--blue); color: #fff; font-weight: 700; }}
        .preset-row {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; margin-bottom: 12px; }}
        .preset {{ background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 12px; }}
        .preset b {{ display: block; margin-bottom: 3px; }}
        .preset span {{ color: var(--muted); font-size: 12px; }}
        .profile-card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; margin-bottom: 12px; box-shadow: 0 1px 2px rgba(16, 24, 40, .04); }}
        .profile-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }}
        .eyebrow {{ color: var(--green); font-size: 11px; font-weight: 800; text-transform: uppercase; }}
        .compact-grid {{ display: grid; grid-template-columns: minmax(220px, 1.4fr) minmax(190px, 1fr) 96px 96px 88px 112px 104px; gap: 8px; align-items: end; }}
        .textarea-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-top: 10px; }}
        .signal-rule {{ display: flex; flex-wrap: wrap; gap: 8px 12px; align-items: center; border: 1px solid #bbf7d0; background: #f0fdf4; color: #166534; border-radius: 8px; padding: 8px 10px; font-size: 13px; }}
        .signal-rule span {{ color: #47715a; }}
        label {{ display: grid; gap: 5px; color: var(--muted); font-size: 12px; }}
        input, select, textarea {{ width: 100%; border: 1px solid #cbd5e1; border-radius: 7px; padding: 8px 10px; font: inherit; color: var(--text); background: #fff; }}
        input, select {{ height: 36px; }}
        textarea {{ min-height: 118px; resize: vertical; line-height: 1.35; }}
        .check {{ display: flex; align-items: center; gap: 8px; height: 36px; color: var(--text); }}
        .check input {{ width: 16px; height: 16px; }}
        @media (max-width: 1180px) {{ .compact-grid, .textarea-grid, .preset-row {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
        @media (max-width: 760px) {{ .topbar {{ display: block; }} .top-actions {{ justify-content: flex-start; margin-top: 12px; }} .compact-grid, .textarea-grid, .preset-row {{ grid-template-columns: 1fr; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="topbar">
          <div>
            <h1>Настройка поиска сигналов</h1>
            <div class="subtitle">Сбор теперь работает в два контура: ищет источники по темам и отдельно ищет сами сообщения по болевым формулировкам. В рабочую базу попадают только тексты, где классификатор видит ICP, боль и повод для аккуратного outreach.</div>
          </div>
          <div class="top-actions">
            <a class="link-btn" href="/web-leads">Web</a>
            <a class="link-btn" href="/people-leads">TenChat</a>
            <a class="link-btn" href="/telegram-signals">Telegram</a>
            <a class="link-btn" href="/telegram-signals">База лидов</a>
            <a class="link-btn" href="/telegram-signals?view=raw">Сырье</a>
          </div>
        </div>
        <section class="preset-row">
          <div class="preset"><b>1. Источники</b><span>Запросы помогают найти чаты селлеров, брендов и интернет-магазинов, но больше не являются единственной точкой входа.</span></div>
          <div class="preset"><b>2. Боль</b><span>Параллельно идет поиск сообщений по CJM-симптомам: потолок MP, рост ДРР/CAC, страх слить бюджет, direct, сайт, внешний трафик.</span></div>
          <div class="preset"><b>3. Отсев</b><span>Новости, рассуждения, кейсы, вакансии, обучение и поставщики режутся минус-словами и скорингом.</span></div>
        </section>
        {"".join(profile_cards)}
        {render_profile_form()}
      </main>
    </body>
    </html>
    """


@app.get("/telegram-signals/analytics", response_class=HTMLResponse)
def telegram_signals_analytics():
    reject_stats = get_reject_reason_stats()
    source_stats = get_source_quality_stats(limit=10)

    total_raw = count_signals()
    total_working = count_signals(lead_fit_in=WORKING_LEAD_FITS)
    total_ok = count_signals(review_status="ok")
    total_not_ok = count_signals(review_status="not_ok")
    total_unchecked = count_signals(review_status="unchecked")

    reject_rows = "".join(
        f"""
        <tr>
          <td><a href="/telegram-signals?review_status=not_ok&reject_reason={escape(str(row['reason']))}">{escape(REJECT_REASON_LABELS.get(row['reason'], row['reason']))}</a></td>
          <td>{row['total']}</td>
          <td>{row['avg_score']}</td>
        </tr>
        """
        for row in reject_stats
    )
    source_rows = "".join(
        f"""
        <tr>
          <td>
            <div class="source-title">{escape(row['chat_title'])}</div>
            {f"<a href='https://t.me/{escape(str(row['chat_username']).lstrip('@'))}' target='_blank'>@{escape(str(row['chat_username']).lstrip('@'))}</a>" if row['chat_username'] else ""}
          </td>
          <td>{row['total']}</td>
          <td>{row['working']}</td>
          <td>{row['ok']}</td>
          <td>{row['not_ok']}</td>
          <td>{row['unchecked']}</td>
          <td>{row['reachable']}</td>
          <td>{row['avg_score']}</td>
          <td>{row['ok_rate']}%</td>
          <td>{row['reject_rate']}%</td>
        </tr>
        """
        for row in source_stats
    )

    return f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Аналитика Telegram Signals</title>
      <style>
        :root {{ --bg:#eef3f7; --panel:#fff; --text:#17202a; --muted:#667085; --line:#d9e2ec; --blue:#2563eb; --green:#0f9f6e; --red:#d92d20; }}
        * {{ box-sizing: border-box; }}
        body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.45 Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif; }}
        .page {{ max-width:1180px; margin:0 auto; padding:22px 18px 44px; }}
        .top {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:16px; }}
        h1 {{ margin:0; font-size:26px; letter-spacing:0; }}
        .subtitle {{ color:var(--muted); margin-top:5px; }}
        .links {{ display:flex; gap:8px; flex-wrap:wrap; }}
        a.button {{ min-height:34px; display:inline-flex; align-items:center; border:1px solid var(--line); border-radius:7px; background:#fff; color:#344054; padding:0 12px; text-decoration:none; }}
        .stats {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; margin-bottom:14px; }}
        .stat {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:12px; }}
        .stat b {{ display:block; font-size:22px; }}
        .stat span {{ color:var(--muted); font-size:12px; }}
        .grid {{ display:grid; grid-template-columns:minmax(300px,.75fr) minmax(0,1.25fr); gap:14px; align-items:start; }}
        section {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }}
        h2 {{ margin:0 0 10px; font-size:18px; }}
        table {{ width:100%; border-collapse:collapse; }}
        th,td {{ padding:8px 9px; border-bottom:1px solid #eef2f7; text-align:left; vertical-align:top; }}
        th {{ color:var(--muted); font-size:12px; font-weight:700; }}
        td a {{ color:var(--blue); text-decoration:none; }}
        .source-title {{ font-weight:700; }}
        .empty {{ color:var(--muted); padding:14px 0; }}
        @media (max-width:900px) {{ .top {{ display:block; }} .links {{ margin-top:10px; }} .stats,.grid {{ grid-template-columns:1fr; }} }}
      </style>
    </head>
    <body>
      <main class="page">
        <div class="top">
          <div>
            <h1>Аналитика качества</h1>
            <div class="subtitle">Смотрим, почему отбраковываем сигналы и какие источники реально дают лидов.</div>
          </div>
          <div class="links">
            <a class="button" href="/web-leads">Web</a>
            <a class="button" href="/people-leads">TenChat</a>
            <a class="button" href="/telegram-signals">Telegram</a>
            <a class="button" href="/telegram-signals">База лидов</a>
            <a class="button" href="/telegram-signals/settings">Настройки</a>
          </div>
        </div>

        <div class="stats">
          <div class="stat"><b>{total_raw}</b><span>всего сигналов</span></div>
          <div class="stat"><b>{total_working}</b><span>в рабочей базе</span></div>
          <div class="stat"><b>{total_ok}</b><span>ОК</span></div>
          <div class="stat"><b>{total_not_ok}</b><span>Не ОК</span></div>
          <div class="stat"><b>{total_unchecked}</b><span>не разобрано</span></div>
        </div>

        <div class="grid">
          <section>
            <h2>Причины Не ОК</h2>
            {f"<table><thead><tr><th>Причина</th><th>Кол-во</th><th>Avg score</th></tr></thead><tbody>{reject_rows}</tbody></table>" if reject_rows else "<div class='empty'>Пока нет размеченных причин.</div>"}
          </section>
          <section>
            <h2>Качество источников</h2>
            {f"<table><thead><tr><th>Источник</th><th>Всего</th><th>Раб.</th><th>ОК</th><th>Не ОК</th><th>?</th><th>Конт.</th><th>Score</th><th>OK%</th><th>Reject%</th></tr></thead><tbody>{source_rows}</tbody></table>" if source_rows else "<div class='empty'>Источников пока нет.</div>"}
          </section>
        </div>
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
    reject_reason: str = "",
    cjm_stage: str = "",
    lead_category: str = "",
    view: str = "work",
    hot: bool = False,
    page: int = 1,
    per_page: int = 50,
):
    score = 80 if hot and min_score < 80 else min_score
    per_page = per_page if per_page in {10, 50, 200} else 50
    page = max(1, page)
    is_raw_view = view == "raw"
    if is_raw_view:
        lead_fit_filter = None
    elif view == "nurture":
        lead_fit_filter = ["nurture"]
    elif view == "hot":
        lead_fit_filter = ["hot_outreach", "target"]
    elif view == "hypothesis":
        lead_fit_filter = ["warm_hypothesis", "warm_reply", "review"]
    else:
        lead_fit_filter = WORKING_LEAD_FITS
    filter_kwargs = {
        "lead_fit_in": lead_fit_filter,
        "min_score": score or None,
        "marketplace": marketplace or None,
        "niche": niche or None,
        "status": status or None,
        "crm_tag": crm_tag or None,
        "review_status": review_status or None,
        "reject_reason": reject_reason or None,
        "cjm_stage": cjm_stage or None,
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
    active_count = count_signals(lead_fit_in=WORKING_LEAD_FITS, status="new") + count_signals(lead_fit_in=WORKING_LEAD_FITS, status="reviewed")
    hot_count = sum(1 for item in items if (item.lead_score_100 or 0) >= 80)
    avg_score = round(sum((item.lead_score_100 or 0) for item in items) / len(items)) if items else 0

    cards = []
    for item in items:
        score_value = item.lead_score_100 or 0
        score_class = "score-hot" if score_value >= 80 else "score-mid" if score_value >= 60 else "score-low"
        message_link = _message_link(item)
        contact_link = _contact_link(item)
        text = escape(_short(item.text_excerpt or item.message_text, 520))
        opener = escape(_short(item.best_reply_draft or item.opener_expert or item.opener_soft or item.recommended_opener, 520))
        category = CATEGORY_LABELS.get(item.lead_category or "", item.lead_category or "Не определено")
        lead_fit_label = LEAD_FIT_LABELS.get(item.lead_fit or "", item.lead_fit or "-")
        status_label = STATUS_LABELS.get(item.status or "new", item.status or "Новый")
        review_label = REVIEW_LABELS.get(item.review_status or "unchecked", item.review_status or "Не разобран")
        reject_reason_label = REJECT_REASON_LABELS.get(item.reject_reason or "", item.reject_reason or "")
        cjm_stage_label = CJM_STAGE_LABELS.get(item.cjm_stage or "", item.cjm_stage or "")
        selected_tags = set(_split_tags(item.crm_tag))
        tag_labels = [CRM_TAG_LABELS.get(tag, tag) for tag in selected_tags]
        tag_label = ", ".join(tag_labels) if tag_labels else "Без тега"
        author = item.author_name or item.author_username or "Без имени"
        why_actionable = escape(_short(item.why_actionable or "", 360))

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
            _action_form(item.id, "Написал", status="contacted", review_status="ok", tone="primary"),
            _action_form(item.id, "Ответил", status="replied", review_status="ok"),
            _action_form(item.id, "Архив", status="dead", review_status="not_ok"),
        ]
        more_actions = [
            _action_form(item.id, "Прочитал", status="reviewed", review_status="ok"),
            _action_form(item.id, "Теплый", status="warm", review_status="ok"),
            _action_form(item.id, "Встреча", status="meeting_booked", review_status="ok"),
            _action_form(item.id, "Продажа", status="sale", review_status="ok"),
        ]
        reject_actions = "".join(
            _action_form(item.id, label, review_status="not_ok", reject_reason=reason, tone="danger")
            for reason, label in REJECT_REASON_LABELS.items()
        )
        review_form_options = "".join(
            f"<option value='{escape(value)}' {_selected(item.review_status or 'unchecked', value)}>{escape(label)}</option>"
            for value, label in REVIEW_LABELS.items()
        )
        reject_reason_options = "<option value=''>Без причины</option>" + "".join(
            f"<option value='{escape(value)}' {_selected(item.reject_reason or '', value)}>{escape(label)}</option>"
            for value, label in REJECT_REASON_LABELS.items()
        )
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
                {f"<span>{escape(reject_reason_label)}</span>" if reject_reason_label else ""}
                {f"<span>{escape(cjm_stage_label)}</span>" if cjm_stage_label else ""}
                <span>{escape(item.marketplace or "MP не указан")}</span>
                <span>{escape(item.niche or "ниша не указана")}</span>
                <span>{escape(item.likely_icp or "ICP unknown")}</span>
                <span>{escape(category)}</span>
                <span>{escape(lead_fit_label)}</span>
                <span>{escape(item.bridge_to_offer or "no_bridge")}</span>
              </div>
              {f"<div class='why-line'>{why_actionable}</div>" if why_actionable else ""}

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
                <details class="more-menu">
                  <summary>Еще</summary>
                  <div class="more-actions">{"".join(more_actions)}</div>
                </details>
                <details class="reject-menu">
                  <summary>Не ОК: причина</summary>
                  <div class="reject-actions">{reject_actions}</div>
                </details>
                <div class="links">{"".join(links)}</div>
              </div>
              <form method="post" action="/telegram-signals/{item.id}/crm" class="crm-form">
                <label>Статус
                  <select name="status">
                    {"".join(f"<option value='{escape(value)}' {_selected(item.status or 'new', value)}>{escape(label)}</option>" for value, label in STATUS_LABELS.items())}
                  </select>
                </label>
                <div class="tag-dropdown">
                  <span>Теги</span>
                  <details>
                    <summary>{escape(tag_label)}</summary>
                    <div class="tag-menu">
                      {"".join(f"<label class='tag-option'><input type='checkbox' name='crm_tag' value='{escape(value)}' {_checked(value in selected_tags)}> {escape(label)}</label>" for value, label in CRM_TAG_LABELS.items())}
                    </div>
                  </details>
                </div>
                <label>Разбор
                  <select name="review_status">{review_form_options}</select>
                </label>
                <label>Причина Не ОК
                  <select name="reject_reason">{reject_reason_options}</select>
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
    reject_reason_filter_options = "".join(
        f"<option value='{escape(value)}' {_selected(reject_reason, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *REJECT_REASON_LABELS.items()]
    )
    cjm_stage_options = "".join(
        f"<option value='{escape(value)}' {_selected(cjm_stage, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *CJM_STAGE_LABELS.items()]
    )
    category_options = "".join(
        f"<option value='{escape(value)}' {_selected(lead_category, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *CATEGORY_LABELS.items()]
    )
    tag_options = "".join(
        f"<option value='{escape(value)}' {_selected(crm_tag, value)}>{escape(label)}</option>"
        for value, label in [("", "Все"), *CRM_TAG_LABELS.items()]
    )
    view_hidden = f"<input type='hidden' name='view' value='{escape(view)}'>" if view in {"raw", "nurture", "hot", "hypothesis"} else ""
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
        "reject_reason": reject_reason,
        "cjm_stage": cjm_stage,
        "lead_category": lead_category,
        "view": view if view in {"raw", "nurture", "hot", "hypothesis"} else "",
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
            f"Последний сбор: +{result.get('created', 0)} новых всего, "
            f"+{result.get('created_working', 0)} в рабочую базу, "
            f"{result.get('updated', 0)} обновлено, "
            f"{result.get('scanned_messages', 0)} сообщений. "
            f"Сырья в базе: {result.get('total_raw', 0)}. "
            f"Завершен: {job.get('last_finished_at') or '-'}"
        )
        job_class = "job-ok"
    else:
        job_text = "Сбор еще не запускался из dashboard."
        job_class = "job-idle"

    quick_links = [
        ("Рабочие", "/telegram-signals", count_signals(lead_fit_in=WORKING_LEAD_FITS)),
        ("Горячие", "/telegram-signals?view=hot", count_signals(lead_fit_in=["hot_outreach", "target"])),
        ("Гипотезы", "/telegram-signals?view=hypothesis", count_signals(lead_fit_in=["warm_hypothesis", "warm_reply", "review"])),
        ("На проверку", "/telegram-signals?review_status=unchecked", _count_signals(review_status="unchecked")),
        ("Наблюдать", "/telegram-signals?view=nurture", count_signals(lead_fit_in=["nurture"])),
        ("ОК написать", "/telegram-signals?review_status=ok&status=new", _count_signals(review_status="ok", status="new")),
        ("Написал", "/telegram-signals?status=contacted", _count_signals(status="contacted")),
        ("Ответили", "/telegram-signals?status=replied", _count_signals(status="replied")),
        ("Встречи", "/telegram-signals?status=meeting_booked", _count_signals(status="meeting_booked")),
        ("Сырье", "/telegram-signals?view=raw", count_signals()),
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
                ("Наблюдать", "/telegram-signals?view=nurture", count_signals(lead_fit_in=["nurture"])),
                ("ОК", "/telegram-signals?review_status=ok", _count_signals(review_status="ok")),
                ("Не ОК", "/telegram-signals?review_status=not_ok", _count_signals(review_status="not_ok")),
                ("Сырье", "/telegram-signals?view=raw", count_signals()),
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
          grid-template-columns: 80px 120px 120px 120px 130px 150px 150px 1fr 110px auto;
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
        .secondary-btn {{
          height: 40px;
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          background: #fff;
          color: #344054;
          padding: 0 12px;
          font: inherit;
          cursor: pointer;
          white-space: nowrap;
        }}
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
        .why-line {{
          border: 1px solid #bbf7d0;
          background: #f0fdf4;
          color: #166534;
          border-radius: 7px;
          padding: 8px 10px;
          margin: -2px 0 12px;
          font-size: 13px;
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
        .actions, .links, .reject-actions, .more-actions {{ display: flex; flex-wrap: wrap; gap: 8px; }}
        .inline-form {{ display: inline; margin: 0; }}
        .reject-menu, .more-menu {{
          position: relative;
        }}
        .reject-menu {{ margin-left: auto; }}
        .reject-menu summary, .more-menu summary {{
          min-height: 34px;
          display: inline-flex;
          align-items: center;
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          background: #fff;
          color: #344054;
          padding: 0 10px;
          cursor: pointer;
          white-space: nowrap;
        }}
        .reject-menu summary {{
          border-color: #fecdd3;
          background: #fff1f3;
          color: var(--red);
        }}
        .reject-actions, .more-actions {{
          position: absolute;
          z-index: 30;
          right: 0;
          top: 40px;
          width: 280px;
          border: 1px solid #cbd5e1;
          border-radius: 8px;
          background: #fff;
          box-shadow: 0 12px 28px rgba(16, 24, 40, .16);
          padding: 8px;
        }}
        .reject-actions {{ width: 320px; border-color: #fecdd3; }}
        .crm-form {{
          display: grid;
          grid-template-columns: 140px 160px 130px 170px minmax(260px, 1fr) auto;
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
        .tag-dropdown {{
          display: grid;
          gap: 5px;
          color: var(--muted);
          font-size: 12px;
        }}
        .tag-dropdown details {{
          position: relative;
        }}
        .tag-dropdown summary {{
          display: flex;
          align-items: center;
          min-height: 38px;
          border: 1px solid #cbd5e1;
          border-radius: 7px;
          background: #fff;
          color: var(--text);
          padding: 0 10px;
          cursor: pointer;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }}
        .tag-menu {{
          position: absolute;
          z-index: 20;
          top: 42px;
          left: 0;
          width: 240px;
          max-height: 220px;
          overflow: auto;
          border: 1px solid #cbd5e1;
          border-radius: 8px;
          background: #fff;
          box-shadow: 0 12px 28px rgba(16, 24, 40, .16);
          padding: 8px;
        }}
        .tag-option {{
          display: flex;
          align-items: center;
          gap: 8px;
          min-height: 30px;
          padding: 4px 6px;
          border-radius: 6px;
          color: #344054;
          font-size: 13px;
        }}
        .tag-option:hover {{ background: #f2f4f7; }}
        .tag-option input {{ width: 15px; height: 15px; }}
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
        .shell {{ display: block; min-height: 100vh; }}
        .sidebar {{ display: none; }}
        .page {{ max-width: 1180px; padding: 18px 18px 40px; }}
        .hero {{ align-items: center; margin-bottom: 12px; }}
        h1 {{ font-size: 24px; }}
        .subtitle {{ margin-top: 4px; font-size: 13px; }}
        .top-links {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }}
        .stats {{ min-width: 0; grid-template-columns: repeat(4, minmax(88px, 1fr)); gap: 8px; }}
        .stat {{ padding: 9px 10px; }}
        .stat b {{ font-size: 18px; }}
        .toolbar {{ padding: 10px; margin-bottom: 10px; align-items: center; }}
        .collect-form {{ display: flex; gap: 8px; align-items: center; }}
        .collect-form select {{ width: 220px; margin-right: 0; }}
        .collect-btn, .secondary-btn {{ height: 36px; }}
        .quick-nav {{ gap: 6px; margin-bottom: 10px; }}
        .quick-link {{ min-height: 30px; padding: 0 9px; font-size: 13px; }}
        .exports {{ margin-bottom: 10px; }}
        .export-link {{ min-height: 30px; padding: 0 9px; font-size: 13px; }}
        .filters {{ position: static; grid-template-columns: 80px 100px 110px 110px 120px 130px 130px minmax(150px, 1fr) 100px auto; gap: 8px; padding: 10px 0; margin-bottom: 10px; background: transparent; border-bottom: 1px solid var(--line); backdrop-filter: none; }}
        .filters label:nth-of-type(4), .filters label:nth-of-type(9) {{ display: none; }}
        input, select {{ height: 34px; }}
        .filter-btn {{ height: 34px; min-height: 34px; }}
        .pagination {{ margin-bottom: 10px; font-size: 13px; }}
        .lead-list {{ gap: 10px; }}
        .lead-card {{ padding: 12px; box-shadow: none; }}
        h2 {{ font-size: 18px; }}
        .score {{ width: 46px; height: 36px; font-size: 17px; }}
        .badges {{ margin: 8px 0; gap: 5px; }}
        .badges span {{ padding: 3px 7px; }}
        .columns {{ grid-template-columns: minmax(0, 1.15fr) minmax(280px, .85fr); gap: 12px; }}
        .card-footer {{ margin-top: 10px; padding-top: 10px; }}
        .actions, .links {{ gap: 6px; }}
        .btn, .link-btn {{ min-height: 30px; padding: 5px 8px; font-size: 13px; }}
        .crm-form {{ grid-template-columns: 120px 140px 120px 150px minmax(220px, 1fr) auto; gap: 8px; margin-top: 10px; padding-top: 10px; }}
        textarea {{ min-height: 34px; }}
        @media (max-width: 900px) {{
          .hero, .toolbar {{ display: block; }}
          .stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); margin-top: 10px; }}
          .collect-form {{ display: grid; grid-template-columns: 1fr; }}
          .collect-form select {{ width: 100%; }}
          .filters, .columns, .crm-form {{ grid-template-columns: 1fr; }}
          .filters label:nth-of-type(4), .filters label:nth-of-type(9) {{ display: grid; }}
        }}
      </style>
    </head>
    <body data-job-running="{'1' if job['running'] else '0'}" data-job-finished="{escape(str(job.get('last_finished_at') or ''), quote=True)}">
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
            <div class="top-links">
              <a class="link-btn" href="/web-leads">Web</a>
              <a class="link-btn" href="/people-leads">TenChat</a>
              <a class="link-btn" href="/telegram-signals">Telegram</a>
              <a class="link-btn" href="/telegram-signals/settings">Настройки</a>
              <a class="link-btn" href="/telegram-signals/analytics">Аналитика</a>
              <a class="link-btn" href="/telegram-signals/export?kind=all">Excel</a>
              <a class="link-btn" href="/telegram-signals?view=raw">Сырье</a>
            </div>
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
          <form method="post" action="/telegram-signals/reclassify" class="collect-form">
            <button type="submit" class="secondary-btn">Пересчитать базу</button>
          </form>
          <div class="job-status {job_class}">{escape(job_text)}</div>
        </section>

        <nav class="quick-nav">{quick_nav}</nav>
        <div class="exports">{export_links}</div>

        <form class="filters">
          {view_hidden}
          <label>Score от <input name="min_score" type="number" value="{score}" min="0" max="100"></label>
          <label>Площадка <select name="marketplace">{marketplace_options}</select></label>
          <label>Статус <select name="status">{status_options}</select></label>
          <label>Тег <select name="crm_tag">{tag_options}</select></label>
          <label>Разбор <select name="review_status">{review_options}</select></label>
          <label>Причина <select name="reject_reason">{reject_reason_filter_options}</select></label>
          <label>CJM <select name="cjm_stage">{cjm_stage_options}</select></label>
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
        (() => {{
          let seenRunning = document.body.dataset.jobRunning === '1';
          const statusEl = document.querySelector('.job-status');
          async function pollJob() {{
            try {{
              const response = await fetch('/telegram-signals/job-status', {{ cache: 'no-store' }});
              const job = await response.json();
              if (job.running) {{
                seenRunning = true;
                if (statusEl) {{
                  statusEl.textContent = 'Идет сбор сигналов. Страница обновится сама после завершения.';
                  statusEl.className = 'job-status job-running';
                }}
                return;
              }}
              if (seenRunning) {{
                window.location.reload();
              }}
            }} catch (e) {{}}
          }}
          setInterval(pollJob, 4000);
        }})();
      </script>
    </body>
    </html>
    """
