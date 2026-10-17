"""Сводка «ИИ-описания переходов» серии одним Markdown-файлом: от первого перехода до последнего.

Без вызова модели — только готовые ответы заданий серии. По каждому переходу — шифр, версии A и B
(изм., дата, файл), по каждому листу — номер листа, название и страницы PDF обеих версий, чтобы
любую строку можно было перепроверить. Из изменения берётся главное: объект и суть
(«Кабельное ответвление … — было → стало»); «Неясно», поля и вопросы остаются в портале,
в сводке — только число вопросов по листу.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from backend.app.services.version_comparison import series as vc_series

from .. import jobs
from .. import packet as old_packet
from . import series_ai, single, view

NOT_DESCRIBED = {"failed": "ошибка ИИ", "not_sent": "не отправлен", "queued": "ещё не описан",
                 "running": "описывается", "cancelled": "не описан (отменено)"}


def _sheet_head(res: dict | None, s: dict) -> str:
    a, b = (res or {}).get("a") or {}, (res or {}).get("b") or {}
    no = b.get("sheet_no") or a.get("sheet_no")
    title = s.get("title") or (res or {}).get("title") or s["spid"]
    if no and not re.match(rf"^\s*л\.\s*{re.escape(str(no))}\b", title):
        title = f"л.{no} · {title}"
    pa, pb = a.get("page") or s.get("a_page"), b.get("page") or s.get("b_page")
    pages = " → ".join(x for x in (f"A стр. PDF {pa}" if pa else "", f"B стр. PDF {pb}" if pb else "") if x)
    return f"### {title}" + (f" ({pages})" if pages else "")


def _one_line(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _sheet_order(s: dict) -> tuple:
    return (s.get("b_page") or s.get("a_page") or 0, s["spid"])


def build(object_id: str, series_id: str) -> dict[str, Any]:
    """Вернуть {"filename", "text"}; ``JobError``, если ИИ-описание серии не запускалось."""
    rec = series_ai._load(object_id, series_id)
    if not rec:
        raise jobs.JobError("ИИ-описание переходов этой серии не запускалось")
    ser = vc_series.get(object_id, series_id)
    proj = old_packet._project(object_id, ser["project_id"]) or {}
    code = proj.get("code") or proj.get("title") or ser["project_id"]
    models: set[str] = set()
    body: list[str] = []
    totals = {"pairs": 0, "sheets": 0, "changes": 0}
    for p in rec["pairs"]:
        job = None
        if p.get("job_id"):
            try:
                job = jobs.get_job(object_id, p["run_id"], p["job_id"])
            except jobs.JobError:
                job = None
        a, b = (job or {}).get("a") or {}, (job or {}).get("b") or {}
        body += ["", f"## {p['index'] + 1}. {code}: {p['a_label']} → {p['b_label']}", ""]
        if a.get("file_name") or b.get("file_name"):
            body += [f"A: {a.get('label') or p['a_label']}, файл «{a.get('file_name', '—')}». "
                     f"B: {b.get('label') or p['b_label']}, файл «{b.get('file_name', '—')}».", ""]
        if not job:
            body.append(f"_{p.get('reason') or 'ИИ-описание не запускалось'}._")
            continue
        totals["pairs"] += 1
        pair_lines: list[str] = []
        for s in sorted(job["sheets"], key=_sheet_order):
            res = view.load_result(object_id, p["run_id"], s["spid"])
            mine = res if res and res.get("job_id") == job["job_id"] else None
            if mine and (mine.get("executor") or {}).get("model"):
                models.add(mine["executor"]["model"])
            if s.get("item_kind") in single.KINDS:
                kind = single.KINDS[s["item_kind"]]
                sg = (mine or {}).get("single") or {}
                page = s.get("b_page") if kind == "added" else s.get("a_page")
                side = "B" if kind == "added" else "A"
                desc = f" — {_one_line(sg['summary'])}" if sg.get("summary") else ""
                pair_lines += [f"### {single.LABELS[kind]}: {s.get('title') or s['spid']} ({side} стр. PDF {page})", "",
                               f"- {sg.get('sheet_kind') or 'лист'}{desc}" if sg else
                               f"- _{NOT_DESCRIBED.get(s['ai_status'], s['ai_status'])}_", ""]
                totals["sheets"] += 1
                continue
            if not mine:
                if s["ai_status"] in NOT_DESCRIBED:
                    pair_lines += [_sheet_head(None, s), "", f"- _{NOT_DESCRIBED[s['ai_status']]}_", ""]
                continue
            changes, unresolved = mine.get("changes") or [], mine.get("unresolved") or []
            if not changes and not unresolved:
                continue
            totals["sheets"] += 1
            totals["changes"] += len(changes)
            pair_lines += [_sheet_head(mine, s), ""]
            pair_lines += [f"- **{_one_line(c['object'])}** — {_one_line(c['summary'])}" for c in changes]
            if unresolved:
                pair_lines.append(f"- _Требует уточнения: {len(unresolved)} (вопросы — в портале по этому листу)._")
            pair_lines.append("")
        body += pair_lines or ["_ИИ не нашёл изменений для строительства._"]
    now = datetime.now(timezone.utc).astimezone()
    head = [f"# Сводка изменений {code}", "",
            f"Переходы от первой версии к последней, {len(rec['pairs'])} шт. Составлено {now:%d.%m.%Y %H:%M}. "
            f"Модель: {', '.join(sorted(models)) or '—'}.",
            f"Описано листов: {totals['sheets']}, изменений: {totals['changes']}.", "",
            "Сводка собрана из ИИ-описаний; каждое изменение проверяйте по указанному листу и страницам PDF. "
            "Подробности (поля «было/стало», «Неясно», вопросы) — в портале, раздел «Сравнение версий»."]
    safe = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._-]+", "_", str(code)).strip("_") or "серия"
    return {"filename": f"Сводка_изменений_{safe}.md", "text": "\n".join(head + body).rstrip() + "\n"}
