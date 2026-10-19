"""Сводка «ИИ-описания переходов» серии одним Markdown-файлом: от первого перехода до последнего.

Без вызова модели — только готовые ответы заданий серии. По каждому переходу — шифр, версии A и B
(изм., дата, файл), по каждому листу — номер листа, название и страницы PDF обеих версий, чтобы
любую строку можно было перепроверить. Из изменения берётся главное: объект и суть
(«Кабельное ответвление … — было → стало»); «Неясно», поля и вопросы остаются в портале,
в сводке — только число вопросов по листу.

Файл воспроизводим: одинаковые сохранённые данные дают одинаковые байты (время в заголовке — время
последних данных, а не формирования). Кроме текста ``build`` возвращает паспорт строк (``records``:
номер строки → сохранённая запись с полями, members и вопросами) и реестр покрытия переходов
(``coverage``); реестр выводится и в конце файла разделом «Реестр покрытия» — после переходов,
чтобы номера их строк не зависели от реестра.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from backend.app.services.version_comparison import paths, series as vc_series, store
from backend.app.services.version_comparison import specification_export as spec_export
from backend.app.services.version_comparison.specification_compare import change_entries, render_entry

from .. import jobs
from .. import packet as old_packet
from . import series_ai, single, view

NOT_DESCRIBED = {"failed": "ошибка ИИ", "not_sent": "не отправлен", "queued": "ещё не описан",
                 "running": "описывается", "cancelled": "не описан (отменено)"}
REGISTRY_TITLE = "## Реестр покрытия"
# Категории покрытия листов перехода (каждый лист — ровно в одной).
COVERAGE = (("engine_same", "совпадает по движку"), ("service_only", "только служебное"),
            ("ai_described", "описано ИИ"), ("spec_whole", "СО целиком"),
            ("ambiguous", "неоднозначно"), ("not_processed", "не обработано"))
# Отметка документа на листе (штамп «не согласовано», водяной знак согласования): её снятие — изменение
# отметки, а не оборудования, и не доказательство согласования.
DOC_MARK = re.compile(r"НЕ\s+СОГЛАСОВАН|СТРОИТЬ\s+ОПАСНО|НЕ\s+ОПЛАЧЕН|вод[яа]н\w*\s+знак\w*\s+согласован", re.I)
QUOTE = re.compile(r"«([^»]{3,80})»")
FIELD_KEYS = ("name", "before", "after", "unit", "certainty", "confirmation")


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


def _fields(items: list[dict] | None) -> list[dict]:
    return [{k: f.get(k, "") for k in FIELD_KEYS} for f in items or []]


def _change_record(p: dict, job: dict, s: dict, res: dict, c: dict) -> dict:
    return {"kind": "ai_change", "run_id": p["run_id"], "job_id": job["job_id"], "spid": s["spid"],
            "change_id": c.get("id"), "object": _one_line(c.get("object")), "place": _one_line(c.get("place")),
            "change_kind": c.get("kind_label") or c.get("kind"), "fields": _fields(c.get("fields")),
            "members": [{"object": _one_line(m.get("object")), "place": _one_line(m.get("place")),
                         "fields": _fields(m.get("fields"))} for m in c.get("members") or []],
            "mapping": c.get("mapping"), "confirmation": c.get("confirmation"), "unclear": bool(c.get("unclear")),
            "limitation": _one_line(c.get("limitation")), "flags": list(c.get("flags") or []),
            "a_page": (res.get("a") or {}).get("page") or s.get("a_page"),
            "b_page": (res.get("b") or {}).get("page") or s.get("b_page")}


def _doc_marks(res: dict | None) -> list[str]:
    """Пояснения ИИ об отметке документа (без строительного содержания) на листе."""
    out = []
    for e in (res or {}).get("excluded") or []:
        note = _one_line(e.get("note"))
        if note and DOC_MARK.search(note):
            out.append(note)
    return out


def _data_time(object_id: str, series_id: str, ser: dict, jobs_seen: list[dict]) -> str | None:
    """Время последних данных сводки: завершение серии, заданий ИИ и полного сравнения СО."""
    stamps = [ser.get("finished_at")] + [j.get("finished_at") for j in jobs_seen]
    ws = paths.series_path(object_id, series_id).parent / "whole_specifications" / "job.json"
    try:
        stamps.append(json.loads(ws.read_text(encoding="utf-8")).get("finished_at"))
    except (OSError, ValueError):
        pass
    parsed = []
    for x in stamps:
        try:
            parsed.append(datetime.fromisoformat(x))
        except (TypeError, ValueError):
            continue
    return max(parsed).astimezone().strftime("%d.%m.%Y %H:%M") if parsed else None


def _coverage(object_id: str, p: dict, run: dict, job: dict | None, ctx: dict) -> dict:
    """Реестр покрытия перехода: каждый лист прогона — в одной категории, с причиной."""
    sheets = {s["spid"]: s for s in (job or {}).get("sheets", [])}
    cats: dict[str, list[dict]] = {k: [] for k, _ in COVERAGE}
    marks: list[dict] = []
    covered = ctx.get("covered") or {"a": set(), "b": set()}
    for it in run.get("items", []):
        title = it.get("title") or it.get("spid")
        row = {"spid": it.get("spid"), "title": title, "a_page": it.get("a_page"), "b_page": it.get("b_page")}
        status = it.get("result_status")
        s = sheets.get(it.get("spid"))
        res = view.load_result(object_id, p["run_id"], it["spid"]) if s else None
        mine = res if res and job and res.get("job_id") == job["job_id"] else None
        if status == "same":
            cats["engine_same"].append(row)
            continue
        if status == "same_zones_changed":
            cats["service_only"].append(row | {"why": "движок: изменены только служебные зоны"})
            continue
        if spec_export.is_specification(ctx, it, mine):
            pages_ok = all(not it.get(f"{x}_page") or it.get(f"{x}_page") in covered[x] for x in ("a", "b"))
            if ctx.get("reports") and pages_ok and not spec_export.coverage_issue(ctx):
                cats["spec_whole"].append(row)
            else:
                cats["not_processed"].append(row | {"why": "нет результата полного сравнения СО"})
            continue
        if not mine:
            why = NOT_DESCRIBED.get((s or {}).get("ai_status"), "ИИ-описание не запускалось")
            cats["not_processed"].append(row | {"why": why})
            continue
        if it.get("kind") in single.KINDS:
            (cats["ai_described"] if (mine.get("single") or {}) else cats["not_processed"]).append(row)
            continue
        found = _doc_marks(mine)
        if found:
            marks.append(row | {"notes": found})
        state = mine.get("state") or "done"
        if state == "no_construction_changes" or (state == "done" and not mine.get("changes")):
            cats["service_only"].append(row | {"why": "ИИ: строительных изменений не выявлено"})
        elif state in ("needs_clarification", "partial") or mine.get("unresolved"):
            cats["ambiguous"].append(row | {"why": view.review_reason(object_id, p["run_id"], mine)
                                            or f"вопросов: {len(mine.get('unresolved') or [])}"})
        elif state == "done":
            cats["ai_described"].append(row)
        else:
            cats["not_processed"].append(row | {"why": val_label(state)})
    return {"index": p["index"], "label": f"{p['a_label']} → {p['b_label']}", "run_id": p.get("run_id"),
            "sheets_total": len(run.get("items", [])), "categories": cats, "document_marks": marks,
            "complete": not cats["not_processed"]}


def val_label(state: str | None) -> str:
    from . import validate as val
    return val.STATE_LABELS.get(state or "", state or "нет результата")


def _short(v: str) -> str:
    """Версия из подписи перехода: «V9 Изм.5 04.03.2026» → «V9»."""
    return (v or "").split()[0] if (v or "").split() else v


def _registry_lines(coverage: list[dict]) -> list[str]:
    out = ["", REGISTRY_TITLE, "",
           "Каждый лист перехода — в одной категории: совпадает по движку; только служебное (служебные зоны, "
           "отметки, оформление); описано ИИ; СО целиком (полное сравнение спецификации); неоднозначно "
           "(вопросы, частичный разбор); не обработано.", ""]
    for c in coverage:
        a, b = c["label"].split(" → ", 1)
        counts = "; ".join(f"{label} {len(c['categories'][k])}" for k, label in COVERAGE)
        out.append(f"- {_short(a)}→{_short(b)}: листов {c['sheets_total']} — {counts}.")
        if c["categories"]["not_processed"]:
            out.append("  - не обработано: " + "; ".join(
                f"{r['title']} ({r.get('why') or 'нет результата'})" for r in c["categories"]["not_processed"]))
        if c["categories"]["ambiguous"]:
            out.append("  - неоднозначно: " + "; ".join(r["title"] for r in c["categories"]["ambiguous"]))
        if c["document_marks"]:
            quotes = sorted({q for m in c["document_marks"] for n in m["notes"] for q in QUOTE.findall(n)
                             if DOC_MARK.search(q)})
            out.append("  - отметка документа: " + ", ".join(m["title"].split(" · ")[0] for m in c["document_marks"])
                       + (f" — {'; '.join('«' + q + '»' for q in quotes)}" if quotes else "")
                       + ". Изменение отметки документа, не оборудования; не доказательство согласования.")
    return out


def build(object_id: str, series_id: str, only: list[int] | None = None) -> dict[str, Any]:
    """Вернуть сводку серии; whole-specification данные доступны и без series AI job.

    ``only`` — номера переходов (с 0) для частичной сводки (демонстрация одного перехода)."""
    ai_rec = series_ai._load(object_id, series_id)
    try:
        ser = vc_series.get(object_id, series_id)
    except store.CatalogError as exc:
        raise jobs.JobError(str(exc)) from exc
    series_pairs = ser.get("pairs") or []
    if series_pairs:
        # The comparison series is authoritative for transition membership;
        # optional per-sheet AI records only enrich it with job IDs.
        ai_by_run = {p.get("run_id"): p for p in (ai_rec or {}).get("pairs", []) if p.get("run_id")}
        pairs = []
        for i, pair in enumerate(series_pairs):
            ai_pair = ai_by_run.get(pair.get("run_id"), {})
            pairs.append({**ai_pair, "index": i, "run_id": pair.get("run_id"),
                          "a_label": pair.get("a_label") or ai_pair.get("a_label") or pair.get("a"),
                          "b_label": pair.get("b_label") or ai_pair.get("b_label") or pair.get("b"),
                          "reason": ai_pair.get("reason") or pair.get("error") or
                                   ("сравнение не запускалось" if not pair.get("run_id") else None)})
        ai_rec = {"pairs": pairs}
    elif not ai_rec:
        raise jobs.JobError("серия не содержит переходов")
    if only is not None:
        ai_rec = {"pairs": [p for p in ai_rec["pairs"] if p["index"] in set(only)]}
        if not ai_rec["pairs"]:
            raise jobs.JobError("в серии нет выбранных переходов")
    proj = old_packet._project(object_id, ser["project_id"]) or {}
    code = proj.get("code") or proj.get("title") or ser["project_id"]
    models: set[str] = set()
    body: list[str] = []
    recs: dict[int, dict] = {}          # индекс строки в body → запись
    coverage: list[dict] = []
    jobs_seen: list[dict] = []
    totals = {"pairs": 0, "sheets": 0, "changes": 0}
    for p in ai_rec["pairs"]:
        job = None
        if p.get("job_id"):
            try:
                job = jobs.get_job(object_id, p["run_id"], p["job_id"])
            except jobs.JobError:
                job = None
        if job:
            jobs_seen.append(job)
        ctx = spec_export.context(object_id, p["run_id"])
        run = ctx["run"] or job or {}
        a, b = run.get("a") or {}, run.get("b") or {}
        body += ["", f"## {p['index'] + 1}. {code}: {p['a_label']} → {p['b_label']}", ""]
        if a.get("file_name") or b.get("file_name"):
            body += [f"A: {a.get('label') or p['a_label']}, файл «{a.get('file_name', '—')}». "
                     f"B: {b.get('label') or p['b_label']}, файл «{b.get('file_name', '—')}».", ""]
        totals["pairs"] += bool(job or ctx["reports"])
        pair_lines: list[str] = []
        pair_recs: dict[int, dict] = {}

        def add(line: str, rec: dict | None = None) -> None:
            if rec is not None:
                pair_recs[len(pair_lines)] = rec
            pair_lines.append(line)
        for s in sorted((job or {}).get("sheets", []), key=_sheet_order):
            res = view.load_result(object_id, p["run_id"], s["spid"])
            mine = res if res and res.get("job_id") == job["job_id"] else None
            if spec_export.is_specification(ctx, s, mine):
                # The whole-specification comparison owns ALL spec MD output,
                # including unmatched/added/removed sheets and failed AI jobs.
                continue
            if mine and (mine.get("executor") or {}).get("model"):
                models.add(mine["executor"]["model"])
            if s.get("item_kind") in single.KINDS:
                kind = single.KINDS[s["item_kind"]]
                sg = (mine or {}).get("single") or {}
                page = s.get("b_page") if kind == "added" else s.get("a_page")
                side = "B" if kind == "added" else "A"
                desc = f" — {_one_line(sg['summary'])}" if sg.get("summary") else ""
                add(f"### {single.LABELS[kind]}: {s.get('title') or s['spid']} ({side} стр. PDF {page})")
                add("")
                if sg:
                    add(f"- {sg.get('sheet_kind') or 'лист'}{desc}",
                        {"kind": "ai_single", "run_id": p["run_id"], "job_id": job["job_id"], "spid": s["spid"],
                         "single": kind, "sheet_kind": sg.get("sheet_kind"), "summary": _one_line(sg.get("summary")),
                         "a_page": s.get("a_page") if kind == "removed" else None,
                         "b_page": s.get("b_page") if kind == "added" else None})
                else:
                    add(f"- _{NOT_DESCRIBED.get(s['ai_status'], s['ai_status'])}_")
                add("")
                totals["sheets"] += 1
                continue
            if not mine:
                if s["ai_status"] in NOT_DESCRIBED:
                    add(_sheet_head(None, s))
                    add("")
                    add(f"- _{NOT_DESCRIBED[s['ai_status']]}_")
                    add("")
                continue
            changes, unresolved = mine.get("changes") or [], mine.get("unresolved") or []
            if not changes and not unresolved:
                continue
            totals["sheets"] += 1
            totals["changes"] += len(changes)
            add(_sheet_head(mine, s))
            add("")
            for c in changes:
                add(f"- **{_one_line(c['object'])}** — {_one_line(c['summary'])}", _change_record(p, job, s, mine, c))
            if unresolved:
                add(f"- _Требует уточнения: {len(unresolved)} (вопросы — в портале по этому листу)._",
                    {"kind": "ai_questions", "run_id": p["run_id"], "job_id": job["job_id"], "spid": s["spid"],
                     "questions": [{"id": u.get("id"), "object": _one_line(u.get("object")),
                                    "place": _one_line(u.get("place")), "question": _one_line(u.get("question"))}
                                   for u in unresolved],
                     "a_page": (mine.get("a") or {}).get("page"), "b_page": (mine.get("b") or {}).get("page")})
            add("")
        spec_lines = []
        seen = set()
        locations = set()
        for item in ctx["reports"]:
            for entry in change_entries(item["comparison"]):
                # Never deduplicate just by text/name: equal materials can be
                # distinct positions in separate systems or on separate sheets.
                if entry["id"] in seen:
                    continue
                seen.add(entry["id"])
                locations.add((tuple(entry["old_pages"]), tuple(entry["new_pages"])))
                spec_lines.append((f"- {render_entry(entry)}", {
                    "kind": "spec_entry", "run_id": p["run_id"], "entry_id": entry["id"], "change_kind": entry["kind"],
                    "old_locations": entry.get("old_locations") or [], "new_locations": entry.get("new_locations") or [],
                    "old_pages": list(entry.get("old_pages") or []), "new_pages": list(entry.get("new_pages") or []),
                    "fields": [{"name": f.get("field"), "before": f.get("old"), "after": f.get("new")}
                               for f in entry.get("field_changes") or []],
                    "confirmation": "spec_text"}))
        for item in ctx["reports"]:
            for g in item["comparison"].get("unresolved") or []:
                def rows_text(rows):
                    return "; ".join(
                        " ".join(x for x in (r["fields"].get("name", "")[:60], r["fields"].get("mark", ""),
                                             (r["fields"].get("qty", "") + " " + r["fields"].get("unit", "")).strip())
                                 if x) for r in rows[:4]) + (" …" if len(rows) > 4 else "")
                pages_o = sorted({s.get("page") for r in g["old_rows"] for s in r.get("sources") or [] if s.get("page")})
                pages_n = sorted({s.get("page") for r in g["new_rows"] for s in r.get("sources") or [] if s.get("page")})
                loc = lambda ps: ", ".join(f"стр. PDF {x}" for x in ps) or "—"  # noqa: E731
                spec_lines.append((f"- Старая: {loc(pages_o)}; новая: {loc(pages_n)}. Требует проверки (соответствие "
                                   f"позиций неоднозначно): было {rows_text(g['old_rows']) or '—'} → стало "
                                   f"{rows_text(g['new_rows']) or '—'}.", {
                    "kind": "spec_entry", "run_id": p["run_id"], "entry_id": None, "change_kind": "unresolved",
                    "old_locations": [], "new_locations": [], "old_pages": pages_o, "new_pages": pages_n,
                    "fields": [], "confirmation": "spec_text"}))
        if spec_lines:
            add("### Спецификация целиком")
            add("")
            for line, rec in spec_lines:
                add(line, rec)
            add("")
            totals["changes"] += len(spec_lines)
            totals["sheets"] += len(locations)
        if ctx.get("model") and ctx["reports"]:
            models.add(ctx["model"])
        issue = spec_export.coverage_issue(ctx)
        if issue:
            add(f"_{issue}_")
            add("")
        elif ctx["reports"] and not spec_lines and not job:
            others = any(not spec_export.is_specification(ctx, item) and item.get("result_status") not in view.ENGINE_SAME
                         for item in run.get("items", []))
            add("_Спецификация: содержательных изменений не выявлено"
                + ("" if others else "; изменений содержания нет (по данным сравнения)") + "._")
            add("")
        # «Не запускалось» — только если есть листы не СО с отличиями: совпавшие по движку и служебные описывать не нужно.
        if not job and ctx["reports"] and any(not spec_export.is_specification(ctx, item)
                                              and item.get("result_status") not in view.ENGINE_SAME
                                              for item in run.get("items", [])):
            add("_ИИ-описание остальных листов не запускалось._")
            add("")
        if not job and not ctx["reports"] and not issue:
            add(f"_{p.get('reason') or 'ИИ-описание не запускалось'}._")
        if not pair_lines:
            pair_lines = ["_ИИ не нашёл изменений для строительства (изменений содержания нет)._"]
        for k, rec in pair_recs.items():
            recs[len(body) + k] = rec
        body += pair_lines
        if ctx["run"]:
            coverage.append(_coverage(object_id, p, ctx["run"], job, ctx))
    at = _data_time(object_id, series_id, ser, jobs_seen)
    head = [f"# Сводка изменений {code}", "",
            f"Переходы от первой версии к последней, {len(ai_rec['pairs'])} шт."
            + (f" Данные на {at}." if at else "") + f" Модель: {', '.join(sorted(models)) or '—'}.",
            f"Описано листов: {totals['sheets']}, изменений: {totals['changes']}.", "",
            "Спецификации приведены по сравнению целиком; остальные листы — по ИИ-описаниям. "
            "Каждое изменение проверяйте по указанному листу и страницам PDF. "
            "Подробности (поля «было/стало», «Неясно», вопросы) — в портале, раздел «Сравнение версий»."]
    body = [*body]
    while body and body[-1] == "":
        body.pop()
    tail = _registry_lines(coverage) if coverage else []
    lines = head + body + tail
    records = {str(len(head) + i + 1): rec for i, rec in recs.items()}
    safe = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._-]+", "_", str(code)).strip("_") or "серия"
    return {"filename": f"Сводка_изменений_{safe}.md", "text": "\n".join(lines).rstrip() + "\n",
            "records": records, "coverage": coverage, "data_at": at}
