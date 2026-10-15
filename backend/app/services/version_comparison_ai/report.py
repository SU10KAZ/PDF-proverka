"""Результат листа и сводка пары версий — из проверенного JSON (план §6, §9.2).

Свободный текст модели не публикуется: MD генерирует программа, поэтому MD,
JSON и интерфейс не расходятся. Служебные дата выпуска и QR сводятся в одну
строку на пару версий. Статусы движка не меняются — рядом показывается статус ИИ.
"""
from __future__ import annotations

from typing import Any

from . import prompt
from .records import FATE_LABELS
from .schema import CATEGORY_LABELS, FATE_LABELS as AI_FATE_LABELS
from .validate import ledger

AI_STATUS_LABELS = {
    "engine_only": "без ИИ — по данным движка",
    "queued": "в очереди", "running": "идёт", "done": "описано ИИ полностью",
    "partial": "описано ИИ не полностью", "failed": "ошибка", "cancelled": "отменено",
    "stale": "устарело — сравнение пересчитано", "not_sent": "не отправлено",
}
BUCKET_LABELS = {
    "ai_reviewed": "описано ИИ",
    "engine_classified": "классифицировано движком",
    "linked_unverified": "учтено через связь, отдельно не проверялось",
    "not_reviewed": "не просмотрено",
}
GROUP_ORDER = (("content", "Изменения содержания"), ("formatting_service", "Оформление и служебное"),
               ("needs_check", "Нужна проверка"))
DISCLAIMER = "Описание составлено ИИ по результатам сравнения и инженером не подтверждено."


def _engine_only_text(packet: dict[str, Any]) -> str:
    eng = packet["engine"]
    kind = eng.get("item_kind")
    if kind == "only_b":
        return f"Лист добавлен в B (стр. PDF {packet['b']['page']}). Содержание ИИ не описывалось."
    if kind == "only_a":
        return f"Лист удалён: в B его нет (стр. PDF A {packet['a']['page']})."
    service = [r for r in packet["records"] if r["type"] == "service" and r.get("field") not in ("date", "qr")]
    if eng.get("status") == "same_zones_changed" and not service:
        return "Основное содержание совпадает по данным движка; изменились только служебные поля (см. «Служебное»)."
    groups: dict[str, int] = {}
    for r in packet["records"]:
        if r["group"] == "engine":
            groups[r["fate"]] = groups.get(r["fate"], 0) + 1
    parts = [f"Статус движка: {eng.get('status_label')}."]
    if eng.get("reasons"):
        parts.append("Причины: " + "; ".join(prompt.REASONS.get(r.split(":")[0], r) for r in eng["reasons"]
                                              if not r.startswith("UNCHECKED_CHANNEL")) + ".")
    if service:
        parts.append("Служебные поля: " + "; ".join(f"{r['label']}: {r['before'] or '—'} → {r['after'] or '—'}"
                                                    for r in service) + ".")
    other = {k: v for k, v in groups.items() if k not in ("service",)}
    if other:
        parts.append("Учтено движком: " + ", ".join(f"{FATE_LABELS.get(k, k)} — {v}" for k, v in other.items()) + ".")
    return " ".join(p for p in parts if p.strip(". "))


def sheet_result(packet: dict[str, Any], chunks: list[dict[str, Any]], job: dict[str, Any]) -> dict[str, Any]:
    """Свести части листа. ``chunks`` — [{chunk_id, status, check|None, error_kind, error}]."""
    fates: dict[str, dict[str, Any]] = {}
    items: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for ch in chunks:
        chk = ch.get("check")
        tag = ch["chunk_id"].rsplit("-", 1)[1]
        if chk:
            for it in chk["items"]:
                it = dict(it, item_id=f"{tag}.{it['item_id']}", chunk_id=ch["chunk_id"])
                items.append(it)
            for rid, f in chk["fates"].items():
                fates[rid] = dict(f, item_id=f"{tag}.{f['item_id']}" if f.get("item_id") else "")
            issues += [dict(i, chunk_id=ch["chunk_id"]) for i in chk["issues"]]
        elif ch.get("status") in ("failed", "not_sent", "cancelled", "queued"):
            reason = ch.get("error") or AI_STATUS_LABELS.get(ch["status"], ch["status"])
            chunk = next(c for c in packet["chunks"] if c["chunk_id"] == ch["chunk_id"])
            for rid in chunk["record_ids"]:
                fates[rid] = {"fate": "not_reviewed", "source": "program", "item_id": "", "reason": reason}
    led, counts = ledger(packet, fates)
    if not packet.get("ai_needed"):
        status = "engine_only"
    elif any(ch.get("status") == "cancelled" for ch in chunks):
        status = "cancelled"
    elif chunks and all(ch.get("status") == "failed" for ch in chunks):
        status = "failed"
    elif counts["not_reviewed"] or any(ch.get("status") != "done" for ch in chunks):
        status = "partial"
    else:
        status = "done"
    by_rec = {r["id"]: r for r in packet["records"]}
    for it in items:
        it["evidence"] = [_evidence(by_rec[e]) for e in it["evidence_ids"] if e in by_rec]
    return {
        "schema": "version_comparison_ai.sheet_result/v1", "job_id": job["job_id"], "packet_id": packet["packet_id"],
        "spid": packet["spid"], "title": packet.get("title"), "a": _side(packet["a"]), "b": _side(packet["b"]),
        "engine": {k: packet["engine"].get(k) for k in ("status", "status_label", "reasons", "result_sha256",
                                                        "spec_sha256", "algo", "item_kind")},
        "ai_status": status, "ai_status_label": AI_STATUS_LABELS[status],
        "engine_only_text": None if packet.get("ai_needed") else _engine_only_text(packet),
        "items": items, "ledger": led, "coverage": counts, "issues": issues,
        "chunks": [{k: ch.get(k) for k in ("chunk_id", "status", "error_kind", "error", "cached")} for ch in chunks],
        "service": [{"label": r["label"], "field": r.get("field"), "before": r["before"], "after": r["after"]}
                    for r in packet["records"] if r["type"] == "service"],
        "executor": job.get("executor"), "engineer_review": None,
    }


def _side(s: dict) -> dict:
    return {k: s.get(k) for k in ("label", "file_name", "page", "sheet_no", "title")}


def _evidence(r: dict) -> dict[str, Any]:
    """Снимок записи для показа. Переход в просмотрщик идёт по ID на текущем результате."""
    return {"id": r["id"], "ref": r["ref"], "type": r["type"], "kind": r["kind"], "before": r["before"],
            "after": r["after"], "bbox_a": r["bbox_a"], "bbox_b": r["bbox_b"], "box": r["box"],
            "label": r.get("label"), "flags": r.get("flags")}


def _group(cat: str) -> str:
    return {"content": "content", "formatting": "formatting_service", "service": "formatting_service"}.get(
        cat, "needs_check")


def sheet_md(res: dict[str, Any]) -> str:
    a, b = res["a"], res["b"]
    head = (f"### {res.get('title') or res['spid']} — лист {a.get('sheet_no') or b.get('sheet_no') or '—'}, "
            f"стр. PDF {a.get('page') or '—'} → {b.get('page') or '—'}")
    lines = [head, "",
             f"A: {a.get('label') or '—'} (файл «{a.get('file_name') or '—'}»), стр. PDF {a.get('page') or '—'}. "
             f"B: {b.get('label') or '—'} (файл «{b.get('file_name') or '—'}»), стр. PDF {b.get('page') or '—'}.",
             f"Движок: {res['engine']['status_label']}. ИИ: {res['ai_status_label']}.", ""]
    if res.get("engine_only_text"):
        lines += [res["engine_only_text"], ""]
    for key, title in GROUP_ORDER:
        its = [i for i in res["items"] if _group(i["category"]) == key]
        if not its:
            continue
        lines += [f"**{title}**", ""]
        for it in its:
            basis = ", ".join(it["evidence_ids"] + it["context_ids"])
            lines.append(f"- {it['summary']} _(основания: {basis})_")
            if it.get("limitation"):
                lines.append(f"  - Ограничение: {it['limitation']}")
            if it.get("original_category"):
                lines.append(f"  - Переведено в «Нужна проверка» проверкой: {', '.join(it['flags'])}")
        lines.append("")
    if res["ledger"]:
        c = res["coverage"]
        lines.append("Учёт записей: " + "; ".join(f"{BUCKET_LABELS[k]} — {c[k]}" for k in BUCKET_LABELS) + ".")
        nr = [(rid, e) for rid, e in res["ledger"].items() if e["bucket"] == "not_reviewed"]
        if nr:
            lines.append("Не просмотрено: " + "; ".join(f"{rid} ({e.get('reason') or 'нет судьбы'})" for rid, e in nr[:40])
                         + (f" … и ещё {len(nr) - 40}" if len(nr) > 40 else ""))
        lines.append("")
    return "\n".join(lines)


def build(job: dict[str, Any], sheets: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    """Сводка пары версий из результатов листов."""
    service: dict[tuple, list[str]] = {}
    totals = {k: 0 for k in BUCKET_LABELS}
    cats = {k: 0 for k in CATEGORY_LABELS}
    status_counts: dict[str, int] = {}
    for s in sheets:
        status_counts[s["ai_status"]] = status_counts.get(s["ai_status"], 0) + 1
        for k in totals:
            totals[k] += s["coverage"].get(k, 0)
        for it in s["items"]:
            cats[it["category"]] = cats.get(it["category"], 0) + 1
        for f in s["service"]:
            if f["field"] in ("date", "qr"):
                key = (f["label"], f["before"], f["after"]) if f["field"] == "date" else (f["label"], "", "")
                service.setdefault(key, []).append(s["spid"])
    with_content = [s for s in sheets if any(i["category"] == "content" for i in s["items"])]
    rep = {
        "schema": "version_comparison_ai.pair_report/v1", "job_id": job["job_id"], "run_id": job["run_id"],
        "object_id": job["object_id"], "project_id": job["project_id"], "a": job["a"], "b": job["b"],
        "executor": job.get("executor"), "disclaimer": DISCLAIMER,
        "summary": {"sheets": len(sheets), "sheets_with_content": len(with_content), "items": cats,
                    "coverage": totals, "ai_status": status_counts},
        "service": [{"label": k[0], "before": k[1], "after": k[2], "sheets": v} for k, v in service.items()],
        "sheets": sheets,
    }
    lab_a, lab_b = job["a"].get("label") or "A", job["b"].get("label") or "B"
    lines = [f"# Что изменилось: {lab_a} → {lab_b}", "",
             f"A: {lab_a} — файл «{job['a'].get('file_name') or '—'}»; B: {lab_b} — файл «{job['b'].get('file_name') or '—'}».",
             "", f"_{DISCLAIMER}_", "",
             f"Исполнитель: {(job.get('executor') or {}).get('executor')}, модель "
             f"{(job.get('executor') or {}).get('model')}. Задание {job['job_id']}, прогон {job['run_id']}.", "",
             "## Сводка", "",
             f"- Листов в отчёте: {len(sheets)}; с изменениями содержания: {len(with_content)}.",
             "- Пунктов: " + ", ".join(f"{CATEGORY_LABELS[k].lower()} — {v}" for k, v in cats.items()) + ".",
             "- Учёт записей: " + "; ".join(f"{BUCKET_LABELS[k]} — {v}" for k, v in totals.items()) + ".",
             "- Статус ИИ по листам: " + ", ".join(f"{(AI_STATUS_LABELS.get(k, k))} — {v}"
                                                   for k, v in status_counts.items()) + "."]
    if service:
        lines += ["", "## Служебное", ""]
        for (label, before, after), spids in service.items():
            change = f"{before} → {after}" if before or after else "заменён"
            lines.append(f"- {label}: {change} — {len(spids)} лист(ов).")
    ai_sheets = [s for s in sheets if s["ai_status"] != "engine_only"]
    if ai_sheets:
        lines += ["", "## По листам", ""]
        for s in ai_sheets:
            lines.append(sheet_md(s))
    rest = [s for s in sheets if s["ai_status"] == "engine_only"]
    if rest:
        lines += ["", "## Листы без ИИ (по данным движка)", ""]
        by_text: dict[str, list[dict]] = {}
        for s in rest:
            by_text.setdefault(s["engine_only_text"], []).append(s)
        for text, group in by_text.items():
            if len(group) == 1:
                s = group[0]
                lines.append(f"- {s.get('title') or s['spid']} (стр. PDF {s['a'].get('page') or '—'} → "
                             f"{s['b'].get('page') or '—'}): {text}")
            else:
                pages = ", ".join(f"{s['a'].get('page') or '—'}→{s['b'].get('page') or '—'}" for s in group)
                lines.append(f"- {len(group)} листов: {text} Стр. PDF A→B: {pages}.")
    return rep, "\n".join(lines) + "\n"


__all__ = ["sheet_result", "sheet_md", "build", "AI_STATUS_LABELS", "BUCKET_LABELS", "AI_FATE_LABELS"]
