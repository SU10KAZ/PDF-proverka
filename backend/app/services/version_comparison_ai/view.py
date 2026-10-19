"""Представление отчёта для интерфейса: краткая сводка + подробности по раскрытию.

Строится программой из сохранённого проверенного результата и пакета; ответ
модели не перезаписывается и повторно не запрашивается.

Свёртка повторов (общее правило, не под конкретный проект): пункты, каждый из
которых описывает изменение ОДНОЙ позиции СО, в одной категории, без признаков
проверки и с одинаковым набором изменённых полей, показываются одной группой.
В группе сохраняются перечень позиций, точное «было → стало» каждой и все ID.
Разные по сути изменения (дописано «Пав» и добавлено «ПАв») в краткой строке
считаются раздельно. Неоднозначные пункты не сворачиваются.

Графика (G/M), отнесённая моделью к содержательному пункту, показывается как
связанное с ним оформление, а не как самостоятельное изменение.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from . import geometry as geo
from . import images as img_mod
from . import jobs, report
from . import summary as summary_mod
from . import verify as verify_mod
from .schema import CATEGORY_LABELS

log = logging.getLogger(__name__)

FIELD_LABELS = {"note": "примечание", "qty": "количество", "unit": "единица измерения", "mark": "тип, марка",
                "code": "код продукции", "supplier": "поставщик", "name": "наименование", "pos": "номер позиции"}
GROUPS = (("content", "Изменения содержания"), ("formatting_service", "Оформление и служебные изменения"),
          ("needs_check", "Нужна проверка"))
GRAPHIC = ("region", "block", "annotation")
BUCKET_LABELS = {"ai_reviewed": "описано ИИ", "engine_classified": "классифицировано движком",
                 "linked_unverified": "учтено через связь, отдельно не проверялось", "not_reviewed": "не просмотрено"}
ENGINE_FATES = {"engine_shift": "сдвиг надписи или блока", "engine_style": "оформление линии",
                "engine_whitespace": "только пробелы", "engine_renumbered": "перенумерация позиции",
                "service": "служебное поле (дата выпуска, QR)", "service_zone": "служебная зона без полей"}


def first_sentence(text: str) -> str:
    """Первое предложение: конец — «.!?» вне кавычек, за ним пробел и заглавная буква."""
    t = (text or "").strip()
    depth = 0
    for i, ch in enumerate(t):
        if ch in "«(":
            depth += 1
        elif ch in "»)":
            depth = max(0, depth - 1)
        elif ch in ".!?" and depth == 0 and i + 2 < len(t) and t[i + 1] == " " and t[i + 2].isupper():
            return t[:i + 1]
    return t


def delta(before: str, after: str) -> str:
    """Суть изменения значения — для подсчёта одинаковых изменений в группе."""
    b, a = " ".join((before or "").split()), " ".join((after or "").split())
    if not b and a:
        return f"добавлено «{a}»"
    if b and not a:
        return f"удалено «{b}»"
    if a.startswith(b):
        return f"дописано «{a[len(b):].strip()}»"
    if b.startswith(a):
        return f"убрано «{b[len(a):].strip()}»"
    return f"«{b}» → «{a}»"


def _image_check(rec: dict, pkt: dict, pdir: Path, used: list[str]) -> dict[str, Any]:
    """Доступность изображения и попадание записи в его область. Не доказательство «правильного просмотра»."""
    by_id = {i["id"]: i for i in pkt.get("images") or []}
    for iid in used:
        im = by_id.get(iid)
        if not im or rec["id"] not in (im.get("record_ids") or []):
            continue
        problem = img_mod.check_png(pdir / im["file"], im.get("sha256"))
        if problem:
            return {"ok": False, "text": f"изображение {iid}: {problem}"}
        crop = im.get("crop_a")
        if crop and rec.get("box") and not geo.contains(geo.expand(crop, 1.0), rec["box"]):
            return {"ok": False, "text": f"запись выходит за область изображения {iid}"}
        return {"ok": True, "text": f"изображение {iid} доступно, запись в его области", "image_id": iid}
    return {"ok": False, "text": "в пункте нет изображения, на котором есть эта запись"}


def _ev(rec: dict, led: dict, pkt: dict, pdir: Path, used: list[str]) -> dict[str, Any]:
    e = {k: rec.get(k) for k in ("id", "type", "kind", "label", "before", "after", "bbox_a", "bbox_b", "flags")}
    e["fields"] = rec.get("fields")
    entry = led.get(rec["id"]) or {}
    e.update(bucket=entry.get("bucket"), fate=entry.get("fate"), reason=entry.get("reason") or "",
             bucket_label=BUCKET_LABELS.get(entry.get("bucket"), ""))
    if rec["type"] in GRAPHIC:
        e["image_check"] = _image_check(rec, pkt, pdir, used)
    return e


def _verification_view(v: dict | None, crop_base: str) -> dict[str, Any] | None:
    if not v:
        return None
    out = dict(v)
    out["crops"] = [dict(c, url=f"{crop_base}/{c['file']}") for c in v.get("crops") or []]
    return out


def _item_view(it: dict, recs: dict, led: dict, pkt: dict, pdir: Path, ver: dict | None = None,
               crop_base: str = "") -> dict[str, Any]:
    main, graphics = [], []
    has_text = any(recs[e]["type"] in ("text", "spec", "service") for e in it["evidence_ids"] if e in recs)
    for eid in it["evidence_ids"]:
        if eid not in recs:
            continue
        ev = _ev(recs[eid], led, pkt, pdir, it.get("images_used") or [])
        (graphics if has_text and recs[eid]["type"] in GRAPHIC else main).append(ev)
    contexts = {c["id"]: c for c in pkt.get("contexts") or []}
    xrefs = {x["id"]: x for x in pkt.get("xrefs") or []}
    category = ver["final_category"] if ver else it["category"]
    xrefs_spec = [{"id": x["id"], "side": x["side"], "page": x["page"], "mark": x.get("mark"),
                   "note": (x.get("fields") or {}).get("note"), "page_label": x.get("page_label")}
                  for c in it["context_ids"] for x in [xrefs.get(c)] if x and x.get("kind") == "spec_position"]
    return {"kind": "item", "id": it["item_id"], "category": category, "ai_category": it["category"],
            "category_label": CATEGORY_LABELS.get(category, category),
            "verification": _verification_view(ver, crop_base), "spec_refs": xrefs_spec,
            "brief": first_sentence(it["summary"]), "summary": it["summary"], "location": it.get("location"),
            "limitation": it.get("limitation"), "flags": it.get("flags") or [], "cross_check": it.get("cross_check"),
            "values": it.get("values") or [], "evidence_ids": it["evidence_ids"], "context_ids": it["context_ids"],
            "evidence": main, "related_graphics": graphics,
            "contexts": [contexts.get(c) or xrefs.get(c) for c in it["context_ids"] if c in contexts or c in xrefs],
            "focus_ids": [e for e in it["evidence_ids"] if e in recs]}


def _group_key(iv: dict, recs: dict) -> tuple | None:
    if iv["category"] == "needs_check" or iv["flags"] or iv["cross_check"]:
        return None
    specs = [recs[e] for e in iv["evidence_ids"] if e in recs and recs[e]["type"] == "spec"]
    if len(specs) != 1 or not specs[0].get("fields"):
        return None
    return iv["category"], tuple(sorted(specs[0]["fields"]))


def _group(members: list[dict], recs: dict, key: tuple) -> dict[str, Any]:
    fields = key[1]
    rows, deltas = [], Counter()
    for m in members:
        s = next(recs[e] for e in m["evidence_ids"] if e in recs and recs[e]["type"] == "spec")
        changes = []
        for f in fields:
            before, after = s["fields"][f]
            changes.append({"field": f, "field_label": FIELD_LABELS.get(f, f), "before": before, "after": after,
                            "delta": delta(before, after)})
            deltas[(f, delta(before, after))] += 1
        rows.append({"position": s.get("label"), "section": s.get("section"), "spec_id": s["id"], "changes": changes,
                     "item_id": m["id"], "evidence_ids": m["evidence_ids"]})
    label = ", ".join(FIELD_LABELS.get(f, f) for f in fields)
    parts = "; ".join(f"{d} — {n}" for (f, d), n in deltas.items())
    brief = f"Изменено поле «{label}» у {len(members)} позиций СО: {parts}."
    return {"kind": "group", "id": "+".join(m["id"] for m in members), "category": key[0],
            "category_label": CATEGORY_LABELS.get(key[0], key[0]), "brief": brief, "positions": rows,
            "members": members, "evidence_ids": [e for m in members for e in m["evidence_ids"]],
            "focus_ids": [e for m in members for e in m["focus_ids"]]}


def _summary_view(sm: dict, items: list[dict]) -> dict[str, Any]:
    by_id = {iv["id"]: iv for iv in items}
    groups = []
    for g in sm["groups"]:
        members = [by_id[i] for i in g["item_ids"] if i in by_id]
        groups.append({**g, "members": members, "focus_ids": [e for m in members for e in m["focus_ids"]]})
    return {"mode": sm["mode"], "version": sm["version"], "key": sm["key"], "validator": sm.get("validator"),
            "groups": groups, "coverage": sm["coverage"], "rejected": sm.get("rejected") or [],
            "call": sm.get("call"), "note": sm.get("note")}


NOT_PREPARED_TEXT = ("Проверка предупреждений ещё не подготовлена администратором — показан ответ без "
                     "проверки и без сводки по группам.")


def sheet_view(object_id: str, run_id: str, job_id: str, spid: str, repair: bool = True,
               materialize: bool = True) -> dict[str, Any]:
    """Лист отчёта. ``materialize=False`` (viewer) — только готовые слои: недостающие не строятся,
    в ответе ``verification.state = "not_prepared"`` и пояснение в ``layer_errors``."""
    res = jobs.sheet(object_id, run_id, job_id, spid, repair)
    sdir = jobs.sheet_dir(object_id, run_id, job_id, spid)
    pdir = sdir / "packet"
    pkt = json.loads((pdir / "packet.json").read_text(encoding="utf-8"))
    recs = {r["id"]: r for r in pkt["records"]}
    led = res["ledger"]
    # Слои поверх ответа: при ошибке остаётся подробный отчёт, причина показывается рядом.
    layer_errors = []
    ver = {"items": {}, "key": None}
    ver_state = "ready"
    if res["items"]:
        try:
            ver = verify_mod.build(sdir, object_id, materialize=materialize)
        except verify_mod.LayerNotPrepared:
            ver_state = "not_prepared"
            layer_errors.append(NOT_PREPARED_TEXT)
        except Exception as e:      # noqa: BLE001 — слой необязателен для показа
            log.warning("vc-ai verify layer failed for %s: %s", spid, e)
            layer_errors.append(f"проверка предупреждений не построена: {e}")
    crop_base = (f"/api/version-comparison/objects/{object_id}/runs/{run_id}/ai/jobs/{job_id}/sheets/{spid}"
                 f"/check-crop")
    items = [_item_view(it, recs, led, pkt, pdir, ver["items"].get(it["item_id"]), crop_base) for it in res["items"]]
    sm = None
    if res["items"]:
        try:
            # без обращения к модели; viewer берёт только готовую сводку или собранную в памяти
            sm = summary_mod.latest(sdir) or summary_mod.build(sdir, object_id, materialize=materialize)
        except verify_mod.LayerNotPrepared:
            sm = None
        except Exception as e:      # noqa: BLE001
            log.warning("vc-ai summary layer failed for %s: %s", spid, e)
            layer_errors.append(f"сводка не построена — показан подробный отчёт: {e}")
    # Свёртка повторов: группа только из 2+ однотипных пунктов; порядок — по первому вхождению.
    keyed = defaultdict(list)
    for iv in items:
        k = _group_key(iv, recs)
        if k:
            keyed[k].append(iv)
    entries, done = [], set()
    for iv in items:
        k = _group_key(iv, recs)
        if k and len(keyed[k]) > 1:
            if k not in done:
                entries.append(_group(keyed[k], recs, k))
                done.add(k)
        else:
            entries.append(iv)
    groups = {key: [e for e in entries if report._group(e["category"]) == key] for key, _ in GROUPS}
    engine = defaultdict(list)
    for r in pkt["records"]:
        if r["group"] == "engine":
            engine[r["fate"]].append(r["id"])
    a, b = pkt["a"], pkt["b"]
    return {
        "spid": spid, "title": pkt.get("title"), "stale": res["stale"],
        "stale_text": ("Результат сравнения этого листа изменился после описания: описание устарело, переход к "
                       "доказательствам отключён. Запустите описание заново.") if res["stale"] else "",
        "a": {k: a.get(k) for k in ("label", "file_name", "page", "sheet_no", "title")},
        "b": {k: b.get(k) for k in ("label", "file_name", "page", "sheet_no", "title")},
        "engine": res["engine"], "engine_status_label": res["engine"].get("status_label"),
        "ai_status": "stale" if res["stale"] else res["ai_status"],
        "ai_status_label": res["ai_status_label"], "engine_only_text": res.get("engine_only_text"),
        "groups": [{"key": key, "title": title, "entries": groups[key]} for key, title in GROUPS],
        "coverage": res["coverage"], "coverage_labels": BUCKET_LABELS,
        "not_reviewed": [{"id": rid, "reason": e.get("reason"), **{k: recs[rid].get(k) for k in ("type", "before",
                                                                                                   "after", "label")}}
                         for rid, e in led.items() if e["bucket"] == "not_reviewed"],
        "engine_classified": [{"fate": f, "label": ENGINE_FATES.get(f, f), "ids": v} for f, v in engine.items()],
        "linked": [{"id": r["id"], "links": r.get("links") or [], "before": r["before"], "after": r["after"]}
                   for r in pkt["records"] if r["group"] == "linked"],
        "service": [{"id": r["id"], "label": r.get("label"), "before": r["before"], "after": r["after"],
                     "fate": r["fate"], "ai": r["group"] == "ai"} for r in pkt["records"] if r["type"] == "service"],
        "issues": res.get("issues") or [], "executor": res.get("executor"),
        "summary": _summary_view(sm, items) if sm else None, "layer_errors": layer_errors,
        "verification": {"version": verify_mod.VERIFY_VERSION, "key": ver.get("key"), "note": ver.get("note"),
                         "items": len(ver["items"]), "state": ver_state},
    }


def job_view(object_id: str, run_id: str, job_id: str, repair: bool = True,
             materialize: bool = True) -> dict[str, Any]:
    """Отчёт задания для окна. ``repair=False, materialize=False`` — показ viewer без записи на диск."""
    job = jobs.get_job(object_id, run_id, job_id, repair)
    from backend.app.services.version_comparison import store
    try:
        proj = store.project(store.load_catalog(object_id), job["project_id"])
    except store.CatalogError:
        proj = {}
    sheets = []
    for s in job["sheets"]:
        try:
            sheets.append(sheet_view(object_id, run_id, job_id, s["spid"], repair, materialize))
        except jobs.JobError:
            sheets.append({"spid": s["spid"], "title": s.get("title"), "ai_status": s["ai_status"],
                           "ai_status_label": report.AI_STATUS_LABELS.get(s["ai_status"], s["ai_status"]),
                           "pending": True})
    totals = Counter()
    for s in sheets:
        for k, v in (s.get("coverage") or {}).items():
            totals[k] += v
    by_spid = {s["spid"]: s for s in job["sheets"]}
    for s in sheets:
        src = (by_spid.get(s["spid"]) or {}).get("reused_from")
        if src:
            s["reused_text"] = (f"Описание взято готовым из задания {src.get('job_id')} ({src.get('source')}): "
                                f"результат движка листа тот же, новых обращений к модели не было.")
    # Охват: ИИ описал только листы задания, а не всю пару версий.
    from backend.app.services.version_comparison import jobs as vc_jobs
    run = vc_jobs.get_run(object_id, run_id, repair)
    changed = [it for it in run["items"] if it.get("result_status") not in ("same", "same_zones_changed")]
    zones_only = sum(1 for it in run["items"] if it.get("result_status") == "same_zones_changed")
    described = [s for s in sheets if s.get("ai_status") not in ("engine_only",) and not s.get("pending")]
    names = ", ".join((s.get("title") or s["spid"]).split(" · ")[0] for s in described)
    scope_all = (job.get("scope") or {}).get("kind") == "all"
    scope_text = (f"ИИ описал {len(described)} лист(ов): {names}. В паре {len(run['items'])} пар листов: "
                  f"с отличиями содержания по данным движка — {len(changed)}, только со служебными изменениями "
                  f"штампа — {zones_only}. "
                  + ("Остальные листы ИИ не описывал — для них есть только данные движка в обычном сравнении."
                     if not scope_all or len(described) < len(changed) else ""))
    return {"job": {k: job.get(k) for k in ("job_id", "state", "reason", "created_at", "finished_at", "usage",
                                             "executor", "progress", "consent")},
            "project": {"title": proj.get("title"), "code": proj.get("code")},
            "a": job["a"], "b": job["b"], "run_id": run_id, "object_id": object_id,
            "coverage": dict(totals), "coverage_labels": BUCKET_LABELS, "disclaimer": report.DISCLAIMER,
            "scope": job.get("scope"), "scope_text": scope_text, "imported_from": [
                {k: x.get(k) for k in ("job_id", "note", "usage")} for x in job.get("imported_from") or []],
            "sheets": sheets,
            # «not_prepared» — у части листов слой проверки ещё не построен (показ без проверки).
            "verification": "not_prepared" if any((s.get("verification") or {}).get("state") == "not_prepared"
                                                  for s in sheets) else "ready"}


def view_md(v: dict[str, Any]) -> str:
    a, b = v["a"], v["b"]
    L = [f"# Что изменилось: {a.get('label')} → {b.get('label')}", "", f"**{v.get('scope_text') or ''}**", "",
         f"Проект: {v['project'].get('title') or '—'}. A: {a.get('label')} — файл «{a.get('file_name')}»; "
         f"B: {b.get('label')} — файл «{b.get('file_name')}».", "", f"_{v['disclaimer']}_", "",
         "Учёт записей: " + "; ".join(f"{v['coverage_labels'][k]} — {v['coverage'].get(k, 0)}"
                                     for k in v["coverage_labels"]) + ".", ""]
    for s in v["sheets"]:
        if s.get("pending") or s.get("ai_status") == "engine_only":
            continue
        L += [f"## {s['title']} — лист {s['b'].get('sheet_no') or s['a'].get('sheet_no') or '—'}, "
              f"стр. PDF {s['a'].get('page') or '—'} → {s['b'].get('page') or '—'}", "",
              f"Движок: {s['engine_status_label']}. ИИ: {s['ai_status_label']}.", ""]
        if s["stale"]:
            L += [f"**{s['stale_text']}**", ""]
        L += [f"_{e}_" for e in s.get("layer_errors") or []]
        sm = s.get("summary")
        if sm:
            L += [f"### Кратко (пунктов {sm['coverage']['items_total']} → групп {len(sm['groups'])})", ""]
            for g in sm["groups"]:
                head = f"**{g['title']}.** " if g.get("title") else ""
                tags = [f"{len(g['item_ids'])} п."]
                if g["limitations"]:
                    tags.append(f"ограничения: {len(g['limitations'])}")
                tags += [c["class_label"] for c in g["checks"]]
                if g["verified_items"]:
                    tags.append(f"проверено при разборе: {len(g['verified_items'])}")
                L.append(f"- [{g['category_label']}] {head}{g['brief']} _({'; '.join(tags)}; пункты: "
                         f"{', '.join(g['item_ids'])})_")
                L += [f"  - Ограничение {x['item_id']}: {x['text']}" for x in g["limitations"]]
            L += ["", "### Подробно", ""]
        for g in s["groups"]:
            if not g["entries"]:
                continue
            L += [f"### {g['title']}", ""]
            for e in g["entries"]:
                L.append(f"- {e['brief']} _(основания: {', '.join(e['evidence_ids'])})_")
                if e["kind"] == "group":
                    for p in e["positions"]:
                        ch = "; ".join(f"{c['field_label']}: «{c['before'] or '—'}» → «{c['after'] or '—'}»"
                                       for c in p["changes"])
                        L.append(f"  - {p['position']}: {ch} ({', '.join(p['evidence_ids'])})")
                else:
                    if e["summary"] != e["brief"]:
                        L.append(f"  - Подробно: {e['summary']}")
                    if e["related_graphics"]:
                        L.append("  - Связанное оформление: " + ", ".join(g_["id"] for g_ in e["related_graphics"]))
                    if e.get("limitation"):
                        L.append(f"  - Ограничение: {e['limitation']}")
                    vv = e.get("verification")
                    if vv:
                        L.append(f"  - Проверка предупреждения (добавлено при проверке): {vv['class_label']}; "
                                 + "; ".join(f"{t['token']} — {t['status_label']}" for t in vv["tokens"])
                                 + ("; " if vv["tokens"] and vv["findings"] else "")
                                 + "; ".join(f["text"] for f in vv["findings"]))
            L.append("")
        c = s["coverage"]
        L += ["Учёт записей листа: " + "; ".join(f"{BUCKET_LABELS[k]} — {c.get(k, 0)}" for k in BUCKET_LABELS) + ".",
              ""]
        if s["service"]:
            L.append("Служебные поля: " + "; ".join(f"{x['label']}: {x['before'] or '—'} → {x['after'] or '—'}"
                                                    for x in s["service"]) + ".")
            L.append("")
    rest = [s for s in v["sheets"] if s.get("ai_status") == "engine_only"]
    if rest:
        L += ["## Листы без ИИ (по данным движка)", ""]
        L += [f"- {s['title']}: {s.get('engine_only_text')}" for s in rest]
    return "\n".join(L) + "\n"
