"""Разбор предупреждений «Нужна проверка» на сохранённых ответах — без обращения к модели.

Слой поверх проверенного результата листа; ответ модели и ``result.json`` не меняются.
Для каждого пункта категории «Нужна проверка» программа ищет основание в исходных
PDF (слова страниц из профиля движка) и относит пункт к одному классу:

* ``contradiction`` — реальное противоречие между источниками;
* ``unreadable`` — нечитаемое или отсутствующее значение (в т.ч. слишком мелкая графика);
* ``image_only`` — прочитано ИИ с изображения, автоматически не подтверждено;
* ``counted`` — число получено подсчётом, надёжного перечня мест нет;
* ``no_basis`` — у значения не найдено основания;
* ``resolved`` — основание найдено при проверке.

Правила привязки (найденное в другом месте документа не годится):

* марка подтверждается, если на СВОЕЙ странице и стороне ближайшая к доказательству
  надпись того же вида (``QF\\d+.\\d+`` и т.п.) — именно она, не дальше ``NEAR_PT`` и
  без близкого конкурента; иначе — «есть рядом, привязка не однозначна»;
* число «в N местах» сверяется с числом уникальных мест (рамок) среди доказательств;
* номер записи (G528, T32) в тексте пункта — ссылка, а не значение.

Всё найденное после вызова помечается ``added_at_check`` и модели не отправлялось.
Предупреждения, поставленные программой, снимаются, только если основание найдено
для каждого сомнительного поля; пункты, которые ИИ сам отметил как неоднозначные,
остаются в «Нужна проверка» — к ним прикладывается найденное основание.
"""
from __future__ import annotations

import difflib
import json
import math
import re
from pathlib import Path
from typing import Any

from backend.app.services.version_comparison import paths, pdfio

from . import context as ctx
from . import geometry as geo
from . import images as img_mod
from . import settings
from .validate import _norm, _numbers, _side_texts

VERIFY_VERSION = "vv2"   # vv2: рамки строк СО для перехода
NEAR_PT = 40.0                 # марка не дальше этого от доказательства
UNIQUE_RATIO, UNIQUE_MARGIN = 1.5, 6.0   # конкурент того же вида — не ближе
REFLOW_PT = 80.0               # поиск удалённого текста в B при переразбивке строк
SAME_BOX_PT = 0.6
CROP_MARGIN_PT = 24.0
CROP_LONG_PX = 1600
CROP_DPI = (300, 900)
ADDED = "добавлено при проверке; модели не отправлялось"

CLASS_LABELS = {
    "contradiction": "реальное противоречие между источниками",
    "unreadable": "нечитаемое или отсутствующее значение",
    "image_only": "прочитано ИИ с изображения, автоматически не подтверждено",
    "counted": "число получено подсчётом, надёжного перечня мест нет",
    "no_basis": "основание значения не найдено",
    "resolved": "основание найдено при проверке",
}
CLASS_ORDER = ("contradiction", "unreadable", "no_basis", "image_only", "counted", "resolved")
TOKEN_LABELS = {
    "id_reference": "номер записи в тексте пункта — ссылка, не значение",
    "context_confirmed": "подтверждено окружающим текстом PDF на своей странице и стороне",
    "context_near": "есть в тексте PDF рядом, но однозначная привязка к объекту не установлена",
    "image_only": "в тексте PDF рядом нет — прочитано ИИ с изображения, автоматически не подтверждено",
    "absent": "в тексте страницы не найдено",
    "count_confirmed": "число совпадает с перечнем уникальных мест",
    "count_unconfirmed": "число не совпадает с перечнем уникальных мест — точное количество не утверждается",
    "direction_context": "значение найдено на своей стороне рядом с записью",
    "direction_unresolved": "значение на своей стороне не найдено",
}
_SPAN = re.compile(r"[0-9A-Za-zА-Яа-яЁё][0-9A-Za-zА-Яа-яЁё.\-/*]*")
_IDREF = re.compile(r"^[TGMSZWNXCI]\d+$")


# ------------------------------------------------------------------ геометрия и слова

def _dist(a: list[float], b: list[float]) -> float:
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def _words(pages: ctx.Pages, side: str, page: int | None) -> list[dict[str, Any]]:
    if not page:
        return []
    return [{"t": str(w[4]), "n": ctx.norm_mark(str(w[4])), "box": [float(x) for x in w[:4]]}
            for w in pages.words(side, page)]


def _family(token: str) -> re.Pattern:
    """Шаблон надписей того же вида: цифры — любые, остальное как в марке."""
    out, i, t = [], 0, ctx.norm_mark(token)
    while i < len(t):
        if t[i].isdigit():
            while i < len(t) and t[i].isdigit():
                i += 1
            out.append(r"\d+")
        else:
            out.append(re.escape(t[i]))
            i += 1
    return re.compile("".join(out))


def _side_boxes(recs: list[dict], side: str) -> list[tuple[str, list[float]]]:
    key = "bbox_a" if side == "A" else "bbox_b"
    return [(r["id"], r[key]) for r in recs if r.get(key)]


def _w_ref(side: str, page: int, w: dict, d: float | None = None) -> dict[str, Any]:
    out = {"side": side, "page": page, "text": w["t"], "box": geo.rounded(w["box"], 1), "source": "текст PDF",
           "note": ADDED}
    if d is not None:
        out["distance_pt"] = round(d, 1)
    return out


# ------------------------------------------------------------------ токены пункта

def suspect_tokens(packet: dict, item: dict) -> list[dict[str, Any]]:
    """Числа и марки текста пункта без опоры в доказательствах — как в ``validate``, но с разбором вида."""
    recs = {r["id"]: r for r in packet["records"]}
    contexts = {c["id"]: c for c in packet.get("contexts") or []}
    xrefs = {x["id"]: x for x in packet.get("xrefs") or []}
    ev = [e for e in item["evidence_ids"] if e in recs]
    support = " ".join(_side_texts(recs[e], contexts, xrefs, s) for e in ev for s in ("a", "b"))
    support += " " + " ".join(_norm(x.get("text") or "") + " " + _norm(" ".join((x.get("fields") or {}).values()))
                              for c in item["context_ids"] for x in [xrefs.get(c) or {}])
    support += " " + " ".join(_norm(f"{v['before']} {v['after']} {v['unit']}") for v in item["values"])
    summary = item["summary"]
    missing_nums = _numbers(summary) - _numbers(support)
    missing_marks = {m for m in ctx.marks(summary) if _norm(m) not in support}
    out: dict[str, dict[str, Any]] = {}
    for m in _SPAN.finditer(summary):
        span = m.group(0).rstrip(".-/*")
        nums = _numbers(span)
        if not nums or not (nums & missing_nums or span in missing_marks or any(span in mm for mm in missing_marks)):
            continue
        if _IDREF.match(span) and (span in recs or span in contexts or span in xrefs):
            out.setdefault(span, {"token": span, "kind": "id_reference"})
        elif re.search(r"[A-Za-zА-Яа-яЁё]", span):
            core = span.rstrip("*")
            out.setdefault(core, {"token": core, "kind": "mark"})
        elif re.fullmatch(r"\d+", span):
            out.setdefault(span, {"token": span, "kind": "number"})
        else:
            out.setdefault(span, {"token": span, "kind": "value"})
    return list(out.values())


def check_mark(pages: ctx.Pages, packet: dict, recs: list[dict], token: str) -> dict[str, Any]:
    fam, tok = _family(token), ctx.norm_mark(token)
    sides: dict[str, Any] = {}
    for side in ("A", "B"):
        boxes = _side_boxes(recs, side)
        if not boxes:
            continue
        page = packet["a" if side == "A" else "b"]["page"]
        cands = []
        for w in _words(pages, side, page):
            mm = fam.search(w["n"])
            if mm:
                cands.append((w, mm.group(0)))
        per_rec, near_hits = [], []
        for rid, box in boxes:
            ranked = sorted(((_dist(w["box"], box), w, txt) for w, txt in cands), key=lambda x: x[0])
            if not ranked:
                continue
            d0, w0, t0 = ranked[0]
            rival = next((d for d, _w, t in ranked[1:] if t != t0), None)
            unique = t0 == tok and d0 <= NEAR_PT and (rival is None or rival >= max(UNIQUE_RATIO * d0, d0 + UNIQUE_MARGIN))
            per_rec.append({"record": rid, "nearest": w0["t"], "nearest_is_token": t0 == tok,
                            "distance_pt": round(d0, 1), "rival_pt": None if rival is None else round(rival, 1),
                            "unique": unique})
            near_hits += [(d, w) for d, w, t in ranked if t == tok and d <= NEAR_PT]
        on_page = [w for w, t in cands if t == tok]
        best = min(near_hits, key=lambda x: x[0]) if near_hits else None
        # Однозначно: хотя бы одна запись в пороге и ни одна запись не ближе к другой надписи того же вида.
        unique = any(p["unique"] for p in per_rec) and all(p["nearest_is_token"] for p in per_rec)
        state = ("unique" if unique else "near" if near_hits
                 else "elsewhere" if on_page else "absent")
        sides[side] = {"page": page, "state": state, "records": per_rec,
                       "coverage": f"{sum(p['unique'] for p in per_rec)}/{len(per_rec)} в пороге, "
                                   f"{sum(p['nearest_is_token'] for p in per_rec)}/{len(per_rec)} ближайшая",
                       "context": _w_ref(side, page, best[1], best[0]) if best else None}
    states = [s["state"] for s in sides.values()]
    if states and all(s == "unique" for s in states):
        status = "context_confirmed"
    elif states and all(s in ("unique", "near") for s in states):
        status = "context_near"
    elif any(s in ("near", "unique", "elsewhere") for s in states):
        status = "image_only"
    else:
        status = "absent"
    return {"token": token, "kind": "mark", "status": status, "sides": sides}


def unique_places(recs: list[dict], side: str) -> list[dict[str, Any]]:
    """Уникальные места: рамки, совпадающие с точностью SAME_BOX_PT, — одно место."""
    places: list[dict[str, Any]] = []
    for rid, box in _side_boxes(recs, side):
        hit = next((p for p in places if max(abs(a - b) for a, b in zip(p["box"], box)) <= SAME_BOX_PT), None)
        if hit:
            hit["records"].append(rid)
        else:
            places.append({"box": geo.rounded(box, 1), "records": [rid]})
    return places


def check_count(recs: list[dict], n: int) -> dict[str, Any]:
    kinds = {r["kind"] for r in recs}
    side = "A" if kinds == {"removed"} else "B"
    places = unique_places(recs, side)
    ok = len(places) == n and len(places) == len(recs)
    return {"token": str(n), "kind": "number", "status": "count_confirmed" if ok else "count_unconfirmed",
            "side": side, "unique_places": len(places), "records": len(recs),
            "places": [{"records": p["records"], "box": p["box"]} for p in places]}


def check_direction(pages: ctx.Pages, packet: dict, rec: dict, value: str, side: str) -> dict[str, Any]:
    """«Было» на стороне A / «стало» на стороне B рядом с записью (запись могла быть только на другой стороне)."""
    page = packet["a" if side == "A" else "b"]["page"]
    tr = {"matrix": packet["coordinates"]["transform_b_to_a"]}
    box = rec.get("bbox_a" if side == "A" else "bbox_b")
    if not box:
        other = rec.get("bbox_b") if side == "A" else rec.get("bbox_a")
        box = (geo.b_page_to_a(tr, other) if side == "A" else geo.a_to_b_page(tr, other)) if other else None
    target = ctx.norm_mark(value)
    hits = sorted(((_dist(w["box"], box), w) for w in _words(pages, side, page) if box and w["n"] == target),
                  key=lambda x: x[0])
    if hits and hits[0][0] <= NEAR_PT:
        return {"value": value, "side": side, "status": "direction_context",
                "context": _w_ref(side, page, hits[0][1], hits[0][0])}
    return {"value": value, "side": side, "status": "direction_unresolved"}


# ------------------------------------------------------------------ правила для пунктов, отмеченных ИИ

def _reflow(pages: ctx.Pages, packet: dict, recs: list[dict]) -> dict[str, Any] | None:
    """Удалённые фрагменты текста есть в B рядом, если не учитывать пробелы и переносы строк."""
    removed = [r for r in recs if r["kind"] == "removed" and r["type"] == "text"]
    if not removed or len(removed) != len(recs):
        return None
    tr = {"matrix": packet["coordinates"]["transform_b_to_a"]}
    area = geo.a_to_b_page(tr, geo.union([r["bbox_a"] for r in removed]))
    near = [w for w in _words(pages, "B", packet["b"]["page"]) if _dist(w["box"], area) <= REFLOW_PT]
    near.sort(key=lambda w: (round(w["box"][1] / 4), w["box"][0]))
    flat = "".join(w["t"] for w in near).replace(" ", "")
    if all(r["before"].replace(" ", "") in flat for r in removed):
        lines: dict[int, list[str]] = {}
        for w in near:
            lines.setdefault(round(w["box"][1] / 4), []).append(w["t"])
        return {"found": True, "b_text": [" ".join(v) for _k, v in sorted(lines.items())],
                "b_page": packet["b"]["page"], "b_area": geo.rounded(geo.expand(area, REFLOW_PT), 1), "note": ADDED}
    return None


def _duplicate_text(pages: ctx.Pages, packet: dict, recs: list[dict]) -> dict[str, Any] | None:
    """Удалённый символ — дубль: в A одинаковый текст дважды в одной рамке, в B — один раз там же."""
    if len(recs) != 1 or recs[0]["kind"] != "removed" or not recs[0].get("bbox_a"):
        return None
    r = recs[0]
    tr = {"matrix": packet["coordinates"]["transform_b_to_a"]}
    same = lambda ws, box: [w for w in ws if w["t"] == r["before"] and _dist(w["box"], box) <= SAME_BOX_PT]
    in_a = same(_words(pages, "A", packet["a"]["page"]), r["bbox_a"])
    in_b = same(_words(pages, "B", packet["b"]["page"]), geo.a_to_b_page(tr, r["bbox_a"]))
    if len(in_a) >= 2 and len(in_b) == len(in_a) - 1:
        return {"a_copies": len(in_a), "b_copies": len(in_b), "box": geo.rounded(r["bbox_a"], 1), "note": ADDED}
    return None


def _overlay(pages: ctx.Pages, packet: dict, recs: list[dict]) -> dict[str, Any] | None:
    """Добавленные в B слова поля легли поверх существующих слов (наложение текста)."""
    if len(recs) != 1 or recs[0]["kind"] != "changed":
        return None
    r = recs[0]
    sm = difflib.SequenceMatcher(a=r["before"].split(), b=r["after"].split())
    added = [w for op, _i1, _i2, j1, j2 in sm.get_opcodes() if op in ("insert", "replace") for w in r["after"].split()[j1:j2]]
    if not added or not r.get("bbox_b"):
        return None
    page = packet["b"]["page"]
    ws = [w for w in _words(pages, "B", page) if geo.intersects(w["box"], r["bbox_b"])]
    ws_a = {(w["t"], tuple(round(x) for x in w["box"])) for w in _words(pages, "A", packet["a"]["page"])}
    new = [w for w in ws if w["t"] in added and (w["t"], tuple(round(x) for x in w["box"])) not in ws_a]
    hits = []
    for w in new:
        under = [o for o in ws if o is not w and o not in new and geo.intersects(o["box"], geo.expand(w["box"], -0.5))]
        if under:
            hits.append({"added": _w_ref("B", page, w), "covers": [_w_ref("B", page, o) for o in under]})
    return {"added_words": added, "overlaps": hits} if hits else None


def _cross_objects(pages: ctx.Pages, packet: dict, item: dict, recs: list[dict]) -> list[dict[str, Any]]:
    """Расхождение схема ↔ СО: объект каждой записи (метка в её колонке) и строка СО B с этим объектом."""
    labels = [m for m in ctx.marks(item["summary"]) if re.search(r"\d+$", m)]
    if not labels:
        return []
    fam = _family(labels[0])
    xrefs = {x["id"]: x for x in packet.get("xrefs") or []}
    sent = [xrefs[c] for c in item["context_ids"] if c in xrefs and xrefs[c].get("kind") == "spec_position"
            and xrefs[c]["side"] == "B"]
    spec_b = pages.spec("B") or {}
    rows = []
    for r in recs:
        if r["type"] != "text" or not r.get("bbox_b"):
            continue
        ws = [w for w in _words(pages, "B", packet["b"]["page"]) if fam.search(w["n"])
              and min(w["box"][2], r["bbox_b"][2]) + 6 >= max(w["box"][0], r["bbox_b"][0])]
        if not ws:
            continue
        w = min(ws, key=lambda w: _dist(w["box"], r["bbox_b"]))
        obj = fam.search(w["n"]).group(0).upper()
        removed = [b for op, i1, i2, _j1, _j2 in difflib.SequenceMatcher(a=r["before"], b=r["after"]).get_opcodes()
                   if op in ("delete", "replace") for b in [r["before"][i1:i2]]]
        tok = re.compile(rf"(?<![\w-]){re.escape(obj)}(?!\d)", re.IGNORECASE)
        via_sent = next((x for x in sent if tok.search(ctx.norm_mark((x.get("fields") or {}).get("note") or "").upper())), None)
        row = None
        if via_sent:
            f = via_sent["fields"]
            row = {"page": via_sent["page"], "mark": f.get("mark"), "note": f.get("note"), "qty": f.get("qty"),
                   "box": via_sent.get("bbox"), "xref": via_sent["id"],
                   "source": f"контекст {via_sent['id']}, отправлен модели", "sent": True}
        else:
            for it in spec_b.get("items") or []:
                f = ctx.spec_fields(it)
                if tok.search((f.get("note") or "").upper()):
                    row = {"page": it.get("page"), "mark": f.get("mark"), "note": f.get("note"), "qty": f.get("qty"),
                           "box": geo.rounded(geo.union([r[1:5] for r in it.get("rows") or []
                                                         if int(r[0]) == int(it.get("page") or 0)]), 1),
                           "source": "СО движка (позиции стороны B)", "sent": False, "note_added": ADDED}
                    break
            if not row:
                row = _note_column_row(pages, spec_b, obj)
        frag = [x for x in ("ПЧ", "МД") if any(x in s for s in removed)]
        mark = ctx.norm_mark((row or {}).get("mark") or "")
        rows.append({"record": r["id"], "object": obj, "label_box_b": geo.rounded(w["box"], 1),
                     "schema_before": r["before"], "schema_after": r["after"], "removed_fragments": frag,
                     "spec_b": row, "in_spec_b": {x: ctx.norm_mark(x) in mark for x in frag}})
    # Подтверждение — по объекту: изменение одной подписи может быть разбито на несколько записей.
    by_obj: dict[str, list[dict]] = {}
    for x in rows:
        by_obj.setdefault(x["object"], []).append(x)
    for obj, xs in by_obj.items():
        removed_any = sorted({f for x in xs for f in x["removed_fragments"]})
        spec = next((x["spec_b"] for x in xs if x["spec_b"]), None)
        mark = ctx.norm_mark((spec or {}).get("mark") or "")
        ok = bool(spec and "ПЧ" in removed_any and ctx.norm_mark("ПЧ") in mark)
        for x in xs:
            x["object_confirmed"] = ok
            x["object_removed"] = removed_any
    return rows


def _note_column_row(pages: ctx.Pages, spec_b: dict, obj: str) -> dict[str, Any] | None:
    """Объект есть на листе СО в столбце примечаний, а разборщик СО оставил примечание строки пустым."""
    pages_b = sorted({it.get("page") for it in spec_b.get("items") or [] if it.get("page")})
    tok = re.compile(rf"^{re.escape(obj)}(?!\d)", re.IGNORECASE)
    for page in pages_b:
        hits = [w for w in _words(pages, "B", page) if tok.search(w["t"].upper())]
        for w in hits:
            yc = (w["box"][1] + w["box"][3]) / 2
            for it in spec_b.get("items") or []:
                rr = [r for r in it.get("rows") or [] if int(r[0]) == page]
                if rr and any(r[2] - 6 <= yc <= r[4] + 6 for r in rr) and not ctx.spec_fields(it).get("note"):
                    f = ctx.spec_fields(it)
                    return {"page": page, "mark": f.get("mark"), "note": "", "qty": f.get("qty"),
                            "box": geo.rounded(geo.union([r[1:5] for r in rr] + [w["box"]]), 1),
                            "note_from_page_text": _w_ref("B", page, w),
                            "source": "строка СО движка с пустым примечанием + текст столбца примечаний на той же "
                                      "высоте", "sent": False, "note_added": ADDED}
    return None


# ------------------------------------------------------------------ вырезки для человека

def zoom_crops(docs: dict, packet: dict, recs: list[dict], out_dir: Path, name: str) -> list[dict[str, Any]]:
    out = []
    for side in ("A", "B"):
        boxes = [b for _rid, b in _side_boxes(recs, side)]
        if not boxes:
            continue
        box = geo.expand(geo.union(boxes), CROP_MARGIN_PT)
        long_pt = max(box[2] - box[0], box[3] - box[1])
        dpi = max(CROP_DPI[0], min(CROP_DPI[1], CROP_LONG_PX * 72.0 / max(long_pt, 1.0)))
        page = packet["a" if side == "A" else "b"]["page"]
        img, used = img_mod.render_crop(docs[side], page, box, dpi)
        if img is None:
            continue
        s = dpi / 72.0
        marks = [(rid, [(b[0] - used[0]) * s, (b[1] - used[1]) * s, (b[2] - used[0]) * s, (b[3] - used[1]) * s])
                 for rid, b in _side_boxes(recs, side)]
        img = img_mod.draw_boxes(img, marks)
        f = out_dir / f"{name}_{side}.png"
        sha = img_mod.save_png(img, f)
        out.append({"side": side, "page": page, "box_pt": geo.rounded(used, 1), "dpi": round(dpi),
                    "file": f.name, "sha256": sha, "purpose": "увеличенная вырезка из исходного PDF для просмотра "
                    "человеком; модели не отправлялась"})
    return out


# ------------------------------------------------------------------ пункт и лист

def _worst(classes: list[str]) -> str:
    return min(classes, key=CLASS_ORDER.index) if classes else "resolved"


def verify_item(pages: ctx.Pages, docs: dict | None, packet: dict, item: dict, issues: list[dict],
                crop_dir: Path | None) -> dict[str, Any]:
    recs_all = {r["id"]: r for r in packet["records"]}
    recs = [recs_all[e] for e in item["evidence_ids"] if e in recs_all]
    flags = item.get("flags") or []
    by_program = bool(flags) and bool(item.get("original_category"))
    tokens: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    classes: list[str] = []
    field_doubts: list[str] = []
    affects_main = False

    if "unsupported_token" in flags:
        for t in suspect_tokens(packet, item):
            if t["kind"] == "id_reference":
                tokens.append({**t, "status": "id_reference"})
            elif t["kind"] == "mark":
                res = check_mark(pages, packet, recs, t["token"])
                tokens.append(res)
                if res["status"] != "context_confirmed":
                    field_doubts.append(t["token"])
                    affects_main = True        # марка указывает, КАКОЙ объект изменён
                    classes.append({"context_near": "image_only", "image_only": "image_only"}.get(res["status"], "no_basis"))
            elif t["kind"] == "number" and len(recs) > 1:
                res = check_count(recs, int(t["token"]))
                tokens.append(res)
                if res["status"] != "count_confirmed":
                    field_doubts.append(t["token"])
                    classes.append("counted")
            else:
                tokens.append({**t, "status": "absent"})
                field_doubts.append(t["token"])
                affects_main = True
                classes.append("no_basis")
    if "direction_unverified" in flags:
        for v in item["values"]:
            r = recs_all.get(v["evidence_id"])
            if not r:
                continue
            for value, side in ((v["before"], "A"), (v["after"], "B")):
                if not value or _norm(value) in _side_texts(r, {}, {}, side.lower()):
                    continue
                res = check_direction(pages, packet, r, value, side)
                tokens.append({"token": value, "kind": "direction", **res})
                if res["status"] != "direction_context":
                    field_doubts.append(value)
                    affects_main = True
                    classes.append("no_basis")
    for f in ("no_evidence", "cross_check_one_side", "unreadable_not_flagged", "graphic_without_image"):
        if f in flags:
            classes.append("no_basis" if f != "unreadable_not_flagged" else "unreadable")
            affects_main = True

    if not by_program:
        # Пункт отметил сам ИИ — ищем основание его сомнения.
        if any("unreadable" in (r.get("flags") or []) for r in recs):
            classes.append("unreadable")
            findings.append({"rule": "unreadable_text", "text": "Значение в B — символы «#»: текстовый слой не "
                             "содержит знаков. Значение не восстанавливается."})
        elif item.get("cross_check"):
            rows = _cross_objects(pages, packet, item, recs)
            ok = [x for x in rows if x["object_confirmed"]]
            findings.append({"rule": "cross_objects", "rows": rows,
                             "text": f"Объектов в пункте: {len({x['object'] for x in rows})}; расхождение с "
                                     f"позицией СО B подтверждено для {len({x['object'] for x in ok})}."})
            classes.append("contradiction" if rows and len(ok) == len(rows) else "no_basis")
        elif (dup := _duplicate_text(pages, packet, recs)):
            findings.append({"rule": "duplicate_text_layer", **dup,
                             "text": f"В текстовом слое A символ записан {dup['a_copies']} раза в одной рамке, в B — "
                                     f"{dup['b_copies']}; видимого изменения нет."})
            classes.append("resolved")
        elif (rf := _reflow(pages, packet, recs)):
            findings.append({"rule": "reflow", **rf,
                             "text": "Удалённые фрагменты есть в B рядом: текст сохранён, изменена разбивка строк."})
            classes.append("resolved")
        elif (ov := _overlay(pages, packet, recs)):
            findings.append({"rule": "text_overlay", **ov,
                             "text": "Добавленный в B текст лёг поверх существующего значения — значение перекрыто."})
            classes.append("unreadable")
        elif recs and all(r["type"] in ("region", "block", "annotation") for r in recs):
            findings.append({"rule": "small_graphic", "text": "Графическое отличие слишком мало для интерпретации "
                             "по вырезке; смысл изменения не установлен."})
            classes.append("unreadable")
        else:
            classes.append("no_basis")

    cls = _worst(classes)
    if by_program and cls == "resolved":
        final = item["original_category"]
    else:
        final = "needs_check"
    crops = []
    if docs is not None and crop_dir is not None and cls in ("unreadable", "image_only", "no_basis", "counted"):
        crop_dir.mkdir(parents=True, exist_ok=True)
        crops = zoom_crops(docs, packet, recs, crop_dir, item["item_id"].replace(".", "_"))
    return {
        "item_id": item["item_id"], "set_by": "program" if by_program else "ai",
        "prior": {"category": item["category"], "original_category": item.get("original_category"), "flags": flags,
                  "issues": [f"{i['code']}: {i.get('detail', '')}" for i in issues]},
        "tokens": [dict(t, status_label=TOKEN_LABELS.get(t.get("status"), "")) for t in tokens],
        "findings": findings, "class": cls, "class_label": CLASS_LABELS[cls], "final_category": final,
        "field_doubts": field_doubts, "main_conclusion_affected": affects_main or final == "needs_check",
        "crops": crops,
    }


def _issues_by_item(res: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for i in res.get("issues") or []:
        d = i.get("detail") or ""
        m = re.match(r"пункт (I\d+)", d)
        if m:
            out.setdefault(i["chunk_id"].rsplit("-", 1)[1] + "." + m.group(1), []).append(i)
    return out


def layer_key(res_sha: str, pkt_sha: str, packet: dict) -> str:
    return settings.digest({"v": VERIFY_VERSION, "result": res_sha, "packet": pkt_sha,
                            "pdf": [packet["a"]["sha256"], packet["b"]["sha256"]],
                            "near": NEAR_PT, "unique": [UNIQUE_RATIO, UNIQUE_MARGIN], "reflow": REFLOW_PT})[:16]


def build(sheet_dir: Path, object_id: str, *, crops: bool = True) -> dict[str, Any]:
    """Построить (или взять из кэша) слой проверки листа. Исходные файлы только читаются."""
    from .packet import _sha_file
    res_f, pkt_f = sheet_dir / "result.json", sheet_dir / "packet" / "packet.json"
    res = json.loads(res_f.read_text(encoding="utf-8"))
    packet = json.loads(pkt_f.read_text(encoding="utf-8"))
    key = layer_key(_sha_file(res_f), _sha_file(pkt_f), packet)
    ldir = sheet_dir / "layers" / f"verify-{VERIFY_VERSION}-{key}"
    out_f = ldir / "verification.json"
    if out_f.exists():
        return json.loads(out_f.read_text(encoding="utf-8"))
    pages = ctx.Pages(object_id, packet["a"]["sha256"], packet["b"]["sha256"])
    docs = None
    if crops:
        docs = {s: pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, packet[s.lower()]["sha256"])))
                for s in ("A", "B")}
    issues = _issues_by_item(res)
    items = {}
    for it in res["items"]:
        if it["category"] != "needs_check":
            continue
        items[it["item_id"]] = verify_item(pages, docs, packet, it, issues.get(it["item_id"], []),
                                           ldir / "crops" if crops else None)
    out = {"schema": "version_comparison_ai.verification/v1", "version": VERIFY_VERSION, "key": key,
           "spid": res["spid"], "inputs": {"result_sha256": _sha_file(res_f), "packet_sha256": _sha_file(pkt_f)},
           "note": "Проверка выполнена программой после ответа модели; найденный контекст модели не отправлялся. "
                   "Инженером не подтверждено.",
           "thresholds": {"near_pt": NEAR_PT, "unique_ratio": UNIQUE_RATIO, "unique_margin_pt": UNIQUE_MARGIN,
                          "reflow_pt": REFLOW_PT},
           "items": items}
    ldir.mkdir(parents=True, exist_ok=True)
    out_f.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def latest(sheet_dir: Path) -> dict[str, Any] | None:
    """Последний построенный слой проверки текущей версии (без построения)."""
    found = sorted((sheet_dir / "layers").glob(f"verify-{VERIFY_VERSION}-*/verification.json"),
                   key=lambda p: p.stat().st_mtime)
    return json.loads(found[-1].read_text(encoding="utf-8")) if found else None


def crop_file(sheet_dir: Path, name: str) -> Path | None:
    """Файл увеличенной вырезки последнего слоя проверки (имя — только из слоя)."""
    v = latest(sheet_dir)
    if not v or not re.fullmatch(r"[A-Za-z0-9_]+_[AB]\.png", name):
        return None
    known = {c["file"] for it in v["items"].values() for c in it.get("crops") or []}
    if name not in known:
        return None
    return sheet_dir / "layers" / f"verify-{v['version']}-{v['key']}" / "crops" / name
