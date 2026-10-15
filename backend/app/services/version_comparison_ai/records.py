"""Записи листа для ИИ: каждая запись результата движка с ID и группой учёта.

Группы (план §4.2 с поправкой А от 15.10):

* ``ai`` — судьбу назначает модель;
* ``engine`` — запись классифицирована движком (сдвиг, оформление линии,
  пробелы, служебное поле даты/QR, служебная зона) и в ИИ не уходит;
* ``linked`` — текстовый регион, целиком лежащий в рамках связанных текстовых
  изменений: учтён через связь, **отдельно не проверялся**.

Регион «текст», рамка которого выходит за связанные надписи, содержит графику
(линию, знак, подключение) и идёт в ИИ как отдельная запись ``G`` — разбор
надписи рядом его не закрывает.
"""
from __future__ import annotations

import re
from typing import Any

from . import geometry as geo
from . import ids, settings

SHIFT_CLASSES = ("text_moved", "shifted", "block_moved")
SERVICE_FIELDS_AGGREGATED = ("date", "qr")
SERVICE_OVERLAYS = ("release_stamp", "qr")
UNREADABLE = re.compile(r"#{3,}")

# Пределы усечения result.json (engine.py: text.changes[:2000], only_a/only_b/moved[:500],
# у региона text_before/text_after[:40]).
TEXT_CHANGES_CAP = 2000
WORDS_CAP = 500
REGION_TEXT_CAP = 40

FATE_LABELS = {
    "engine_shift": "сдвиг (класс движка)",
    "engine_style": "оформление линии (класс движка)",
    "engine_whitespace": "только пробелы (класс движка)",
    "engine_renumbered": "только номер позиции (класс движка)",
    "service": "служебное поле (дата выпуска / QR)",
    "service_zone": "служебная зона без полей",
    "linked": "учтено через связь с текстовым изменением",
}


def _zone_bbox(rec: dict[str, Any] | None, zone: str) -> list[float] | None:
    if not rec:
        return None
    want = "release_stamp" if zone in ("qr", "release_stamp") else zone
    boxes = [z["bbox"] for z in rec.get("zones", []) if z.get("type") == want]
    return geo.union(boxes)


def _rows_box(rows: list[list[float]], page: int | None) -> list[float] | None:
    if page is None:
        return None
    return geo.union([r[1:5] for r in rows or [] if int(r[0]) == int(page)])


def _record(**kw: Any) -> dict[str, Any]:
    base = {"id": None, "ref": None, "type": None, "kind": None, "before": "", "after": "",
            "bbox_a": None, "bbox_b": None, "box": None, "group": "ai", "fate": None,
            "flags": [], "links": [], "label": ""}
    base.update(kw)
    for key in ("bbox_a", "bbox_b", "box"):
        base[key] = geo.rounded(base[key])
    return base


def build(result: dict[str, Any], item: dict[str, Any], rec_a: dict[str, Any] | None,
          rec_b: dict[str, Any] | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Записи листа и сведения об усечении. ``result`` — из ``jobs.sheet_result``."""
    tr = result.get("transform") or {"matrix": [[1, 0, 0], [0, 1, 0]]}
    include_engine = settings.include_engine_classified()
    out: list[dict[str, Any]] = []
    text = result.get("text") or {}
    changes = text.get("changes") or []
    by_tid = {c["id"]: c for c in changes}

    for c in changes:
        flags = []
        if UNREADABLE.search(c.get("after") or "") or UNREADABLE.search(c.get("before") or ""):
            flags.append("unreadable")
        if c.get("block"):
            flags.append("in_moved_block")
        if c.get("norm") in ("symbol", "number_format"):
            flags.append(f"norm_{c['norm']}")
        group, fate = "ai", None
        if c["kind"] == "moved":
            group, fate = "engine", "engine_shift"
        elif c.get("norm") == "spaces":
            group, fate = "engine", "engine_whitespace"
        if group == "engine" and include_engine:
            group, fate = "ai", None
        out.append(_record(id=ids.local("text", c["id"]), ref=f"text/{c['id']}", type="text", kind=c["kind"],
                           before=c.get("before") or "", after=c.get("after") or "",
                           bbox_a=c.get("a_bbox"), bbox_b=c.get("b_bbox"),
                           box=geo.union([c.get("a_bbox"), c.get("b_in_a_bbox")]),
                           group=group, fate=fate, flags=flags, numbers=c.get("numbers") or [],
                           shift_pt=c.get("shift_pt")))

    for g in result.get("regions") or []:
        cls = g["class"]
        rid = g["id"]
        typ = "block" if rid.startswith("b") else "region"
        links = [ids.local("text", t) for t in g.get("text_changes") or [] if t in by_tid]
        flags = ["text_unreadable"] if g.get("text_unreadable") else []
        group, fate = "ai", None
        if cls in SHIFT_CLASSES:
            group, fate = "engine", "engine_shift"
        elif cls == "style":
            group, fate = "engine", "engine_style"
        elif cls == "text" and links and not g.get("text_unreadable"):
            boxes = []
            for t in g.get("text_changes") or []:
                c = by_tid.get(t)
                if c:
                    boxes += [c.get("a_bbox"), c.get("b_in_a_bbox")]
            text_box = geo.union(boxes)
            beyond = settings.text_beyond_pt()
            if text_box and geo.contains(geo.expand(text_box, beyond), g["bbox_a_pt"]):
                group, fate = "linked", "linked"
            else:
                flags.append("text_and_graphics")
        if group == "engine" and include_engine:
            group, fate = "ai", None
        if len(g.get("text_before") or []) >= REGION_TEXT_CAP or len(g.get("text_after") or []) >= REGION_TEXT_CAP:
            flags.append("region_text_truncated")
        out.append(_record(id=ids.local(typ, rid), ref=f"region/{rid}", type=typ, kind=cls,
                           before=" ".join(g.get("text_before") or []), after=" ".join(g.get("text_after") or []),
                           bbox_a=g.get("bbox_a_pt"), bbox_b=g.get("bbox_b_pt"),
                           box=g.get("bbox_b_in_a_pt") and geo.union([g["bbox_a_pt"], g["bbox_b_in_a_pt"]])
                           or g.get("bbox_a_pt"),
                           bbox_px=g.get("bbox_px"), group=group, fate=fate, flags=flags, links=links,
                           pixels={k: g.get(k, 0) for k in ("a_only_px", "b_only_px", "light_a_px", "light_b_px",
                                                            "color_px", "style_px")},
                           moved_text=" ".join(g.get("moved_text") or []), shift_pt=g.get("shift_pt")))

    a_page, b_page = item.get("a_page"), item.get("b_page")
    for s in (result.get("spec_page") or {}).get("items") or []:
        ba = _rows_box(s.get("a_rows"), a_page)
        bb = _rows_box(s.get("b_rows"), b_page)
        box = geo.union([ba, geo.b_page_to_a(tr, bb) if bb else None])
        fields = s.get("fields") or {}
        before = "; ".join(f"{k}: {v[0]}" for k, v in fields.items())
        after = "; ".join(f"{k}: {v[1]}" for k, v in fields.items())
        group, fate = ("engine", "engine_renumbered") if s["kind"] == "renumbered" else ("ai", None)
        out.append(_record(id=ids.local("spec", s["id"]), ref=f"spec/{s['id']}", type="spec", kind=s["kind"],
                           before=before, after=after, bbox_a=ba, bbox_b=bb, box=box, group=group, fate=fate,
                           flags=["ambiguous"] if s.get("ambiguous") else [], label=s.get("label") or "",
                           section=s.get("section") or "", fields=fields,
                           a_pages=s.get("a_pages") or [], b_pages=s.get("b_pages") or []))

    n = 0
    for f in result.get("service") or []:
        n += 1
        ba, bb = _zone_bbox(rec_a, f.get("zone")), _zone_bbox(rec_b, f.get("zone"))
        group, fate = ("engine", "service") if f.get("field") in SERVICE_FIELDS_AGGREGATED else ("ai", None)
        out.append(_record(id=ids.local("service", str(n)), ref=f"service/{f.get('zone')}.{f.get('field')}",
                           type="service", kind=f.get("change"), before=f.get("before") or "",
                           after=f.get("after") or "", bbox_a=ba, bbox_b=bb,
                           box=ba or (geo.b_page_to_a(tr, bb) if bb else None), group=group, fate=fate,
                           flags=["zone_bbox"] if (ba or bb) else [], label=f.get("label") or "",
                           field=f.get("field"), zone=f.get("zone")))
    for z in result.get("zones") or []:
        if not z.get("changed"):
            continue
        n += 1
        ba, bb = _zone_bbox(rec_a, z["type"]), _zone_bbox(rec_b, z["type"])
        out.append(_record(id=ids.local("service", str(n)), ref=f"zone/{z['type']}", type="zone", kind="changed",
                           before=" ".join(z.get("words_only_a") or []), after=" ".join(z.get("words_only_b") or []),
                           bbox_a=ba, bbox_b=bb, box=ba or (geo.b_page_to_a(tr, bb) if bb else None),
                           group="engine", fate="service_zone", flags=["zone_bbox"] if (ba or bb) else [],
                           label=f"служебная зона «{z['type']}»", zone=z["type"]))

    k = 0
    for o in result.get("overlays") or []:
        if o.get("change") == "same_text":
            continue
        k += 1
        service = o.get("kind") in SERVICE_OVERLAYS
        out.append(_record(id=ids.local("overlay", str(k)), ref=f"overlay/{k - 1}", type="overlay",
                           kind=o.get("change"), before=" | ".join(o.get("before") or []),
                           after=" | ".join(o.get("after") or []), group="engine" if service else "ai",
                           fate="service" if service else None, label=o.get("kind") or "",
                           flags=["warning_removed_not_approval"] if o.get("kind") == "warning"
                           and o.get("change") == "removed" else []))

    ann = result.get("annotations") or {}
    k = 0
    for side, items in (("added", ann.get("added") or []), ("removed", ann.get("removed") or []),
                        ("changed", ann.get("changed") or [])):
        for a in items:
            k += 1
            if side == "changed":
                ra, rb, before, after = a["before"].get("rect"), a["after"].get("rect"), a["before"].get("content"), \
                    a["after"].get("content")
                label = a["before"].get("subject") or a["before"].get("type")
            elif side == "added":
                ra, rb, before, after = None, a.get("rect"), "", a.get("content")
                label = a.get("subject") or a.get("type")
            else:
                ra, rb, before, after = a.get("rect"), None, a.get("content"), ""
                label = a.get("subject") or a.get("type")
            out.append(_record(id=ids.local("annotation", str(k)), ref=f"annotation/{side}/{k - 1}",
                               type="annotation", kind=side, before=before or "", after=after or "",
                               bbox_a=ra, bbox_b=rb, box=ra or (geo.b_page_to_a(tr, rb) if rb else None),
                               label=label or ""))

    truncated = {
        "text_changes": len(changes) >= TEXT_CHANGES_CAP,
        "words": any(len(text.get(k) or []) >= WORDS_CAP for k in ("only_a", "only_b", "moved")),
        "region_text": any("region_text_truncated" in r["flags"] for r in out),
    }
    return out, truncated


def counts(records: list[dict[str, Any]]) -> dict[str, int]:
    out = {"ai": 0, "engine": 0, "linked": 0}
    for r in records:
        out[r["group"]] += 1
    return out
