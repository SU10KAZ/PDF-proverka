"""Переход от пункта отчёта к области листа (план §9.3, поправка Б от 15.10).

Координаты берутся из ТЕКУЩЕГО результата движка по ID записей, а не из ответа
модели и не из снимка в отчёте. Контракт (см. ``geometry``): холст просмотрщика —
система A; рамки страницы B переводятся в неё **прямой** матрицей
``transform.matrix`` (страница B → система A), обратная нужна только для
перевода из системы A на страницу B.
"""
from __future__ import annotations

from typing import Any

from backend.app.services.version_comparison import jobs as vc_jobs

from . import geometry as geo
from . import ids


def focus(object_id: str, run_id: str, spid: str, evidence_ids: list[str],
          records: list[dict[str, Any]] | None = None, repair: bool = True) -> dict[str, Any]:
    """Рамки доказательств для просмотрщика: pt системы A, px холста, ID регионов."""
    data, _ = vc_jobs.sheet_result(object_id, run_id, spid, repair)
    res = data.get("result")
    if not res:
        return {"spid": spid, "available": False, "reason": "у листа нет наложения"}
    tr, canvas = res["transform"], res["canvas"]
    item = data["item"]
    tchanges = {ids.local("text", c["id"]): c for c in (res.get("text") or {}).get("changes") or []}
    regions = {}
    for g in res.get("regions") or []:
        regions[ids.local("block" if g["id"].startswith("b") else "region", g["id"])] = g
    spec = {ids.local("spec", s["id"]): s for s in (res.get("spec_page") or {}).get("items") or []}
    rec_by_id = {r["id"]: r for r in records or []}
    boxes, region_ids, missing = [], [], []
    for eid in evidence_ids:
        box = None
        if eid in regions:
            g = regions[eid]
            box = g.get("bbox_b_in_a_pt") and geo.union([g["bbox_a_pt"], g["bbox_b_in_a_pt"]]) or g["bbox_a_pt"]
            region_ids.append(g["id"])
        elif eid in tchanges:
            c = tchanges[eid]
            box = geo.union([c.get("a_bbox"), c.get("b_in_a_bbox")])
            region_ids += [g["id"] for g in res.get("regions") or [] if c["id"] in (g.get("text_changes") or [])]
        elif eid in spec:
            s = spec[eid]
            ra = [r[1:5] for r in s.get("a_rows") or [] if int(r[0]) == int(item.get("a_page") or -1)]
            rb = [geo.b_page_to_a(tr, r[1:5]) for r in s.get("b_rows") or [] if int(r[0]) == int(item.get("b_page") or -1)]
            box = geo.union(ra + rb)
        elif eid in rec_by_id and rec_by_id[eid].get("box"):
            box = rec_by_id[eid]["box"]      # служебная зона: рамка зоны из профиля движка
        if box:
            boxes.append({"id": eid, "box_a": geo.rounded(box), "px": geo.rounded(geo.to_px(canvas, box), 1)})
        else:
            missing.append(eid)
    union = geo.union([b["box_a"] for b in boxes])
    return {"spid": spid, "available": bool(boxes), "a_page": item.get("a_page"), "b_page": item.get("b_page"),
            "boxes": boxes, "union_a": geo.rounded(union), "union_px": geo.rounded(geo.to_px(canvas, union), 1)
            if union else None, "region_ids": sorted(set(region_ids)), "missing": missing,
            "union_b_page": geo.rounded(geo.a_to_b_page(tr, union)) if union else None}


def page_focus(object_id: str, run_id: str, side: str, page: int, boxes: list[tuple[str, list[float]]],
               prefer_spid: str | None = None, repair: bool = True) -> dict[str, Any]:
    """Переход к области ЛЮБОЙ страницы пары версий (например, к строке СО на другом листе).

    ``boxes`` — рамки в pt страницы ``page`` стороны ``side``. Лист прогона выбирается по
    номеру страницы этой стороны; рамки стороны B переводятся в систему A прямой матрицей.
    """
    run = vc_jobs.get_run(object_id, run_id, repair)
    key = "a_page" if side == "A" else "b_page"
    cands = [it for it in run["items"] if it.get(key) == page and it.get("kind") == "compare"]
    if not cands:
        return {"available": False, "reason": f"страница {page} стороны {side} не сопоставлена ни с одним листом"}
    item = next((it for it in cands if it["spid"] == prefer_spid), cands[0])
    data, _ = vc_jobs.sheet_result(object_id, run_id, item["spid"], repair)
    res = data.get("result")
    if not res:
        return {"available": False, "reason": "у листа нет наложения"}
    tr, canvas = res["transform"], res["canvas"]
    out = []
    for bid, box in boxes:
        box_a = geo.b_page_to_a(tr, box) if side == "B" else box
        out.append({"id": bid, "box_a": geo.rounded(box_a), "px": geo.rounded(geo.to_px(canvas, box_a), 1)})
    union = geo.union([b["box_a"] for b in out])
    return {"spid": item["spid"], "available": bool(out), "a_page": item.get("a_page"), "b_page": item.get("b_page"),
            "title": item.get("title"), "boxes": out, "union_px": geo.rounded(geo.to_px(canvas, union), 1) if union else None,
            "missing": [], "region_ids": []}
