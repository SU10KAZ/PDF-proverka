"""Нормализация блоков из result.json + IoU-сопоставление."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _to_int_safe(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _coerce_bbox(value: Any) -> list[float] | None:
    """Принять координаты в нескольких форматах и вернуть [x0, y0, x1, y1].

    Поддерживается:
      • [x0, y0, x1, y1] — bbox напрямую,
      • [[x, y], [x, y], ...] — polygon (берём min/max),
      • dict {x, y, width, height} или {x0/y0/x1/y1} или {left/top/right/bottom}.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        # x/y/width/height
        if "width" in value and "height" in value and ("x" in value or "left" in value):
            x = float(value.get("x", value.get("left", 0)) or 0)
            y = float(value.get("y", value.get("top", 0)) or 0)
            w = float(value.get("width", 0) or 0)
            h = float(value.get("height", 0) or 0)
            return [x, y, x + w, y + h]
        # x0/y0/x1/y1
        if all(k in value for k in ("x0", "y0", "x1", "y1")):
            return [float(value["x0"]), float(value["y0"]),
                    float(value["x1"]), float(value["y1"])]
        # left/top/right/bottom
        if all(k in value for k in ("left", "top", "right", "bottom")):
            return [float(value["left"]), float(value["top"]),
                    float(value["right"]), float(value["bottom"])]
        return None
    if not isinstance(value, list) or not value:
        return None
    # bbox-плоский: 4 числа
    if len(value) == 4 and all(isinstance(c, (int, float)) for c in value):
        return [float(value[0]), float(value[1]), float(value[2]), float(value[3])]
    # polygon: [[x, y], ...]
    try:
        xs = []
        ys = []
        for pt in value:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                xs.append(float(pt[0]))
                ys.append(float(pt[1]))
        if xs and ys:
            return [min(xs), min(ys), max(xs), max(ys)]
    except (TypeError, ValueError):
        pass
    return None


def _block_type(raw: dict) -> str:
    t = raw.get("block_type") or raw.get("type") or raw.get("label") or raw.get("kind") or ""
    t = str(t).lower().strip()
    if not t:
        return "unknown"
    if any(k in t for k in ("table", "таблиц")):
        return "table"
    if any(k in t for k in ("image", "picture", "figure", "карт", "схем")):
        return "image"
    if "text" in t or "para" in t or "header" in t or "title" in t:
        return "text"
    return t or "unknown"


def _resolve_page_number(raw: dict, fallback_page: int) -> int:
    for key in ("page_number", "page", "page_index", "page_idx"):
        if key in raw and raw[key] is not None:
            val = _to_int_safe(raw[key], 0)
            if key in ("page_index", "page_idx"):
                # внутри result.json часто 1-based; но если это явный index — подстраиваемся
                if val >= 1:
                    return val
                return val + 1
            if val >= 1:
                return val
    return fallback_page or 1


def _make_block_id(raw_id: Any, fallback_seq: int) -> str:
    if raw_id is None:
        return f"blk{fallback_seq:04d}"
    s = str(raw_id).strip()
    return s or f"blk{fallback_seq:04d}"


def _safe_load_json(path: str | Path) -> dict | None:
    try:
        p = Path(path)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _block_raw_passthrough(raw: dict) -> dict:
    """Минимально-полезные raw-поля для UI + источники block-PDF/текст-слоя.

    Сохраняем block-level PDF ссылки (``crop_url``/``image_file``) и уже
    извлечённый upstream'ом ``pdfplumber_text`` — их использует
    ``block_pdf_source`` как приоритетный источник изображения и словарь
    буквальных значений. Поля опциональны (могут отсутствовать у части блоков).
    """
    out = {
        "shape_type": raw.get("shape_type"),
        "ocr_text_present": bool(raw.get("ocr_text") or raw.get("text")),
    }
    for k in ("crop_url", "image_file", "pdfplumber_text", "coords_px"):
        v = raw.get(k)
        if v not in (None, ""):
            out[k] = v
    return out


def normalize_blocks_from_result_json(path: str | Path) -> tuple[list[dict], dict]:
    """Прочитать result.json и вернуть список нормализованных блоков + метаданные.

    Каждый блок:
      {
        "id": str,
        "page": int (1-based),
        "bbox": [x0, y0, x1, y1] in pixels (если есть в JSON),
        "bbox_norm": [x0, y0, x1, y1] in [0,1] (если есть),
        "type": "image/table/text/unknown",
        "source": "result.json",
        "label": str (raw label/type),
        "page_width": int,
        "page_height": int,
        "raw": {...} (минимально-полезные поля для UI)
      }

    Метаданные: {"pages_total": N, "pages": [{"page_number", "width", "height"}, ...]}
    """
    data = _safe_load_json(path)
    if not isinstance(data, dict):
        return [], {"pages_total": 0, "pages": []}

    blocks: list[dict] = []
    seq = 0
    pages_meta: list[dict] = []

    # Формат A: data["pages"] = [{page_number, width, height, blocks}]
    pages = data.get("pages") if isinstance(data, dict) else None
    if isinstance(pages, list):
        for p_idx, page in enumerate(pages):
            if not isinstance(page, dict):
                continue
            page_num = _to_int_safe(page.get("page_number") or page.get("page") or (p_idx + 1), p_idx + 1)
            pw = _to_int_safe(page.get("width", 0))
            ph = _to_int_safe(page.get("height", 0))
            pages_meta.append({"page_number": page_num, "width": pw, "height": ph})

            page_blocks = page.get("blocks") or []
            if not isinstance(page_blocks, list):
                continue
            for raw in page_blocks:
                if not isinstance(raw, dict):
                    continue
                bbox = _coerce_bbox(raw.get("coords_px") or raw.get("bbox") or raw.get("coords") or raw.get("polygon"))
                bbox_norm = _coerce_bbox(raw.get("coords_norm") or raw.get("bbox_norm"))
                if bbox is None and bbox_norm is not None and pw and ph:
                    # Из норм в пиксели
                    bbox = [bbox_norm[0]*pw, bbox_norm[1]*ph, bbox_norm[2]*pw, bbox_norm[3]*ph]
                seq += 1
                blocks.append({
                    "id": _make_block_id(raw.get("id") or raw.get("block_id"), seq),
                    "page": _resolve_page_number(raw, fallback_page=page_num),
                    "bbox": bbox,
                    "bbox_norm": bbox_norm,
                    "type": _block_type(raw),
                    "label": str(raw.get("label") or raw.get("block_type") or raw.get("type") or "")[:64],
                    "source": "result.json",
                    "page_width": pw,
                    "page_height": ph,
                    "raw": _block_raw_passthrough(raw),
                })

    # Формат B: data["blocks"] = [...]
    if not blocks:
        flat = data.get("blocks") if isinstance(data, dict) else None
        if isinstance(flat, list):
            for raw in flat:
                if not isinstance(raw, dict):
                    continue
                page_num = _resolve_page_number(raw, fallback_page=1)
                bbox = _coerce_bbox(raw.get("coords_px") or raw.get("bbox") or raw.get("coords") or raw.get("polygon"))
                bbox_norm = _coerce_bbox(raw.get("coords_norm") or raw.get("bbox_norm"))
                pw = _to_int_safe(raw.get("page_width", 0))
                ph = _to_int_safe(raw.get("page_height", 0))
                if bbox is None and bbox_norm is not None and pw and ph:
                    bbox = [bbox_norm[0]*pw, bbox_norm[1]*ph, bbox_norm[2]*pw, bbox_norm[3]*ph]
                seq += 1
                blocks.append({
                    "id": _make_block_id(raw.get("id") or raw.get("block_id"), seq),
                    "page": page_num,
                    "bbox": bbox,
                    "bbox_norm": bbox_norm,
                    "type": _block_type(raw),
                    "label": str(raw.get("label") or raw.get("block_type") or raw.get("type") or "")[:64],
                    "source": "result.json",
                    "page_width": pw,
                    "page_height": ph,
                    "raw": _block_raw_passthrough(raw),
                })

    if not pages_meta:
        # Если страниц не нашли явно — собрать из блоков (без size)
        pages_set: dict[int, dict] = {}
        for b in blocks:
            p = b.get("page") or 1
            if p not in pages_set:
                pages_set[p] = {
                    "page_number": p,
                    "width": b.get("page_width", 0) or 0,
                    "height": b.get("page_height", 0) or 0,
                }
        pages_meta = [pages_set[k] for k in sorted(pages_set.keys())]

    return blocks, {"pages_total": len(pages_meta), "pages": pages_meta}


# ─── IoU auto-link ───────────────────────────────────────────────────────

def _iou(a: list[float] | None, b: list[float] | None) -> float:
    if not a or not b:
        return 0.0
    if len(a) != 4 or len(b) != 4:
        return 0.0
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    if ax1 <= ax0 or ay1 <= ay0 or bx1 <= bx0 or by1 <= by0:
        return 0.0
    ix0 = max(ax0, bx0)
    iy0 = max(ay0, by0)
    ix1 = min(ax1, bx1)
    iy1 = min(ay1, by1)
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    inter = (ix1 - ix0) * (iy1 - iy0)
    area_a = (ax1 - ax0) * (ay1 - ay0)
    area_b = (bx1 - bx0) * (by1 - by0)
    union = area_a + area_b - inter
    if union <= 0:
        return 0.0
    return inter / union


def auto_link_blocks(
    left_blocks: list[dict],
    right_blocks: list[dict],
    *,
    iou_threshold: float = 0.5,
    alignment_items: list[dict] | None = None,
) -> list[dict]:
    """Жадное IoU-сопоставление блоков.

    Если alignment_items передан, блок левой страницы L сопоставляется только
    с блоками правой страницы R по карте (L→R, slot). Если карта не передана,
    используется legacy-режим (одинаковые номера страниц).

    Возвращает список:
      {
        "left_block_id", "right_block_id",
        "method": "auto", "score",
        "left_page", "right_page", "alignment_slot",
        "page" (== left_page, обратная совместимость)
      }
    """
    left_to_right_map: dict[int, int | None] = {}
    left_to_slot: dict[int, int] = {}
    if alignment_items:
        for it in alignment_items:
            lp = it.get("left_page")
            rp = it.get("right_page")
            slot = it.get("slot")
            if lp is None:
                continue
            left_to_right_map[int(lp)] = (int(rp) if rp is not None else None)
            if slot is not None:
                left_to_slot[int(lp)] = int(slot)

    right_by_page: dict[int, list[dict]] = {}
    for rb in right_blocks:
        right_by_page.setdefault(rb.get("page") or 1, []).append(rb)

    candidates: list[tuple[int, int, str, str, float, int | None]] = []
    for lb in left_blocks:
        lp = lb.get("page") or 1
        if alignment_items is not None:
            if lp not in left_to_right_map:
                continue
            rp = left_to_right_map[lp]
            if rp is None:
                continue
        else:
            rp = lp
        slot = left_to_slot.get(lp)
        for rb in right_by_page.get(rp, []):
            a = lb.get("bbox_norm") or lb.get("bbox")
            b = rb.get("bbox_norm") or rb.get("bbox")
            sc = _iou(a, b)
            if sc >= iou_threshold:
                candidates.append((lp, rp, lb["id"], rb["id"], sc, slot))

    candidates.sort(key=lambda x: -x[4])
    used_left: set[str] = set()
    used_right: set[str] = set()
    links: list[dict] = []
    for lp, rp, lid, rid, sc, slot in candidates:
        if lid in used_left or rid in used_right:
            continue
        used_left.add(lid)
        used_right.add(rid)
        links.append({
            "left_block_id": lid,
            "right_block_id": rid,
            "method": "auto",
            "score": round(sc, 4),
            "left_page": lp,
            "right_page": rp,
            "alignment_slot": slot,
            "page": lp,  # обратная совместимость с MVP
        })
    return links


__all__ = [
    "normalize_blocks_from_result_json",
    "auto_link_blocks",
]
