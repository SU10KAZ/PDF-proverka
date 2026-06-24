# -*- coding: utf-8 -*-
"""Pipeline V2 — Block Matching OLD↔NEW (этап 2, backend-only, изолированный).

Второй слой нового режима сравнения стадий. Принимает ДВЕ нормализованные модели
документа (``normalized_document_model`` из этапа 1 —
[pipeline_v2_prepared_ingest](pipeline_v2_prepared_ingest.py)) — OLD (left,
старая стадия) и NEW (right, новая стадия) — и строит ДЕТЕРМИНИРОВАННОЕ
сопоставление сначала страниц, затем блоков внутри сопоставленных страниц.

Режим — observe / read-only:

    left model (OLD)  ─┐
                       ├─► match_pages    ─► page_matches (1:1)
    right model (NEW) ─┘        │
                                ▼
                          match_blocks (внутри пары страниц) ─► block_matches (1:1)
                                │
                                ▼
                          block_matching_report.json

Принципы:
  * единица сравнения — лист и блок (а не «весь том одним Opus»);
  * нельзя полагаться на физический номер страницы: старый лист 52 может
    соответствовать новому листу 21 — поэтому приоритет у имени листа/штампа;
  * precision > recall: лучше оставить блок/страницу непарными, чем выдать
    неверную «сильную» пару;
  * несовместимые семантические типы не матчатся как strong (stamp↔scheme,
    text↔stamp, table↔scheme и т.п.).

Модуль НЕ ходит в сеть, НЕ скачивает ``crop_url``, НЕ вызывает Qwen/Opus/OCR/
PDF-render и НЕ создаёт findings. Только stdlib (включая ``difflib``).

Все функции чистые, кроме ``write_block_matching_report`` (атомарная запись).

См. docs/stage_comparison_pipeline_v2_block_matching.md.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_block_matching"

# ─── Параметры (детерминированные пороги) ───────────────────────────────────

_DEFAULTS = {
    # page-level
    "page_min_candidate_score": 0.30,
    "page_strong_score": 0.85,
    "page_medium_score": 0.60,
    "page_fuzzy_threshold": 0.60,
    # block-level
    "block_min_candidate_score": 0.20,
    "block_strong_score": 0.80,
    "block_medium_score": 0.50,
    "block_iou_low": 0.30,
    "text_weak_threshold": 0.50,
    # ambiguity
    "duplicate_margin": 0.08,
}


def _opt(options: Optional[dict], key: str) -> Any:
    if options and key in options and options[key] is not None:
        return options[key]
    return _DEFAULTS[key]


# ─── Текстовая нормализация / identity keys ─────────────────────────────────

_WS_RE = re.compile(r"\s+")
_NONWORD_RE = re.compile(r"[^0-9a-zа-я]+")
_SHEET_NOISE_RE = re.compile(r"\b(?:лист|стр|страница)\s*\d+\b")
_PAREN_FROM_RE = re.compile(r"\(\s*из\s*\d+\s*\)")


def normalize_match_text(text: Any) -> str:
    """Канонизировать строку для сопоставления: NFKC, lower, ё→е, убрать
    служебные «лист N»/«стр N»/«(из N)», свести пунктуацию к пробелам."""
    s = "" if text is None else str(text)
    s = unicodedata.normalize("NFKC", s).lower().replace("ё", "е")
    s = _PAREN_FROM_RE.sub(" ", s)
    s = _SHEET_NOISE_RE.sub(" ", s)
    s = _NONWORD_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


def _text_ratio(a: str, b: str) -> float:
    """SequenceMatcher-сходство уже нормализованных строк (0..1)."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def make_page_identity_key(page: dict) -> str:
    """Стабильный identity-ключ страницы (для группировки/дедупа кандидатов)."""
    name = normalize_match_text(page.get("sheet_name"))
    ptype = _clean(page.get("page_type")) or "unknown"
    if name:
        return f"{ptype}|name:{name}"
    sn = _clean(page.get("sheet_number"))
    if sn:
        return f"{ptype}|sheet:{normalize_match_text(sn)}"
    return f"{ptype}|page:{page.get('page_number')}"


def make_block_identity_key(block: dict) -> str:
    """Стабильный identity-ключ блока (semantic_type + block_id)."""
    st = _clean(block.get("semantic_type")) or "unknown"
    bid = _clean(block.get("block_id"))
    return f"{st}|{bid}"


# ─── Низкоуровневые помощники ────────────────────────────────────────────────


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_id(value: Any) -> str:
    s = _clean(value) or "x"
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in s)[:48]


def _coords4(coords: Any) -> Optional[list[float]]:
    if not isinstance(coords, (list, tuple)) or len(coords) < 4:
        return None
    try:
        x0, y0, x1, y1 = (float(coords[0]), float(coords[1]),
                          float(coords[2]), float(coords[3]))
    except (TypeError, ValueError):
        return None
    # нормализуем порядок углов
    return [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]


def compute_bbox_iou_norm(left_coords_norm: Any, right_coords_norm: Any) -> float:
    """IoU двух нормализованных bbox ([x0,y0,x1,y1] в [0,1]). 0.0 если нет/битые."""
    a = _coords4(left_coords_norm)
    b = _coords4(right_coords_norm)
    if a is None or b is None:
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
    union = (ax1 - ax0) * (ay1 - ay0) + (bx1 - bx0) * (by1 - by0) - inter
    if union <= 0:
        return 0.0
    return max(0.0, min(1.0, inter / union))


def _page_confidence(score: float, options: Optional[dict]) -> str:
    if score >= _opt(options, "page_strong_score"):
        return "strong"
    if score >= _opt(options, "page_medium_score"):
        return "medium"
    return "weak"


def _block_confidence(score: float, options: Optional[dict]) -> str:
    if score >= _opt(options, "block_strong_score"):
        return "strong"
    if score >= _opt(options, "block_medium_score"):
        return "medium"
    return "weak"


# ─── Семантические группы блоков ────────────────────────────────────────────


def _sem_group(semantic_type: str) -> str:
    st = (semantic_type or "").strip()
    if st == "stamp":
        return "stamp"
    if st in ("text", "legend", "title"):
        return "text"
    if st == "table":
        return "table"
    if st in ("scheme", "large_scheme", "plan"):
        return "scheme"
    return "unknown"


# ─── 1. Page candidates / matching ──────────────────────────────────────────


def _score_page_pair(lp: dict, rp: dict, options: Optional[dict]) -> Optional[dict]:
    """Лучший метод+score для пары страниц или None, если не дотягивает."""
    ln = normalize_match_text(lp.get("sheet_name"))
    rn = normalize_match_text(rp.get("sheet_name"))
    ltype = _clean(lp.get("page_type")) or "unknown"
    rtype = _clean(rp.get("page_type")) or "unknown"
    lsn = normalize_match_text(lp.get("sheet_number"))
    rsn = normalize_match_text(rp.get("sheet_number"))
    ldc = normalize_match_text(lp.get("document_code"))
    rdc = normalize_match_text(rp.get("document_code"))

    same_type = ltype == rtype and ltype != "unknown"
    name_sim = _text_ratio(ln, rn)
    reasons: list[str] = []

    # 1) Точное имя листа.
    if ln and rn and ln == rn:
        score = 0.95 + (0.03 if same_type else 0.0)
        reasons.append("sheet_name_exact")
        if same_type:
            reasons.append("page_type_match")
        return {"method": "exact_sheet", "score": min(1.0, score), "reasons": reasons}

    # 2) Совпал номер листа из штампа (+ подтверждение типом/именем).
    if lsn and rsn and lsn == rsn and (same_type or name_sim >= 0.5):
        score = 0.80 + (0.08 if same_type else 0.0) + 0.10 * name_sim
        reasons.append("stamp_sheet_number")
        if same_type:
            reasons.append("page_type_match")
        return {"method": "stamp_sheet", "score": min(0.97, score), "reasons": reasons}

    # 3) Fuzzy по имени листа.
    if name_sim >= _opt(options, "page_fuzzy_threshold"):
        score = 0.50 + 0.40 * name_sim + (0.05 if same_type else 0.0)
        reasons.append(f"sheet_name_fuzzy:{name_sim:.2f}")
        if same_type:
            reasons.append("page_type_match")
        return {"method": "content_fuzzy", "score": min(0.94, score), "reasons": reasons}

    # 4) Одинаковый document_code + одинаковый тип, когда имён листов нет.
    if ldc and rdc and ldc == rdc and same_type and not ln and not rn:
        reasons.append("document_code_match")
        reasons.append("page_type_match")
        return {"method": "document_code", "score": 0.55, "reasons": reasons}

    # 5) Слабый fallback: совпал физический номер страницы.
    if lp.get("page_number") is not None and lp.get("page_number") == rp.get("page_number"):
        score = 0.30 + (0.10 if same_type else 0.0)
        reasons.append("page_number_only")
        return {"method": "page_number", "score": score, "reasons": reasons}

    return None


def build_page_match_candidates(left_pages: list[dict], right_pages: list[dict],
                                options: Optional[dict] = None) -> list[dict]:
    """Все пары страниц с проходным score (для последующей жадной 1:1 сборки)."""
    min_score = _opt(options, "page_min_candidate_score")
    cands: list[dict] = []
    for li, lp in enumerate(left_pages):
        for ri, rp in enumerate(right_pages):
            scored = _score_page_pair(lp, rp, options)
            if not scored or scored["score"] < min_score:
                continue
            cands.append({
                "left_idx": li, "right_idx": ri,
                "left_page": lp, "right_page": rp,
                **scored,
            })
    cands.sort(key=lambda c: (-c["score"], c["left_idx"], c["right_idx"]))
    return cands


def _page_risk_flags(lp: dict, rp: dict, method: str, score: float,
                     duplicate: bool, options: Optional[dict]) -> list[str]:
    flags: list[str] = []
    if method == "page_number":
        flags.append("page_number_only_match")
    if not _clean(lp.get("sheet_name")) or not _clean(rp.get("sheet_name")):
        flags.append("sheet_name_missing")
    lt, rt = _clean(lp.get("page_type")), _clean(rp.get("page_type"))
    if lt and rt and lt != rt and lt != "unknown" and rt != "unknown":
        flags.append("page_type_mismatch")
    if score < _opt(options, "page_medium_score"):
        flags.append("low_score")
    if duplicate:
        flags.append("duplicate_candidate")
    return flags


def match_pages(left_model: dict, right_model: dict,
                options: Optional[dict] = None) -> list[dict]:
    """Жадно (1:1) сопоставить страницы двух моделей по убыванию score."""
    left_pages = list(left_model.get("pages") or [])
    right_pages = list(right_model.get("pages") or [])
    cands = build_page_match_candidates(left_pages, right_pages, options)

    by_left: dict[int, list[float]] = {}
    by_right: dict[int, list[float]] = {}
    for c in cands:
        by_left.setdefault(c["left_idx"], []).append(c["score"])
        by_right.setdefault(c["right_idx"], []).append(c["score"])

    margin = _opt(options, "duplicate_margin")
    used_left: set[int] = set()
    used_right: set[int] = set()
    matches: list[dict] = []
    for c in cands:
        li, ri = c["left_idx"], c["right_idx"]
        if li in used_left or ri in used_right:
            continue
        used_left.add(li)
        used_right.add(ri)
        # дубль-кандидат: у левой ИЛИ правой стороны есть ещё кандидат с близким score
        dup = (sum(1 for s in by_left.get(li, []) if s >= c["score"] - margin) > 1
               or sum(1 for s in by_right.get(ri, []) if s >= c["score"] - margin) > 1)
        lp, rp = c["left_page"], c["right_page"]
        flags = _page_risk_flags(lp, rp, c["method"], c["score"], dup, options)
        matches.append({
            "match_id": f"pm_{lp.get('page_number')}_{rp.get('page_number')}",
            "left_page_number": lp.get("page_number"),
            "right_page_number": rp.get("page_number"),
            "left_page_type": lp.get("page_type"),
            "right_page_type": rp.get("page_type"),
            "left_sheet_name": lp.get("sheet_name", ""),
            "right_sheet_name": rp.get("sheet_name", ""),
            "left_document_code": lp.get("document_code", ""),
            "right_document_code": rp.get("document_code", ""),
            "method": c["method"],
            "score": round(float(c["score"]), 4),
            "confidence": _page_confidence(c["score"], options),
            "reasons": c["reasons"],
            "risk_flags": flags,
        })
    matches.sort(key=lambda m: (m["left_page_number"] if m["left_page_number"] is not None else 1e9,
                                m["right_page_number"] if m["right_page_number"] is not None else 1e9))
    return matches


# ─── 2. Block candidates / matching ─────────────────────────────────────────


def _block_text(block: dict) -> str:
    return normalize_match_text(block.get("text_excerpt")
                                or block.get("pdfplumber_text_excerpt"))


def _stamp_field_sim(lb: dict, rb: dict) -> float:
    ls = lb.get("stamp_data") if isinstance(lb.get("stamp_data"), dict) else {}
    rs = rb.get("stamp_data") if isinstance(rb.get("stamp_data"), dict) else {}
    if not ls and not rs:
        return 0.0
    keys = ("document_code", "sheet_name", "sheet_number", "organization", "stage")
    hits = total = 0
    for k in keys:
        lv, rv = normalize_match_text(ls.get(k)), normalize_match_text(rs.get(k))
        if not lv and not rv:
            continue
        total += 1
        if lv and rv and lv == rv:
            hits += 1
    return (hits / total) if total else 0.0


def _score_block_pair(lb: dict, rb: dict, options: Optional[dict]) -> Optional[dict]:
    """Лучший метод+score+iou для пары блоков или None."""
    iou = compute_bbox_iou_norm(lb.get("coords_norm"), rb.get("coords_norm"))
    reasons: list[str] = []

    # 1) Совпал block_id.
    if _clean(lb.get("block_id")) and _clean(lb.get("block_id")) == _clean(rb.get("block_id")):
        return {"method": "same_block_id", "score": 1.0, "iou": iou,
                "reasons": ["same_block_id"]}

    lg = _sem_group(lb.get("semantic_type"))
    rg = _sem_group(rb.get("semantic_type"))
    text_sim = _text_ratio(_block_text(lb), _block_text(rb))

    # Несовместимые конкретные группы (не unknown) — не матчим вовсе.
    incompatible = (lg != rg) and ("unknown" not in (lg, rg))
    if incompatible:
        return None

    same_group = (lg == rg)
    group = lg if lg != "unknown" else rg  # рабочая группа (unknown-wildcard)

    if group == "stamp":
        score = max(0.40 + 0.40 * iou, 0.30 + 0.55 * _stamp_field_sim(lb, rb))
        reasons.append("stamp_vs_stamp")
        if iou > 0:
            reasons.append(f"iou:{iou:.2f}")
        return {"method": "stamp", "score": min(1.0, score), "iou": iou, "reasons": reasons}

    if group == "table":
        score = 0.65 * text_sim + 0.35 * iou
        reasons.append(f"table_text:{text_sim:.2f}")
        return {"method": "table_fuzzy", "score": min(1.0, score), "iou": iou, "reasons": reasons}

    if group == "text":
        score = 0.65 * text_sim + 0.35 * iou
        reasons.append(f"text_fuzzy:{text_sim:.2f}")
        return {"method": "text_fuzzy", "score": min(1.0, score), "iou": iou, "reasons": reasons}

    if group == "scheme":
        score = iou
        bonus = 0.0
        if _clean(lb.get("semantic_type")) == _clean(rb.get("semantic_type")):
            bonus += 0.10
        both_crop = bool(lb.get("has_crop_pdf")) and bool(rb.get("has_crop_pdf"))
        if both_crop:
            bonus += 0.05
        score = min(1.0, score + bonus)
        method = "scheme_crop" if both_crop else "semantic_type_iou"
        reasons.append(f"iou:{iou:.2f}")
        if both_crop:
            reasons.append("both_have_crop")
        return {"method": method, "score": score, "iou": iou, "reasons": reasons}

    # unknown↔unknown (или unknown-wildcard без явной группы): только геометрия.
    score = min(0.55, 0.55 * iou)  # capped weak — без явной семантики не «сильно»
    reasons.append(f"iou:{iou:.2f}")
    if not same_group:
        reasons.append("semantic_wildcard")
    return {"method": "semantic_type_iou", "score": score, "iou": iou, "reasons": reasons}


def build_block_match_candidates(left_blocks: list[dict], right_blocks: list[dict],
                                 options: Optional[dict] = None) -> list[dict]:
    """Все пары блоков (внутри одной пары страниц) с проходным score."""
    min_score = _opt(options, "block_min_candidate_score")
    cands: list[dict] = []
    for li, lb in enumerate(left_blocks):
        for ri, rb in enumerate(right_blocks):
            scored = _score_block_pair(lb, rb, options)
            if not scored or scored["score"] < min_score:
                continue
            cands.append({
                "left_idx": li, "right_idx": ri,
                "left_block": lb, "right_block": rb,
                **scored,
            })
    cands.sort(key=lambda c: (-c["score"], c["left_idx"], c["right_idx"]))
    return cands


def _block_risk_flags(lb: dict, rb: dict, method: str, score: float, iou: float,
                      duplicate: bool, options: Optional[dict]) -> list[str]:
    flags: list[str] = []
    lg, rg = _sem_group(lb.get("semantic_type")), _sem_group(rb.get("semantic_type"))
    if lg != rg:
        flags.append("semantic_type_mismatch")
    if not _coords4(lb.get("coords_norm")) or not _coords4(rb.get("coords_norm")):
        flags.append("missing_coords")
    if method in ("semantic_type_iou", "scheme_crop") and iou < _opt(options, "block_iou_low"):
        flags.append("low_iou")
    if method in ("text_fuzzy", "table_fuzzy"):
        if _text_ratio(_block_text(lb), _block_text(rb)) < _opt(options, "text_weak_threshold"):
            flags.append("weak_text_match")
    if lg == "scheme" and rg == "scheme":
        if not lb.get("has_crop_pdf") or not rb.get("has_crop_pdf"):
            flags.append("missing_crop")
    if duplicate:
        flags.append("duplicate_candidate")
    return flags


def _blocks_for_page(model: dict, block_ids: list) -> list[dict]:
    registry = model.get("blocks") or {}
    out: list[dict] = []
    for bid in (block_ids or []):
        b = registry.get(bid)
        if isinstance(b, dict):
            out.append(b)
    return out


def match_blocks(left_model: dict, right_model: dict, page_matches: list[dict],
                 options: Optional[dict] = None) -> list[dict]:
    """Сопоставить блоки ВНУТРИ каждой пары страниц (жадно 1:1)."""
    left_pages = {p.get("page_number"): p for p in (left_model.get("pages") or [])}
    right_pages = {p.get("page_number"): p for p in (right_model.get("pages") or [])}
    margin = _opt(options, "duplicate_margin")

    matches: list[dict] = []
    for pm in page_matches:
        lp = left_pages.get(pm["left_page_number"])
        rp = right_pages.get(pm["right_page_number"])
        if not lp or not rp:
            continue
        left_blocks = _blocks_for_page(left_model, lp.get("blocks"))
        right_blocks = _blocks_for_page(right_model, rp.get("blocks"))
        cands = build_block_match_candidates(left_blocks, right_blocks, options)

        by_left: dict[int, list[float]] = {}
        by_right: dict[int, list[float]] = {}
        for c in cands:
            by_left.setdefault(c["left_idx"], []).append(c["score"])
            by_right.setdefault(c["right_idx"], []).append(c["score"])

        used_left: set[int] = set()
        used_right: set[int] = set()
        for c in cands:
            li, ri = c["left_idx"], c["right_idx"]
            if li in used_left or ri in used_right:
                continue
            used_left.add(li)
            used_right.add(ri)
            dup = (sum(1 for s in by_left.get(li, []) if s >= c["score"] - margin) > 1
                   or sum(1 for s in by_right.get(ri, []) if s >= c["score"] - margin) > 1)
            lb, rb = c["left_block"], c["right_block"]
            flags = _block_risk_flags(lb, rb, c["method"], c["score"], c["iou"], dup, options)
            matches.append({
                "match_id": f"bm_{_safe_id(lb.get('block_id'))}__{_safe_id(rb.get('block_id'))}",
                "page_match_id": pm["match_id"],
                "left_block_id": lb.get("block_id"),
                "right_block_id": rb.get("block_id"),
                "left_page_number": pm["left_page_number"],
                "right_page_number": pm["right_page_number"],
                "left_semantic_type": lb.get("semantic_type"),
                "right_semantic_type": rb.get("semantic_type"),
                "left_block_type": lb.get("block_type"),
                "right_block_type": rb.get("block_type"),
                "method": c["method"],
                "score": round(float(c["score"]), 4),
                "iou": round(float(c["iou"]), 4),
                "confidence": _block_confidence(c["score"], options),
                "reasons": c["reasons"],
                "risk_flags": flags,
            })
    return matches


# ─── 3. Оркестрация: match_normalized_documents ─────────────────────────────


def _page_brief(p: dict, *, one_sided: bool) -> dict:
    return {
        "page_number": p.get("page_number"),
        "page_type": p.get("page_type"),
        "sheet_name": p.get("sheet_name", ""),
        "document_code": p.get("document_code", ""),
        "risk_flags": ["one_sided_page"] if one_sided else ["unmatched"],
    }


def _block_brief(b: dict, *, page_number, one_sided: bool) -> dict:
    flags = ["one_sided_block"] if one_sided else ["unmatched"]
    if not _coords4(b.get("coords_norm")):
        flags.append("missing_coords")
    return {
        "block_id": b.get("block_id"),
        "page_number": page_number,
        "semantic_type": b.get("semantic_type"),
        "block_type": b.get("block_type"),
        "risk_flags": flags,
    }


def match_normalized_documents(left_model: dict, right_model: dict,
                               options: Optional[dict] = None) -> dict:
    """Полное сопоставление двух нормализованных моделей (страницы + блоки).

    Возвращает ``block_matching_report`` (см. docs). Чистая функция, без сети,
    без Qwen/Opus, без скачивания crop.
    """
    left_model = left_model or {}
    right_model = right_model or {}
    left_pages = list(left_model.get("pages") or [])
    right_pages = list(right_model.get("pages") or [])

    page_matches = match_pages(left_model, right_model, options)
    block_matches = match_blocks(left_model, right_model, page_matches, options)

    # ── unmatched страницы ──
    matched_left_pn = {m["left_page_number"] for m in page_matches}
    matched_right_pn = {m["right_page_number"] for m in page_matches}
    # были ли вообще кандидаты для страницы (для one_sided vs lost)
    page_cands = build_page_match_candidates(left_pages, right_pages, options)
    left_had_cand = {c["left_idx"] for c in page_cands}
    right_had_cand = {c["right_idx"] for c in page_cands}

    unmatched_left_pages = [
        _page_brief(p, one_sided=(i not in left_had_cand))
        for i, p in enumerate(left_pages) if p.get("page_number") not in matched_left_pn]
    unmatched_right_pages = [
        _page_brief(p, one_sided=(i not in right_had_cand))
        for i, p in enumerate(right_pages) if p.get("page_number") not in matched_right_pn]

    # ── unmatched блоки ──
    matched_left_bid = {m["left_block_id"] for m in block_matches}
    matched_right_bid = {m["right_block_id"] for m in block_matches}
    # страницы, попавшие в page_match (для one_sided блоков: блок на непарной
    # странице — one_sided; блок на парной странице, но без пары — lost)
    paired_left_pn = matched_left_pn
    paired_right_pn = matched_right_pn

    left_blocks_reg = left_model.get("blocks") or {}
    right_blocks_reg = right_model.get("blocks") or {}

    unmatched_left_blocks: list[dict] = []
    for bid, b in left_blocks_reg.items():
        if bid in matched_left_bid:
            continue
        pn = b.get("page_number")
        unmatched_left_blocks.append(
            _block_brief(b, page_number=pn, one_sided=(pn not in paired_left_pn)))
    unmatched_right_blocks: list[dict] = []
    for bid, b in right_blocks_reg.items():
        if bid in matched_right_bid:
            continue
        pn = b.get("page_number")
        unmatched_right_blocks.append(
            _block_brief(b, page_number=pn, one_sided=(pn not in paired_right_pn)))

    # ── warnings ──
    warnings: list[str] = []
    if not left_pages or not right_pages:
        warnings.append("empty_model_pages")
    if not page_matches and left_pages and right_pages:
        warnings.append("no_page_matches")
    missing_coords = sum(
        1 for reg in (left_blocks_reg, right_blocks_reg)
        for b in reg.values() if not _coords4(b.get("coords_norm")))
    if missing_coords:
        warnings.append(f"blocks_missing_coords: {missing_coords}")
    pt_mismatch = sum(1 for m in page_matches if "page_type_mismatch" in m["risk_flags"])
    if pt_mismatch:
        warnings.append(f"page_type_mismatch_matches: {pt_mismatch}")

    # ── summary ──
    strong_pm = sum(1 for m in page_matches if m["confidence"] == "strong")
    weak_pm = sum(1 for m in page_matches if m["confidence"] == "weak")
    strong_bm = sum(1 for m in block_matches if m["confidence"] == "strong")
    weak_bm = sum(1 for m in block_matches if m["confidence"] == "weak")

    report = {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "left": {
            "document_code": (left_model.get("document") or {}).get("document_code", ""),
            "pages_total": len(left_pages),
            "blocks_total": len(left_blocks_reg),
        },
        "right": {
            "document_code": (right_model.get("document") or {}).get("document_code", ""),
            "pages_total": len(right_pages),
            "blocks_total": len(right_blocks_reg),
        },
        "summary": {
            "page_matches_total": len(page_matches),
            "block_matches_total": len(block_matches),
            "unmatched_left_pages": len(unmatched_left_pages),
            "unmatched_right_pages": len(unmatched_right_pages),
            "unmatched_left_blocks": len(unmatched_left_blocks),
            "unmatched_right_blocks": len(unmatched_right_blocks),
            "strong_page_matches": strong_pm,
            "weak_page_matches": weak_pm,
            "strong_block_matches": strong_bm,
            "weak_block_matches": weak_bm,
            "warnings_count": len(warnings),
        },
        "page_matches": page_matches,
        "block_matches": block_matches,
        "unmatched_left_pages": unmatched_left_pages,
        "unmatched_right_pages": unmatched_right_pages,
        "unmatched_left_blocks": unmatched_left_blocks,
        "unmatched_right_blocks": unmatched_right_blocks,
        "warnings": warnings,
    }
    return report


# ─── 4. write_block_matching_report (атомарная запись) ──────────────────────


def write_block_matching_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт в JSON-файл (tmp + ``os.replace``)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "match_normalized_documents",
    "match_pages",
    "match_blocks",
    "compute_bbox_iou_norm",
    "build_page_match_candidates",
    "build_block_match_candidates",
    "normalize_match_text",
    "make_page_identity_key",
    "make_block_identity_key",
    "write_block_matching_report",
]
