# -*- coding: utf-8 -*-
"""Pipeline V2 — Block Link Preview (read-only витрина предложенных связей).

Строит ``block_link_preview_report.json`` из уже готовых артефактов dry-run:

* ``left/right_normalized_document_model.json`` — bbox/страницы блоков;
* ``block_matching_report.json``                — предложенные связи OLD↔NEW;
* опционально ``left/right_graphic_descriptor_report.json`` — readiness;
* опционально ``visual_equivalence_gate_report.json``       — визуальный статус.

Назначение — UI-режим «Pipeline V2 — предложенные связи» в разделе
«Связь блоков»: старая страница слева, новая справа, блоки подсвечены цветом
по статусу связи. Builder НИЧЕГО не применяет: существующие ручные связи
блоков (`links` пары) не читаются и не изменяются, никаких job'ов/моделей.

Статус связи (``link_status``) детерминированный:

* ``strong``        — block match с confidence=strong          → зелёный;
* ``weak``          — confidence medium/weak                   → жёлтый;
* ``manual_review`` — visual gate решил manual_review ИЛИ у match'а
  risk-флаг из ``MANUAL_REVIEW_RISK_FLAGS``                    → оранжевый;
* ``unmatched``     — односторонний блок (нет пары)            → серый.

Синий цвет зарезервирован за выбранной в UI связью (``SELECTED_COLOR``) —
сам отчёт «выбранность» не хранит.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_block_link_preview"

LINK_STRONG = "strong"
LINK_WEAK = "weak"
LINK_MANUAL = "manual_review"
LINK_UNMATCHED = "unmatched"

# Цвет карточки/обводки по статусу; выбранная связь поверх — синий контур.
COLOR_BY_STATUS = {
    LINK_STRONG: "green",
    LINK_WEAK: "yellow",
    LINK_MANUAL: "orange",
    LINK_UNMATCHED: "gray",
}
SELECTED_COLOR = "blue"

LABEL_BY_STATUS = {
    LINK_STRONG: "Надёжная связь",
    LINK_WEAK: "Слабая связь — проверить",
    LINK_MANUAL: "Нужна ручная проверка",
    LINK_UNMATCHED: "Без пары",
}

# Risk-флаги match'а, поднимающие связь до manual_review (независимо от
# confidence): дубль-кандидат опасен ложной парой, localized_residual_diff —
# анти-dilution сигнал visual gate (малый реальный diff на «identical» блоке).
MANUAL_REVIEW_RISK_FLAGS = {
    "duplicate_candidate",
    "localized_residual_diff",
}

# Визуальные статусы gate, требующие ручной проверки (не «identical/changed»).
_VISUAL_UNCERTAIN_STATUSES = {"uncertain", "render_failed"}

_GRAPHIC_SEMANTIC_TYPES = {
    "scheme", "large_scheme", "plan", "legend", "figure", "image", "graphic",
}


# ─── helpers ──────────────────────────────────────────────────────────────────


def _blocks_by_id(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    blocks = m.get("blocks")
    if isinstance(blocks, dict):
        return {k: v for k, v in blocks.items() if isinstance(v, dict)}
    if isinstance(blocks, list):
        return {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
    return {}


def _is_graphic_block(block: Optional[dict]) -> bool:
    if not isinstance(block, dict):
        return False
    if block.get("block_type") == "image":
        return True
    return str(block.get("semantic_type") or "").lower() in _GRAPHIC_SEMANTIC_TYPES


def _is_graphic_semantic(semantic_type: Any) -> bool:
    return str(semantic_type or "").lower() in _GRAPHIC_SEMANTIC_TYPES


def _bbox_norm(block: Optional[dict]) -> Optional[list]:
    if not isinstance(block, dict):
        return None
    bbox = block.get("coords_norm")
    if (isinstance(bbox, (list, tuple)) and len(bbox) == 4
            and all(isinstance(v, (int, float)) for v in bbox)):
        return [float(v) for v in bbox]
    return None


def _semantic_type(*candidates: Any) -> str:
    for c in candidates:
        s = str(c or "").strip()
        if s:
            return s
    return "unknown"


def _readiness_low_flags(graphic_report: Any, block_id: str,
                         side: str) -> list[str]:
    """risk-флаг про низкую diff-readiness графического дескриптора.

    Реальный артефакт хранит уровень в ``diff_readiness.readiness``
    (как читает и visual gate); ``level``/plain-строка — legacy fallback.
    """
    r = graphic_report if isinstance(graphic_report, dict) else {}
    for d in r.get("descriptors") or []:
        if isinstance(d, dict) and d.get("block_id") == block_id:
            dr = d.get("diff_readiness")
            if isinstance(dr, dict):
                readiness = str(dr.get("readiness")
                                or dr.get("level") or "").lower()
            else:
                readiness = str(dr or "").lower()
            if readiness in ("low", "not_usable"):
                return [f"{side}_readiness_{readiness}"]
            return []
    return []


def _visual_pairs_index(visual_gate_report: Any) -> dict:
    """(left_block_id, right_block_id) → block_pair из visual gate отчёта."""
    r = visual_gate_report if isinstance(visual_gate_report, dict) else {}
    out: dict = {}
    for bp in r.get("block_pairs") or []:
        if not isinstance(bp, dict):
            continue
        key = (bp.get("left_block_id"), bp.get("right_block_id"))
        if key[0] and key[1]:
            out[key] = bp
    return out


def _ui(status: str, *, default_visible: bool = True) -> dict:
    return {
        "color": COLOR_BY_STATUS.get(status, "gray"),
        "label": LABEL_BY_STATUS.get(status, status),
        "default_visible": bool(default_visible),
    }


def _link_status(match: dict, visual_entry: Optional[dict]) -> str:
    risk = set(match.get("risk_flags") or [])
    if isinstance(visual_entry, dict):
        # status=skipped — gate пару НЕ сравнивал (cap бюджета / блок вне
        # модели): визуальных свидетельств нет, эскалация была бы ложной.
        if (visual_entry.get("decision") == "manual_review"
                and visual_entry.get("status") != "skipped"):
            return LINK_MANUAL
        risk |= set(visual_entry.get("risk_flags") or [])
    if risk & MANUAL_REVIEW_RISK_FLAGS:
        return LINK_MANUAL
    confidence = str(match.get("confidence") or "").lower()
    return LINK_STRONG if confidence == "strong" else LINK_WEAK


# ─── builder ──────────────────────────────────────────────────────────────────


def build_block_link_preview(left_model: Any, right_model: Any,
                             block_matching_report: Any, *,
                             left_graphic_report: Any = None,
                             right_graphic_report: Any = None,
                             visual_gate_report: Any = None) -> dict:
    """Собрать отчёт preview из готовых артефактов (чистая функция, офлайн).

    ``ValueError`` — если block_matching_report непригоден (нет block_matches
    даже пустым списком); отсутствие graphic/visual отчётов — норма
    (визуальные поля = null).
    """
    bm = block_matching_report if isinstance(block_matching_report, dict) else None
    if bm is None or not isinstance(bm.get("block_matches"), list):
        raise ValueError("block_matching_report is missing or invalid "
                         "(block_matches list required)")

    warnings: list[str] = []
    left_blocks = _blocks_by_id(left_model)
    right_blocks = _blocks_by_id(right_model)
    if not left_blocks or not right_blocks:
        warnings.append("normalized document model has no blocks for "
                        + ("left" if not left_blocks else "right")
                        + " side — bbox highlighting degraded")

    visual_idx = _visual_pairs_index(visual_gate_report)
    if visual_gate_report is not None and not isinstance(visual_gate_report, dict):
        warnings.append("visual_equivalence_gate_report is not an object — "
                        "visual fields skipped")

    # ── block links ──
    block_links: list[dict] = []
    counts = {LINK_STRONG: 0, LINK_WEAK: 0, LINK_MANUAL: 0}
    visual_counts = {"identical": 0, "minor": 0, "changed": 0,
                     "uncertain": 0, "skipped": 0}
    graphic_links = 0
    links_by_page_match: dict[str, list[str]] = {}

    for match in bm.get("block_matches") or []:
        if not isinstance(match, dict):
            continue
        lid, rid = match.get("left_block_id"), match.get("right_block_id")
        lb, rb = left_blocks.get(lid), right_blocks.get(rid)
        ventry = visual_idx.get((lid, rid))

        risk_flags = list(match.get("risk_flags") or [])
        if isinstance(ventry, dict):
            for f in ventry.get("risk_flags") or []:
                if f not in risk_flags:
                    risk_flags.append(f)
            if ventry.get("status") == "skipped":
                # пара не сравнивалась gate'ом (cap/блок вне модели) —
                # прозрачный неэскалирующий маркер
                risk_flags.append("visual_gate_skipped")
        for side, rep, bid in (("left", left_graphic_report, lid),
                               ("right", right_graphic_report, rid)):
            for f in _readiness_low_flags(rep, bid, side):
                if f not in risk_flags:
                    risk_flags.append(f)

        lbox, rbox = _bbox_norm(lb), _bbox_norm(rb)
        if lbox is None and lid:
            risk_flags.append("left_bbox_missing")
        if rbox is None and rid:
            risk_flags.append("right_bbox_missing")

        status = _link_status({**match, "risk_flags": risk_flags}, ventry)
        counts[status] = counts.get(status, 0) + 1

        # модельные блоки могут отсутствовать (деградированная модель) —
        # match-level semantic types остаются вторым источником истины
        is_graphic = (_is_graphic_block(lb) or _is_graphic_block(rb)
                      or _is_graphic_semantic(match.get("left_semantic_type"))
                      or _is_graphic_semantic(match.get("right_semantic_type")))
        if is_graphic:
            graphic_links += 1

        vstatus = vdecision = None
        metrics: dict = {}
        if isinstance(ventry, dict):
            vstatus = ventry.get("status")
            vdecision = ventry.get("decision")
            vm = ventry.get("metrics") if isinstance(ventry.get("metrics"), dict) else {}
            metrics = {
                "mask_iou": vm.get("mask_iou"),
                "normalized_correlation": vm.get("normalized_correlation"),
                "total_diff_ratio": vm.get("total_diff_ratio"),
                "alignment_method": vm.get("alignment_method"),
            }
            if vstatus == "identical_visual":
                visual_counts["identical"] += 1
            elif vstatus == "minor_visual":
                visual_counts["minor"] += 1
            elif vstatus == "changed_visual":
                visual_counts["changed"] += 1
            elif vstatus in _VISUAL_UNCERTAIN_STATUSES:
                visual_counts["uncertain"] += 1
            elif vstatus == "skipped":
                visual_counts["skipped"] += 1

        link_id = match.get("match_id") or f"blp_{lid}__{rid}"
        link = {
            "block_link_id": link_id,
            "page_match_id": match.get("page_match_id"),
            "left_block_id": lid,
            "right_block_id": rid,
            "left_page_number": match.get("left_page_number")
                if match.get("left_page_number") is not None
                else (lb or {}).get("page_number"),
            "right_page_number": match.get("right_page_number")
                if match.get("right_page_number") is not None
                else (rb or {}).get("page_number"),
            "left_bbox_norm": lbox,
            "right_bbox_norm": rbox,
            "semantic_type": _semantic_type(match.get("left_semantic_type"),
                                            match.get("right_semantic_type"),
                                            (lb or {}).get("semantic_type"),
                                            (rb or {}).get("semantic_type")),
            "is_graphic": is_graphic,
            "link_status": status,
            "method": match.get("method"),
            "confidence_score": match.get("score"),
            "match_confidence": match.get("confidence"),
            "iou": match.get("iou"),
            "risk_flags": risk_flags,
            "visual_status": vstatus,
            "visual_decision": vdecision,
            "visual_metrics": metrics or None,
            "ui": _ui(status),
        }
        block_links.append(link)
        pm_id = match.get("page_match_id")
        if pm_id:
            links_by_page_match.setdefault(pm_id, []).append(link_id)

    # ── page links: matched пары страниц + односторонние листы ──
    link_by_id = {l["block_link_id"]: l for l in block_links}
    page_links: list[dict] = []
    matched_page_links = 0
    for pmatch in bm.get("page_matches") or []:
        if not isinstance(pmatch, dict):
            continue
        pm_id = pmatch.get("match_id")
        ids = links_by_page_match.get(pm_id, []) if pm_id else []
        by_status: dict[str, int] = {}
        for link_id in ids:
            st = link_by_id[link_id]["link_status"]
            by_status[st] = by_status.get(st, 0) + 1
        page_links.append({
            "page_link_id": pm_id,
            "page_link_kind": "matched",
            "left_page_number": pmatch.get("left_page_number"),
            "right_page_number": pmatch.get("right_page_number"),
            "left_sheet_name": pmatch.get("left_sheet_name") or "",
            "right_sheet_name": pmatch.get("right_sheet_name") or "",
            "method": pmatch.get("method"),
            "confidence_score": pmatch.get("score"),
            "match_confidence": pmatch.get("confidence"),
            "risk_flags": list(pmatch.get("risk_flags") or []),
            "block_link_ids": ids,
            "block_links_by_status": by_status,
        })
        matched_page_links += 1

    # односторонние листы (есть только в OLD или только в NEW): их unmatched
    # блоки иначе невозможно увидеть в превью — добавляем page_link с пустой
    # второй стороной (UI уже умеет «∅ нет страницы»)
    one_sided_page_links = 0
    for side, key in (("left", "unmatched_left_pages"),
                      ("right", "unmatched_right_pages")):
        for upage in bm.get(key) or []:
            if not isinstance(upage, dict):
                continue
            page_no = upage.get("page_number")
            if page_no is None:
                continue
            unmatched_here = sum(
                1 for it in bm.get(f"unmatched_{side}_blocks") or []
                if isinstance(it, dict) and it.get("page_number") == page_no)
            page_links.append({
                "page_link_id": f"pl_{side}_only_{page_no}",
                "page_link_kind": "one_sided",
                "left_page_number": page_no if side == "left" else None,
                "right_page_number": page_no if side == "right" else None,
                "left_sheet_name": (upage.get("sheet_name") or ""
                                    if side == "left" else ""),
                "right_sheet_name": (upage.get("sheet_name") or ""
                                     if side == "right" else ""),
                "method": None,
                "confidence_score": None,
                "match_confidence": None,
                "risk_flags": list(upage.get("risk_flags") or []),
                "block_link_ids": [],
                "block_links_by_status": (
                    {LINK_UNMATCHED: unmatched_here} if unmatched_here else {}),
            })
            one_sided_page_links += 1

    # ── unmatched blocks ──
    def _unmatched_entries(items: Any, side: str, blocks: dict) -> list[dict]:
        out: list[dict] = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            bid = it.get("block_id")
            block = blocks.get(bid)
            out.append({
                "block_id": bid,
                "side": side,
                "page_number": it.get("page_number")
                    if it.get("page_number") is not None
                    else (block or {}).get("page_number"),
                "bbox_norm": _bbox_norm(block),
                "semantic_type": _semantic_type(it.get("semantic_type"),
                                                (block or {}).get("semantic_type")),
                # сам unmatched-item несёт block_type/semantic_type —
                # _is_graphic_block работает и по нему (деградированная модель)
                "is_graphic": _is_graphic_block(block) or _is_graphic_block(it),
                "link_status": LINK_UNMATCHED,
                "risk_flags": list(it.get("risk_flags") or []),
                "ui": _ui(LINK_UNMATCHED),
            })
        return out

    unmatched_left = _unmatched_entries(bm.get("unmatched_left_blocks"),
                                        "left", left_blocks)
    unmatched_right = _unmatched_entries(bm.get("unmatched_right_blocks"),
                                         "right", right_blocks)

    visual_available = bool(visual_idx)
    # пустой block_pairs — НОРМА для пар без matched-графики (text-only ПЗ):
    # warning только когда graphic-связи есть, а визуальных полей нет
    if (not visual_available and isinstance(visual_gate_report, dict)
            and graphic_links > 0):
        warnings.append("visual gate report has no block_pairs — "
                        "visual fields are null")

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": "ok",
        "summary": {
            "page_links_total": len(page_links),
            "matched_page_links": matched_page_links,
            "one_sided_page_links": one_sided_page_links,
            "block_links_total": len(block_links),
            "strong_links": counts.get(LINK_STRONG, 0),
            "weak_links": counts.get(LINK_WEAK, 0),
            "manual_review_links": counts.get(LINK_MANUAL, 0),
            "unmatched_left_blocks": len(unmatched_left),
            "unmatched_right_blocks": len(unmatched_right),
            "graphic_links_total": graphic_links,
            "visual_identical": visual_counts["identical"],
            "visual_minor": visual_counts["minor"],
            "visual_changed": visual_counts["changed"],
            "visual_uncertain": visual_counts["uncertain"],
            "visual_skipped": visual_counts["skipped"],
            "visual_gate_available": visual_available,
        },
        "page_links": page_links,
        "block_links": block_links,
        "unmatched": {
            "left_blocks": unmatched_left,
            "right_blocks": unmatched_right,
        },
        "warnings": warnings,
    }


def write_block_link_preview_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт preview (tmp + os.replace)."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, out)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "LINK_STRONG",
    "LINK_WEAK",
    "LINK_MANUAL",
    "LINK_UNMATCHED",
    "COLOR_BY_STATUS",
    "SELECTED_COLOR",
    "MANUAL_REVIEW_RISK_FLAGS",
    "build_block_link_preview",
    "write_block_link_preview_report",
]
