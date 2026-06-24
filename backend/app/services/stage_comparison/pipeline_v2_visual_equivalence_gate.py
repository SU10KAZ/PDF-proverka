# -*- coding: utf-8 -*-
"""Pipeline V2 — Visual Equivalence Gate (mark-only, до vision).

Наложение/визуальное сравнение matched graphic blocks OLD↔NEW ПЕРЕД
vision-описанием: если блоки визуально идентичны после выравнивания —
помечаем `exclude_from_vision` (нет смысла жечь vision-вызовы и плодить
description-variance); изменились или алгоритм не уверен — `send_to_vision`;
не удалось отрендерить/сравнить — `manual_review`.

Этап **mark-only**: ничего не удаляет и не фильтрует физически, downstream
(entity diff / delta explanation) пока НЕ обязан использовать пометки.
Vision-модели отсюда НЕ запускаются; сравнение чисто офлайн.

Движок переиспользован из проверенного контура visual_block_equivalence
(Stage 3E cascade): grayscale → line-art mask → trim content bbox →
ECC Euclidean → ECC Affine → fallback (mask IoU + normalized correlation).
cv2 опционален: без него все сравнения честно деградируют в `uncertain`
→ `send_to_vision` (ложных исключений не бывает).
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.block_equivalence_precheck import (
    EqBlock,
    cv2_available,
    load_or_render_block_image,
)
from backend.app.services.stage_comparison.visual_block_equivalence import (
    VS_CHANGED,
    VS_IDENTICAL,
    VS_MINOR,
    VS_RENDER_FAILED,
    VS_UNCERTAIN,
    VisualBlockEquivalenceConfig,
    compare_block_images_cascade,
)

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_visual_equivalence_gate"

# Gate-статусы (минорный шум рендера движка → minor_visual)
GS_IDENTICAL = "identical_visual"
GS_MINOR = "minor_visual"
GS_CHANGED = "changed_visual"
GS_UNCERTAIN = "uncertain"
GS_RENDER_FAILED = "render_failed"
GS_SKIPPED = "skipped"

DECISION_EXCLUDE = "exclude_from_vision"
DECISION_VISION = "send_to_vision"
DECISION_MANUAL = "manual_review"

_DEFAULT_OPTIONS = {
    "render_long_side": 1000,
    # minor_visual исключается из vision только при высокой уверенности
    # (осторожный default), иначе — manual_review
    "minor_exclude_min_confidence": 0.8,
    # анти-dilution guard: ratio-пороги identical на большом блоке (1000px ≈
    # 1M px) растворяют малую реальную правку (смена «160А»→«250А» ≈ 50 px =
    # 0.005%); если diff_bbox непуст и оценка абсолютного diff больше cap —
    # identical НЕ исключается, а уходит на manual_review
    "identical_max_abs_diff_px": 60,
    "max_pairs": 400,
    "debug_dir": None,          # diagnostics-only: PNG-панели сравнения
}

# semantic_type, считающиеся графикой, для fallback-отбора пар из
# block_matching (когда graphic_matched_report не передан)
_GRAPHIC_SEMANTIC_TYPES = {"image", "scheme", "large_scheme", "plan",
                           "stamp", "legend", "figure", "drawing"}


def _opt(options: Optional[dict], key: str) -> Any:
    if isinstance(options, dict) and options.get(key) is not None:
        return options[key]
    return _DEFAULT_OPTIONS[key]


def _safe_int(value: Any, fallback: int) -> int:
    if isinstance(value, bool):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _safe_float(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


# ─── входные структуры ───────────────────────────────────────────────────────


def _blocks_by_id(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    blocks = m.get("blocks")
    if isinstance(blocks, dict):
        return {k: v for k, v in blocks.items() if isinstance(v, dict)}
    if isinstance(blocks, list):
        return {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
    return {}


def _pages_by_number(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    out: dict = {}
    for p in m.get("pages") or []:
        if isinstance(p, dict) and p.get("page_number") is not None:
            out[p["page_number"]] = p
    return out


def _model_pdf_path(model: Any) -> Optional[str]:
    m = model if isinstance(model, dict) else {}
    src = m.get("source") if isinstance(m.get("source"), dict) else {}
    return src.get("pdf_path") or None


def _is_graphic_block(block: Optional[dict]) -> bool:
    if not isinstance(block, dict):
        return False
    if block.get("block_type") == "image":
        return True
    return str(block.get("semantic_type") or "").lower() in _GRAPHIC_SEMANTIC_TYPES


def _matched_graphic_pairs(block_matching_report: Any,
                           graphic_matched_report: Any,
                           left_blocks: dict, right_blocks: dict,
                           warnings: list[str]) -> list[dict]:
    """Пары graphic-блоков для сравнения.

    Приоритет — graphic_descriptor_matched_report (уже отобранные графические
    пары с risk_flags); fallback — block_matches, у которых ОБЕ стороны
    графические. Non-graphic matched-блоки игнорируются by design.
    """
    matched: list = []
    gmr = graphic_matched_report
    if isinstance(gmr, dict):
        matched = gmr.get("matched") or gmr.get("matched_graphic_blocks") or []
    elif isinstance(gmr, list):
        matched = gmr
    pairs: list[dict] = []
    seen: set = set()
    for m in matched:
        if not isinstance(m, dict):
            continue
        lid, rid = m.get("left_block_id"), m.get("right_block_id")
        if not lid or not rid:
            continue
        # симметрично fallback-пути: обе ИЗВЕСТНЫЕ стороны должны быть
        # графикой (отсутствующий в модели блок не отбрасываем — он честно
        # уйдёт в skipped на этапе сравнения)
        lb, rb = left_blocks.get(lid), right_blocks.get(rid)
        if (lb is not None and not _is_graphic_block(lb)) or \
                (rb is not None and not _is_graphic_block(rb)):
            continue
        key = f"{lid}__{rid}"
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"left_block_id": lid, "right_block_id": rid,
                      "match_quality": m.get("match_quality"),
                      "risk_flags": list(m.get("risk_flags") or [])})
    if pairs:
        return pairs

    bmr = block_matching_report if isinstance(block_matching_report, dict) else {}
    for m in bmr.get("block_matches") or []:
        if not isinstance(m, dict):
            continue
        lid, rid = m.get("left_block_id"), m.get("right_block_id")
        if not lid or not rid:
            continue
        if not (_is_graphic_block(left_blocks.get(lid))
                and _is_graphic_block(right_blocks.get(rid))):
            continue
        key = f"{lid}__{rid}"
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"left_block_id": lid, "right_block_id": rid,
                      "match_quality": m.get("confidence"),
                      "risk_flags": list(m.get("risk_flags") or [])})
    if not matched and pairs:
        warnings.append("graphic_matched_report missing — graphic pairs "
                        "derived from block_matching")
    return pairs


def _eq_block(block: dict, pages: dict) -> EqBlock:
    page_no = block.get("page_number") or 0
    page = pages.get(page_no) if isinstance(pages.get(page_no), dict) else {}
    return EqBlock(
        block_id=str(block.get("block_id") or ""),
        page=_safe_int(page_no, 0),
        block_type=str(block.get("block_type") or "image"),
        coords_norm=block.get("coords_norm"),
        coords_px=block.get("coords_px"),
        page_width=_safe_int(page.get("width"), 0),
        page_height=_safe_int(page.get("height"), 0),
        text=str(block.get("text_excerpt") or ""),
        image_file=block.get("image_file"),
        crop_url=block.get("crop_url"),
        raw=block,
    )


def _readiness_of(graphic_report: Any, block_id: str) -> Optional[str]:
    gr = graphic_report if isinstance(graphic_report, dict) else {}
    for d in gr.get("descriptors") or []:
        if isinstance(d, dict) and d.get("block_id") == block_id:
            return ((d.get("diff_readiness") or {}).get("readiness")
                    if isinstance(d.get("diff_readiness"), dict) else None)
    return None


# ─── статус/решение ──────────────────────────────────────────────────────────


_ENGINE_TO_GATE = {
    VS_IDENTICAL: GS_IDENTICAL,
    VS_MINOR: GS_MINOR,
    VS_CHANGED: GS_CHANGED,
    VS_UNCERTAIN: GS_UNCERTAIN,
    VS_RENDER_FAILED: GS_RENDER_FAILED,
    # alignment_failed / visual_unavailable и прочее → uncertain (safe)
}


def _gate_confidence(status: str, metrics: dict,
                     cfg: VisualBlockEquivalenceConfig) -> float:
    """Детерминированная уверенность решения по метрикам движка."""
    iou = metrics.get("mask_iou")
    ncc = metrics.get("normalized_correlation")
    ratio = _safe_float(metrics.get("total_diff_ratio"), 1.0)
    if status == GS_IDENTICAL:
        cands = [v for v in (iou, ncc) if isinstance(v, (int, float))]
        return round(min(cands) if cands else 0.95, 3)
    if status == GS_MINOR:
        # 1.0 у границы identical, 0.0 у границы changed — линейно
        lo = float(cfg.visual_identical_max_ratio)
        hi = max(float(cfg.minor_noise_max_ratio), lo + 1e-6)
        return round(max(0.0, min(1.0, 1.0 - (ratio - lo) / (hi - lo))), 3)
    if status == GS_CHANGED:
        return 0.9 if metrics.get("alignment_method") not in (
            None, "failed", "fallback_mask", "fallback_correlation") else 0.7
    if status == GS_UNCERTAIN:
        return 0.3
    return 0.0


def decide_from_status(status: str, confidence: float,
                       options: Optional[dict] = None) -> tuple[str, str]:
    """(decision, reason) по gate-статусу. Mark-only: только пометка."""
    if status == GS_IDENTICAL:
        return DECISION_EXCLUDE, "блоки визуально совпадают после выравнивания"
    if status == GS_MINOR:
        thr = _safe_float(_opt(options, "minor_exclude_min_confidence"), 0.8)
        if confidence >= thr:
            return DECISION_EXCLUDE, (f"минорный шум рендера, "
                                      f"confidence {confidence:.2f} ≥ {thr:.2f}")
        return DECISION_MANUAL, (f"минорное отличие, confidence "
                                 f"{confidence:.2f} < {thr:.2f} — взгляд человека")
    if status == GS_CHANGED:
        return DECISION_VISION, "визуальное изменение — нужен vision-анализ"
    if status == GS_UNCERTAIN:
        return DECISION_VISION, ("алгоритм не уверен — лучше не пропустить "
                                 "изменение")
    if status == GS_RENDER_FAILED:
        return DECISION_MANUAL, "не удалось получить изображение блока"
    return DECISION_MANUAL, "пара не сравнивалась"


# ─── основной вход ───────────────────────────────────────────────────────────


def run_visual_equivalence_gate(left_model: Any, right_model: Any,
                                block_matching_report: Any,
                                left_graphic_report: Any = None,
                                right_graphic_report: Any = None,
                                graphic_matched_report: Any = None,
                                options: Optional[dict] = None) -> dict:
    """Сравнить matched graphic blocks OLD↔NEW и пометить решения для vision.

    Mark-only и fail-soft: ошибка по одной паре не валит отчёт; cv2/рендер
    недоступны → uncertain/render_failed, НЕ ложный exclude.
    """
    warnings: list[str] = []
    cfg = VisualBlockEquivalenceConfig.from_env()
    render_long_side = _safe_int(_opt(options, "render_long_side"), 1000)
    max_pairs = _safe_int(_opt(options, "max_pairs"), 400)
    debug_dir = _opt(options, "debug_dir")
    debug_dir = Path(debug_dir) if debug_dir else None
    if debug_dir:
        try:
            debug_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            warnings.append(f"debug_dir unavailable: {exc}")
            debug_dir = None

    left_blocks = _blocks_by_id(left_model)
    right_blocks = _blocks_by_id(right_model)
    left_pages = _pages_by_number(left_model)
    right_pages = _pages_by_number(right_model)
    left_pdf = _model_pdf_path(left_model)
    right_pdf = _model_pdf_path(right_model)

    if not cv2_available():
        warnings.append("cv2 unavailable — visual compare degraded to "
                        "uncertain/send_to_vision")

    pairs = _matched_graphic_pairs(block_matching_report,
                                   graphic_matched_report,
                                   left_blocks, right_blocks, warnings)

    # кеш загруженных изображений: один блок может входить в несколько пар
    img_cache: dict = {}

    def _load(side: str, block: dict, pages: dict, pdf: Optional[str]):
        bid = block.get("block_id")
        key = (side, bid)
        if key not in img_cache:
            img_cache[key] = load_or_render_block_image(
                _eq_block(block, pages), source_pdf_path=pdf,
                render_long_side=render_long_side)
        return img_cache[key]

    block_pairs: list[dict] = []
    counts = {GS_IDENTICAL: 0, GS_MINOR: 0, GS_CHANGED: 0, GS_UNCERTAIN: 0,
              GS_RENDER_FAILED: 0, GS_SKIPPED: 0}
    decisions = {DECISION_EXCLUDE: 0, DECISION_VISION: 0, DECISION_MANUAL: 0}
    compared = 0

    if len(pairs) > max_pairs:
        warnings.append(f"matched graphic pairs {len(pairs)} > max_pairs "
                        f"{max_pairs} — excess marked skipped")

    for idx, pair in enumerate(pairs):
        lid, rid = pair["left_block_id"], pair["right_block_id"]
        entry: dict[str, Any] = {
            "pair_key": f"{lid}__{rid}",
            "left_block_id": lid,
            "right_block_id": rid,
            "left_page_number": (left_blocks.get(lid) or {}).get("page_number"),
            "right_page_number": (right_blocks.get(rid) or {}).get("page_number"),
            "left_crop_path": (left_blocks.get(lid) or {}).get("image_file"),
            "right_crop_path": (right_blocks.get(rid) or {}).get("image_file"),
            "match_quality": pair.get("match_quality"),
            "risk_flags": list(pair.get("risk_flags") or []),
            "metrics": {"mask_iou": None, "normalized_correlation": None,
                        "foreground_ratio_left": None,
                        "foreground_ratio_right": None,
                        "alignment_method": None,
                        "total_diff_ratio": None,
                        "diff_bbox": None},
        }
        lr = _readiness_of(left_graphic_report, lid)
        rr = _readiness_of(right_graphic_report, rid)
        if lr in ("low", "not_usable"):
            entry["risk_flags"].append(f"left_readiness_{lr}")
        if rr in ("low", "not_usable"):
            entry["risk_flags"].append(f"right_readiness_{rr}")

        try:
            if idx >= max_pairs:
                status, reason = GS_SKIPPED, "max_pairs cap"
            elif lid not in left_blocks or rid not in right_blocks:
                status, reason = GS_SKIPPED, "block missing in normalized model"
            else:
                limg, lmeta = _load("left", left_blocks[lid], left_pages, left_pdf)
                rimg, rmeta = _load("right", right_blocks[rid], right_pages, right_pdf)
                entry["left_render"] = lmeta.get("status")
                entry["right_render"] = rmeta.get("status")
                if limg is None or rimg is None:
                    status, reason = GS_RENDER_FAILED, (
                        f"render failed (left={lmeta.get('status')}, "
                        f"right={rmeta.get('status')})")
                else:
                    compared += 1
                    dbg = (debug_dir / f"{entry['pair_key'][:80]}.png"
                           if debug_dir else None)
                    res = compare_block_images_cascade(limg, rimg, cfg=cfg,
                                                       debug_path=dbg)
                    for mk in ("mask_iou", "normalized_correlation",
                               "alignment_method", "total_diff_ratio",
                               "diff_bbox"):
                        entry["metrics"][mk] = res.get(mk)
                    entry["metrics"]["foreground_ratio_left"] = res.get(
                        "foreground_ratio_old")
                    entry["metrics"]["foreground_ratio_right"] = res.get(
                        "foreground_ratio_new")
                    engine_status = res.get("status")
                    # minor-band: движок (cascade) сам minor не возвращает —
                    # реклассифицируем changed в полосе «шум рендера», как в
                    # compare_one_link_visual_equivalence
                    tdr = res.get("total_diff_ratio")
                    cdr = res.get("colored_overlay_diff_ratio")
                    if (engine_status == VS_CHANGED
                            and isinstance(tdr, (int, float))
                            and tdr <= cfg.minor_noise_max_ratio
                            and (cdr is None
                                 or cdr <= cfg.colored_minor_max_ratio)):
                        engine_status = VS_MINOR
                    status = _ENGINE_TO_GATE.get(engine_status, GS_UNCERTAIN)
                    reason = f"engine: {engine_status}"
                    # анти-dilution: identical с непустым diff_bbox и заметным
                    # АБСОЛЮТНЫМ остатком — это локальная правка (смена
                    # номинала на большом блоке), не исключаем
                    if status == GS_IDENTICAL and res.get("diff_bbox"):
                        area = int(limg.shape[0]) * int(limg.shape[1])
                        est_px = _safe_float(tdr, 0.0) * area
                        cap_px = _safe_float(
                            _opt(options, "identical_max_abs_diff_px"), 60.0)
                        if est_px > cap_px:
                            entry["risk_flags"].append("localized_residual_diff")
                            entry["residual_diff_px_estimate"] = round(est_px, 1)
        except Exception as exc:  # noqa: BLE001 — одна пара не валит gate
            status, reason = GS_UNCERTAIN, f"{type(exc).__name__}: {exc}"
            warnings.append(f"{entry['pair_key']}: {type(exc).__name__}: {exc}")

        confidence = _gate_confidence(status, entry["metrics"], cfg)
        if status == GS_IDENTICAL and entry["metrics"].get("diff_bbox"):
            confidence = min(confidence, 0.95)  # ненулевой остаток ≠ 1.0
        decision, dreason = decide_from_status(status, confidence, options)
        if "localized_residual_diff" in entry["risk_flags"]:
            decision = DECISION_MANUAL
            dreason = ("локализованный остаточный diff "
                       f"(~{entry.get('residual_diff_px_estimate')} px) — "
                       "возможна малая реальная правка, взгляд человека")
        entry["status"] = status
        entry["decision"] = decision
        entry["confidence"] = confidence
        entry["reason"] = f"{reason}; {dreason}"
        counts[status] = counts.get(status, 0) + 1
        decisions[decision] = decisions.get(decision, 0) + 1
        block_pairs.append(entry)

    status = "completed_with_warnings" if warnings else "ok"
    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": {
            "matched_graphic_blocks_total": len(pairs),
            "compared_total": compared,
            "identical_visual": counts[GS_IDENTICAL],
            "minor_visual": counts[GS_MINOR],
            "changed_visual": counts[GS_CHANGED],
            "uncertain": counts[GS_UNCERTAIN],
            "render_failed": counts[GS_RENDER_FAILED],
            "skipped": counts[GS_SKIPPED],
            "exclude_from_vision": decisions[DECISION_EXCLUDE],
            "send_to_vision": decisions[DECISION_VISION],
            "manual_review": decisions[DECISION_MANUAL],
            "cv2_available": cv2_available(),
        },
        "block_pairs": block_pairs,
        "warnings": warnings,
    }


def write_visual_equivalence_gate_report(out_path: str | Path,
                                         report: dict) -> Path:
    """Атомарно записать отчёт gate (tmp + os.replace)."""
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
    "GS_IDENTICAL",
    "GS_MINOR",
    "GS_CHANGED",
    "GS_UNCERTAIN",
    "GS_RENDER_FAILED",
    "GS_SKIPPED",
    "DECISION_EXCLUDE",
    "DECISION_VISION",
    "DECISION_MANUAL",
    "run_visual_equivalence_gate",
    "decide_from_status",
    "write_visual_equivalence_gate_report",
]
