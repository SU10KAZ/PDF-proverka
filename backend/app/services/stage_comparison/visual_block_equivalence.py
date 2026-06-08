# -*- coding: utf-8 -*-
"""Visual block equivalence precheck (Stage 2 — links-based, mark-only).

Отдельный безопасный слой ПОВЕРХ уже существующей механики
:mod:`block_equivalence_precheck`. Отличия от Stage 1:

  * парирование берётся из ЯВНЫХ связей ``links.json`` (а не из IoU);
  * сравниваются только связанные пары блоков (никакого IoU-fallback);
  * результат пишется в ОТДЕЛЬНЫЙ артефакт
    ``pairs/<pid>/visual_block_equivalence/visual_block_equivalence.json``;
  * режим ``mark_only`` — никакого реального skip. Флаги
    ``exclude_from_qwen`` / ``exclude_from_opus_md`` записываются, но
    ИНФОРМАЦИОННЫ: ни Qwen, ни enriched MD, ни Opus их пока не читают.
    ``enforced`` всегда ``False``.

Решения по спорным вопросам (из задачи Stage 2):

  * Используются ТОЛЬКО явные связи ``links.json``. IoU-fallback не делаем.
  * ``*_stale`` связи НЕ сравниваются → статус ``skipped_stale_link``.
  * ``manual_cross_page`` сравнивается (страницы блоков берутся из result.json;
    в связи также есть left_page/right_page для контекста).
  * Скоуп — только image/graphic-блоки. text/table со ЗНАЧИМЫМ типом
    пропускаются (``skipped_non_image``).
  * Связь, чей блок участвует в 1↔много / много↔1, не сравнивается
    (``skipped_not_one_to_one``).

Переиспользование (без дублирования тяжёлой логики): рендер кропа, ECC/pixel/
colored визуальное сравнение и текстовое сравнение берутся как есть из
:mod:`block_equivalence_precheck` (``load_or_render_block_image``,
``compare_visual_blocks``, ``compare_text_blocks``, ``extract_blocks_for_equivalence``).

Модуль fail-soft: ошибка рендера/сравнения одной связи не валит batch
(``render_failed`` / ``uncertain`` для этой связи, остальные считаются).

ВАЖНО: модуль НЕ встроен в работающий pipeline. Его можно вызвать из
тестов/CLI/будущего endpoint, но он не подключён к ``md_enrichment_jobs`` и
ничего в Qwen/MD/Opus не меняет.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from .block_equivalence_precheck import (
    BlockEquivalenceConfig,
    EqBlock,
    compare_text_blocks,
    compare_visual_blocks,
    cv2_available,
    extract_blocks_for_equivalence,
    load_or_render_block_image,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# env helpers (локальные копии — модуль self-contained, без приватных импортов)
# ═══════════════════════════════════════════════════════════════════════════


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return int(float(raw))
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════════════════
# Status constants
# ═══════════════════════════════════════════════════════════════════════════

# Статусы сравнённых связей
VS_IDENTICAL = "identical_visual"
VS_MINOR = "minor_render_noise"
VS_CHANGED = "changed_visual"
VS_UNCERTAIN = "uncertain"
VS_RENDER_FAILED = "render_failed"

# Статусы НЕ сравнённых (skipped) связей
VS_SKIP_STALE = "skipped_stale_link"
VS_SKIP_NOT_1_1 = "skipped_not_one_to_one"
VS_SKIP_NON_IMAGE = "skipped_non_image"
VS_SKIP_BLOCK_MISSING = "skipped_block_missing"

# Группы для агрегации summary
_COMPARED_STATUSES = {VS_IDENTICAL, VS_MINOR, VS_CHANGED, VS_UNCERTAIN, VS_RENDER_FAILED}
_SKIPPED_STATUSES = {VS_SKIP_STALE, VS_SKIP_NOT_1_1, VS_SKIP_NON_IMAGE, VS_SKIP_BLOCK_MISSING}

# Единственный статус, дающий потенциальное исключение (mark-only).
_EXCLUDE_STATUSES = {VS_IDENTICAL}


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class VisualBlockEquivalenceConfig:
    """Параметры визуального прекчека по связям (env, безопасные дефолты).

    Пороги визуального сравнения совпадают с :class:`BlockEquivalenceConfig`,
    чтобы links-based слой и IoU-слой судили одинаково. ``minor_noise_*`` —
    дополнительная «полоса шума рендера» между identical и changed.
    """

    enabled: bool = False           # главный флаг (по умолчанию OFF); НЕ авто-включает pipeline
    mode: str = "mark_only"         # Stage 2: единственный режим
    enforced: bool = False          # Stage 2: всегда False — реального skip нет

    render_long_side: int = 1000
    visual_diff_pixel_threshold: int = 30
    visual_identical_max_ratio: float = 0.02
    colored_diff_sat_threshold: int = 40
    colored_identical_max_ratio: float = 0.01
    ecc_min_score: float = 0.55

    # «Полоса незначимого шума рендера»: changed_visual с diff в этой полосе
    # переклассифицируется в minor_render_noise (НЕ исключается, информативно).
    minor_noise_max_ratio: float = 0.05
    colored_minor_max_ratio: float = 0.02

    max_links_compared: int = 2000  # safety cap на число визуальных сравнений на пару

    @classmethod
    def from_env(cls) -> "VisualBlockEquivalenceConfig":
        return cls(
            enabled=_env_flag("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_ENABLED", False),
            render_long_side=_env_int(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_RENDER_LONG_SIDE", 1000),
            visual_diff_pixel_threshold=_env_int(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_VISUAL_DIFF_PIXEL_THRESHOLD", 30),
            visual_identical_max_ratio=_env_float(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_VISUAL_IDENTICAL_MAX_RATIO", 0.02),
            colored_diff_sat_threshold=_env_int(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_COLORED_DIFF_SAT_THRESHOLD", 40),
            colored_identical_max_ratio=_env_float(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_COLORED_IDENTICAL_MAX_RATIO", 0.01),
            ecc_min_score=_env_float(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_ECC_MIN_SCORE", 0.55),
            minor_noise_max_ratio=_env_float(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_MINOR_NOISE_MAX_RATIO", 0.05),
            colored_minor_max_ratio=_env_float(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_COLORED_MINOR_MAX_RATIO", 0.02),
            max_links_compared=_env_int(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_MAX_LINKS_COMPARED", 2000),
        )

    def to_block_cfg(self) -> BlockEquivalenceConfig:
        """BlockEquivalenceConfig с теми же порогами — для ``compare_visual_blocks``."""
        return BlockEquivalenceConfig(
            render_long_side=self.render_long_side,
            visual_diff_pixel_threshold=self.visual_diff_pixel_threshold,
            visual_identical_max_ratio=self.visual_identical_max_ratio,
            colored_diff_sat_threshold=self.colored_diff_sat_threshold,
            colored_identical_max_ratio=self.colored_identical_max_ratio,
            ecc_min_score=self.ecc_min_score,
        )


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════


def _is_stale_link(method: Any) -> bool:
    """Связь считается stale, если method оканчивается на ``_stale``."""
    return str(method or "").endswith("_stale")


def _link_pages(link: dict, old_block: Optional[EqBlock], new_block: Optional[EqBlock]):
    """left_page/right_page для записи в артефакт: предпочитаем связь, затем блок."""
    lp = link.get("left_page")
    rp = link.get("right_page")
    if lp is None and old_block is not None:
        lp = old_block.page
    if rp is None and new_block is not None:
        rp = new_block.page
    return lp, rp


def _empty_metrics() -> dict:
    return {"total_diff_ratio": None, "colored_overlay_diff_ratio": None, "alignment_score": None}


def _exclusion_for(status: str) -> bool:
    return status in _EXCLUDE_STATUSES


def _base_record(link: dict, old_block: Optional[EqBlock], new_block: Optional[EqBlock],
                 *, status: str, reason: str, metrics: Optional[dict] = None,
                 confidence: Optional[float] = None,
                 old_render: Optional[str] = None, new_render: Optional[str] = None,
                 diff_mask: Optional[str] = None) -> dict:
    lp, rp = _link_pages(link, old_block, new_block)
    exclude = _exclusion_for(status)
    return {
        "left_block_id": link.get("left_block_id"),
        "right_block_id": link.get("right_block_id"),
        "left_page": lp,
        "right_page": rp,
        "link_method": link.get("method"),
        "link_score": link.get("score"),
        "old_type": getattr(old_block, "block_type", None),
        "new_type": getattr(new_block, "block_type", None),
        "status": status,
        "confidence": confidence,
        "metrics": metrics or _empty_metrics(),
        "reason": reason,
        "exclude_from_qwen": exclude,
        "exclude_from_opus_md": exclude,
        "enforced": False,           # Stage 2: всегда False
        "debug": {
            "old_render": old_render,
            "new_render": new_render,
            "diff_mask": diff_mask,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# compare_one_link_visual_equivalence
# ═══════════════════════════════════════════════════════════════════════════


def compare_one_link_visual_equivalence(
    link: dict,
    old_block: Optional[EqBlock],
    new_block: Optional[EqBlock],
    *,
    cfg: Optional[VisualBlockEquivalenceConfig] = None,
    old_pdf_path: Optional[str | Path] = None,
    new_pdf_path: Optional[str | Path] = None,
    debug_path: Optional[str | Path] = None,
    skip_status: Optional[str] = None,
    skip_reason: Optional[str] = None,
    render_fn: Optional[Callable[..., tuple]] = None,
    visual_compare_fn: Optional[Callable[..., dict]] = None,
    text_compare_fn: Optional[Callable[..., dict]] = None,
) -> dict:
    """Сравнить ОДНУ связь блоков визуально и вернуть структурированную запись.

    ``link`` — словарь связи из ``links.json`` (left_block_id/right_block_id/
    method/score/…). ``old_block``/``new_block`` — уже загруженные ``EqBlock``
    из result.json (или ``None``, если блок не найден).

    ``skip_status``/``skip_reason`` — если переданы (например, batch уже
    определил stale / не-1↔1), запись формируется БЕЗ рендера/сравнения.

    Инъекции для тестов (не дёргают PDF/cv2 при наличии):
      * ``render_fn(block, source_pdf_path=, render_long_side=) -> (img, meta)``
        (default ``load_or_render_block_image``);
      * ``visual_compare_fn(old_img, new_img, cfg=, debug_path=) -> dict``
        (default ``compare_visual_blocks``);
      * ``text_compare_fn(old_block, new_block) -> dict``
        (default ``compare_text_blocks``).

    Полностью fail-soft: исключение → ``render_failed`` / ``uncertain``, не
    бросает наружу.
    """
    cfg = cfg or VisualBlockEquivalenceConfig.from_env()
    render_fn = render_fn or load_or_render_block_image
    visual_compare_fn = visual_compare_fn or compare_visual_blocks
    text_compare_fn = text_compare_fn or compare_text_blocks

    # 0. Принудительный skip от batch (stale / не-1↔1) — без рендера/сравнения.
    if skip_status:
        return _base_record(link, old_block, new_block,
                            status=skip_status, reason=skip_reason or skip_status)

    left_id = link.get("left_block_id")
    right_id = link.get("right_block_id")

    # 1. Блок не найден в result.json → сравнивать нечего.
    if not left_id or not right_id or old_block is None or new_block is None:
        missing = []
        if not left_id or old_block is None:
            missing.append(f"left:{left_id}")
        if not right_id or new_block is None:
            missing.append(f"right:{right_id}")
        return _base_record(link, old_block, new_block, status=VS_SKIP_BLOCK_MISSING,
                            reason=f"block not found in result.json ({', '.join(missing)})")

    # 2. Скоуп — только image/graphic. Известный text/table пропускаем.
    if old_block.is_text_like or new_block.is_text_like:
        return _base_record(link, old_block, new_block, status=VS_SKIP_NON_IMAGE,
                            reason=(f"non-image block (old={old_block.block_type}, "
                                    f"new={new_block.block_type}); scope=image only"))

    # 3. Рендер обоих кропов (fail-soft).
    try:
        old_img, old_meta = render_fn(
            old_block, source_pdf_path=old_pdf_path, render_long_side=cfg.render_long_side)
        new_img, new_meta = render_fn(
            new_block, source_pdf_path=new_pdf_path, render_long_side=cfg.render_long_side)
    except Exception as exc:  # noqa: BLE001 — fail-soft
        logger.debug("visual_block_equivalence: render failed %s/%s: %s", left_id, right_id, exc)
        return _base_record(link, old_block, new_block, status=VS_RENDER_FAILED,
                            reason=f"render exception: {type(exc).__name__}")

    old_render = (old_meta or {}).get("status")
    new_render = (new_meta or {}).get("status")
    if old_img is None or new_img is None:
        return _base_record(link, old_block, new_block, status=VS_RENDER_FAILED,
                            reason=f"render failed (old={old_render}, new={new_render})",
                            old_render=old_render, new_render=new_render)

    # 4. Визуальное сравнение (переиспользуем существующую ECC/pixel/colored логику).
    try:
        vis = visual_compare_fn(old_img, new_img, cfg=cfg.to_block_cfg(), debug_path=debug_path)
    except Exception as exc:  # noqa: BLE001 — fail-soft
        logger.debug("visual_block_equivalence: compare failed %s/%s: %s", left_id, right_id, exc)
        return _base_record(link, old_block, new_block, status=VS_UNCERTAIN,
                            reason=f"compare exception: {type(exc).__name__}",
                            old_render=old_render, new_render=new_render)

    vis = vis or {}
    total_diff = vis.get("total_diff_ratio")
    colored_diff = vis.get("colored_overlay_diff_ratio")
    align = vis.get("alignment_score")
    metrics = {
        "total_diff_ratio": total_diff,
        "colored_overlay_diff_ratio": colored_diff,
        "alignment_score": align,
    }
    diff_mask = None
    if debug_path is not None:
        try:
            if Path(debug_path).exists():
                diff_mask = str(debug_path)
        except Exception:  # noqa: BLE001
            diff_mask = None

    # 5. Маппинг статуса визуала → статус прекчека.
    vstatus = vis.get("status")
    confidence = round(float(align), 4) if isinstance(align, (int, float)) else None

    if vstatus == "identical_visual":
        status = VS_IDENTICAL
        reason = f"ECC ok; diff={total_diff} colored={colored_diff} ≤ identical thresholds"
    elif vstatus == "changed_visual":
        # Полоса «незначимого шума рендера» → minor_render_noise (не исключаем).
        is_minor = (
            isinstance(total_diff, (int, float))
            and total_diff <= cfg.minor_noise_max_ratio
            and (not isinstance(colored_diff, (int, float))
                 or colored_diff <= cfg.colored_minor_max_ratio)
        )
        if is_minor:
            status = VS_MINOR
            reason = (f"minor render noise; diff={total_diff} colored={colored_diff} "
                      f"≤ minor band (keep for Qwen)")
        else:
            status = VS_CHANGED
            reason = f"visual changed; diff={total_diff} colored={colored_diff}"
    elif vstatus == "render_failed":
        status = VS_RENDER_FAILED
        reason = "visual render_failed"
    else:
        # alignment_failed / visual_unavailable / неизвестный → uncertain (НЕ исключаем).
        status = VS_UNCERTAIN
        reason = f"visual uncertain (status={vstatus})"

    return _base_record(
        link, old_block, new_block,
        status=status, reason=reason, metrics=metrics, confidence=confidence,
        old_render=old_render, new_render=new_render, diff_mask=diff_mask,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Batch over links.json
# ═══════════════════════════════════════════════════════════════════════════


def _empty_summary() -> dict:
    return {
        "links_total": 0,
        "links_compared": 0,
        "identical_visual": 0,
        "minor_render_noise": 0,
        "changed_visual": 0,
        "uncertain": 0,
        "render_failed": 0,
        "skipped": 0,
        "skipped_breakdown": {
            "stale_link": 0,
            "not_one_to_one": 0,
            "non_image": 0,
            "block_missing": 0,
        },
        "potential_qwen_saved": 0,
        "potential_opus_blocks_removed": 0,
    }


_SKIP_SUMMARY_KEY = {
    VS_SKIP_STALE: "stale_link",
    VS_SKIP_NOT_1_1: "not_one_to_one",
    VS_SKIP_NON_IMAGE: "non_image",
    VS_SKIP_BLOCK_MISSING: "block_missing",
}


def _tally(summary: dict, record: dict) -> None:
    status = record.get("status")
    if status in _COMPARED_STATUSES:
        summary["links_compared"] += 1
        summary[status] = summary.get(status, 0) + 1
        if status == VS_IDENTICAL:
            summary["potential_qwen_saved"] += 1
            summary["potential_opus_blocks_removed"] += 1
    elif status in _SKIPPED_STATUSES:
        summary["skipped"] += 1
        key = _SKIP_SUMMARY_KEY.get(status)
        if key:
            summary["skipped_breakdown"][key] += 1


def _one_to_one_violations(links: list[dict]) -> set[int]:
    """Индексы связей, нарушающих 1↔1 (block участвует в >1 НЕ-stale связи).

    stale-связи в подсчёте множественности не учитываются (они и так
    пропускаются)."""
    from collections import Counter

    left_counter: Counter = Counter()
    right_counter: Counter = Counter()
    for link in links:
        if _is_stale_link(link.get("method")):
            continue
        lid = link.get("left_block_id")
        rid = link.get("right_block_id")
        if lid:
            left_counter[lid] += 1
        if rid:
            right_counter[rid] += 1

    violations: set[int] = set()
    for idx, link in enumerate(links):
        if _is_stale_link(link.get("method")):
            continue
        lid = link.get("left_block_id")
        rid = link.get("right_block_id")
        if (lid and left_counter[lid] > 1) or (rid and right_counter[rid] > 1):
            violations.add(idx)
    return violations


def run_pair_visual_block_equivalence(
    session_id: str,
    pair_id: str,
    *,
    cfg: Optional[VisualBlockEquivalenceConfig] = None,
    links: Optional[list[dict]] = None,
    old_blocks: Optional[list[EqBlock]] = None,
    new_blocks: Optional[list[EqBlock]] = None,
    old_pdf_path: Optional[str | Path] = None,
    new_pdf_path: Optional[str | Path] = None,
    write_artifact: bool = True,
    write_debug: bool = True,
    generated_at: Optional[str] = None,
    render_fn: Optional[Callable[..., tuple]] = None,
    visual_compare_fn: Optional[Callable[..., dict]] = None,
    text_compare_fn: Optional[Callable[..., dict]] = None,
) -> dict:
    """Прогнать визуальный прекчек по ВСЕМ явным связям пары и собрать артефакт.

    ``left`` = OLD (старая стадия), ``right`` = NEW (новая стадия).

    Источники по умолчанию (если не переданы явно) резолвятся из store/paths:
      * ``links`` ← ``store._pair_links``;
      * ``old_blocks``/``new_blocks`` ← ``extract_blocks_for_equivalence`` по
        result.json из ``pair.json``;
      * ``old_pdf_path``/``new_pdf_path`` ← ``pair.json``.

    Параметры ``links``/``old_blocks``/``new_blocks``/``*_pdf_path`` и
    ``*_fn`` позволяют тестам работать без store/PDF/cv2.

    mark-only: статусы/метрики пишутся, реального skip нет. Возвращает
    собранный отчёт (dict). Fail-soft на уровне каждой связи.
    """
    cfg = cfg or VisualBlockEquivalenceConfig.from_env()
    generated_at = generated_at or _utc_now_iso()

    # ── Резолв источников (lazy import store/paths — модуль остаётся
    #    импортируемым в изоляции; тесты передают данные напрямую). ──
    debug_dir: Optional[Path] = None
    if links is None or old_blocks is None or new_blocks is None \
            or old_pdf_path is None or new_pdf_path is None or (write_artifact or write_debug):
        try:
            from . import store as store_mod
            from . import paths as paths_mod
        except Exception:  # noqa: BLE001
            store_mod = None  # type: ignore
            paths_mod = None  # type: ignore

        pair = None
        if store_mod is not None:
            try:
                pair = store_mod._find_pair_meta(session_id, pair_id)
            except Exception:  # noqa: BLE001 — fail-soft
                pair = None
        left = (pair or {}).get("left") or {}
        right = (pair or {}).get("right") or {}

        if links is None and store_mod is not None:
            try:
                links = store_mod._pair_links(session_id, pair_id)
            except Exception:  # noqa: BLE001
                links = []
        if old_blocks is None:
            old_blocks = extract_blocks_for_equivalence(left.get("result_json_path")) \
                if left.get("result_json_path") else []
        if new_blocks is None:
            new_blocks = extract_blocks_for_equivalence(right.get("result_json_path")) \
                if right.get("result_json_path") else []
        if old_pdf_path is None:
            old_pdf_path = left.get("pdf_path")
        if new_pdf_path is None:
            new_pdf_path = right.get("pdf_path")
        if write_debug and paths_mod is not None:
            try:
                debug_dir = paths_mod.visual_block_equivalence_debug_dir(session_id, pair_id)
            except Exception:  # noqa: BLE001
                debug_dir = None

    links = list(links or [])
    old_by_id = {b.block_id: b for b in (old_blocks or [])}
    new_by_id = {b.block_id: b for b in (new_blocks or [])}

    violations = _one_to_one_violations(links)

    summary = _empty_summary()
    summary["links_total"] = len(links)
    pairs_out: list[dict] = []
    warnings: list[str] = []
    compares_done = 0

    for idx, link in enumerate(links):
        old_block = old_by_id.get(link.get("left_block_id"))
        new_block = new_by_id.get(link.get("right_block_id"))

        skip_status: Optional[str] = None
        skip_reason: Optional[str] = None
        if _is_stale_link(link.get("method")):
            skip_status = VS_SKIP_STALE
            skip_reason = f"stale link (method={link.get('method')})"
        elif idx in violations:
            skip_status = VS_SKIP_NOT_1_1
            skip_reason = "block participates in 1↔many / many↔1 (not one-to-one)"

        debug_path = None
        will_compare = (
            skip_status is None
            and old_block is not None and new_block is not None
            and not old_block.is_text_like and not new_block.is_text_like
        )
        if will_compare and compares_done >= cfg.max_links_compared:
            # safety cap — не сравниваем, помечаем uncertain
            warnings.append("max_links_compared_reached")
            rec = _base_record(link, old_block, new_block, status=VS_UNCERTAIN,
                              reason="max_links_compared cap reached")
            pairs_out.append(rec)
            _tally(summary, rec)
            continue

        if will_compare and debug_dir is not None:
            safe = "".join(c if c.isalnum() else "_"
                           for c in str(link.get("left_block_id") or f"link{idx}"))[:48]
            debug_path = debug_dir / f"{safe}_diff.png"

        rec = compare_one_link_visual_equivalence(
            link, old_block, new_block,
            cfg=cfg,
            old_pdf_path=old_pdf_path, new_pdf_path=new_pdf_path,
            debug_path=debug_path,
            skip_status=skip_status, skip_reason=skip_reason,
            render_fn=render_fn, visual_compare_fn=visual_compare_fn,
            text_compare_fn=text_compare_fn,
        )
        if rec.get("status") in _COMPARED_STATUSES:
            compares_done += 1
        pairs_out.append(rec)
        _tally(summary, rec)

    if not cv2_available():
        warnings.append("cv2_unavailable_visual_compare_degraded")

    report = {
        "schema_version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "mode": cfg.mode,
        "source": "links_json",
        "enabled": cfg.enabled,
        "enforced": False,                 # Stage 2: реального skip нет
        "cv2_available": cv2_available(),
        "thresholds": {
            "visual_identical_max_ratio": cfg.visual_identical_max_ratio,
            "colored_identical_max_ratio": cfg.colored_identical_max_ratio,
            "minor_noise_max_ratio": cfg.minor_noise_max_ratio,
            "colored_minor_max_ratio": cfg.colored_minor_max_ratio,
            "ecc_min_score": cfg.ecc_min_score,
            "render_long_side": cfg.render_long_side,
        },
        "summary": summary,
        "pairs": pairs_out,
        "warnings": warnings,
        "compares_done": compares_done,
    }

    if write_artifact:
        try:
            from . import paths as paths_mod
            out = paths_mod.visual_block_equivalence_report_path(session_id, pair_id)
            _atomic_write_json(out, report)
        except Exception as exc:  # noqa: BLE001 — artifact write non-fatal
            logger.warning("visual_block_equivalence: report write failed %s/%s: %s",
                           session_id, pair_id, exc)

    return report


def read_pair_visual_block_equivalence(session_id: str, pair_id: str) -> Optional[dict]:
    """Прочитать сохранённый артефакт пары (если есть). Для будущего endpoint/UI."""
    try:
        import json
        from . import paths as paths_mod
        p = paths_mod.visual_block_equivalence_report_path(session_id, pair_id)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════════════════════════════


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Атомарная запись JSON (tmp → os.replace) — паттерн проекта (store._atomic_write_json)."""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


__all__ = [
    "VisualBlockEquivalenceConfig",
    "compare_one_link_visual_equivalence",
    "run_pair_visual_block_equivalence",
    "read_pair_visual_block_equivalence",
    # status constants
    "VS_IDENTICAL",
    "VS_MINOR",
    "VS_CHANGED",
    "VS_UNCERTAIN",
    "VS_RENDER_FAILED",
    "VS_SKIP_STALE",
    "VS_SKIP_NOT_1_1",
    "VS_SKIP_NON_IMAGE",
    "VS_SKIP_BLOCK_MISSING",
]
