# -*- coding: utf-8 -*-
"""Text block equivalence precheck (Stage POS-1 — links-based, mark-only).

Параллель к :mod:`visual_block_equivalence`, но для ТЕКСТОВЫХ/ТАБЛИЧНЫХ блоков.
Назначение — найти связанные пары текстовых блоков OLD↔NEW, чьё содержимое
ИДЕНТИЧНО (с точностью до форматного шума), чтобы в будущем (НЕ сейчас) такие
блоки можно было не гонять повторно через Opus enriched-MD сравнение.

Отличия от ``visual_block_equivalence`` и почему модуль отдельный:

  * скоуп — ТОЛЬКО text/table-блоки (image/graphic → ``skipped_non_text``);
  * сравнение текстовое (нормализованный текст), не визуальное;
  * источник текста блока — ``ocr_text`` из result.json. В проекте ПОС он
    HTML-обёрнут (``<div data-bbox=...>...</div>``), поэтому НЕОБХОДИМА
    нормализация: снять HTML-разметку, debug-префиксы ``BLOCK: <id>``,
    схлопнуть пробелы, lower, NFKC/ё→е. Без этого два идентичных по смыслу
    блока расходятся только из-за разных bbox-координат в разметке.

КОНСЕРВАТИВНОСТЬ (требование задачи):

  * ``identical_text`` — ТОЛЬКО при ТОЧНОМ равенстве нормализованного текста
    обеих сторон. Любое расхождение (включая изменившиеся числа/даты/объёмы/
    марки/ссылки на листы) → ``near_identical_text`` или ``changed_text``, но
    НЕ ``identical_text``. Точное равенство нормализованного текста гарантирует
    идентичность чисел, поэтому отдельного «числа не изменились» гейта для
    identical не требуется — он выполняется по построению.
  * ``near_identical_text`` НЕ исключается (mark-only флаг не выставляется).
  * исключающий флаг ``exclude_from_opus_md=true`` ставится ТОЛЬКО для
    ``identical_text``. ``exclude_from_qwen`` для текстовых блоков всегда
    ``False`` (Qwen описывает графику, не текст). ``enforced`` всегда ``False``.

ВАЖНО: модуль НЕ встроен в работающий pipeline. Он НЕ подключён к
``md_enrichment_jobs`` / enriched-MD / Opus и ничего в них не меняет. Его можно
вызвать из тестов/CLI/будущего endpoint; артефакт пишется в ОТДЕЛЬНЫЙ файл
``pairs/<pid>/text_block_equivalence/text_block_equivalence.json``.

Модуль fail-soft: ошибка нормализации/сравнения одной связи не валит batch.
"""
from __future__ import annotations

import difflib
import html
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from .block_equivalence_precheck import EqBlock, extract_blocks_for_equivalence
# Переиспользуем чистые link-хелперы из визуального слоя (без дублирования логики
# 1↔1 / stale). Это НЕ меняет visual_block_equivalence и его тесты.
from .visual_block_equivalence import _is_stale_link, _one_to_one_violations

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# env helpers (self-contained, без приватных импортов)
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

# Сравнённые связи
TS_IDENTICAL = "identical_text"
TS_NEAR = "near_identical_text"
TS_CHANGED = "changed_text"
TS_UNCERTAIN = "uncertain_text"

# НЕ сравнённые (skipped) связи
TS_SKIP_NO_TEXT = "skipped_no_text"
TS_SKIP_NON_TEXT = "skipped_non_text"
TS_SKIP_STALE = "skipped_stale_link"
TS_SKIP_NOT_1_1 = "skipped_not_one_to_one"
TS_SKIP_BLOCK_MISSING = "skipped_block_missing"

_COMPARED_STATUSES = {TS_IDENTICAL, TS_NEAR, TS_CHANGED, TS_UNCERTAIN}
_SKIPPED_STATUSES = {
    TS_SKIP_NO_TEXT, TS_SKIP_NON_TEXT, TS_SKIP_STALE, TS_SKIP_NOT_1_1, TS_SKIP_BLOCK_MISSING,
}

# Единственный статус, дающий потенциальное исключение из Opus (mark-only).
_EXCLUDE_STATUSES = {TS_IDENTICAL}


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class TextBlockEquivalenceConfig:
    """Параметры текстового прекчека по связям (env, безопасные дефолты)."""

    enabled: bool = False           # главный флаг (по умолчанию OFF); НЕ авто-включает pipeline
    mode: str = "mark_only"         # единственный режим
    enforced: bool = False          # всегда False — реального skip нет

    # минимальная длина нормализованного текста, чтобы блок считался «текстовым»
    min_chars: int = 3
    # порог char-similarity для near_identical_text (ниже → changed_text)
    near_threshold: float = 0.92
    # safety cap на число текстовых сравнений на пару
    max_links_compared: int = 5000

    @classmethod
    def from_env(cls) -> "TextBlockEquivalenceConfig":
        return cls(
            enabled=_env_flag("STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_ENABLED", False),
            min_chars=_env_int("STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_MIN_CHARS", 3),
            near_threshold=_env_float(
                "STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_NEAR_THRESHOLD", 0.92),
            max_links_compared=_env_int(
                "STAGE_COMPARISON_TEXT_BLOCK_EQUIVALENCE_MAX_LINKS_COMPARED", 5000),
        )


# ═══════════════════════════════════════════════════════════════════════════
# Нормализация текста
# ═══════════════════════════════════════════════════════════════════════════

# reserc.md #98: нормализация делегирована единому модулю text_norm — раньше эти
# функции дублировались тут и в нескольких других местах с расходящейся
# семантикой. strip_html / normalize_block_text сохранены как публичные имена
# (на них завязаны block_equivalence_precheck и тесты).
from .text_norm import (  # noqa: E402
    normalize_block_content as _tn_normalize_block_content,
    salient_numbers as _tn_salient_numbers,
    strip_html as strip_html,  # noqa: F401 — re-export
)


def normalize_block_text(text: Optional[str]) -> str:
    """Нормализация текста блока для сравнения эквивалентности (strip_html +
    снятие префикса ``BLOCK: <id>`` + NFKC/ё→е/collapse/lower). Делегирует
    единому :func:`text_norm.normalize_block_content` (reserc.md #98)."""
    return _tn_normalize_block_content(text)


def _salient_numbers(text: str) -> list[str]:
    """Значимые числовые токены (min_len=1, как было). См. text_norm #98."""
    return _tn_salient_numbers(text, min_len=1)


def _tokens(text: str) -> set[str]:
    return {t for t in text.split(" ") if t}


def _token_jaccard(a: str, b: str) -> float:
    ta, tb = _tokens(a), _tokens(b)
    if not ta and not tb:
        return 1.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return round(inter / union, 4) if union else 0.0


def _length_ratio(a: str, b: str) -> float:
    la, lb = len(a), len(b)
    if la == 0 and lb == 0:
        return 1.0
    hi = max(la, lb)
    return round(min(la, lb) / hi, 4) if hi else 0.0


def compute_text_metrics(norm_old: str, norm_new: str) -> dict:
    """Метрики сходства двух нормализованных текстов."""
    exact = bool(norm_old) and norm_old == norm_new
    char_ratio = round(difflib.SequenceMatcher(None, norm_old, norm_new).ratio(), 4)
    nums_old = _salient_numbers(norm_old)
    nums_new = _salient_numbers(norm_new)
    numbers_changed = sorted(set(nums_old)) != sorted(set(nums_new))
    return {
        "exact": exact,
        "char_ratio": char_ratio,
        "token_jaccard": _token_jaccard(norm_old, norm_new),
        "length_ratio": _length_ratio(norm_old, norm_new),
        "numbers_changed": numbers_changed,
        "numbers_old": nums_old[:30],
        "numbers_new": nums_new[:30],
        "chars_old": len(norm_old),
        "chars_new": len(norm_new),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Helpers (запись связи)
# ═══════════════════════════════════════════════════════════════════════════


def _link_pages(link: dict, old_block: Optional[EqBlock], new_block: Optional[EqBlock]):
    lp = link.get("left_page")
    rp = link.get("right_page")
    if lp is None and old_block is not None:
        lp = old_block.page
    if rp is None and new_block is not None:
        rp = new_block.page
    return lp, rp


def _empty_metrics() -> dict:
    return {
        "exact": None, "char_ratio": None, "token_jaccard": None, "length_ratio": None,
        "numbers_changed": None, "numbers_old": None, "numbers_new": None,
        "chars_old": None, "chars_new": None,
    }


def _exclusion_for(status: str) -> bool:
    return status in _EXCLUDE_STATUSES


def _base_record(link: dict, old_block: Optional[EqBlock], new_block: Optional[EqBlock],
                 *, status: str, reason: str, metrics: Optional[dict] = None,
                 confidence: Optional[float] = None,
                 norm_old_excerpt: Optional[str] = None,
                 norm_new_excerpt: Optional[str] = None) -> dict:
    lp, rp = _link_pages(link, old_block, new_block)
    exclude_opus = _exclusion_for(status)
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
        "exclude_from_qwen": False,          # текст не описывается Qwen
        "exclude_from_opus_md": exclude_opus,  # только identical_text
        "enforced": False,                   # всегда False — реального skip нет
        "debug": {
            "norm_old_excerpt": norm_old_excerpt,
            "norm_new_excerpt": norm_new_excerpt,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# compare_one_link_text_equivalence
# ═══════════════════════════════════════════════════════════════════════════


def compare_one_link_text_equivalence(
    link: dict,
    old_block: Optional[EqBlock],
    new_block: Optional[EqBlock],
    *,
    cfg: Optional[TextBlockEquivalenceConfig] = None,
    skip_status: Optional[str] = None,
    skip_reason: Optional[str] = None,
    normalize_fn: Optional[Callable[[Optional[str]], str]] = None,
) -> dict:
    """Сравнить ОДНУ связь текстовых блоков и вернуть структурированную запись.

    ``link`` — словарь связи из ``links.json``. ``old_block``/``new_block`` —
    ``EqBlock`` из result.json (или ``None``, если блок не найден).

    ``skip_status``/``skip_reason`` — если переданы (batch определил stale /
    не-1↔1), запись формируется без сравнения.

    ``normalize_fn`` инъектируется в тестах (default ``normalize_block_text``).

    Полностью fail-soft: исключение → ``uncertain_text``.
    """
    cfg = cfg or TextBlockEquivalenceConfig.from_env()
    normalize_fn = normalize_fn or normalize_block_text

    if skip_status:
        return _base_record(link, old_block, new_block,
                            status=skip_status, reason=skip_reason or skip_status)

    left_id = link.get("left_block_id")
    right_id = link.get("right_block_id")

    if not left_id or not right_id or old_block is None or new_block is None:
        missing = []
        if not left_id or old_block is None:
            missing.append(f"left:{left_id}")
        if not right_id or new_block is None:
            missing.append(f"right:{right_id}")
        return _base_record(link, old_block, new_block, status=TS_SKIP_BLOCK_MISSING,
                            reason=f"block not found in result.json ({', '.join(missing)})")

    # Скоуп — только text/table. image/graphic пропускаем.
    if not old_block.is_text_like or not new_block.is_text_like:
        return _base_record(link, old_block, new_block, status=TS_SKIP_NON_TEXT,
                            reason=(f"non-text block (old={old_block.block_type}, "
                                    f"new={new_block.block_type}); scope=text/table only"))

    try:
        norm_old = normalize_fn(old_block.text)
        norm_new = normalize_fn(new_block.text)
    except Exception as exc:  # noqa: BLE001 — fail-soft
        logger.debug("text_block_equivalence: normalize failed %s/%s: %s", left_id, right_id, exc)
        return _base_record(link, old_block, new_block, status=TS_UNCERTAIN,
                            reason=f"normalize exception: {type(exc).__name__}")

    has_old = len(norm_old) >= cfg.min_chars
    has_new = len(norm_new) >= cfg.min_chars
    metrics = compute_text_metrics(norm_old, norm_new)
    ex_old = norm_old[:240] or None
    ex_new = norm_new[:240] or None

    # оба пустые/слишком короткие → нечего сравнивать
    if not has_old and not has_new:
        return _base_record(link, old_block, new_block, status=TS_SKIP_NO_TEXT,
                            reason=f"both texts empty/short after normalization (<{cfg.min_chars} chars)",
                            metrics=metrics, norm_old_excerpt=ex_old, norm_new_excerpt=ex_new)

    # ровно одна сторона пустая → неоднозначно (markup-only vs реальное удаление)
    if has_old != has_new:
        return _base_record(link, old_block, new_block, status=TS_UNCERTAIN,
                            reason=("one side empty after normalization "
                                    f"(old={len(norm_old)}, new={len(norm_new)} chars)"),
                            metrics=metrics, confidence=metrics["char_ratio"],
                            norm_old_excerpt=ex_old, norm_new_excerpt=ex_new)

    # обе непустые
    if metrics["exact"]:
        status = TS_IDENTICAL
        reason = "normalized text exact match (excludable from Opus enriched-MD)"
    elif metrics["char_ratio"] >= cfg.near_threshold:
        status = TS_NEAR
        flag = " numbers_changed" if metrics["numbers_changed"] else ""
        reason = (f"near-identical (char_ratio={metrics['char_ratio']} ≥ "
                  f"{cfg.near_threshold}; not exact{flag}); NOT excluded")
    else:
        status = TS_CHANGED
        reason = (f"changed (char_ratio={metrics['char_ratio']} < {cfg.near_threshold}, "
                  f"jaccard={metrics['token_jaccard']})")

    return _base_record(link, old_block, new_block, status=status, reason=reason,
                        metrics=metrics, confidence=metrics["char_ratio"],
                        norm_old_excerpt=ex_old, norm_new_excerpt=ex_new)


# ═══════════════════════════════════════════════════════════════════════════
# Batch over links.json
# ═══════════════════════════════════════════════════════════════════════════


def _empty_summary() -> dict:
    return {
        "links_total": 0,
        "links_compared": 0,
        "identical_text": 0,
        "near_identical_text": 0,
        "changed_text": 0,
        "uncertain_text": 0,
        "skipped": 0,
        "skipped_breakdown": {
            "no_text": 0,
            "non_text": 0,
            "stale_link": 0,
            "not_one_to_one": 0,
            "block_missing": 0,
        },
        "potential_qwen_saved": 0,           # текст не Qwen-описывается → всегда 0
        "potential_opus_blocks_removed": 0,  # = identical_text
    }


_SKIP_SUMMARY_KEY = {
    TS_SKIP_NO_TEXT: "no_text",
    TS_SKIP_NON_TEXT: "non_text",
    TS_SKIP_STALE: "stale_link",
    TS_SKIP_NOT_1_1: "not_one_to_one",
    TS_SKIP_BLOCK_MISSING: "block_missing",
}


def _tally(summary: dict, record: dict) -> None:
    status = record.get("status")
    if status in _COMPARED_STATUSES:
        summary["links_compared"] += 1
        summary[status] = summary.get(status, 0) + 1
        if status == TS_IDENTICAL:
            summary["potential_opus_blocks_removed"] += 1
    elif status in _SKIPPED_STATUSES:
        summary["skipped"] += 1
        key = _SKIP_SUMMARY_KEY.get(status)
        if key:
            summary["skipped_breakdown"][key] += 1


def run_pair_text_block_equivalence(
    session_id: str,
    pair_id: str,
    *,
    cfg: Optional[TextBlockEquivalenceConfig] = None,
    links: Optional[list[dict]] = None,
    old_blocks: Optional[list[EqBlock]] = None,
    new_blocks: Optional[list[EqBlock]] = None,
    write_artifact: bool = True,
    generated_at: Optional[str] = None,
    normalize_fn: Optional[Callable[[Optional[str]], str]] = None,
) -> dict:
    """Прогнать текстовый прекчек по ВСЕМ явным связям пары и собрать артефакт.

    ``left`` = OLD (старая стадия), ``right`` = NEW (новая стадия).

    Источники по умолчанию (если не переданы) резолвятся из store/paths:
      * ``links`` ← ``store._pair_links``;
      * ``old_blocks``/``new_blocks`` ← ``extract_blocks_for_equivalence`` по
        result.json из ``pair.json``.

    mark-only: статусы/метрики пишутся, реального skip нет. Возвращает отчёт
    (dict). Fail-soft на уровне каждой связи.
    """
    cfg = cfg or TextBlockEquivalenceConfig.from_env()
    generated_at = generated_at or _utc_now_iso()

    if links is None or old_blocks is None or new_blocks is None or write_artifact:
        try:
            from . import store as store_mod
        except Exception:  # noqa: BLE001
            store_mod = None  # type: ignore

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
            skip_status = TS_SKIP_STALE
            skip_reason = f"stale link (method={link.get('method')})"
        elif idx in violations:
            skip_status = TS_SKIP_NOT_1_1
            skip_reason = "block participates in 1↔many / many↔1 (not one-to-one)"

        will_compare = (
            skip_status is None
            and old_block is not None and new_block is not None
            and old_block.is_text_like and new_block.is_text_like
        )
        if will_compare and compares_done >= cfg.max_links_compared:
            warnings.append("max_links_compared_reached")
            rec = _base_record(link, old_block, new_block, status=TS_UNCERTAIN,
                              reason="max_links_compared cap reached")
            pairs_out.append(rec)
            _tally(summary, rec)
            continue

        rec = compare_one_link_text_equivalence(
            link, old_block, new_block,
            cfg=cfg, skip_status=skip_status, skip_reason=skip_reason,
            normalize_fn=normalize_fn,
        )
        if rec.get("status") in _COMPARED_STATUSES:
            compares_done += 1
        pairs_out.append(rec)
        _tally(summary, rec)

    report = {
        "schema_version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "mode": cfg.mode,
        "source": "links_json",
        "text_source": "result_json_ocr_text_html_stripped",
        "enabled": cfg.enabled,
        "enforced": False,                 # реального skip нет
        "thresholds": {
            "min_chars": cfg.min_chars,
            "near_threshold": cfg.near_threshold,
        },
        "summary": summary,
        "pairs": pairs_out,
        "warnings": warnings,
        "compares_done": compares_done,
    }

    if write_artifact:
        try:
            from . import paths as paths_mod
            out = paths_mod.text_block_equivalence_report_path(session_id, pair_id)
            _atomic_write_json(out, report)
        except Exception as exc:  # noqa: BLE001 — artifact write non-fatal
            logger.warning("text_block_equivalence: report write failed %s/%s: %s",
                           session_id, pair_id, exc)

    return report


def read_pair_text_block_equivalence(session_id: str, pair_id: str) -> Optional[dict]:
    """Прочитать сохранённый артефакт пары (если есть). Для будущего endpoint/UI."""
    try:
        import json
        from . import paths as paths_mod
        p = paths_mod.text_block_equivalence_report_path(session_id, pair_id)
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
    """Атомарная запись JSON (tmp → os.replace) — паттерн проекта."""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


__all__ = [
    "TextBlockEquivalenceConfig",
    "strip_html",
    "normalize_block_text",
    "compute_text_metrics",
    "compare_one_link_text_equivalence",
    "run_pair_text_block_equivalence",
    "read_pair_text_block_equivalence",
    # status constants
    "TS_IDENTICAL",
    "TS_NEAR",
    "TS_CHANGED",
    "TS_UNCERTAIN",
    "TS_SKIP_NO_TEXT",
    "TS_SKIP_NON_TEXT",
    "TS_SKIP_STALE",
    "TS_SKIP_NOT_1_1",
    "TS_SKIP_BLOCK_MISSING",
]
