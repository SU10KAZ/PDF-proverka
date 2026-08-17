"""Unified Stage Comparison Analysis: Opus-сравнение по готовым enriched MD.

Локальное распознавание графики (Qwen через LM Studio) с платформы удалено,
поэтому здесь остался единственный этап: `enriched_comparison` (Claude Opus
через Claude Code subscription) поверх УЖЕ подготовленных enriched MD.

Если enriched MD для пары нет, анализ честно завершается статусом
`enriched_md_missing` — новых описаний графики платформа не производит.

Никакой автоматический live run без подтверждения. Здесь — детерминированные
функции preflight/run, остальное берёт на себя unified_analysis_jobs.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from . import analysis_profile as analysis_profile_mod
from . import enriched_comparison as enriched_mod
from . import md_image_enrichment as md_enrich_mod
from . import paths as paths_mod
from . import store as store_mod
from .text_llm_provider import ClaudeCodeProvider

logger = logging.getLogger(__name__)


# ─── Preflight ───────────────────────────────────────────────────────────


@dataclass
class PairPreflight:
    """Готовность пары к unified-анализу. Чисто read-only расчёт."""

    pair_id: str
    pair_label: str
    has_md: bool = False
    md_left_path: Optional[str] = None
    md_right_path: Optional[str] = None

    image_blocks_source: str = "cache"              # cache | parsed_md | none

    # Qwen enrichment
    enrichment_ready: bool = False                  # left+right enriched MD existуют
    enrichment_left_status: str = "not_run"
    enrichment_right_status: str = "not_run"
    image_blocks_left: int = 0
    image_blocks_right: int = 0
    cache_hits_left: int = 0
    cache_hits_right: int = 0
    qwen_calls_estimated: int = 0                   # сколько image-блоков ещё не покрыто cache
    qwen_provider_available: bool = False
    qwen_provider_reason: Optional[str] = None
    # Replacement-format metadata.
    enriched_md_format_version_left: str = "unknown"
    enriched_md_format_version_right: str = "unknown"
    enriched_md_outdated_format: bool = False
    needs_rebuild: bool = False

    # Opus comparison
    comparison_ready: bool = False                  # есть comparison_result.json со status=done
    comparison_status: str = "not_run"
    comparison_changes_count: int = 0
    opus_provider_available: bool = False
    opus_provider_reason: Optional[str] = None
    opus_enabled: bool = False
    enriched_total_chars: int = 0
    enriched_limit_chars: int = 0
    too_large: bool = False

    will_run_enrichment: bool = False
    will_run_comparison: bool = False
    can_run: bool = False
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    estimated_duration_sec: int = 0

    def as_dict(self) -> dict:
        return {
            "pair_id": self.pair_id,
            "pair_label": self.pair_label,
            "has_md": self.has_md,
            "md_left_path": self.md_left_path,
            "md_right_path": self.md_right_path,
            "image_blocks_source": self.image_blocks_source,
            "enrichment": {
                "ready": self.enrichment_ready,
                "left_status": self.enrichment_left_status,
                "right_status": self.enrichment_right_status,
                "image_blocks_left": self.image_blocks_left,
                "image_blocks_right": self.image_blocks_right,
                "cache_hits_left": self.cache_hits_left,
                "cache_hits_right": self.cache_hits_right,
                "qwen_calls_estimated": self.qwen_calls_estimated,
                "provider_available": self.qwen_provider_available,
                "provider_reason": self.qwen_provider_reason,
                "format_version_left": self.enriched_md_format_version_left,
                "format_version_right": self.enriched_md_format_version_right,
                "outdated_format": self.enriched_md_outdated_format,
                "needs_rebuild": self.needs_rebuild,
            },
            "comparison": {
                "ready": self.comparison_ready,
                "status": self.comparison_status,
                "changes_count": self.comparison_changes_count,
                "provider_available": self.opus_provider_available,
                "provider_reason": self.opus_provider_reason,
                "enabled": self.opus_enabled,
                "enriched_total_chars": self.enriched_total_chars,
                "enriched_limit_chars": self.enriched_limit_chars,
                "too_large": self.too_large,
            },
            "will_run_enrichment": self.will_run_enrichment,
            "will_run_comparison": self.will_run_comparison,
            "can_run": self.can_run,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "estimated_duration_sec": self.estimated_duration_sec,
        }


def _pair_label(pair: dict) -> str:
    left = (pair or {}).get("left") or {}
    right = (pair or {}).get("right") or {}
    return f"{left.get('filename') or '—'} ↔ {right.get('filename') or '—'}"


def _md_present(pair: dict) -> tuple[bool, Optional[str], Optional[str]]:
    left_md = (pair.get("left") or {}).get("md_path")
    right_md = (pair.get("right") or {}).get("md_path")
    return bool(left_md) and bool(right_md), left_md, right_md


def _qwen_estimate_duration_sec(calls: int) -> int:
    """Очень грубая оценка: ~10 секунд на блок при 100 DPI на Qwen 35B."""
    return max(0, int(calls) * 10)


def _opus_estimate_duration_sec(total_chars: int) -> int:
    """Грубая оценка длительности enriched_comparison: ~30 сек + 1с на 5000 chars."""
    base = 30
    per_5k = max(0, int(total_chars)) // 5000
    return base + per_5k


def _count_image_blocks_from_md(md_path: Optional[str]) -> int:
    """Read MD и посчитать image/imagine-блоки (Task 9 fallback).

    Используется когда image_descriptions.json ещё не существует — это
    безопасный dry-run без вызова Qwen.
    """
    if not md_path:
        return 0
    try:
        from pathlib import Path
        txt = Path(md_path).read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeError):
        return 0
    if not txt:
        return 0
    try:
        blocks = md_enrich_mod.parse_md_blocks(txt)
    except Exception:  # noqa: BLE001
        return 0
    return sum(1 for b in blocks if getattr(b, "is_image", False))


def preflight_pair(
    session_id: str,
    pair_id: str,
    *,
    force_enrichment: bool = False,
    force_compare: bool = False,
) -> PairPreflight:
    """Собрать readiness-метрики для одной пары без запуска моделей."""
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        raise KeyError("pair_not_found")

    out = PairPreflight(pair_id=pair_id, pair_label=_pair_label(pair))
    has_md, lmd, rmd = _md_present(pair)
    out.has_md = has_md
    out.md_left_path = lmd
    out.md_right_path = rmd

    if not has_md:
        out.errors.append("Markdown отсутствует на одной из сторон пары.")
        return out

    # --- Qwen enrichment readiness
    # Cache-первый источник: read_summary_only из image_descriptions.json.
    left_sum = md_enrich_mod.read_summary_only(session_id, pair_id, "left")
    right_sum = md_enrich_mod.read_summary_only(session_id, pair_id, "right")
    out.enrichment_left_status = left_sum.get("status") or "not_run"
    out.enrichment_right_status = right_sum.get("status") or "not_run"
    cached_left  = int(left_sum.get("image_blocks") or 0)
    cached_right = int(right_sum.get("image_blocks") or 0)
    out.image_blocks_left = cached_left
    out.image_blocks_right = cached_right
    out.cache_hits_left = int(left_sum.get("from_cache") or 0)
    out.cache_hits_right = int(right_sum.get("from_cache") or 0)

    # Task 9: если ни одна сторона ещё не enrichment'илась, image_descriptions.json
    # не существует, и кеш-метод возвращает 0. В этом случае распарсим MD
    # напрямую — это безопасный dry-run без вызова Qwen.
    if cached_left == 0 and cached_right == 0:
        parsed_left = _count_image_blocks_from_md(lmd)
        parsed_right = _count_image_blocks_from_md(rmd)
        if parsed_left or parsed_right:
            out.image_blocks_left = parsed_left
            out.image_blocks_right = parsed_right
            out.image_blocks_source = "parsed_md"
        else:
            out.image_blocks_source = "none"

    enriched_status = enriched_mod.enriched_md_status(session_id, pair_id)
    out.enrichment_ready = bool(enriched_status.get("ready"))
    out.enriched_md_format_version_left = str((enriched_status.get("left") or {}).get("format_version") or "unknown")
    out.enriched_md_format_version_right = str((enriched_status.get("right") or {}).get("format_version") or "unknown")
    out.enriched_md_outdated_format = bool(enriched_status.get("outdated_format"))
    # Rebuild требуется, если enriched.md уже есть, но формат не replacement.
    if out.enrichment_ready and out.enriched_md_outdated_format:
        out.needs_rebuild = True
        out.warnings.append(
            "Enriched MD в устаревшем формате (append_v0). Требуется пересборка в "
            "replace_image_blocks_v1 (без повторного Qwen). При запуске unified-analysis "
            "пересборка произойдёт автоматически перед Opus."
        )

    # Сколько вызовов Qwen ещё нужно: всего image-блоков − уже покрытых cache.
    # Если force_enrichment=true — считаем все блоки заново.
    if force_enrichment:
        out.qwen_calls_estimated = out.image_blocks_left + out.image_blocks_right
    else:
        miss_left = max(0, out.image_blocks_left - out.cache_hits_left)
        miss_right = max(0, out.image_blocks_right - out.cache_hits_right)
        out.qwen_calls_estimated = miss_left + miss_right

    # Будем ли реально запускать enrichment.
    need_enrichment = force_enrichment or not out.enrichment_ready
    out.will_run_enrichment = need_enrichment

    # Распознавание графики локальной VLM удалено с платформы: enrichment
    # больше не запускается, работаем только по готовым enriched MD.
    if need_enrichment:
        out.qwen_provider_available = False
        out.qwen_provider_reason = "локальные LLM-мощности удалены с платформы"
        out.warnings.append(
            "Enriched MD отсутствует, а распознавание графики удалено с платформы — "
            "сравнение возможно только по уже подготовленным enriched MD."
        )

    # --- Opus comparison readiness
    cfg = enriched_mod.load_config()
    out.opus_enabled = bool(cfg.enabled)
    out.enriched_limit_chars = int(cfg.max_chars or 0)
    out.enriched_total_chars = int(enriched_status.get("total_chars") or 0)
    if cfg.max_chars > 0 and out.enriched_total_chars > cfg.max_chars:
        out.too_large = True
        out.warnings.append(
            f"Суммарный enriched MD ({out.enriched_total_chars}) превышает лимит ({cfg.max_chars})."
        )

    existing = enriched_mod.get_comparison_result(session_id, pair_id)
    if existing:
        out.comparison_status = str(existing.get("status") or "not_run")
        out.comparison_changes_count = len(existing.get("changes") or [])
    out.comparison_ready = bool(existing and existing.get("status") == "done")

    need_comparison = force_compare or not out.comparison_ready
    # Если enriched MD ещё нет — после enrichment попробуем сравнить.
    if need_comparison:
        out.will_run_comparison = bool(need_enrichment or out.enrichment_ready)

    if out.will_run_comparison and cfg.enabled:
        prov = ClaudeCodeProvider()
        ok, reason = prov.check_availability()
        out.opus_provider_available = bool(ok)
        out.opus_provider_reason = reason
        if not ok:
            out.warnings.append(
                f"Claude Code provider недоступен ({reason or 'unknown'}). "
                "Prompt будет сохранён для ручного запуска."
            )
    elif out.will_run_comparison and not cfg.enabled:
        out.warnings.append(
            "Enriched comparison выключен в env (STAGE_COMPARISON_ENRICHED_COMPARE_ENABLED!=true)."
        )

    # Can run: либо есть смысл запускать enrichment, либо есть смысл запускать
    # сравнение (или и то и другое).
    out.can_run = bool(out.will_run_enrichment or out.will_run_comparison)
    out.estimated_duration_sec = (
        _qwen_estimate_duration_sec(out.qwen_calls_estimated)
        + (_opus_estimate_duration_sec(out.enriched_total_chars) if out.will_run_comparison else 0)
    )
    return out


def preflight_session(
    session_id: str,
    *,
    pair_ids: Optional[list[str]] = None,
    force_enrichment: bool = False,
    force_compare: bool = False,
) -> dict:
    """Сводка по выбранным парам сессии (для batch UI)."""
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    all_pairs = [p for p in (session.get("pairs") or []) if p.get("status") != "disabled" and p.get("id")]
    valid_ids = {p["id"] for p in all_pairs}

    if pair_ids:
        target_ids = [pid for pid in pair_ids if pid in valid_ids]
    else:
        target_ids = [p["id"] for p in all_pairs]

    items: list[dict] = []
    total_qwen = 0
    total_opus = 0
    total_seconds = 0
    runnable_pairs = 0
    skipped_pairs = 0
    for pid in target_ids:
        try:
            pre = preflight_pair(
                session_id, pid,
                force_enrichment=force_enrichment,
                force_compare=force_compare,
            )
        except KeyError:
            skipped_pairs += 1
            continue
        items.append(pre.as_dict())
        if pre.can_run:
            runnable_pairs += 1
        else:
            skipped_pairs += 1
        total_qwen += pre.qwen_calls_estimated
        if pre.will_run_comparison:
            total_opus += 1
        total_seconds += pre.estimated_duration_sec

    return {
        "session_id": session_id,
        "total_pairs": len(target_ids),
        "runnable_pairs": runnable_pairs,
        "skipped_pairs": skipped_pairs,
        "qwen_calls_total": total_qwen,
        "opus_calls_total": total_opus,
        "estimated_duration_sec": total_seconds,
        "items": items,
    }


# ─── Pair runner ─────────────────────────────────────────────────────────


@dataclass
class PairRunResult:
    """Результат одного полного запуска (enrichment + comparison) пары."""

    pair_id: str
    status: str = "queued"                 # queued|enriching|comparing|done|failed|skipped
    enrichment_status: str = "not_run"     # not_run|done|partial|error|skipped
    comparison_status: str = "not_run"
    changes_count: int = 0
    enriched_paths: dict = field(default_factory=dict)
    comparison_result_path: Optional[str] = None
    warnings: list[str] = field(default_factory=list)
    error: Optional[str] = None
    duration_sec: float = 0.0

    def as_dict(self) -> dict:
        return {
            "pair_id": self.pair_id,
            "status": self.status,
            "enrichment_status": self.enrichment_status,
            "comparison_status": self.comparison_status,
            "changes_count": self.changes_count,
            "enriched_paths": dict(self.enriched_paths),
            "comparison_result_path": self.comparison_result_path,
            "warnings": list(self.warnings),
            "error": self.error,
            "duration_sec": round(self.duration_sec, 3),
        }


async def run_pair(
    session_id: str,
    pair_id: str,
    *,
    force_enrichment: bool = False,
    force_compare: bool = False,
    force_fallback: bool = False,
    analysis_profile: Optional[str] = None,
    allow_profile_downgrade: bool = False,
    progress_cb: Optional[Any] = None,
) -> "PairRunResult":
    """Тонкая обёртка: выставляет per-run профиль анализа (override без правки
    .env) на ВЕСЬ прогон пары — и enrichment (Qwen), и comparison (Opus).
    contextvars копируются в asyncio.to_thread, поэтому override виден и
    blocking-сравнению. None → env-профиль (массовый default-прогон).

    `analysis_profile="rich_grsh"` + `force_enrichment=True` = эталонный
    глубокий прогон одной пары без глобального включения флагов.
    """
    with analysis_profile_mod.profile_override_for(analysis_profile):
        return await _run_pair_impl(
            session_id, pair_id,
            force_enrichment=force_enrichment, force_compare=force_compare,
            force_fallback=force_fallback,
            allow_profile_downgrade=allow_profile_downgrade,
            progress_cb=progress_cb,
        )


async def _run_pair_impl(
    session_id: str,
    pair_id: str,
    *,
    force_enrichment: bool = False,
    force_compare: bool = False,
    force_fallback: bool = False,
    allow_profile_downgrade: bool = False,
    progress_cb: Optional[Any] = None,
) -> PairRunResult:
    """Цепочка enrichment (если нужно) + enriched_comparison для одной пары.

    Контракт:
      - Если MD одной из сторон нет → status="failed", enrichment не запускается.
      - Если enrichment уже сделан и force_enrichment=False → skip enrichment.
      - Если enriched MD одной из сторон отсутствует и enrichment упал —
        не запускаем comparison, статус failed.
      - Если enriched MD есть (даже после partial enrichment) — comparison
        запускается; status зависит от Opus.
      - Если сравнение выключено / provider unavailable — статус comparing
        отражает причину, общий status = done (но changes_count=0).
    """
    import time as _time

    started = _time.monotonic()
    res = PairRunResult(pair_id=pair_id)
    session = store_mod.get_session(session_id)
    if session is None:
        res.status = "failed"
        res.error = "session_not_found"
        return res
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        res.status = "failed"
        res.error = "pair_not_found"
        return res

    has_md, lmd, rmd = _md_present(pair)
    if not has_md:
        res.status = "failed"
        res.error = "missing_md"
        res.warnings.append("Markdown отсутствует на одной из сторон.")
        return res

    # --- Phase 1: enrichment УДАЛЁН вместе с локальными LLM-мощностями.
    # Работаем только по готовым enriched MD; новых описаний графики нет.
    res.enrichment_status = "skipped"

    # --- Phase 2: enriched comparison
    enriched_status = enriched_mod.enriched_md_status(session_id, pair_id)
    if not enriched_status.get("ready"):
        # После enrichment всё равно нет enriched MD на обеих сторонах.
        res.status = "failed"
        res.error = "enriched_md_missing"
        res.comparison_status = "not_ready"
        res.warnings.append(
            "Enriched MD одной из сторон отсутствует. Распознавание графики "
            "удалено с платформы — подготовить enriched MD заново нечем."
        )
        res.duration_sec = _time.monotonic() - started
        return res

    # Auto-rebuild outdated enriched.md в replacement format (офлайн, по сохранённым описаниям).
    if enriched_status.get("outdated_format"):
        outdated_sides = enriched_status.get("outdated_sides") or []
        for _side in outdated_sides:
            try:
                rebuild_info = md_enrich_mod.rebuild_enriched_md_from_descriptions(
                    session_id, pair_id, _side,
                )
                if rebuild_info.get("status") == "rebuilt":
                    res.warnings.append(
                        f"{_side}_enriched.md auto-rebuilt в формат "
                        f"replace_image_blocks_v1 (replaced "
                        f"{rebuild_info.get('replaced_image_blocks')}/"
                        f"{rebuild_info.get('original_image_blocks')} image blocks)."
                    )
                else:
                    res.warnings.append(
                        f"{_side}_enriched.md rebuild failed: {rebuild_info.get('status')}"
                    )
            except Exception as exc:  # noqa: BLE001
                logger.exception("unified_analysis: rebuild %s enriched failed", _side)
                res.warnings.append(f"{_side}_enriched_rebuild_exception:{type(exc).__name__}:{exc}")
        # обновляем enriched_status уже из новых файлов (для actual chars)
        enriched_status = enriched_mod.enriched_md_status(session_id, pair_id)

    res.status = "comparing"
    if progress_cb:
        try:
            progress_cb(res)
        except Exception:  # noqa: BLE001
            pass

    # run_enriched_comparison делает blocking subprocess → выносим в to_thread.
    try:
        comp = await asyncio.to_thread(
            enriched_mod.run_enriched_comparison,
            session_id, pair_id, force=bool(force_compare),
            force_fallback=bool(force_fallback),
            allow_profile_downgrade=bool(allow_profile_downgrade),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("unified_analysis: enriched_comparison failed")
        res.status = "failed"
        res.error = f"enriched_comparison_exception:{type(exc).__name__}"
        res.warnings.append(str(exc)[:300])
        res.duration_sec = _time.monotonic() - started
        return res

    res.comparison_status = str(comp.get("status") or "unknown")
    res.changes_count = len(comp.get("changes") or [])
    res.comparison_result_path = str(
        paths_mod.enriched_comparison_result_path(session_id, pair_id)
    )

    # Финальный pair status:
    #   done   — comparison.status == done (changes найдены или нет — неважно).
    #   skipped — provider_not_available / disabled / too_large / not_ready.
    #   failed  — error / invalid_json / timeout.
    if comp.get("status") == "done":
        res.status = "done"
    elif comp.get("status") in ("provider_not_available", "disabled", "too_large", "not_ready"):
        res.status = "skipped"
        res.error = comp.get("error")
    else:
        res.status = "failed"
        res.error = comp.get("error") or str(comp.get("status"))

    res.duration_sec = _time.monotonic() - started
    return res


__all__ = [
    "PairPreflight",
    "PairRunResult",
    "preflight_pair",
    "preflight_session",
    "run_pair",
]
