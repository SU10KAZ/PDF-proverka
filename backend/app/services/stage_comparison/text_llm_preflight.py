"""Pre-run оценка стоимости/времени для семантического LLM-анализа текста.

Перед фактическим запуском `claude -p --model sonnet` UI должен показать
пользователю размер MD, ориентировочное время и стоимость. Этот модуль
считает оценки и применяет soft/hard лимиты, чтобы случайный клик не запустил
длительный платный батч.

Эвристика опирается на один точный замер (см. отчёт live-теста 2026-05-23):

    total_chars = 50 543
    duration_sec ≈ 481.5
    cost_usd ≈ 0.61

Применяем линейное масштабирование + множитель запаса 1.2 + минимальные
пороги, чтобы оценка не была занижена для крошечных MD. Это **не биллинг** —
фактический cost берётся из stdout Claude после реального запуска.

Никаких сетевых вызовов. Никаких subprocess'ов. Только синхронные локальные
расчёты + чтение размеров MD-файлов.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Optional

from . import paths as paths_mod
from . import store as store_mod
from . import text_llm as text_llm_mod
from .text_llm_input import prepare_text_only_markdown
from .text_llm_provider import ProviderConfig, load_config, resolve_provider


# ─── Baseline ───────────────────────────────────────────────────────────

# Один реальный замер; см. описание в module docstring. Менять только при
# повторном калибровочном прогоне.
REFERENCE_CHARS = 50_543
REFERENCE_DURATION_SEC = 481.5
REFERENCE_COST_USD = 0.61
SAFETY_FACTOR = 1.2

# Минимумы: даже самый маленький диф через Claude будет иметь стартовый
# оверхед на init и API round-trip, так что нечего показывать оценку 5с/$0.01.
MIN_DURATION_SEC = 60
MIN_COST_USD = 0.05


# ─── Env limits ─────────────────────────────────────────────────────────


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def warn_cost_usd() -> float:
    return _env_float("STAGE_COMPARISON_TEXT_LLM_WARN_COST_USD", 3.0)


def warn_duration_sec() -> float:
    return _env_float("STAGE_COMPARISON_TEXT_LLM_WARN_DURATION_SEC", 1800.0)


def hard_cost_usd() -> float:
    return _env_float("STAGE_COMPARISON_TEXT_LLM_HARD_COST_USD", 10.0)


def hard_duration_sec() -> float:
    return _env_float("STAGE_COMPARISON_TEXT_LLM_HARD_DURATION_SEC", 7200.0)


# ─── Core calculations ──────────────────────────────────────────────────


def estimate_duration_sec(total_chars: int) -> int:
    """Грубая верхняя оценка длительности `claude -p` для пары MD-файлов."""
    if total_chars <= 0:
        return MIN_DURATION_SEC
    raw = total_chars / REFERENCE_CHARS * REFERENCE_DURATION_SEC * SAFETY_FACTOR
    return max(MIN_DURATION_SEC, int(math.ceil(raw)))


def estimate_cost_usd(total_chars: int) -> float:
    """Грубая верхняя оценка стоимости одного запуска."""
    if total_chars <= 0:
        return MIN_COST_USD
    raw = total_chars / REFERENCE_CHARS * REFERENCE_COST_USD * SAFETY_FACTOR
    rounded = round(raw, 2)
    return max(MIN_COST_USD, rounded)


def _read_chars(path: str | None) -> tuple[int, bool]:
    """Размер MD-файла в символах. Возвращает (chars, exists)."""
    if not path:
        return 0, False
    p = Path(path)
    if not p.exists():
        return 0, False
    try:
        text = p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return 0, True
    return len(text), True


def _read_and_filter(path: str | None) -> tuple[dict, bool]:
    """Прочитать MD и применить prepare_text_only_markdown.

    Возвращает (stats, exists), где stats — словарь
    {original_chars, filtered_chars, removed_image_blocks, removed_image_chars}.
    Для отсутствующего файла — нули и exists=False.
    """
    empty = {
        "original_chars": 0, "filtered_chars": 0,
        "removed_image_blocks": 0, "removed_image_chars": 0,
    }
    if not path:
        return empty, False
    p = Path(path)
    if not p.exists():
        return empty, False
    try:
        text = p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return empty, True
    prep = prepare_text_only_markdown(text)
    return dict(prep["stats"]), True


# ─── Public API ─────────────────────────────────────────────────────────


def estimate_pair(session_id: str, pair_id: str, *, config: Optional[ProviderConfig] = None) -> dict:
    """Posсчитать preflight для одной пары.

    Возвращает dict со всеми полями, ожидаемыми UI; не бросает исключений
    (кроме session_not_found / pair_not_found).
    """
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        raise KeyError("pair_not_found")

    cfg = config or load_config()
    # provider может быть None даже при enabled=True, если provider unknown.
    # В тестах config передаётся явно, но мы всё равно резолвим провайдера, чтобы
    # проверить доступность бинаря (claude CLI).
    provider, _cfg = resolve_provider(cfg)
    provider_enabled = bool(cfg.enabled)
    provider_available = False
    provider_reason: Optional[str] = None
    if not provider_enabled:
        provider_reason = "disabled_via_env"
    elif provider is None:
        provider_reason = f"unknown_provider:{cfg.provider}"
    else:
        ok, reason = provider.check_availability()
        provider_available = bool(ok)
        provider_reason = reason

    left_md_path = (pair.get("left") or {}).get("md_path")
    right_md_path = (pair.get("right") or {}).get("md_path")
    left_stats, has_left = _read_and_filter(left_md_path)
    right_stats, has_right = _read_and_filter(right_md_path)

    # Оригинал — что лежит в MD-файле; filtered — что фактически уйдёт в LLM
    # после удаления image/imagine-блоков. Лимит и оценка стоимости/времени
    # считаются по filtered (а не по оригиналу), потому что графика не
    # потребляет токены текстовой модели.
    left_original_chars = int(left_stats["original_chars"])
    right_original_chars = int(right_stats["original_chars"])
    left_filtered_chars = int(left_stats["filtered_chars"])
    right_filtered_chars = int(right_stats["filtered_chars"])
    total_original_chars = left_original_chars + right_original_chars
    total_filtered_chars = left_filtered_chars + right_filtered_chars

    removed_image_blocks_left = int(left_stats["removed_image_blocks"])
    removed_image_blocks_right = int(right_stats["removed_image_blocks"])
    removed_image_chars_left = int(left_stats["removed_image_chars"])
    removed_image_chars_right = int(right_stats["removed_image_chars"])

    # legacy-поля (UI и старые тесты ожидают это имя): отражают filtered
    left_chars = left_filtered_chars
    right_chars = right_filtered_chars
    total_chars = total_filtered_chars

    max_chars = int(cfg.max_chars) if cfg.max_chars else 0
    within_limit = bool(total_chars > 0 and (max_chars <= 0 or total_chars <= max_chars))

    est_dur = estimate_duration_sec(total_chars)
    est_cost = estimate_cost_usd(total_chars)

    warnings: list[str] = []
    blocking: list[str] = []
    if not has_left or not has_right:
        warnings.append("missing_md")
        blocking.append("missing_md")
    if not provider_enabled:
        warnings.append("disabled")
        blocking.append("disabled")
    elif not provider_available:
        warnings.append("provider_unavailable")
        blocking.append("provider_unavailable")
    if max_chars > 0 and total_chars > max_chars:
        warnings.append("too_large")
        blocking.append("too_large")
    if has_left and has_right and total_filtered_chars > 0 and total_filtered_chars < 1000:
        warnings.append("filtered_text_too_short")

    # soft/hard cost limits
    w_cost, h_cost = warn_cost_usd(), hard_cost_usd()
    w_dur, h_dur = warn_duration_sec(), hard_duration_sec()
    cost_warning = est_cost > w_cost
    cost_hard = est_cost > h_cost
    dur_warning = est_dur > w_dur
    dur_hard = est_dur > h_dur
    if cost_warning:
        warnings.append("cost_above_warn")
    if dur_warning:
        warnings.append("duration_above_warn")
    if cost_hard:
        warnings.append("cost_above_hard")
    if dur_hard:
        warnings.append("duration_above_hard")

    # Уже посчитанный ранее результат
    existing = text_llm_mod.get_text_llm_diff(session_id, pair_id)
    has_cached_result = bool(existing and existing.get("status") == "done")
    cached_status = (existing or {}).get("status") if existing else None

    # Финальный статус пары для batch-агрегации
    if "missing_md" in blocking:
        item_status = "missing_md"
    elif "too_large" in blocking:
        item_status = "too_large"
    elif "disabled" in blocking or "provider_unavailable" in blocking:
        item_status = "provider_unavailable"
    elif has_cached_result:
        item_status = "cached"
    else:
        item_status = "ready"

    return {
        "ok": True,
        "pair_id": pair_id,
        "left_md_path": left_md_path,
        "right_md_path": right_md_path,
        "left_pdf_name": (pair.get("left") or {}).get("filename"),
        "right_pdf_name": (pair.get("right") or {}).get("filename"),
        "has_left_md": has_left,
        "has_right_md": has_right,
        # Legacy / совместимость: количество символов, реально отправляемых в LLM
        "left_chars": left_chars,
        "right_chars": right_chars,
        "total_chars": total_chars,
        # Полная статистика после фильтрации image/imagine-блоков
        "left_original_chars": left_original_chars,
        "right_original_chars": right_original_chars,
        "total_original_chars": total_original_chars,
        "left_filtered_chars": left_filtered_chars,
        "right_filtered_chars": right_filtered_chars,
        "total_filtered_chars": total_filtered_chars,
        "removed_image_blocks_left": removed_image_blocks_left,
        "removed_image_blocks_right": removed_image_blocks_right,
        "removed_image_blocks_total": removed_image_blocks_left + removed_image_blocks_right,
        "removed_image_chars_left": removed_image_chars_left,
        "removed_image_chars_right": removed_image_chars_right,
        "removed_image_chars_total": removed_image_chars_left + removed_image_chars_right,
        "max_chars": max_chars,
        "within_limit": within_limit,
        "provider_enabled": provider_enabled,
        "provider_available": provider_available,
        "provider": cfg.provider,
        "model": cfg.model,
        "provider_reason": provider_reason,
        "estimated_duration_sec": est_dur,
        "estimated_cost_usd": est_cost,
        "warnings": warnings,
        "blocking": blocking,
        "has_cached_result": has_cached_result,
        "cached_status": cached_status,
        "status": item_status,
        "limits": {
            "warn_cost_usd": w_cost,
            "warn_duration_sec": w_dur,
            "hard_cost_usd": h_cost,
            "hard_duration_sec": h_dur,
            "cost_warning": cost_warning,
            "cost_hard": cost_hard,
            "duration_warning": dur_warning,
            "duration_hard": dur_hard,
        },
    }


def estimate_session(
    session_id: str,
    *,
    scope: str = "session",
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    force: bool = False,
    config: Optional[ProviderConfig] = None,
) -> dict:
    """Агрегированный preflight для нескольких пар.

    `scope`:
      • "pair"     — одна пара по pair_id
      • "selected" — список pair_ids
      • "session"  — все pairs сессии

    `force=False` исключает уже выполненные (status=done) из runnable, помечая
    их `cached/skipped`. `force=True` включает их в runnable.
    """
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    cfg = config or load_config()
    all_pairs = [p for p in (session.get("pairs") or [])
                 if p.get("status") != "disabled" and p.get("id")]
    valid_ids = {p["id"] for p in all_pairs}

    if scope == "pair":
        target_ids = [pair_id] if (pair_id and pair_id in valid_ids) else []
    elif scope == "selected":
        target_ids = [pid for pid in (pair_ids or []) if pid in valid_ids]
    elif scope == "session":
        target_ids = [p["id"] for p in all_pairs]
    else:
        raise ValueError(f"invalid_scope:{scope}")

    items: list[dict] = []
    runnable: list[dict] = []
    skipped_reasons: dict[str, int] = {}
    total_chars_sum = 0
    total_original_sum = 0
    total_filtered_sum = 0
    total_removed_blocks = 0
    total_removed_chars = 0
    total_dur = 0
    total_cost = 0.0

    for pid in target_ids:
        try:
            info = estimate_pair(session_id, pid, config=cfg)
        except KeyError:
            continue
        item = {
            "pair_id": pid,
            "left_pdf_name": info["left_pdf_name"],
            "right_pdf_name": info["right_pdf_name"],
            "total_chars": info["total_chars"],
            "total_original_chars": info.get("total_original_chars", info["total_chars"]),
            "total_filtered_chars": info.get("total_filtered_chars", info["total_chars"]),
            "removed_image_blocks_total": info.get("removed_image_blocks_total", 0),
            "removed_image_chars_total": info.get("removed_image_chars_total", 0),
            "status": info["status"],
            "estimated_duration_sec": info["estimated_duration_sec"],
            "estimated_cost_usd": info["estimated_cost_usd"],
            "has_cached_result": info["has_cached_result"],
            "warnings": info["warnings"],
        }
        items.append(item)

        is_blocked = bool(info.get("blocking"))
        if is_blocked:
            for r in info["blocking"]:
                skipped_reasons[r] = skipped_reasons.get(r, 0) + 1
            continue
        if info["has_cached_result"] and not force:
            skipped_reasons["cached"] = skipped_reasons.get("cached", 0) + 1
            # cached считаем как skipped для runnable, но оставляем в items
            item["status"] = "cached"
            continue
        if force and info["has_cached_result"]:
            item["status"] = "ready"  # перезапустим
        runnable.append(item)
        total_chars_sum += int(info["total_chars"] or 0)
        total_original_sum += int(info.get("total_original_chars") or 0)
        total_filtered_sum += int(info.get("total_filtered_chars") or 0)
        total_removed_blocks += int(info.get("removed_image_blocks_total") or 0)
        total_removed_chars += int(info.get("removed_image_chars_total") or 0)
        total_dur += int(info["estimated_duration_sec"] or 0)
        total_cost += float(info["estimated_cost_usd"] or 0.0)

    total_cost = round(total_cost, 2)

    # Limits на агрегат
    w_cost, h_cost = warn_cost_usd(), hard_cost_usd()
    w_dur, h_dur = warn_duration_sec(), hard_duration_sec()
    cost_warning = total_cost > w_cost
    cost_hard = total_cost > h_cost
    dur_warning = total_dur > w_dur
    dur_hard = total_dur > h_dur

    warnings_text: list[str] = []
    if skipped_reasons.get("missing_md"):
        warnings_text.append(f"{skipped_reasons['missing_md']} пары будут пропущены: отсутствует MD")
    if skipped_reasons.get("too_large"):
        warnings_text.append(f"{skipped_reasons['too_large']} пары будут пропущены: MD слишком большой")
    if skipped_reasons.get("provider_unavailable") or skipped_reasons.get("disabled"):
        n = (skipped_reasons.get("provider_unavailable", 0)
             + skipped_reasons.get("disabled", 0))
        warnings_text.append(f"{n} пары будут пропущены: provider недоступен")
    if skipped_reasons.get("cached"):
        warnings_text.append(
            f"{skipped_reasons['cached']} пары уже посчитаны (используйте force=true для перезапуска)"
        )
    if runnable:
        warnings_text.append(_humanize_duration(total_dur))
        warnings_text.append(f"Ориентировочная стоимость: ~${total_cost:.2f}")
    if cost_warning:
        warnings_text.append(
            f"Оценка стоимости (${total_cost:.2f}) превышает warn-лимит ${w_cost:.2f}"
        )
    if dur_warning:
        warnings_text.append(
            f"Оценочное время ({_humanize_duration(total_dur).split(': ')[-1]}) "
            f"превышает warn-лимит {_humanize_duration(int(w_dur)).split(': ')[-1]}"
        )
    if cost_hard or dur_hard:
        warnings_text.append(
            "Оценка превышает безопасный лимит. Запустите анализ по отдельным парам."
        )

    can_run_batch = bool(runnable) and not cost_hard and not dur_hard

    return {
        "ok": True,
        "scope": scope,
        "force": bool(force),
        "total_pairs": len(items),
        "runnable_pairs": len(runnable),
        "skipped_pairs": len(items) - len(runnable),
        "skipped_reasons": skipped_reasons,
        "total_chars": total_chars_sum,
        "total_original_chars": total_original_sum,
        "total_filtered_chars": total_filtered_sum,
        "removed_image_blocks_total": total_removed_blocks,
        "removed_image_chars_total": total_removed_chars,
        "estimated_duration_sec": total_dur,
        "estimated_cost_usd": total_cost,
        "items": items,
        "warnings": warnings_text,
        "limits": {
            "warn_cost_usd": w_cost,
            "warn_duration_sec": w_dur,
            "hard_cost_usd": h_cost,
            "hard_duration_sec": h_dur,
            "cost_warning": cost_warning,
            "cost_hard": cost_hard,
            "duration_warning": dur_warning,
            "duration_hard": dur_hard,
        },
        "can_run_batch": can_run_batch,
        "provider": cfg.provider,
        "model": cfg.model,
        "provider_enabled": bool(cfg.enabled),
    }


def _humanize_duration(seconds: int) -> str:
    if seconds <= 0:
        return "Оценочное время: —"
    if seconds < 60:
        return f"Оценочное время анализа: {seconds} с"
    if seconds < 3600:
        m = seconds // 60
        s = seconds % 60
        if s:
            return f"Оценочное время анализа: {m} мин {s} с"
        return f"Оценочное время анализа: {m} мин"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if m:
        return f"Оценочное время анализа: {h} ч {m} мин"
    return f"Оценочное время анализа: {h} ч"


__all__ = [
    "REFERENCE_CHARS",
    "REFERENCE_DURATION_SEC",
    "REFERENCE_COST_USD",
    "SAFETY_FACTOR",
    "MIN_DURATION_SEC",
    "MIN_COST_USD",
    "estimate_duration_sec",
    "estimate_cost_usd",
    "estimate_pair",
    "estimate_session",
    "warn_cost_usd",
    "warn_duration_sec",
    "hard_cost_usd",
    "hard_duration_sec",
]
