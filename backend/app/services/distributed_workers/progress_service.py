"""Честный прогресс задания.

Правило техпроцекта §14.3: выдуманный процент не показывается. Процент
считается ТОЛЬКО когда воркер прислал достоверный `total` и явно пометил
`percent_reliable`. Иначе интерфейс обязан показать четыре вещи:
неопределённый индикатор, длительность, последнюю строку лога и число
завершённых внутренних операций.

ETA заполняется только при выполнении трёх условий (см. `_eta`); во всех
остальных случаях `eta_sec = None`, `eta_basis = "unavailable"`.
"""
from __future__ import annotations

import time
from typing import Any, Optional


def build_view(
    job: dict[str, Any], snapshot: Optional[dict[str, Any]], *, now: Optional[float] = None
) -> dict[str, Any]:
    """Собрать блок прогресса для API/UI из последнего снимка событий."""
    stamp = now or time.time()
    started_at = job.get("started_at") or job.get("assigned_at") or job.get("created_at")
    elapsed = max(0.0, stamp - float(started_at)) if started_at else 0.0

    if not snapshot:
        return {
            "indeterminate": True,
            "stage": None,
            "stage_index": None,
            "stage_total": None,
            "unit": None,
            "processed": None,
            "total": None,
            "percent": None,
            "percent_reliable": False,
            "elapsed_sec": round(elapsed, 1),
            "throughput_per_min": None,
            "delta_5min": None,
            "eta_sec": None,
            "eta_basis": "unavailable",
            "last_significant_event": None,
            "completed_operations": 0,
        }

    processed = snapshot.get("processed")
    total = snapshot.get("total")
    reliable = bool(snapshot.get("percent_reliable")) and isinstance(total, int) and total > 0

    percent = None
    if reliable and isinstance(processed, int):
        percent = round(min(100.0, max(0.0, processed / total * 100.0)), 1)

    throughput = snapshot.get("throughput_per_min")
    eta_sec, eta_basis = _eta(
        reliable=reliable,
        processed=processed if isinstance(processed, int) else None,
        total=total if isinstance(total, int) else None,
        throughput_per_min=throughput,
    )

    return {
        "indeterminate": not reliable,
        "stage": snapshot.get("stage"),
        "stage_index": snapshot.get("stage_index"),
        "stage_total": snapshot.get("stage_total"),
        "unit": snapshot.get("unit"),
        "processed": processed,
        "total": total,
        "percent": percent,
        "percent_reliable": reliable,
        "elapsed_sec": round(snapshot.get("elapsed_sec", elapsed), 1),
        "throughput_per_min": throughput,
        "delta_5min": snapshot.get("delta_5min"),
        "eta_sec": eta_sec,
        "eta_basis": eta_basis,
        "last_significant_event": snapshot.get("last_significant_event"),
        "completed_operations": snapshot.get("completed_operations", processed or 0),
        "snapshot_age_sec": round(max(0.0, stamp - float(snapshot.get("received_at", stamp))), 1),
    }


def _eta(
    *,
    reliable: bool,
    processed: Optional[int],
    total: Optional[int],
    throughput_per_min: Optional[float],
) -> tuple[Optional[float], str]:
    """Три условия из §14.3 — иначе ETA не показываем вовсе."""
    if not reliable or not processed or not total or not throughput_per_min:
        return None, "unavailable"
    if processed / total < 0.10:          # обработано меньше 10 % — рано
        return None, "too_early"
    if throughput_per_min <= 0:
        return None, "unavailable"
    remaining = max(0, total - processed)
    return round(remaining / throughput_per_min * 60.0, 1), "linear_on_throughput"
