"""Dashboard-friendly агрегат по платным расходам.

Собирает данные из двух источников:
  1. paid_cost.json → daily_breakdown — агрегаты по дням (есть начиная
     с момента внедрения daily_breakdown);
  2. paid_cost_events.jsonl — детальные события (есть начиная с момента
     внедрения paid_api_guard).

Контракт ответа:
{
  "days": [
    {
      "date": "2026-05-16",
      "total_usd": 4.1951,
      "n_calls": 13,
      "by_model": {"openai/gpt-5.4": 4.1951},
      "by_project": {"M31A": 4.1951},
      "by_stage": {"block_analysis": 4.1951},
      "aggregated_only": false,        # true если по этому дню нет events
      "events": [
        {"ts": "...", "time": "14:30:16", "cost_usd": 0.3227, ...}
      ]
    }
  ],
  "window_days": 30,
  "totals": {
    "period_total_usd": ...,
    "period_calls": ...
  }
}

Dashboard читает только данные; никаких побочных эффектов.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _safe_read_jsonl(path: Path) -> list[dict]:
    """Прочитать jsonl, пропуская битые строки.

    Никогда не падает — в худшем случае возвращает []. Это критично:
    dashboard не должен ломаться из-за повреждённой строки журнала.
    """
    if not path.exists():
        return []
    out: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        out.append(obj)
                except (json.JSONDecodeError, ValueError):
                    # Битая строка — пропускаем, не уроняя endpoint.
                    continue
    except OSError as e:
        logger.warning("Failed to read jsonl %s: %s", path, e)
    return out


def _shorten_event(ev: dict) -> dict:
    """Облегчённый формат события для UI."""
    ts = str(ev.get("ts") or "")
    time_str = ts[11:19] if len(ts) >= 19 else ""
    return {
        "ts": ts,
        "time": time_str,
        "cost_usd": round(float(ev.get("cost_usd") or 0.0), 8),
        "model": ev.get("model") or "",
        "project_id": ev.get("project_id") or "",
        "version_id": ev.get("version_id") or "",
        "stage": ev.get("stage") or "",
        "source": ev.get("source") or "",
        "manual_run_id": ev.get("manual_run_id") or "",
        "job_id": ev.get("job_id") or "",
        "input_tokens": ev.get("input_tokens"),
        "output_tokens": ev.get("output_tokens"),
        "response_id": ev.get("response_id") or "",
    }


def _aggregate_events(events: list[dict]) -> dict:
    """Сложить набор events в формат, совместимый с daily_breakdown."""
    total = 0.0
    by_model: dict[str, float] = {}
    by_project: dict[str, float] = {}
    by_stage: dict[str, float] = {}
    for ev in events:
        cost = float(ev.get("cost_usd") or 0.0)
        if cost <= 0:
            continue
        total += cost
        m = ev.get("model") or "unknown"
        p = ev.get("project_id") or "unknown"
        s = ev.get("stage") or "unknown"
        by_model[m] = by_model.get(m, 0.0) + cost
        by_project[p] = by_project.get(p, 0.0) + cost
        by_stage[s] = by_stage.get(s, 0.0) + cost
    return {
        "total": round(total, 6),
        "n_calls": len(events),
        "by_model": {k: round(v, 6) for k, v in by_model.items()},
        "by_project": {k: round(v, 6) for k, v in by_project.items()},
        "by_stage": {k: round(v, 6) for k, v in by_stage.items()},
    }


def build_paid_cost_daily_dashboard(
    *,
    days: int = 30,
    paid_cost_file: Optional[Path] = None,
    events_file: Optional[Path] = None,
    max_events_per_day: int = 200,
    today: Optional[datetime] = None,
) -> dict:
    """Построить dashboard-friendly агрегат за последние N дней.

    Args:
        days: размер окна.
        paid_cost_file: путь к paid_cost.json (None → config.PAID_COST_FILE).
        events_file: путь к paid_cost_events.jsonl (None → config.PAID_COST_EVENTS_FILE).
        max_events_per_day: лимит событий на день, чтобы не раздувать ответ.
        today: для тестов; реальное "сегодня" по умолчанию.

    Returns:
        dict с полями days[], window_days, totals{period_total_usd, period_calls}.
    """
    # ── Lazy import чтобы не было circular dependency при тестах ──
    if paid_cost_file is None or events_file is None:
        from backend.app.core.config import (
            PAID_COST_EVENTS_FILE as _events,
        )
        from backend.app.services.common.usage_service import PAID_COST_FILE as _pc
        if paid_cost_file is None:
            paid_cost_file = _pc
        if events_file is None:
            events_file = _events

    days = max(1, min(int(days), 365))
    now = today or datetime.now()
    today_date = now.date()
    cutoff_date = today_date - timedelta(days=days - 1)

    # ── 1. Загрузить агрегаты из paid_cost.json ──
    breakdown: dict[str, dict] = {}
    if paid_cost_file and Path(paid_cost_file).exists():
        try:
            with open(paid_cost_file, "r", encoding="utf-8") as f:
                pc_data = json.load(f)
            if isinstance(pc_data, dict):
                raw = pc_data.get("daily_breakdown") or {}
                if isinstance(raw, dict):
                    for d, v in raw.items():
                        if isinstance(v, dict):
                            breakdown[d] = v
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to read paid_cost.json: %s", e)

    # ── 2. Загрузить detail events ──
    all_events = _safe_read_jsonl(Path(events_file)) if events_file else []
    events_by_day: dict[str, list[dict]] = {}
    for ev in all_events:
        ts = str(ev.get("ts") or "")
        date_key = ts[:10]
        if not date_key:
            continue
        events_by_day.setdefault(date_key, []).append(ev)

    # ── 3. Объединить ключи (множество дней с любым из источников) ──
    all_dates: set[str] = set(breakdown.keys()) | set(events_by_day.keys())
    days_out: list[dict] = []
    for d in sorted(all_dates, reverse=True):
        # Фильтр по окну.
        try:
            d_date = datetime.fromisoformat(d).date()
        except (ValueError, TypeError):
            continue
        if d_date < cutoff_date or d_date > today_date:
            continue

        day_events_raw = sorted(
            events_by_day.get(d, []),
            key=lambda e: str(e.get("ts") or ""),
            reverse=True,
        )
        day_events = [_shorten_event(e) for e in day_events_raw[:max_events_per_day]]

        # Агрегаты: предпочитаем daily_breakdown если он есть, иначе синтезируем
        # из events. Если есть и breakdown, и events — даём breakdown как
        # source of truth для total/n_calls (он суммируется в реальном времени
        # paid_cost_tracker'ом), а events используем для деталей.
        if d in breakdown:
            bd = breakdown[d]
            total = float(bd.get("total") or 0.0)
            n_calls = int(bd.get("n_calls") or 0)
            by_model = {
                k: round(float(v), 6)
                for k, v in (bd.get("by_model") or {}).items()
            }
            by_project = {
                k: round(float(v), 6)
                for k, v in (bd.get("by_project") or {}).items()
            }
            by_stage = {
                k: round(float(v), 6)
                for k, v in (bd.get("by_stage") or {}).items()
            }
            aggregated_only = len(day_events_raw) == 0
        else:
            agg = _aggregate_events(day_events_raw)
            total = float(agg["total"])
            n_calls = int(agg["n_calls"])
            by_model = agg["by_model"]
            by_project = agg["by_project"]
            by_stage = agg["by_stage"]
            aggregated_only = False  # есть events → детализация доступна

        days_out.append({
            "date": d,
            "total_usd": round(total, 4),
            "n_calls": n_calls,
            "by_model": by_model,
            "by_project": by_project,
            "by_stage": by_stage,
            "aggregated_only": aggregated_only,
            "events": day_events,
            "events_truncated": len(day_events_raw) > max_events_per_day,
        })

    period_total = round(sum(d["total_usd"] for d in days_out), 4)
    period_calls = sum(int(d["n_calls"] or 0) for d in days_out)

    return {
        "days": days_out,
        "window_days": days,
        "totals": {
            "period_total_usd": period_total,
            "period_calls": period_calls,
        },
    }
