"""Отметка «когда конвейер последний раз звал модель» — для heartbeat и отчёта.

Зачем отдельная отметка, если есть журнал вызовов. Журнал живёт ВНУТРИ каталога
попытки: он полон, точен и уезжает в пакет — но исчезает вместе с попыткой при
удержании, и агент, собирая heartbeat, не имеет права обходить все каталоги
заданий ради одного числа. Отметка — это ровно то, что переживает уборку и
читается одним `read_text`.

Что в отметке НЕ хранится и не будет: промпт, ответ, идентификатор задания,
модель, стоимость. Только «когда», «каким провайдером» и «сколько вызовов было
в той попытке». Всё остальное — в журнале попытки, который на центр уходит
пакетом, а не heartbeat'ом.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

MARKER_FILENAME = "last_pipeline_inference.json"


def marker_path(worker_root: Path) -> Path:
    return Path(worker_root) / "runtime" / MARKER_FILENAME


def record(
    worker_root: Path,
    *,
    provider: str,
    calls_started: int,
    calls_completed: int,
    now: Optional[float] = None,
) -> Path:
    """Записать отметку атомарно. Ошибки записи наверх не поднимаются."""
    target = marker_path(worker_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "observed_at": now if now is not None else time.time(),
        "provider": str(provider),
        "calls_started": int(calls_started),
        "calls_completed": int(calls_completed),
    }
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
    return target


def read(worker_root: Path) -> Optional[dict[str, Any]]:
    """Прочитать отметку. Отсутствие и повреждение одинаково дают None."""
    try:
        data = json.loads(marker_path(worker_root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None
