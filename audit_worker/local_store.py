"""Локальное состояние воркера: файлы, переживающие рестарт.

Хранилище файловое (а не SQLite) — так решено в §7.3 техпроекта: у воркера
один писатель и десятки записей, а файлы проще инспектировать на чужом VPS.
Все записи атомарные: tmp + os.replace, как принято в этом репозитории.

Модуль назван local_store, а не local_database, именно потому, что базы здесь
нет — имя не должно врать о содержимом.

Раскладка (§19.3 техпроекта):
    <root>/worker_state.json
    <root>/jobs/<job_id>/<attempt_id>/metadata.json
                                     /source/  work/  result/  events/  logs/  uploads/
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterator, Optional


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


class WorkerStateStore:
    """worker_id / instance_id / токен. instance_id — новый на каждый запуск."""

    def __init__(self, state_path: Path, token_path: Path):
        self.state_path = state_path
        self.token_path = token_path

    def load(self) -> dict[str, Any]:
        return read_json(self.state_path, {}) or {}

    def save(self, data: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, data)

    def read_token(self) -> Optional[str]:
        if not self.token_path.is_file():
            return None
        return self.token_path.read_text(encoding="utf-8").strip() or None

    def write_token(self, token: str) -> None:
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.token_path.with_suffix(".tmp")
        tmp.write_text(token, encoding="utf-8")
        os.chmod(tmp, 0o600)          # права ДО переименования: гонки нет
        os.replace(tmp, self.token_path)


class LocalJobStore:
    """Реестр заданий воркера: одно metadata.json на попытку."""

    def __init__(self, jobs_dir: Path):
        self.jobs_dir = jobs_dir

    def job_dir(self, job_id: str, attempt_id: str) -> Path:
        return self.jobs_dir / job_id / attempt_id

    def meta_path(self, job_id: str, attempt_id: str) -> Path:
        return self.job_dir(job_id, attempt_id) / "metadata.json"

    def create(self, assignment: dict[str, Any]) -> dict[str, Any]:
        job_id = assignment["job_id"]
        attempt_id = assignment["attempt_id"]
        base = self.job_dir(job_id, attempt_id)
        for sub in ("source", "work", "result", "events", "logs", "uploads"):
            (base / sub).mkdir(parents=True, exist_ok=True)
        meta = {
            "job_id": job_id,
            "attempt_id": attempt_id,
            "job_type": assignment.get("job_type"),
            "project_id": assignment.get("project_id"),
            "version_id": assignment.get("version_id"),
            "execution_token": assignment.get("execution_token"),
            "params": assignment.get("params") or {},
            "package": assignment.get("package") or {},
            "local_state": "assigned",
            "created_at": time.time(),
            "started_at": None,
            "completed_locally_at": None,
            "result_hash": None,
            "result_size": None,
            # NULL до подтверждения приёма центром. Пока NULL — автоудаление
            # запрещено при любых условиях (инвариант I-08 и признак
            # retention_unconfirmed §10.6 техпроекта).
            "retention_until": None,
            "pid": None,
        }
        self.save(meta)
        return meta

    def save(self, meta: dict[str, Any]) -> None:
        atomic_write_json(self.meta_path(meta["job_id"], meta["attempt_id"]), meta)

    def load(self, job_id: str, attempt_id: str) -> Optional[dict[str, Any]]:
        return read_json(self.meta_path(job_id, attempt_id), None)

    def update(self, job_id: str, attempt_id: str, **fields: Any) -> dict[str, Any]:
        meta = self.load(job_id, attempt_id) or {"job_id": job_id, "attempt_id": attempt_id}
        meta.update(fields)
        self.save(meta)
        return meta

    def iter_all(self) -> Iterator[dict[str, Any]]:
        if not self.jobs_dir.is_dir():
            return
        for job_dir in sorted(self.jobs_dir.iterdir()):
            if not job_dir.is_dir():
                continue
            for attempt_dir in sorted(job_dir.iterdir()):
                meta = read_json(attempt_dir / "metadata.json", None)
                if meta:
                    yield meta

    def active(self) -> list[dict[str, Any]]:
        """Незавершённые локально задания (для heartbeat и слотов)."""
        return [
            m
            for m in self.iter_all()
            if m.get("local_state")
            in {"assigned", "downloading", "verified", "running", "completed_locally",
                "uploading"}
        ]

    def retention_unconfirmed(self) -> list[dict[str, Any]]:
        """Готовые результаты, приём которых центр НЕ подтвердил.

        Вычисляемый признак, не состояние: (результат материализован) AND
        (retention_until IS NULL). Удалять такие задания запрещено —
        даже при нехватке диска.
        """
        return [
            m
            for m in self.iter_all()
            if m.get("result_hash") and m.get("retention_until") is None
        ]
