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
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterator, Optional
import fcntl


def atomic_write_json(path: Path, data: Any) -> None:
    """Запись через временный файл и `os.replace` — читатель видит либо старое, либо новое.

    Имя временного файла уникально на КАЖДЫЙ вызов: pid + идентификатор потока +
    случайный суффикс. С одним лишь pid два потока одного процесса (а с двумя
    слотами их теперь именно два) писали в ОДИН и тот же tmp: содержимое
    перемешивалось, и в metadata.json попытки уезжал наполовину чужой JSON.

    Одного `get_ident()` мало: он уникален только среди ЖИВЫХ потоков и
    переиспользуется после смерти потока. Для основного случая (два потока
    пишут одновременно) этого хватило бы, но остаётся хвост: поток умер между
    записью tmp и `os.replace`, его номер достался новому потоку — и новый
    пишет поверх чужого мусора. Случайный суффикс закрывает и это.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(
        f"{path.suffix}.tmp.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex[:8]}"
    )
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
        self._epoch_lock = threading.Lock()

    def load(self) -> dict[str, Any]:
        return read_json(self.state_path, {}) or {}

    def save(self, data: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, data)

    def reserve_connection_epoch(self) -> int:
        """Durably reserve a strictly increasing epoch before opening a stream."""
        lock_path = self.state_path.with_name("worker_state.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._epoch_lock, lock_path.open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            state = self.load()
            epoch = int(state.get("connection_epoch") or 0) + 1
            state["connection_epoch"] = epoch
            self.save(state)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        return epoch

    def read_token(self) -> Optional[str]:
        if not self.token_path.is_file():
            return None
        return self.token_path.read_text(encoding="utf-8").strip() or None

    def write_token(self, token: str) -> None:
        self._write_secret(self.token_path, token)

    # ─── Одноразовый claim-secret ────────────────────────────────────────────
    # Живёт между регистрацией и одобрением оператором. После обмена на токен
    # удаляется: второй раз он всё равно не сработает.
    @property
    def claim_path(self) -> Path:
        return self.token_path.with_name("claim_secret")

    def read_claim_secret(self) -> Optional[str]:
        if not self.claim_path.is_file():
            return None
        return self.claim_path.read_text(encoding="utf-8").strip() or None

    def write_claim_secret(self, secret: str) -> None:
        self._write_secret(self.claim_path, secret)

    def drop_claim_secret(self) -> None:
        self.claim_path.unlink(missing_ok=True)

    @staticmethod
    def _write_secret(path: Path, value: str) -> None:
        """Записать секрет атомарно и с правами 0600.

        chmod делается ДО переименования: иначе между replace и chmod файл
        секунду доступен на чтение всем.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(
            f".tmp.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex[:8]}"
        )
        tmp.write_text(value, encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)


class LocalJobStore:
    """Реестр заданий воркера: одно metadata.json на попытку."""

    def __init__(self, jobs_dir: Path):
        self.jobs_dir = jobs_dir

    def job_dir(self, job_id: str, attempt_id: str) -> Path:
        # Через paths.attempt_dir, а не склейкой: правило I-11 «сегмент пути
        # строится только из безопасного ключа» не должно иметь обходных
        # дорожек. Внешний код проекта здесь не пройдёт ни при каких данных.
        from audit_worker.paths import attempt_dir

        return attempt_dir(self.jobs_dir, job_id, attempt_id)

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
        # 0600: в metadata.json лежит execution_token попытки. Каталог данных
        # и так 0750, но полагаться на одни только права каталога для файла с
        # секретом не стоит — worker-token и claim-secret пишутся так же.
        path = self.meta_path(meta["job_id"], meta["attempt_id"])
        atomic_write_json(path, meta)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

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
