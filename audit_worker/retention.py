"""RetentionManager — единственное, что вправе удалять данные на воркере.

Отвечает ТОЛЬКО за локальные копии. Центральные исходные и результирующие
пакеты он не трогает и трогать не может: у него нет ни сети, ни доступа к
хранилищу центра (I-14).

Два правила, из-за которых всё и построено так, а не «удалять старое по cron»:

  * пока центр не подтвердил приём результата, `retention_until` пуст, признак
    `retention_unconfirmed` взведён, и автоматическое удаление ЗАПРЕЩЕНО при
    любых условиях — включая нехватку диска (I-12). Освобождать место, стирая
    единственную копию сделанной работы, — не оптимизация, а потеря данных;
  * физическое удаление включается ОТДЕЛЬНЫМ флагом. По умолчанию менеджер
    работает в режиме сухого прогона: считает кандидатов, показывает
    ожидаемый выигрыш и пишет событие — но ничего не стирает.

Порядок удаления — tombstone → атомарное переименование в локальную корзину →
стирание содержимого. Так повтор после сбоя безопасен: каталог уже не по
рабочему пути, а запись о попытке и её hash остаются в БД навсегда.
"""
from __future__ import annotations

import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from audit_worker import local_db
from audit_worker.config import WorkerConfig
from audit_worker.event_outbox import EventOutbox
from audit_worker.local_store import LocalJobStore, atomic_write_json

# Локальные состояния, при которых удалять нельзя ни при каких флагах.
_LIVE_LOCAL_STATES = frozenset(
    {"assigned", "downloading", "verified", "running", "completed_locally", "uploading"}
)


class DeletionRefused(RuntimeError):
    """Удаление отклонено правилом безопасности."""


def _dir_size(path: Path) -> int:
    total = 0
    if not path.is_dir():
        return 0
    for item in path.rglob("*"):
        try:
            if item.is_file() and not item.is_symlink():
                total += item.stat().st_size
        except OSError:
            continue
    return total


class RetentionManager:
    def __init__(
        self,
        config: WorkerConfig,
        db: local_db.LocalDB,
        *,
        jobs: Optional[LocalJobStore] = None,
    ):
        self.config = config
        self.db = db
        self.jobs = jobs or LocalJobStore(config.jobs_dir)
        self._last_scan = 0.0

    # ─── Периодический проход ────────────────────────────────────────────────
    def tick(self, *, force: bool = False) -> Optional[dict[str, Any]]:
        if not self.config.retention_enabled:
            return None
        now = time.time()
        if not force and now - self._last_scan < self.config.retention_scan_interval_sec:
            return None
        self._last_scan = now
        return self.sweep()

    def purge_trash(self) -> int:
        """Дочистить корзину после обрыва между переименованием и стиранием.

        Каталог в корзине уже не на рабочем пути, но место занимает. Без этого
        прохода сбой посреди удаления оставлял бы мусор навсегда — то есть
        «удаление освободило место» оказывалось бы неправдой.
        """
        removed = 0
        if not self.config.trash_dir.is_dir():
            return 0
        for item in list(self.config.trash_dir.iterdir()):
            try:
                if item.is_dir() and not item.is_symlink():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
                removed += 1
            except OSError:
                continue
        return removed

    def sweep(self) -> dict[str, Any]:
        """Найти кандидатов и — если разрешено — удалить их."""
        self.purge_trash()
        candidates = self.candidates()
        report: dict[str, Any] = {
            "at": time.time(),
            "dry_run": not self.config.retention_delete_enabled,
            "candidates": candidates,
            "candidate_count": len(candidates),
            "reclaimable_bytes": sum(c["size_bytes"] for c in candidates),
            "deleted": [],
        }
        if not self.config.retention_delete_enabled:
            # Сухой прогон: только считаем и показываем. Записи в БД о
            # «удалено» не появляется — иначе экран врал бы оператору.
            self._write_report(report)
            return report
        for candidate in candidates:
            outcome = self.delete_attempt(
                job_id=candidate["job_id"],
                attempt_id=candidate["attempt_id"],
                manual=False,
            )
            report["deleted"].append(outcome)
        self._write_report(report)
        return report

    def _write_report(self, report: dict[str, Any]) -> None:
        atomic_write_json(self.config.runtime_dir / "retention_report.json", report)

    # ─── Кандидаты ───────────────────────────────────────────────────────────
    def candidates(self, *, now: Optional[float] = None) -> list[dict[str, Any]]:
        """Попытки, срок хранения которых наступил и удаление которых безопасно."""
        stamp = now if now is not None else time.time()
        out: list[dict[str, Any]] = []
        for meta in self.jobs.iter_all():
            job_id, attempt_id = meta.get("job_id"), meta.get("attempt_id")
            if not job_id or not attempt_id:
                continue
            allowed, reason = self.deletion_allowed(job_id, attempt_id, now=stamp)
            if not allowed:
                continue
            job_dir = self.jobs.job_dir(job_id, attempt_id)
            out.append(
                {
                    "job_id": job_id,
                    "attempt_id": attempt_id,
                    "retention_until": meta.get("retention_until"),
                    "result_hash": meta.get("result_hash"),
                    "size_bytes": _dir_size(job_dir),
                    "reason": reason,
                }
            )
        return out

    def deletion_allowed(
        self,
        job_id: str,
        attempt_id: str,
        *,
        now: Optional[float] = None,
        manual: bool = False,
    ) -> tuple[bool, str]:
        """Можно ли удалить локальные данные попытки. Возвращает (да/нет, причина).

        Проверки дублируют центральные намеренно: команда приходит по сети, а
        доверять входу нельзя. Порядок — от самого дорогого последствия к
        самому дешёвому.
        """
        stamp = now if now is not None else time.time()
        meta = self.jobs.load(job_id, attempt_id)
        if meta is None:
            return False, "нет метаданных попытки"

        queue = self.db.queue_item(attempt_id)
        if queue and queue.get("state") not in local_db.TERMINAL_QUEUE_STATES:
            return False, "попытка ещё в работе"
        if meta.get("local_state") in _LIVE_LOCAL_STATES:
            return False, f"локальное состояние {meta.get('local_state')}"
        row = self.db.process_row(attempt_id)
        if row and row.get("status") == "running":
            return False, "процесс помечен работающим"

        if not meta.get("result_hash"):
            return False, "нет подтверждённого hash результата"
        if meta.get("retention_until") is None:
            # Ровно тот случай, ради которого признак и заведён: центр приём
            # НЕ подтвердил. Удалять запрещено даже ручной командой (I-12).
            return False, "retention_unconfirmed: центр не подтвердил приём"

        job_dir = self.jobs.job_dir(job_id, attempt_id)
        outbox_dir = job_dir / "events"
        if outbox_dir.is_dir():
            outbox = EventOutbox(outbox_dir)
            if outbox.has_pending:
                return False, "в outbox остались неотправленные события"

        if not manual and float(meta["retention_until"]) > stamp:
            return False, "срок хранения ещё не наступил"
        return True, "срок хранения наступил" if not manual else "ручная команда оператора"

    # ─── Физическое удаление ─────────────────────────────────────────────────
    def delete_attempt(
        self, *, job_id: str, attempt_id: str, manual: bool = False
    ) -> dict[str, Any]:
        """Удалить локальные данные попытки. Идемпотентно."""
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        if not job_dir.exists():
            return {"status": "ok",
                    "detail": {"outcome": "already_deleted", "attempt_id": attempt_id}}

        allowed, reason = self.deletion_allowed(job_id, attempt_id, manual=manual)
        if not allowed:
            return {"status": "error",
                    "detail": {"outcome": "refused", "reason": reason,
                               "attempt_id": attempt_id}}
        if not self.config.retention_delete_enabled:
            return {
                "status": "ok",
                "detail": {
                    "outcome": "dry_run",
                    "attempt_id": attempt_id,
                    "size_bytes": _dir_size(job_dir),
                    "message": (
                        "Физическое удаление выключено "
                        "(AUDIT_WORKER_RETENTION_DELETE_ENABLED=false)"
                    ),
                },
            }
        # Снимок метаданных снимается ДО удаления: metadata.json лежит
        # внутри удаляемого каталога, и после стирания читать уже нечего —
        # tombstone остался бы без hash, ради которого он и заводится.
        meta_snapshot = self.jobs.load(job_id, attempt_id) or {}
        try:
            self._safe_remove(job_dir, job_id=job_id, attempt_id=attempt_id)
        except DeletionRefused as exc:
            return {"status": "error",
                    "detail": {"outcome": "refused", "reason": str(exc),
                               "attempt_id": attempt_id}}

        self._record_tombstone(job_id, attempt_id, meta_snapshot)
        return {
            "status": "ok",
            "detail": {
                "outcome": "deleted",
                "attempt_id": attempt_id,
                "job_id": job_id,
                "result_hash": meta_snapshot.get("result_hash"),
                "manual": manual,
            },
        }

    def _safe_remove(self, job_dir: Path, *, job_id: str, attempt_id: str) -> None:
        """Переименовать в корзину и стереть. Выход за корень данных невозможен."""
        root = self.config.jobs_dir.resolve()
        try:
            target = job_dir.resolve(strict=True)
        except OSError as exc:
            raise DeletionRefused(f"каталог недоступен: {exc}") from exc
        if job_dir.is_symlink():
            # Симлинк удаляем как ссылку и НЕ идём по нему: иначе удаление
            # ушло бы за пределы каталога данных.
            job_dir.unlink()
            return
        # `target == root` было в РАЗРЕШАЮЩЕЙ части условия: симлинк вида
        # `jobs/X -> ..` вместе с ключами `job_id=X, attempt_id=jobs` давал
        # target == jobs_dir, и в корзину уезжал весь каталог заданий.
        if target == root or root not in target.parents:
            raise DeletionRefused(
                f"путь {target} вне {root} — удаление отклонено"
            )
        trash = self.config.trash_dir / f"{attempt_id}.{uuid.uuid4().hex[:8]}"
        trash.parent.mkdir(parents=True, exist_ok=True)
        # Tombstone пишется ДО переименования: обрыв после него оставит след,
        # по которому повтор безопасно доделает работу.
        atomic_write_json(
            trash.with_suffix(".tombstone.json"),
            {"job_id": job_id, "attempt_id": attempt_id, "at": time.time(),
             "stage": "renaming"},
        )
        target.rename(trash)
        shutil.rmtree(trash, ignore_errors=True)
        trash.with_suffix(".tombstone.json").unlink(missing_ok=True)

    def _record_tombstone(
        self, job_id: str, attempt_id: str, meta: dict[str, Any]
    ) -> None:
        """Запись о попытке и её hash сохраняются, сами данные — нет."""
        path = self.config.tombstones_dir / f"{attempt_id}.json"
        atomic_write_json(
            path,
            {
                "job_id": job_id,
                "attempt_id": attempt_id,
                "result_hash": meta.get("result_hash"),
                "result_size": meta.get("result_size"),
                "retention_until": meta.get("retention_until"),
                "deleted_at": time.time(),
                "note": "локальная копия удалена; центральная копия не затронута",
            },
        )
        self.db.update_process(attempt_id, status="deleted")

    def tombstones(self) -> list[dict[str, Any]]:
        out = []
        if not self.config.tombstones_dir.is_dir():
            return out
        for path in sorted(self.config.tombstones_dir.glob("*.json")):
            try:
                out.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                continue
        return out

    # ─── Диск ────────────────────────────────────────────────────────────────
    def disk_snapshot(self) -> dict[str, Any]:
        """Разрез диска, по которому оператор принимает решение (§12.5)."""
        usage = shutil.disk_usage(str(self.config.root))
        confirmed = 0
        unconfirmed = 0
        for meta in self.jobs.iter_all():
            job_id, attempt_id = meta.get("job_id"), meta.get("attempt_id")
            if not job_id or not attempt_id:
                continue
            size = _dir_size(self.jobs.job_dir(job_id, attempt_id))
            if meta.get("result_hash") and meta.get("retention_until") is None:
                unconfirmed += size
            elif meta.get("result_hash"):
                confirmed += size
        candidates = self.candidates()
        free = usage.free
        level = "ok"
        if free <= self.config.disk_critical_free_bytes:
            level = "critical"
        elif free <= self.config.disk_warning_free_bytes:
            level = "warning"
        return {
            "total_bytes": float(usage.total),
            "used_bytes": float(usage.used),
            "free_bytes": float(free),
            "jobs_bytes": float(_dir_size(self.config.jobs_dir)),
            "confirmed_results_bytes": float(confirmed),
            "unconfirmed_results_bytes": float(unconfirmed),
            "cleanup_candidates_bytes": float(sum(c["size_bytes"] for c in candidates)),
            "cleanup_candidates": len(candidates),
            "level": level,
        }
