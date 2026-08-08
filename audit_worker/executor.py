"""audit-worker-executor — локальный исполнитель. Сети не знает.

Разделение с сетевым агентом сделано не ради красоты, а ради инвариантов
I-02/I-03: пока запуск процесса жил в том же питоне, что и HTTP-клиент,
перезапуск агента гарантированно убивал работу, а второй запуск агента
гарантированно порождал второй процесс.

Что делает исполнитель:
  * читает локальную очередь worker.db и АТОМАРНО захватывает попытку;
  * запускает единственный известный ему конвейер `test_pipeline_v1`;
  * ведёт реестр процессов (pid + тик старта + отпечаток команды + группа);
  * пишет stdout/stderr в файлы, прогресс — на диск, события — в EventOutbox;
  * создаёт `completed.marker` и собирает архив результата;
  * исполняет локальные команды: отмену и удаление данных попытки.

Чего исполнитель НЕ делает и не может:
  * не ходит в центр — ни одного HTTP-вызова в этом модуле нет;
  * не знает worker-token и bootstrap-secret: они лежат в файлах, которые
    читает только агент (см. systemd-юниты в документации);
  * не решает сам, повторить ли задание: после `executor_interrupted` он
    ничего не перезапускает — это решение оператора (§8.6).
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

from audit_worker import (
    PROTOCOL_VERSION,
    __version__,
    local_db,
    package_io,
    process_control,
    process_registry,
    test_runner,
)
from audit_worker.config import WorkerConfig
from audit_worker.event_outbox import EventOutbox
from audit_worker.local_store import LocalJobStore, read_json
from audit_worker.retention import RetentionManager

HEARTBEAT_SEC = 15.0
POLL_SEC = 1.0


def _log(message: str) -> None:
    print(f"[executor {time.strftime('%H:%M:%S')}] {message}", flush=True)


def write_completed_marker(job_dir: Path, outcome: "test_runner.RunOutcome") -> None:
    """Маркер «процесс отработал» рядом с результатом.

    Различает два неотличимых по pid состояния: процесс завершился штатно
    против «процесс исчез». Без маркера рестарт не может понять, надо ли
    что-то доделывать, — и готовая работа объявляется провалом.
    """
    marker = job_dir / "work" / "completed.marker"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            {
                "exit_code": outcome.exit_code,
                "duration_sec": round(outcome.duration_sec, 3),
                "steps_done": outcome.steps_done,
                "steps_total": outcome.steps_total,
                "stdout_lines": outcome.stdout_lines,
                "stderr_lines": outcome.stderr_lines,
                "finished_at": time.time(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def read_completed_marker(job_dir: Path) -> Optional[dict[str, Any]]:
    data = read_json(job_dir / "work" / "completed.marker", None)
    return data if isinstance(data, dict) else None


class Executor:
    def __init__(self, config: WorkerConfig, *, db: Optional[local_db.LocalDB] = None):
        self.config = config
        config.ensure_dirs()
        self.db = db or local_db.LocalDB(config.local_db_path)
        self.jobs = LocalJobStore(config.jobs_dir)
        self.retention = RetentionManager(config, self.db, jobs=self.jobs)
        self.instance_id = self.db.register_executor(version=__version__)
        self._stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._workers: list[threading.Thread] = []

    # ─── Жизненный цикл ──────────────────────────────────────────────────────
    def run_forever(self, *, max_jobs: Optional[int] = None) -> None:
        _log(f"исполнитель запущен: {self.instance_id}, pid={os.getpid()}")
        self.recover_after_restart()
        self._start_heartbeat()
        done = 0
        try:
            while not self._stop.is_set():
                self.drain_local_commands()
                self.retention.tick()
                item = self.db.claim_next(self.instance_id)
                if item is None:
                    self._stop.wait(POLL_SEC)
                    continue
                self.run_attempt(item)
                done += 1
                if max_jobs is not None and done >= max_jobs:
                    _log(f"выполнено попыток: {done} — останавливаюсь по max_jobs")
                    break
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        self._stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
        self.db.executor_stopped(self.instance_id)

    def _start_heartbeat(self) -> None:
        def loop() -> None:
            while not self._stop.is_set():
                try:
                    self.db.executor_heartbeat(self.instance_id)
                except Exception:  # noqa: BLE001 — отметка живости не роняет работу
                    pass
                self._stop.wait(HEARTBEAT_SEC)

        self._heartbeat_thread = threading.Thread(
            target=loop, name="executor-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    # ─── Восстановление после рестарта исполнителя ───────────────────────────
    def recover_after_restart(self) -> list[dict[str, Any]]:
        """Разобраться с попытками, начатыми прошлым воплощением исполнителя.

        Одного pid недостаточно (I-17), поэтому смотрим четыре вещи: живость
        pid, тик старта, отпечаток команды и `completed.marker`. Второй процесс
        при любой неоднозначности НЕ запускаем — это прямой запрет §8.6.
        """
        outcomes: list[dict[str, Any]] = []
        for item in self.db.list_queue(states=(local_db.QUEUE_CLAIMED, local_db.QUEUE_RUNNING)):
            attempt_id = item["attempt_id"]
            job_dir = self.jobs.job_dir(item["job_id"], attempt_id)
            row = self.db.process_row(attempt_id)
            marker = read_completed_marker(job_dir)
            verdict = process_control.classify_after_restart(row, marker=marker)
            if verdict == "running":
                # Процесс пережил рестарт исполнителя — не трогаем и не
                # перезапускаем; наблюдение подхватит следующий цикл.
                self.db.set_queue_state(attempt_id, local_db.QUEUE_RUNNING)
                self._watch_survived(item, row or {})
                outcomes.append({"attempt_id": attempt_id, "verdict": "running"})
                continue
            if verdict == "exited":
                _log(f"попытка {attempt_id[:8]}: процесс отработал до рестарта — доупаковываю")
                self._finish_from_marker(item, marker or {})
                outcomes.append({"attempt_id": attempt_id, "verdict": "exited"})
                continue
            # interrupted / unknown: диагностическое состояние, автоповтора нет.
            self.db.set_queue_state(
                attempt_id,
                local_db.QUEUE_INTERRUPTED,
                result={
                    "outcome": "executor_interrupted",
                    "message": "процесс не найден и маркер завершения отсутствует",
                    "verdict": verdict,
                },
            )
            self.db.update_process(attempt_id, status="interrupted")
            self.jobs.update(
                item["job_id"], attempt_id,
                local_state="executor_interrupted",
                process_status="interrupted",
            )
            self._emit(
                item,
                "job_failed",
                {
                    "code": "executor_interrupted",
                    "reason": "error",
                    "message": (
                        "Исполнитель перезапущен, процесс не найден, маркер "
                        "завершения отсутствует. Автоматический повтор не "
                        "выполняется — решение за оператором."
                    ),
                },
            )
            outcomes.append({"attempt_id": attempt_id, "verdict": verdict})
        return outcomes

    def _watch_survived(self, item: dict[str, Any], row: dict[str, Any]) -> None:
        """Дождаться выжившего процесса в фоне и штатно его завершить."""

        def loop() -> None:
            pid = int(row.get("pid") or 0)
            identity = row.get("process_start_identity")
            while not self._stop.is_set() and process_registry.is_alive(pid, identity):
                self.db.renew_lease(item["attempt_id"], self.instance_id)
                self._stop.wait(POLL_SEC)
            marker = read_completed_marker(
                self.jobs.job_dir(item["job_id"], item["attempt_id"])
            )
            if marker is not None:
                self._finish_from_marker(item, marker)
            else:
                self.db.set_queue_state(
                    item["attempt_id"],
                    local_db.QUEUE_INTERRUPTED,
                    result={"outcome": "executor_interrupted",
                            "message": "процесс исчез без маркера"},
                )

        thread = threading.Thread(
            target=loop, name=f"watch-{item['attempt_id'][:8]}", daemon=True
        )
        thread.start()
        self._workers.append(thread)

    # ─── Исполнение попытки ──────────────────────────────────────────────────
    def run_attempt(self, item: dict[str, Any]) -> dict[str, Any]:
        job_id, attempt_id = item["job_id"], item["attempt_id"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        meta = self.jobs.load(job_id, attempt_id) or {}
        outbox = self._outbox(item)

        try:
            params = test_runner.validate_params(
                json.loads(item.get("params_json") or "{}"),
                max_total_sec=self.config.test_max_total_sec,
            )
        except test_runner.TestJobRejected as exc:
            self.db.set_queue_state(
                attempt_id, local_db.QUEUE_FAILED,
                result={"outcome": "rejected", "message": str(exc)},
            )
            self.jobs.update(job_id, attempt_id, local_state="rejected", error=str(exc))
            outbox.append("job_failed", {"code": "params_rejected", "reason": "error",
                                         "message": str(exc)})
            return {"ok": False, "reason": "rejected"}

        self.db.set_queue_state(attempt_id, local_db.QUEUE_RUNNING, executor_instance_id=self.instance_id)
        self.jobs.update(job_id, attempt_id, local_state="running", started_at=time.time())
        outbox.append("job_started", {"stage": "test_pipeline_v1"})
        outbox.append("stage_started",
                      {"stage": "test_pipeline_v1", "stage_index": 1, "stage_total": 1})

        started = time.time()
        last = {"processed": 0, "at": started}

        def on_progress(step: int, total: int, elapsed: float, message: str) -> None:
            now = time.time()
            window = max(1e-6, now - last["at"])
            delta = step - last["processed"]
            outbox.append(
                "stage_progress",
                {
                    "stage": "test_pipeline_v1",
                    "stage_index": 1,
                    "stage_total": 1,
                    "unit": "steps",
                    "processed": step,
                    "total": total,
                    "percent": round(step / total * 100, 1) if total else None,
                    "percent_reliable": bool(total),
                    "elapsed_sec": round(elapsed, 2),
                    "throughput_per_min": round(step / max(1e-6, now - started) * 60, 2),
                    "delta_5min": {"processed": delta, "window_sec": round(window, 2)},
                    "last_significant_event": message,
                    "completed_operations": step,
                },
            )
            last["processed"] = step
            last["at"] = now
            self.db.renew_lease(attempt_id, self.instance_id)

        def on_log(stream: str, level: str, line: str) -> None:
            outbox.append(
                "log_line",
                {"level": level, "stage": "test_pipeline_v1", "source": stream,
                 "message": line},
            )
            self._append_stream_file(job_dir, stream, line)

        def on_start(pid: int, fingerprint: str) -> None:
            try:
                pgid = os.getpgid(pid)
            except OSError:
                pgid = 0
            self.db.register_process(
                job_id=job_id,
                attempt_id=attempt_id,
                executor_instance_id=self.instance_id,
                pid=pid,
                process_start_identity=process_registry.process_start_time(pid),
                command_fingerprint=fingerprint,
                process_group_id=pgid,
            )
            self.jobs.update(
                job_id, attempt_id,
                pid=pid,
                command_fingerprint=fingerprint,
                process_start_time=process_registry.process_start_time(pid),
                process_group_id=pgid,
                process_status="running",
                executor_instance_id=self.instance_id,
            )

        outcome = test_runner.run_test_job(
            params=params,
            job_dir=job_dir,
            on_progress=on_progress,
            on_log=on_log,
            on_start=on_start,
        )
        write_completed_marker(job_dir, outcome)
        self.db.update_process(
            attempt_id,
            status="exited" if outcome.exit_code == 0 else "failed",
            exit_code=outcome.exit_code,
        )
        self.jobs.update(job_id, attempt_id, process_status="exited",
                         exit_code=outcome.exit_code)

        if self._was_cancelled(attempt_id):
            outbox.append("cancellation_received", {"job_id": job_id})
            outbox.append("job_failed", {"code": "cancelled", "reason": "cancelled",
                                         "message": "Отменено оператором"})
            self.jobs.update(job_id, attempt_id, local_state="cancelled")
            self.db.set_queue_state(attempt_id, local_db.QUEUE_CANCELLED,
                                    result={"outcome": "cancelled"})
            return {"ok": False, "reason": "cancelled"}

        if outcome.exit_code != 0:
            outbox.append("stage_completed",
                          {"stage": "test_pipeline_v1", "status": "error",
                           "duration_sec": round(outcome.duration_sec, 2)})
            outbox.append(
                "job_failed",
                {
                    "code": "test_process_failed",
                    "message": outcome.failed_message
                    or f"тестовый процесс вернул код {outcome.exit_code}",
                    "stage": "test_pipeline_v1",
                    "reason": "error",
                },
            )
            self.jobs.update(job_id, attempt_id, local_state="failed")
            self.db.set_queue_state(attempt_id, local_db.QUEUE_FAILED,
                                    result={"outcome": "failed",
                                            "exit_code": outcome.exit_code})
            return {"ok": False, "reason": "test_process_failed"}

        outbox.append(
            "stage_completed",
            {"stage": "test_pipeline_v1", "status": "done",
             "duration_sec": round(outcome.duration_sec, 2),
             "stdout_lines": outcome.stdout_lines,
             "stderr_lines": outcome.stderr_lines},
        )
        self._announce_artifacts(job_dir, outbox)
        self._package(item, meta, outbox)
        return {"ok": True, "outcome": outcome}

    def _finish_from_marker(self, item: dict[str, Any], marker: dict[str, Any]) -> None:
        """Доупаковать работу, завершённую до рестарта.

        Окно между выходом процесса и сборкой архива раньше стоило всей
        сделанной работы. Признак того, что работа сделана, — маркер с нулевым
        кодом возврата.
        """
        job_id, attempt_id = item["job_id"], item["attempt_id"]
        outbox = self._outbox(item)
        if int(marker.get("exit_code", 1)) != 0:
            self.jobs.update(job_id, attempt_id, local_state="failed")
            self.db.set_queue_state(
                attempt_id, local_db.QUEUE_FAILED,
                result={"outcome": "failed", "exit_code": marker.get("exit_code")},
            )
            return
        meta = self.jobs.load(job_id, attempt_id) or {}
        if meta.get("result_hash"):
            self.db.set_queue_state(attempt_id, local_db.QUEUE_FINISHED,
                                    result={"outcome": "already_packaged"})
            return
        self._announce_artifacts(self.jobs.job_dir(job_id, attempt_id), outbox)
        self._package(item, meta, outbox)

    def _package(
        self, item: dict[str, Any], meta: dict[str, Any], outbox: EventOutbox
    ) -> None:
        """Собрать архив результата на диск и объявить работу законченной локально.

        Архив материализуется ДО того, как кто-либо об этом узнает: падение
        между «сказал» и «собрал» иначе потеряло бы результат.
        """
        job_id, attempt_id = item["job_id"], item["attempt_id"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        archive = job_dir / "result" / f"{attempt_id}.tar.gz"
        package = meta.get("package") or {}
        manifest = package_io.build_result_package(
            dest_path=archive,
            job_dir=job_dir,
            job_id=job_id,
            attempt_id=attempt_id,
            project_id=meta.get("project_id", ""),
            version_id=meta.get("version_id"),
            worker_id=meta.get("worker_id", ""),
            worker_version=__version__,
            protocol_version=PROTOCOL_VERSION,
            manifest_version=int(package.get("manifest_version", 1) or 1),
            source_package_hash=package.get("sha256"),
            exit_code=int(meta.get("exit_code", 0) or 0),
        )
        self.jobs.update(
            job_id, attempt_id,
            local_state="completed_locally",
            completed_locally_at=time.time(),
            result_hash=manifest["archive"]["sha256"],
            result_size=manifest["archive"]["compressed_bytes"],
        )
        outbox.append(
            "job_completed_locally",
            {
                "result_hash": manifest["archive"]["sha256"],
                "result_size": manifest["archive"]["compressed_bytes"],
                "deferred_stages": [],
            },
        )
        self.db.set_queue_state(
            attempt_id, local_db.QUEUE_FINISHED,
            result={"outcome": "finished", "result_hash": manifest["archive"]["sha256"]},
        )

    def _announce_artifacts(self, job_dir: Path, outbox: EventOutbox) -> None:
        for artifact in sorted((job_dir / "result").rglob("*")):
            if not artifact.is_file():
                continue
            rel = artifact.relative_to(job_dir / "result").as_posix()
            outbox.append(
                "artifact_created",
                {
                    "name": rel,
                    "path_rel": f"result/{rel}",
                    "bytes": artifact.stat().st_size,
                    "sha256": package_io.sha256_file(artifact),
                },
            )

    @staticmethod
    def _append_stream_file(job_dir: Path, stream: str, line: str) -> None:
        """stdout/stderr пишутся В ФАЙЛЫ напрямую — этим владеет исполнитель.

        Агент их не держит и не перехватывает: его перезапуск не должен рвать
        трубы работающего процесса.
        """
        path = job_dir / "logs" / f"{'stderr' if stream == 'stderr' else 'stdout'}.log"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _outbox(self, item: dict[str, Any]) -> EventOutbox:
        job_dir = self.jobs.job_dir(item["job_id"], item["attempt_id"])
        meta = self.jobs.load(item["job_id"], item["attempt_id"]) or {}
        # Секреты вычищаются ПРИ ЗАПИСИ (I-12). Исполнителю известен только
        # токен попытки — worker-token ему не передают вовсе.
        return EventOutbox(
            job_dir / "events",
            secret_literals=(meta.get("execution_token") or "",),
        )

    def _emit(self, item: dict[str, Any], event_type: str, payload: dict[str, Any]) -> None:
        self._outbox(item).append(event_type, payload)

    def _was_cancelled(self, attempt_id: str) -> bool:
        row = self.db.queue_item(attempt_id)
        return bool(row and row.get("state") == local_db.QUEUE_CANCELLED)

    # ─── Локальные команды ───────────────────────────────────────────────────
    def drain_local_commands(self) -> list[dict[str, Any]]:
        """Исполнить накопившиеся локальные команды. Возвращает результаты."""
        results: list[dict[str, Any]] = []
        while True:
            command = self.db.claim_local_command()
            if command is None:
                return results
            result = self.execute_local_command(command)
            self.db.complete_local_command(command["local_command_id"], result)
            results.append(result)

    def execute_local_command(self, command: dict[str, Any]) -> dict[str, Any]:
        ctype = command.get("command_type")
        payload = json.loads(command.get("payload_json") or "{}")
        if ctype == "cancel_attempt":
            return self.cancel_attempt(
                job_id=str(payload.get("job_id") or command.get("job_id") or ""),
                attempt_id=str(payload.get("attempt_id") or command.get("attempt_id") or ""),
                grace_period_sec=float(payload.get("grace_period_sec", 30) or 30),
            )
        if ctype == "delete_attempt_data":
            return self.retention.delete_attempt(
                job_id=str(payload.get("job_id") or command.get("job_id") or ""),
                attempt_id=str(payload.get("attempt_id") or command.get("attempt_id") or ""),
                manual=True,
            )
        # Закрытый набор: неизвестное не исполняем никогда (I-10).
        return {"status": "error",
                "detail": {"outcome": "unsupported_command", "received": ctype}}

    def cancel_attempt(
        self, *, job_id: str, attempt_id: str, grace_period_sec: float = 30.0
    ) -> dict[str, Any]:
        """Остановить ТОЛЬКО свой процесс этой попытки (§10)."""
        queue = self.db.queue_item(attempt_id)
        job_dir = self.jobs.job_dir(job_id, attempt_id) if job_id else None
        marker = read_completed_marker(job_dir) if job_dir else None

        if queue and queue.get("state") == local_db.QUEUE_CANCELLED:
            return {"status": "ok", "detail": {"outcome": process_control.OUTCOME_ALREADY_CANCELLED}}
        if queue and queue.get("state") in (
            local_db.QUEUE_FINISHED, local_db.QUEUE_FAILED
        ):
            # Процесс успел закончиться: результат НЕ уничтожаем и попытку
            # задним числом отменённой не объявляем (§15.1).
            return {"status": "ok",
                    "detail": {"outcome": process_control.OUTCOME_ALREADY_COMPLETED,
                               "queue_state": queue.get("state")}}

        row = self.db.process_row(attempt_id)
        if row is None and marker is None:
            if queue is not None:
                self.db.set_queue_state(attempt_id, local_db.QUEUE_CANCELLED,
                                        result={"outcome": "not_running_locally"})
                self.jobs.update(job_id, attempt_id, local_state="cancelled")
            return {"status": "ok",
                    "detail": {"outcome": process_control.OUTCOME_NOT_RUNNING}}
        if row is None and marker is not None:
            return {"status": "ok",
                    "detail": {"outcome": process_control.OUTCOME_ALREADY_COMPLETED}}

        owned, why = process_control.verify_ownership(
            row, job_id=job_id, attempt_id=attempt_id
        )
        if not owned:
            if marker is not None:
                return {"status": "ok",
                        "detail": {"outcome": process_control.OUTCOME_ALREADY_COMPLETED}}
            if row and not process_registry.is_alive(
                int(row.get("pid") or 0), row.get("process_start_identity")
            ):
                self.db.set_queue_state(attempt_id, local_db.QUEUE_CANCELLED,
                                        result={"outcome": "not_running_locally"})
                self.jobs.update(job_id, attempt_id, local_state="cancelled")
                return {"status": "ok",
                        "detail": {"outcome": process_control.OUTCOME_NOT_RUNNING,
                                   "reason": why}}
            # Принадлежность не доказана — процесс НЕ трогаем ни при каких
            # условиях. Пусть лучше висит, чем убьём чужой (I-17).
            return {"status": "error",
                    "detail": {"outcome": process_control.OUTCOME_OWNERSHIP_MISMATCH,
                               "reason": why}}

        assert row is not None
        detail = process_control.terminate(row, grace_period_sec=grace_period_sec)
        if detail.get("outcome") == process_control.OUTCOME_CANCELLED:
            self.db.update_process(attempt_id, status="cancelled")
            self.db.set_queue_state(attempt_id, local_db.QUEUE_CANCELLED,
                                    result=detail)
            self.jobs.update(job_id, attempt_id, local_state="cancelled",
                             process_status="cancelled")
            item = queue or {"job_id": job_id, "attempt_id": attempt_id}
            self._emit(item, "cancellation_received",
                       {"job_id": job_id, "attempt_id": attempt_id})
            self._emit(item, "job_failed",
                       {"code": "cancelled", "reason": "cancelled",
                        "message": "Отменено оператором"})
            return {"status": "ok", "detail": detail}
        return {"status": "error", "detail": detail}
