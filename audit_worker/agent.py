"""WorkerAgent — супервизор агента.

Собирает вместе: регистрацию, heartbeat, long-poll задания, скачивание и
проверку пакета, запуск безопасного тестового процесса, дисковый outbox
событий, отправку событий пакетами и возобновляемую загрузку результата.

Три свойства, ради которых всё и затевалось:
  * конвейер не имеет вызовов к центру на критическом пути — он пишет в
    outbox, а сеть разгребает отдельный поток (I-01);
  * `seq` монотонный и переживает рестарт — дедуп на центре работает (I-04);
  * архив результата материализуется на диск ДО уведомления центра, а
    удаление невозможно, пока центр не подтвердил приём (I-08).
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Optional

from audit_worker import (
    PROTOCOL_VERSION,
    __version__,
    package_io,
    process_registry,
    reconciliation,
    test_runner,
)
from audit_worker.client import (
    AttemptSupersededError,
    CenterClient,
    CenterError,
    SequenceGapError,
    backoff_delays,
)
from audit_worker.config import WorkerConfig
from audit_worker.event_outbox import EventOutbox
from audit_worker.heartbeat import HeartbeatClient
from audit_worker.job_poller import JobPullClient
from audit_worker.local_store import LocalJobStore, WorkerStateStore
from audit_worker.process_registry import ProcessRegistry
from audit_worker.resource_monitor import ResourceMonitor
from audit_worker.uploader import upload_result


def _write_completed_marker(job_dir: Path, outcome: "test_runner.RunOutcome") -> None:
    """Маркер «процесс отработал» рядом с результатом.

    Различает два неотличимых по pid состояния: процесс завершился штатно
    против «процесс исчез». Без маркера рестарт агента не может понять,
    надо ли что-то доделывать.
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


class UploadDeferred(RuntimeError):
    """Результат готов, но канал недоступен: передача отложена, НЕ провал."""


def _log(message: str) -> None:
    print(f"[audit-worker {time.strftime('%H:%M:%S')}] {message}", flush=True)


class WorkerAgent:
    def __init__(self, config: WorkerConfig, identity: dict[str, Any]):
        self.config = config
        self.identity = identity
        self.worker_id: str = identity["worker_id"]
        self.instance_id: str = identity["instance_id"]
        self.token: str = identity["token"]

        config.ensure_dirs()
        self.state_store = WorkerStateStore(config.state_path, config.token_path)
        self.jobs = LocalJobStore(config.jobs_dir)
        self.registry = ProcessRegistry(config.runtime_dir)
        self.monitor = ResourceMonitor(config.root, configured_max_slots=config.max_slots)

        self.client = CenterClient(
            config.dispatcher_url,
            token=self.token,
            worker_id=self.worker_id,
            instance_id=self.instance_id,
            timeout=config.request_timeout_sec,
            verify=config.verify_tls,
            transport=config.transport,
        )
        self.poller = JobPullClient(self.client, wait_sec=config.poll_wait_sec)
        self.heartbeat = HeartbeatClient(
            self.client,
            interval_sec=config.heartbeat_interval_sec,
            build_payload=self._heartbeat_payload,
            on_response=self._on_heartbeat_response,
            on_error=lambda exc: _log(f"heartbeat не прошёл: {exc}"),
        )

        self._stop = threading.Event()
        self._sender_thread: Optional[threading.Thread] = None
        self._active: dict[str, dict[str, Any]] = {}     # job_id → runtime-контекст
        self._active_lock = threading.Lock()
        self._cancelled: set[str] = set()

    # ─── Жизненный цикл ──────────────────────────────────────────────────────
    def run_forever(self, *, max_jobs: Optional[int] = None) -> None:
        """Основной цикл. max_jobs ограничивает число заданий (для smoke-прогона)."""
        self._startup_reconcile()
        self.heartbeat.start()
        self._start_sender()
        _log(f"агент запущен: worker_id={self.worker_id}, центр={self.config.dispatcher_url}")

        done = 0
        delays = backoff_delays()
        try:
            while not self._stop.is_set():
                try:
                    self._drain_commands()
                    self._deliver_pending_results()
                    assignment = self.poller.poll(
                        free_slots=self._free_slots(),
                        compressions=["gzip", "none"],
                    )
                    delays = backoff_delays()
                except Exception as exc:  # noqa: BLE001 — центр недоступен
                    _log(f"опрос заданий не удался: {exc}")
                    self._stop.wait(next(delays))
                    continue

                if assignment is None:
                    self._stop.wait(1.0)
                    continue

                self.execute_job(assignment)
                done += 1
                if max_jobs is not None and done >= max_jobs:
                    _log(f"выполнено заданий: {done} — останавливаюсь по max_jobs")
                    break
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        self._stop.set()
        self.heartbeat.stop()
        if self._sender_thread is not None:
            self._sender_thread.join(timeout=10)
        self._flush_all_outboxes()
        self.client.close()

    def _startup_reconcile(self) -> None:
        survived = reconciliation.survived_processes(self.jobs, self.registry)
        for meta in survived:
            _log(
                f"задание {meta['job_id'][:8]} пережило рестарт агента "
                f"(процессы живы) — не трогаю"
            )
        self._package_finished_jobs()
        self._report_lost_processes()
        verdict = reconciliation.reconcile(
            self.client,
            self.jobs,
            instance_id=self.instance_id,
            previous_instance_id=self.identity.get("previous_instance_id"),
            registry=self.registry,
        )
        if verdict.get("error"):
            _log(f"сверка с центром недоступна ({verdict['error']}) — продолжаю локально")
            return
        for item in verdict.get("jobs", []):
            _log(
                f"сверка: {item['job_id'][:8]} → {item['action']} "
                f"(центр: {item.get('center_state')}, ждёт seq {item['expected_next_seq']})"
            )
            # Приём подтверждён — только теперь у пакета появляется срок
            # хранения. Пока retention_until пуст, удалять его запрещено (I-08).
            if item.get("result_accepted") and item.get("retention_until"):
                self.jobs.update(
                    item["job_id"], item["attempt_id"],
                    retention_until=item["retention_until"],
                )
            if item["action"] == "stop_superseded":
                self.registry.terminate_job(item["job_id"], item["attempt_id"])
                self.jobs.update(
                    item["job_id"], item["attempt_id"], local_state="superseded"
                )
            elif item["action"] == "upload_result":
                self._resume_upload(item["job_id"], item["attempt_id"])

    def _package_finished_jobs(self) -> None:
        """Доупаковать работу, завершённую до рестарта агента.

        Окно между выходом процесса и сборкой архива раньше стоило всей
        сделанной работы: задание попадало в «процесс потерян» и объявлялось
        провалом. Признак того, что работа сделана, — `completed.marker`
        с нулевым кодом возврата.
        """
        for meta in reconciliation.finished_but_unpackaged(self.jobs, self.registry):
            job_id, attempt_id = meta["job_id"], meta["attempt_id"]
            job_dir = self.jobs.job_dir(job_id, attempt_id)
            outbox = EventOutbox(
                job_dir / "events",
                secret_literals=(self.token, meta.get("execution_token") or ""),
            )
            ctx = {
                "job_id": job_id,
                "attempt_id": attempt_id,
                "execution_token": meta.get("execution_token"),
                "outbox": outbox,
                "stage": "package",
            }
            assignment = {
                "project_id": meta.get("project_id", ""),
                "version_id": meta.get("version_id"),
                "package": meta.get("package") or {"manifest_version": 1},
            }
            _log(
                f"задание {job_id[:8]}: процесс отработал до рестарта — "
                f"собираю результат и отправляю"
            )
            try:
                self._package_and_upload(assignment, ctx, job_dir)
            except UploadDeferred:
                pass    # архив на диске, дошлём позже
            except Exception as exc:  # noqa: BLE001 — одно задание не роняет агента
                _log(f"не удалось собрать результат {job_id[:8]}: {exc}")

    def _report_lost_processes(self) -> None:
        """Сообщить о заданиях, чей процесс не пережил рестарт агента.

        На этапе 0 возобновления нет (реальный конвейер и `resume` вне объёма),
        поэтому честный исход — провал с явной причиной, а не вечное `running`.
        """
        for meta in reconciliation.lost_processes(self.jobs, self.registry):
            job_id, attempt_id = meta["job_id"], meta["attempt_id"]
            job_dir = self.jobs.job_dir(job_id, attempt_id)
            outbox = EventOutbox(
                job_dir / "events",
                secret_literals=(self.token, meta.get("execution_token") or ""),
            )
            outbox.append(
                "job_failed",
                {
                    "code": "process_lost_after_restart",
                    "message": "Процесс задания не пережил рестарт агента",
                    "reason": "error",
                    "pid": meta.get("pid"),
                },
            )
            self.jobs.update(
                job_id, attempt_id,
                local_state="failed",
                process_status="lost",
                error="process_lost_after_restart",
            )
            _log(f"задание {job_id[:8]}: процесс потерян при рестарте — отмечено провалом")
            self._flush_outbox(
                {
                    "job_id": job_id,
                    "attempt_id": attempt_id,
                    "outbox": outbox,
                    "execution_token": meta.get("execution_token"),
                }
            )

    # ─── Слоты и heartbeat ───────────────────────────────────────────────────
    def _free_slots(self) -> int:
        with self._active_lock:
            active = len(self._active)
        snapshot = self.monitor.snapshot(
            active_jobs=active, live_processes=self.registry.live_count()
        )
        return int(snapshot["slots"]["calculated_free"])

    def _heartbeat_payload(self) -> dict[str, Any]:
        with self._active_lock:
            active = [
                {
                    "job_id": ctx["job_id"],
                    "attempt_id": ctx["attempt_id"],
                    "project_id": ctx.get("project_id", ""),
                    "stage": ctx.get("stage", ""),
                    "last_event_seq": ctx["outbox"].last_written_seq,
                    "started_at": ctx.get("started_at"),
                }
                for ctx in self._active.values()
            ]
        snapshot = self.monitor.snapshot(
            active_jobs=len(active), live_processes=self.registry.live_count()
        )
        warnings: list[dict[str, Any]] = []
        unconfirmed = self.jobs.retention_unconfirmed()
        if unconfirmed:
            warnings.append(
                {
                    "code": "retention_unconfirmed",
                    "severity": "warn",
                    "count": len(unconfirmed),
                    "message": "Центр не подтвердил приём — автоматическое удаление запрещено",
                }
            )
        if snapshot["slots"]["calculated_free"] == 0 and not active:
            warnings.append(
                {
                    "code": "no_free_slots",
                    "severity": "warn",
                    "message": f"Слотов нет: ограничивает {snapshot['slots']['binding_constraint']}",
                }
            )
        return {
            "instance_id": self.instance_id,
            "sent_at": time.time(),
            "worker_state": "busy" if active else "idle",
            "configured_max_slots": self.config.max_slots,
            "calculated_free_slots": int(snapshot["slots"]["calculated_free"]),
            "active_jobs": active,
            "resource_snapshot": snapshot,
            "warnings": warnings,
        }

    def _on_heartbeat_response(self, response: dict[str, Any]) -> None:
        for update in response.get("retention_updates", []):
            if update.get("retention_until") is None:
                continue
            self.jobs.update(
                update["job_id"],
                update["attempt_id"],
                retention_until=update["retention_until"],
            )

    # ─── Команды ─────────────────────────────────────────────────────────────
    def _drain_commands(self) -> None:
        try:
            payload = self.client.get_commands()
        except Exception:  # noqa: BLE001 — команды подождут до следующего круга
            return
        for command in payload.get("commands", []):
            ctype = command.get("command_type")
            result: dict[str, Any]
            if ctype == "cancel_job":
                job_id = (command.get("payload") or {}).get("job_id")
                attempt_id = (command.get("payload") or {}).get("attempt_id")
                self._cancelled.add(job_id)
                killed = self.registry.terminate_job(job_id, attempt_id or "")
                result = {"status": "ok", "detail": {"terminated": killed}}
            elif ctype in ("drain", "undrain"):
                result = {"status": "ok", "detail": {"applied": ctype}}
            else:
                # Закрытый enum: неизвестное не исполняем (инвариант I-11).
                result = {
                    "status": "error",
                    "detail": {"code": "unsupported_command", "received": ctype},
                }
            try:
                self.client.ack_command(command["command_id"], result)
            except Exception:  # noqa: BLE001 — повторим при следующем опросе
                pass

    # ─── Исполнение задания ──────────────────────────────────────────────────
    def execute_job(self, assignment: dict[str, Any]) -> dict[str, Any]:
        job_id = assignment["job_id"]
        attempt_id = assignment["attempt_id"]
        token = assignment["execution_token"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        meta = self.jobs.create(assignment)

        outbox = EventOutbox(
            job_dir / "events", secret_literals=(self.token, token)
        )
        ctx = {
            "job_id": job_id,
            "attempt_id": attempt_id,
            "project_id": assignment.get("project_id", ""),
            "execution_token": token,
            "outbox": outbox,
            "stage": "download",
            "started_at": time.time(),
        }
        with self._active_lock:
            self._active[job_id] = ctx

        try:
            self._download_and_verify(assignment, ctx, job_dir)
            self._accept(assignment, ctx)
            outcome = self._run(assignment, ctx, job_dir)
            if outcome["ok"]:
                self._package_and_upload(assignment, ctx, job_dir)
            return outcome
        except AttemptSupersededError:
            _log(f"задание {job_id[:8]}: попытка отозвана — останавливаюсь")
            self.registry.terminate_job(job_id, attempt_id)
            self.jobs.update(job_id, attempt_id, local_state="superseded")
            return {"ok": False, "reason": "superseded"}
        except UploadDeferred as exc:
            # Работа сделана, потерян только канал. Задание остаётся
            # completed_locally и попадёт в очередь досылки.
            return {"ok": False, "reason": "upload_deferred", "detail": str(exc)}
        except Exception as exc:  # noqa: BLE001 — одно задание не роняет агента
            _log(f"задание {job_id[:8]} завершилось ошибкой: {exc}")
            outbox.append(
                "job_failed",
                {"code": "worker_exception", "message": str(exc), "reason": "error"},
            )
            self.jobs.update(job_id, attempt_id, local_state="failed", error=str(exc))
            self._flush_outbox(ctx)
            return {"ok": False, "reason": str(exc)}
        finally:
            with self._active_lock:
                self._active.pop(job_id, None)
            self._flush_outbox(ctx)

    def _download_and_verify(
        self, assignment: dict[str, Any], ctx: dict[str, Any], job_dir: Path
    ) -> None:
        package = assignment["package"]
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        suffix = {"gzip": ".tar.gz", "zstd": ".tar.zst"}.get(
            package.get("compression", "gzip"), ".tar"
        )
        dest = job_dir / "source" / f"{package['package_id']}{suffix}"

        self.jobs.update(job_id, attempt_id, local_state="downloading")
        ctx["stage"] = "download"
        self.client.download_source(job_id, dest, ctx["execution_token"])

        try:
            info = package_io.verify_and_unpack(
                archive=dest,
                expected_sha256=package["sha256"],
                work_dir=job_dir / "work",
                compression=package.get("compression"),
            )
        except package_io.BundleError as exc:
            ctx["outbox"].append("source_invalid", {"message": str(exc)})
            self._flush_outbox(ctx)
            raise

        ctx["outbox"].append(
            "source_verified",
            {
                "sha256_ok": True,
                "files": info["files"],
                "bytes": info["bytes"],
                "manifest_version": info["manifest"].get("manifest_version"),
            },
        )
        self.jobs.update(job_id, attempt_id, local_state="verified")

    def _accept(self, assignment: dict[str, Any], ctx: dict[str, Any]) -> None:
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        self.client.accept_job(
            job_id,
            {
                "attempt_id": attempt_id,
                "accepted_at": time.time(),
                "source_verified": {"sha256_ok": True, "manifest_version": 1},
                "planned_stages": ["test_pipeline_v1"],
            },
            ctx["execution_token"],
        )
        ctx["outbox"].append("job_accepted", {"planned_stages": ["test_pipeline_v1"]})
        self._flush_outbox(ctx)

    def _run(
        self, assignment: dict[str, Any], ctx: dict[str, Any], job_dir: Path
    ) -> dict[str, Any]:
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        outbox: EventOutbox = ctx["outbox"]

        try:
            params = test_runner.validate_params(
                assignment.get("params") or {},
                max_total_sec=self.config.test_max_total_sec,
            )
        except test_runner.TestJobRejected as exc:
            self.client.reject_job(
                job_id, {"attempt_id": attempt_id, "reason": str(exc)},
                ctx["execution_token"],
            )
            self.jobs.update(job_id, attempt_id, local_state="rejected", error=str(exc))
            return {"ok": False, "reason": str(exc)}

        ctx["stage"] = "test_pipeline_v1"
        self.jobs.update(job_id, attempt_id, local_state="running", started_at=time.time())
        outbox.append("job_started", {"stage": "test_pipeline_v1"})
        outbox.append(
            "stage_started",
            {"stage": "test_pipeline_v1", "stage_index": 1, "stage_total": 1},
        )

        started = time.time()
        last_progress = {"processed": 0, "at": started}

        def on_progress(step: int, total: int, elapsed: float, message: str) -> None:
            now = time.time()
            window = max(1e-6, now - last_progress["at"])
            delta = step - last_progress["processed"]
            outbox.append(
                "stage_progress",
                {
                    "stage": "test_pipeline_v1",
                    "stage_index": 1,
                    "stage_total": 1,
                    "unit": "steps",
                    "processed": step,
                    "total": total,
                    # percent_reliable=True только потому, что total достоверно
                    # известен от самого процесса. Выдуманного процента нет.
                    "percent": round(step / total * 100, 1) if total else None,
                    "percent_reliable": bool(total),
                    "elapsed_sec": round(elapsed, 2),
                    "throughput_per_min": round(step / max(1e-6, now - started) * 60, 2),
                    "delta_5min": {"processed": delta, "window_sec": round(window, 2)},
                    "last_significant_event": message,
                    "completed_operations": step,
                },
            )
            last_progress["processed"] = step
            last_progress["at"] = now

        def on_log(stream: str, level: str, line: str) -> None:
            # stdout и stderr различаются полем `source` — они читаются
            # раздельными потоками и не сливаются в один.
            outbox.append(
                "log_line",
                {"level": level, "stage": "test_pipeline_v1", "source": stream,
                 "message": line},
            )

        def on_pid(pid: int) -> None:
            self.jobs.update(job_id, attempt_id, pid=pid)

        def on_start(pid: int, fingerprint: str) -> None:
            self.registry.register(
                pid, job_id=job_id, attempt_id=attempt_id,
                command_fingerprint=fingerprint,
            )
            self.jobs.update(
                job_id, attempt_id,
                pid=pid,
                command_fingerprint=fingerprint,
                process_start_time=process_registry.process_start_time(pid),
                process_status="running",
            )

        outcome = test_runner.run_test_job(
            params=params,
            job_dir=job_dir,
            on_progress=on_progress,
            on_log=on_log,
            on_pid=on_pid,
            on_start=on_start,
            cancel_check=lambda: job_id in self._cancelled,
        )
        self.registry.prune_dead()
        # Маркер завершения: по нему после рестарта агента видно, что процесс
        # ОТРАБОТАЛ, а не «исчез». Без него мёртвый pid неотличим от убитого.
        _write_completed_marker(job_dir, outcome)
        self.jobs.update(job_id, attempt_id, process_status="exited",
                         exit_code=outcome.exit_code)

        if job_id in self._cancelled:
            outbox.append("cancellation_received", {"job_id": job_id})
            outbox.append("job_failed", {"code": "cancelled", "reason": "cancelled",
                                         "message": "Отменено оператором"})
            self.jobs.update(job_id, attempt_id, local_state="cancelled")
            self._flush_outbox(ctx)
            return {"ok": False, "reason": "cancelled"}

        if outcome.exit_code != 0:
            outbox.append(
                "stage_completed",
                {"stage": "test_pipeline_v1", "status": "error",
                 "duration_sec": round(outcome.duration_sec, 2)},
            )
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
            self._flush_outbox(ctx)
            return {"ok": False, "reason": "test_process_failed"}

        outbox.append(
            "stage_completed",
            {"stage": "test_pipeline_v1", "status": "done",
             "duration_sec": round(outcome.duration_sec, 2),
             "stdout_lines": outcome.stdout_lines,
             "stderr_lines": outcome.stderr_lines},
        )
        # Артефакты объявляются явно: центр должен знать, ЧТО создано, ещё до
        # того, как получит архив.
        for artifact in sorted((job_dir / "result").rglob("*")):
            if not artifact.is_file():
                continue
            outbox.append(
                "artifact_created",
                {
                    "name": artifact.relative_to(job_dir / "result").as_posix(),
                    "path_rel": "result/" + artifact.relative_to(job_dir / "result").as_posix(),
                    "bytes": artifact.stat().st_size,
                    "sha256": package_io.sha256_file(artifact),
                },
            )
        return {"ok": True, "outcome": outcome}

    def _package_and_upload(
        self, assignment: dict[str, Any], ctx: dict[str, Any], job_dir: Path
    ) -> None:
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        outbox: EventOutbox = ctx["outbox"]
        ctx["stage"] = "package"

        archive = job_dir / "result" / f"{attempt_id}.tar.gz"
        # ВАЖНО: архив пишется на диск ДО уведомления центра — иначе падение
        # между «сказал» и «собрал» потеряло бы результат.
        meta = self.jobs.load(job_id, attempt_id) or {}
        manifest = package_io.build_result_package(
            dest_path=archive,
            job_dir=job_dir,
            job_id=job_id,
            attempt_id=attempt_id,
            project_id=assignment.get("project_id", ""),
            version_id=assignment.get("version_id"),
            worker_id=self.worker_id,
            worker_version=__version__,
            protocol_version=PROTOCOL_VERSION,
            manifest_version=int(assignment["package"].get("manifest_version", 1)),
            source_package_hash=assignment["package"].get("sha256"),
            exit_code=int(meta.get("exit_code", 0) or 0),
        )
        result_hash = manifest["archive"]["sha256"]
        result_size = manifest["archive"]["compressed_bytes"]
        self.jobs.update(
            job_id,
            attempt_id,
            local_state="completed_locally",
            completed_locally_at=time.time(),
            result_hash=result_hash,
            result_size=result_size,
        )
        outbox.append(
            "job_completed_locally",
            {"result_hash": result_hash, "result_size": result_size,
             "deferred_stages": []},
        )
        self._flush_outbox(ctx)

        ctx["stage"] = "upload"
        outbox.append("result_upload_started", {"result_size": result_size})
        self.jobs.update(job_id, attempt_id, local_state="uploading")

        def on_chunk(sent: int, total: int, bytes_sent: int) -> None:
            outbox.append(
                "result_upload_progress",
                {"chunk_idx": sent - 1, "chunks_total": total, "bytes_sent": bytes_sent},
            )

        try:
            response = upload_result(
                client=self.client,
                job_id=job_id,
                attempt_id=attempt_id,
                archive=archive,
                execution_token=ctx["execution_token"],
                uploads_dir=job_dir / "uploads",
                on_progress=on_chunk,
            )
        except Exception as exc:  # noqa: BLE001 — включая обрыв связи
            # Аудит УЖЕ выполнен, архив лежит на диске. Неудачная передача —
            # это не провал задания: события job_failed здесь быть не должно
            # (по §10.3 перевести result_uploading → failed вправе только центр
            # и только по исчерпании попыток). Остаёмся в completed_locally,
            # результат досылается позже — сам или по reconcile.
            _log(
                f"задание {job_id[:8]}: результат готов, но передать не вышло "
                f"({exc}) — оставляю на диске, дошлю позже"
            )
            self.jobs.update(
                job_id, attempt_id,
                local_state="completed_locally",
                upload_error=str(exc),
            )
            self._flush_outbox(ctx)
            raise UploadDeferred(str(exc)) from exc

        retention_until = response.get("retention_until")
        # Центр мог принять архив, но НЕ принять результат (валидация не
        # прошла). Тогда «finished» — ложь: пакет остаётся на воркере без
        # подтверждения, а `finished` исключает его и из досылки, и из
        # collect_known_jobs — задание становилось невидимым навсегда.
        accepted = retention_until is not None
        self.jobs.update(
            job_id,
            attempt_id,
            local_state="finished" if accepted else "completed_locally",
            retention_until=retention_until,
            center_state=response.get("state"),
            center_validation=response.get("validation"),
        )
        outbox.append(
            "job_completed",
            {"center_state": response.get("state"), "retention_until": retention_until},
        )
        self._flush_outbox(ctx)
        if not accepted:
            _log(
                f"задание {job_id[:8]}: центр не подтвердил приём "
                f"(состояние «{response.get('state')}») — результат остаётся на "
                f"воркере как retention_unconfirmed и попадёт в следующую сверку"
            )

    def _deliver_pending_results(self) -> None:
        """Дослать результаты, оставшиеся на диске после обрыва передачи.

        Без этого прохода готовый пакет ждал бы рестарта агента: сверка
        выполняется только на старте, а сам цикл о нём бы не вспомнил.
        """
        for meta in self.jobs.iter_all():
            if meta.get("local_state") != "completed_locally":
                continue
            if not meta.get("result_hash"):
                continue
            self._resume_upload(meta["job_id"], meta["attempt_id"])

    def _resume_upload(self, job_id: str, attempt_id: str) -> None:
        meta = self.jobs.load(job_id, attempt_id)
        if not meta or not meta.get("result_hash"):
            return
        _log(f"досылаю результат задания {job_id[:8]} после перерыва")
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        archive = job_dir / "result" / f"{attempt_id}.tar.gz"
        if not archive.is_file():
            return
        # Сначала события, потом архив: центр должен узнать о завершении
        # раньше, чем получит пакет. Порядок восстановим и на его стороне, но
        # правильный порядок дешевле, чем догон.
        pending = EventOutbox(
            job_dir / "events",
            secret_literals=(self.token, meta.get("execution_token") or ""),
        )
        self._flush_outbox({
            "job_id": job_id, "attempt_id": attempt_id, "outbox": pending,
            "execution_token": meta.get("execution_token"),
        })
        try:
            response = upload_result(
                client=self.client,
                job_id=job_id,
                attempt_id=attempt_id,
                archive=archive,
                execution_token=meta.get("execution_token", ""),
                uploads_dir=job_dir / "uploads",
            )
            self.jobs.update(
                job_id, attempt_id, local_state="finished",
                retention_until=response.get("retention_until"),
                center_state=response.get("state"),
            )
            # Хвост журнала не должен отличаться от обычного пути: иначе в
            # истории задания просто нет отметки о завершении.
            outbox = EventOutbox(
                job_dir / "events",
                secret_literals=(self.token, meta.get("execution_token") or ""),
            )
            outbox.append(
                "job_completed",
                {"center_state": response.get("state"),
                 "retention_until": response.get("retention_until")},
            )
            self._flush_outbox({
                "job_id": job_id, "attempt_id": attempt_id, "outbox": outbox,
                "execution_token": meta.get("execution_token"),
            })
        except Exception as exc:  # noqa: BLE001 — досылка повторится позже
            _log(f"досылка не удалась: {exc}")

    # ─── Отправка событий ────────────────────────────────────────────────────
    def _start_sender(self) -> None:
        self._sender_thread = threading.Thread(
            target=self._sender_loop, name="event-sender", daemon=True
        )
        self._sender_thread.start()

    def _sender_loop(self) -> None:
        while not self._stop.is_set():
            self._flush_all_outboxes()
            self._stop.wait(self.config.event_flush_interval_sec)

    def _flush_all_outboxes(self) -> None:
        with self._active_lock:
            contexts = list(self._active.values())
        for ctx in contexts:
            try:
                self._flush_outbox(ctx)
            except Exception:  # noqa: BLE001 — отправка повторится
                continue

    def _flush_outbox(self, ctx: dict[str, Any]) -> None:
        """Отправить накопленное. При обрыве просто выходим — outbox копит дальше."""
        outbox: EventOutbox = ctx["outbox"]
        while outbox.has_pending and not self._stop.is_set():
            batch = outbox.pending_batch(limit=self.config.event_batch_max)
            if not batch:
                return
            try:
                response = self.client.post_events(
                    ctx["job_id"], ctx["attempt_id"], batch[0]["seq"], batch,
                    ctx["execution_token"],
                )
            except SequenceGapError as gap:
                outbox.rewind_to(gap.expected_seq)
                continue
            except AttemptSupersededError:
                raise
            except CenterError as exc:
                # Ошибка ПРОТОКОЛА (422/4xx) — это дефект, а не обрыв связи.
                # Молча проглатывать её нельзя: события просто перестанут
                # доходить, и снаружи это выглядит как «воркер молчит».
                if exc.status and exc.status < 500 and exc.status != 429:
                    _log(f"отправка событий отвергнута центром: {exc}")
                ctx["last_send_error"] = str(exc)
                return
            except Exception as exc:  # noqa: BLE001 — нет связи: копим дальше (I-01)
                ctx["last_send_error"] = str(exc)
                return
            reconnected = bool(ctx.pop("last_send_error", None))
            if reconnected:
                # Связь была потеряна и восстановилась — центр должен узнать
                # об этом явно, а не догадываться по возобновлению потока.
                outbox.append(
                    "worker_reconnected",
                    {
                        "pending_events": outbox.last_written_seq - outbox.last_acked_seq,
                        "instance_id": self.instance_id,
                    },
                )
            outbox.ack(int(response.get("last_seen_seq", 0)))
            # Досылаем в этом же проходе: иначе только что добавленное
            # worker_reconnected повисло бы неотправленным до следующего цикла.
            if len(batch) < self.config.event_batch_max and not reconnected:
                return
