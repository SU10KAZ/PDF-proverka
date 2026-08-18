"""audit-worker-agent — СЕТЕВОЙ агент. Процессы аудита не запускает.

Собирает вместе: регистрацию, heartbeat, long-poll задания, скачивание и
проверку пакета, дисковый outbox событий, отправку событий пакетами,
возобновляемую загрузку результата и приём команд центра.

Чего здесь НЕТ с этапа 3.5 и не должно появиться: запуска тестового процесса.
Работу выполняет отдельный процесс `python -m audit_worker executor`, а агент
ставит попытку в локальную очередь worker.db и НАБЛЮДАЕТ. Причина ровно одна:
пока запуск жил здесь, перезапуск сетевого агента убивал сделанную работу
(I-02), а второй запуск агента порождал второй процесс аудита (I-03).

Три свойства, ради которых всё и затевалось:
  * конвейер не имеет вызовов к центру на критическом пути — он пишет в
    outbox, а сеть разгребает отдельный поток (I-01);
  * `seq` монотонный и переживает рестарт — дедуп на центре работает (I-04);
  * архив результата материализуется на диск ДО уведомления центра, а
    удаление невозможно, пока центр не подтвердил приём (I-08).
"""
from __future__ import annotations

import errno
import json
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Optional

import httpx

from audit_worker import (
    PROTOCOL_VERSION,
    __version__,
    diagnostics,
    local_db,
    package_io,
    reconciliation,
)
from audit_worker import slots as worker_slots
from audit_worker.client import (
    AttemptSupersededError,
    ControlContextUnavailable,
    CenterClient,
    CenterError,
    ResultRejectedError,
    SequenceGapError,
    backoff_delays,
)
from audit_worker.config import WorkerConfig, data_plane_tls_verify
from audit_worker.event_outbox import EventOutbox
from audit_worker.heartbeat import HeartbeatClient
from audit_worker.job_poller import JobPullClient
from audit_worker.local_store import LocalJobStore, WorkerStateStore
from audit_worker.providers.manager import ProviderManager
from audit_worker.resource_monitor import ResourceMonitor
from audit_worker.retention import RetentionManager
from audit_worker.uploader import upload_result


#: Как часто агент спрашивает центр о командах в цикле ожидания.
_COMMAND_POLL_SEC = 1.0

#: Как часто агент сверяет своё состояние с центром на ходу (не только на
#: старте). Пять минут — компромисс: реже, чем heartbeat, потому что сверка
#: тяжелее, но достаточно часто, чтобы застрявший из-за потерянного ответа
#: слот освобождался сам, без перезапуска агента.
RECONCILE_INTERVAL_SEC = 300.0

#: Состояния ДОСТАВКИ результата, после которых повтор отправки запрещён.
#: Ось доставки намеренно отделена от оси ХРАНЕНИЯ: пакет остаётся на диске
#: положенный срок в обоих случаях, но досылать его больше нечего и некуда.
DELIVERY_ACKNOWLEDGED = "acknowledged"
DELIVERY_REJECTED_PERMANENTLY = "rejected_permanently"
TERMINAL_DELIVERY_STATES = frozenset({DELIVERY_ACKNOWLEDGED, DELIVERY_REJECTED_PERMANENTLY})


#: Локальные отказы, которые повтором не лечатся: диск полон, прав нет,
#: путь занят каталогом, слишком длинное имя. Всё остальное (обрывы TCP/TLS,
#: таймауты, временная недоступность) остаётся повторяемым.
_PERMANENT_LOCAL_ERRNOS = frozenset({
    errno.EACCES, errno.EPERM, errno.ENOSPC, errno.EROFS, errno.EDQUOT,
    errno.EISDIR, errno.ENAMETOOLONG, errno.ENOTDIR, errno.EMFILE, errno.ENFILE,
})


class UploadDeferred(RuntimeError):
    """Результат готов, но канал недоступен: передача отложена, НЕ провал."""


def _routing_binding(meta: dict[str, Any]) -> dict[str, str]:
    """Маршрут и ревизия попытки из СОХРАНЁННЫХ метаданных.

    Единственный источник, переживающий рестарт агента: выданное задание
    живёт в памяти транспорта, а досылка случается именно после перерыва.
    """
    params = meta.get("params") if isinstance(meta.get("params"), dict) else {}
    routing = params.get("routing_plan") if isinstance(params.get("routing_plan"), dict) else {}
    return {
        "routing_plan_hash": str(routing.get("routing_plan_hash") or ""),
        "pipeline_revision": str(params.get("pipeline_revision") or ""),
    }


def delivery_is_terminal(meta: dict[str, Any]) -> bool:
    """Доставка этого результата завершена — повторять её нельзя.

    Помимо явной отметки признаём терминальным и старый признак приёма
    (`retention_until`): попытки, подтверждённые до появления этого поля,
    не должны получить второй круг досылки после обновления воркера.
    """
    if str(meta.get("delivery_state") or "") in TERMINAL_DELIVERY_STATES:
        return True
    return meta.get("retention_until") is not None


def _log(message: str) -> None:
    print(f"[audit-worker {time.strftime('%H:%M:%S')}] {message}", flush=True)


def _heartbeat_reason_code(state: str) -> str:
    """Translate legacy center-state names to 12E operational reason codes."""
    normalized = str(state).lower()
    if "dns" in normalized:
        return "DNS_FAILED"
    if "tls" in normalized:
        return "TLS_FAILED"
    if "auth" in normalized or "token" in normalized:
        return "MTLS_AUTH_FAILED"
    if "protocol" in normalized:
        return "PROTOCOL_MISMATCH"
    if "unreachable" in normalized or "offline" in normalized:
        return "GRPC_UNAVAILABLE"
    return "UNKNOWN"


class WorkerAgent:
    def __init__(self, config: WorkerConfig, identity: dict[str, Any]):
        from audit_worker.config import validate_control_transport

        validate_control_transport(config)
        self.config = config
        self.identity = identity
        self.worker_id: str = identity["worker_id"]
        self.instance_id: str = identity["instance_id"]
        self.token: str = identity["token"]

        config.ensure_dirs()
        self.state_store = WorkerStateStore(config.state_path, config.token_path)
        self.jobs = LocalJobStore(config.jobs_dir)
        # Общий стык с исполнителем. Агент сюда ПИШЕТ задания и команды, но
        # процессами не владеет: реестр процессов ведёт исполнитель, агент его
        # только читает.
        self.db = local_db.LocalDB(config.local_db_path)
        self.registry = reconciliation.LocalDbProcessView(self.db)
        self.retention = RetentionManager(config, self.db, jobs=self.jobs)
        self.monitor = ResourceMonitor(config.root, configured_max_slots=config.max_slots)
        # Наблюдение за провайдерами. Агент их только СПРАШИВАЕТ и пересказывает
        # центру; ни одного вызова модели этот путь не делает.
        self.providers = ProviderManager(
            worker_root=config.root,
            enabled=config.provider_gate_enabled,
            auth_check_interval_sec=config.provider_auth_check_interval_sec,
            quota_probe_interval_sec=config.provider_quota_probe_interval_sec,
            stale_after_sec=config.provider_quota_stale_after_sec,
            timeout_sec=config.provider_timeout_sec,
            low_threshold_pct=config.provider_quota_low_threshold_pct,
            account_groups=dict(config.provider_account_groups or {}),
            policy_blocked=dict(config.provider_policy_blocked or {}),
            auth_modes=dict(config.provider_auth_modes or {}),
            executables=dict(config.provider_executables or {}),
            inference_allowed=config.allow_real_provider_probe,
            pipeline_bridge_enabled=config.pipeline_provider_bridge_enabled,
            log=_log,
        )
        self._provider_thread: Optional[threading.Thread] = None
        self.certificate_renewal = None

        data_client = CenterClient(
            config.data_plane_base_url or config.dispatcher_url,
            token=self.token,
            worker_id=self.worker_id,
            instance_id=self.instance_id,
            timeout=config.request_timeout_sec,
            verify=data_plane_tls_verify(config),
            transport=config.transport,
        )
        if config.control_transport == "grpc":
            from audit_worker.grpc_transport import GrpcStreamControlTransport

            self.client = GrpcStreamControlTransport(
                target=str(config.grpc_target),
                data_client=data_client,
                state_store=self.state_store,
                jobs=self.jobs,
                worker_id=self.worker_id,
                instance_id=self.instance_id,
                config=config,
                build_heartbeat=self._heartbeat_payload,
                log=_log,
            )
            if config.grpc_security_mode == "mtls":
                from audit_worker.certificate_renewal import AutomaticCertificateRenewal

                self.certificate_renewal = AutomaticCertificateRenewal(
                    config=config, worker_id=self.worker_id, log=_log
                )
        else:
            self.client = data_client
        self.poller = JobPullClient(self.client, wait_sec=config.poll_wait_sec)
        self.heartbeat = HeartbeatClient(
            self.client,
            interval_sec=config.heartbeat_interval_sec,
            build_payload=self._heartbeat_payload,
            on_response=self._on_heartbeat_response,
            on_error=self._on_center_error,
        )
        # Диагностическое состояние связи с центром. TLS-, auth- и
        # protocol-ошибки НЕ маскируются под «сеть моргнула»: по ним ожидание
        # бессмысленно, и оператор должен видеть разницу (§2.3 задания).
        self.center_state: str = str(identity.get("center_state") or "online")

        self._stop = threading.Event()
        self._sender_thread: Optional[threading.Thread] = None
        self._command_thread: Optional[threading.Thread] = None
        # Ключ — attempt_id, а НЕ job_id. У задания попыток может быть
        # несколько, и в момент подмены (оператор признал попытку
        # потерянной и создал новую) обе живут одновременно: по job_id
        # вторая затирала первую в учёте — слот считался свободным, а
        # снятие первой выкидывало из `_active` контекст ВТОРОЙ.
        self._active: dict[str, dict[str, Any]] = {}   # attempt_id → контекст
        self._active_lock = threading.Lock()
        # Потоки заданий. С двумя слотами агент ведёт до двух заданий сразу, и
        # цикл больше не блокируется первым из них: ошибка задания A не вправе
        # остановить работу задания B (§19 задания).
        self._job_threads: dict[str, threading.Thread] = {}  # attempt_id → поток
        # Отправку журнала ведут несколько потоков (потоки заданий и
        # отправитель). Без лока каждый читал свой пакет от одного и того же
        # last_acked_seq и слал центру дубли.
        self._flush_lock = threading.Lock()
        # Опрос команд централизован в одном потоке. Раньше его делал цикл
        # ожидания задания — при двух заданиях это давало два независимых
        # опроса и двойное подтверждение одной команды.
        self._command_lock = threading.Lock()
        self._max_slots = config.max_slots
        self._last_reconcile_at = 0.0
        self._last_runtime_telemetry_at = 0.0
        self._control_context_warned = False
        self._runtime_telemetry_thread: Optional[threading.Thread] = None

    def _on_center_error(self, exc: BaseException) -> None:
        """Классифицировать отказ центра и запомнить его как состояние связи."""
        from audit_worker.registration import classify_center_failure

        try:
            state = classify_center_failure(exc)
        except BaseException:                    # noqa: BLE001 — чужая ошибка
            state = "center_unreachable"
        if state != self.center_state:
            _log(f"состояние связи с центром: {self.center_state} → {state}")
        self.center_state = state
        try:
            self.state_store.update_runtime_diagnostics(
                last_heartbeat_error_reason=_heartbeat_reason_code(state),
                gateway_status="unavailable",
            )
        except Exception:  # noqa: BLE001 - health reporting is best effort
            pass
        _log(f"heartbeat не прошёл ({state}): {exc}")

    def _on_center_ok(self) -> None:
        if self.center_state != "online":
            _log(f"связь с центром восстановлена (было {self.center_state})")
        self.center_state = "online"

    def _record_heartbeat_ok(self) -> None:
        try:
            self.state_store.update_runtime_diagnostics(
                last_heartbeat_at=time.time(),
                last_heartbeat_error_reason="",
                gateway_status="ready",
            )
        except Exception:  # noqa: BLE001 - health reporting is best effort
            pass

    # ─── Жизненный цикл ──────────────────────────────────────────────────────
    def run_forever(self, *, max_jobs: Optional[int] = None) -> None:
        """Основной цикл. max_jobs ограничивает число заданий (для smoke-прогона).

        Цикл больше НЕ выполняет задание сам: он получает назначение и отдаёт
        его отдельному потоку. Иначе второй слот существовал бы только на
        бумаге — агент простаивал бы на первом задании все его минуты, а
        ошибка одного задания уносила бы за собой второе (§19 задания).
        """
        self._startup_reconcile()
        self.heartbeat.start()
        self._start_sender()
        self._start_command_poller()
        self._start_provider_poller()
        if self.certificate_renewal is not None:
            self.certificate_renewal.start()
        _log(
            f"агент запущен: worker_id={self.worker_id}, "
            f"центр={self.config.dispatcher_url}, слотов={self._max_slots}"
        )

        started = 0
        delays = backoff_delays()
        self._last_reconcile_at = time.time()
        try:
            while not self._stop.is_set():
                self._reap_job_threads()
                if max_jobs is not None and started >= max_jobs:
                    if not self._job_threads:
                        _log(f"взято заданий: {started} — останавливаюсь по max_jobs")
                        break
                    self._stop.wait(0.2)
                    continue
                try:
                    self._deliver_pending_results()
                    self._publish_runtime_telemetry()
                    if (
                        time.time() - self._last_reconcile_at
                        >= RECONCILE_INTERVAL_SEC
                    ):
                        self._reconcile_with_center()
                    counts = self._busy_counts()
                    free = self._free_slots()
                    if free <= 0:
                        # Слотов нет — за заданием не ходим вовсе. Забрать
                        # работу, которую не сможем выполнить, значит подвесить
                        # её у себя вместо очереди центра.
                        self._stop.wait(0.5)
                        continue
                    assignment = self.poller.poll(
                        free_slots=free,
                        busy_slots=counts["busy"],
                        compressions=["gzip", "none"],
                        executor_status=str(
                            self.db.executor_snapshot().get("status") or ""
                        ),
                    )
                    delays = backoff_delays()
                    self._on_center_ok()
                except Exception as exc:  # noqa: BLE001 — центр недоступен
                    self._on_center_error(exc)
                    _log(f"опрос заданий не удался: {exc}")
                    # Ограниченный exponential backoff с джиттером. Процессы
                    # аудита при этом не трогаются, EventOutbox не сбрасывается,
                    # повторные задания не создаются — сверка выполнится, когда
                    # центр вернётся (RECONCILE_INTERVAL_SEC).
                    self._stop.wait(next(delays))
                    continue

                if assignment is None:
                    self._stop.wait(1.0)
                    continue

                self._start_job_thread(assignment)
                started += 1
        finally:
            self.shutdown()

    def _publish_runtime_telemetry(self) -> None:
        """Запустить отправку сводки в ОТДЕЛЬНОМ потоке и сразу вернуться.

        Синхронный вызов отсюда был бы прямым нарушением I-01. Центр может
        принять соединение и замолчать; тогда HTTP-клиент ждёт свой таймаут
        (десятки секунд), и всё это время главный цикл не опрашивает задания,
        не сверяется и не досылает результаты — из-за диагностической строки
        на экране, притом что боевой канал управления полностью исправен.

        Поток ровно один (single-flight): пока предыдущая отправка висит,
        новая не заводится, иначе зависший центр порождал бы поток в минуту.
        """
        now = time.time()
        if now - self._last_runtime_telemetry_at < diagnostics.RUNTIME_TELEMETRY_INTERVAL_SEC:
            return
        publish = getattr(self.client, "post_resources", None)
        if publish is None:
            return
        thread = self._runtime_telemetry_thread
        if thread is not None and thread.is_alive():
            return
        self._last_runtime_telemetry_at = now
        self._runtime_telemetry_thread = threading.Thread(
            target=self._send_runtime_telemetry, args=(publish,),
            name="runtime-telemetry", daemon=True,
        )
        self._runtime_telemetry_thread.start()

    def _send_runtime_telemetry(self, publish: Any) -> None:
        """Тело отправки. Любая ошибка гасится: это показания, а не решение."""
        try:
            payload = diagnostics.collect_runtime_telemetry(self.config, self.jobs)
            publish({"at": payload["at"], "runtime": payload})
        except Exception as exc:  # noqa: BLE001 — диагностика не критична
            _log(f"сводка диагностики не отправлена: {exc}")

    def _reap_job_threads(self) -> None:
        with self._active_lock:
            for key in [k for k, t in self._job_threads.items() if not t.is_alive()]:
                self._job_threads.pop(key, None)

    def _start_job_thread(self, assignment: dict[str, Any]) -> None:
        """Занять слот и отдать задание отдельному потоку.

        Контекст регистрируется в `_active` ЗДЕСЬ, в главном потоке, до старта
        потока: иначе между «получили назначение» и «поток успел записаться»
        было бы окно, в котором `_free_slots` считает слот свободным и агент
        забирает третье задание.
        """
        attempt_id = assignment["attempt_id"]
        meta = self.jobs.load(assignment["job_id"], attempt_id) or {}
        queue_item = self.db.queue_item(attempt_id)
        with self._active_lock:
            duplicate = attempt_id in self._active or (
                attempt_id in self._job_threads
                and self._job_threads[attempt_id].is_alive()
            )
        if (
            not duplicate
            and queue_item is None
            and meta.get("local_state") in {"verified", "accepted"}
        ):
            # Crash after source verification / JobAccept but before local
            # enqueue: resume from the durable boundary, never redownload or
            # overwrite metadata, and enqueue the attempt idempotently once.
            self._start_recovered_attempt(meta, assignment=assignment)
            return
        duplicate = duplicate or queue_item is not None or meta.get("local_state") in {
            "verified", "accepted", "running", "completed_locally",
            "uploading", "finished", "failed", "rejected", "cancelled",
            "executor_interrupted", "superseded",
        }
        if duplicate:
            # A re-offer after reconnect is recovery evidence, not new work.
            # If local source verification has already completed, repeat the
            # idempotent JobAccept so an ACK lost with the old stream cannot
            # leave Center in source_uploading. Never enqueue/launch again.
            if meta.get("local_state") in {
                "verified", "accepted", "running", "completed_locally",
                "uploading", "finished",
            }:
                try:
                    self.client.accept_job(
                        assignment["job_id"],
                        {
                            "attempt_id": attempt_id,
                            "accepted_at": time.time(),
                            "source_verified": {"sha256_ok": True, "manifest_version": 1},
                            "planned_stages": ["test_pipeline_v1"],
                        },
                        meta.get("execution_token") or "",
                    )
                except Exception:  # noqa: BLE001 - next re-offer/reconnect retries
                    pass
            return
        ctx = self._prepare_ctx(assignment)
        thread = threading.Thread(
            target=self._run_job_guarded,
            args=(assignment, ctx),
            name=f"job-{assignment['job_id'][:8]}",
            daemon=True,
        )
        with self._active_lock:
            self._job_threads[assignment["attempt_id"]] = thread
        thread.start()

    def _run_job_guarded(self, assignment: dict[str, Any], ctx: dict[str, Any]) -> None:
        try:
            self.execute_job(assignment, ctx=ctx)
        except Exception as exc:  # noqa: BLE001 — одно задание не роняет агента
            _log(f"задание {assignment['job_id'][:8]}: поток завершился ошибкой: {exc}")

    def shutdown(self) -> None:
        self._stop.set()
        if self.certificate_renewal is not None:
            self.certificate_renewal.stop()
        self.heartbeat.stop()
        for thread in (self._sender_thread, self._command_thread):
            if thread is not None:
                thread.join(timeout=10)
        # Потоки заданий — наблюдатели: работа принадлежит ИСПОЛНИТЕЛЮ и
        # продолжится без нас. Ждём их недолго и только чтобы дописать журнал.
        for thread in list(self._job_threads.values()):
            thread.join(timeout=5)
        # Последняя отправка идёт с force: `_flush_outbox_locked` крутится в
        # цикле `while ... and not self._stop.is_set()`, а `_stop` к этому
        # моменту уже взведён — без флага финальный сброс не отправлял ни
        # одного события, то есть был чистой видимостью.
        self._flush_all_outboxes(force=True)
        self.client.close()

    def _startup_reconcile(self) -> None:
        survived = reconciliation.survived_processes(self.jobs, self.registry)
        for meta in survived:
            _log(
                f"задание {meta['job_id'][:8]} пережило рестарт агента "
                f"(процессы живы) — не трогаю"
            )
        self._adopt_surviving_attempts(survived)
        self._resume_pre_dispatch_attempts()
        self._report_interrupted_attempts()
        self._reconcile_with_center()

    def _resume_pre_dispatch_attempts(self) -> None:
        """Resume the durable verify/accept → enqueue crash window."""
        for meta in self.jobs.iter_all():
            if meta.get("local_state") not in {"verified", "accepted"}:
                continue
            if self.db.queue_item(meta["attempt_id"]) is not None:
                continue
            self._start_recovered_attempt(meta)

    def _start_recovered_attempt(
        self,
        meta: dict[str, Any],
        *,
        assignment: Optional[dict[str, Any]] = None,
    ) -> None:
        """Start one Agent observer from durable metadata, never a second Executor."""
        attempt_id = meta["attempt_id"]
        with self._active_lock:
            if attempt_id in self._active or (
                attempt_id in self._job_threads
                and self._job_threads[attempt_id].is_alive()
            ):
                return
        recovered = assignment or {
            "job_id": meta["job_id"],
            "attempt_id": attempt_id,
            "job_type": meta.get("job_type") or "test_pipeline_v1",
            "project_id": meta.get("project_id") or "",
            "version_id": meta.get("version_id"),
            "params": meta.get("params") or {},
            "package": meta.get("package") or {},
            "execution_token": meta.get("execution_token") or "",
        }
        ctx = {
            "job_id": meta["job_id"],
            "attempt_id": attempt_id,
            "project_id": meta.get("project_id") or "",
            "execution_token": meta.get("execution_token") or "",
            "outbox": self._outbox_for(
                meta["job_id"], attempt_id,
                execution_token=meta.get("execution_token") or "",
            ),
            "stage": "verified",
            "started_at": meta.get("started_at") or time.time(),
            "recovered_pre_dispatch": True,
        }
        thread = threading.Thread(
            target=self._run_verified_guarded,
            args=(recovered, ctx),
            name=f"recover-{attempt_id[:8]}",
            daemon=True,
        )
        with self._active_lock:
            self._active[attempt_id] = ctx
            self._job_threads[attempt_id] = thread
        thread.start()

    def _run_verified_guarded(
        self, assignment: dict[str, Any], ctx: dict[str, Any]
    ) -> None:
        try:
            self.execute_job(assignment, ctx=ctx, source_already_verified=True)
        except Exception as exc:  # pragma: no cover - execute_job contains failures
            _log(f"recovery {assignment['job_id'][:8]} завершился ошибкой: {exc}")

    def _reconcile_with_center(self) -> None:
        """Сверить локальное состояние с центром и выполнить его вердикты.

        Зовётся не только на старте, но и периодически из главного цикла.
        Причина в застревающем слоте: если ответ `/jobs/next` потерялся в сети,
        центр уже перевёл попытку в состояние, ЗАНИМАЮЩЕЕ слот, а воркер о ней
        не знает и никогда не спросит. Единственный, кто это разбирает, —
        `reoffer_unknown_jobs` на стороне центра, а он срабатывает только по
        нашему запросу сверки. Пока сверка была лишь стартовой, слот держался
        занятым до перезапуска агента, и центр честно показывал «занято»,
        не умея назвать причину.
        """
        self._last_reconcile_at = time.time()
        verdict = reconciliation.reconcile(
            self.client,
            self.jobs,
            instance_id=self.instance_id,
            previous_instance_id=self.identity.get("previous_instance_id"),
            registry=self.registry,
            db=self.db,
            executor=self.db.executor_snapshot(),
            disk=self.retention.disk_snapshot(),
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
            #
            # Закрываем попытку ЦЕЛИКОМ, а не одним полем срока. Прежде здесь
            # ставился только `retention_until`, и попытка навсегда оставалась
            # `completed_locally`: для RetentionManager она выглядела живой и не
            # убиралась даже по истечении срока, в каждую сверку возвращалась
            # заново, а отметки о завершении в журнале не появлялось вовсе.
            if item.get("result_accepted") and item.get("retention_until"):
                self._finalize_delivery(
                    item["job_id"], item["attempt_id"],
                    {"retention_until": item["retention_until"],
                     "state": item.get("center_state")},
                )
            if item["action"] == "stop_superseded":
                # Останавливает ИСПОЛНИТЕЛЬ, а не агент: у агента нет права
                # слать сигналы процессам, и проверки принадлежности (pid +
                # время старта + отпечаток) живут там же, где реестр (I-17).
                self.db.enqueue_local_command(
                    command_type="cancel_attempt",
                    job_id=item["job_id"],
                    attempt_id=item["attempt_id"],
                    payload={
                        "job_id": item["job_id"],
                        "attempt_id": item["attempt_id"],
                        "reason": "попытка отозвана центром",
                    },
                )
                self.jobs.update(
                    item["job_id"], item["attempt_id"],
                    local_state="superseded", local_disposition="superseded",
                )
            elif item["action"] == "upload_result":
                self._resume_upload(item["job_id"], item["attempt_id"])

    def _adopt_surviving_attempts(self, survived: list[dict[str, Any]]) -> None:
        """Взять под наблюдение попытки, чьи процессы пережили рестарт агента.

        Раньше рестарт агента только ПЕЧАТАЛ «не трогаю»: контекст в `_active`
        не заводился, поток наблюдения не поднимался. Последствия были не
        косметические.

        * События обеих попыток лежали на диске до самого конца работы —
          отправитель ходит только по `_active`. Оператор видел замерший
          прогресс и не мог отличить это от зависшего аудита.
        * Занятость считалась мимо них, и центр показывал свободные слоты у
          воркера, на котором в этот момент шло два процесса.

        Процесс НЕ перезапускается и не трогается: работа принадлежит
        исполнителю (I-02/I-03). Мы только возвращаем себе роль наблюдателя —
        досылаем журнал, а когда исполнитель допишет исход, обычный проход
        `_deliver_pending_results` заберёт готовый архив.
        """
        for meta in survived:
            job_id, attempt_id = meta["job_id"], meta["attempt_id"]
            with self._active_lock:
                if attempt_id in self._active:
                    continue
            ctx = {
                "job_id": job_id,
                "attempt_id": attempt_id,
                "project_id": meta.get("project_id", ""),
                "execution_token": meta.get("execution_token") or "",
                "outbox": self._outbox_for(
                    job_id, attempt_id,
                    execution_token=meta.get("execution_token") or "",
                ),
                "stage": "running",
                "started_at": meta.get("started_at") or time.time(),
                "adopted": True,
            }
            with self._active_lock:
                self._active[attempt_id] = ctx
            thread = threading.Thread(
                target=self._observe_adopted, args=(ctx,),
                name=f"adopt-{attempt_id[:8]}", daemon=True,
            )
            with self._active_lock:
                self._job_threads[attempt_id] = thread
            thread.start()
            _log(f"задание {job_id[:8]}: взято под наблюдение после рестарта агента")

    def _observe_adopted(self, ctx: dict[str, Any]) -> None:
        """Досылать журнал подхваченной попытки, пока исполнитель её не закончит."""
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        try:
            while not self._stop.is_set():
                try:
                    self._flush_outbox(ctx)
                except Exception:  # noqa: BLE001 — отправка повторится
                    pass
                item = self.db.queue_item(attempt_id) or {}
                if item.get("state") in local_db.TERMINAL_QUEUE_STATES:
                    break
                self._stop.wait(_COMMAND_POLL_SEC)
        finally:
            with self._active_lock:
                self._active.pop(attempt_id, None)
            try:
                self._flush_outbox(ctx)
            except Exception:  # noqa: BLE001 — досылка повторится проходом доставки
                pass
            _log(f"задание {job_id[:8]}: наблюдение после рестарта завершено")

    def _report_interrupted_attempts(self) -> None:
        """Досылать диагностику о попытках, которые исполнитель признал прерванными.

        Рестарт СЕТЕВОГО агента сюда не приводит: его перезапуск не убивает ни
        исполнителя, ни процесс (I-02). Сюда попадает только то, что исполнитель
        сам пометил `executor_interrupted` — процесс исчез без маркера. Агент
        ничего не перезапускает: решение о повторе принимает оператор (§8.6).
        """
        for item in self.db.list_queue(states=(local_db.QUEUE_INTERRUPTED,)):
            job_id, attempt_id = item["job_id"], item["attempt_id"]
            meta = self.jobs.load(job_id, attempt_id) or {}
            if meta.get("interrupt_reported"):
                continue
            outbox = self._outbox_for(
                job_id, attempt_id, execution_token=meta.get("execution_token") or ""
            )
            _log(
                f"задание {job_id[:8]}: исполнитель сообщил о прерывании — "
                f"передаю диагностику, повтор не запускаю"
            )
            # Признак ставится ПОСЛЕ успешной отправки. Раньше он писался до
            # неё: если центр в этот момент был недоступен, диагностика
            # оставалась на диске, а попытка навсегда считалась «уже
            # сообщённой» и не попадала сюда больше никогда.
            try:
                self._flush_outbox(
                    {
                        "job_id": job_id,
                        "attempt_id": attempt_id,
                        "outbox": outbox,
                        "execution_token": meta.get("execution_token"),
                    }
                )
            except Exception as exc:  # noqa: BLE001 — попробуем на следующем проходе
                _log(f"задание {job_id[:8]}: диагностику отправить не удалось: {exc}")
                continue
            if outbox.has_pending:
                continue
            self.jobs.update(job_id, attempt_id, interrupt_reported=True)

    # ─── Слоты и heartbeat ───────────────────────────────────────────────────
    def _free_slots(self) -> int:
        """Сколько заданий агент готов взять ПРЯМО СЕЙЧАС.

        Считается по трём независимым числам: сколько заданий ведёт сам агент,
        сколько попыток захвачено локальной очередью и сколько процессов живо.
        Берётся худшее — иначе агент забирал бы работу, которую исполнитель
        всё равно не запустит, и она висела бы у воркера вместо очереди центра.
        """
        counts = self._busy_counts()
        if not counts["db_ok"]:
            # Занятость неизвестна — работу не берём. Прежний запасной путь
            # («считаем по своему учёту») ошибался в ОПАСНУЮ сторону: сразу
            # после рестарта агента `_active` пуст, процессы при этом живы, и
            # агент забирал задание, которое исполнитель принять не может, —
            # оно повисало у воркера вместо очереди центра. Пропуск одного
            # оборота стоит секунды.
            return 0
        snapshot = self.monitor.snapshot(
            active_jobs=counts["busy"], live_processes=counts["live"]
        )
        return int(snapshot["slots"]["calculated_free"])

    def _busy_counts(self) -> dict[str, Any]:
        """Единственный ответ на вопрос «сколько слотов занято».

        Раньше их было два: `_free_slots` брал худшее из трёх чисел, а
        heartbeat считал занятость только по `len(self._active)`. После
        рестарта агента это расходилось до противоположного: агент докладывал
        центру свободный слот, которым сам же не мог воспользоваться, — центр
        показывал оператору доступную ёмкость и ждал запроса, которого не
        будет.
        """
        with self._active_lock:
            active = len(self._active)
        try:
            claimed = self.db.claimed_attempt_count()
            live = self.db.live_process_count()
            db_ok = True
        except Exception:  # noqa: BLE001 — база занята: честно говорим «не знаю»
            claimed, live, db_ok = active, active, False
        return {
            "active": active,
            "claimed": claimed,
            "live": live,
            "busy": max(active, claimed, live),
            "db_ok": db_ok,
        }

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
        # Занятость считается ТЕМ ЖЕ способом, что и в `_free_slots`: иначе
        # центр получает одно число, а решение агент принимает по другому.
        counts = self._busy_counts()
        snapshot = self.monitor.snapshot(
            active_jobs=counts["busy"], live_processes=counts["live"]
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
        if snapshot["slots"]["calculated_free"] == 0 and not counts["busy"]:
            warnings.append(
                {
                    "code": "no_free_slots",
                    "severity": "warn",
                    "message": f"Слотов нет: ограничивает {snapshot['slots']['binding_constraint']}",
                }
            )
        executor = self.db.executor_snapshot()
        if executor.get("status") not in ("online",):
            # Агент онлайн ≠ VPS работает. Молчание исполнителя — отдельная
            # новость, и экран обязан показать её отдельно (§16.6).
            warnings.append(
                {
                    "code": "executor_not_online",
                    "severity": "warn",
                    "message": (
                        f"Локальный исполнитель: {executor.get('status')}. "
                        "Новые задания выполняться не будут."
                    ),
                }
            )
        # Провайдеры — ОТДЕЛЬНЫЕ предупреждения, severity=warn. Ни одно из них
        # не меняет worker_state: провайдер может быть не авторизован, а воркер
        # при этом полностью работоспособен для тестовых заданий (§27 задания).
        try:
            warnings.extend(self.providers.warnings())
        except Exception as exc:                       # noqa: BLE001 — см. §27
            _log(f"предупреждения провайдеров не собраны: {exc}")
        disk = self.retention.disk_snapshot()
        if disk["level"] != "ok":
            warnings.append(
                {
                    "code": f"disk_{disk['level']}",
                    "severity": "error" if disk["level"] == "critical" else "warn",
                    "message": (
                        f"Свободно {disk['free_bytes'] / (1024 ** 3):.1f} ГБ. "
                        "Неподтверждённые результаты не удаляются автоматически."
                    ),
                }
            )
        try:
            provider_payload = self.providers.heartbeat_payload()
        except Exception as exc:                       # noqa: BLE001 — см. §27
            _log(f"снимок провайдеров не собран: {exc}")
            provider_payload = []
        claimed, live = counts["claimed"], counts["live"]
        if not counts["db_ok"]:
            warnings.append(
                {
                    "code": "local_db_unavailable",
                    "severity": "warn",
                    "message": (
                        "Локальная база воркера не прочиталась: числа занятости "
                        "ниже приведены по собственному учёту агента и могут "
                        "быть занижены. Новые задания агент в этом состоянии "
                        "не берёт."
                    ),
                }
            )
        return {
            "instance_id": self.instance_id,
            "sent_at": time.time(),
            "worker_state": "busy" if counts["busy"] else "idle",
            "configured_max_slots": self.config.max_slots,
            "calculated_free_slots": (
                0 if not counts["db_ok"]
                else int(snapshot["slots"]["calculated_free"])
            ),
            "active_jobs": active,
            "resource_snapshot": snapshot,
            "warnings": warnings,
            "executor": executor,
            "disk": disk,
            # Что ПРОВЕРЕНО сборкой воркера, а не что пожелал оператор.
            "max_verified_slots": worker_slots.MAX_VERIFIED_SLOTS,
            # Диагностика для сверки с расчётом центра (slot_count_mismatch).
            "active_local_jobs": counts["active"],
            "running_processes": live,
            "locally_reserved_slots": claimed,
            # Состояние провайдеров: последний ИЗВЕСТНЫЙ снимок, без опроса в
            # такте heartbeat. Секретов здесь нет по построению — payload
            # собирается перечислением разрешённых полей (см.
            # ProviderIdentity.as_center_payload).
            "providers": provider_payload,
        }

    def _on_heartbeat_response(self, response: dict[str, Any]) -> None:
        self._record_heartbeat_ok()
        for update in response.get("retention_updates", []):
            if update.get("retention_until") is None:
                continue
            self.jobs.update(
                update["job_id"],
                update["attempt_id"],
                retention_until=update["retention_until"],
            )

    # ─── Команды ─────────────────────────────────────────────────────────────
    # Разрешённые типы. Агент их НЕ исполняет — он кладёт их в локальную
    # очередь, а исполняет исполнитель либо RetentionManager. У агента нет и не
    # должно быть кода, останавливающего процессы или стирающего каталоги.
    _LOCAL_COMMANDS = ("cancel_attempt", "delete_attempt_data")

    def _start_provider_poller(self) -> None:
        """Отдельный поток опроса провайдеров.

        Отдельный — по той же причине, что и у команд: heartbeat обязан идти
        каждые 30 секунд, а опрос провайдера поднимает процесс CLI и ходит в
        сеть. Делать это в такте heartbeat значило бы держать 2880 запусков
        CLI в сутки ради данных, которые меняются раз в час, и подвешивать
        heartbeat на время каждого из них.
        """
        if not self.config.provider_gate_enabled:
            _log("наблюдение за провайдерами выключено (AUDIT_WORKER_PROVIDER_GATE_ENABLED)")
            return

        def loop() -> None:
            # Первый проход сразу: центр должен увидеть состояние провайдеров
            # с первого heartbeat, а не через пять минут после старта.
            first = True
            while not self._stop.is_set():
                try:
                    self.providers.refresh(force=first)
                except Exception as exc:               # noqa: BLE001 — см. §27
                    _log(f"опрос провайдеров не удался: {exc}")
                first = False
                # Тик — минимальная из двух частот; сам менеджер решает,
                # чему подошёл срок.
                self._stop.wait(
                    max(30.0, min(
                        self.config.provider_auth_check_interval_sec,
                        self.config.provider_quota_probe_interval_sec,
                    ))
                )

        self._provider_thread = threading.Thread(
            target=loop, name="provider-poller", daemon=True
        )
        self._provider_thread.start()

    def _start_command_poller(self) -> None:
        """Отдельный поток опроса команд.

        Раньше команды опрашивал цикл ожидания задания. При двух заданиях это
        значило два независимых опроса одной очереди и, как следствие, гонку
        подтверждений; а при занятых слотах главный цикл вообще не заходил в
        опрос — отмена не доезжала, пока не освободится слот.
        """

        def loop() -> None:
            while not self._stop.is_set():
                try:
                    self._drain_commands()
                except Exception as exc:  # noqa: BLE001 — опрос не роняет агента
                    _log(f"опрос команд не удался: {exc}")
                self._stop.wait(_COMMAND_POLL_SEC)

        self._command_thread = threading.Thread(
            target=loop, name="command-poller", daemon=True
        )
        self._command_thread.start()

    def _drain_commands(self) -> None:
        """Принять команды центра → положить в локальную очередь → отчитаться."""
        if not self._command_lock.acquire(blocking=False):
            return                      # опрос уже идёт: второй не нужен
        try:
            self._drain_commands_locked()
        finally:
            self._command_lock.release()

    def _drain_commands_locked(self) -> None:
        try:
            payload = self.client.get_commands()
        except Exception:  # noqa: BLE001 — команды подождут до следующего круга
            payload = {"commands": []}
        for command in payload.get("commands", []):
            ctype = command.get("command_type")
            body = command.get("payload") or {}
            if ctype in self._LOCAL_COMMANDS:
                local = self.db.enqueue_local_command(
                    command_type=ctype,
                    job_id=body.get("job_id") or command.get("job_id"),
                    attempt_id=body.get("attempt_id") or command.get("attempt_id"),
                    payload=body,
                    central_command_id=command["command_id"],
                )
                if local and local.get("status") == "reported":
                    # Center can re-emit only an unacknowledged command. A
                    # duplicate after reconnect therefore proves the previous
                    # fire-and-stream ACK was lost before commit; make its
                    # durable result reportable again without re-executing it.
                    self.db.retry_reported_command(local["local_command_id"])
                continue
            # Закрытый набор: неизвестное не исполняем и честно говорим об этом.
            self._ack_center(
                command["command_id"],
                {"status": "error",
                 "detail": {"outcome": "unsupported_command", "received": ctype}},
            )
        self._report_local_command_results()

    def _report_local_command_results(self) -> None:
        """Передать центру результаты исполненных локальных команд."""
        for item in self.db.finished_local_commands():
            try:
                result = json.loads(item.get("result_json") or "{}")
            except ValueError:
                result = {"status": "error", "detail": {"outcome": "bad_result"}}
            if self._ack_center(item["central_command_id"], result):
                self.db.mark_command_reported(item["local_command_id"])

    def _ack_center(self, command_id: str, result: dict[str, Any]) -> bool:
        try:
            self.client.ack_command(command_id, result)
        except Exception:  # noqa: BLE001 — повторим при следующем опросе
            return False
        return True

    # ─── Исполнение задания ──────────────────────────────────────────────────
    def _outbox_for(
        self, job_id: str, attempt_id: str, *, execution_token: str = ""
    ) -> EventOutbox:
        """Журнал событий попытки со СЧЁТЧИКОМ ИЗ БАЗЫ.

        Номер выдаёт worker.db: в один каталог пишут два процесса, и файловый
        счётчик под `flock` для этого недостаточен (см. `local_db` миграция 2).
        """
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        return EventOutbox(
            job_dir / "events",
            secret_literals=(self.token, execution_token or ""),
            sequence_db=self.db,
            job_id=job_id,
            attempt_id=attempt_id,
        )

    def _prepare_ctx(self, assignment: dict[str, Any]) -> dict[str, Any]:
        """Завести каталоги, метаданные и контекст задания; ЗАНЯТЬ слот."""
        job_id = assignment["job_id"]
        attempt_id = assignment["attempt_id"]
        token = assignment.get("execution_token") or ""
        self.jobs.create(assignment)
        ctx = {
            "job_id": job_id,
            "attempt_id": attempt_id,
            "project_id": assignment.get("project_id", ""),
            "execution_token": token,
            "outbox": self._outbox_for(job_id, attempt_id, execution_token=token),
            "stage": "download",
            "started_at": time.time(),
        }
        with self._active_lock:
            self._active[attempt_id] = ctx
        return ctx

    def execute_job(
        self,
        assignment: dict[str, Any],
        *,
        ctx: Optional[dict[str, Any]] = None,
        source_already_verified: bool = False,
    ) -> dict[str, Any]:
        if ctx is None:
            ctx = self._prepare_ctx(assignment)
        job_id = ctx["job_id"]
        attempt_id = ctx["attempt_id"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        outbox: EventOutbox = ctx["outbox"]

        try:
            if not source_already_verified:
                self._download_and_verify(assignment, ctx, job_dir)
            self._accept(assignment, ctx)
            outcome = self._dispatch_and_wait(assignment, ctx)
            if outcome["ok"]:
                self._upload_ready_result(assignment, ctx, job_dir)
            return outcome
        except AttemptSupersededError:
            _log(f"задание {job_id[:8]}: попытка отозвана — прошу исполнителя остановить")
            # Останавливает ИСПОЛНИТЕЛЬ: только он вправе слать сигналы и
            # только процессу с доказанной принадлежностью (§10, I-17).
            self.db.enqueue_local_command(
                command_type="cancel_attempt", job_id=job_id, attempt_id=attempt_id,
                payload={"job_id": job_id, "attempt_id": attempt_id,
                         "reason": "попытка отозвана центром"},
            )
            self.jobs.update(
                job_id, attempt_id,
                local_state="superseded", local_disposition="superseded",
            )
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
                self._active.pop(attempt_id, None)
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
        is_audit = str(assignment.get("job_type") or "") == "audit_pipeline_v1"
        unpack_target = (job_dir / "unpack_staging") if is_audit else (job_dir / "work")

        self.jobs.update(job_id, attempt_id, local_state="downloading")
        ctx["stage"] = "download"
        # ``CenterClient.download_source`` deliberately preserves ``.part``
        # and sends Range on the next invocation.  A single invocation here
        # made that resumability unreachable after a mid-body TCP/TLS reset:
        # the generic job handler marked the attempt failed instead.  Keep the
        # attempt in its pre-dispatch state and retry only transport failures;
        # HTTP/auth/hash failures still fail closed and are never masked.
        delays = backoff_delays()
        while True:
            try:
                self.client.download_source(
                    job_id,
                    dest,
                    ctx["execution_token"],
                    attempt_id=ctx["attempt_id"],
                )
                break
            except (httpx.TransportError, OSError, ControlContextUnavailable) as exc:
                # Обрыв потока управления попадает СЮДА, а не в общий `except`
                # ниже. Иначе переподключение между выдачей задания и первым
                # байтом исходников помечало бы живую попытку `failed`: работа
                # не начиналась, а задание уходило в отказ по чужой причине.
                #
                # Но повторять можно только ВРЕМЕННОЕ. Локальный отказ диска —
                # «нет места», «нет прав» — сам не пройдёт, и вечный повтор
                # держал бы слот занятым: при заполнении слотов воркер
                # перестаёт брать работу вообще, и снаружи это выглядит как
                # молчание, а не как переполненный диск.
                if isinstance(exc, OSError) and exc.errno in _PERMANENT_LOCAL_ERRNOS:
                    raise
                if self._stop.is_set():
                    raise UploadDeferred(
                        "Agent stopped while source download was awaiting resume"
                    ) from exc
                delay = next(delays)
                _log(
                    f"задание {job_id[:8]}: source transfer interrupted; "
                    f"resume через {delay:.1f} с"
                )
                if self._stop.wait(delay):
                    raise UploadDeferred(
                        "Agent stopped while source download was awaiting resume"
                    ) from exc

        try:
            info = package_io.verify_and_unpack(
                archive=dest,
                expected_sha256=package["sha256"],
                work_dir=unpack_target,
                compression=package.get("compression"),
            )
        except package_io.BundleError as exc:
            ctx["outbox"].append("source_invalid", {"message": str(exc)})
            self._flush_outbox(ctx)
            raise
        if is_audit:
            # Пакет реального аудита распаковывается в `unpack_staging`, а
            # секции переносятся в каталог попытки, где их ждёт `audit_runner`.
            # Распаковывать прямо в `job_dir` нельзя: `verify_and_unpack`
            # очищает каталог назначения и снёс бы logs/, metadata/ и уже
            # накопленный EventOutbox.
            package_io.require_portable_layout(info["manifest"], unpack_target)
            for source_name, dest_name in package_io.AUDIT_PACKAGE_SECTIONS:
                source = unpack_target / source_name
                if not source.is_dir():
                    continue
                destination = job_dir / dest_name
                shutil.rmtree(destination, ignore_errors=True)
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, destination)

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
        payload = {
            "attempt_id": attempt_id,
            "accepted_at": time.time(),
            "source_verified": {"sha256_ok": True, "manifest_version": 1},
            "planned_stages": ["test_pipeline_v1"],
        }
        delays = backoff_delays()
        while True:
            try:
                self.client.accept_job(
                    job_id, payload, ctx["execution_token"],
                )
                break
            except Exception as exc:  # retry only gRPC transport loss
                if self.config.control_transport != "grpc":
                    raise
                from audit_worker.grpc_transport import GrpcTransportError

                if not isinstance(exc, GrpcTransportError):
                    raise
                if self._stop.is_set():
                    raise UploadDeferred(
                        "Agent stopped before JobAccept could be delivered"
                    ) from exc
                self._stop.wait(next(delays))
        self.jobs.update(job_id, attempt_id, local_state="accepted")
        ctx["outbox"].append("job_accepted", {"planned_stages": ["test_pipeline_v1"]})
        self._flush_outbox(ctx)

    def _dispatch_and_wait(
        self, assignment: dict[str, Any], ctx: dict[str, Any]
    ) -> dict[str, Any]:
        """Поставить попытку в локальную очередь и НАБЛЮДАТЬ за исполнителем.

        Агент здесь ничего не запускает. Это и есть I-02/I-03: если агента
        убить в любой точке этого метода, процесс аудита продолжит работу под
        исполнителем, а поднявшийся заново агент увидит попытку уже в очереди
        и второй раз её не поставит (enqueue идемпотентен по attempt_id).
        """
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        ctx["stage"] = "queued"
        fresh = self.db.enqueue(
            job_id=job_id,
            attempt_id=attempt_id,
            job_type=str(assignment.get("job_type") or "test_pipeline_v1"),
            params=assignment.get("params") or {},
        )
        if not fresh:
            _log(f"задание {job_id[:8]}: уже в локальной очереди — не дублирую")

        executor = self.db.executor_snapshot()
        if executor.get("status") != "online":
            _log(
                f"внимание: локальный исполнитель «{executor.get('status')}» — "
                f"задание {job_id[:8]} будет ждать его запуска"
            )

        while not self._stop.is_set():
            item = self.db.queue_item(attempt_id)
            if item is None:
                return {"ok": False, "reason": "queue_row_lost"}
            state = item.get("state")
            if state == local_db.QUEUE_RUNNING:
                ctx["stage"] = "test_pipeline_v1"
            if state in local_db.TERMINAL_QUEUE_STATES:
                break
            self._flush_outbox(ctx)
            # Команды здесь НЕ опрашиваются: этим занят отдельный поток
            # (`_start_command_poller`). При двух заданиях два наблюдателя
            # опрашивали бы одну очередь и подтверждали одну команду дважды.
            self._stop.wait(0.2)
        else:
            # Агента останавливают. Работу НЕ трогаем: она принадлежит
            # исполнителю и продолжится без нас (I-01, I-02).
            return {"ok": False, "reason": "agent_stopping"}

        item = self.db.queue_item(attempt_id) or {}
        self._flush_outbox(ctx)
        state = item.get("state")
        if state == local_db.QUEUE_FINISHED:
            return {"ok": True, "queue_state": state}
        return {"ok": False, "reason": state or "unknown", "queue_state": state}

    def _upload_ready_result(
        self, assignment: dict[str, Any], ctx: dict[str, Any], job_dir: Path
    ) -> None:
        """Передать архив, собранный ИСПОЛНИТЕЛЕМ. Сборка — не дело агента."""
        archive = job_dir / "result" / f"{ctx['attempt_id']}.tar.gz"
        if not archive.is_file():
            # Страховка на случай, когда исполнитель отработал, но архив не
            # материализовался (например, ручной прогон без него). Обычный
            # путь сюда не заходит.
            self._package_and_upload(assignment, ctx, job_dir)
            return
        self._upload_archive(assignment, ctx, job_dir, archive)

    def _package_and_upload(
        self, assignment: dict[str, Any], ctx: dict[str, Any], job_dir: Path
    ) -> None:
        """Резервный путь: собрать архив здесь и передать.

        Обычно архив собирает ИСПОЛНИТЕЛЬ — он владеет файлами задания. Этот
        метод остаётся для случая, когда архива на диске не оказалось.
        """
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
            routing_plan=(assignment.get("params") or {}).get("routing_plan"),
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
        self._upload_archive(assignment, ctx, job_dir, archive)

    def _upload_archive(
        self,
        assignment: dict[str, Any],
        ctx: dict[str, Any],
        job_dir: Path,
        archive: Path,
    ) -> None:
        """Передать готовый архив центру. Работа уже сделана и лежит на диске."""
        job_id, attempt_id = ctx["job_id"], ctx["attempt_id"]
        outbox: EventOutbox = ctx["outbox"]
        meta = self.jobs.load(job_id, attempt_id) or {}
        result_size = int(meta.get("result_size") or archive.stat().st_size)

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
                **_routing_binding(meta),
            )
        except ResultRejectedError as exc:
            if not exc.retryable:
                # Невосстановимый отказ на ПЕРВОЙ же отправке. Прежде он попадал
                # в общий `except` ниже, превращался в отложенную передачу и
                # возвращал попытку в очередь досылки — то есть в тот же вечный
                # цикл, только начинавшийся сразу.
                self._record_permanent_rejection(job_id, attempt_id, exc)
                self._flush_outbox(ctx)
                raise UploadDeferred(str(exc)) from exc
            _log(f"задание {job_id[:8]}: центр отверг результат с правом повтора ({exc.detail})")
            self.jobs.update(
                job_id, attempt_id,
                local_state="completed_locally", upload_error=str(exc),
            )
            self._flush_outbox(ctx)
            raise UploadDeferred(str(exc)) from exc
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

        # Центр мог принять архив, но НЕ принять результат (валидация не
        # прошла). Тогда «finished» — ложь: пакет остаётся на воркере без
        # подтверждения, а `finished` исключает его и из досылки, и из
        # collect_known_jobs — задание становилось невидимым навсегда.
        accepted = self._finalize_delivery(job_id, attempt_id, response)
        if not accepted:
            outbox.append(
                "job_completed",
                {"center_state": response.get("state"), "retention_until": None},
            )
        self._flush_outbox(ctx)
        if not accepted:
            _log(
                f"задание {job_id[:8]}: центр не подтвердил приём "
                f"(состояние «{response.get('state')}») — результат остаётся на "
                f"воркере как retention_unconfirmed и попадёт в следующую сверку"
            )


    # ─── Единая финализация доставки ─────────────────────────────────────────
    def _finalize_delivery(
        self, job_id: str, attempt_id: str, response: dict[str, Any],
    ) -> bool:
        """Закрыть доставку по подтверждению центра — ОДИНАКОВО на всех путях.

        Путей, на которых центр сообщает о приёме, три: обычная отправка,
        досылка и сверка. Раньше каждый закрывал попытку по-своему, и путь
        сверки закрывал её неполно: он ставил только `retention_until`. Из-за
        этого попытка навсегда оставалась `completed_locally` — то есть
        считалась живой: `RetentionManager` отказывался её убирать даже по
        истечении срока, `collect_known_jobs` возвращал её в каждую сверку, а в
        журнале не появлялось события о завершении.

        Возвращает True, если приём подтверждён.
        """
        retention_until = response.get("retention_until")
        accepted = retention_until is not None
        fields: dict[str, Any] = {
            "local_state": "finished" if accepted else "completed_locally",
            "retention_until": retention_until,
        }
        if response.get("state") is not None:
            fields["center_state"] = response.get("state")
        if response.get("validation") is not None:
            fields["center_validation"] = response.get("validation")
        if accepted:
            fields["delivery_state"] = DELIVERY_ACKNOWLEDGED
            fields["delivery_acknowledged_at"] = time.time()
        meta = self.jobs.load(job_id, attempt_id) or {}
        self.jobs.update(job_id, attempt_id, **fields)
        if not accepted:
            return False
        # Отметка о завершении в журнале ставится РОВНО один раз: повторная
        # финализация уже подтверждённой попытки не должна плодить события.
        if not meta.get("delivery_acknowledged_at"):
            try:
                outbox = self._outbox_for(
                    job_id, attempt_id,
                    execution_token=meta.get("execution_token") or "",
                )
                outbox.append(
                    "job_completed",
                    {"center_state": response.get("state"),
                     "retention_until": retention_until},
                )
                self._flush_outbox({
                    "job_id": job_id, "attempt_id": attempt_id, "outbox": outbox,
                    "execution_token": meta.get("execution_token"),
                })
            except Exception as exc:  # noqa: BLE001 — журнал догонит по сверке
                _log(f"задание {job_id[:8]}: отметку о завершении отправить не удалось: {exc}")
        return True

    def _record_permanent_rejection(
        self, job_id: str, attempt_id: str, exc: "ResultRejectedError",
    ) -> None:
        """Невосстановимый отказ: досылку прекращаем, работу НЕ удаляем.

        Пакет остаётся неподтверждённым и виден оператору предупреждением
        `retention_unconfirmed`. Решение о его судьбе принимает человек, а не
        молчаливый цикл, который иначе долбил бы шлюз раз в ~26 секунд вечно.
        """
        # Центр мог принять пакет по HTTP и назначить срок хранения, а поток
        # отвергнуть по другой причине. Срок сохраняем: он про ХРАНЕНИЕ.
        if isinstance(exc.acknowledgement, dict) and self._finalize_delivery(
            job_id, attempt_id, exc.acknowledgement
        ):
            _log(
                f"задание {job_id[:8]}: поток отверг досылку, но центр уже "
                f"подтвердил приём по HTTP — доставка закрыта"
            )
            return
        self.jobs.update(
            job_id, attempt_id,
            delivery_state=DELIVERY_REJECTED_PERMANENTLY,
            delivery_rejected_at=time.time(),
            delivery_reject_detail=str(exc.detail)[:500],
        )
        _log(
            f"задание {job_id[:8]}: центр отверг результат без права повтора "
            f"({exc.detail}) — досылку прекращаю, пакет остаётся на воркере "
            f"как неподтверждённый"
        )

    def _control_context_ready(self) -> bool:
        """Готов ли транспорт к операциям, требующим владения потоком.

        У опрашивающего транспорта такого понятия нет — там принадлежность
        доказывает execution_token, и метода не существует. Отсутствие метода
        означает «готов всегда», а не «не готов».
        """
        client = getattr(self, "client", None)
        if client is None:
            # Транспорта нет вовсе — значит нет и понятия владения потоком.
            # Такое бывает у частично собранных объектов в тестах и на ранних
            # стадиях запуска; выдумывать здесь «не готов» значило бы молча
            # запретить досылку там, где раньше она работала.
            return True
        probe = getattr(client, "control_context_ready", None)
        return True if probe is None else bool(probe())

    def _deliver_pending_results(self) -> None:
        """Дослать результаты, оставшиеся на диске после обрыва передачи.

        Без этого прохода готовый пакет ждал бы рестарта агента: сверка
        выполняется только на старте, а сам цикл о нём бы не вспомнил.

        Задания, которые ведёт поток прямо сейчас, пропускаются: иначе главный
        цикл начал бы второй upload того же архива параллельно с первым.
        """
        if not self._control_context_ready():
            # Поток ещё не назвал себя центру. Любая отправка сейчас уйдёт без
            # заголовка принадлежности и получит 409 «попытка отозвана» — шум,
            # который читается как настоящая потеря попытки. Ждём не таймером,
            # а состоянием: следующий оборот цикла наступит через доли секунды.
            if not self._control_context_warned:
                self._control_context_warned = True
                _log("досылка отложена: поток управления ещё не готов")
            return
        self._control_context_warned = False
        with self._active_lock:
            busy = set(self._active)
        for meta in self.jobs.iter_all():
            if meta["attempt_id"] in busy:
                continue
            if delivery_is_terminal(meta):
                # Доставка закрыта: центр подтвердил приём или отказал
                # невосстановимо. Пакет при этом остаётся на диске —
                # удаление подчиняется ТОЛЬКО сроку хранения (I-08/I-12).
                continue
            state = meta.get("local_state")
            if state in ("completed_locally", "uploading") and meta.get("result_hash"):
                # `uploading` сюда добавлено осознанно: обрыв посреди передачи
                # оставлял попытку именно в этом состоянии, и её не подбирал
                # НИКТО — проход требовал строго `completed_locally`, а сверка
                # выполняется только на старте агента. Загрузка возобновляемая
                # и идемпотентная, а активные попытки отсечены выше по `busy`.
                self._resume_upload(meta["job_id"], meta["attempt_id"])
                continue
            if state in ("failed", "rejected", "cancelled", "executor_interrupted"):
                # Провал, случившийся при мёртвом агенте, тоже надо ДОВЕЗТИ.
                # Раньше его исход (`job_failed` в outbox) не отправлял никто:
                # проход доставки требовал успешного результата, отчёт о
                # прерывании — состояния `executor_interrupted`, а сверка
                # событий не шлёт. Центр держал попытку в `running` вечно,
                # то есть слот не освобождался до вмешательства оператора.
                self._flush_terminal_events(meta)

    def _flush_terminal_events(self, meta: dict[str, Any]) -> None:
        """Дослать журнал попытки, закончившейся без успешного результата."""
        job_id, attempt_id = meta["job_id"], meta["attempt_id"]
        outbox = self._outbox_for(
            job_id, attempt_id, execution_token=meta.get("execution_token") or ""
        )
        outbox.reload()
        if not outbox.has_pending:
            return
        _log(f"задание {job_id[:8]}: досылаю исход попытки ({meta.get('local_state')})")
        try:
            self._flush_outbox({
                "job_id": job_id, "attempt_id": attempt_id, "outbox": outbox,
                "execution_token": meta.get("execution_token"),
            })
        except Exception as exc:  # noqa: BLE001 — повторим на следующем проходе
            _log(f"задание {job_id[:8]}: исход отправить не удалось: {exc}")

    def _resume_upload(self, job_id: str, attempt_id: str) -> None:
        meta = self.jobs.load(job_id, attempt_id)
        if not meta or not meta.get("result_hash"):
            return
        if delivery_is_terminal(meta):
            # Второй заслон рядом с проходом доставки: сюда ведёт ещё и путь
            # сверки (`action == "upload_result"`), а подтверждённый результат
            # не должен уезжать повторно ни по одному из них.
            return
        if not self._control_context_ready():
            # Тот же заслон, что и в проходе доставки, но у САМОЙ отправки:
            # сюда ведут ещё стартовая сверка и вердикт центра, и оба могут
            # сработать раньше, чем поток назовёт себя.
            return
        _log(f"досылаю результат задания {job_id[:8]} после перерыва")
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        archive = job_dir / "result" / f"{attempt_id}.tar.gz"
        if not archive.is_file():
            return
        # Сначала события, потом архив: центр должен узнать о завершении
        # раньше, чем получит пакет. Порядок восстановим и на его стороне, но
        # правильный порядок дешевле, чем догон.
        pending = self._outbox_for(
            job_id, attempt_id, execution_token=meta.get("execution_token") or ""
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
                **_routing_binding(meta),
            )
            # Та же проверка, что и на основном пути: «finished» ставится
            # только если центр ПОДТВЕРДИЛ приём (выдал retention_until).
            # Безусловный «finished» здесь выводил невалидированный результат
            # и из досылки, и из сверки — задание исчезало навсегда.
            self._finalize_delivery(job_id, attempt_id, response)
        except ResultRejectedError as exc:
            if exc.retryable:
                _log(f"досылка не удалась: {exc}")
                return
            self._record_permanent_rejection(job_id, attempt_id, exc)
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

    def _flush_all_outboxes(self, *, force: bool = False) -> None:
        with self._active_lock:
            contexts = list(self._active.values())
        for ctx in contexts:
            try:
                self._flush_outbox(ctx, force=force)
            except Exception:  # noqa: BLE001 — отправка повторится
                continue

    def _flush_outbox(self, ctx: dict[str, Any], *, force: bool = False) -> None:
        """Отправить накопленное. При обрыве просто выходим — outbox копит дальше."""
        with self._flush_lock:
            self._flush_outbox_locked(ctx, force=force)

    def _flush_outbox_locked(self, ctx: dict[str, Any], *, force: bool = False) -> None:
        outbox: EventOutbox = ctx["outbox"]
        # Журнал наполняет ИСПОЛНИТЕЛЬ — другой процесс. Без перечитывания
        # позиций с диска агент вечно считал бы, что отправлять нечего.
        outbox.reload()
        while outbox.has_pending and (force or not self._stop.is_set()):
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
