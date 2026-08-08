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
from audit_worker import slots as worker_slots
from audit_worker.config import WorkerConfig
from audit_worker.event_outbox import EventOutbox
from audit_worker.local_store import LocalJobStore, read_json
from audit_worker.retention import RetentionManager

HEARTBEAT_SEC = 15.0
POLL_SEC = 1.0


#: Верхняя граница паузы между SIGTERM и SIGKILL. Центр валидирует поле на
#: выдаче (ge=0, le=600), но исполнитель обязан не зависеть от этого: с
#: `grace_period_sec: 1e9` цикл ожидания встал бы навсегда, а вместе с ним и
#: весь главный цикл — ни новых заданий, ни retention.
MAX_GRACE_SEC = 600.0


def _grace_period(value: Any) -> float:
    """Пауза перед SIGKILL: число в границах, иначе значение по умолчанию."""
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return 30.0
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        return 30.0
    return max(0.0, min(MAX_GRACE_SEC, seconds))


class _StopLoop(Exception):
    """Штатный выход из главного цикла (max_jobs). Не ошибка."""


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
    """Отметка о завершении процесса — своя или написанная им самим.

    Два независимых источника, и это не избыточность:
      * `completed.marker` пишет исполнитель, дождавшийся выхода процесса;
      * `process_exit.json` пишет САМ процесс последним действием.

    Второй источник и есть ответ на вопрос «что делать, если исполнителя
    перезапустили посреди работы»: он единственный, кто знает исход
    достоверно, и его отметка переживает смерть наблюдателя.
    """
    data = read_json(job_dir / "work" / "completed.marker", None)
    if isinstance(data, dict):
        return data
    own = read_json(job_dir / "work" / "process_exit.json", None)
    if isinstance(own, dict):
        return {
            "exit_code": int(own.get("exit_code", 1)),
            "duration_sec": own.get("duration_sec"),
            "steps_done": own.get("steps_done"),
            "steps_total": own.get("steps_total"),
            "finished_at": own.get("finished_at"),
            "source": "process_exit",
        }
    return None


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
        self._command_thread: Optional[threading.Thread] = None
        self._workers: list[threading.Thread] = []
        # Попытки, по которым отмена УЖЕ начата. Признак ставится до отправки
        # сигнала: иначе поток задания успевал увидеть «процесс вернул -15» и
        # объявить обычный провал раньше, чем поток команды успевал записать
        # отмену в очередь — и попытка навсегда застревала в `failed`.
        self._cancelling: set[str] = set()
        # По одному EventOutbox на попытку. Два объекта на один каталог вели
        # каждый свой счётчик seq и выдавали двум событиям один номер: центр
        # дедуплицирует по (job, attempt, seq), и второе терялось молча.
        self._outboxes: dict[str, EventOutbox] = {}
        self._outbox_lock = threading.Lock()

    # ─── Жизненный цикл ──────────────────────────────────────────────────────
    def run_forever(self, *, max_jobs: Optional[int] = None) -> None:
        """Главный цикл.

        Работа идёт в ОТДЕЛЬНОМ потоке, а цикл продолжает разбирать локальные
        команды. Иначе отмена доходила бы только после завершения задания —
        то есть не работала бы вовсе: команда лежала бы в очереди все те
        минуты, ради прерывания которых её и отправили.
        """
        _log(f"исполнитель запущен: {self.instance_id}, pid={os.getpid()}")
        try:
            self.recover_after_restart()
        except Exception as exc:  # noqa: BLE001 — разбор прошлого не блокирует старт
            _log(f"восстановление после рестарта не удалось: {exc!r}")
        self._start_heartbeat()
        self._start_command_worker()
        started_ref = [0]
        try:
            while not self._stop.is_set():
                try:
                    self._tick(max_jobs, started_ref)
                except _StopLoop:
                    break
                except Exception as exc:  # noqa: BLE001 — цикл обязан выжить
                    # Повреждённый worker.db, кончившийся диск, битая команда —
                    # всё это раньше валило исполнителя целиком, а вместе с ним
                    # и наблюдение за живыми процессами аудита. Под systemd это
                    # давало крэш-луп с Restart=always.
                    _log(f"сбой в главном цикле: {exc!r} — продолжаю")
                    self._stop.wait(POLL_SEC)
        finally:
            self.shutdown()

    def slot_limit(self) -> int:
        """Доказанный потолок одновременных попыток этого исполнителя."""
        return worker_slots.normalize_max_slots(self.config.max_slots).value

    def local_capacity(self) -> tuple[int, str]:
        """Сколько попыток исполнитель готов начать ПРЯМО СЕЙЧАС и что мешает.

        Считается по четырём независимым источникам, берётся худший:
      * потоки-наблюдатели этого воплощения;
      * попытки, захваченные локальной очередью (`claimed`/`running`);
      * ДОКАЗАННО живые процессы реестра (pid + тик старта);
      * лимит конфигурации, зажатый доказанным максимумом этапа.

        Это второй, независимый от центра рубеж (S-16): даже если центр по
        ошибке выдаст третье задание, третий процесс здесь не стартует.
        Критический диск обнуляет ёмкость, но НЕ трогает уже идущие процессы.
        """
        limit = self.slot_limit()
        self._workers = [t for t in self._workers if t.is_alive()]
        try:
            claimed = self.db.claimed_attempt_count()
            live = self.db.live_process_count()
        except Exception:  # noqa: BLE001 — база занята: считаем по своим потокам
            claimed = live = len(self._workers)
        busy = max(len(self._workers), claimed, live)
        try:
            disk = self.retention.disk_snapshot()
        except Exception:  # noqa: BLE001 — разрез диска не обязан быть доступен
            disk = {"level": "unknown"}
        if disk.get("level") == "critical":
            return 0, (
                "критически мало места на диске: новые попытки не запускаются, "
                "текущие продолжают работу"
            )
        free = max(0, limit - busy)
        return free, ("" if free else f"занято {busy} из {limit}")

    def _start_command_worker(self) -> None:
        """Отдельный поток под локальные команды.

        Раньше команды разбирались прямо в `_tick`, то есть в главном цикле.
        Отмена внутри своего гарантийного срока ждёт смерти процесса — до
        `grace_period_sec`, а его верхняя граница на центре 600 с. Всё это
        время главный цикл стоял: не считалась ёмкость, не захватывалась
        следующая попытка (освободившийся слот простаивал), не работало
        удержание, и ВТОРАЯ команда отмены ждала окончания первой. При двух
        слотах это ровно тот сценарий, ради которого второй слот и заводился.

        Поток один, поэтому команды по-прежнему исполняются строго по одной —
        порядок и идемпотентность не меняются, меняется только то, что цикл
        больше не стоит рядом.
        """

        def loop() -> None:
            while not self._stop.is_set():
                try:
                    if not self.drain_local_commands():
                        self._stop.wait(POLL_SEC)
                except Exception as exc:  # noqa: BLE001 — поток обязан выжить
                    _log(f"поток локальных команд: {exc!r} — продолжаю")
                    self._stop.wait(POLL_SEC)

        self._command_thread = threading.Thread(
            target=loop, name="executor-commands", daemon=True
        )
        self._command_thread.start()

    def _tick(self, max_jobs: Optional[int], started_ref: list[int]) -> None:
        """Один оборот главного цикла. Исключение отсюда цикл не роняет."""
        started = started_ref[0]
        self.retention.tick()
        free, _why = self.local_capacity()
        enough = max_jobs is not None and started >= max_jobs
        if enough and not self._workers:
            _log(f"выполнено попыток: {started} — останавливаюсь по max_jobs")
            raise _StopLoop
        if free <= 0 or enough:
            self._stop.wait(POLL_SEC)
            return
        # Ёмкость передаётся в захват: проверка и захват идут ОДНОЙ транзакцией,
        # иначе между ними два исполнителя (или два оборота цикла) успели бы
        # взять по попытке сверх лимита.
        item = self.db.claim_next(self.instance_id, capacity_limit=self.slot_limit())
        if item is None:
            self._stop.wait(POLL_SEC)
            return
        thread = threading.Thread(
            target=self._run_guarded, args=(item,),
            name=f"attempt-{item['attempt_id'][:8]}", daemon=True,
        )
        thread.start()
        self._workers.append(thread)
        started_ref[0] = started + 1

    def _run_guarded(self, item: dict[str, Any]) -> None:
        """Одна попытка не должна ронять исполнителя целиком."""
        try:
            self.run_attempt(item)
        except Exception as exc:  # noqa: BLE001 — исполнитель обязан выжить
            _log(f"попытка {item['attempt_id'][:8]} упала: {exc}")
            self.db.set_queue_state(
                item["attempt_id"], local_db.QUEUE_FAILED,
                result={"outcome": "executor_exception", "message": str(exc)},
            )
        finally:
            # Попытка закончена — держать её служебные объекты в памяти
            # долгоживущего исполнителя незачем.
            with self._outbox_lock:
                self._outboxes.pop(item["attempt_id"], None)
            self._cancelling.discard(item["attempt_id"])

    def shutdown(self) -> None:
        self._stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
        if self._command_thread is not None:
            # Ждём дольше: поток мог стоять в гарантийном сроке отмены.
            self._command_thread.join(timeout=10)
        # Процессы аудита НЕ трогаем: они в своих сессиях и переживают уход
        # исполнителя. Ждём только собственные потоки-наблюдатели.
        for thread in list(self._workers):
            thread.join(timeout=2)
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
        # Локальные команды, застрявшие в `processing` вместе с прошлым
        # воплощением: без возврата в очередь они не исполнялись бы никогда и
        # никогда не подтверждались бы центру. Исполнение идемпотентно.
        requeued = self.db.requeue_orphan_commands()
        if requeued:
            _log(f"возвращено в очередь локальных команд: {requeued}")

        for item in self.db.list_queue(states=(local_db.QUEUE_CLAIMED, local_db.QUEUE_RUNNING)):
            attempt_id = item["attempt_id"]
            if self.db.executor_alive(item.get("claimed_by_executor")):
                # Попытку держит ДРУГОЙ живой исполнитель. Забрать её — значит
                # завести второго наблюдателя за тем же процессом и второго
                # сборщика того же архива.
                _log(f"попытка {attempt_id[:8]} занята живым исполнителем — не трогаю")
                outcomes.append({"attempt_id": attempt_id, "verdict": "owned_by_peer"})
                continue
            job_dir = self.jobs.job_dir(item["job_id"], attempt_id)
            row = self.db.process_row(attempt_id)
            marker = read_completed_marker(job_dir)
            verdict = process_control.classify_after_restart(row, marker=marker)
            if verdict == "running":
                # Процесс пережил рестарт исполнителя — не трогаем и не
                # перезапускаем; наблюдение подхватит следующий цикл.
                # Владельца строки переписываем на себя: прошлое воплощение
                # мертво, и без этого продление аренды не обновляло бы ничего.
                self.db.adopt_claim(attempt_id, self.instance_id)
                self.db.set_queue_state(attempt_id, local_db.QUEUE_RUNNING)
                self._watch_survived(item, row or {})
                outcomes.append({"attempt_id": attempt_id, "verdict": "running"})
                continue
            if verdict == "exited":
                _log(f"попытка {attempt_id[:8]}: процесс отработал до рестарта — доупаковываю")
                self._finish_from_marker(item, marker or {})
                outcomes.append({"attempt_id": attempt_id, "verdict": "exited"})
                continue
            if (
                item.get("state") == local_db.QUEUE_CLAIMED
                and row is None
                and marker is None
            ):
                # Захвачено, но процесс так и не стартовал: терять тут нечего и
                # сирот быть не может. Возврат в очередь — не «повтор работы»
                # (I-03), потому что работа не начиналась.
                self.db.release_claim(attempt_id)
                outcomes.append({"attempt_id": attempt_id, "verdict": "requeued"})
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
                return
            # Процесс мог исчезнуть потому, что мы сами его отменили. Тогда
            # исход уже записан, и «executor_interrupted» поверх него — ложь.
            current = self.db.queue_item(item["attempt_id"]) or {}
            if current.get("state") in local_db.TERMINAL_QUEUE_STATES:
                return
            # Тот же долг, что и в упаковке: без явной отметки запись реестра
            # остаётся `running` и вечно блокирует удержание.
            self.db.update_process(item["attempt_id"], status="interrupted")
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

        # Последняя развилка перед запуском НАСТОЯЩЕГО процесса. Между
        # `claim_next` и этой строкой проходит подготовка (разбор параметров,
        # открытие outbox, лечение разрывов нумерации) — десятки миллисекунд и
        # больше. За это время оператор успевает отменить попытку: отмена
        # видит `claimed`, ставит `cancelled` и отвечает центру «локально не
        # выполняется». Если после этого запустить процесс, центр будет считать
        # попытку отменённой, слот — свободным, а на VPS будет идти работа,
        # результат которой уже некуда девать.
        #
        # Условная запись «только из claimed» превращает это в честную гонку с
        # одним победителем: не мы — значит попытка отменена, и стартовать
        # нельзя.
        started_running = self.db.set_queue_state(
            attempt_id, local_db.QUEUE_RUNNING,
            executor_instance_id=self.instance_id,
            expect_states=(local_db.QUEUE_CLAIMED,),
        )
        if not started_running:
            current = (self.db.queue_item(attempt_id) or {}).get("state")
            _log(
                f"попытка {attempt_id[:8]}: состояние сменилось на {current!r} "
                f"до старта процесса — не запускаю"
            )
            self.jobs.update(job_id, attempt_id, local_state=str(current or "unknown"))
            return {"ok": False, "reason": "superseded", "queue_state": current}
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
            # В файлы stdout.log/stderr.log пишет САМ процесс (test_runner
            # отдаёт ему дескрипторы). Здесь только событие для центра —
            # дублировать строку в файл значило бы записать её дважды.
            outbox.append(
                "log_line",
                {"level": level, "stage": "test_pipeline_v1", "source": stream,
                 "message": line},
            )

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
            # Архив собран прошлым воплощением. Само по себе это ещё не значит,
            # что центр об этом знает: падение могло случиться между сборкой и
            # событием. Событие повторяем — центр дедуплицирует по (job,
            # attempt, seq), а вот его отсутствие оставило бы готовый архив
            # незамеченным навсегда.
            if meta.get("local_state") != "uploaded":
                outbox.append(
                    "job_completed_locally",
                    {
                        "result_hash": meta.get("result_hash"),
                        "result_size": meta.get("result_size"),
                        "deferred_stages": [],
                    },
                )
            self.jobs.update(job_id, attempt_id, local_state="completed_locally")
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
        # Отметка процесса ставится и здесь, а не только на штатном пути в
        # `run_attempt`. Путь после рестарта исполнителя («процесс отработал,
        # пока нас не было» и «выживший процесс закончил») до сюда доходит с
        # записью реестра в статусе `running` — и оставлял её такой навсегда.
        # Удержание в этом состоянии удалять попытку отказывается («процесс
        # помечен работающим», retention.py), то есть архивы копились бы на
        # диске молча и без единой ошибки в журнале.
        self.db.update_process(
            attempt_id,
            status="exited" if int(meta.get("exit_code", 0) or 0) == 0 else "failed",
            exit_code=int(meta.get("exit_code", 0) or 0),
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

    def _outbox(self, item: dict[str, Any]) -> EventOutbox:
        """Единственный на попытку журнал событий (см. self._outboxes)."""
        attempt_id = item["attempt_id"]
        with self._outbox_lock:
            existing = self._outboxes.get(attempt_id)
            if existing is not None:
                return existing
            job_dir = self.jobs.job_dir(item["job_id"], attempt_id)
            meta = self.jobs.load(item["job_id"], attempt_id) or {}
            # Секреты вычищаются ПРИ ЗАПИСИ (I-12). Исполнителю известен только
            # токен попытки — worker-token ему не передают вовсе.
            #
            # Номер события выдаёт worker.db: в этот каталог пишет ещё и агент,
            # а он — ДРУГОЙ процесс. Файловый счётчик под flock для двух
            # процессов ненадёжен, и на двух слотах это перестаёт быть редкой
            # гонкой (см. local_db, миграция 2).
            outbox = EventOutbox(
                job_dir / "events",
                secret_literals=(meta.get("execution_token") or "",),
                sequence_db=self.db,
                job_id=item["job_id"],
                attempt_id=attempt_id,
            )
            self._outboxes[attempt_id] = outbox
            return outbox

    def _emit(self, item: dict[str, Any], event_type: str, payload: dict[str, Any]) -> None:
        self._outbox(item).append(event_type, payload)

    def _was_cancelled(self, attempt_id: str) -> bool:
        """Была ли отмена — включая начатую, но ещё не записанную в очередь.

        Проверять только очередь мало: поток команды ставит `cancelled` уже
        ПОСЛЕ того, как процесс умер, а поток задания к этому моменту успевает
        увидеть код возврата −15 и объявить обычный провал.
        """
        if attempt_id in self._cancelling:
            return True
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
            try:
                result = self.execute_local_command(command)
            except Exception as exc:  # noqa: BLE001 — команда не роняет цикл
                # Негодная нагрузка (битый job_id, нечисловая пауза) раньше
                # улетала из главного цикла и убивала исполнителя целиком.
                # Каждый рубеж держит оборону сам: центр валидирует на выдаче,
                # воркер — на приёме.
                _log(f"локальная команда {command.get('command_type')} упала: {exc!r}")
                result = {
                    "status": "error",
                    "detail": {"outcome": "command_failed", "message": str(exc)[:300]},
                }
            self.db.complete_local_command(command["local_command_id"], result)
            results.append(result)

    def execute_local_command(self, command: dict[str, Any]) -> dict[str, Any]:
        ctype = command.get("command_type")
        try:
            payload = json.loads(command.get("payload_json") or "{}")
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if ctype == "cancel_attempt":
            return self.cancel_attempt(
                job_id=str(payload.get("job_id") or command.get("job_id") or ""),
                attempt_id=str(payload.get("attempt_id") or command.get("attempt_id") or ""),
                grace_period_sec=_grace_period(payload.get("grace_period_sec")),
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

    def _cancel_lost_race(self, attempt_id: str, observed: str) -> dict[str, Any]:
        """Состояние попытки сменилось между чтением и записью отмены.

        Отвечаем ОШИБКОЙ, а не «отменено»: центр по ошибке состояние попытки не
        меняет и слот не освобождает, а команда отмены останется невыполненной
        и придёт снова. Повторять здесь же нельзя — в этот момент поток задания
        как раз стартует процесс, и мы бы крутились в рекурсии вместо того,
        чтобы дать ему дописать состояние.
        """
        current = (self.db.queue_item(attempt_id) or {}).get("state")
        _log(
            f"отмена {attempt_id[:8]}: состояние сменилось {observed!r} → "
            f"{current!r} — отмена не применена, команда будет повторена"
        )
        return {
            "status": "error",
            "detail": {
                "outcome": "state_changed_concurrently",
                "observed_state": observed,
                "current_state": current,
                "reason": "попытка меняла состояние во время отмены; повторите команду",
            },
        }

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
                # Условие «состояние не менялось с момента чтения» обязательно:
                # попытка могла быть в `claimed`, и прямо сейчас поток задания
                # переводит её в `running` и запускает процесс. Проиграв эту
                # гонку, мы не имеем права отвечать «локально не выполняется» —
                # центр по такому ответу объявит попытку отменённой и отдаст
                # слот, пока процесс работает.
                observed = str(queue.get("state") or "")
                if not self.db.set_queue_state(
                    attempt_id, local_db.QUEUE_CANCELLED,
                    result={"outcome": "not_running_locally"},
                    expect_states=(observed,),
                ):
                    return self._cancel_lost_race(attempt_id, observed)
                self.jobs.update(job_id, attempt_id, local_state="cancelled")
            return {"status": "ok",
                    "detail": {"outcome": process_control.OUTCOME_NOT_RUNNING}}
        if row is None and marker is not None:
            return {"status": "ok",
                    "detail": {"outcome": process_control.OUTCOME_ALREADY_COMPLETED}}

        # Отпечаток берём из ВТОРОГО, независимого источника — metadata.json
        # попытки. Совпадение двух записей и есть доказательство «наш процесс»:
        # сверять реестр сам с собой бессмысленно (§10, I-17).
        meta = self.jobs.load(job_id, attempt_id) or {}
        owned, why = process_control.verify_ownership(
            row,
            job_id=job_id,
            attempt_id=attempt_id,
            expected_fingerprint=meta.get("command_fingerprint"),
        )
        if not owned:
            if marker is not None:
                return {"status": "ok",
                        "detail": {"outcome": process_control.OUTCOME_ALREADY_COMPLETED}}
            if row and not process_registry.is_alive(
                int(row.get("pid") or 0), row.get("process_start_identity")
            ):
                observed = str((queue or {}).get("state") or "")
                if observed and not self.db.set_queue_state(
                    attempt_id, local_db.QUEUE_CANCELLED,
                    result={"outcome": "not_running_locally"},
                    expect_states=(observed,),
                ):
                    return self._cancel_lost_race(attempt_id, observed)
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
        # Признак ставится ДО сигнала: между смертью процесса и записью
        # состояния в очередь есть окно, и в нём поток задания не должен
        # принять отмену за провал.
        self._cancelling.add(attempt_id)
        detail = process_control.terminate(row, grace_period_sec=grace_period_sec)
        if detail.get("outcome") != process_control.OUTCOME_CANCELLED:
            self._cancelling.discard(attempt_id)
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
