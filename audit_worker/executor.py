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
    audit_runner,
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

#: Имена типов заданий. Именно ИМЕНА: реализацию по ним выбирает исполнитель.
AUDIT_JOB_TYPE_TEST = "test_pipeline_v1"
AUDIT_JOB_TYPE_REAL = "audit_pipeline_v1"

#: Реальный аудит занимает VPS целиком. Доказанный максимум — один.
REAL_AUDIT_MAX_SLOTS = 1


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


class _GrantPending(Exception):
    """Разрешения оператора ещё нет — это ОЖИДАНИЕ, а не провал.

    Отдельный тип, а не `AuditJobRejected`, потому что исход другой: попытка
    возвращается в очередь и повторяется, как при занятом слоте. Разрешение
    привязано к заданию и физически не может быть выписано раньше, чем задание
    создано, — значит окно «задание есть, разрешения ещё нет» штатное.
    """


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


def _grantable_providers() -> frozenset[str]:
    """Провайдеры, которым воркер вообще умеет выписать разрешение.

    Берётся из реестра провайдерского слоя, а не перечисляется здесь строками:
    имена настоящих CLI в пакете воркера запрещены отдельным рубежом
    (`test_no_real_cli_names_in_the_worker_package`), и дублирование их
    литералами разошлось бы с реестром при первом же добавлении провайдера.
    Для провайдера вне набора отказ даёт резолвер маршрутов — там он
    содержательнее («план требует X, которого воркер не поддерживает»).
    """
    from audit_worker.providers.paths import SUPPORTED_PROVIDERS

    return frozenset(SUPPORTED_PROVIDERS)


def _routes_from_plan(
    params: "audit_runner.SafeAuditParams",
) -> tuple[tuple[tuple[str, str], ...], str]:
    """Пары «провайдер + способность» и хэш ИЗ ЗАМОРОЖЕННОГО ПЛАНА.

    Разбор намеренно примитивен и не импортирует доменную схему платформы:
    воркер не имеет права зависеть от внутренностей конвейера (та же граница,
    что держит `audit_runner` в стороне от провайдерского слоя). Нужны ровно
    два ответа — какие маршруты разрешить локальной политике и с каким хэшем
    сверяться. Смысл плана проверяет процесс конвейера, которому план и
    адресован.

    Берутся ТОЛЬКО действия worker-области: нормативный хвост исполняет центр,
    и требовать под него локальную модель значило бы отказывать воркеру за
    отсутствие того, чем он всё равно не воспользуется.
    """
    plan = getattr(params, "routing_plan", None)
    if not isinstance(plan, dict):
        return (), ""
    routes: set[tuple[str, str]] = set()
    for stage in plan.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        if str(stage.get("execution_scope") or "") != "worker":
            continue
        for action in stage.get("actions") or []:
            if not isinstance(action, dict):
                continue
            if str(action.get("kind") or "model") != "model":
                continue
            provider = str(action.get("provider") or "").strip()
            capability = str(action.get("capability") or "").strip()
            if provider and capability:
                routes.add((provider, capability))
    return tuple(sorted(routes)), str(plan.get("routing_plan_hash") or "")


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
        # Какой провайдер привязан к какой попытке. Нужно ровно для отметки в
        # heartbeat: сам факт вызова знает журнал попытки, а «кем» — привязка,
        # и держать её в памяти дешевле, чем перечитывать файл после прогона.
        self._bound_providers: dict[str, str] = {}
        # Когда попытка ВПЕРВЫЕ упёрлась в отсутствие разрешения оператора.
        # В памяти, а не на диске: это срок ожидания решения человека, и после
        # перезапуска исполнителя ему честно начинаться заново.
        self._grant_wait_since: dict[str, float] = {}

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
        # Ёмкость передаётся в захват, и ВНУТРИ транзакции claim_next сверяет с
        # ней счётчик состояний очереди (claimed/running) — иначе между «посчитал»
        # и «захватил» два исполнителя (или два оборота цикла) успели бы взять по
        # попытке сверх лимита.
        #
        # Честная граница: транзакционен только этот счётчик. Три остальных
        # наблюдения local_capacity — живые потоки, доказанно живые процессы
        # реестра и уровень диска — посчитаны ВЫШЕ по коду и внутрь транзакции не
        # попадают. Практически это значит: живой процесс, чья строка очереди уже
        # терминальна, для транзакционного счётчика невидим, и ловит его только
        # внешняя проверка. Второй исполнитель поверх той же worker.db лимит всё
        # равно не превысит (счёт идёт по таблице, а не по памяти процесса).
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
            self._bound_providers.pop(item["attempt_id"], None)

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
        """Развилка по типу задания. Реализацию выбирает ВОРКЕР, а не центр.

        Тип задания — имя, а не описание того, что запускать. Неизвестное имя
        отвергается: канала «выполни произвольное» нет и появиться не может.
        """
        job_type = str(item.get("job_type") or AUDIT_JOB_TYPE_TEST)
        if job_type == AUDIT_JOB_TYPE_REAL:
            return self.run_audit_attempt(item)
        if job_type != AUDIT_JOB_TYPE_TEST:
            self.db.set_queue_state(
                item["attempt_id"], local_db.QUEUE_FAILED,
                result={"outcome": "rejected",
                        "message": f"неизвестный тип задания {job_type!r}"},
            )
            return {"ok": False, "reason": "unknown_job_type"}
        return self._run_test_attempt(item)

    def _run_test_attempt(self, item: dict[str, Any]) -> dict[str, Any]:
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
        conflict = self.test_slot_conflict(attempt_id)
        if conflict:
            # Пока идёт реальный аудит, тестовые задания не стартуют: они
            # делят с ним CPU-пул и память (§28 задания). Попытка ждёт в
            # очереди, в failed не переводится.
            self.db.set_queue_state(attempt_id, local_db.QUEUE_QUEUED)
            _log(f"тестовое задание {attempt_id[:8]} ждёт слот: {conflict}")
            return {"ok": False, "reason": "waiting_for_slot", "detail": conflict}
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

    # ─── Реальный аудит ──────────────────────────────────────────────────────
    def audit_slot_conflict(self, attempt_id: str) -> Optional[str]:
        """Почему реальный аудит стартовать нельзя. None = можно.

        Два правила, оба жёсткие: одновременно идёт не больше одного реального
        аудита, и он не смешивается с тестовыми заданиями. Обоснование не
        «осторожность», а измеримое: реальный аудит занимает VPS целиком
        (CPU-пул, память, диск под кропы), и вывод «два test_pipeline_v1
        работают» на него не переносится.
        """
        rows = self.db.list_queue(
            states=(local_db.QUEUE_CLAIMED, local_db.QUEUE_RUNNING)
        )
        others = [r for r in rows if r.get("attempt_id") != attempt_id]
        audits = [r for r in others if r.get("job_type") == AUDIT_JOB_TYPE_REAL]
        tests = [r for r in others if r.get("job_type") != AUDIT_JOB_TYPE_REAL]
        if len(audits) >= REAL_AUDIT_MAX_SLOTS:
            return (
                f"на воркере уже идёт реальный аудит "
                f"({len(audits)}/{REAL_AUDIT_MAX_SLOTS})"
            )
        if tests:
            return (
                f"на воркере идут тестовые задания ({len(tests)}) — реальный "
                "аудит стартует только после их завершения"
            )
        return None

    def test_slot_conflict(self, attempt_id: str) -> Optional[str]:
        """Обратная сторона того же правила: пока идёт аудит, тесты не стартуют."""
        rows = self.db.list_queue(
            states=(local_db.QUEUE_CLAIMED, local_db.QUEUE_RUNNING)
        )
        audits = [
            r for r in rows
            if r.get("attempt_id") != attempt_id
            and r.get("job_type") == AUDIT_JOB_TYPE_REAL
        ]
        if audits:
            return "на воркере идёт реальный аудит — тестовые задания ждут"
        return None

    def _provider_dir(self) -> Optional[Path]:
        """Каталог поддельных провайдеров либо None для настоящих.

        Fail-closed: если настоящие модели не разрешены, а подделок нет —
        задание отвергается. Молча уйти к настоящему CLI нельзя.
        """
        if getattr(self.config, "allow_real_llm", False):
            return None
        configured = getattr(self.config, "fake_provider_dir", None)
        if configured is None:
            raise audit_runner.AuditJobRejected(
                "Настоящие модели запрещены (AUDIT_WORKER_ALLOW_REAL_LLM=false), "
                "а каталог поддельных провайдеров не задан "
                "(AUDIT_WORKER_FAKE_PROVIDER_DIR)"
            )
        path = Path(configured)
        if not path.is_dir():
            raise audit_runner.AuditJobRejected(
                f"Каталог поддельных провайдеров не найден: {path}"
            )
        # Существующего каталога недостаточно. Пустой (или указанный на
        # `~/.local/bin`) каталог префиксует PATH, ничего не перекрывая, — и
        # `which("claude")` находит НАСТОЯЩИЙ CLI, пока воркер рапортует центру
        # provider_mode="fake". Маркер каталога — единственное, что превращает
        # это заявление в проверяемый факт.
        if not audit_runner.provider_dir_is_fake(path):
            raise audit_runner.AuditJobRejected(
                f"Каталог {path} не помечен как поддельный (нет PROVIDERS.json "
                "с mode=fake либо в нём нет нужных подделок). Настоящие модели "
                "запрещены, поэтому задание отвергнуто."
            )
        return path

    def _grant_wait_elapsed(self, attempt_id: str) -> float:
        """Сколько попытка уже ждёт разрешения оператора."""
        started = self._grant_wait_since.setdefault(attempt_id, time.time())
        return max(0.0, time.time() - started)

    def _forbidden_literals(self) -> tuple[str, ...]:
        """Значения, которых не должно быть в ответе модели.

        Читаются из файла оператора, а не из кода: контрольная строка, лежащая
        в репозитории, превращает «в ответе её не нашли» в утверждение о
        репозитории, а не о модели. Отсутствие файла — не ошибка: проверка
        просто останется без литералов, и отчёт это покажет числом.
        """
        path = getattr(self.config, "provider_forbidden_literals_file", None)
        if not path:
            return ()
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            _log(f"файл контрольных литералов не прочитан: {exc}")
            return ()
        return tuple(
            line.strip() for line in text.splitlines()
            if line.strip() and not line.startswith("#") and len(line.strip()) >= 8
        )

    def prepare_provider_binding(
        self, item: dict[str, Any], params: "audit_runner.SafeAuditParams"
    ) -> Optional[Path]:
        """Выбрать провайдера, списать разрешение и выписать привязку.

        Порядок обязателен и не переставляется:

          1. рубеж машины — администратор VPS разрешил каналу существовать;
          2. режим провайдеров — привязка не имеет смысла в fake-режиме, где
             конвейер по построению ходит к подделкам;
          3. РЕЗОЛВ — провайдер установлен, не заблокирован политикой и
             авторизован. Отказ здесь ничего не стоит: разрешение ещё цело;
          4. СПИСАНИЕ разрешения оператора — последним из проверок и ДО запуска
             процесса. Дальше любое падение работает в безопасную сторону:
             попытка засчитана, второго бесплатного прогона не будет;
          5. запись привязки в каталог попытки.

        Возвращает путь к привязке либо None, если задание модели не требует.
        """
        requirement_payload = params.provider_requirement
        if not requirement_payload or int(requirement_payload.get("max_inferences") or 0) <= 0:
            return None

        # Импорт локальный: провайдерский слой не должен тянуться в исполнитель
        # на путях, где модель не требуется вовсе.
        from audit_worker.providers import inference_grant
        from audit_worker.providers.manager import ProviderManager
        from audit_worker.providers.resolver import (
            ProviderRequirement,
            ProviderResolutionError,
            ProviderResolver,
            ambient_root_for_attempt,
        )

        if not getattr(self.config, "pipeline_provider_bridge_enabled", False):
            raise audit_runner.AuditJobRejected(
                "задание требует вызова модели из конвейера, но канал не "
                "разрешён администратором VPS "
                "(AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED=false)"
            )
        if not getattr(self.config, "allow_real_llm", False):
            raise audit_runner.AuditJobRejected(
                "задание требует вызова модели, но настоящие модели на этом "
                "воркере запрещены (AUDIT_WORKER_ALLOW_REAL_LLM=false)"
            )

        job_id, attempt_id = item["job_id"], item["attempt_id"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        try:
            requirement = ProviderRequirement.from_payload(requirement_payload)
        except ProviderResolutionError as exc:
            raise audit_runner.AuditJobRejected(str(exc)) from None
        assert requirement is not None

        manager = ProviderManager(
            worker_root=self.config.root,
            enabled=True,
            timeout_sec=max(120.0, self.config.provider_timeout_sec),
            account_groups=dict(self.config.provider_account_groups or {}),
            policy_blocked=dict(self.config.provider_policy_blocked or {}),
            auth_modes=dict(self.config.provider_auth_modes or {}),
            executables=dict(self.config.provider_executables or {}),
            # Менеджер строит адаптеры ДЛЯ НАБЛЮДЕНИЯ. Разрешение на вызов
            # модели даёт только мост, и только по выписанной привязке.
            inference_allowed=False,
            log=lambda message: _log(f"provider: {message}"),
        )
        manager.refresh(force=True)
        resolver = ProviderResolver(manager, worker_root=self.config.root)
        provider_root = ambient_root_for_attempt(job_dir, requirement.provider)
        # Разрешение по заданию центра (этап 11G). Выписывается ДО списания и
        # идемпотентно по попытке: повторный вход возвращает ту же запись с уже
        # потраченными единицами. Если автоматические разрешения на машине не
        # включены, ничего не происходит и работает прежний путь — файл,
        # созданный оператором.
        auto_grant_enabled = bool(
            getattr(self.config, "pipeline_provider_auto_grant_enabled", False)
        )

        # ПРОВАЙДЕРЫ, ЧЬИ ПОДПИСКИ ТРАТИТ ЭТО ЗАДАНИЕ (этап 11I).
        #
        # До 11I их был ровно один: привязка несла одну модель на попытку, и
        # разрешение оператора совпадало с ней по построению. План требует
        # нескольких, а разрешение осталось бы одним — план `claude_gpt_codex`
        # тратил бы codex-подписку под claude-разрешением, и в файле грантов не
        # было бы ни одной записи по codex. Разрешение — учёт чужих денег и
        # чужих лимитов, и «списали не с той подписки» здесь не мелочь.
        #
        # Провайдер требования идёт ПЕРВЫМ: его грант остаётся тем, который
        # попадает в привязку и в evidence, — прежнее поведение сохраняется.
        required_routes, plan_hash = _routes_from_plan(params)
        grant_providers: list[str] = [requirement.provider]
        for provider_name, _capability in required_routes:
            if provider_name in grant_providers:
                continue
            if provider_name not in _grantable_providers():
                # Провайдера, которого воркер не поддерживает, до резолва
                # маршрутов задание всё равно не доживёт — там отказ
                # содержательнее («план требует X, воркер его не знает»).
                continue
            grant_providers.append(provider_name)

        # Разрешение по заданию центра (этап 11G). Выписывается ДО списания и
        # идемпотентно по попытке: повторный вход возвращает ту же запись с уже
        # потраченными единицами. Если автоматические разрешения на машине не
        # включены, ничего не происходит и работает прежний путь — файл,
        # созданный оператором.
        if auto_grant_enabled:
            for provider_name in grant_providers:
                try:
                    issued = inference_grant.issue_for_job(
                        self.config.root,
                        provider=provider_name,
                        job_id=str(job_id),
                        attempt_id=str(attempt_id),
                        capability=str(requirement.capability or ""),
                        requested_max_inferences=int(requirement.max_inferences),
                        machine_ceiling=int(
                            getattr(self.config, "pipeline_provider_max_inferences", 0)
                        ),
                        ttl_sec=float(
                            getattr(
                                self.config, "pipeline_provider_grant_ttl_sec", 6 * 3600.0
                            )
                        ),
                    )
                except inference_grant.InferenceGrantError as exc:
                    # Отказ рубежа машины. Ожидание оператора здесь бессмысленно:
                    # потолок не появится сам, а требование задания не изменится.
                    raise audit_runner.AuditJobRejected(
                        f"автоматическое разрешение для провайдера "
                        f"{provider_name!r} не выписано: {exc}"
                    ) from None
                _log(
                    f"разрешение {issued.grant_id} выписано автоматически по "
                    f"заданию центра: {issued.max_uses} обращений, провайдер "
                    f"{provider_name!r}, способность {requirement.capability!r}"
                )

        # Разрешения ДОПОЛНИТЕЛЬНЫХ провайдеров плана списываются здесь, до
        # основного: если чужой подписки не разрешили, задание не должно
        # начинаться вовсе — иначе часть ансамбля отработает, а часть упрётся
        # в отказ уже после оплаты.
        for provider_name in grant_providers[1:]:
            try:
                extra_grant = inference_grant.consume(
                    self.config.root, provider=provider_name, task_id=str(job_id)
                )
            except inference_grant.InferenceGrantError as exc:
                raise audit_runner.AuditJobRejected(
                    f"нет разрешения оператора на вызовы провайдера "
                    f"{provider_name!r}, которого требует план: {exc}"
                ) from None
            _log(
                f"разрешение {extra_grant.grant_id} списано: "
                f"{extra_grant.used}/{extra_grant.max_uses} по {extra_grant.provider}"
            )

        try:
            grant = inference_grant.consume(
                self.config.root, provider=requirement.provider, task_id=str(job_id)
            )
        except inference_grant.InferenceGrantError as exc:
            waited = self._grant_wait_elapsed(str(attempt_id))
            limit = float(
                getattr(self.config, "pipeline_provider_grant_wait_sec", 0.0) or 0.0
            )
            if waited < limit:
                raise _GrantPending(
                    f"ждёт разрешения оператора ({int(waited)}/{int(limit)} с): {exc}"
                ) from None
            raise audit_runner.AuditJobRejected(
                f"нет разрешения оператора на вызов модели: {exc}"
            ) from None
        _log(
            f"разрешение {grant.grant_id} списано: "
            f"{grant.used}/{grant.max_uses} по {grant.provider}"
        )
        try:
            binding = resolver.resolve(
                requirement,
                job_id=str(job_id),
                attempt_id=str(attempt_id),
                task_id=str(job_id),
                grant_id=grant.grant_id,
                provider_root=provider_root,
                forbidden_literals=self._forbidden_literals(),
                required_routes=required_routes,
                routing_plan_hash=plan_hash,
            )
        except ProviderResolutionError as exc:
            # Разрешение уже списано и НЕ возвращается: единица тратится за
            # попытку, а не за успех. Возврат сделал бы бесконечным цикл
            # «попробовать ещё раз» на сломанной машине.
            raise audit_runner.AuditJobRejected(
                f"провайдер не выбран: {exc} (разрешение {grant.grant_id} "
                "уже списано и не возвращается)"
            ) from None
        audit_runner.prepare_job_dir(job_dir)
        path = binding.write(job_dir / "metadata")
        self._grant_wait_since.pop(str(attempt_id), None)
        self._bound_providers[str(attempt_id)] = binding.provider
        _log(
            f"привязка провайдера выписана: {binding.provider} "
            f"({binding.auth_mode}), этапы {list(binding.allowed_stages)}"
            + (
                f"; маршрутов плана {len(binding.routes)} "
                f"({', '.join(sorted({r.provider for r in binding.routes}))})"
                if binding.routes else ""
            )
        )
        return path

    def run_audit_attempt(self, item: dict[str, Any]) -> dict[str, Any]:
        """Выполнить `audit_pipeline_v1` в изолированном каталоге попытки."""
        job_id, attempt_id = item["job_id"], item["attempt_id"]
        job_dir = self.jobs.job_dir(job_id, attempt_id)
        meta = self.jobs.load(job_id, attempt_id) or {}
        outbox = self._outbox(item)
        provider_binding: Optional[Path] = None

        try:
            params = audit_runner.validate_params(
                json.loads(item.get("params_json") or "{}"), config=self.config
            )
            provider_dir = self._provider_dir()
            conflict = self.audit_slot_conflict(attempt_id)
            if conflict:
                # Не провал: попытка возвращается в очередь и ждёт слот.
                # ВАЖНО: проверка слота стоит ДО подготовки привязки — иначе
                # ожидание слота списывало бы разрешение оператора.
                self.db.set_queue_state(attempt_id, local_db.QUEUE_QUEUED)
                _log(f"аудит {attempt_id[:8]} ждёт слот: {conflict}")
                return {"ok": False, "reason": "waiting_for_slot", "detail": conflict}
            provider_binding = self.prepare_provider_binding(item, params)
        except _GrantPending as exc:
            # Не провал: попытка возвращается в очередь ровно так же, как при
            # занятом слоте, и ждёт решения человека.
            self.db.set_queue_state(attempt_id, local_db.QUEUE_QUEUED)
            _log(f"аудит {attempt_id[:8]}: {exc}")
            return {"ok": False, "reason": "waiting_for_grant", "detail": str(exc)}
        except audit_runner.AuditJobRejected as exc:
            self.db.set_queue_state(
                attempt_id, local_db.QUEUE_FAILED,
                result={"outcome": "rejected", "message": str(exc)},
            )
            self.jobs.update(job_id, attempt_id, local_state="rejected", error=str(exc))
            outbox.append("job_failed", {"code": "params_rejected", "reason": "error",
                                         "message": str(exc)})
            self._grant_wait_since.pop(str(attempt_id), None)
            return {"ok": False, "reason": "rejected"}

        started_running = self.db.set_queue_state(
            attempt_id, local_db.QUEUE_RUNNING,
            executor_instance_id=self.instance_id,
            expect_states=(local_db.QUEUE_CLAIMED,),
        )
        if not started_running:
            current = (self.db.queue_item(attempt_id) or {}).get("state")
            _log(f"аудит {attempt_id[:8]}: состояние сменилось на {current!r} — не запускаю")
            return {"ok": False, "reason": "superseded", "queue_state": current}

        self.jobs.update(job_id, attempt_id, local_state="running", started_at=time.time())
        outbox.append("job_started", {
            "stage": AUDIT_JOB_TYPE_REAL,
            "profile": params.execution_profile,
            "action": params.action,
            "provider_mode": "fake" if provider_dir else "real",
            # Факт наличия привязки, а не её содержимое: путь к файлу и
            # контрольные литералы оператора центру не нужны.
            "provider_bridge": bool(provider_binding),
        })

        def on_progress(event: dict[str, Any]) -> None:
            kind = str(event.get("type") or "")
            outbox.append(
                "stage_progress" if kind == "stage_progress" else kind,
                {k: v for k, v in event.items() if k != "type"},
            )
            self.db.renew_lease(attempt_id, self.instance_id)

        def on_log(stream: str, level: str, line: str) -> None:
            outbox.append(
                "log_line",
                {"level": level, "stage": AUDIT_JOB_TYPE_REAL, "source": stream,
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
                pid=pid, command_fingerprint=fingerprint,
                process_start_time=process_registry.process_start_time(pid),
                process_group_id=pgid, process_status="running",
                executor_instance_id=self.instance_id,
            )

        outcome = audit_runner.run_audit_job(
            params=params,
            job_dir=job_dir,
            job_id=job_id,
            attempt_id=attempt_id,
            project_id=str(meta.get("project_id") or ""),
            version_id=meta.get("version_id"),
            config=self.config,
            provider_dir=provider_dir,
            provider_binding=provider_binding,
            on_progress=on_progress,
            on_log=on_log,
            on_start=on_start,
        )
        write_completed_marker(
            job_dir,
            test_runner.RunOutcome(
                exit_code=outcome.exit_code,
                duration_sec=outcome.duration_sec,
                steps_done=outcome.stages_done,
                steps_total=outcome.stages_total,
                failed_message=outcome.failed_message,
                stdout_lines=outcome.stdout_lines,
                stderr_lines=outcome.stderr_lines,
            ),
        )
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

        missing = audit_runner.missing_required_artifacts(
            job_dir, params.required_result_artifacts
        )
        # Журнал вызовов модели уезжает в пакет как evidence: без него
        # «оплачен ровно один вызов» — заявление, а не факт.
        self._announce_inference_ledger(job_dir, attempt_id, job_id, outbox)
        if outcome.exit_code != 0 or missing:
            # Пакет НЕ помечается успешным при отсутствии обязательного
            # артефакта: «успех без 03_findings.json» — худший из исходов,
            # потому что он проходит все проверки транспорта.
            message = outcome.failed_message or (
                f"нет обязательных артефактов: {', '.join(missing)}" if missing
                else f"конвейер вернул код {outcome.exit_code}"
            )
            outbox.append("stage_completed", {"stage": AUDIT_JOB_TYPE_REAL,
                                              "status": "error",
                                              "duration_sec": round(outcome.duration_sec, 2)})
            outbox.append("job_failed", {"code": "audit_pipeline_failed",
                                         "message": message,
                                         "stage": AUDIT_JOB_TYPE_REAL,
                                         "reason": "error",
                                         "missing_artifacts": missing})
            self.jobs.update(job_id, attempt_id, local_state="failed")
            self.db.set_queue_state(attempt_id, local_db.QUEUE_FAILED,
                                    result={"outcome": "failed",
                                            "exit_code": outcome.exit_code,
                                            "missing_artifacts": missing})
            return {"ok": False, "reason": "audit_pipeline_failed", "missing": missing}

        outbox.append(
            "stage_completed",
            {"stage": AUDIT_JOB_TYPE_REAL, "status": "done",
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
        job_type = str(item.get("job_type") or meta.get("job_type")
                       or AUDIT_JOB_TYPE_TEST)
        is_audit = job_type == AUDIT_JOB_TYPE_REAL
        audit_manifest = read_json(job_dir / "result" / "audit_manifest.json") or {}
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
            job_type=job_type,
            required_artifacts=(
                list(
                    audit_runner.required_artifacts_for(
                        str(audit_manifest.get("action") or "")
                    )
                ) if is_audit else None
            ),
            pipeline_revision=(
                audit_manifest.get("pipeline_revision") if is_audit else None
            ),
            stage_completion=(
                audit_manifest.get("stage_completion") if is_audit else None
            ),
            resume_hint=(audit_manifest.get("resume_hint") if is_audit else None),
            # Что ФАКТИЧЕСКИ применялось. Источник — манифест, который написал
            # сам процесс конвейера: исполнитель тут ничего не додумывает, он
            # переносит факт в пакет, чтобы центр мог его проверить.
            project_version_rel=(
                (meta.get("package") or {}).get("version_relative_path")
                if is_audit else None
            ),
            runtime_snapshot_hash=(
                (audit_manifest.get("applied_runtime_config") or {}).get(
                    "runtime_snapshot_hash"
                ) if is_audit else None
            ),
            applied_write_mode=(
                (audit_manifest.get("applied_runtime_config") or {}).get(
                    "applied_write_mode"
                ) if is_audit else None
            ),
            execution_profile=(audit_manifest.get("profile") if is_audit else None),
            worker_stage_plan=(
                audit_manifest.get("worker_stage_plan") if is_audit else None
            ),
            completed_stages=(
                audit_manifest.get("completed_stages") if is_audit else None
            ),
            forbidden_stages_not_run=(
                audit_manifest.get("forbidden_stages_not_run") if is_audit else None
            ),
            provider_mode=(audit_manifest.get("provider_mode") if is_audit else None),
            # Дисциплина и хэш ФАКТИЧЕСКИ применённого профиля. Источник —
            # манифест процесса конвейера: исполнитель переносит факт, а не
            # повторяет задание.
            discipline_id=(audit_manifest.get("discipline_id") if is_audit else None),
            discipline_profile_hash=(
                (audit_manifest.get("applied_discipline_profile") or {}).get(
                    "discipline_profile_hash"
                ) if is_audit else None
            ),
            source_integrity=(
                audit_manifest.get("source_integrity") if is_audit else None
            ),
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

    def _announce_inference_ledger(
        self, job_dir: Path, attempt_id: str, job_id: str, outbox: EventOutbox
    ) -> None:
        """Событие со СВОДКОЙ журнала вызовов модели. Без промптов и ответов.

        В событие уходят только числа и состояния ключей: сырой ответ модели в
        heartbeat и в EventOutbox не имеет права попасть (§6 задания), а
        «сколько оплаченных вызовов сделала эта попытка» центр обязан видеть.
        """
        try:
            from audit_worker.providers.inference_ledger import InferenceLedger

            summary = InferenceLedger(
                job_dir, attempt_id=attempt_id, job_id=job_id
            ).summary()
        except Exception as exc:                   # noqa: BLE001 — учёт не блокер
            _log(f"свод журнала вызовов не собран: {exc!r}")
            return
        if not summary.get("keys"):
            return
        try:
            from audit_worker.providers import pipeline_status

            pipeline_status.record(
                self.config.root,
                provider=self._bound_providers.get(attempt_id, ""),
                calls_started=int(summary.get("calls_started", 0)),
                calls_completed=int(summary.get("calls_completed", 0)),
            )
        except Exception as exc:                   # noqa: BLE001 — отметка не блокер
            _log(f"отметка о вызове конвейера не записана: {exc!r}")
        outbox.append("stage_progress", {
            "stage": AUDIT_JOB_TYPE_REAL,
            "unit": "inference_calls",
            "processed": int(summary.get("calls_completed", 0)),
            "total": int(summary.get("calls_started", 0)),
            "percent_reliable": False,
            "last_significant_event": "журнал вызовов модели",
            "inference_ledger": {
                "calls_started": summary.get("calls_started"),
                "calls_completed": summary.get("calls_completed"),
                "keys": [
                    {"key": row.get("key"), "state": row.get("state")}
                    for row in summary.get("keys", [])
                ],
            },
        })

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
