"""Слоты исполнения: сколько попыток воркер вправе вести одновременно.

Один модуль на всю подсистему — намеренно. До этого этапа «сколько можно»
считалось в четырёх местах по-разному: `record_heartbeat` зажимал значение в
[0..5], `ResourceMonitor` считал свой `calculated_free`, `Executor._tick`
сравнивал число потоков с `config.max_slots`, а центр при выдаче задания не
считал НИЧЕГО. Пока слот был один, расхождение не проявлялось; на двух оно
становится дефектом планировщика.

Главные решения этапа и их обоснование:

**Доказанный максимум — 2, и это не то же самое, что «поддержано 5».**
`MAX_VERIFIED_SLOTS = 2` — граница, которую закрывают тесты и живой smoke.
Значение больше двух не отвергается грубо (оператор не должен ловить 500 из-за
цифры в конфиге), а зажимается до двух с явным предупреждением, которое видно
и в API, и на экране. Заявлять поддержку 3–5 без прогона было бы ровно тем
враньём, которое эта подсистема запрещает себе везде.

**`assigned` НЕ занимает слот исполнения.** Задание §16 предлагает считать
`assigned` занятым слотом. В этой архитектуре так нельзя: назначение делает
ОПЕРАТОР при создании задания (ADR-004, автовыбора нет), поэтому `assigned`
означает «лежит в очереди центра», а не «исполняется на VPS». Если считать его
занятым, три созданных задания при лимите 2 заблокировали бы выдачу вообще —
включая первое. Поэтому граница проведена по факту передачи работы воркеру:

    занято    = source_uploading | source_ready | accepted_by_worker
                | running | cancel_requested
    ждёт слот = assigned

`cancel_requested` остаётся занятым до фактической остановки: пока воркер не
подтвердил, процесс может работать (I-06). `completed_locally`,
`result_uploading` и терминальные состояния слот НЕ занимают — процесс уже
завершён, а передача архива ограничивается сетью и диском, а не слотом.

**Признанная потерянной попытка не считается доказанно остановленной.**
`mark_lost` меняет только ось disposition; процесс на VPS может продолжать
работу. Принятая политика (§34 задания) такова:

  * такая попытка НЕ занимает слот центра — иначе один mark-lost навсегда
    съедал бы единственный слот воркера, и задание нельзя было бы повторить
    вообще;
  * она попадает в отдельный счётчик `unproven_remote`, который виден в API и
    на экране как предупреждение «фактических процессов на VPS может быть
    больше»: центр не изображает знание, которого у него нет (I-06);
  * настоящим рубежом остаётся ЛОКАЛЬНАЯ проверка ёмкости исполнителя
    (S-16): даже если центр ошибётся, третий процесс не стартует;
  * новая попытка на воркере, с которым НЕТ связи, требует явного признания
    риска (`accept_capacity_risk`) — там центр не может ни увидеть чужие
    процессы, ни попросить их остановить.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from backend.app.models.distributed_workers import ConnectivityState, JobState

#: Доказанный на этом этапе максимум одновременных попыток на воркере.
MAX_VERIFIED_SLOTS = 2

#: Значение по умолчанию. Два слота включаются ТОЛЬКО явной конфигурацией.
DEFAULT_MAX_SLOTS = 1

#: Состояния, в которых работа физически у воркера.
OCCUPYING_EXECUTION_STATES: frozenset[str] = frozenset(
    {
        JobState.SOURCE_UPLOADING.value,
        JobState.SOURCE_READY.value,
        JobState.ACCEPTED_BY_WORKER.value,
        JobState.RUNNING.value,
        JobState.CANCEL_REQUESTED.value,
    }
)

#: Состояние «назначено, но воркер ещё не забрал». Слот НЕ занимает.
AWAITING_SLOT_STATES: frozenset[str] = frozenset({JobState.ASSIGNED.value})

#: Состояния, где локальное исполнение достоверно закончилось.
RELEASED_EXECUTION_STATES: frozenset[str] = frozenset(
    {
        JobState.COMPLETED_LOCALLY.value,
        JobState.RESULT_UPLOADING.value,
        JobState.RESULT_RECEIVED.value,
        JobState.VALIDATING.value,
        JobState.COMPLETED.value,
        JobState.FAILED.value,
        JobState.CANCELLED.value,
        JobState.SUPERSEDED_RESULT_RECEIVED.value,
    }
)


@dataclass(frozen=True)
class SlotLimit:
    """Нормализованный лимит + честное объяснение, если его пришлось поправить."""

    value: int
    notice: Optional[str] = None
    raw: Any = None

    @property
    def clamped(self) -> bool:
        return self.notice is not None


def normalize_max_slots(raw: Any, *, source: str = "конфигурация") -> SlotLimit:
    """Привести настройку числа слотов к доказанному диапазону [1..2].

    Нечисловое, нулевое, отрицательное и большее двух значение не роняет ни
    воркер, ни центр: оно зажимается, а оператор получает текст, объясняющий
    что именно произошло. Молчаливое зажатие было бы хуже отказа — оператор
    считал бы, что у него пять слотов.
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return SlotLimit(DEFAULT_MAX_SLOTS, None, raw)
    if isinstance(raw, bool):
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: логическое значение вместо числа слотов — принято "
            f"{DEFAULT_MAX_SLOTS}",
            raw,
        )
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: нечисловое значение {raw!r} — принято {DEFAULT_MAX_SLOTS}",
            raw,
        )
    if value < 1:
        return SlotLimit(
            DEFAULT_MAX_SLOTS,
            f"{source}: значение {value} меньше единицы — принято {DEFAULT_MAX_SLOTS}",
            raw,
        )
    if value > MAX_VERIFIED_SLOTS:
        return SlotLimit(
            MAX_VERIFIED_SLOTS,
            f"{source}: запрошено {value} слотов, но на этом этапе доказан "
            f"максимум {MAX_VERIFIED_SLOTS} — принято {MAX_VERIFIED_SLOTS}. "
            "Поддержка 3–5 слотов НЕ проверялась и не заявляется.",
            raw,
        )
    return SlotLimit(value, None, raw)


# ─── Предикаты состояния попытки ─────────────────────────────────────────────
def _disposition(attempt: dict[str, Any]) -> str:
    return (attempt.get("attempt_disposition") or "active") or "active"


def _state(attempt: dict[str, Any]) -> str:
    return str(attempt.get("state") or attempt.get("execution_state") or "")


def attempt_occupies_execution_slot(attempt: dict[str, Any]) -> bool:
    """Единственный предикат «эта попытка занимает слот исполнения».

    Списки состояний живут здесь и больше нигде: дублировать их по модулям —
    это гарантированное расхождение через полгода.
    """
    return _disposition(attempt) == "active" and _state(attempt) in OCCUPYING_EXECUTION_STATES


def attempt_awaiting_slot(attempt: dict[str, Any]) -> bool:
    """Попытка назначена, но воркер её ещё не забрал."""
    return _disposition(attempt) == "active" and _state(attempt) in AWAITING_SLOT_STATES


def attempt_unproven_remote(attempt: dict[str, Any]) -> bool:
    """Попытка, которую оператор признал потерянной, а процесс мог остаться жив.

    Центр НЕ вправе считать её остановленной (I-06), поэтому она попадает в
    ОТДЕЛЬНЫЙ счётчик `unproven` и показывается оператору как «недоказанная».
    В `reserved` она НЕ входит: иначе признание попытки потерянной навсегда
    съедало бы слот и новую попытку было бы негде запустить. Цена решения —
    процесс на VPS мог остаться жив, и об этом предупреждает `unproven_warning`.
    """
    return (
        _disposition(attempt) == "operator_declared_lost"
        and _state(attempt) in OCCUPYING_EXECUTION_STATES
    )


# ─── Эффективный лимит воркера ───────────────────────────────────────────────
@dataclass(frozen=True)
class EffectiveLimit:
    """Из чего сложился лимит. `components` показывается оператору как есть."""

    value: int
    binding: str
    components: dict[str, int]
    notices: tuple[str, ...] = ()
    blocked_reason: Optional[str] = None


def worker_reported_max_slots(worker: dict[str, Any]) -> SlotLimit:
    raw = worker.get("worker_reported_max_slots")
    if raw is None:
        raw = worker.get("configured_max_slots")
    return normalize_max_slots(raw, source="воркер")


def center_configured_max_slots(worker: dict[str, Any]) -> SlotLimit:
    return normalize_max_slots(
        worker.get("configured_max_slots"), source="настройка оператора"
    )


def worker_max_verified_slots(worker: dict[str, Any]) -> int:
    """Capability воркера: сколько слотов ПРОВЕРЕНО его сборкой.

    Старый воркер поля не присылает — для него это 1, и это правильный ответ:
    доказательств двух слотов у его сборки нет.
    """
    raw = worker.get("max_verified_slots")
    if raw is None:
        capabilities = worker.get("capabilities") or {}
        if isinstance(capabilities, dict):
            raw = capabilities.get("max_verified_slots")
    limit = normalize_max_slots(raw, source="capability воркера")
    return limit.value


def effective_limit(
    worker: dict[str, Any],
    *,
    executor_status: Optional[str] = None,
    disk_level: Optional[str] = None,
    protocol_version: Optional[int] = None,
) -> EffectiveLimit:
    """Сколько попыток центр вправе держать на этом воркере ОДНОВРЕМЕННО.

    Минимум из всех известных ограничений. Каждое из них умеет обнулить лимит,
    и это правильная сторона ошибки: лишний невыданный слот стоит ожидания,
    лишний выданный — второго процесса там, где его не ждут.
    """
    notices: list[str] = []
    center = center_configured_max_slots(worker)
    reported = worker_reported_max_slots(worker)
    for limit in (center, reported):
        if limit.notice:
            notices.append(limit.notice)

    components: dict[str, int] = {
        "center_configured": center.value,
        "worker_configured": reported.value,
        "max_verified": min(worker_max_verified_slots(worker), MAX_VERIFIED_SLOTS),
    }

    blocked: Optional[str] = None
    if worker.get("registration_status") != "approved":
        components["registration"] = 0
        blocked = "Регистрация не одобрена оператором"
    if not bool(worker.get("intake_enabled", 0)):
        components["operator_intake"] = 0
        blocked = blocked or "Приём новых заданий остановлен оператором"
    if worker.get("connection_status") != ConnectivityState.ONLINE.value:
        components["agent_connection"] = 0
        blocked = blocked or f"Связь с агентом: {worker.get('connection_status')}"

    status = executor_status if executor_status is not None else _executor_status(worker)
    if status in ("offline", "interrupted"):
        # `unknown` и `stale` лимит не обнуляют: это «нет свежих сведений», а не
        # «исполнитель мёртв». Жёсткий отказ по неизвестности сделал бы старые
        # сборки агента неработоспособными без единого сообщения о причине.
        components["executor"] = 0
        blocked = blocked or f"Локальный исполнитель: {status}"

    level = disk_level if disk_level is not None else _disk_level(worker)
    if level == "critical":
        components["disk"] = 0
        blocked = blocked or "Критически мало места на диске воркера"

    # Состояние самого воркера. Раньше его знала только `can_receive_jobs`, чей
    # вердикт применялся ТОЛЬКО если в причине встречалось слово «диск», — то
    # есть воркер, плавно уходящий в останов (`draining`) или объявивший себя
    # `degraded`, продолжал получать новую работу.
    worker_state = str(worker.get("worker_state") or "").strip().lower()
    if worker_state in ("draining", "drained", "degraded", "revoked"):
        components["worker_state"] = 0
        blocked = blocked or f"Воркер сообщил состояние «{worker_state}»"

    expected_proto = protocol_version
    if expected_proto is not None:
        worker_proto = int(worker.get("protocol_version") or 0)
        if worker_proto != expected_proto:
            components["protocol"] = 0
            blocked = blocked or (
                f"Несовместимая версия протокола: воркер {worker_proto}, "
                f"центр {expected_proto}"
            )

    value = max(0, min(components.values())) if components else 0
    binding = min(components, key=lambda key: components[key]) if components else "unknown"
    return EffectiveLimit(
        value=value,
        binding=binding,
        components=components,
        notices=tuple(notices),
        blocked_reason=blocked,
    )


def _executor_status(worker: dict[str, Any]) -> str:
    snapshot = worker.get("resource_snapshot")
    if isinstance(snapshot, str):
        import json

        try:
            snapshot = json.loads(snapshot)
        except (TypeError, ValueError):
            snapshot = {}
    executor = (snapshot or {}).get("executor") or {}
    return str(executor.get("status") or "unknown")


def _disk_level(worker: dict[str, Any]) -> str:
    snapshot = worker.get("resource_snapshot")
    if isinstance(snapshot, str):
        import json

        try:
            snapshot = json.loads(snapshot)
        except (TypeError, ValueError):
            snapshot = {}
    disk = (snapshot or {}).get("disk_report") or {}
    return str(disk.get("level") or "unknown")


# ─── Сводка для API и экрана ─────────────────────────────────────────────────
@dataclass(frozen=True)
class SlotUsage:
    """Занятость воркера, посчитанная ЦЕНТРОМ по своей базе."""

    occupied: int
    awaiting: int
    unproven: int

    @property
    def reserved(self) -> int:
        """Ёмкость, которую центр считает ДОКАЗАННО занятой.

        Недоказанные (признанные потерянными) сюда НЕ входят — см. заголовок
        модуля: иначе один mark-lost навсегда блокировал бы воркер. Они
        показываются отдельно и предупреждают о возможном превышении.
        """
        return self.occupied


def usage_from_attempts(attempts: list[dict[str, Any]]) -> SlotUsage:
    return SlotUsage(
        occupied=sum(1 for a in attempts if attempt_occupies_execution_slot(a)),
        awaiting=sum(1 for a in attempts if attempt_awaiting_slot(a)),
        unproven=sum(1 for a in attempts if attempt_unproven_remote(a)),
    )


def build_slot_view(
    worker: dict[str, Any],
    usage: SlotUsage,
    limit: EffectiveLimit,
    *,
    worker_claimed_free: Optional[int] = None,
) -> dict[str, Any]:
    """Что показывать оператору. Заявленное воркером и посчитанное центром — раздельно.

    Совпадение двух чисел — это диагностика, а не украшение: расхождение
    означает, что одна из сторон считает не то, и назначать по большему
    значению нельзя (§29 задания).
    """
    center_free = max(0, limit.value - usage.reserved)
    claimed = worker_claimed_free
    if claimed is None:
        claimed = int(worker.get("calculated_free_slots") or 0)

    # ФИЗИЧЕСКАЯ свободная ёмкость: только то, сколько слотов существует, без
    # политических запретов (приём выключен, регистрация, связь, исполнитель,
    # диск). Она и сравнивается с числом воркера ниже.
    capacity_facts = [
        limit.components.get(key)
        for key in ("center_configured", "worker_configured", "max_verified")
        if isinstance(limit.components.get(key), int)
    ]
    physical_limit = min(capacity_facts) if capacity_facts else limit.value
    physical_free = max(0, physical_limit - usage.reserved)
    # Расхождение считается в ОБЕ стороны. Односторонняя проверка
    # («воркер обещает больше, чем есть у центра») пропускала ровно тот
    # случай, который тише всего заканчивается тупиком: центр видит слот
    # свободным, воркер — занятым. Так бывает после `mark-lost` на ОНЛАЙН-
    # воркере: центр перестал считать попытку занимающей слот, процесс на VPS
    # работает, локальная ёмкость съедена. Новая попытка висит в `assigned`,
    # агент за ней не идёт, потому что у него занято, и никто не сообщает
    # оператору, почему ничего не происходит.
    # Сравнивать надо СЧЁТ со СЧЁТОМ. Прежде число воркера сверялось с
    # `center_free`, в которое уже вложены политические запреты, и выключенный
    # оператором приём немедленно давал «расхождение»: воркер честно сообщает
    # один физически свободный слот, центр отвечает нулём назначаемых — при
    # полном согласии сторон о том, СКОЛЬКО слотов есть. Оператор видел
    # предупреждение «одна из сторон считает не то» на штатном состоянии, а
    # ровно это состояние 12I.1 и делает нормой: физически свободно 1,
    # доступно для назначения 0. Настоящее расхождение — только когда стороны
    # не сходятся в физической ёмкости.
    mismatch = claimed != physical_free
    mismatch_direction = (
        None if claimed == physical_free
        else ("worker_claims_more" if claimed > physical_free else "worker_claims_fewer")
    )
    return {
        "effective_limit": limit.value,
        "limit_binding": limit.binding,
        "limit_components": limit.components,
        "max_verified_slots": MAX_VERIFIED_SLOTS,
        "occupied": usage.occupied,
        "awaiting": usage.awaiting,
        "unproven_remote": usage.unproven,
        "reserved": usage.reserved,
        "center_free_slots": center_free,
        "physical_free_slots": physical_free,
        "worker_claimed_free_slots": claimed,
        # Центр использует МЕНЬШЕЕ из двух — безопасная сторона (S-15).
        "effective_free_slots": min(center_free, max(0, claimed)) if claimed >= 0 else 0,
        "slot_count_mismatch": mismatch,
        "slot_count_mismatch_direction": mismatch_direction,
        "slot_count_mismatch_hint": (
            None if not mismatch
            else (
                "Воркер обещает больше свободных слотов, чем насчитал центр. "
                "Назначаем по меньшему числу."
                if claimed > physical_free
                else "Центр считает слот свободным, а воркер — занятым: на VPS "
                     "могла остаться работа, которую центр перестал учитывать "
                     "(например, после признания попытки потерянной). Новое "
                     "задание встанет в очередь и не начнётся, пока воркер не "
                     "освободится."
            )
        ),
        "notices": list(limit.notices),
        "blocked_reason": limit.blocked_reason,
        "occupancy_label": (
            f"{usage.reserved}/{limit.value}"
            + (f" (+{usage.unproven} недоказанных)" if usage.unproven else "")
        ),
        "unproven_warning": (
            "Есть попытки, признанные потерянными: процессы на VPS могли не "
            "остановиться, и фактических процессов там может быть больше."
            if usage.unproven
            else None
        ),
    }
