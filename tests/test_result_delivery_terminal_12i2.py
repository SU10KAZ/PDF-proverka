"""12I.2 — подтверждение приёма результата терминально для ДОСТАВКИ.

Наблюдаемый дефект (задание 12H, попытка dd149bff): центр принял результат,
проимпортировал его и назначил срок хранения, а воркер об этом не узнал и
раз в ~26 секунд начинал досылку заново — бесконечно, до перезапуска агента.

Разбор показал два независимых слоя:

1. **Досылка не могла удаться в принципе.** Потоковый транспорт брал
   `routing_plan_hash` из ПАМЯТИ (карта выданных заданий), а досылка случается
   как раз после перерыва, когда память пуста. Уходил пустой хэш, шлюз отвечал
   невосстановимым отказом.
2. **Ответ центра выбрасывался.** На повторный `complete` центр отвечает
   `validation.replayed` и сразу отдаёт `retention_until` — то есть прямо
   говорит «этот пакет я уже принял». Транспорт этот ответ игнорировал и всё
   равно слал второй ResultReady.

Здесь проверяется разделение двух осей: ДОСТАВКА становится терминальной,
ХРАНЕНИЕ живёт своим сроком, а файл результата не удаляется ни в одном из
исходов.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit_worker import agent as agent_module  # noqa: E402
from audit_worker.agent import (  # noqa: E402
    DELIVERY_ACKNOWLEDGED,
    DELIVERY_REJECTED_PERMANENTLY,
    _routing_binding,
    delivery_is_terminal,
)
from audit_worker.client import ResultRejectedError  # noqa: E402
from audit_worker.uploader import delivery_already_acknowledged  # noqa: E402
from audit_worker.local_store import LocalJobStore  # noqa: E402

JOB = "f4f2f214-3ab4-431b-894a-de75813f0326"
ATTEMPT = "dd149bff-09b3-466f-862b-ebbb49269679"
ROUTE = "sha256:" + "f" * 64
REVISION = "e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f"
RETENTION_DAYS = 30


def _store(tmp_path: Path) -> LocalJobStore:
    return LocalJobStore(tmp_path / "jobs")


def _completed_attempt(store: LocalJobStore, *, at: float = 1_786_861_471.0) -> Path:
    """Попытка в том же состоянии, в каком её застал живой дефект."""
    job_dir = store.job_dir(JOB, ATTEMPT)
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    archive = job_dir / "result" / f"{ATTEMPT}.tar.gz"
    archive.write_bytes(b"result-package-bytes")
    store.update(
        JOB, ATTEMPT,
        local_state="completed_locally",
        result_hash="3bc772de",
        result_size=archive.stat().st_size,
        completed_locally_at=at,
        params={"routing_plan": {"routing_plan_hash": ROUTE}, "pipeline_revision": REVISION},
    )
    return archive


# ═════════════ 1. Маршрут берётся с диска, а не из памяти ════════════════════
def test_routing_binding_comes_from_persisted_metadata(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    binding = _routing_binding(store.load(JOB, ATTEMPT))
    assert binding == {"routing_plan_hash": ROUTE, "pipeline_revision": REVISION}


def test_routing_binding_is_empty_when_metadata_has_none(tmp_path):
    assert _routing_binding({}) == {"routing_plan_hash": "", "pipeline_revision": ""}


# ═════════════ 2. Признание уже подтверждённой доставки ══════════════════════
def test_replayed_complete_with_retention_is_an_acknowledgement():
    assert delivery_already_acknowledged(
        {"state": "completed", "validation": {"replayed": True}, "retention_until": 1.0}
    )


def test_first_complete_is_not_treated_as_replay():
    """Обычный путь обязан идти через ResultReady: иначе шлюз не узнает о пакете."""
    assert not delivery_already_acknowledged(
        {"state": "completed", "validation": {"upload_id": "upl_1"}, "retention_until": 1.0}
    )


def test_replay_without_retention_is_not_an_acknowledgement():
    """«Байты у меня» — ещё не «результат принят»."""
    assert not delivery_already_acknowledged(
        {"state": "result_uploading", "validation": {"replayed": True}, "retention_until": None}
    )


@pytest.mark.parametrize("payload", [None, {}, "ok", {"validation": None, "retention_until": 1.0}])
def test_malformed_complete_response_is_never_an_acknowledgement(payload):
    assert not delivery_already_acknowledged(payload)


# ═════════════ 3. Терминальность доставки переживает рестарт ═════════════════
def test_acknowledged_delivery_is_terminal(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    store.update(JOB, ATTEMPT, delivery_state=DELIVERY_ACKNOWLEDGED,
                 retention_until=1_789_453_471.0)
    # Новый объект хранилища = состояние прочитано с диска, как после рестарта.
    assert delivery_is_terminal(LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT))


def test_legacy_attempt_with_retention_only_is_also_terminal(tmp_path):
    """Попытки, подтверждённые ДО появления поля, не получают второй круг."""
    store = _store(tmp_path)
    _completed_attempt(store)
    store.update(JOB, ATTEMPT, retention_until=1_789_453_471.0)
    assert delivery_is_terminal(store.load(JOB, ATTEMPT))


def test_unconfirmed_attempt_is_not_terminal(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    assert not delivery_is_terminal(store.load(JOB, ATTEMPT))


def test_permanent_rejection_is_terminal_for_delivery(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    store.update(JOB, ATTEMPT, delivery_state=DELIVERY_REJECTED_PERMANENTLY)
    assert delivery_is_terminal(store.load(JOB, ATTEMPT))


# ═════════════ 4. Поведение агента: досылка, рестарт, отказ ══════════════════
class _FakeAgent:
    """Минимальный носитель проходов доставки: без сети, потоков и конвейера."""

    _deliver_pending_results = agent_module.WorkerAgent._deliver_pending_results
    _resume_upload = agent_module.WorkerAgent._resume_upload
    _finalize_delivery = agent_module.WorkerAgent._finalize_delivery
    _record_permanent_rejection = agent_module.WorkerAgent._record_permanent_rejection
    # 12I.3: досылка спрашивает у транспорта, заявлено ли владение потоком.
    # Берём НАСТОЯЩИЙ метод, а не заглушку: иначе тест перестал бы проверять
    # тот код, который выполняется в проде.
    _control_context_ready = agent_module.WorkerAgent._control_context_ready

    def __init__(self, store: LocalJobStore):
        import threading

        self.jobs = store
        self._active: dict = {}
        self._active_lock = threading.Lock()
        self.client = object()   # нет control_context_ready → владение не требуется
        self._control_context_warned = False
        self.resumed: list[tuple[str, str]] = []
        self.flushed: list[dict] = []

    # Реальный `_resume_upload` зовут только через этот перехват: сам он
    # проверяется отдельным тестом ниже.
    def _flush_outbox(self, ctx, **_):
        self.flushed.append(ctx)

    def _flush_terminal_events(self, meta):
        pass

    def _outbox_for(self, *_args, **_kwargs):
        return SimpleNamespace(reload=lambda: None, has_pending=False, append=lambda *a, **k: None)


def test_deliver_pass_skips_acknowledged_attempt_after_restart(tmp_path, monkeypatch):
    store = _store(tmp_path)
    archive = _completed_attempt(store)
    store.update(JOB, ATTEMPT, delivery_state=DELIVERY_ACKNOWLEDGED,
                 retention_until=1_789_453_471.0)

    fake = _FakeAgent(LocalJobStore(store.jobs_dir))
    monkeypatch.setattr(
        _FakeAgent, "_resume_upload",
        lambda self, job_id, attempt_id: self.resumed.append((job_id, attempt_id)),
        raising=False,
    )
    fake._deliver_pending_results()

    assert fake.resumed == [], "подтверждённый результат не должен уезжать повторно"
    assert archive.is_file(), "пакет обязан остаться на воркере"


def test_deliver_pass_still_resends_unconfirmed_attempt(tmp_path, monkeypatch):
    """Контроль обратной стороны: настоящая недоставка обязана дослаться."""
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _FakeAgent(LocalJobStore(store.jobs_dir))
    monkeypatch.setattr(
        _FakeAgent, "_resume_upload",
        lambda self, job_id, attempt_id: self.resumed.append((job_id, attempt_id)),
        raising=False,
    )
    fake._deliver_pending_results()
    assert fake.resumed == [(JOB, ATTEMPT)]


def test_resume_upload_returns_immediately_when_delivery_is_terminal(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    store.update(JOB, ATTEMPT, delivery_state=DELIVERY_ACKNOWLEDGED,
                 retention_until=1_789_453_471.0)
    fake = _FakeAgent(LocalJobStore(store.jobs_dir))

    calls: list = []

    def _never(**kwargs):
        calls.append(kwargs)
        raise AssertionError("upload_result не должен вызываться")

    original = agent_module.upload_result
    agent_module.upload_result = _never
    try:
        fake._resume_upload(JOB, ATTEMPT)
    finally:
        agent_module.upload_result = original
    assert calls == []


def test_resume_upload_persists_acknowledgement_and_retention(tmp_path):
    store = _store(tmp_path)
    archive = _completed_attempt(store)
    fake = _FakeAgent(store)
    seen: dict = {}
    retention_until = 1_786_861_471.0 + RETENTION_DAYS * 86400

    def _accepting(**kwargs):
        seen.update(kwargs)
        return {"state": "completed", "validation": {"replayed": True},
                "retention_until": retention_until}

    original = agent_module.upload_result
    agent_module.upload_result = _accepting
    try:
        fake._resume_upload(JOB, ATTEMPT)
    finally:
        agent_module.upload_result = original

    assert seen["routing_plan_hash"] == ROUTE, "досылка обязана нести сохранённый маршрут"
    assert seen["pipeline_revision"] == REVISION
    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert meta["delivery_state"] == DELIVERY_ACKNOWLEDGED
    assert meta["retention_until"] == retention_until
    assert meta["local_state"] == "finished"
    assert isinstance(meta.get("delivery_acknowledged_at"), float)
    assert archive.is_file(), "подтверждение не удаляет пакет"
    # Хранение: ровно 30 суток от приёма — это оси ДОСТАВКИ не касается.
    assert round((retention_until - 1_786_861_471.0) / 86400) == RETENTION_DAYS


def test_second_delivery_pass_after_acknowledgement_sends_nothing(tmp_path, monkeypatch):
    """Идемпотентность: повтор прохода после подтверждения — ноль отправок."""
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _FakeAgent(store)
    retention_until = 1_789_453_471.0

    calls: list = []

    def _accepting(**kwargs):
        calls.append(kwargs)
        return {"state": "completed", "validation": {"replayed": True},
                "retention_until": retention_until}

    original = agent_module.upload_result
    agent_module.upload_result = _accepting
    try:
        fake._resume_upload(JOB, ATTEMPT)
        restarted = _FakeAgent(LocalJobStore(store.jobs_dir))
        restarted._deliver_pending_results()
        restarted._resume_upload(JOB, ATTEMPT)
    finally:
        agent_module.upload_result = original

    assert len(calls) == 1, "после подтверждения повторной отправки быть не должно"


def test_non_retryable_rejection_stops_retrying_but_keeps_package(tmp_path):
    store = _store(tmp_path)
    archive = _completed_attempt(store)
    fake = _FakeAgent(store)

    calls: list = []

    def _rejecting(**kwargs):
        calls.append(kwargs)
        raise ResultRejectedError(422, "routing plan hash mismatch", retryable=False)

    original = agent_module.upload_result
    agent_module.upload_result = _rejecting
    try:
        fake._resume_upload(JOB, ATTEMPT)
        after = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
        assert after["delivery_state"] == DELIVERY_REJECTED_PERMANENTLY
        assert after.get("retention_until") is None, (
            "невосстановимый отказ не назначает срок хранения"
        )
        assert after["local_state"] == "completed_locally", (
            "невосстановимый отказ не отменяет неподтверждённость: удалять нельзя"
        )
        _FakeAgent(LocalJobStore(store.jobs_dir))._resume_upload(JOB, ATTEMPT)
    finally:
        agent_module.upload_result = original

    assert len(calls) == 1, "невосстановимый отказ повторять нельзя"
    assert archive.is_file(), "отказ центра не даёт права удалить работу"


def test_retryable_rejection_keeps_the_attempt_in_the_retry_queue(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _FakeAgent(store)

    def _rejecting(**kwargs):
        raise ResultRejectedError(422, "unknown HTTPS upload transfer", retryable=True)

    original = agent_module.upload_result
    agent_module.upload_result = _rejecting
    try:
        fake._resume_upload(JOB, ATTEMPT)
    finally:
        agent_module.upload_result = original

    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert meta.get("delivery_state") is None
    assert not delivery_is_terminal(meta)


def test_retention_manager_refuses_to_delete_after_permanent_rejection(tmp_path):
    """Настоящий RetentionManager отказывается удалять неподтверждённый пакет.

    Проверка идёт через реальный менеджер с ВКЛЮЧЁННЫМ физическим удалением и
    просроченным сроком: прежняя версия этого теста лишь смотрела на константу
    и прошла бы даже на коде, который архив стирает.
    """
    from audit_worker import local_db
    from audit_worker.retention import RetentionManager

    root = tmp_path / "worker"
    store = LocalJobStore(root / "jobs")
    archive = _completed_attempt(store)
    store.update(JOB, ATTEMPT, delivery_state=DELIVERY_REJECTED_PERMANENTLY,
                 delivery_rejected_at=1.0)

    config = SimpleNamespace(
        root=root, jobs_dir=store.jobs_dir, trash_dir=root / "trash",
        retention_enabled=True, retention_delete_enabled=True,
        retention_scan_interval_sec=0, retention_days=30,
        local_db_path=root / "worker.db", runtime_dir=root / "runtime",
    )
    manager = RetentionManager(config, local_db.LocalDB(root / "worker.db"), jobs=store)

    allowed, reason = manager.deletion_allowed(JOB, ATTEMPT)
    assert not allowed and reason, "неподтверждённый пакет удалять нельзя"
    outcome = manager.delete_attempt(job_id=JOB, attempt_id=ATTEMPT)
    assert outcome["status"] == "error"
    assert outcome["detail"]["outcome"] == "refused"
    assert archive.is_file(), "невосстановимый отказ центра не даёт права стереть работу"
    assert manager.candidates() == [], "неподтверждённый пакет не кандидат на удаление"
    # И через полный проход менеджера с включённым физическим удалением тоже.
    manager.sweep()
    assert archive.is_file()


# ═════════════ 5. Настоящий путь первой отправки ═════════════════════════════
class _UploadAgent(_FakeAgent):
    """Носитель НАСТОЯЩЕГО `_upload_archive` — пути первой доставки.

    Дефект первой отправки (невосстановимый отказ уходил в общий `except` и
    возвращал попытку в очередь) на проходе досылки не виден вовсе: там другой
    обработчик. Поэтому путь исполняется как есть.
    """

    _upload_archive = agent_module.WorkerAgent._upload_archive
    _finalize_delivery = agent_module.WorkerAgent._finalize_delivery
    _record_permanent_rejection = agent_module.WorkerAgent._record_permanent_rejection


def _upload_ctx(store: LocalJobStore):
    return {
        "job_id": JOB, "attempt_id": ATTEMPT, "execution_token": "",
        "outbox": SimpleNamespace(append=lambda *a, **k: None),
        "stage": "", "project_id": "p",
    }


def _upload_args(store: LocalJobStore):
    """Реальная сигнатура: (assignment, ctx, job_dir, archive)."""
    job_dir = store.job_dir(JOB, ATTEMPT)
    return ({}, _upload_ctx(store), job_dir, job_dir / "result" / f"{ATTEMPT}.tar.gz")


def test_first_upload_permanent_rejection_stops_the_retry_queue(tmp_path):
    store = _store(tmp_path)
    archive = _completed_attempt(store)
    fake = _UploadAgent(store)

    calls: list = []

    def _rejecting(**kwargs):
        calls.append(kwargs)
        raise ResultRejectedError(422, "routing plan hash mismatch", retryable=False)

    original = agent_module.upload_result
    agent_module.upload_result = _rejecting
    try:
        with pytest.raises(agent_module.UploadDeferred):
            fake._upload_archive(*_upload_args(store))
        meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
        assert meta["delivery_state"] == DELIVERY_REJECTED_PERMANENTLY
        assert delivery_is_terminal(meta), "попытка не должна вернуться в очередь досылки"
        restarted = _UploadAgent(LocalJobStore(store.jobs_dir))
        restarted._deliver_pending_results()
    finally:
        agent_module.upload_result = original
    assert len(calls) == 1
    assert archive.is_file()


def test_first_upload_keeps_retention_when_stream_rejects_after_http_acceptance(tmp_path):
    """Поток отверг, но центр уже принял пакет по HTTP — срок хранения не теряем."""
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _UploadAgent(store)
    retention_until = 1_789_453_471.0

    def _rejecting(**kwargs):
        raise ResultRejectedError(
            422, "routing plan hash mismatch", retryable=False,
            acknowledgement={"state": "completed", "validation": {"replayed": True},
                             "retention_until": retention_until},
        )

    original = agent_module.upload_result
    agent_module.upload_result = _rejecting
    try:
        with pytest.raises(agent_module.UploadDeferred):
            fake._upload_archive(*_upload_args(store))
    finally:
        agent_module.upload_result = original

    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert meta["delivery_state"] == DELIVERY_ACKNOWLEDGED
    assert meta["retention_until"] == retention_until
    assert meta["local_state"] == "finished"


def test_first_upload_retryable_rejection_stays_in_the_queue(tmp_path):
    store = _store(tmp_path)
    archive = _completed_attempt(store)
    fake = _UploadAgent(store)

    def _rejecting(**kwargs):
        raise ResultRejectedError(422, "unknown HTTPS upload transfer", retryable=True)

    original = agent_module.upload_result
    agent_module.upload_result = _rejecting
    try:
        with pytest.raises(agent_module.UploadDeferred):
            fake._upload_archive(*_upload_args(store))
    finally:
        agent_module.upload_result = original

    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert meta.get("delivery_state") is None
    assert not delivery_is_terminal(meta)


def test_reconcile_acceptance_finalizes_the_attempt_completely(tmp_path):
    """Приём, узнанный через сверку, закрывает попытку так же, как отправка.

    Прежде путь сверки ставил ТОЛЬКО `retention_until`: попытка навсегда
    оставалась `completed_locally`, то есть выглядела живой для менеджера
    хранения и возвращалась в каждую сверку.
    """
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _UploadAgent(store)
    retention_until = 1_789_453_471.0

    accepted = fake._finalize_delivery(
        JOB, ATTEMPT, {"retention_until": retention_until, "state": "completed"}
    )
    assert accepted
    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert meta["local_state"] == "finished"
    assert meta["delivery_state"] == DELIVERY_ACKNOWLEDGED
    assert meta["retention_until"] == retention_until
    assert meta["center_state"] == "completed"


def test_finalize_is_idempotent_and_does_not_duplicate_completion_events(tmp_path):
    store = _store(tmp_path)
    _completed_attempt(store)
    fake = _UploadAgent(store)
    appended: list = []
    fake._outbox_for = lambda *a, **k: SimpleNamespace(
        reload=lambda: None, has_pending=False,
        append=lambda kind, payload: appended.append(kind),
    )
    response = {"retention_until": 1_789_453_471.0, "state": "completed"}
    fake._finalize_delivery(JOB, ATTEMPT, response)
    fake._finalize_delivery(JOB, ATTEMPT, response)
    assert appended == ["job_completed"], "повторное подтверждение не плодит событий"
