"""12I.3 — досылка не стартует раньше, чем поток заявит владение.

Наблюдение 12I.2: в 08:14:27 досылка ушла в ту же секунду, что и установка
gRPC-потока, и получила `HTTP 409 attempt_superseded` — при том, что попытка
никем не отзывалась. Причина: пакетный канал HTTPS передаёт центру заголовок
`X-Agent-Stream-Connection-Id`, а идентификатор появляется только после
CenterHello. До него центр не признаёт попытку нашей.

Отказ был восстановимым и сам прошёл через 26 секунд, но в журнале он
неотличим от настоящей потери попытки — а такие строки оператор читает как
аварию. Здесь проверяется явное условие готовности, а не пауза.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit_worker import agent as agent_module  # noqa: E402
from audit_worker.agent import DELIVERY_ACKNOWLEDGED, delivery_is_terminal  # noqa: E402
from audit_worker.local_store import LocalJobStore  # noqa: E402

JOB = "f4f2f214-3ab4-431b-894a-de75813f0326"
ATTEMPT = "dd149bff-09b3-466f-862b-ebbb49269679"


class _Transport:
    """Транспорт потока: владение появляется только после CenterHello."""

    def __init__(self, ready: bool = False, connection_id: str = ""):
        self._ready = ready
        self._connection_id = connection_id
        self.uploads = 0

    def control_context_ready(self) -> bool:
        return self._ready and bool(self._connection_id)

    def become_ready(self, connection_id: str = "gconn_a61189e7") -> None:
        self._ready = True
        self._connection_id = connection_id


class _PollingClient:
    """У опрашивающего транспорта владения потоком нет как понятия."""


class _Agent:
    _deliver_pending_results = agent_module.WorkerAgent._deliver_pending_results
    _resume_upload = agent_module.WorkerAgent._resume_upload
    _control_context_ready = agent_module.WorkerAgent._control_context_ready
    _finalize_delivery = agent_module.WorkerAgent._finalize_delivery
    _record_permanent_rejection = agent_module.WorkerAgent._record_permanent_rejection

    def __init__(self, store, client):
        self.jobs = store
        self.client = client
        self._active = {}
        self._active_lock = threading.Lock()
        self._control_context_warned = False

    def _flush_outbox(self, ctx, **_):
        pass

    def _flush_terminal_events(self, meta):
        pass

    def _outbox_for(self, *a, **k):
        return SimpleNamespace(reload=lambda: None, has_pending=False,
                               append=lambda *x, **y: None)


def _attempt(store, *, acknowledged: bool) -> Path:
    job_dir = store.job_dir(JOB, ATTEMPT)
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    archive = job_dir / "result" / f"{ATTEMPT}.tar.gz"
    archive.write_bytes(b"package")
    fields = {
        "local_state": "completed_locally", "result_hash": "3bc772de",
        "params": {"routing_plan": {"routing_plan_hash": "sha256:" + "f" * 64},
                   "pipeline_revision": "e6015d33"},
    }
    if acknowledged:
        fields.update({"local_state": "finished",
                       "delivery_state": DELIVERY_ACKNOWLEDGED,
                       "retention_until": 1_789_453_471.0})
    store.update(JOB, ATTEMPT, **fields)
    return archive


def _capture_uploads(monkeypatch):
    calls = []

    def _upload(**kwargs):
        calls.append(kwargs)
        return {"state": "completed", "validation": {"replayed": True},
                "retention_until": 1_789_453_471.0}

    monkeypatch.setattr(agent_module, "upload_result", _upload)
    return calls


def test_no_resend_while_stream_has_not_claimed_ownership(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    agent = _Agent(store, _Transport(ready=False))
    agent._deliver_pending_results()
    assert calls == [], "отправка без заголовка принадлежности даёт ложный 409"


def test_ready_without_connection_id_is_still_not_ready(tmp_path, monkeypatch):
    """Ровно окно наблюдавшегося отказа: поток «поднялся», имени ещё нет."""
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    agent = _Agent(store, _Transport(ready=True, connection_id=""))
    agent._deliver_pending_results()
    assert calls == []


def test_unacknowledged_result_is_sent_once_ownership_exists(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    transport = _Transport(ready=False)
    agent = _Agent(store, transport)
    agent._deliver_pending_results()
    assert calls == []
    transport.become_ready()
    agent._deliver_pending_results()
    assert len(calls) == 1, "готовность обязана разблокировать досылку"


def test_acknowledged_result_never_resends_even_after_ready(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    archive = _attempt(store, acknowledged=True)
    calls = _capture_uploads(monkeypatch)
    transport = _Transport(ready=False)
    agent = _Agent(store, transport)
    agent._deliver_pending_results()
    transport.become_ready()
    for _ in range(5):
        agent._deliver_pending_results()
    assert calls == [], "12I.2 остаётся в силе: подтверждённое не уезжает"
    assert archive.is_file()


def test_resume_upload_itself_is_guarded(tmp_path, monkeypatch):
    """Сверка зовёт досылку напрямую — заслон обязан стоять и там."""
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    agent = _Agent(store, _Transport(ready=False))
    agent._resume_upload(JOB, ATTEMPT)
    assert calls == []


def test_polling_transport_has_no_ownership_concept_and_is_never_blocked(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    agent = _Agent(store, _PollingClient())
    agent._deliver_pending_results()
    assert len(calls) == 1, "у опроса принадлежность доказывает execution_token"


def test_reconnect_restores_the_gate(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    transport = _Transport(ready=True, connection_id="gconn_1")
    agent = _Agent(store, transport)
    agent._deliver_pending_results()
    assert len(calls) == 1
    meta = LocalJobStore(store.jobs_dir).load(JOB, ATTEMPT)
    assert delivery_is_terminal(meta)
    transport._ready = False
    transport._connection_id = ""
    agent._deliver_pending_results()
    assert len(calls) == 1, "обрыв не должен порождать новую отправку"


def test_no_duplicate_upload_when_gate_opens_repeatedly(tmp_path, monkeypatch):
    store = LocalJobStore(tmp_path / "jobs")
    _attempt(store, acknowledged=False)
    calls = _capture_uploads(monkeypatch)
    transport = _Transport(ready=True, connection_id="gconn_1")
    agent = _Agent(store, transport)
    for _ in range(10):
        agent._deliver_pending_results()
    assert len(calls) == 1


def test_gate_uses_state_not_sleep():
    """Инвариант заявлен явно: никаких пауз в проверке готовности."""
    import inspect

    source = inspect.getsource(agent_module.WorkerAgent._control_context_ready)
    assert "sleep" not in source
    source = inspect.getsource(agent_module.WorkerAgent._deliver_pending_results)
    assert "sleep" not in source


# ═════════════ Заслон стоит в САМОМ канале данных ════════════════════════════
class _RecordingData:
    def __init__(self):
        self.calls = []

    def create_upload(self, payload, token):
        self.calls.append("create_upload")
        return {"upload_id": "upl_1", "chunk_size": 1, "chunks_total": 1,
                "received_chunks": []}

    def put_chunk(self, *a, **k):
        self.calls.append("put_chunk")
        return {}

    def complete_upload(self, *a, **k):
        self.calls.append("complete_upload")
        return {}

    def download_source(self, *a, **k):
        self.calls.append("download_source")
        return 0

    def set_control_context(self, **k):
        pass


def _transport_under_test(ready: bool):
    """Настоящий класс транспорта без сети: проверяем его собственный заслон."""
    pytest.importorskip("grpc")
    from audit_worker.grpc_transport import GrpcStreamControlTransport

    transport = GrpcStreamControlTransport.__new__(GrpcStreamControlTransport)
    transport._ready = threading.Event()
    if ready:
        transport._ready.set()
    transport._connection_id = "gconn_1" if ready else ""
    transport.data = _RecordingData()
    transport._uploads = {}
    transport._assignments = {}
    return transport


@pytest.mark.parametrize(
    "operation",
    ["create_upload", "put_chunk", "complete_upload", "download_source"],
)
def test_data_plane_refuses_before_ownership(operation):
    """Ни один вызывающий не может обойти заслон — он внутри канала данных."""
    from audit_worker.client import ControlContextUnavailable

    transport = _transport_under_test(ready=False)
    with pytest.raises(ControlContextUnavailable):
        if operation == "create_upload":
            transport.create_upload({}, "")
        elif operation == "put_chunk":
            transport.put_chunk("upl", 0, b"x", "sha")
        elif operation == "complete_upload":
            transport.complete_upload("upl", {"attempt_id": "a"}, "")
        else:
            transport.download_source("job", None, "")
    assert transport.data.calls == [], "запрос не должен был уйти"


def test_data_plane_refusal_is_not_attempt_superseded():
    """Ключевое: отказ обязан быть ТРАНСПОРТНЫМ, а не «попытка отозвана».

    По `AttemptSupersededError` агент просит исполнителя остановить работу.
    Гонка старта, приведённая к этому коду, убивала бы живую попытку.
    """
    from audit_worker.client import AttemptSupersededError, ControlContextUnavailable

    transport = _transport_under_test(ready=False)
    with pytest.raises(ControlContextUnavailable) as caught:
        transport.create_upload({}, "")
    assert not isinstance(caught.value, AttemptSupersededError)


def test_data_plane_allows_once_ownership_exists():
    transport = _transport_under_test(ready=True)
    transport.create_upload({"job_id": "j", "attempt_id": "a"}, "")
    assert transport.data.calls == ["create_upload"]


# ═════ Гонка МЕЖДУ проверкой и отправкой (TOCTOU) ═══════════════════════════
#
# Заслона перед вызовом мало: поток может оборваться уже после проверки, и
# запрос уйдёт без заголовка принадлежности. Центр отвечает на такой запрос
# тем же `attempt_superseded`, что и на настоящий отзыв, — и воркер убивал бы
# живую работу. Отличать эти случаи можно ровно там, где виден УШЕДШИЙ запрос.
def _client_with_409(*, bound: bool, send_header: bool):
    import httpx

    from audit_worker.client import CenterClient

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409, json={"error": "attempt_superseded", "detail": "not yours"},
            request=request,
        )

    client = CenterClient("https://center.invalid",
                          transport=httpx.MockTransport(handler))
    if bound:
        client.set_control_context(connection_id="gconn_a61189e7")
        if not send_header:
            # Поток оборвался ПОСЛЕ проверки: заголовок снят, режим прежний.
            client.set_control_context(connection_id=None)
    return client


def test_409_without_ownership_header_is_not_a_verdict_about_the_attempt():
    from audit_worker.client import AttemptSupersededError, ControlContextLostError

    client = _client_with_409(bound=True, send_header=False)
    with pytest.raises(ControlContextLostError) as caught:
        client.request("POST", "/api/agent/uploads")
    assert not isinstance(caught.value, AttemptSupersededError), (
        "по этому типу агент останавливает исполнителя — живую работу"
    )


def test_409_with_ownership_header_is_still_a_real_supersede():
    """Обратная сторона: настоящий отзыв обязан остаться отзывом."""
    from audit_worker.client import AttemptSupersededError

    client = _client_with_409(bound=True, send_header=True)
    with pytest.raises(AttemptSupersededError):
        client.request("POST", "/api/agent/uploads")


def test_polling_transport_keeps_the_old_meaning_of_409():
    """У опрашивающего транспорта заголовка нет вовсе — и 409 там авторитетен.

    Принадлежность там доказывает execution_token, а не заголовок потока.
    Ослабить 409 для него значило бы игнорировать настоящий отзыв попытки.
    """
    from audit_worker.client import AttemptSupersededError

    client = _client_with_409(bound=False, send_header=False)
    with pytest.raises(AttemptSupersededError):
        client.request("POST", "/api/agent/uploads")


def test_lost_ownership_is_resumed_not_failed_during_source_download(tmp_path,
                                                                     monkeypatch):
    """Обрыв потока до первого байта исходников — не провал задания.

    Такая ошибка попадала в общий обработчик и помечала живую попытку
    `failed`. Она обязана вести в тот же цикл возобновления, что и разрыв TCP:
    подождать и скачать заново.
    """
    from audit_worker import package_io
    from audit_worker.client import ControlContextLostError

    store = LocalJobStore(tmp_path / "jobs")
    attempts = {"n": 0}

    class _Client:
        def download_source(self, job_id, dest, token, **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ControlContextLostError(409, {"error": "attempt_superseded"}, {})
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            Path(dest).write_bytes(b"payload")
            return 7

    monkeypatch.setattr(
        package_io, "verify_and_unpack",
        lambda **kwargs: {"files": 1, "bytes": 7, "manifest": {"manifest_version": 1}},
    )
    monkeypatch.setattr(agent_module, "backoff_delays", lambda: iter([0.0] * 8))

    agent = _Agent(store, _Client())
    agent._stop = threading.Event()
    agent._download_and_verify = agent_module.WorkerAgent._download_and_verify.__get__(agent)
    job_dir = store.job_dir(JOB, ATTEMPT)
    store.update(JOB, ATTEMPT, local_state="assigned")
    ctx = {"job_id": JOB, "attempt_id": ATTEMPT, "execution_token": "",
           "outbox": agent._outbox_for()}
    assignment = {"job_type": "test_pipeline_v1",
                  "package": {"package_id": "pkg1", "sha256": "0" * 64,
                              "compression": "gzip"}}

    agent._download_and_verify(assignment, ctx, job_dir)

    assert attempts["n"] == 2, "скачивание обязано повториться, а не отказать"
    meta = store.load(JOB, ATTEMPT)
    assert meta["local_state"] != "failed", (
        "гонка потока управления не имеет права хоронить живую попытку"
    )


def test_chunk_retry_does_not_turn_a_lost_stream_into_upload_failure():
    """`UploadFailed` на 409 означает «чанк принят с другим содержимым».

    Гонка потока — не это. Превратив её в `UploadFailed`, воркер выбросил бы
    уже готовый результат аудита.
    """
    from audit_worker import uploader
    from audit_worker.client import ControlContextLostError

    class _Client:
        def put_chunk(self, *a, **k):
            raise ControlContextLostError(409, {"error": "attempt_superseded"}, {})

    with pytest.raises(ControlContextLostError):
        uploader._put_chunk_with_retry(_Client(), "upl", 0, b"x", attempts=3)
