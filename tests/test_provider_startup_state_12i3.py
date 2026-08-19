"""12I.3 — состояние провайдеров после рестарта воркера не залипает.

Доказанный дефект. `ProviderManager.refresh(force=True)` на живом воркере
возвращает claude installed/logged_in 2.1.220 и codex installed/logged_in
0.147.0. Но первый heartbeat уходит РАНЬШЕ, чем завершается первый опрос, и
в центр уезжает заглушка `missing` — «CLI не установлен» про исправно
работающий провайдер.

Дальше эта заглушка ЗАЩЁЛКИВАЛАСЬ, и это и был настоящий эксплуатационный
дефект: снимок провайдеров доезжает до центра только с `CapabilitiesChanged`,
а транспорт отправлял его лишь при смене `capability.sha256`. Этот хэш
считается по СТАТИЧЕСКОМУ контракту возможностей (типы заданий, сжатия, карта
provider_capabilities, версия политики) — живого состояния провайдеров в нём
нет вовсе. Значит хэш не менялся никогда, и центр ВСЁ СОЕДИНЕНИЕ показывал
«CLI не установлен».

ГРАНИЦА ЭТОЙ ПРАВКИ (решение заказчика от 18.08.2026). Защёлка снята: первый
же завершившийся опрос доезжает до центра в ТОМ ЖЕ соединении. Сама стартовая
заглушка остаётся `missing` и принята как остаточное ограничение — честное
значение `not_observed` работающий шлюз ui-real-16c533a7 всё равно приводит к
`missing` своим санитайзером, а перекатывать шлюз ради стартового окна решено
не будет. Поэтому тесты ниже ТРЕБУЮТ прежней кодировки провода и отдельно
фиксируют, что честное значение в коде есть и ждёт будущей выкатки шлюза.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit_worker.providers.identity import (  # noqa: E402
    AUTH_LOGGED_IN,
    AUTH_UNKNOWN,
    INSTALL_INSTALLED,
    INSTALL_MISSING,
    INSTALL_NOT_OBSERVED,
    INSTALLATION_STATES,
)
from backend.app.services.distributed_workers import distributed_ui  # noqa: E402

pytest.importorskip("google.protobuf")

from contracts.agent_stream.v1 import adapters  # noqa: E402
from contracts.agent_stream.v1.adapters import provider_status_digest  # noqa: E402

PROVIDERS = ("claude", "codex", "openrouter")


def _snapshot(provider, *, install, auth, observed_at=1000.0, remaining=None, cli=None,
              policy="allowed", quota_state=None, detail=None):
    return {
        "provider": provider,
        "installation_status": install,
        "auth_state": auth,
        "policy_state": policy,
        "inference_allowed": False,
        "cli_version": cli,
        "observed_at": observed_at,
        "quota": {
            "quota_state": quota_state or ("ready" if remaining is not None else "unknown"),
            "estimated_remaining_pct": remaining,
            "raw_remaining_supported": remaining is not None,
            "source": "official_app_server_rpc" if remaining is not None else None,
            "observed_at": observed_at,
            "detail": detail,
        },
    }


def _settings():
    import dataclasses

    from backend.app.services.distributed_workers.settings import get_settings

    return dataclasses.replace(get_settings(), quota_stale_sec=3600)


# ═════ 1. Принятый остаточный дефект: провод остаётся прежним ════════════════
def test_not_observed_exists_in_code_but_is_not_a_validated_state():
    """Честное значение определено и ждёт выкатки шлюза, а не удалено."""
    assert INSTALL_NOT_OBSERVED != INSTALL_MISSING
    assert INSTALL_NOT_OBSERVED not in INSTALLATION_STATES, (
        "у настоящего наблюдения такого исхода не бывает: значение живёт "
        "только до первого опроса и не должно проходить валидацию ProviderIdentity"
    )


def test_agent_hello_before_first_refresh_says_not_observed():
    """ДЕФЕКТ ЗАКРЫТ 19.08.2026 вместе с выкаткой контракта.

    Раньше здесь стояло обратное требование: провод обязан слать `missing`,
    потому что работающий шлюз ui-real-16c533a7 всё равно переписал бы честное
    значение, а перекатывать шлюз ради одного стартового окна было незачем.
    Тест сторожил ГРАНИЦУ решения и прямо предупреждал: чинить кодировку можно
    только вместе с выкаткой шлюза.

    Ровно это и произошло — правка вошла в общую выкатку контракта (окна
    лимита, код причины, честный ноль). Требование перевёрнуто: «ещё не
    опрашивали» больше не выдаётся за «CLI не установлен».
    """
    capability = adapters.capabilities_from_domain(
        {"job_types": ["audit_pipeline_v1"],
         "provider_capabilities": {p: ["strong_audit"] for p in PROVIDERS}},
        provider_snapshots=(),
    )
    assert {item.provider for item in capability.providers} == set(PROVIDERS)
    assert {item.installation_status for item in capability.providers} == {
        INSTALL_NOT_OBSERVED
    }
    # Статическая карта способностей обязана уцелеть: по ней центр решает
    # вопросы совместимости маршрута.
    restored = adapters.capabilities_to_domain(capability)
    assert set(restored["provider_capabilities"]) == set(PROVIDERS)


def test_worker_reports_missing_only_when_it_really_observed_it(tmp_path):
    """«Не установлен» говорится ТОЛЬКО про настоящее наблюдение.

    Прежняя редакция требовала обратного — чтобы `not_observed` не покидал
    воркер вовсе. После выкатки контракта различие доезжает до центра, и
    важным стало другое: снимок, собранный по завершённому опросу, обязан
    называть установку своим именем, а не стартовой заглушкой.
    """
    from audit_worker.providers import quota
    from audit_worker.providers.manager import ProviderManager

    manager = ProviderManager(worker_root=tmp_path, inference_allowed=False)
    for provider in PROVIDERS:
        manager._quotas[provider] = quota.unknown_snapshot(
            provider, auth_state=AUTH_UNKNOWN, observed_at=1000.0,
            reason="опрос ещё не завершён",
        )
    rows = manager.heartbeat_payload()
    assert {row["provider"] for row in rows} == set(PROVIDERS)
    # Опроса не было — значит и `missing` утверждать не о чем.
    assert {row["installation_status"] for row in rows} == {INSTALL_MISSING}
    capability = adapters.capabilities_from_domain(
        {"provider_capabilities": {p: [] for p in PROVIDERS}}, provider_snapshots=rows,
    )
    # Значение из снимка передаётся как есть, без подмены в обе стороны.
    assert {item.installation_status for item in capability.providers} == {
        INSTALL_MISSING
    }


def test_gateway_side_decoder_understands_not_observed():
    """Приёмная сторона (исполняется ШЛЮЗОМ) понимает «не наблюдали».

    До 19.08.2026 этот код спал: функция живёт в дереве релиза шлюза, а шлюз
    не перекатывался. Теперь обе стороны провода говорят на одном языке, и
    тест сторожит именно это.
    """
    from contracts.agent_stream.v1 import common_pb2 as common_pb

    empty = common_pb.ProviderCapabilitySnapshot(provider="claude")
    assert adapters.provider_capability_to_center(empty)["installation_status"] == (
        INSTALL_NOT_OBSERVED
    )


def test_real_snapshot_still_reports_missing_when_truly_missing():
    """Обратная сторона: доказанное отсутствие CLI обязано остаться `missing`."""
    capability = adapters.capabilities_from_domain(
        {"provider_capabilities": {"codex": []}},
        provider_snapshots=[_snapshot("codex", install=INSTALL_MISSING, auth=AUTH_UNKNOWN)],
    )
    assert capability.providers[0].installation_status == INSTALL_MISSING


def test_observed_providers_are_reported_normally():
    capability = adapters.capabilities_from_domain(
        {"provider_capabilities": {p: ["strong_audit"] for p in PROVIDERS}},
        provider_snapshots=[
            _snapshot("codex", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, cli="0.147.0")
        ],
    )
    observed = {item.provider: item for item in capability.providers}
    assert observed["codex"].installation_status == INSTALL_INSTALLED
    assert observed["codex"].cli_version == "0.147.0"


# ═════ 2. Экран центра ═══════════════════════════════════════════════════════
def test_center_view_of_absent_row_is_unchanged():
    """Живое поведение центра эта правка не трогает."""
    view = distributed_ui._provider_quota(None, settings=_settings(), now=1000.0)
    assert view["availability"] == "unknown"
    assert view["status"] == "unknown"
    assert view["percentageRemaining"] is None


@pytest.mark.parametrize("provider", PROVIDERS)
def test_center_understands_not_observed_when_a_newer_gateway_forwards_it(provider):
    """Спящая ветка центра — на будущую выкатку шлюза."""
    view = distributed_ui._provider_quota(
        _snapshot(provider, install=INSTALL_NOT_OBSERVED, auth=AUTH_UNKNOWN),
        settings=_settings(), now=1000.0,
    )
    assert view["availability"] == "unknown", "отсутствие наблюдения — не авария"
    assert view["status"] == "not_observed"


def test_proven_blocker_outranks_absence_of_observation():
    """Запрет политики известен и без опроса CLI — прятать его нельзя."""
    view = distributed_ui._provider_quota(
        _snapshot("claude", install=INSTALL_NOT_OBSERVED, auth=AUTH_UNKNOWN,
                  policy="policy_blocked"),
        settings=_settings(), now=1000.0,
    )
    assert view["availability"] == "unavailable"
    assert view["status"] == "unavailable"


@pytest.mark.parametrize("provider", PROVIDERS)
def test_center_shows_available_after_real_observation(provider):
    view = distributed_ui._provider_quota(
        _snapshot(provider, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN,
                  observed_at=999.0, remaining=11.0, cli="2.1.220"),
        settings=_settings(), now=1000.0,
    )
    assert view["availability"] == "available"
    assert view["percentageRemaining"] == 11.0


# ═════ 3. Обнаружение изменений видит СОСТОЯНИЕ провайдеров ══════════════════
def test_status_digest_changes_when_provider_becomes_observed():
    before = [_snapshot(p, install=INSTALL_MISSING, auth=AUTH_UNKNOWN) for p in PROVIDERS]
    after = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, cli="2.1.220")
             for p in PROVIDERS]
    assert provider_status_digest(before) != provider_status_digest(after)


def test_status_digest_ignores_pure_passage_of_time():
    """Иначе CapabilitiesChanged уходил бы с каждым heartbeat — раз в 30 секунд."""
    a = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, observed_at=1000.0)
         for p in PROVIDERS]
    b = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, observed_at=9999.0)
         for p in PROVIDERS]
    assert provider_status_digest(a) == provider_status_digest(b)


def test_status_digest_ignores_the_regenerated_age_of_a_stale_snapshot():
    """Ловушка, из-за которой отпечаток по домашнему словарю был бы негоден.

    `_staleness_applied` пересобирает `quota.detail` С ВОЗРАСТОМ внутри на
    каждом такте. Отпечаток по словарю менялся бы каждые 30 секунд навсегда —
    то есть «починка» породила бы вечный поток CapabilitiesChanged. Отпечаток
    считается по проводу, а `detail` на провод не уезжает вовсе.
    """
    older = [_snapshot("claude", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN,
                       remaining=50.0, quota_state="stale",
                       detail="снимок устарел (30 с назад, было ready)")]
    newer = [_snapshot("claude", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN,
                       remaining=50.0, quota_state="stale", observed_at=9999.0,
                       detail="снимок устарел (9000 с назад, было ready)")]
    assert provider_status_digest(older) == provider_status_digest(newer)


def test_status_digest_changes_on_real_quota_change():
    a = [_snapshot("codex", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, remaining=11.0)]
    b = [_snapshot("codex", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, remaining=9.0)]
    assert provider_status_digest(a) != provider_status_digest(b)


def test_status_digest_changes_when_a_provider_disappears():
    both = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN)
            for p in ("claude", "codex")]
    assert provider_status_digest(both) != provider_status_digest(both[:1])


def test_status_digest_is_order_independent():
    rows = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN) for p in PROVIDERS]
    assert provider_status_digest(rows) == provider_status_digest(list(reversed(rows)))


@pytest.mark.parametrize("bad", [None, "x", 5, [object()], [{"no": "provider"}]])
def test_status_digest_never_raises_on_junk(bad):
    provider_status_digest(bad)


# ═════ 4. НАСТОЯЩИЙ транспорт: одна отправка на изменение ════════════════════
#
# Здесь намеренно НЕ повторяется алгоритм обнаружения изменений в подделке:
# такая подделка осталась бы зелёной и в том случае, если бы продовый
# транспорт вернулся к старому ключу «только статический sha». Драйвится
# `GrpcStreamControlTransport` — тот самый класс, что работает на .31.
grpc = pytest.importorskip("grpc")

from audit_worker.config import WorkerConfig  # noqa: E402
from audit_worker.grpc_transport import GrpcStreamControlTransport  # noqa: E402
from audit_worker.local_store import LocalJobStore, WorkerStateStore  # noqa: E402


class _NullData:
    def __init__(self):
        self.connection_id = None

    def set_control_context(self, *, connection_id=None):
        self.connection_id = connection_id

    def close(self):
        pass


def _live_transport(tmp_path, providers_ref):
    config = WorkerConfig(
        dispatcher_url="https://center.invalid", root=tmp_path,
        display_name="12i3-agent", provider_gate_enabled=False, max_slots=2,
        pipeline_revision="rev-test", grpc_target="127.0.0.1:12345",
        control_transport="grpc",
    )
    config.ensure_dirs()

    def build_heartbeat():
        return {
            "sent_at": 1000.0, "worker_state": "idle", "configured_max_slots": 2,
            "calculated_free_slots": 2, "active_jobs": [], "resource_snapshot": {},
            "disk": {}, "executor": {"status": "online"},
            "providers": list(providers_ref[0]),
        }

    transport = GrpcStreamControlTransport(
        target=config.grpc_target, data_client=_NullData(),
        state_store=WorkerStateStore(config.state_path, config.token_path),
        jobs=LocalJobStore(config.jobs_dir),
        worker_id="wrk_0123456789abcdef", instance_id="inst_0123456789abcdef",
        config=config, build_heartbeat=build_heartbeat,
    )
    # Соединение в этих тестах не поднимается: проверяется ЛОГИКА обнаружения
    # изменений, а не сеть. Всё остальное — настоящий код транспорта.
    transport._ensure_started = lambda: None
    return transport


def _beat(transport, providers_ref, rows):
    """Такт heartbeat. Возвращает True, если транспорт создал CapabilitiesChanged."""
    providers_ref[0] = rows
    transport._latest_capabilities = None
    transport.heartbeat(transport.build_heartbeat())
    return transport._latest_capabilities is not None


PLACEHOLDER = [_snapshot(p, install=INSTALL_MISSING, auth=AUTH_UNKNOWN) for p in PROVIDERS]
OBSERVED = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, cli="2.1.220")
            for p in PROVIDERS]


def test_hello_fixes_the_baseline_so_a_refresh_that_won_the_race_is_announced(tmp_path):
    """Опрос может завершиться РАНЬШЕ первого heartbeat.

    Без опорной точки из самого Hello первый heartbeat лишь запоминал уже
    наблюдённое состояние, и настоящий результат опроса не объявлялся никогда:
    следующего изменения могло не быть часами.
    """
    ref = [list(PLACEHOLDER)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)                      # AgentHello с заглушкой
    assert _beat(transport, ref, OBSERVED), (
        "первый же heartbeat после опроса обязан объявить настоящее состояние"
    )


def test_first_real_refresh_produces_exactly_one_capabilities_change(tmp_path):
    ref = [list(PLACEHOLDER)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)
    assert _beat(transport, ref, PLACEHOLDER) is False, (
        "о заглушке центр уже знает из AgentHello — дублировать нечего"
    )
    assert _beat(transport, ref, OBSERVED) is True
    for _ in range(20):
        assert _beat(transport, ref, OBSERVED) is False, (
            "один и тот же снимок не должен слаться повторно"
        )


def test_later_real_change_emits_another_update(tmp_path):
    ready = [_snapshot("codex", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN,
                       remaining=11.0)]
    spent = [_snapshot("codex", install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN,
                       remaining=9.0)]
    ref = [list(ready)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)
    assert _beat(transport, ref, ready) is False
    assert _beat(transport, ref, spent) is True


def test_static_capability_change_still_propagates(tmp_path):
    ref = [list(OBSERVED)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)
    assert _beat(transport, ref, OBSERVED) is False
    transport.config.extra_capabilities = {"provider_policy_version": 7}
    assert _beat(transport, ref, OBSERVED) is True


def test_worker_restart_repeats_the_same_sequence(tmp_path):
    for run in range(3):
        ref = [list(PLACEHOLDER)]
        transport = _live_transport(tmp_path / f"run{run}", ref)
        transport._hello(epoch=1)
        assert _beat(transport, ref, OBSERVED) is True
        assert _beat(transport, ref, OBSERVED) is False


def test_no_reconnect_is_required_for_the_update(tmp_path):
    """Обновление уезжает в ТОМ ЖЕ соединении: эпоха и попытки не меняются."""
    ref = [list(PLACEHOLDER)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)
    attempts_before = transport._connection_attempts
    assert _beat(transport, ref, OBSERVED) is True
    assert transport._connection_attempts == attempts_before, (
        "смена состояния провайдеров не имеет права требовать переподключения"
    )


# ═════ 5. Безопасность ══════════════════════════════════════════════════════
def test_provider_payload_carries_no_secrets(tmp_path):
    from audit_worker.providers.manager import ProviderManager

    manager = ProviderManager(worker_root=tmp_path, inference_allowed=False)
    blob = json.dumps(manager.heartbeat_payload(), ensure_ascii=False, default=str).lower()
    for forbidden in ("token", "secret", "password", "authorization", "cookie",
                      "api_key", "apikey", "bearer", "begin ", "private"):
        assert forbidden not in blob, f"в снимок провайдеров просочилось «{forbidden}»"


def test_no_provider_inference_is_triggered_by_status_reporting(tmp_path, monkeypatch):
    """Отчёт о состоянии не имеет права вызывать модель."""
    import subprocess

    from audit_worker.providers.manager import ProviderManager

    def _forbidden(*args, **kwargs):
        raise AssertionError("отчёт о состоянии запустил подпроцесс провайдера")

    monkeypatch.setattr(subprocess, "run", _forbidden)
    monkeypatch.setattr(subprocess, "check_output", _forbidden)
    manager = ProviderManager(worker_root=tmp_path, inference_allowed=False)
    manager.heartbeat_payload()


# ═════ 6. Спящая цепочка обязана быть РАБОЧЕЙ к выкатке шлюза ═══════════════
def test_dormant_chain_survives_the_center_sanitizer():
    """Провод → санитайзер → строка БД → экран: сквозная проверка спящего пути.

    Ловушка, ради которой этот тест написан: санитайзер центра приводит
    `installation_status` к закрытому списку. Пока в списке не было
    `not_observed`, честное значение схлопывалось бы обратно в `missing` уже
    ЗДЕСЬ — и правка на проводе оказалась бы бесполезной, а выяснилось бы это
    только после выкатки шлюза, в бою.
    """
    from backend.app.services.distributed_workers import provider_accounts

    sanitized = provider_accounts.sanitize_provider_snapshot({
        "provider": "claude",
        "installation_status": INSTALL_NOT_OBSERVED,
        "auth_state": AUTH_UNKNOWN,
        "policy_state": "allowed",
    })
    assert sanitized["installation_status"] == INSTALL_NOT_OBSERVED, (
        "санитайзер стёр честное значение — спящая правка мертва"
    )
    view = distributed_ui._provider_quota(
        dict(sanitized, quota={"quota_state": "unknown"}, observed_at=1000.0),
        settings=_settings(), now=1000.0,
    )
    assert view["status"] == "not_observed"
    assert view["availability"] == "unknown"


def test_sanitizer_still_rejects_invented_states():
    """Расширение списка не превращает его в «принимаем что угодно»."""
    from backend.app.services.distributed_workers import provider_accounts

    sanitized = provider_accounts.sanitize_provider_snapshot({
        "provider": "claude", "installation_status": "totally_fine",
    })
    assert sanitized["installation_status"] == INSTALL_MISSING


def test_worker_identity_still_refuses_not_observed():
    """Настоящее наблюдение таким не бывает — валидация обязана отвергать."""
    from audit_worker.providers.identity import ProviderIdentity

    fields = dict(
        provider="claude", auth_state=AUTH_UNKNOWN, auth_method="none",
        policy_state="allowed", inference_allowed=False, last_auth_check_at=1000.0,
    )
    # Контроль: с настоящим исходом конструктор проходит.
    ProviderIdentity(installation_status=INSTALL_MISSING, **fields)
    with pytest.raises(ValueError, match="installation_status"):
        ProviderIdentity(installation_status=INSTALL_NOT_OBSERVED, **fields)


def test_event_from_the_previous_connection_never_overwrites_fresh_state(tmp_path):
    """Снимок, накопленный до обрыва, не имеет права пережить переподключение.

    Последовательность, которая ломала бы состояние навсегда: в соединении №1
    поставлено в очередь событие со снимком B; связь рвётся до отправки; к
    моменту переподключения состояние стало C, и новый Hello сообщает C; сразу
    после CenterHello уезжает СТАРОЕ B и затирает свежую запись. Исправить это
    следующим heartbeat невозможно — опорная точка уже равна C, изменений
    транспорт не видит, и ложь остаётся до конца соединения.
    """
    ref = [list(PLACEHOLDER)]
    transport = _live_transport(tmp_path, ref)
    transport._hello(epoch=1)                       # соединение №1
    stale = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, remaining=50.0)
             for p in PROVIDERS]
    assert _beat(transport, ref, stale) is True     # событие встало в очередь
    transport.heartbeat(transport.build_heartbeat())
    assert transport._latest_capabilities is not None, "событие ждёт отправки"

    fresh = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, remaining=9.0)
             for p in PROVIDERS]
    ref[0] = fresh
    transport._hello(epoch=2)                       # соединение №2 с C
    assert transport._latest_capabilities is None, (
        "событие прошлого поколения уехало бы после CenterHello и затёрло бы C"
    )
    # И новое состояние остаётся объявляемым: изменение после Hello доезжает.
    changed = [_snapshot(p, install=INSTALL_INSTALLED, auth=AUTH_LOGGED_IN, remaining=8.0)
               for p in PROVIDERS]
    assert _beat(transport, ref, changed) is True


def test_digest_covers_exactly_what_the_stream_delivers():
    """Граница отпечатка = граница ПРОВОДА, и это надо знать явно.

    По gRPC-потоку до центра доезжает лишь часть снимка: `auth_method`,
    `plan_type`, `account_fingerprint`, `credential_mode` в контракте потока
    отсутствуют вовсе. Отпечаток не может «пропустить» их изменение — центр
    их не видит НИ ПРИ КАКОМ отпечатке. Расширение возможно только вместе с
    контрактом `common.proto`, то есть с выкаткой шлюза.

    Тест сторожит, чтобы это не приняли за упущение отпечатка и чтобы никто
    не «починил» его добавлением полей, которых на проводе нет.
    """
    from contracts.agent_stream.v1 import adapters as ad

    rich = {"provider": "claude", "installation_status": INSTALL_INSTALLED,
            "auth_state": AUTH_LOGGED_IN, "auth_method": "claudeai",
            "plan_type": "max20", "account_fingerprint": "AAA",
            "credential_mode": "600", "observed_at": 1.0,
            "quota": {"quota_state": "ready"}}
    delivered = ad.provider_capability_to_center(ad._provider_snapshot_to_proto(rich))
    for absent in ("auth_method", "plan_type", "account_fingerprint", "credential_mode"):
        assert absent not in delivered, (
            f"{absent} появилось на проводе — отпечаток обязан его учитывать"
        )
    # Смена учётной записи, ВИДИМАЯ центру, отпечаток менять обязана.
    other = dict(rich, account_group_id="grp-b")
    assert provider_status_digest([rich]) != provider_status_digest([other])
