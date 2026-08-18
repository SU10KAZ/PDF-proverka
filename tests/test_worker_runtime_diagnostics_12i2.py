"""12I.2 — эксплуатационная сводка воркера: EventOutbox, адрес шлюза, релизы.

До этого экран диагностики показывал `eventOutbox: {null, null, null}` и
`gatewayTarget: null`, хотя и то и другое на воркере СУЩЕСТВУЕТ: журнал
событий ведёт `cursor.json`/`ack.json` по каждой попытке, адрес шлюза лежит в
конфигурации. `null` на экране читается либо как ноль, либо как поломка — то
есть это выдуманное значение с обратным знаком.

Здесь проверяются три вещи и ни одной больше: сводка собирается из настоящего
состояния, доезжает до центра через уже существующий канал БЕЗ секретов, и
превращается в слова, а не в `null`.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit_worker import diagnostics  # noqa: E402
from audit_worker.event_outbox import EventOutbox  # noqa: E402
from audit_worker.local_store import LocalJobStore  # noqa: E402
from backend.app.services.distributed_workers import (  # noqa: E402
    distributed_ui,
    worker_registry,
)

JOB = "job-12i2"
ATTEMPT = "attempt-12i2"


def _worker_config(tmp_path: Path, **overrides):
    root = tmp_path / "worker"
    (root / "jobs").mkdir(parents=True, exist_ok=True)
    base = {
        "root": root,
        "jobs_dir": root / "jobs",
        "control_transport": "grpc",
        "grpc_target": "176.12.77.128:8443",
        "pipeline_root": tmp_path / "app" / "20260817T125827-f814d9f33058",
    }
    base.update(overrides)
    Path(base["pipeline_root"]).mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(**base)


def _attempt_with_events(config, *, written: int, acked: int) -> None:
    store = LocalJobStore(config.jobs_dir)
    store.update(JOB, ATTEMPT, local_state="finished")
    events = store.job_dir(JOB, ATTEMPT) / "events"
    outbox = EventOutbox(events)
    for index in range(written):
        outbox.append("log_line", {"n": index})
    outbox.ack(acked)


# ═════════════ 1. Сбор на воркере ════════════════════════════════════════════
def test_telemetry_reports_synced_outbox(tmp_path):
    config = _worker_config(tmp_path)
    _attempt_with_events(config, written=34, acked=34)
    telemetry = diagnostics.collect_runtime_telemetry(config)
    assert telemetry["event_outbox"]["last_written_seq"] == 34
    assert telemetry["event_outbox"]["last_acked_seq"] == 34
    assert telemetry["event_outbox"]["pending"] == 0
    assert telemetry["event_outbox"]["status"] == "synced"


def test_telemetry_reports_pending_batch(tmp_path):
    config = _worker_config(tmp_path)
    _attempt_with_events(config, written=40, acked=37)
    outbox = diagnostics.collect_runtime_telemetry(config)["event_outbox"]
    assert (outbox["last_written_seq"], outbox["last_acked_seq"]) == (40, 37)
    assert outbox["pending"] == 3
    assert outbox["status"] == "pending"


def test_telemetry_without_any_attempt_reports_unavailable(tmp_path):
    """Ни одного журнала — это «нет данных», а не «ноль событий»."""
    telemetry = diagnostics.collect_runtime_telemetry(_worker_config(tmp_path))
    assert telemetry["event_outbox"] == {"attempts": 0, "status": "unavailable"}


def test_sequence_numbers_are_per_attempt_and_pending_is_total(tmp_path):
    """Номера последовательности — у последней попытки, ожидание — по всем.

    Сложить `last_written_seq` двух независимых попыток нельзя: это два разных
    счётчика. А вот незакрытый хвост осмыслен именно суммой.
    """
    config = _worker_config(tmp_path)
    store = LocalJobStore(config.jobs_dir)
    for attempt, (written, acked, started) in {
        "older": (100, 98, 10.0),
        "newer": (7, 7, 20.0),
    }.items():
        store.update(JOB, attempt, local_state="finished", started_at=started)
        outbox = EventOutbox(store.job_dir(JOB, attempt) / "events")
        for index in range(written):
            outbox.append("log_line", {"n": index})
        outbox.ack(acked)

    rollup = diagnostics.collect_runtime_telemetry(config)["event_outbox"]
    assert rollup["attempts"] == 2
    assert (rollup["last_written_seq"], rollup["last_acked_seq"]) == (7, 7)
    assert rollup["pending"] == 2, "хвост старой попытки обязан быть виден"
    assert rollup["status"] == "pending"


def test_collecting_telemetry_never_writes_to_a_desynced_event_journal(tmp_path):
    """Диагностика не имеет права стать вторым писателем в живой журнал.

    Условие подобрано так, чтобы `EventOutbox.__init__` ТОЧНО захотел чинить:
    курсор намеренно отстаёт от сегментов. Через конструктор такой журнал был бы
    переписан (`_repair_cursor_against_segments` сохраняет курсор и ack), и
    рядом с пишущим исполнителем это гонка двух писателей за один файл. На
    согласованной фикстуре тест был бы зелёным и на опасной реализации тоже.
    """
    config = _worker_config(tmp_path)
    _attempt_with_events(config, written=9, acked=4)
    events = LocalJobStore(config.jobs_dir).job_dir(JOB, ATTEMPT) / "events"
    cursor = json.loads((events / "cursor.json").read_text(encoding="utf-8"))
    cursor["last_written_seq"] = 2          # курсор отстал от сегментов
    (events / "cursor.json").write_text(json.dumps(cursor), encoding="utf-8")

    before = {
        path.name: (path.stat().st_mtime_ns, path.read_bytes())
        for path in sorted(events.iterdir())
    }
    # Предусловие: через конструктор этот журнал ИМЕННО чинится и переписывается.
    probe = tmp_path / "probe"
    probe.mkdir()
    for item in events.iterdir():
        (probe / item.name).write_bytes(item.read_bytes())
    EventOutbox(probe)
    assert json.loads((probe / "cursor.json").read_text())["last_written_seq"] == 9, (
        "предусловие не выполнено: конструктор не стал чинить курсор"
    )

    time.sleep(0.01)
    telemetry = diagnostics.collect_runtime_telemetry(config)
    after = {
        path.name: (path.stat().st_mtime_ns, path.read_bytes())
        for path in sorted(events.iterdir())
    }
    assert after == before, "сбор диагностики изменил журнал событий"
    # И читает он то, что записано в курсоре, ничего не «починив» по дороге.
    assert telemetry["event_outbox"]["last_written_seq"] == 2


def test_position_reader_is_defensive_about_broken_cursor(tmp_path):
    events = tmp_path / "events"
    events.mkdir()
    (events / "cursor.json").write_text("{not json", encoding="utf-8")
    assert diagnostics.read_outbox_position(events) is None
    (events / "cursor.json").write_text('{"last_written_seq": "x"}', encoding="utf-8")
    assert diagnostics.read_outbox_position(events) is None


def test_position_reader_clamps_ack_above_written(tmp_path):
    events = tmp_path / "events"
    events.mkdir()
    (events / "cursor.json").write_text('{"last_written_seq": 5}', encoding="utf-8")
    (events / "ack.json").write_text('{"last_acked_seq": 9}', encoding="utf-8")
    assert diagnostics.read_outbox_position(events) == {
        "last_written_seq": 5, "last_acked_seq": 5,
    }


def test_telemetry_carries_gateway_target_and_release(tmp_path):
    telemetry = diagnostics.collect_runtime_telemetry(_worker_config(tmp_path))
    assert telemetry["gateway_target"] == "176.12.77.128:8443"
    assert telemetry["worker_release"] == "20260817T125827-f814d9f33058"
    assert telemetry["transport"] == "grpc_stream"


def test_polling_worker_reports_no_gateway_target(tmp_path):
    config = _worker_config(tmp_path, control_transport="polling")
    telemetry = diagnostics.collect_runtime_telemetry(config)
    assert "gateway_target" not in telemetry
    assert telemetry["transport"] == "polling"


def test_telemetry_is_json_serialisable_and_carries_no_secrets(tmp_path):
    config = _worker_config(tmp_path)
    _attempt_with_events(config, written=5, acked=5)
    blob = json.dumps(diagnostics.collect_runtime_telemetry(config), ensure_ascii=False)
    lowered = blob.lower()
    for forbidden in ("token", "secret", "authorization", "cookie", "private", "begin ",
                      "password", "api_key", "apikey", "bearer", ".pem"):
        assert forbidden not in lowered, f"в сводку просочилось «{forbidden}»"


# ═════════════ 2. Санитайзер центра ══════════════════════════════════════════
def test_sanitizer_keeps_only_whitelisted_runtime_fields():
    clean = worker_registry.sanitize_resource_snapshot({
        "at": 100.0,
        "runtime": {
            "transport": "grpc_stream",
            "gateway_target": "176.12.77.128:8443",
            "worker_release": "20260817T125827-f814d9f33058",
            "event_outbox": {"last_written_seq": 34, "last_acked_seq": 34, "pending": 0,
                             "status": "synced", "attempts": 1, "smuggled": "x"},
            "authorization": "Bearer secret-token",
            "cert_path": "/etc/ssl/private/client.key",
        },
    })
    runtime = clean["runtime"]
    assert set(runtime) == {"at", "transport", "gateway_target", "worker_release", "event_outbox"}
    assert "smuggled" not in runtime["event_outbox"]
    assert "Bearer" not in json.dumps(clean)


def test_sanitizer_rejects_non_numeric_sequence_values():
    clean = worker_registry.sanitize_resource_snapshot({
        "runtime": {"event_outbox": {"last_written_seq": "<script>", "pending": None}},
    })
    assert clean["runtime"]["event_outbox"] == {}


def test_sanitizer_truncates_long_gateway_target():
    clean = worker_registry.sanitize_resource_snapshot({"runtime": {"gateway_target": "a" * 400}})
    assert len(clean["runtime"]["gateway_target"]) == 120


def test_snapshot_without_runtime_section_has_no_runtime_key():
    assert "runtime" not in worker_registry.sanitize_resource_snapshot({"at": 1.0, "cpu": {}})


# ═════════════ 3. Проекция на экран ══════════════════════════════════════════
def _outbox_view(runtime, *, now=1000.0):
    return distributed_ui._event_outbox(runtime, now=now)


def test_view_reports_synced():
    view = _outbox_view({"at": 990.0, "event_outbox": {
        "last_written_seq": 34, "last_acked_seq": 34, "pending": 0, "last_ack_at": 985.0}})
    assert view["status"] == "synced"
    assert (view["lastWrittenSeq"], view["lastAckedSeq"], view["pending"]) == (34, 34, 0)
    assert view["lastAckAt"] is not None


def test_view_reports_pending():
    view = _outbox_view({"at": 990.0, "event_outbox": {
        "last_written_seq": 40, "last_acked_seq": 37, "pending": 3}})
    assert view["status"] == "pending"
    assert view["pending"] == 3


def test_view_reports_stale_telemetry():
    stale_at = 1000.0 - distributed_ui.RUNTIME_TELEMETRY_STALE_SEC - 1
    view = _outbox_view({"at": stale_at, "event_outbox": {
        "last_written_seq": 34, "last_acked_seq": 34, "pending": 0}})
    assert view["status"] == "stale"
    assert view["lastWrittenSeq"] == 34, "устаревшие числа показываем, но помечаем"


@pytest.mark.parametrize("runtime", [{}, {"event_outbox": {}}, {"event_outbox": {"pending": 1}}])
def test_view_reports_unavailable_without_inventing_zeroes(runtime):
    view = _outbox_view(runtime)
    assert view["status"] == "unavailable"
    assert view["lastWrittenSeq"] is None and view["pending"] is None


# ═════════════ 4. Перенос сводки через heartbeat ═════════════════════════════
@pytest.fixture()
def center(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "state"))
    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def _registered_worker(settings):
    from backend.app.services.distributed_workers import repositories

    return repositories.create_worker(
        display_name="worker-12i2", instance_id="inst_12i2", worker_version="12i2",
        protocol_version=1, pipeline_revision=None,
        capabilities={"job_types": ["audit_pipeline_v1"]}, configured_max_slots=1,
        settings=settings,
    )["worker_id"]


def _publish_runtime(worker_id, settings, runtime):
    """То, что делает роутер `/resources`: пишет снимок в ИСТОРИЮ."""
    from backend.app.services.distributed_workers import repositories

    clean = worker_registry.sanitize_resource_snapshot({"at": time.time(), "runtime": runtime})
    repositories.record_resource_snapshot(worker_id, clean, settings=settings)
    return clean


def _old_gateway_heartbeat(worker_id, settings):
    """Heartbeat в том виде, в каком его исполняет СТАРЫЙ релиз шлюза.

    Ключевой факт топологии: heartbeat потокового воркера обрабатывает шлюз, а
    он — отдельный процесс со своим, возможно более старым, деревом релиза. Он
    перезаписывает колонку `workers.resource_snapshot` целиком и о разделе
    `runtime` не знает вовсе. Диагностика обязана это пережить.
    """
    worker_registry.record_heartbeat(
        worker_id=worker_id, instance_id="inst_12i2", worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={"at": time.time(), "cpu": {"utilization_pct": 5.0}},
        warnings=[], settings=settings,
    )


def test_runtime_survives_heartbeat_written_by_an_older_gateway(center):
    from backend.app.services.distributed_workers import repositories

    worker_id = _registered_worker(center)
    _publish_runtime(worker_id, center, {
        "gateway_target": "176.12.77.128:8443",
        "event_outbox": {"last_written_seq": 34, "last_acked_seq": 34, "pending": 0},
    })
    for _ in range(5):
        _old_gateway_heartbeat(worker_id, center)

    stored = json.loads(repositories.get_worker(worker_id, settings=center)["resource_snapshot"])
    assert "runtime" not in stored, "предусловие: шлюз колонку действительно перетирает"

    runtime = repositories.latest_runtime_diagnostics(worker_id, settings=center)
    assert runtime is not None, "сводка обязана пережить heartbeat старого шлюза"
    assert runtime["gateway_target"] == "176.12.77.128:8443"
    assert runtime["event_outbox"]["last_written_seq"] == 34


def test_runtime_reader_returns_none_when_worker_never_reported(center):
    from backend.app.services.distributed_workers import repositories

    worker_id = _registered_worker(center)
    _old_gateway_heartbeat(worker_id, center)
    assert repositories.latest_runtime_diagnostics(worker_id, settings=center) is None


def test_runtime_reader_takes_the_newest_report(center):
    from backend.app.services.distributed_workers import repositories

    worker_id = _registered_worker(center)
    _publish_runtime(worker_id, center, {"gateway_target": "old:8443"})
    _old_gateway_heartbeat(worker_id, center)
    _publish_runtime(worker_id, center, {"gateway_target": "new:8443"})
    runtime = repositories.latest_runtime_diagnostics(worker_id, settings=center)
    assert runtime["gateway_target"] == "new:8443"
    # Метка времени — центра, а не воркера: воркер полу-доверенный.
    assert abs(runtime["at"] - time.time()) < 60


def test_runtime_only_post_never_erases_resources(center):
    """Одинокая сводка не должна стирать CPU/диск из колонки ресурсов."""
    from backend.app.services.distributed_workers import repositories

    worker_id = _registered_worker(center)
    worker_registry.record_heartbeat(
        worker_id=worker_id, instance_id="inst_12i2", worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={"at": time.time(), "cpu": {"utilization_pct": 7.0},
                           "disk": {"free_gb": 51.6}},
        warnings=[], settings=center,
    )
    clean = worker_registry.sanitize_resource_snapshot(
        {"at": time.time(), "runtime": {"gateway_target": "176.12.77.128:8443"}}
    )
    # Роутер обновляет колонку ТОЛЬКО при наличии настоящих разделов ресурсов.
    assert not any(section in clean for section in worker_registry.RESOURCE_SNAPSHOT_SECTIONS)
    stored = json.loads(repositories.get_worker(worker_id, settings=center)["resource_snapshot"])
    assert stored["cpu"]["utilization_pct"] == 7.0
    assert stored["disk"]["free_gb"] == 51.6


def test_absurd_sequence_number_cannot_break_the_operator_screen(center):
    """`10**999` от скомпрометированного воркера не должен ронять проекцию."""
    from backend.app.services.distributed_workers import repositories

    worker_id = _registered_worker(center)
    _publish_runtime(worker_id, center, {
        "event_outbox": {"last_written_seq": 10 ** 999, "last_acked_seq": 0, "pending": 10 ** 999},
    })
    runtime = repositories.latest_runtime_diagnostics(worker_id, settings=center) or {}
    view = distributed_ui._event_outbox(runtime, now=time.time())
    assert view["status"] == "unavailable"


# ═════════════ 5. Разъезд релизов центра и шлюза ═════════════════════════════
def _release(tmp_path: Path, name: str, *, release_id: str, wire: str, schema: int) -> Path:
    root = tmp_path / name
    descriptor = root / "app" / "contracts" / "agent_stream" / "v1"
    descriptor.mkdir(parents=True)
    # Сравнивается ПОЛНЫЙ контракт, а не сокращённый список критических полей:
    # смена номера поля в ResultReady прошла бы мимо `descriptor_snapshot.json`.
    (descriptor / "agent_stream_v1.desc").write_bytes(wire.encode("utf-8"))
    (descriptor / "agent_stream.proto").write_text("syntax = \"proto3\";\n", encoding="utf-8")
    (descriptor / "common.proto").write_text("syntax = \"proto3\";\n", encoding="utf-8")
    (descriptor / "descriptor_snapshot.json").write_text('{"critical_fields": {}}', encoding="utf-8")
    manifest = root / "release-manifest.json"
    manifest.write_text(json.dumps({
        "release_id": release_id, "commit": release_id[-8:] * 5,
        "database_schema": {"target": schema},
    }), encoding="utf-8")
    return manifest


def _settings_with(center_manifest, gateway_manifest, base):
    import dataclasses

    return dataclasses.replace(
        base, center_release_manifest=center_manifest, gateway_release_manifest=gateway_manifest
    )


def test_different_releases_are_supported_when_the_wire_matches(tmp_path, center):
    wire = "descriptor-bytes"
    settings = _settings_with(
        _release(tmp_path, "center", release_id="ui-real-43ee9769", wire=wire, schema=13),
        _release(tmp_path, "gateway", release_id="ui-real-16c533a7", wire=wire, schema=13),
        center,
    )
    verdict = distributed_ui._release_compatibility(settings=settings)
    assert verdict["status"] == "ok"
    assert verdict["centerRelease"] == "ui-real-43ee9769"
    assert verdict["gatewayRelease"] == "ui-real-16c533a7"
    assert verdict["wireContractMatches"] is True


def test_wire_contract_drift_is_reported_as_mismatch(tmp_path, center):
    settings = _settings_with(
        _release(tmp_path, "center", release_id="c", wire="descriptor-v2", schema=13),
        _release(tmp_path, "gateway", release_id="g", wire="descriptor-v1", schema=13),
        center,
    )
    verdict = distributed_ui._release_compatibility(settings=settings)
    assert verdict["status"] == "mismatch"
    assert verdict["wireContractMatches"] is False


def test_schema_target_drift_is_reported_as_mismatch(tmp_path, center):
    wire = "descriptor-bytes"
    settings = _settings_with(
        _release(tmp_path, "center", release_id="c", wire=wire, schema=14),
        _release(tmp_path, "gateway", release_id="g", wire=wire, schema=13),
        center,
    )
    assert distributed_ui._release_compatibility(settings=settings)["status"] == "mismatch"


def test_unknown_when_a_manifest_is_missing(tmp_path, center):
    wire = "descriptor-bytes"
    settings = _settings_with(
        _release(tmp_path, "center", release_id="c", wire=wire, schema=13),
        tmp_path / "absent" / "release-manifest.json",
        center,
    )
    verdict = distributed_ui._release_compatibility(settings=settings)
    assert verdict["status"] == "unknown"
    assert verdict["gatewayRelease"] is None
    assert verdict["reason"]


def test_unconfigured_manifests_do_not_invent_compatibility(center):
    verdict = distributed_ui._release_compatibility(settings=center)
    assert verdict["status"] == "unknown"
    assert verdict["centerRelease"] is None and verdict["gatewayRelease"] is None
