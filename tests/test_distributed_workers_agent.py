"""
test_distributed_workers_agent.py
---------------------------------
Пакет audit_worker: дисковый outbox, валидация параметров тестового задания,
построение argv, реестр процессов, расчёт слотов, очистка секретов, локальное
состояние и признак retention_unconfirmed.

Главное, что здесь закрепляется: центр НЕ может передать воркеру команду.
Никакой ветки, где argv или env приходят из задания, не существует.

Run: python -m pytest tests/test_distributed_workers_agent.py -v
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Параметры тестового задания: три рубежа зажима ──────────────────────────
def test_params_are_clamped_not_trusted():
    from audit_worker import test_runner

    params = test_runner.validate_params(
        {"label": "smoke", "steps": 100_000, "step_seconds": 0.0,
         "result_bytes": 10 ** 12},
        max_total_sec=300.0,
    )
    assert params.steps == test_runner.MAX_STEPS
    assert params.result_bytes == test_runner.MAX_RESULT_BYTES


def test_params_reject_unknown_fields():
    """Попытка протащить лишнее поле отвергается, а не игнорируется молча."""
    from audit_worker import test_runner

    with pytest.raises(test_runner.TestJobRejected) as info:
        test_runner.validate_params(
            {"label": "x", "cmd": "rm -rf /", "env": {"PATH": "/evil"}},
            max_total_sec=300.0,
        )
    assert "cmd" in str(info.value)


@pytest.mark.parametrize(
    "label",
    ["../../etc/passwd", "a; rm -rf /", "$(whoami)", "with space", "`id`", "a|b"],
)
def test_label_rejects_injection_shapes(label):
    from audit_worker import test_runner

    with pytest.raises(test_runner.TestJobRejected):
        test_runner.validate_params({"label": label}, max_total_sec=300.0)


def test_params_reject_too_long_job():
    from audit_worker import test_runner

    with pytest.raises(test_runner.TestJobRejected) as info:
        test_runner.validate_params(
            {"steps": 100, "step_seconds": 10.0}, max_total_sec=60.0
        )
    assert "потолок" in str(info.value)


# ─── argv и окружение строит ВОРКЕР ──────────────────────────────────────────
def test_argv_is_fixed_and_built_by_worker(tmp_path):
    from audit_worker import test_process, test_runner

    argv = test_runner.build_argv(tmp_path / "params.json")
    assert len(argv) == 4
    assert argv[0] == (sys.executable or "python3")
    assert argv[1] == "-u"
    assert argv[2] == str(Path(test_process.__file__).resolve())
    assert argv[3] == str(tmp_path / "params.json")


def test_env_is_whitelisted(monkeypatch):
    from audit_worker import test_runner

    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-should-not-leak")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "secret")
    env = test_runner.build_env()
    assert "OPENROUTER_API_KEY" not in env
    assert "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET" not in env
    assert set(env) <= set(test_runner._ENV_WHITELIST)


def test_test_process_runs_and_writes_artifacts(tmp_path):
    from audit_worker import test_runner

    params = test_runner.validate_params(
        {"label": "unit", "steps": 2, "step_seconds": 0.0, "result_bytes": 128},
        max_total_sec=60.0,
    )
    seen: list[tuple[int, int]] = []
    outcome = test_runner.run_test_job(
        params=params,
        job_dir=tmp_path / "job",
        on_progress=lambda s, t, e, m: seen.append((s, t)),
        on_log=lambda level, line: None,
    )
    assert outcome.exit_code == 0
    assert seen == [(1, 2), (2, 2)]
    summary = json.loads((tmp_path / "job" / "result" / "summary.json").read_text())
    assert summary["status"] == "ok" and summary["steps"] == 2
    assert (tmp_path / "job" / "result" / "run_log.txt").is_file()


def test_test_process_reports_failure(tmp_path):
    from audit_worker import test_runner

    params = test_runner.validate_params(
        {"label": "fail", "steps": 3, "step_seconds": 0.0, "fail_at_step": 2},
        max_total_sec=60.0,
    )
    outcome = test_runner.run_test_job(
        params=params, job_dir=tmp_path / "job",
        on_progress=lambda *a: None, on_log=lambda *a: None,
    )
    assert outcome.exit_code != 0
    assert outcome.failed_message


# ─── EventOutbox ─────────────────────────────────────────────────────────────
def test_outbox_seq_is_monotonic_and_survives_restart(tmp_path):
    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events")
    assert outbox.append("job_started", {"a": 1}) == 1
    assert outbox.append("log_line", {"message": "x"}) == 2

    reopened = EventOutbox(tmp_path / "events")     # «рестарт воркера»
    assert reopened.last_written_seq == 2
    assert reopened.append("stage_completed", {}) == 3   # нумерация НЕ сбрасывается


def test_outbox_batch_is_contiguous_and_ack_advances(tmp_path):
    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events")
    for i in range(5):
        outbox.append("log_line", {"message": f"m{i}"})
    batch = outbox.pending_batch()
    assert [e["seq"] for e in batch] == [1, 2, 3, 4, 5]

    outbox.ack(3)
    assert [e["seq"] for e in outbox.pending_batch()] == [4, 5]
    assert outbox.has_pending
    outbox.ack(5)
    assert not outbox.has_pending


def test_outbox_rewind_after_gap(tmp_path):
    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events")
    for _ in range(4):
        outbox.append("log_line", {})
    outbox.ack(4)
    outbox.rewind_to(2)          # центр сказал: ждём seq 2
    assert [e["seq"] for e in outbox.pending_batch()] == [2, 3, 4]


def test_outbox_redacts_secrets_on_write(tmp_path):
    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events", secret_literals=("wtk_supersecrettoken",))
    outbox.append(
        "log_line",
        {"message": "запуск с токеном wtk_supersecrettoken и ANTHROPIC_API_KEY=sk-abc123456789012345"},
    )
    raw = (tmp_path / "events" / "outbox-0001.jsonl").read_text(encoding="utf-8")
    assert "supersecrettoken" not in raw
    assert "sk-abc123456789012345" not in raw


def test_outbox_truncation_is_visible(tmp_path, monkeypatch):
    """Потеря строк лога при переполнении должна быть ЯВНОЙ, а не молчаливой."""
    import audit_worker.event_outbox as mod

    monkeypatch.setattr(mod, "OUTBOX_MAX_BYTES", 2048)
    outbox = mod.EventOutbox(tmp_path / "events")
    for i in range(200):
        outbox.append("log_line", {"message": "x" * 64})
    content = (tmp_path / "events" / "outbox-0001.jsonl").read_text(encoding="utf-8")
    assert "events_truncated" in content
    # Структурное событие проходит даже при переполнении.
    seq = outbox.append("job_failed", {"code": "x"})
    assert seq is not None


# ─── Реестр процессов ────────────────────────────────────────────────────────
def test_process_registry_detects_dead_and_pid_reuse(tmp_path):
    from audit_worker.process_registry import ProcessRegistry, is_alive, process_start_time

    registry = ProcessRegistry(tmp_path)
    my_pid = os.getpid()
    registry.register(my_pid, job_id="j1", attempt_id="a1")
    assert registry.alive_for_job("j1", "a1")
    assert registry.live_count() == 1

    # Тот же pid, но с чужой меткой старта — это НЕ наш процесс.
    fake_start = (process_start_time(my_pid) or 0.0) + 999.0
    assert is_alive(my_pid, fake_start) is False

    registry.register(2_147_483_600, job_id="j2", attempt_id="a2")   # заведомо мёртвый
    assert registry.prune_dead() >= 1


def test_process_registry_survives_restart(tmp_path):
    from audit_worker.process_registry import ProcessRegistry

    first = ProcessRegistry(tmp_path)
    first.register(os.getpid(), job_id="j1", attempt_id="a1")
    second = ProcessRegistry(tmp_path)      # «рестарт агента»
    assert second.alive_for_job("j1", "a1")


# ─── Слоты ───────────────────────────────────────────────────────────────────
def test_slots_hard_zero_on_swap_and_disk(tmp_path):
    from audit_worker.resource_monitor import ResourceMonitor

    monitor = ResourceMonitor(tmp_path, configured_max_slots=5)
    swapped = monitor.calculate_slots(
        ram_available_gb=64, swap_used_gb=2.0, disk_free_gb=500,
        cores=16, la5=0.1, active_jobs=0, now=1000.0,
    )
    assert swapped.calculated_free == 0 and swapped.binding_constraint == "s_ram"

    monitor2 = ResourceMonitor(tmp_path, configured_max_slots=5)
    low_disk = monitor2.calculate_slots(
        ram_available_gb=64, swap_used_gb=0.0, disk_free_gb=1.0,
        cores=16, la5=0.1, active_jobs=0, now=1000.0,
    )
    assert low_disk.calculated_free == 0 and low_disk.binding_constraint == "s_disk"


def test_slots_respect_hard_cap_and_config(tmp_path):
    from audit_worker.resource_monitor import HARD_CAP, ResourceMonitor

    monitor = ResourceMonitor(tmp_path, configured_max_slots=5)
    big = monitor.calculate_slots(
        ram_available_gb=512, swap_used_gb=0, disk_free_gb=5000,
        cores=128, la5=0.0, active_jobs=0, now=1000.0,
    )
    assert big.calculated_free == HARD_CAP

    limited = ResourceMonitor(tmp_path, configured_max_slots=2).calculate_slots(
        ram_available_gb=512, swap_used_gb=0, disk_free_gb=5000,
        cores=128, la5=0.0, active_jobs=0, now=1000.0,
    )
    assert limited.calculated_free == 2 and limited.binding_constraint == "s_cfg"


def test_slots_hysteresis_shrinks_fast_grows_slow(tmp_path):
    from audit_worker.resource_monitor import SLOT_GROW_STABLE_SEC, ResourceMonitor

    monitor = ResourceMonitor(tmp_path, configured_max_slots=5)
    good = dict(ram_available_gb=64, swap_used_gb=0, disk_free_gb=500,
                cores=16, la5=0.0, active_jobs=0)
    assert monitor.calculate_slots(**good, now=0.0).calculated_free == 5
    # Сокращение — мгновенно.
    assert monitor.calculate_slots(**{**good, "active_jobs": 4}, now=1.0).calculated_free == 1
    # Рост — только после периода стабильности.
    assert monitor.calculate_slots(**good, now=2.0).calculated_free == 1
    assert monitor.calculate_slots(**good, now=3.0).calculated_free == 1
    assert monitor.calculate_slots(
        **good, now=3.0 + SLOT_GROW_STABLE_SEC + 1
    ).calculated_free == 5


def test_snapshot_explains_binding_constraint(tmp_path):
    from audit_worker.resource_monitor import ResourceMonitor

    snapshot = ResourceMonitor(tmp_path, configured_max_slots=1).snapshot()
    assert snapshot["slots"]["binding_constraint"]
    assert snapshot["slots"]["explanation"]
    assert set(snapshot) >= {"ram", "cpu", "disk", "processes", "slots"}


# ─── Локальное состояние ─────────────────────────────────────────────────────
def test_local_job_store_retention_unconfirmed(tmp_path):
    from audit_worker.local_store import LocalJobStore

    store = LocalJobStore(tmp_path / "jobs")
    store.create({"job_id": "j1", "attempt_id": "a1", "job_type": "test_pipeline_v1"})
    store.update("j1", "a1", result_hash="abc", local_state="completed_locally")
    assert len(store.retention_unconfirmed()) == 1

    store.update("j1", "a1", retention_until=time.time() + 100)
    assert store.retention_unconfirmed() == []


def test_token_file_permissions(tmp_path):
    from audit_worker.local_store import WorkerStateStore

    store = WorkerStateStore(tmp_path / "state.json", tmp_path / "token")
    store.write_token("wtk_abc")
    assert oct((tmp_path / "token").stat().st_mode)[-3:] == "600"
    assert store.read_token() == "wtk_abc"


def test_atomic_write_leaves_no_partial(tmp_path):
    from audit_worker.local_store import atomic_write_json, read_json

    target = tmp_path / "nested" / "meta.json"
    atomic_write_json(target, {"a": 1})
    assert read_json(target) == {"a": 1}
    assert not list(tmp_path.rglob("*.tmp*"))


# ─── Безопасность пакета на стороне воркера ──────────────────────────────────
def test_worker_rejects_bad_hash(tmp_path):
    from audit_worker import package_io

    archive = tmp_path / "pkg.tar.gz"
    import io
    import tarfile

    with tarfile.open(archive, "w:gz") as tar:
        data = json.dumps({"manifest_version": 1}).encode()
        info = tarfile.TarInfo("package_manifest.json")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    with pytest.raises(package_io.BundleError) as exc:
        package_io.verify_and_unpack(
            archive=archive, expected_sha256="0" * 64, work_dir=tmp_path / "work"
        )
    assert "SHA-256" in str(exc.value)


def test_worker_rejects_traversal(tmp_path):
    from audit_worker import package_io

    import io
    import tarfile

    archive = tmp_path / "evil.tar.gz"
    manifest = json.dumps(
        {"manifest_version": 1, "archive": {"uncompressed_bytes": 10, "entries": 2}}
    ).encode()
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo("package_manifest.json")
        info.size = len(manifest)
        tar.addfile(info, io.BytesIO(manifest))
        evil = tarfile.TarInfo("payload/../../escape.txt")
        evil.size = 3
        tar.addfile(evil, io.BytesIO(b"bad"))
    digest = package_io.sha256_file(archive)
    with pytest.raises(package_io.BundleError):
        package_io.verify_and_unpack(
            archive=archive, expected_sha256=digest, work_dir=tmp_path / "work"
        )
    assert not (tmp_path / "work").exists()


def test_result_package_requires_content(tmp_path):
    from audit_worker import package_io

    (tmp_path / "result").mkdir()
    with pytest.raises(package_io.BundleError):
        package_io.build_result_package(
            dest_path=tmp_path / "out.tar.gz", result_dir=tmp_path / "result",
            job_id="j", attempt_id="a", project_id="p", version_id=None,
            worker_version="0.1.0", protocol_version=1, manifest_version=1,
        )


# ─── Очистка секретов ────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "text,leaked",
    [
        ("OPENROUTER_API_KEY=sk-abcdefghijklmnopqrstuvwxyz", "sk-abcdefghij"),
        ("Authorization: Bearer wtk_aaaaaaaaaaaaaaaaaaaa", "wtk_aaaaaaaaaaaaaaaaaaaa"),
        ('{"worker_token": "wtk_bbbbbbbbbbbbbbbb"}', "wtk_bbbbbbbbbbbbbbbb"),
        ("PORTAL_SESSION_SECRET=verysecretvalue123", "verysecretvalue123"),
        ("ключ sk-ant-api03-XXXXXXXXXXXXXXXX в логе", "sk-ant-api03-XXXXXXXXXXXXXXXX"),
    ],
)
def test_redaction_removes_known_shapes(text, leaked):
    from audit_worker import redaction

    assert leaked not in redaction.redact(text)


def test_redaction_failure_drops_line_instead_of_leaking(monkeypatch):
    from audit_worker import redaction

    def boom(*args, **kwargs):
        raise RuntimeError("редактор упал")

    monkeypatch.setattr(redaction, "_redact_once", boom)
    out = redaction.redact("совершенно секретная строка")
    assert "секретная" not in out
    assert "redaction_failed" in out


def test_redaction_handles_nested_payload():
    from audit_worker import redaction

    payload = {"outer": {"list": ["ANTHROPIC_API_KEY=sk-zzzzzzzzzzzzzzzzzzzzzz"]}}
    cleaned = redaction.redact_mapping(payload)
    assert "sk-zzzzzzzzzzzzzzzzzzzzzz" not in json.dumps(cleaned)
