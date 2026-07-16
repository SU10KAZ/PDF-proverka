import json

import pytest

from backend.app.services.common import audit_logger


def _enable_v2_primary(monkeypatch, v2_root):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    (v2_root / "objects").mkdir(parents=True)


def test_unknown_project_log_never_falls_back_to_legacy(tmp_path, monkeypatch):
    v2_root = tmp_path / "projects_v2"
    legacy_root = tmp_path / "projects"
    _enable_v2_primary(monkeypatch, v2_root)

    def legacy_fallback_forbidden(*args, **kwargs):
        raise AssertionError("v2-primary logger attempted a legacy fallback")

    monkeypatch.setattr(
        audit_logger, "resolve_project_dir", legacy_fallback_forbidden,
    )

    # Persistent logging is fail-soft for an unknown id, but must not create a
    # replacement legacy project directory.
    audit_logger.persist_log("M31A", "synthetic pytest job", "error", "prepare")

    assert not legacy_root.exists()
    assert not list(v2_root.rglob("audit_log.jsonl"))


def test_unknown_project_pipeline_log_fails_without_legacy_write(tmp_path, monkeypatch):
    v2_root = tmp_path / "projects_v2"
    legacy_root = tmp_path / "projects"
    _enable_v2_primary(monkeypatch, v2_root)

    with pytest.raises(FileNotFoundError, match="projects_v2"):
        audit_logger.update_pipeline_log("M31A", "prepare", "running")

    assert not legacy_root.exists()


def test_batch_service_log_is_written_under_projects_v2_system(tmp_path, monkeypatch):
    v2_root = tmp_path / "projects_v2"
    legacy_root = tmp_path / "projects"
    _enable_v2_primary(monkeypatch, v2_root)

    audit_logger.persist_log(
        "__BATCH__", "pause requested", "warn", "prepare",
    )

    log_path = (
        v2_root / "_system" / "runtime_logs" / "audit" / "batch"
        / "audit_log.jsonl"
    )
    entry = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert entry["message"] == "pause requested"
    assert entry["level"] == "warn"
    assert not legacy_root.exists()
