from __future__ import annotations

from argparse import Namespace

from backend.app.services.distributed_workers import job_service
from backend.app.services.distributed_workers import settings as worker_settings
from tools import physical_12e_harness


def test_sequential_job_label_obeys_test_job_contract():
    from backend.app.models.distributed_workers import TestJobParams

    label = physical_12e_harness._sequential_label(1)
    assert TestJobParams(label=label, steps=1, step_seconds=0).label == label


def test_create_job_uses_an_authorized_center_actor(tmp_path, monkeypatch):
    root = tmp_path / "isolated-12e"
    captured = {}

    monkeypatch.setattr(physical_12e_harness, "_safe_root", lambda _raw: root)
    monkeypatch.setattr(physical_12e_harness, "_center_env", lambda _root: None)
    monkeypatch.setattr(
        physical_12e_harness,
        "_context",
        lambda _root: {"worker_id": "wrk_test"},
    )
    monkeypatch.setattr(worker_settings, "get_settings", lambda: object())

    def fake_create_test_job(**kwargs):
        captured.update(kwargs)
        return {
            "job_id": "job_test",
            "attempt_id": "att_test",
            "state": "assigned",
            "assigned_worker_id": "wrk_test",
            "package_id": "pkg_test",
        }

    monkeypatch.setattr(job_service, "create_test_job", fake_create_test_job)

    result = physical_12e_harness.create_job(
        Namespace(
            root=str(root),
            project_id="project-test",
            label="synthetic",
            steps=2,
            step_seconds=0.0,
        )
    )

    assert result == 0
    assert captured["actor"] == "center:12e-isolated-harness"
    assert captured["worker_id"] == "wrk_test"
