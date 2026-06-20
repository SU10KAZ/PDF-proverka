"""reserc.md #4 — тесты detect_resume_stage (раньше 0 тестов).

Version-aware: detect_resume_stage резолвит _output активной версии. Здесь —
детерминированные кейсы (невалидная версия, пустой проект) без тяжёлых
gemma-фикстур.
"""
from __future__ import annotations

import backend.app.pipeline.resume_detector as rd


def test_invalid_version_not_resumable(monkeypatch, tmp_path):
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)

    def _raise(*a, **k):
        raise rd.version_service.VersionNotFoundError("v9")

    monkeypatch.setattr(rd.version_service, "get_version_dir", _raise)

    res = rd.detect_resume_stage("p", version_id="v9")
    assert res["can_resume"] is False
    assert res["stage"] == "prepare"


def test_empty_project_resumes_at_prepare(monkeypatch, tmp_path):
    (tmp_path / "_output").mkdir()
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)
    monkeypatch.setattr(rd.version_service, "get_version_dir", lambda root, pid, vid: tmp_path)

    res = rd.detect_resume_stage("p")
    # Пустой проект (нет md/blocks/01/02/03) → начинаем с подготовки.
    assert res["stage"] == "prepare"
    assert res["can_resume"] is True


def test_returns_contract_keys(monkeypatch, tmp_path):
    (tmp_path / "_output").mkdir()
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)
    monkeypatch.setattr(rd.version_service, "get_version_dir", lambda root, pid, vid: tmp_path)

    res = rd.detect_resume_stage("p")
    # Контракт результата стабилен (его читают API/UI).
    for key in ("stage", "stage_label", "detail", "can_resume"):
        assert key in res, f"detect_resume_stage потерял ключ {key}"
