"""
Tests for endpoint priority: project-local critic_v2_triage_ui.json beats the
global /tmp export.

Контракт:
    A. Если есть <project>/_output/<CRITIC_V2_OUTPUT_SUBDIR>/critic_v2_triage_ui.json
       → endpoint возвращает его (source='project_local', matched_by='project_local_artifact').
    B. Если нет project-local — fallback на CRITIC_V2_UI_EXPORT_PATH
       (source='global_fallback' или 'global_fallback_empty').
    C. Если нет ни одного — warning + hint_command.
    D. Endpoint остаётся read-only.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_GLOBAL_FIXTURE = {
    "summary": {"total": 2, "primary_count": 1, "needs_context_count": 0,
                "suggested_reject_count": 1, "hidden_by_critic_count": 0,
                "primary_queue_reduction_percent": 50.0,
                "accepted_not_hidden_recall": None,
                "accepted_primary_visible_recall": None,
                "profile": "conservative", "experimental": True},
    "tabs": [
        {"key": "primary", "title": "Основная", "default_open": True,
         "queues": ["strong_keep"], "count": 1},
        {"key": "needs_context", "title": "Контекст", "default_open": False,
         "queues": ["needs_context"], "count": 0},
        {"key": "suggested_reject", "title": "К отклонению", "default_open": False,
         "queues": ["suggested_reject"], "count": 1},
        {"key": "hidden_by_critic", "title": "Скрыто", "default_open": False,
         "queues": ["hidden"], "count": 0},
    ],
    "items": [
        {"finding_id": "GLOB:F-001", "project_name": "GLOB",
         "section": "EOM", "title": "global1", "tab": "primary",
         "queue": "strong_keep"},
        {"finding_id": "GLOB:F-002", "project_name": "GLOB",
         "section": "EOM", "title": "global2", "tab": "suggested_reject",
         "queue": "suggested_reject"},
    ],
}

_LOCAL_FIXTURE = {
    "summary": {"total": 1, "primary_count": 1, "needs_context_count": 0,
                "suggested_reject_count": 0, "hidden_by_critic_count": 0,
                "primary_queue_reduction_percent": 0.0,
                "accepted_not_hidden_recall": None,
                "accepted_primary_visible_recall": None,
                "profile": "conservative", "experimental": True},
    "tabs": [
        {"key": "primary", "title": "Основная", "default_open": True,
         "queues": ["strong_keep"], "count": 1},
        {"key": "needs_context", "title": "Контекст", "default_open": False,
         "queues": ["needs_context"], "count": 0},
        {"key": "suggested_reject", "title": "К отклонению", "default_open": False,
         "queues": ["suggested_reject"], "count": 0},
        {"key": "hidden_by_critic", "title": "Скрыто", "default_open": False,
         "queues": ["hidden"], "count": 0},
    ],
    "items": [
        {"finding_id": "F-LOCAL-001", "project_name": "LOCAL-PROJECT",
         "section": "EOM", "title": "local item 1",
         "tab": "primary", "queue": "strong_keep",
         "score": 9, "reason": "local_test",
         "evidence_quality": "valid", "source_dependency": "enough_source",
         "taxonomy_reason": None},
    ],
    "experimental": True,
    "profile": "conservative",
    "generated_at": "2026-05-16T00:00:00Z",
    "source_project": {
        "project_id": "LOCAL-PROJECT",
        "project_name": "LOCAL-PROJECT",
        "project_dir": "/tmp/whatever",
    },
    "production_pipeline_modified": False,
}


@pytest.fixture
def projects_dir(tmp_path: Path) -> Path:
    """Synthetic projects/ root + один проект LOCAL-PROJECT."""
    root = tmp_path / "projects"
    p = root / "LOCAL-PROJECT"
    (p / "_output" / "critic_v2").mkdir(parents=True)
    (p / "_output" / "critic_v2" / "critic_v2_triage_ui.json").write_text(
        json.dumps(_LOCAL_FIXTURE, ensure_ascii=False), encoding="utf-8",
    )
    return root


@pytest.fixture
def global_artifact(tmp_path: Path) -> Path:
    p = tmp_path / "global" / "critic_v2_triage_ui.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(_GLOBAL_FIXTURE, ensure_ascii=False), encoding="utf-8")
    return p


def _make_client(monkeypatch, projects_dir: Path, global_artifact_path: Path | None):
    """Build a fresh TestClient with patched env + reloaded modules."""
    monkeypatch.setenv("AUDIT_PROJECTS_DIR", str(projects_dir))
    if global_artifact_path is not None:
        monkeypatch.setenv("CRITIC_V2_UI_EXPORT_PATH", str(global_artifact_path))
    else:
        monkeypatch.delenv("CRITIC_V2_UI_EXPORT_PATH", raising=False)

    # Перезагружаем config + project_service + router, чтобы env vars применились
    import importlib
    import backend.app.core.config as _cfg
    importlib.reload(_cfg)
    import backend.app.services.common.project_service as _ps
    importlib.reload(_ps)
    from backend.app.api.routers import critic_v2_ui as _mod
    importlib.reload(_mod)
    # Сбрасываем backend-кеш (mtime-based) — другой artifact path
    _mod._CACHE["key"] = None
    _mod._CACHE["artifact"] = None
    _mod._CACHE["items_by_exact"] = {}
    _mod._CACHE["items_by_normalized"] = {}

    app = FastAPI()
    app.include_router(_mod.router)
    return TestClient(app)


# ─── A. Project-local artifact wins ──────────────────────────────────────────


def test_project_local_artifact_takes_priority(monkeypatch, projects_dir, global_artifact):
    client = _make_client(monkeypatch, projects_dir, global_artifact)
    r = client.get("/api/critic-v2/projects/LOCAL-PROJECT/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "project_local"
    assert body["scope"]["matched_by"] == "project_local_artifact"
    # Items пришли из локального файла (1 шт), а не из глобального (2 шт)
    assert len(body["items"]) == 1
    assert body["items"][0]["finding_id"] == "F-LOCAL-001"
    assert body["profile"] == "conservative"
    assert body["generated_at"] == "2026-05-16T00:00:00Z"
    # Production safety markers
    assert body["production_pipeline_modified"] is False
    assert body["experimental"] is True


def test_project_local_artifact_summary_recomputed(monkeypatch, projects_dir, global_artifact):
    """Endpoint должен пересчитать summary под локальные items (как для глобального)."""
    client = _make_client(monkeypatch, projects_dir, global_artifact)
    body = client.get("/api/critic-v2/projects/LOCAL-PROJECT/triage-ui").json()
    s = body["summary"]
    assert s["total"] == 1
    assert s["primary_count"] == 1
    assert s["suggested_reject_count"] == 0


# ─── B. Fallback to global ───────────────────────────────────────────────────


def test_fallback_to_global_when_no_project_local(monkeypatch, tmp_path, global_artifact):
    """Проекта LOCAL нет → fallback к глобальному GLOB."""
    empty_projects = tmp_path / "empty-projects"
    empty_projects.mkdir()
    client = _make_client(monkeypatch, empty_projects, global_artifact)
    r = client.get("/api/critic-v2/projects/GLOB/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "global_fallback"
    assert body["scope"]["matched_by"] == "project_name"
    assert len(body["items"]) == 2


def test_fallback_returns_empty_with_warning_when_project_unknown(
    monkeypatch, tmp_path, global_artifact
):
    empty_projects = tmp_path / "empty-projects"
    empty_projects.mkdir()
    client = _make_client(monkeypatch, empty_projects, global_artifact)
    r = client.get("/api/critic-v2/projects/UNKNOWN/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "global_fallback_empty"
    assert body["items"] == []
    assert "warning" in body
    assert body["scope"]["matched_by"] is None


# ─── C. No global, no local → graceful warning ───────────────────────────────


def test_no_global_no_local_returns_warning(monkeypatch, tmp_path):
    """Глобальный artifact не существует, проекта тоже нет."""
    empty_projects = tmp_path / "empty-projects"
    empty_projects.mkdir()
    nonexistent = tmp_path / "doesnt-exist.json"
    client = _make_client(monkeypatch, empty_projects, nonexistent)
    r = client.get("/api/critic-v2/projects/SOMETHING/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "none"
    assert body["items"] == []
    assert "warning" in body
    assert "backfill" in body["warning"].lower()


def test_project_local_works_even_without_global(monkeypatch, projects_dir, tmp_path):
    """Глобального artifact нет — локальный всё равно отдаётся."""
    nonexistent = tmp_path / "no-global.json"
    client = _make_client(monkeypatch, projects_dir, nonexistent)
    r = client.get("/api/critic-v2/projects/LOCAL-PROJECT/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "project_local"
    assert len(body["items"]) == 1


# ─── E. AR-style project_id mismatch (.pdf suffix) ───────────────────────────


@pytest.fixture
def ar_project_layout(tmp_path: Path) -> Path:
    """Synthetic projects/ with an AR-style 3-level layout:

      projects/
        <OBJECT>/
          AR/
            13АВ-РД-АР0.1-ПА/        ← folder name (no .pdf)
              _output/
                critic_v2/critic_v2_triage_ui.json
                critic_v2_assisted_round2/critic_v2_triage_ui.json

    But project_info.json declares project_id = "13АВ-РД-АР0.1-ПА.pdf"
    (with .pdf), so the endpoint is called with the .pdf-suffixed id.
    """
    root = tmp_path / "projects"
    folder = root / "214. Alia (ASTERUS)" / "AR" / "13АВ-РД-АР0.1-ПА"
    (folder / "_output" / "critic_v2").mkdir(parents=True)
    (folder / "_output" / "critic_v2_assisted_round2").mkdir(parents=True)

    ar_local = dict(_LOCAL_FIXTURE)
    ar_local["profile"] = "assisted_round2_candidate"
    ar_local["source_project"] = {
        "project_id": "13АВ-РД-АР0.1-ПА.pdf",
        "project_name": "13АВ-РД-АР0.1-ПА",
        "project_dir": str(folder),
    }
    (folder / "_output" / "critic_v2" / "critic_v2_triage_ui.json").write_text(
        json.dumps({**ar_local, "profile": "conservative"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (folder / "_output" / "critic_v2_assisted_round2" / "critic_v2_triage_ui.json").write_text(
        json.dumps(ar_local, ensure_ascii=False), encoding="utf-8",
    )
    return root


def test_ar_project_id_with_pdf_suffix_resolves_to_folder_without_pdf(
    monkeypatch, ar_project_layout, global_artifact
):
    """AR projects: project_id='<name>.pdf' must resolve to folder '<name>' in 3-level layout."""
    client = _make_client(monkeypatch, ar_project_layout, global_artifact)
    # Call with .pdf-suffixed id (as the UI does for AR projects)
    r = client.get("/api/critic-v2/projects/13АВ-РД-АР0.1-ПА.pdf/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "project_local", body
    assert body["scope"]["matched_by"] == "project_local_artifact"
    assert body["profile"] == "conservative"  # default subdir = critic_v2
    assert len(body["items"]) == 1


def test_ar_project_id_with_pdf_suffix_reads_round2_subdir(
    monkeypatch, ar_project_layout, global_artifact
):
    """With CRITIC_V2_OUTPUT_SUBDIR=critic_v2_assisted_round2, AR .pdf id must still work."""
    monkeypatch.setenv("CRITIC_V2_OUTPUT_SUBDIR", "critic_v2_assisted_round2")
    client = _make_client(monkeypatch, ar_project_layout, global_artifact)
    r = client.get("/api/critic-v2/projects/13АВ-РД-АР0.1-ПА.pdf/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "project_local"
    assert body["profile"] == "assisted_round2_candidate"


def test_ar_project_id_without_pdf_still_works(
    monkeypatch, ar_project_layout, global_artifact
):
    """Backward compat: project_id without .pdf must also resolve."""
    client = _make_client(monkeypatch, ar_project_layout, global_artifact)
    r = client.get("/api/critic-v2/projects/13АВ-РД-АР0.1-ПА/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["source"] == "project_local"


# ─── D. Endpoint stays read-only ─────────────────────────────────────────────


def test_endpoint_does_not_modify_local_artifact(monkeypatch, projects_dir, global_artifact):
    client = _make_client(monkeypatch, projects_dir, global_artifact)
    local_path = (projects_dir / "LOCAL-PROJECT" / "_output" / "critic_v2"
                  / "critic_v2_triage_ui.json")
    before_mt = local_path.stat().st_mtime_ns
    before_body = local_path.read_bytes()

    for _ in range(5):
        client.get("/api/critic-v2/projects/LOCAL-PROJECT/triage-ui")

    assert local_path.stat().st_mtime_ns == before_mt
    assert local_path.read_bytes() == before_body
