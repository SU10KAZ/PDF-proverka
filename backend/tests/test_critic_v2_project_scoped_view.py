"""
Tests for the experimental project-scoped Critic v2 UI endpoint.

Verifies:
  1) endpoint returns project-scoped export (items filtered by project_name);
  2) summary recomputed for the project (not global counts);
  3) tab counts recomputed for the project;
  4) scope is included in response;
  5) missing artifact returns 404 with hint_command;
  6) unknown project returns empty result with warning (no 404);
  7) endpoint is read-only: no files written, no LLM, no production paths touched;
  8) frontend has the per-project button and view block wiring.

These checks pin down the offline/experimental contract — production pipeline
and 03_findings_review.json are not touched anywhere in this code path.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures: synthetic UI artifact with two projects.
# ──────────────────────────────────────────────────────────────────────────────

ARTIFACT_FIXTURE = {
    "summary": {
        "total": 4,
        "primary_count": 2,
        "needs_context_count": 1,
        "suggested_reject_count": 1,
        "hidden_by_critic_count": 0,
        "primary_queue_reduction_percent": 50.0,
        "accepted_not_hidden_recall": 1.0,
        "accepted_primary_visible_recall": 1.0,
        "profile": "conservative",
        "experimental": True,
    },
    "tabs": [
        {"key": "primary", "title": "Основная проверка", "default_open": True,
         "queues": ["strong_keep"], "count": 2},
        {"key": "needs_context", "title": "Требует смежников / контекста",
         "default_open": False, "queues": ["needs_context"], "count": 1},
        {"key": "suggested_reject", "title": "Критик рекомендует отклонить",
         "default_open": False, "queues": ["suggested_reject"], "count": 1},
        {"key": "hidden_by_critic", "title": "Скрыто критиком",
         "default_open": False, "queues": ["hidden"], "count": 0},
    ],
    "items": [
        {"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
         "title": "t1", "tab": "primary", "queue": "strong_keep",
         "human_decision": "accepted"},
        {"finding_id": "P1:F-002", "project_name": "P1", "section": "AR",
         "title": "t2", "tab": "needs_context", "queue": "needs_context"},
        {"finding_id": "P2:F-001", "project_name": "P2", "section": "EOM",
         "title": "t3", "tab": "primary", "queue": "strong_keep",
         "human_decision": "accepted"},
        {"finding_id": "P2:F-002", "project_name": "P2", "section": "EOM",
         "title": "t4", "tab": "suggested_reject", "queue": "suggested_reject"},
    ],
}


@pytest.fixture
def artifact_path(tmp_path: Path) -> Path:
    p = tmp_path / "critic_v2_triage_ui.json"
    p.write_text(json.dumps(ARTIFACT_FIXTURE, ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture
def client(artifact_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("CRITIC_V2_UI_EXPORT_PATH", str(artifact_path))
    # Re-import to pick up env. Router resolves path lazily per-request.
    from backend.app.api.routers import critic_v2_ui as _mod
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(_mod.router)
    return TestClient(app)


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────


def test_artifact_info_reports_path_and_exists(client: TestClient, artifact_path: Path):
    r = client.get("/api/critic-v2/artifact-info")
    assert r.status_code == 200
    body = r.json()
    assert body["expected_path"] == str(artifact_path)
    assert body["exists"] is True
    assert body["experimental"] is True
    assert body["production_pipeline_modified"] is False


def test_project_scope_filters_items(client: TestClient):
    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r.status_code == 200
    body = r.json()
    # Only P1 items
    assert len(body["items"]) == 2
    for it in body["items"]:
        assert it["project_name"] == "P1"


def test_project_scope_summary_recomputed(client: TestClient):
    """Summary is recomputed for the project, not the global file."""
    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    body = r.json()
    s = body["summary"]
    assert s["total"] == 2  # not 4
    assert s["primary_count"] == 1  # not 2
    assert s["needs_context_count"] == 1
    assert s["suggested_reject_count"] == 0
    assert s["hidden_by_critic_count"] == 0
    # 1 collapsed (needs_context) out of 2 → 50%
    assert s["primary_queue_reduction_percent"] == 50.0
    assert s["profile"] == "conservative"
    assert s["experimental"] is True


def test_project_scope_tabs_recounted(client: TestClient):
    r = client.get("/api/critic-v2/projects/P2/triage-ui")
    body = r.json()
    counts = {t["key"]: t["count"] for t in body["tabs"]}
    assert counts == {"primary": 1, "needs_context": 0, "suggested_reject": 1,
                      "hidden_by_critic": 0}


def test_project_scope_includes_scope_block(client: TestClient):
    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    body = r.json()
    assert body["scope"] == {
        "mode": "project",
        "project_id": "P1",
        "project_name": "P1",
        "matched_by": "project_name",
    }


def test_unknown_project_returns_empty_with_warning(client: TestClient):
    r = client.get("/api/critic-v2/projects/UNKNOWN/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["items"] == []
    assert body["summary"]["total"] == 0
    assert body["scope"]["mode"] == "project"
    assert body["scope"]["matched_by"] is None
    assert "warning" in body
    assert body["experimental"] is True
    assert body["production_pipeline_modified"] is False


def test_pdf_suffix_match(client: TestClient, tmp_path: Path,
                         monkeypatch: pytest.MonkeyPatch):
    """project_id with .pdf suffix should match project_name without it."""
    fixture = json.loads(json.dumps(ARTIFACT_FIXTURE))
    fixture["items"] = [
        {"finding_id": "X:F-001", "project_name": "MyProject",
         "section": "AR", "title": "t", "tab": "primary",
         "queue": "strong_keep"}
    ]
    p = tmp_path / "ui.json"
    p.write_text(json.dumps(fixture), encoding="utf-8")
    monkeypatch.setenv("CRITIC_V2_UI_EXPORT_PATH", str(p))
    from backend.app.api.routers import critic_v2_ui as _mod
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(_mod.router)
    c = TestClient(app)
    r = c.get("/api/critic-v2/projects/MyProject.pdf/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 1
    assert body["scope"]["matched_by"] == "project_name_no_pdf"


def test_missing_artifact_returns_404_with_hint(tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch):
    nonexistent = tmp_path / "nope.json"
    monkeypatch.setenv("CRITIC_V2_UI_EXPORT_PATH", str(nonexistent))
    from backend.app.api.routers import critic_v2_ui as _mod
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(_mod.router)
    c = TestClient(app)
    r = c.get("/api/critic-v2/projects/P1/triage-ui")
    assert r.status_code == 404
    detail = r.json()["detail"]
    assert detail["error"] == "critic_v2_artifact_missing"
    assert "expected_path" in detail
    assert "hint_command" in detail
    assert "replay_critic_v2_triage_policy" in detail["hint_command"]


def test_endpoint_does_not_write_files(client: TestClient, tmp_path: Path,
                                       monkeypatch: pytest.MonkeyPatch):
    """The endpoint must not touch _output, the artifact directory, or anything else."""
    # Snapshot artifact mtime.
    art = Path(os.environ["CRITIC_V2_UI_EXPORT_PATH"])
    before = art.stat().st_mtime_ns

    # Hit endpoints multiple times.
    client.get("/api/critic-v2/artifact-info")
    client.get("/api/critic-v2/projects/P1/triage-ui")
    client.get("/api/critic-v2/projects/P2/triage-ui")
    client.get("/api/critic-v2/projects/UNKNOWN/triage-ui")

    # Artifact untouched.
    assert art.stat().st_mtime_ns == before


def test_endpoint_does_not_import_pipeline_modules():
    """
    Sanity guard: the router must not import production pipeline modules.

    If someone wires this endpoint into manager.py or runner.py, this test will
    catch it. We only allow stdlib + fastapi imports.
    """
    src = (Path(_PROJECT_ROOT) / "backend" / "app" / "api" / "routers"
           / "critic_v2_ui.py").read_text(encoding="utf-8")
    forbidden = [
        "from backend.app.pipeline",
        "import backend.app.pipeline",
        "from backend.app.services.findings",
        "anthropic",  # no LLM SDK
        "openai",
    ]
    for token in forbidden:
        assert token not in src, f"Forbidden import found in router: {token}"


# ──────────────────────────────────────────────────────────────────────────────
# Frontend wiring smoke checks.
# ──────────────────────────────────────────────────────────────────────────────

INDEX_HTML = _PROJECT_ROOT / "frontend" / "index.html"
APP_JS = _PROJECT_ROOT / "frontend" / "static" / "js" / "app.js"


def test_frontend_has_per_project_critic_v2_button():
    """Button must be in project nav, after 'Проработка замечаний'."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    # The button uses the navigate('/critic-v2') hash route.
    assert "/critic-v2'" in html
    assert "currentView === 'critic-v2-project'" in html
    # And it should appear after the 'Проработка замечаний' button.
    discuss_pos = html.find("Проработка замечаний")
    btn_pos = html.find("currentView === 'critic-v2-project'")
    assert 0 < discuss_pos < btn_pos


def test_frontend_view_block_has_no_file_input():
    """
    The project-scoped view block must not contain a file <input>:
    data comes from the backend endpoint, not user upload.
    """
    html = INDEX_HTML.read_text(encoding="utf-8")
    # Locate the explicit view-block marker (the <div v-if=...>),
    # not the navigation button which uses the same currentView token.
    marker = '<div v-if="currentView === \'critic-v2-project\'"'
    start = html.find(marker)
    assert start >= 0, "project-scoped view block not found"
    end = html.find("</div><!-- /main-area -->", start)
    assert end > start
    block = html[start:end]
    assert '<input type="file"' not in block


def test_frontend_route_and_loader_wired():
    js = APP_JS.read_text(encoding="utf-8")
    assert "cv2LoadProject" in js
    assert "/api/critic-v2/projects/" in js
    assert "/critic-v2$" in js  # route regex
    assert "currentView.value = 'critic-v2-project'" in js


def test_frontend_feedback_export_includes_scope():
    js = APP_JS.read_text(encoding="utf-8")
    # feedback build payload must include a 'scope' field.
    # find the cv2BuildFeedbackExport function body window.
    start = js.find("function cv2BuildFeedbackExport")
    assert start >= 0
    # take ~2500 chars window
    body = js[start:start + 2500]
    assert "scope" in body
    assert "project_id" in body


# ──────────────────────────────────────────────────────────────────────────────
# "Critic v2: Расхождения" entry-point (per-project disagreements view).
#
# The reviewer must be able to open the project page, click a button right after
# "Проработка замечаний", and land on a project-scoped Critic v2 view that is
# already filtered to disagreements with the expert. Same endpoint, same view —
# only the default filter and the feedback-export scope change.
# ──────────────────────────────────────────────────────────────────────────────


def test_frontend_has_disagreements_button_after_discussions():
    """Spec §3: button 'Critic v2: Расхождения' immediately after 'Проработка замечаний'."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "Critic v2: Расхождения" in html
    discuss_pos = html.find("Проработка замечаний")
    btn_pos = html.find("Critic v2: Расхождения")
    # Plain "Critic v2" project-tab button uses the /critic-v2' route (no -disagreements suffix).
    plain_btn_pos = html.find("/critic-v2'")
    assert discuss_pos > 0 and btn_pos > discuss_pos, \
        "'Critic v2: Расхождения' must come AFTER 'Проработка замечаний'"
    # And BEFORE the plain 'Critic v2' tab — disagreements is the recommended entry.
    if plain_btn_pos > 0:
        assert btn_pos < plain_btn_pos, \
            "'Critic v2: Расхождения' should appear before the plain 'Critic v2' tab"


def test_frontend_disagreements_button_uses_dedicated_route():
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "/critic-v2-disagreements'" in html


def test_frontend_route_loads_project_in_disagreements_mode():
    """Spec §4: the dedicated route auto-loads project data and sets the filter."""
    js = APP_JS.read_text(encoding="utf-8")
    assert "/critic-v2-disagreements$" in js
    # Loader is called with disagreementsMode flag.
    assert "disagreementsMode: true" in js
    # cv2LoadProject accepts opts.
    assert "function cv2LoadProject" in js or "cv2LoadProject(projectId, opts)" in js
    # The state ref exists.
    assert "cv2ProjDisagreementsMode" in js


def test_frontend_disagreements_mode_preselects_filter():
    """When cv2LoadProject is called with disagreementsMode, alignment filter
    is pre-applied to __disagreement__."""
    js = APP_JS.read_text(encoding="utf-8")
    # Locate cv2LoadProject body
    start = js.find("async function cv2LoadProject")
    assert start >= 0
    end = js.find("\n        function ", start + 10)
    body = js[start:end]
    # The body must set cv2Filter.value.alignment = '__disagreement__' under
    # the disagreementsMode branch.
    assert "cv2Filter.value.alignment = '__disagreement__'" in body
    # And the function must reset filters before applying the new one so the
    # two views don't bleed into each other.
    assert "cv2ResetFilters()" in body


def test_frontend_feedback_export_marks_disagreements_scope():
    """Spec §8: feedback export from the disagreements view must carry
    scope.mode = 'project_disagreements' + alignment_filter."""
    js = APP_JS.read_text(encoding="utf-8")
    start = js.find("function cv2BuildFeedbackExport")
    assert start >= 0
    end = js.find("function cv2ExportFeedback", start)
    body = js[start:end]
    assert "'project_disagreements'" in body
    assert "alignment_filter" in body
    assert "'__disagreement__'" in body


def test_frontend_project_view_has_mode_toggles():
    """Spec §6: explicit 'Показать все замечания проекта' and 'Только расхождения' buttons."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "Показать все замечания проекта" in html
    assert "Только расхождения" in html
    assert "cv2-project-mode-toggle" in html


def test_frontend_project_view_has_empty_disagreements_state():
    """Spec §9: empty-state message when disagreements mode + 0 disagreements."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "Расхождений с экспертом не найдено" in html


def test_frontend_disagreements_banner_text():
    """The disagreements banner must clearly state the experimental, LLM-free contract."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    assert "Critic v2 — Расхождения с экспертом" in html
    assert "Legacy critic остаётся основным" in html
    assert "LLM не запускается" in html


def test_frontend_project_view_has_no_file_input_even_in_disagreements_mode():
    """The project-scoped view is loaded from backend; no file input anywhere."""
    html = INDEX_HTML.read_text(encoding="utf-8")
    marker = '<div v-if="currentView === \'critic-v2-project\'"'
    start = html.find(marker)
    assert start >= 0
    end = html.find("</div><!-- /main-area -->", start)
    block = html[start:end]
    assert '<input type="file"' not in block


def test_production_pipeline_files_not_touched_by_disagreements_view():
    """The new disagreements entry-point must not have leaked into production files."""
    PROD_FILES = [
        "backend/app/pipeline/manager.py",
        "backend/app/pipeline/stages/findings_review/runner.py",
    ]
    for rel in PROD_FILES:
        text = (Path(_PROJECT_ROOT) / rel).read_text(encoding="utf-8")
        for token in ("critic-v2-disagreements", "cv2ProjDisagreementsMode",
                      "project_disagreements"):
            assert token not in text, (
                f"{rel} unexpectedly references {token!r}"
            )
