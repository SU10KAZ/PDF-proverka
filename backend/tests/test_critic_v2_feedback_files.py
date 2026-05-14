"""
Tests for /api/critic-v2/feedback-files endpoints.

Контекст: эти endpoints позволяют SPA подгружать ранее сохранённые
*_feedback.json (preferred_tab экспертa) после reload браузера, чтобы
findings, которые expert вручную перенёс в "suggested_reject", появлялись
во вкладке. До этого badge показывал только то, что отнёс туда сам critic.

Endpoints read-only: list + read одного файла.
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


@pytest.fixture
def feedback_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "feedback"
    d.mkdir()
    monkeypatch.setenv("CRITIC_V2_FEEDBACK_DIR", str(d))
    return d


@pytest.fixture
def client(feedback_dir: Path) -> TestClient:
    # CRITIC_V2_FEEDBACK_DIR resolved per-request, so importing app after
    # setting the env is not strictly required, but we do it for clarity.
    from backend.app.main import app
    return TestClient(app)


def _write_feedback(d: Path, name: str, payload: dict) -> Path:
    p = d / name
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return p


# ─── List endpoint ──────────────────────────────────────────────────────────


class TestListFeedbackFiles:
    def test_empty_dir_returns_empty_list(self, client: TestClient, feedback_dir: Path):
        resp = client.get("/api/critic-v2/feedback-files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["files"] == []
        assert data["exists"] is True

    def test_lists_feedback_json_files(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "PROJ_A_feedback.json", {"feedback": []})
        _write_feedback(feedback_dir, "critic_v2_triage_feedback_2026.json", {"feedback": []})
        resp = client.get("/api/critic-v2/feedback-files")
        assert resp.status_code == 200
        names = sorted(f["name"] for f in resp.json()["files"])
        assert names == [
            "PROJ_A_feedback.json",
            "critic_v2_triage_feedback_2026.json",
        ]

    def test_skips_non_json_files(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "ok_feedback.json", {"feedback": []})
        (feedback_dir / "notes.txt").write_text("ignore me")
        (feedback_dir / "summary.csv").write_text("a,b,c")
        resp = client.get("/api/critic-v2/feedback-files")
        names = [f["name"] for f in resp.json()["files"]]
        assert names == ["ok_feedback.json"]

    def test_skips_unrelated_json(self, client: TestClient, feedback_dir: Path):
        # "feedback" must be in the filename — guards against serving e.g. dump.json.
        (feedback_dir / "dump.json").write_text("{}")
        _write_feedback(feedback_dir, "x_feedback.json", {"feedback": []})
        resp = client.get("/api/critic-v2/feedback-files")
        names = [f["name"] for f in resp.json()["files"]]
        assert names == ["x_feedback.json"]

    def test_files_carry_size_and_mtime(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "x_feedback.json", {"feedback": [{"finding_id": "X:1"}]})
        resp = client.get("/api/critic-v2/feedback-files")
        f = resp.json()["files"][0]
        assert f["size"] > 0
        assert isinstance(f["mtime"], int)

    def test_files_carry_scope_project_name(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "x_feedback.json", {
            "scope": {"project_name": "13АВ-РД-ОЗДС.pdf"},
            "feedback": [
                {"finding_id": "X:1", "preferred_tab": "suggested_reject"},
                {"finding_id": "X:2", "preferred_tab": "suggested_reject"},
                {"finding_id": "X:3", "preferred_tab": "hidden_by_critic"},
            ],
        })
        resp = client.get("/api/critic-v2/feedback-files")
        f = resp.json()["files"][0]
        assert f["scope_project_name"] == "13АВ-РД-ОЗДС.pdf"
        assert f["entries"] == 3
        assert f["suggested_reject_count"] == 2


class TestMatchByProjectId:
    """When ?project_id=... is provided, the listing returns `matches[]`
    sorted by match quality (exact > exact_no_pdf > normalized > substring)."""

    def test_no_query_returns_no_matches_key(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "x_feedback.json", {"feedback": []})
        resp = client.get("/api/critic-v2/feedback-files")
        assert "matches" not in resp.json()

    def test_exact_match(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "a_feedback.json", {
            "scope": {"project_name": "13АВ-РД-ОЗДС.pdf"},
            "feedback": [{"finding_id": "X:1"}],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "13АВ-РД-ОЗДС.pdf"},
        )
        m = resp.json()["matches"]
        assert len(m) == 1
        assert m[0]["name"] == "a_feedback.json"
        assert m[0]["match_quality"] == "exact"

    def test_exact_no_pdf_match(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "a_feedback.json", {
            "scope": {"project_name": "13АВ-РД-ОЗДС"},
            "feedback": [],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "13АВ-РД-ОЗДС.pdf"},
        )
        m = resp.json()["matches"]
        assert len(m) == 1
        assert m[0]["match_quality"] == "exact_no_pdf"

    def test_normalized_match(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "a_feedback.json", {
            "scope": {"project_name": "  13АВ-РД-ОЗДС.pdf  "},
            "feedback": [],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "13АВ-РД-ОЗДС.PDF"},
        )
        m = resp.json()["matches"]
        assert len(m) == 1
        assert m[0]["match_quality"] in ("exact_no_pdf", "normalized")

    def test_no_match_returns_empty_list(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "a_feedback.json", {
            "scope": {"project_name": "completely-different-project"},
            "feedback": [],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "13АВ-РД-ОЗДС"},
        )
        assert resp.json()["matches"] == []

    def test_ranking_exact_first(self, client: TestClient, feedback_dir: Path):
        # exact match должен ранжироваться выше substring.
        _write_feedback(feedback_dir, "exact_feedback.json", {
            "scope": {"project_name": "13АВ-РД-ОЗДС"},
            "feedback": [],
        })
        _write_feedback(feedback_dir, "substring_feedback.json", {
            "scope": {"project_name": "13АВ-РД-ОЗДС-EXTRA-CONTEXT"},
            "feedback": [],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "13АВ-РД-ОЗДС"},
        )
        m = resp.json()["matches"]
        assert m[0]["name"] == "exact_feedback.json"
        assert m[0]["match_quality"] == "exact"

    def test_match_carries_counts(self, client: TestClient, feedback_dir: Path):
        _write_feedback(feedback_dir, "a_feedback.json", {
            "scope": {"project_name": "P1"},
            "feedback": [
                {"finding_id": "P1:F-1", "preferred_tab": "suggested_reject"},
                {"finding_id": "P1:F-2", "preferred_tab": "suggested_reject"},
                {"finding_id": "P1:F-3", "preferred_tab": "hidden_by_critic"},
            ],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "P1"},
        )
        m = resp.json()["matches"][0]
        assert m["entries"] == 3
        assert m["suggested_reject_count"] == 2

    def test_broken_file_does_not_crash_listing(self, client: TestClient, feedback_dir: Path):
        (feedback_dir / "broken_feedback.json").write_text("{not json", encoding="utf-8")
        _write_feedback(feedback_dir, "good_feedback.json", {
            "scope": {"project_name": "P1"},
            "feedback": [{"finding_id": "P1:F-1"}],
        })
        resp = client.get(
            "/api/critic-v2/feedback-files",
            params={"project_id": "P1"},
        )
        # Listing should still succeed (good file is returned, broken file just
        # has no scope metadata and won't match).
        assert resp.status_code == 200
        m = resp.json()["matches"]
        assert any(x["name"] == "good_feedback.json" for x in m)


# ─── Read endpoint ──────────────────────────────────────────────────────────


class TestReadFeedbackFile:
    def test_returns_parsed_json(self, client: TestClient, feedback_dir: Path):
        payload = {
            "export_type": "critic_v2_triage_feedback",
            "feedback": [
                {
                    "finding_id": "PROJ:F-001",
                    "preferred_tab": "suggested_reject",
                    "triage_correct": "no",
                }
            ],
        }
        _write_feedback(feedback_dir, "x_feedback.json", payload)
        resp = client.get("/api/critic-v2/feedback-files/x_feedback.json")
        assert resp.status_code == 200
        assert resp.json() == payload

    def test_404_when_missing(self, client: TestClient, feedback_dir: Path):
        resp = client.get("/api/critic-v2/feedback-files/missing_feedback.json")
        assert resp.status_code == 404

    def test_400_on_path_traversal(self, client: TestClient, feedback_dir: Path):
        # Even if FastAPI normalizes some paths, the validator must reject ..
        for bad in ["../etc/passwd.json", "sub/x_feedback.json"]:
            resp = client.get(f"/api/critic-v2/feedback-files/{bad}")
            assert resp.status_code in (400, 404), f"Failed for {bad!r}: {resp.status_code}"

    def test_400_on_non_json_extension(self, client: TestClient, feedback_dir: Path):
        (feedback_dir / "notes.txt").write_text("ignore me")
        resp = client.get("/api/critic-v2/feedback-files/notes.txt")
        assert resp.status_code == 400
