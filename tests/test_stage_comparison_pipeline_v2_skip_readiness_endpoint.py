# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 Skip Readiness read-only endpoint.

GET /api/stage-comparison/pipeline-v2/{session_id}/skip-readiness
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# ─── helpers ─────────────────────────────────────────────────────────────────

_SAMPLE_REPORT = {
    "version": "1",
    "kind": "skip_readiness_report_v1",
    "status": "ok",
    "session_id": "sess1",
    "pair_id": "pair1",
    "auto_enforce_enabled": False,
    "enforce_allowed": False,
    "summary": {
        "items_total": 3,
        "ready_to_skip": 1,
        "blocked": 1,
        "needs_review": 0,
        "keep": 1,
        "operator_approved": 1,
        "operator_rejected": 0,
        "missing_operator_decision": 1,
        "auto_enforce_enabled": False,
    },
    "items": [
        {
            "item_id": "xp_bp::A__B",
            "left_block_id": "A",
            "right_block_id": "B",
            "classification": "candidate_exclude",
            "readiness_status": "ready_to_skip",
            "confidence": 0.95,
            "skip_scope": {
                "exclude_from_enrichment": True,
                "exclude_from_grounded_evidence": False,
                "exclude_from_delta_explanation": False,
                "exclude_from_findings": False,
            },
            "auto_apply": False,
            "enforce_allowed": False,
            "requires_explicit_operator_approval": True,
        },
        {
            "item_id": "xp_bp::C__D",
            "left_block_id": "C",
            "right_block_id": "D",
            "classification": "candidate_exclude",
            "readiness_status": "blocked",
            "blocked_reason": "missing_operator_approval",
            "confidence": 0.80,
            "skip_scope": {
                "exclude_from_enrichment": True,
                "exclude_from_grounded_evidence": False,
                "exclude_from_delta_explanation": False,
                "exclude_from_findings": False,
            },
            "auto_apply": False,
            "enforce_allowed": False,
            "requires_explicit_operator_approval": True,
        },
        {
            "item_id": "xp_bp::E__F",
            "left_block_id": "E",
            "right_block_id": "F",
            "classification": "keep",
            "readiness_status": "keep",
            "confidence": 0.99,
            "auto_apply": False,
            "enforce_allowed": False,
            "requires_explicit_operator_approval": True,
        },
    ],
    "warnings": [],
}


def _make_art_dir(tmp: Path, session_id: str, pair_id: str) -> Path:
    d = tmp / "sessions" / session_id / "pairs" / pair_id / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ─── unit tests for discover_skip_readiness ──────────────────────────────────

from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_skip_readiness,
    pipeline_v2_artifacts_dir,
)
from backend.app.services.stage_comparison.paths import sessions_root_path


def _patch_session_root(tmp_root: Path):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp_root / "sessions",
    )


class TestDiscoverSkipReadiness:
    def test_not_found_when_no_report(self, tmp_path):
        _make_art_dir(tmp_path, "s1", "p1")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        assert r["status"] == "not_found"
        assert r["available"] is False
        assert r["items"] == []

    def test_ok_returns_summary_and_items(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        assert r["status"] == "ok"
        assert r["available"] is True
        assert r["total_count"] == 3
        assert r["summary"]["ready_to_skip"] == 1
        assert r["summary"]["blocked"] == 1
        assert r["summary"]["keep"] == 1

    def test_filter_by_readiness(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1", readiness="ready_to_skip")
        assert r["filtered_count"] == 1
        assert all(it["readiness_status"] == "ready_to_skip" for it in r["items"])

    def test_filter_blocked(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1", readiness="blocked")
        assert r["filtered_count"] == 1
        assert r["items"][0]["readiness_status"] == "blocked"

    def test_invalid_readiness_filter_returns_error(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1", readiness="invalid_status")
        assert r["status"] == "error"

    def test_pagination(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        report = dict(_SAMPLE_REPORT)
        (d / "skip_readiness_report.json").write_text(
            json.dumps(report), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1", limit=2, offset=0)
        assert len(r["items"]) == 2
        assert r["total_count"] == 3

        with _patch_session_root(tmp_path):
            r2 = discover_skip_readiness("s1", "p1", limit=2, offset=2)
        assert len(r2["items"]) == 1

    def test_limit_clamped_at_500(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1", limit=99999)
        assert r["limit"] == 500

    def test_mark_only_invariants_forced(self, tmp_path):
        """auto_apply/enforce_allowed/requires_explicit_operator_approval forced False/True."""
        d = _make_art_dir(tmp_path, "s1", "p1")
        report = dict(_SAMPLE_REPORT)
        # poison the item with wrong values
        bad_items = [dict(it) for it in report["items"]]
        for it in bad_items:
            it["auto_apply"] = True
            it["enforce_allowed"] = True
            it["requires_explicit_operator_approval"] = False
        report = dict(report, items=bad_items)
        (d / "skip_readiness_report.json").write_text(
            json.dumps(report), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        for it in r["items"]:
            assert it["auto_apply"] is False
            assert it["enforce_allowed"] is False
            assert it["requires_explicit_operator_approval"] is True

    def test_auto_enforce_enabled_false_in_response(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(_SAMPLE_REPORT), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        assert r["auto_enforce_enabled"] is False
        assert r["enforce_allowed"] is False

    def test_error_on_invalid_kind(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        bad = dict(_SAMPLE_REPORT, kind="wrong_kind")
        (d / "skip_readiness_report.json").write_text(
            json.dumps(bad), encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        assert r["status"] == "error"

    def test_invalid_pair_id_raises_valueerror(self, tmp_path):
        with _patch_session_root(tmp_path):
            with pytest.raises(ValueError):
                discover_skip_readiness("s1", "../evil")

    def test_error_on_corrupt_json(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "skip_readiness_report.json").write_text("{{not json", encoding="utf-8")
        with _patch_session_root(tmp_path):
            r = discover_skip_readiness("s1", "p1")
        assert r["status"] == "error"
