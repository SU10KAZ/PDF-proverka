# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 Controlled Enforce Dry-run read-only endpoint.

GET /api/stage-comparison/pipeline-v2/{session_id}/controlled-enforce-dry-run
"""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_controlled_enforce_dry_run,
)

_KIND = "stage_comparison_pipeline_v2_controlled_enforce_dry_run"

_SAMPLE = {
    "version": 1, "kind": _KIND, "status": "ok",
    "session_id": "s1", "pair_id": "p1",
    "summary": {
        "eligible_items": 2, "logical_transitions": 1, "would_skip_block_pairs": 2,
        "would_exclude_from_enrichment": 2, "would_apply": False, "enforce_enabled": False,
    },
    "logical_transitions": [
        {"transition_id": "ВРУ-3→ВРУ-2", "item_count": 2,
         "operator_decision": "approve_exclude",
         "link_validation_decisions": ["reject_mapping"], "confidence": 0.99}],
    "would_skip_items": [
        {"item_id": "xp_bp::A__B", "left_block_id": "A", "right_block_id": "B",
         "would_apply": True, "enforce_allowed": True, "runtime_write_allowed": True,
         "raw_response": "SECRET", "trace": "DBG"},
        {"item_id": "xp_bp::C__D", "left_block_id": "C", "right_block_id": "D",
         "would_apply": False, "enforce_allowed": False, "runtime_write_allowed": False}],
    "protected_artifacts": {"will_modify": [], "must_remain_unchanged": ["entity_diff_report.json"]},
    "would_apply": False, "enforce_enabled": False,
}


def _art(tmp, sid, pid):
    d = tmp / "sessions" / sid / "pairs" / pid / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _patch_root(tmp):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp / "sessions")


class TestDiscoverDryRunEndpoint:
    def test_ready_report_returns_ok(self, tmp_path):
        """(9) endpoint ready report returns ok."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_dry_run_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1")
        assert r["status"] == "ok"
        assert r["available"] is True
        assert r["summary"]["logical_transitions"] == 1
        assert len(r["logical_transitions"]) == 1
        assert len(r["would_skip_items"]) == 2

    def test_missing_returns_not_found(self, tmp_path):
        """(10) endpoint missing returns not_found."""
        _art(tmp_path, "s1", "p1")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1")
        assert r["status"] == "not_found"
        assert r["available"] is False

    def test_broken_returns_error_not_500(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_dry_run_report.json").write_text(
            "{{bad", encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1")
        assert r["status"] == "error"

    def test_invalid_kind_error(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_dry_run_report.json").write_text(
            json.dumps(dict(_SAMPLE, kind="wrong")), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1")
        assert r["status"] == "error"

    def test_observe_only_flags_and_no_raw_leak(self, tmp_path):
        """(11-ish) observe-only flags forced + no raw/debug leak."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_dry_run_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1")
        assert r["would_apply"] is False
        assert r["enforce_enabled"] is False
        for w in r["would_skip_items"]:
            assert w["would_apply"] is False
            assert w["enforce_allowed"] is False
            assert w["runtime_write_allowed"] is False
            assert "raw_response" not in w
            assert "trace" not in w
        blob = json.dumps(r)
        assert "SECRET" not in blob and "DBG" not in blob

    def test_pagination_and_clamp(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        many = dict(_SAMPLE, would_skip_items=[
            {"item_id": f"x{i}"} for i in range(10)])
        (d / "controlled_enforce_dry_run_report.json").write_text(
            json.dumps(many), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_dry_run("s1", "p1", limit=3, offset=0)
        assert len(r["would_skip_items"]) == 3
        assert r["total_count"] == 10
        with _patch_root(tmp_path):
            r2 = discover_controlled_enforce_dry_run("s1", "p1", limit=99999)
        assert r2["limit"] == 500

    def test_read_only_no_new_files(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_dry_run_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(tmp_path.rglob("*")) if p.is_file()}
        before = snap()
        with _patch_root(tmp_path):
            discover_controlled_enforce_dry_run("s1", "p1")
        assert snap() == before

    def test_invalid_pair_id_raises(self, tmp_path):
        with _patch_root(tmp_path):
            with pytest.raises(ValueError):
                discover_controlled_enforce_dry_run("s1", "../evil")
