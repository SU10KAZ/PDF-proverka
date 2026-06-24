# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 Controlled Enforce STATE read-only endpoint.

GET /api/stage-comparison/pipeline-v2/{session_id}/controlled-enforce-state
"""
import json
from unittest.mock import patch

import pytest

from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_controlled_enforce_state,
)

_KIND = "stage_comparison_pipeline_v2_controlled_enforce_state"

_STATE = {
    "version": 1, "kind": _KIND, "status": "active",
    "session_id": "s1", "pair_id": "p1",
    "run_id": "ce_run_X", "rollback_id": "ce_rb_X",
    "mode": "enforce_one_logical_transition",
    "applied_exclusions": [
        {"run_id": "ce_run_X", "transition_id": "ВРУ-3→ВРУ-2",
         "item_ids": ["xp_bp::A__B", "xp_bp::C__D"],
         "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
         "left_block_ids": ["A", "C"], "right_block_ids": ["B", "D"],
         "operator_decision_id": "xrd_x", "rollback_id": "ce_rb_X",
         "scope": {"exclude_from_enrichment": True,
                   "exclude_from_grounded_evidence": False,
                   "exclude_from_delta_explanation": False,
                   "exclude_from_findings": False},
         "active": True, "raw": "SECRET", "_debug": "DBG"}],
}


def _art(tmp, sid, pid):
    d = tmp / "sessions" / sid / "pairs" / pid / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _patch_root(tmp):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp / "sessions")


class TestDiscoverStateEndpoint:
    def test_ready_state_returns_ok(self, tmp_path):
        """(1) controlled-enforce-state ready report returns ok + summary."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text(json.dumps(_STATE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "ok"
        assert r["available"] is True
        assert r["kind"] == _KIND
        assert r["summary"]["active_exclusions"] == 1
        assert r["summary"]["active_transitions"] == 1
        assert r["summary"]["active_block_pairs"] == 2
        assert r["summary"]["scope_enrichment_only"] is True
        assert r["run_id"] == "ce_run_X"
        assert r["rollback_id"] == "ce_rb_X"
        assert len(r["applied_exclusions"]) == 1

    def test_missing_returns_not_found(self, tmp_path):
        """(2) missing state → not_found (NOTHING is built)."""
        _art(tmp_path, "s1", "p1")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "not_found"
        assert r["available"] is False
        assert r["summary"]["active_exclusions"] == 0

    def test_broken_returns_error_not_500(self, tmp_path):
        """(3) broken state → error, not raise/500."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text("{{bad", encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "error"
        assert r["available"] is False

    def test_invalid_kind_error(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text(
            json.dumps(dict(_STATE, kind="wrong")), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "error"

    def test_no_raw_debug_leak(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text(json.dumps(_STATE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        blob = json.dumps(r, ensure_ascii=False)
        assert "SECRET" not in blob and "DBG" not in blob
        for ex in r["applied_exclusions"]:
            assert "raw" not in ex and "_debug" not in ex

    def test_inactive_exclusions_not_counted(self, tmp_path):
        """active=False записи не считаются в summary."""
        d = _art(tmp_path, "s1", "p1")
        st = json.loads(json.dumps(_STATE))
        st["applied_exclusions"][0]["active"] = False
        (d / "controlled_enforce_state.json").write_text(json.dumps(st), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "ok"
        assert r["summary"]["active_exclusions"] == 0
        assert r["summary"]["active_block_pairs"] == 0

    def test_read_only_no_new_files(self, tmp_path):
        """(4) endpoint read-only, no writes."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text(json.dumps(_STATE), encoding="utf-8")

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(tmp_path.rglob("*")) if p.is_file()}
        before = snap()
        with _patch_root(tmp_path):
            discover_controlled_enforce_state("s1", "p1")
        assert snap() == before

    def test_invalid_pair_id_raises(self, tmp_path):
        with _patch_root(tmp_path):
            with pytest.raises(ValueError):
                discover_controlled_enforce_state("s1", "../evil")
