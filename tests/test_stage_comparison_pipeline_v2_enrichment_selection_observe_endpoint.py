# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 enrichment-selection-observe read-only endpoint.

GET /api/stage-comparison/pipeline-v2/{session_id}/enrichment-selection-observe
"""
import json
from unittest.mock import patch

import pytest

from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_enrichment_selection_observe,
)

_KIND = "stage_comparison_pipeline_v2_enrichment_selection_observe"

_REPORT = {
    "version": 1, "kind": _KIND, "status": "ok",
    "session_id": "s1", "pair_id": "p1",
    "created_at": "2026-06-14T00:00:00Z",
    "controlled_enforce_run_id": "ce_run_X",
    "selection_source": "real_candidate_pool",
    "summary": {
        "default_candidates_total": 54, "state_on_candidates_total": 52,
        "excluded_by_state": 2, "excluded_logical_transitions": 1,
        "qwen_calls": 0, "runtime_modified": False, "protected_reports_modified": False,
    },
    "excluded_by_state": [
        {"left_block_id": "6XDP-JLWQ-KNX", "right_block_id": "3T6X-4PHG-D96",
         "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
         "transition_id": "ВРУ-3→ВРУ-2", "controlled_enforce_run_id": "ce_run_X",
         "reason": "controlled_enforce_state_active",
         "scope": {"exclude_from_enrichment": True},
         "raw": "SECRET", "_debug": "DBG"}],
    "remaining_candidates_sample": [
        {"left_block_id": "E", "right_block_id": "F", "candidate_kind": "table",
         "raw": "X"}],
    "invariants": {"qwen_not_called": True, "state_not_modified": True},
    "warnings": [],
}


def _art(tmp, sid, pid):
    d = tmp / "sessions" / sid / "pairs" / pid / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _patch_root(tmp):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp / "sessions")


_F = "controlled_enforce_enrichment_selection_observe_report.json"


class TestDiscoverEnrichmentSelectionObserve:
    def test_ready_returns_ok(self, tmp_path):
        """(6) endpoint ready returns ok + summary."""
        d = _art(tmp_path, "s1", "p1")
        (d / _F).write_text(json.dumps(_REPORT), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_enrichment_selection_observe("s1", "p1")
        assert r["status"] == "ok" and r["available"] is True
        assert r["summary"]["default_candidates_total"] == 54
        assert r["summary"]["state_on_candidates_total"] == 52
        assert r["summary"]["excluded_by_state"] == 2
        assert r["summary"]["excluded_logical_transitions"] == 1
        assert len(r["excluded_by_state"]) == 1
        assert len(r["remaining_candidates_sample"]) == 1

    def test_missing_returns_not_found(self, tmp_path):
        """(7) missing report returns not_found."""
        _art(tmp_path, "s1", "p1")
        with _patch_root(tmp_path):
            r = discover_enrichment_selection_observe("s1", "p1")
        assert r["status"] == "not_found"
        assert r["available"] is False

    def test_broken_returns_error_not_500(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / _F).write_text("{{bad", encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_enrichment_selection_observe("s1", "p1")
        assert r["status"] == "error"

    def test_invalid_kind_error(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / _F).write_text(json.dumps(dict(_REPORT, kind="wrong")), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_enrichment_selection_observe("s1", "p1")
        assert r["status"] == "error"

    def test_observe_invariants_forced_and_no_raw_leak(self, tmp_path):
        d = _art(tmp_path, "s1", "p1")
        (d / _F).write_text(json.dumps(_REPORT), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_enrichment_selection_observe("s1", "p1")
        assert r["summary"]["qwen_calls"] == 0
        assert r["summary"]["runtime_modified"] is False
        assert r["summary"]["protected_reports_modified"] is False
        blob = json.dumps(r, ensure_ascii=False)
        assert "SECRET" not in blob and "DBG" not in blob
        for e in r["excluded_by_state"]:
            assert "raw" not in e and "_debug" not in e

    def test_read_only_no_new_files(self, tmp_path):
        """(8) endpoint read-only, no writes."""
        d = _art(tmp_path, "s1", "p1")
        (d / _F).write_text(json.dumps(_REPORT), encoding="utf-8")

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(tmp_path.rglob("*")) if p.is_file()}
        before = snap()
        with _patch_root(tmp_path):
            discover_enrichment_selection_observe("s1", "p1")
        assert snap() == before

    def test_invalid_pair_id_raises(self, tmp_path):
        with _patch_root(tmp_path):
            with pytest.raises(ValueError):
                discover_enrichment_selection_observe("s1", "../evil")
