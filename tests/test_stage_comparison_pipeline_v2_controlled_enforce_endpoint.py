# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 Controlled Enforce Preflight read-only endpoint.

GET /api/stage-comparison/pipeline-v2/{session_id}/controlled-enforce-preflight

Observe-only: no enforce, no writes, no jobs, no models.
"""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_controlled_enforce_preflight,
)

_KIND = "stage_comparison_pipeline_v2_controlled_enforce_preflight"

_SAMPLE = {
    "version": 1,
    "kind": _KIND,
    "status": "blocked",
    "session_id": "s1",
    "pair_id": "p1",
    "summary": {
        "ready_to_skip_items": 0,
        "eligible_items": 0,
        "blocked_items": 2,
        "fatal_blocks": 1,
        "warnings": 0,
        "would_apply": False,
        "enforce_enabled": False,
    },
    "global_guards": {
        "active_runtime_root_confirmed": True,
        "ready_to_skip_present": False,
        "protected_hashes_available": True,
        "dry_run_only": True,
    },
    "runtime_root": {"active": "/x/comparison", "confirmed": True, "source": "/api/info"},
    "fatal_blocks": ["ready_to_skip_zero"],
    "eligible_items": [],
    "blocked_items": [
        {"item_id": "a", "reason": "missing_operator_approval",
         "source_readiness": "blocked", "operator_decision": None,
         "raw_response": "SECRET", "trace": "DEBUG"},
        {"item_id": "b", "reason": "needs_review",
         "source_readiness": "needs_review", "operator_decision": None},
    ],
    "would_write": [],
    "would_skip": [],
    "auto_apply": False,
    "enforce_allowed": False,
}


def _make_art_dir(tmp: Path, sid: str, pid: str) -> Path:
    d = tmp / "sessions" / sid / "pairs" / pid / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _patch_root(tmp: Path):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp / "sessions",
    )


class TestDiscoverControlledEnforcePreflight:
    def test_ready_report_returns_ok(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert r["status"] == "ok"
        assert r["available"] is True
        assert r["report_status"] == "blocked"
        assert r["summary"]["ready_to_skip_items"] == 0
        assert r["fatal_blocks"] == ["ready_to_skip_zero"]

    def test_missing_report_not_found(self, tmp_path):
        _make_art_dir(tmp_path, "s1", "p1")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert r["status"] == "not_found"
        assert r["available"] is False
        assert "not found" in r["message"].lower()

    def test_broken_report_returns_error_not_500(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            "{{not json", encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert r["status"] == "error"
        assert r["available"] is False

    def test_invalid_kind_returns_error(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(dict(_SAMPLE, kind="wrong")), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert r["status"] == "error"

    def test_status_filter_match(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1", status="blocked")
        assert r["status"] == "ok"
        assert r.get("filtered_out") is not True
        assert len(r["blocked_items"]) == 2

    def test_status_filter_no_match_returns_empty_ok(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1", status="preflight_ok")
        assert r["status"] == "ok"
        assert r["filtered_out"] is True
        assert r["blocked_items"] == []
        assert r["report_status"] == "blocked"

    def test_invalid_status_filter_returns_error(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1", status="bogus")
        assert r["status"] == "error"

    def test_pagination(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        many = dict(_SAMPLE, blocked_items=[
            {"item_id": f"x{i}", "reason": "needs_review"} for i in range(10)])
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(many), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1", limit=3, offset=0)
        assert len(r["blocked_items"]) == 3
        assert r["total_count"] == 10
        with _patch_root(tmp_path):
            r2 = discover_controlled_enforce_preflight("s1", "p1", limit=3, offset=9)
        assert len(r2["blocked_items"]) == 1

    def test_limit_clamped_at_500(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1", limit=99999)
        assert r["limit"] == 500

    def test_no_raw_debug_leak(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        for it in r["blocked_items"]:
            assert "raw_response" not in it
            assert "trace" not in it
        blob = json.dumps(r)
        assert "SECRET" not in blob
        assert "DEBUG" not in blob

    def test_observe_only_flags_forced(self, tmp_path):
        """auto_apply/enforce_allowed/would_apply/enforce_enabled forced false."""
        d = _make_art_dir(tmp_path, "s1", "p1")
        # poison report-level + item-level flags
        poisoned = dict(_SAMPLE, would_apply=True, enforce_enabled=True,
                        auto_apply=True, enforce_allowed=True)
        poisoned["blocked_items"] = [
            {"item_id": "a", "reason": "x", "auto_apply": True,
             "enforce_allowed": True, "would_apply": True}]
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(poisoned), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert r["would_apply"] is False
        assert r["enforce_enabled"] is False
        assert r["auto_apply"] is False
        assert r["enforce_allowed"] is False
        for it in r["blocked_items"]:
            assert it["auto_apply"] is False
            assert it["enforce_allowed"] is False
            assert it["would_apply"] is False

    def test_read_only_no_new_files(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(_SAMPLE), encoding="utf-8")

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted((tmp_path).rglob("*")) if p.is_file()}
        before = snap()
        with _patch_root(tmp_path):
            discover_controlled_enforce_preflight("s1", "p1")
        assert snap() == before

    def test_invalid_pair_id_raises_valueerror(self, tmp_path):
        with _patch_root(tmp_path):
            with pytest.raises(ValueError):
                discover_controlled_enforce_preflight("s1", "../evil")

    def test_eligible_items_returned_and_scrubbed(self, tmp_path):
        d = _make_art_dir(tmp_path, "s1", "p1")
        rep = dict(_SAMPLE, status="preflight_ok")
        rep["eligible_items"] = [
            {"item_id": "e1", "would_skip": True, "applied": False,
             "auto_apply": True, "raw_prompt": "LEAK"}]
        (d / "controlled_enforce_preflight_report.json").write_text(
            json.dumps(rep), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_preflight("s1", "p1")
        assert len(r["eligible_items"]) == 1
        e = r["eligible_items"][0]
        assert e["auto_apply"] is False
        assert e["would_apply"] is False
        assert "raw_prompt" not in e
