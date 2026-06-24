# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce DRY-RUN / impact report (mark-only).

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_dry_run.py
"""
import ast
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_dry_run import (
    build_controlled_enforce_dry_run,
    build_controlled_enforce_dry_run_from_dir,
    write_controlled_enforce_dry_run_report,
    DRY_RUN_KIND,
    STATUS_OK,
    STATUS_BLOCKED,
    STATUS_NO_ELIGIBLE,
)

PREFLIGHT_KIND = "stage_comparison_pipeline_v2_controlled_enforce_preflight"
_SCOPE = {
    "exclude_from_enrichment": True,
    "exclude_from_grounded_evidence": False,
    "exclude_from_delta_explanation": False,
    "exclude_from_findings": False,
}


def _preflight(status, eligible_ids):
    return {
        "version": 1, "kind": PREFLIGHT_KIND, "status": status,
        "session_id": "s1", "pair_id": "p1",
        "summary": {"ready_to_skip_items": len(eligible_ids),
                    "eligible_items": len(eligible_ids), "blocked_items": 0,
                    "fatal_blocks": 0, "would_apply": False, "enforce_enabled": False},
        "fatal_blocks": [],
        "eligible_items": [{"item_id": i, "source_readiness": "ready_to_skip",
                            "operator_decision": "approve_exclude",
                            "classification": "candidate_exclude",
                            "would_skip": True, "applied": False} for i in eligible_ids],
        "blocked_items": [],
        "runtime_root": {"active": "/x/comparison", "confirmed": True, "source": "/api/info"},
        "would_apply": False, "enforce_enabled": False,
    }


def _sr(items):
    return {"version": "1", "kind": "skip_readiness_report_v1", "status": "ok",
            "items": items}


def _sr_item(iid, ll, rl, lb, rb):
    return {"item_id": iid, "left_entity_label": ll, "right_entity_label": rl,
            "left_block_id": lb, "right_block_id": rb,
            "readiness_status": "ready_to_skip", "operator_decision": "approve_exclude",
            "skip_scope": dict(_SCOPE)}


def _xp(items):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_exclusion_preview",
            "status": "ok", "items": items}


def _xp_item(iid, conf=0.99, lv="reject_mapping", signals=None):
    return {"item_id": iid, "confidence": conf,
            "link_validation": {"decision": lv, "entity_relation": "different_entity"},
            "source_signals": signals or ["link_validation:reject_mapping"]}


# two block-pairs of the SAME transition ВРУ-3 → ВРУ-2
_IDS = ["xp_bp::A__B", "xp_bp::C__D"]
_SR2 = _sr([_sr_item("xp_bp::A__B", "ВРУ-3", "ВРУ-2", "A", "B"),
            _sr_item("xp_bp::C__D", "ВРУ-3", "ВРУ-2", "C", "D")])
_XP2 = _xp([_xp_item("xp_bp::A__B"), _xp_item("xp_bp::C__D", conf=0.95)])


def _build(status="preflight_ok", ids=None):
    ids = _IDS if ids is None else ids
    return build_controlled_enforce_dry_run(
        session_id="s1", pair_id="p1",
        preflight_report=_preflight(status, ids),
        skip_readiness_report=_SR2, exclusion_preview_report=_XP2)


class TestDryRun:
    def test_preflight_blocked_gives_blocked(self):
        """(1) preflight not ok → dry_run status blocked."""
        pf = _preflight("blocked", [])
        pf["fatal_blocks"] = ["ready_to_skip_zero"]
        r = build_controlled_enforce_dry_run(
            session_id="s1", pair_id="p1", preflight_report=pf)
        assert r["status"] == STATUS_BLOCKED
        assert r["would_skip_items"] == []
        assert "ready_to_skip_zero" in r["blocked_reasons"]

    def test_preflight_ok_builds_would_skip(self):
        """(2) preflight ok + eligible → would_skip_items built."""
        r = _build()
        assert r["status"] == STATUS_OK
        assert r["kind"] == DRY_RUN_KIND
        assert len(r["would_skip_items"]) == 2
        assert {w["item_id"] for w in r["would_skip_items"]} == set(_IDS)

    def test_same_transition_one_logical(self):
        """(3) eligible 2 same transition → logical_transitions=1."""
        r = _build()
        assert r["summary"]["logical_transitions"] == 1
        assert r["summary"]["would_skip_block_pairs"] == 2
        t = r["logical_transitions"][0]
        assert t["transition_id"] == "ВРУ-3→ВРУ-2"
        assert t["item_count"] == 2
        assert t["operator_decision"] == "approve_exclude"
        assert "reject_mapping" in t["link_validation_decisions"]
        assert t["confidence"] == 0.99

    def test_would_apply_always_false(self):
        """(4) would_apply always false."""
        r = _build()
        assert r["summary"]["would_apply"] is False
        assert r["would_apply"] is False
        for w in r["would_skip_items"]:
            assert w["would_apply"] is False
            assert w["runtime_write_allowed"] is False
            assert w["enforce_allowed"] is False

    def test_enforce_enabled_always_false(self):
        """(5) enforce_enabled always false."""
        r = _build()
        assert r["summary"]["enforce_enabled"] is False
        assert r["enforce_enabled"] is False

    def test_protected_will_modify_empty(self):
        """(6) protected artifacts will_modify empty."""
        r = _build()
        assert r["protected_artifacts"]["will_modify"] == []
        must = r["protected_artifacts"]["must_remain_unchanged"]
        assert "entity_diff_report.json" in must
        assert "grounded_evidence_report.json" in must
        assert "delta_explanation_report.json" in must
        assert "block_link_preview_report.json" in must

    def test_skip_scope_only_enrichment(self):
        """(7) skip scope only enrichment."""
        r = _build()
        for w in r["would_skip_items"]:
            sc = w["skip_scope"]
            assert sc["exclude_from_enrichment"] is True
            assert sc["exclude_from_grounded_evidence"] is False
            assert sc["exclude_from_delta_explanation"] is False
            assert sc["exclude_from_findings"] is False
            assert w["dry_run_action"] == "would_exclude_from_enrichment"
        assert r["summary"]["would_exclude_from_enrichment"] == 2

    def test_missing_preflight_blocked(self):
        """(8) missing preflight → blocked."""
        r = build_controlled_enforce_dry_run(
            session_id="s1", pair_id="p1", preflight_report=None)
        assert r["status"] == STATUS_BLOCKED
        assert "preflight_missing" in r["blocked_reasons"]
        assert r["would_skip_items"] == []

    def test_no_eligible_items(self):
        """preflight ok-shaped but eligible empty → no_eligible_items."""
        r = build_controlled_enforce_dry_run(
            session_id="s1", pair_id="p1",
            preflight_report=_preflight("preflight_ok", []))
        assert r["status"] == STATUS_NO_ELIGIBLE
        assert r["would_skip_items"] == []

    def test_invalid_kind_preflight_blocked(self):
        bad = _preflight("preflight_ok", _IDS)
        bad["kind"] = "wrong"
        r = build_controlled_enforce_dry_run(
            session_id="s1", pair_id="p1", preflight_report=bad)
        assert r["status"] == STATUS_BLOCKED
        assert "preflight_missing" in r["blocked_reasons"]

    def test_blocked_from_real_apply_reasons(self):
        r = _build()
        for w in r["would_skip_items"]:
            assert "dry_run_only" in w["blocked_from_real_apply_reasons"]
            assert "enforce_config_disabled" in w["blocked_from_real_apply_reasons"]


class TestFromDirAndWrite:
    def _dir(self, tmp_path, with_preflight=True):
        d = tmp_path / "pipeline_v2"
        d.mkdir()
        (d / "skip_readiness_report.json").write_text(json.dumps(_SR2), encoding="utf-8")
        (d / "exclusion_preview_v2_report.json").write_text(json.dumps(_XP2), encoding="utf-8")
        if with_preflight:
            (d / "controlled_enforce_preflight_report.json").write_text(
                json.dumps(_preflight("preflight_ok", _IDS)), encoding="utf-8")
        return d

    def test_from_dir_ok(self, tmp_path):
        d = self._dir(tmp_path)
        r = build_controlled_enforce_dry_run_from_dir(d, session_id="s1", pair_id="p1")
        assert r["status"] == STATUS_OK
        assert r["summary"]["logical_transitions"] == 1

    def test_from_dir_missing_preflight_blocked(self, tmp_path):
        d = self._dir(tmp_path, with_preflight=False)
        r = build_controlled_enforce_dry_run_from_dir(d, session_id="s1", pair_id="p1")
        assert r["status"] == STATUS_BLOCKED

    def test_from_dir_no_runtime_writes(self, tmp_path):
        d = self._dir(tmp_path)

        def snap():
            return {str(p): (p.stat().st_size, p.stat().st_mtime_ns)
                    for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        build_controlled_enforce_dry_run_from_dir(d, session_id="s1", pair_id="p1")
        assert snap() == before  # nothing written/touched

    def test_write_report_explicit_only(self, tmp_path):
        out = tmp_path / "out" / "controlled_enforce_dry_run_report.json"
        r = _build()
        p = write_controlled_enforce_dry_run_report(out, r)
        assert p.is_file()
        assert json.loads(p.read_text())["kind"] == DRY_RUN_KIND


class TestSafety:
    def test_no_model_or_llm_imports(self):
        """(12) module source imports no Qwen/Gemma/Claude/Opus/job/runner."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce_dry_run.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus",
                     "md_enrichment_jobs", "unified_analysis_jobs",
                     "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "problem_block_retry", "providers",
                     "text_llm_provider")
        offenders = [m for m in imported
                     if any(sub in m.lower() for sub in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
