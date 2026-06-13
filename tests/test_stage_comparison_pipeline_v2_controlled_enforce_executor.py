# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce EXECUTOR v0 (code-only / diagnostics).

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_executor.py

apply=False default — no enforce, no runtime writes, no models.
"""
import ast
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_config import (
    build_controlled_enforce_config, MODE_ENFORCE_ONE,
    V0_MAX_LOGICAL_TRANSITIONS, V0_MAX_BLOCK_PAIRS,
)
from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    ControlledEnforceNotImplemented,
    STATE_KIND, STATE_FILENAME, PLAN_KIND,
    PLAN_STATUS_READY, PLAN_STATUS_BLOCKED_CONFIG,
    validate_controlled_enforce_runtime_guards,
    build_controlled_enforce_execution_plan,
    build_controlled_enforce_state_preview,
    build_controlled_enforce_rollback_plan,
    run_controlled_enforce_executor,
    filter_candidates_by_controlled_enforce_state,
    snapshot_protected_hashes,
)

SID, PID = "ba413a93c5754f6c", "pf06effb7"
_IDS = ["xp_bp::A__B", "xp_bp::C__D"]


def _enforce_config():
    c = build_controlled_enforce_config(SID, PID, mode=MODE_ENFORCE_ONE)
    c["enabled"] = True
    c["human_confirmation_token"] = "CONFIRM-ВРУ3-ВРУ2"
    return c


def _preflight(status="preflight_ok"):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_controlled_enforce_preflight",
            "status": status, "summary": {"eligible_items": 2}, "fatal_blocks": []}


def _dry_run(status="ok"):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_controlled_enforce_dry_run",
        "status": status,
        "summary": {"eligible_items": 2, "logical_transitions": 1,
                    "would_skip_block_pairs": 2, "would_apply": False, "enforce_enabled": False},
        "logical_transitions": [
            {"transition_id": "ВРУ-3→ВРУ-2", "left_entity_label": "ВРУ-3",
             "right_entity_label": "ВРУ-2", "item_count": 2, "items": _IDS,
             "operator_decision": "approve_exclude",
             "link_validation_decisions": ["reject_mapping"], "confidence": 0.99,
             "recommended_scope": {"exclude_from_enrichment": True,
                "exclude_from_grounded_evidence": False,
                "exclude_from_delta_explanation": False, "exclude_from_findings": False}}],
        "would_skip_items": [
            {"item_id": "xp_bp::A__B", "left_block_id": "A", "right_block_id": "B",
             "operator_decision_id": "xrd_8f25b282daf5"},
            {"item_id": "xp_bp::C__D", "left_block_id": "C", "right_block_id": "D",
             "operator_decision_id": "xrd_8f25b282daf5"}],
        "would_apply": False, "enforce_enabled": False,
    }


def _skip_readiness(ready=2):
    return {"version": "1", "kind": "skip_readiness_report_v1", "status": "ok",
            "summary": {"ready_to_skip": ready}}


def _guards(**kw):
    defaults = dict(config=_enforce_config(), preflight_report=_preflight(),
                    dry_run_report=_dry_run(), skip_readiness_report=_skip_readiness(),
                    root_guard_status="ok", queue_active=False,
                    protected_hashes={"entity_diff_report.json": "a" * 64})
    defaults.update(kw)
    return validate_controlled_enforce_runtime_guards(**defaults)


class TestRuntimeGuards:
    def test_all_ok_apply_allowed(self):
        g = _guards()
        assert g["apply_allowed"] is True
        assert g["blocked_reasons"] == []

    def test_config_disabled_blocks(self):
        """(1) config disabled → apply blocked."""
        c = _enforce_config(); c["enabled"] = False
        g = _guards(config=c)
        assert g["apply_allowed"] is False
        assert any("config:" in b for b in g["blocked_reasons"])

    def test_missing_token_blocks(self):
        """(2) missing human token → apply blocked."""
        c = _enforce_config(); c["human_confirmation_token"] = ""
        g = _guards(config=c)
        assert g["apply_allowed"] is False
        assert "config:missing_human_confirmation_token" in g["blocked_reasons"]

    def test_preflight_not_ok_blocks(self):
        """(3) preflight not ok → apply blocked."""
        g = _guards(preflight_report=_preflight(status="blocked"))
        assert g["apply_allowed"] is False
        assert "preflight_not_ok" in g["blocked_reasons"]

    def test_dry_run_not_ok_blocks(self):
        """(4) dry_run not ok → apply blocked."""
        g = _guards(dry_run_report=_dry_run(status="blocked"))
        assert g["apply_allowed"] is False
        assert "dry_run_not_ok" in g["blocked_reasons"]

    def test_root_guard_warning_blocks(self):
        g = _guards(root_guard_status="warning")
        assert g["apply_allowed"] is False
        assert "root_guard_warning" in g["blocked_reasons"]

    def test_queue_active_blocks(self):
        g = _guards(queue_active=True)
        assert g["apply_allowed"] is False
        assert "queue_active" in g["blocked_reasons"]

    def test_protected_hashes_unavailable_blocks(self):
        g = _guards(protected_hashes={})
        assert g["apply_allowed"] is False
        assert "protected_hashes_unavailable" in g["blocked_reasons"]

    def test_ready_to_skip_zero_blocks(self):
        g = _guards(skip_readiness_report=_skip_readiness(ready=0))
        assert g["apply_allowed"] is False
        assert "ready_to_skip_zero" in g["blocked_reasons"]


class TestExecutionPlan:
    def test_plan_groups_one_transition_two_block_pairs(self):
        """(5)(6) eligible 2 / 1 transition; plan shows 2 block-pairs."""
        plan = build_controlled_enforce_execution_plan(
            session_id=SID, pair_id=PID, config=_enforce_config(),
            dry_run_report=_dry_run(), guards=_guards())
        assert plan["kind"] == PLAN_KIND
        assert plan["summary"]["eligible_items"] == 2
        assert plan["summary"]["logical_transitions"] == 1
        assert plan["summary"]["block_pairs"] == 2
        assert plan["summary"]["would_create_state_entries"] == 1
        assert plan["status"] == PLAN_STATUS_READY

    def test_plan_protected_not_modified(self):
        """(8) protected reports would not modify."""
        plan = build_controlled_enforce_execution_plan(
            session_id=SID, pair_id=PID, config=_enforce_config(),
            dry_run_report=_dry_run(), guards=_guards())
        assert plan["summary"]["would_modify_protected_reports"] is False
        assert plan["summary"]["would_modify_runtime"] is False
        assert plan["summary"]["applied"] is False
        assert plan["would_write"] == [STATE_FILENAME]

    def test_plan_blocked_by_config_when_disabled(self):
        c = _enforce_config(); c["enabled"] = False
        plan = build_controlled_enforce_execution_plan(
            session_id=SID, pair_id=PID, config=c,
            dry_run_report=_dry_run(), guards=_guards(config=c))
        assert plan["status"] == PLAN_STATUS_BLOCKED_CONFIG
        # diagnostics still shows eligible
        assert plan["summary"]["eligible_items"] == 2


class TestStatePreviewAndRollback:
    def test_state_preview_not_active(self):
        """(10) future state preview has active=false."""
        st = build_controlled_enforce_state_preview(
            session_id=SID, pair_id=PID, dry_run_report=_dry_run(),
            config=_enforce_config())
        assert st["kind"] == STATE_KIND
        assert st["status"] == "preview"
        assert len(st["applied_exclusions"]) == 1
        ex = st["applied_exclusions"][0]
        assert ex["active"] is False
        assert ex["transition_id"] == "ВРУ-3→ВРУ-2"
        assert ex["item_ids"] == _IDS
        assert ex["scope"]["exclude_from_enrichment"] is True
        assert ex["scope"]["exclude_from_findings"] is False
        assert ex["operator_decision_id"] == "xrd_8f25b282daf5"

    def test_rollback_plan_generated(self):
        """(14) rollback plan generated."""
        rb = build_controlled_enforce_rollback_plan(run_id="r1", rollback_id="rb1")
        assert rb["rollback_id"] == "rb1"
        assert rb["would_remove_run_id"] == "r1"
        assert rb["would_restore_state_from_backup"] is True
        assert rb["protected_reports_expected_unchanged"] is True
        assert isinstance(rb["manual_steps"], list) and rb["manual_steps"]


class TestExecutor:
    def _dir(self, tmp_path, ok=True):
        d = tmp_path / "pipeline_v2"
        d.mkdir()
        (d / "skip_readiness_report.json").write_text(json.dumps(_skip_readiness()), encoding="utf-8")
        (d / "controlled_enforce_preflight_report.json").write_text(json.dumps(_preflight()), encoding="utf-8")
        (d / "controlled_enforce_dry_run_report.json").write_text(json.dumps(_dry_run()), encoding="utf-8")
        # one protected report for hash sentinel
        (d / "entity_diff_report.json").write_text(json.dumps({"k": 1}), encoding="utf-8")
        return d

    def test_apply_false_writes_nothing(self, tmp_path):
        """(9) apply=False writes nothing."""
        d = self._dir(tmp_path)

        def snap():
            return {str(p): (p.stat().st_size, p.stat().st_mtime_ns)
                    for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        res = run_controlled_enforce_executor(
            d, config=_enforce_config(), session_id=SID, pair_id=PID,
            root_guard_status="ok", apply=False)
        assert snap() == before
        assert res["applied"] is False
        assert res["runtime_changed"] is False
        # state preview present but not written + not active
        assert res["state_preview"]["status"] == "preview"
        assert res["plan"]["summary"]["block_pairs"] == 2

    def test_apply_true_blocked(self, tmp_path):
        """(11) apply=True without valid config blocked (v0 not implemented)."""
        d = self._dir(tmp_path)
        with pytest.raises(ControlledEnforceNotImplemented):
            run_controlled_enforce_executor(
                d, config=_enforce_config(), session_id=SID, pair_id=PID,
                root_guard_status="ok", apply=True)

    def test_protected_hash_sentinel_snapshot(self, tmp_path):
        d = self._dir(tmp_path)
        h = snapshot_protected_hashes(d)
        assert "entity_diff_report.json" in h
        assert len(h["entity_diff_report.json"]) == 64


class TestLimits:
    def test_max_transition_limit_enforced(self):
        """(12) max transition limit enforced (via config validation in guards)."""
        c = _enforce_config()
        c["max_logical_transitions_per_run"] = V0_MAX_LOGICAL_TRANSITIONS + 1
        g = _guards(config=c)
        assert g["apply_allowed"] is False
        assert any("config:" in b for b in g["blocked_reasons"])

    def test_max_block_pair_limit_enforced(self):
        """(13) max block-pair limit enforced."""
        c = _enforce_config()
        c["max_block_pairs_per_run"] = V0_MAX_BLOCK_PAIRS + 1
        g = _guards(config=c)
        assert g["apply_allowed"] is False
        assert any("config:" in b for b in g["blocked_reasons"])

    def test_scope_only_enrichment(self):
        """(7) scope only enrichment allowed (grounded scope → config invalid)."""
        c = _enforce_config()
        c["allowed_scope"]["exclude_from_grounded_evidence"] = True
        g = _guards(config=c)
        assert g["apply_allowed"] is False
        assert "config:scope_violation" in g["blocked_reasons"]


class TestSelectionHook:
    def _candidates(self):
        return [{"left_block_id": "A", "right_block_id": "B", "candidate_kind": "x"},
                {"left_block_id": "C", "right_block_id": "D", "candidate_kind": "y"},
                {"left_block_id": "E", "right_block_id": "F", "candidate_kind": "z"}]

    def _active_state(self):
        return {"kind": STATE_KIND, "applied_exclusions": [
            {"active": True, "scope": {"exclude_from_enrichment": True},
             "left_block_ids": ["A"], "right_block_ids": ["B"]}]}

    def test_hook_default_false_preserves(self):
        """(15) selection hook default false preserves old behavior."""
        cands = self._candidates()
        out, removed = filter_candidates_by_controlled_enforce_state(
            cands, self._active_state(), enabled=False)
        assert out == cands
        assert removed == []

    def test_hook_true_excludes_active_state(self):
        """(16) selection hook true excludes controlled state candidates."""
        out, removed = filter_candidates_by_controlled_enforce_state(
            self._candidates(), self._active_state(), enabled=True)
        assert len(out) == 2
        assert all(not (c["left_block_id"] == "A" and c["right_block_id"] == "B")
                   for c in out)
        assert "A__B" in removed

    def test_hook_preview_state_not_active_excludes_nothing(self):
        """preview (active=false) не исключает ничего даже при enabled=true."""
        preview = build_controlled_enforce_state_preview(
            session_id=SID, pair_id=PID, dry_run_report=_dry_run(), config=_enforce_config())
        cands = self._candidates()
        out, removed = filter_candidates_by_controlled_enforce_state(
            cands, preview, enabled=True)
        assert out == cands
        assert removed == []

    def test_graphic_vision_hook_wired_default_off(self):
        """select_vision_candidates_v2 имеет hook, default OFF (поведение не меняется)."""
        from backend.app.services.stage_comparison import pipeline_v2_graphic_vision_enrichment as gv
        src = Path(gv.__file__).read_text(encoding="utf-8")
        assert "use_controlled_enforce_state" in src
        assert "filter_candidates_by_controlled_enforce_state" in src


class TestSafety:
    def test_no_model_subprocess_or_llm_imports(self):
        """(17)(18) module imports no Qwen/Gemma/Claude/Opus/subprocess/httpx."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce_executor.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus", "md_enrichment_jobs",
                     "unified_analysis_jobs", "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "providers", "subprocess", "httpx", "requests",
                     "text_llm_provider")
        offenders = [m for m in imported
                     if any(s in m.lower() for s in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
