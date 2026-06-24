# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce config + preflight preview.

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce.py

Mark-only / preflight. No enforce, no runtime writes to pipeline inputs,
no Qwen/Gemma/Claude/Opus.
"""
import ast
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce import (
    build_controlled_enforce_config,
    build_controlled_enforce_preflight,
    build_controlled_enforce_preflight_from_dir,
    evaluate_item,
    snapshot_protected_hashes,
    write_controlled_enforce_preflight_report,
    CONFIG_KIND,
    PREFLIGHT_KIND,
    STATUS_BLOCKED,
    STATUS_PREFLIGHT_OK,
    STATUS_NO_ELIGIBLE,
    OP_APPROVE_EXCLUDE,
    FB_READY_TO_SKIP_ZERO,
    FB_SKIP_READINESS_MISSING,
    FB_RUNTIME_ROOT_UNCONFIRMED,
    FB_PROTECTED_HASHES_MISSING,
    FB_PROTECTED_HASH_MISMATCH,
    BI_MARK_ONLY_VIOLATION,
    BI_MISSING_APPROVAL,
    BI_INVALID_SCOPE,
)

SID = "ba413a93c5754f6c"
PID = "pf06effb7"

_GOOD_SCOPE = {
    "exclude_from_enrichment": True,
    "exclude_from_grounded_evidence": False,
    "exclude_from_delta_explanation": False,
    "exclude_from_findings": False,
}


def _item(item_id, *, readiness, op=None, cls="candidate_exclude",
          scope=None, auto_apply=False, enforce_allowed=False,
          blocked_reason=None):
    it = {
        "item_id": item_id,
        "readiness_status": readiness,
        "operator_decision": op,
        "classification": cls,
        "skip_scope": scope if scope is not None else dict(_GOOD_SCOPE),
        "auto_apply": auto_apply,
        "enforce_allowed": enforce_allowed,
        "requires_explicit_operator_approval": True,
    }
    if blocked_reason:
        it["blocked_reason"] = blocked_reason
    return it


def _sr_report(items, *, ready=0, approved=0):
    return {
        "version": "1",
        "kind": "skip_readiness_report_v1",
        "status": "ok",
        "session_id": SID,
        "pair_id": PID,
        "summary": {
            "items_total": len(items),
            "ready_to_skip": ready,
            "blocked": sum(1 for i in items if i["readiness_status"] == "blocked"),
            "needs_review": sum(1 for i in items if i["readiness_status"] == "needs_review"),
            "keep": sum(1 for i in items if i["readiness_status"] == "keep"),
            "operator_approved": approved,
            "operator_rejected": 0,
            "missing_operator_decision": sum(1 for i in items if i["operator_decision"] is None),
            "auto_enforce_enabled": False,
        },
        "items": items,
        "auto_enforce_enabled": False,
        "enforce_allowed": False,
    }


# ── confirmed-root + hashes available baseline (so only the tested guard fires)
_OK_ROOT = "/home/coder/projects/PDF-proverka-deploy/comparison"
_OK_HASHES = {
    "skip_readiness_report.json": "a" * 64,
    "exclusion_preview_v2_report.json": "b" * 64,
}


def _preflight(sr, **kw):
    defaults = dict(
        session_id=SID, pair_id=PID, skip_readiness_report=sr,
        active_runtime_root=_OK_ROOT, runtime_root_confirmed=True,
        runtime_root_source="/api/info",
        protected_hashes=dict(_OK_HASHES), protected_hashes_match=True)
    defaults.update(kw)
    return build_controlled_enforce_preflight(**defaults)


# ─── config ──────────────────────────────────────────────────────────────────


class TestConfig:
    def test_config_invariants(self):
        cfg = build_controlled_enforce_config(SID, PID)
        assert cfg["kind"] == CONFIG_KIND
        assert cfg["enabled"] is False
        assert cfg["mode"] == "preflight_only"
        assert cfg["allowed_decisions"] == [OP_APPROVE_EXCLUDE]
        assert cfg["allowed_scopes"]["exclude_from_enrichment"] is True
        assert cfg["allowed_scopes"]["exclude_from_findings"] is False
        for g in ("active_runtime_root_confirmed", "backup_required",
                  "operator_approval_required", "ready_to_skip_required",
                  "protected_hashes_required", "dry_run_required",
                  "rollback_plan_required"):
            assert cfg["required_guards"][g] is True


# ─── decision logic ────────────────────────────────────────────────────────


class TestPreflight:
    def test_ready_zero_blocks_with_fatal(self):
        """(1) ready_to_skip=0 → status blocked, fatal ready_to_skip_zero."""
        items = [_item("x1", readiness="blocked", blocked_reason="missing_operator_approval"),
                 _item("x2", readiness="keep")]
        r = _preflight(_sr_report(items, ready=0))
        assert r["status"] == STATUS_BLOCKED
        assert FB_READY_TO_SKIP_ZERO in r["fatal_blocks"]
        assert r["summary"]["would_apply"] is False
        assert r["summary"]["eligible_items"] == 0
        assert r["summary"]["enforce_enabled"] is False

    def test_missing_skip_readiness_blocks(self):
        """(2) missing skip_readiness → blocked."""
        r = _preflight(None)
        assert r["status"] == STATUS_BLOCKED
        assert FB_SKIP_READINESS_MISSING in r["fatal_blocks"]

    def test_eligible_requires_approve_exclude(self):
        """(3) ready item w/o approve_exclude → blocked item, not eligible."""
        items = [_item("x1", readiness="ready_to_skip", op=None)]
        r = _preflight(_sr_report(items, ready=1, approved=0))
        assert r["summary"]["eligible_items"] == 0
        bi = {b["item_id"]: b["reason"] for b in r["blocked_items"]}
        assert bi["x1"] == BI_MISSING_APPROVAL

    def test_approve_plus_ready_eligible_not_applied(self):
        """(4) approve_exclude + ready_to_skip → eligible but NOT applied."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)]
        r = _preflight(_sr_report(items, ready=1, approved=1))
        assert r["status"] == STATUS_PREFLIGHT_OK
        assert r["summary"]["eligible_items"] == 1
        assert r["would_skip"] == ["x1"]
        assert r["summary"]["would_apply"] is False
        assert r["auto_apply"] is False
        assert r["enforce_allowed"] is False
        # eligible item explicitly marked NOT applied
        assert r["eligible_items"][0]["applied"] is False
        assert r["eligible_items"][0]["would_skip"] is True

    def test_keep_and_needs_review_blocked(self):
        """(5) keep / needs_review → blocked items (not eligible)."""
        items = [_item("k1", readiness="keep"),
                 _item("n1", readiness="needs_review")]
        r = _preflight(_sr_report(items, ready=0))
        assert r["summary"]["eligible_items"] == 0
        reasons = {b["item_id"]: b["reason"] for b in r["blocked_items"]}
        assert reasons["k1"] == "keep"
        assert reasons["n1"] == "needs_review"

    def test_invalid_scope_blocks_item(self):
        """(6) invalid skip_scope blocks an otherwise-eligible item."""
        bad_scope = dict(_GOOD_SCOPE, exclude_from_findings=True)
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE,
                       scope=bad_scope)]
        r = _preflight(_sr_report(items, ready=1, approved=1))
        assert r["summary"]["eligible_items"] == 0
        assert r["blocked_items"][0]["reason"] == BI_INVALID_SCOPE
        # no eligible → no fatal but no_eligible_items status
        assert r["status"] == STATUS_NO_ELIGIBLE

    def test_source_invariant_violation_blocks(self):
        """(7) source auto_apply/enforce_allowed=true → mark_only violation."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE,
                       enforce_allowed=True)]
        r = _preflight(_sr_report(items, ready=1, approved=1))
        assert r["summary"]["eligible_items"] == 0
        assert r["blocked_items"][0]["reason"] == BI_MARK_ONLY_VIOLATION

        items2 = [_item("x2", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE,
                        auto_apply=True)]
        r2 = _preflight(_sr_report(items2, ready=1, approved=1))
        assert r2["blocked_items"][0]["reason"] == BI_MARK_ONLY_VIOLATION

    def test_runtime_root_missing_blocks(self):
        """(8) active runtime root not confirmed → blocked."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)]
        r = _preflight(_sr_report(items, ready=1, approved=1),
                       active_runtime_root=None, runtime_root_confirmed=False)
        assert r["status"] == STATUS_BLOCKED
        assert FB_RUNTIME_ROOT_UNCONFIRMED in r["fatal_blocks"]
        assert r["runtime_root"]["confirmed"] is False

    def test_protected_hashes_missing_blocks(self):
        """(9a) protected hashes missing → blocked."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)]
        r = _preflight(_sr_report(items, ready=1, approved=1),
                       protected_hashes={})
        assert r["status"] == STATUS_BLOCKED
        assert FB_PROTECTED_HASHES_MISSING in r["fatal_blocks"]

    def test_protected_hash_mismatch_blocks(self):
        """(9b) protected hash mismatch → blocked."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)]
        r = _preflight(_sr_report(items, ready=1, approved=1),
                       protected_hashes_match=False)
        assert r["status"] == STATUS_BLOCKED
        assert FB_PROTECTED_HASH_MISMATCH in r["fatal_blocks"]

    def test_would_apply_always_false(self):
        """(10) would_apply / enforce_enabled always false, even when eligible."""
        items = [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)]
        r = _preflight(_sr_report(items, ready=1, approved=1))
        assert r["summary"]["would_apply"] is False
        assert r["summary"]["enforce_enabled"] is False
        assert r["would_write"] == []
        assert r["auto_apply"] is False
        assert r["enforce_allowed"] is False

    def test_kind_and_required_fields(self):
        r = _preflight(_sr_report([], ready=0))
        assert r["kind"] == PREFLIGHT_KIND
        for k in ("status", "summary", "global_guards", "runtime_root",
                  "eligible_items", "blocked_items", "fatal_blocks",
                  "would_write", "would_skip"):
            assert k in r


# ─── from-dir + no runtime writes ────────────────────────────────────────────


class TestFromDir:
    def _make_dir(self, tmp_path, sr_report):
        d = tmp_path / "pipeline_v2"
        d.mkdir()
        (d / "skip_readiness_report.json").write_text(
            json.dumps(sr_report), encoding="utf-8")
        (d / "exclusion_preview_v2_report.json").write_text(
            json.dumps({"kind": "x"}), encoding="utf-8")
        return d

    def test_from_dir_blocks_on_ready_zero(self, tmp_path):
        d = self._make_dir(tmp_path, _sr_report(
            [_item("x1", readiness="blocked")], ready=0))
        r = build_controlled_enforce_preflight_from_dir(
            d, session_id=SID, pair_id=PID,
            active_runtime_root=_OK_ROOT, runtime_root_confirmed=True)
        assert r["status"] == STATUS_BLOCKED
        assert FB_READY_TO_SKIP_ZERO in r["fatal_blocks"]
        # protected hashes were snapshotted from the dir
        assert r["protected_hashes"]["available"] is True

    def test_no_runtime_writes(self, tmp_path):
        """(11) building preflight does NOT write/modify pipeline inputs."""
        d = self._make_dir(tmp_path, _sr_report(
            [_item("x1", readiness="ready_to_skip", op=OP_APPROVE_EXCLUDE)],
            ready=1, approved=1))

        def snap():
            return {str(p): (p.stat().st_size, p.stat().st_mtime_ns)
                    for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        build_controlled_enforce_preflight_from_dir(
            d, session_id=SID, pair_id=PID,
            active_runtime_root=_OK_ROOT, runtime_root_confirmed=True)
        assert snap() == before  # nothing written or touched

    def test_snapshot_protected_hashes(self, tmp_path):
        d = self._make_dir(tmp_path, _sr_report([], ready=0))
        h = snapshot_protected_hashes(d)
        assert "skip_readiness_report.json" in h
        assert "exclusion_preview_v2_report.json" in h
        assert all(len(v) == 64 for v in h.values())

    def test_write_report_is_explicit_only(self, tmp_path):
        """write helper writes ONLY the preflight report, nowhere else."""
        out = tmp_path / "out" / "controlled_enforce_preflight_report.json"
        r = _preflight(_sr_report([], ready=0))
        p = write_controlled_enforce_preflight_report(out, r)
        assert p.is_file()
        assert json.loads(p.read_text())["kind"] == PREFLIGHT_KIND


# ─── dry-run integration + ui payload ────────────────────────────────────────


class TestDryRunIntegration:
    def test_stage_disabled_by_default(self):
        """(12) dry-run stage controlled_enforce_preflight disabled by default."""
        from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
            _controlled_enforce_section,
        )
        # enabled=False default → disabled section, no report
        sec = _controlled_enforce_section(None, False, None)
        assert sec["enabled"] is False
        assert sec["status"] == "disabled"
        assert sec["would_apply"] is False
        assert sec["enforce_enabled"] is False

    def test_dry_run_section_reads_report(self):
        from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
            _controlled_enforce_section,
        )
        r = _preflight(_sr_report(
            [_item("x1", readiness="blocked")], ready=0))
        sec = _controlled_enforce_section(r, True, None)
        assert sec["enabled"] is True
        assert sec["status"] == STATUS_BLOCKED
        assert sec["ready_to_skip_items"] == 0
        assert sec["fatal_blocks"] >= 1
        assert sec["would_apply"] is False

    def test_ui_payload_summary_reads_report(self):
        """(13) ui_payload surfaces controlled_enforce_preflight section."""
        from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
            build_pipeline_v2_ui_payload,
        )
        from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
            _controlled_enforce_section,
        )
        ce_report = _preflight(_sr_report(
            [_item("x1", readiness="blocked")], ready=0))
        # minimal valid summary with the controlled_enforce_preflight section
        summary = {
            "version": 1, "kind": "stage_comparison_pipeline_v2_summary",
            "status": "ok", "artifacts": {}, "inputs": {},
            "stages": {
                "prepared_ingest": {}, "block_matching": {},
                "entity_extraction": {}, "entity_diff": {},
            },
            "controlled_enforce_preflight": _controlled_enforce_section(
                ce_report, True, None),
        }
        payload = build_pipeline_v2_ui_payload(summary)
        cep = payload.get("controlled_enforce_preflight")
        assert cep is not None
        assert cep["available"] is True
        assert cep["status"] == STATUS_BLOCKED
        assert cep["ready_to_skip_items"] == 0
        assert cep["would_apply"] is False
        assert cep["enforce_enabled"] is False

    def test_ui_payload_omits_when_disabled(self):
        from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
            build_pipeline_v2_ui_payload,
        )
        from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
            _controlled_enforce_section,
        )
        summary = {
            "version": 1, "kind": "stage_comparison_pipeline_v2_summary",
            "status": "ok", "artifacts": {}, "inputs": {},
            "stages": {"prepared_ingest": {}, "block_matching": {},
                       "entity_extraction": {}, "entity_diff": {}},
            "controlled_enforce_preflight": _controlled_enforce_section(
                None, False, None),
        }
        payload = build_pipeline_v2_ui_payload(summary)
        assert "controlled_enforce_preflight" not in payload


# ─── safety ──────────────────────────────────────────────────────────────────


class TestSafety:
    def test_no_model_or_llm_imports(self):
        """(14) module source imports no Qwen/Gemma/Claude/Opus/job/runner."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce.py")
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

    def test_ios11_fixture_ready_zero_blocks(self):
        """(15) ИОС 1.1-like fixture (54 items, ready=0) → blocked."""
        items = ([_item(f"b{i}", readiness="blocked",
                        blocked_reason="missing_operator_approval") for i in range(21)]
                 + [_item(f"n{i}", readiness="needs_review") for i in range(20)]
                 + [_item(f"k{i}", readiness="keep") for i in range(13)])
        assert len(items) == 54
        r = _preflight(_sr_report(items, ready=0, approved=0))
        assert r["status"] == STATUS_BLOCKED
        assert r["summary"]["ready_to_skip_items"] == 0
        assert r["summary"]["eligible_items"] == 0
        assert FB_READY_TO_SKIP_ZERO in r["fatal_blocks"]
        assert r["summary"]["would_apply"] is False
        assert r["summary"]["enforce_enabled"] is False
        assert len(r["blocked_items"]) == 54
