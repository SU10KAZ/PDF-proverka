# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce CONFIG schema + validation (v0).

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_config.py

Schema/validation only — no enforce, no runtime writes, no models.
"""
import ast
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_config import (
    build_controlled_enforce_config,
    validate_controlled_enforce_config,
    MODE_DRY_RUN_ONLY,
    MODE_ENFORCE_ONE,
    CONFIG_KIND,
    V0_MAX_LOGICAL_TRANSITIONS,
    V0_MAX_BLOCK_PAIRS,
)

SID = "ba413a93c5754f6c"
PID = "pf06effb7"


def _enforce_ready():
    """Минимальный config-shape, при котором enforce РАЗРЕШЁН (для негативных мутаций)."""
    c = build_controlled_enforce_config(SID, PID, mode=MODE_ENFORCE_ONE)
    c["enabled"] = True
    c["human_confirmation_token"] = "CONFIRM-ВРУ3-ВРУ2-2026-06-14"
    return c


class TestConfig:
    def test_default_config_disabled(self):
        """(1) default config disabled, enforce not allowed."""
        c = build_controlled_enforce_config(SID, PID)
        assert c["kind"] == CONFIG_KIND
        assert c["enabled"] is False
        assert c["mode"] == MODE_DRY_RUN_ONLY
        assert c["human_confirmation_token"] == ""
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is True
        assert r["enforce_allowed"] is False
        assert "config_disabled" in r["deny_reasons"]

    def test_enforce_ready_shape_allowed(self):
        """Полный enforce-shape + root ok → enforce_allowed=True."""
        r = validate_controlled_enforce_config(_enforce_ready(), root_guard_status="ok")
        assert r["ok"] is True
        assert r["enforce_allowed"] is True
        assert r["deny_reasons"] == []

    def test_invalid_mode_rejected(self):
        """(2) invalid mode rejected."""
        c = _enforce_ready()
        c["mode"] = "enforce_everything"
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert r["enforce_allowed"] is False
        assert any("mode must be one of" in e for e in r["errors"])

    def test_missing_human_token_blocks_enforce(self):
        """(3) missing human token blocks enforce."""
        c = _enforce_ready()
        c["human_confirmation_token"] = ""
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["enforce_allowed"] is False
        assert "missing_human_confirmation_token" in r["deny_reasons"]
        # whitespace-only also blocks
        c["human_confirmation_token"] = "   "
        r2 = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r2["enforce_allowed"] is False
        assert "missing_human_confirmation_token" in r2["deny_reasons"]

    def test_scope_grounded_evidence_rejected(self):
        """(4) scope attempting grounded evidence skip rejected."""
        c = _enforce_ready()
        c["allowed_scope"]["exclude_from_grounded_evidence"] = True
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert r["enforce_allowed"] is False
        assert any("exclude_from_grounded_evidence" in e for e in r["errors"])
        assert "scope_violation" in r["deny_reasons"]

    def test_scope_delta_rejected(self):
        """(5) scope attempting delta skip rejected."""
        c = _enforce_ready()
        c["allowed_scope"]["exclude_from_delta_explanation"] = True
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("exclude_from_delta_explanation" in e for e in r["errors"])

    def test_scope_findings_rejected(self):
        """(6) scope attempting findings skip rejected."""
        c = _enforce_ready()
        c["allowed_scope"]["exclude_from_findings"] = True
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("exclude_from_findings" in e for e in r["errors"])

    def test_enrichment_false_rejected(self):
        """scope с enrichment=false бессмыслен → отклоняется."""
        c = _enforce_ready()
        c["allowed_scope"]["exclude_from_enrichment"] = False
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("exclude_from_enrichment" in e for e in r["errors"])

    def test_max_logical_transitions_over_limit_rejected(self):
        """(7) max_logical_transitions > 1 rejected for v0."""
        c = _enforce_ready()
        c["max_logical_transitions_per_run"] = V0_MAX_LOGICAL_TRANSITIONS + 1
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("max_logical_transitions_per_run" in e for e in r["errors"])

    def test_max_block_pairs_over_limit_rejected(self):
        """(8) max_block_pairs > 2 rejected for v0."""
        c = _enforce_ready()
        c["max_block_pairs_per_run"] = V0_MAX_BLOCK_PAIRS + 1
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("max_block_pairs_per_run" in e for e in r["errors"])

    def test_protected_reports_required(self):
        """(9) protected reports list required."""
        c = _enforce_ready()
        c["protected_reports"] = ["entity_diff_report.json"]  # incomplete
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("protected_reports must include" in e for e in r["errors"])
        # also required_reports
        c2 = _enforce_ready()
        c2["required_reports"] = []
        r2 = validate_controlled_enforce_config(c2, root_guard_status="ok")
        assert any("required_reports must include" in e for e in r2["errors"])

    def test_root_guard_warning_blocks_enforce(self):
        """(10) root guard status warning/dangerous blocks enforce."""
        c = _enforce_ready()
        rw = validate_controlled_enforce_config(c, root_guard_status="warning")
        assert rw["enforce_allowed"] is False
        assert "root_guard_warning" in rw["deny_reasons"]
        rd = validate_controlled_enforce_config(c, root_guard_status="dangerous")
        assert rd["enforce_allowed"] is False
        assert "root_guard_dangerous" in rd["deny_reasons"]

    def test_comparison_root_must_match_required(self):
        """required_root_guard.comparison_root_must_match_api_info обязателен."""
        c = _enforce_ready()
        c["required_root_guard"]["comparison_root_must_match_api_info"] = False
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["ok"] is False
        assert any("comparison_root_must_match_api_info" in e for e in r["errors"])

    def test_dry_run_mode_never_enforces(self):
        """dry_run_only mode никогда не разрешает enforce, даже с токеном."""
        c = build_controlled_enforce_config(SID, PID, mode=MODE_DRY_RUN_ONLY)
        c["enabled"] = True
        c["human_confirmation_token"] = "CONFIRM"
        r = validate_controlled_enforce_config(c, root_guard_status="ok")
        assert r["enforce_allowed"] is False
        assert "mode_not_enforce" in r["deny_reasons"]

    def test_non_dict_config(self):
        r = validate_controlled_enforce_config("not a dict")
        assert r["ok"] is False
        assert r["enforce_allowed"] is False


class TestSafety:
    def test_no_model_or_llm_imports(self):
        """(11) module source imports no Qwen/Gemma/Claude/Opus/job/runner."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce_config.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus", "md_enrichment_jobs",
                     "unified_analysis_jobs", "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "providers", "text_llm_provider")
        offenders = [m for m in imported
                     if any(s in m.lower() for s in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
