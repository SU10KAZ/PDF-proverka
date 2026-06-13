# -*- coding: utf-8 -*-
"""Tests for production data-root guardrails (offline, read-only).

backend/app/services/stage_comparison/production_root_health.py
+ /api/info data_roots payload safety.
"""
import ast
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.production_root_health import (
    evaluate_production_data_roots,
    STATUS_OK,
    STATUS_WARNING,
    STATUS_DANGEROUS,
    HEALTH_KIND,
)


class TestEvaluateProductionDataRoots:
    def test_healthy_main_root(self, tmp_path):
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=109,
            comparison_root=str(comp), pipeline_v2_artifacts_present=True,
            base_dir="/home/coder/projects/PDF-proverka-deploy")
        assert r["kind"] == HEALTH_KIND
        assert r["status"] == STATUS_OK
        assert r["dangerous"] is False
        assert r["checks"]["objects_ok"] is True
        assert r["checks"]["projects_ok"] is True
        assert r["warnings"] == []

    def test_deploy_root_one_object_dangerous(self, tmp_path):
        """(7) wrong deploy root with objects_count=1 flags dangerous state."""
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=1, projects_count=0, comparison_root=str(comp))
        assert r["status"] == STATUS_DANGEROUS
        assert r["dangerous"] is True
        assert any("objects_count" in w for w in r["warnings"])

    def test_projects_zero_dangerous(self, tmp_path):
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=0, comparison_root=str(comp))
        assert r["status"] == STATUS_DANGEROUS
        assert any("projects_count=0" in w for w in r["warnings"])

    def test_missing_comparison_root_dangerous(self, tmp_path):
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=109,
            comparison_root=str(tmp_path / "nope" / "comparison"))
        assert r["status"] == STATUS_DANGEROUS
        assert any("comparison_root" in w for w in r["warnings"])

    def test_low_objects_warning_not_dangerous(self, tmp_path):
        """objects=2 (>1 but <3) → warning, not dangerous."""
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=2, projects_count=109, comparison_root=str(comp))
        assert r["status"] == STATUS_WARNING
        assert r["dangerous"] is False

    def test_low_projects_warning(self, tmp_path):
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=42, comparison_root=str(comp))
        assert r["status"] == STATUS_WARNING
        assert any("projects_count" in w for w in r["warnings"])

    def test_missing_pipeline_v2_artifacts_warning(self, tmp_path):
        """(8) missing comparison artifacts flags warning."""
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=109, comparison_root=str(comp),
            pipeline_v2_artifacts_present=False)
        assert r["status"] == STATUS_WARNING
        assert any("Pipeline V2" in w for w in r["warnings"])

    def test_no_writes_to_fs(self, tmp_path):
        """(9) evaluate does not write to roots (only reads is_dir)."""
        comp = tmp_path / "comparison"
        comp.mkdir()

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(tmp_path.rglob("*"))}
        before = snap()
        evaluate_production_data_roots(
            objects_count=3, projects_count=109, comparison_root=str(comp))
        assert snap() == before

    def test_none_counts_warn_not_crash(self, tmp_path):
        comp = tmp_path / "comparison"
        comp.mkdir()
        r = evaluate_production_data_roots(
            objects_count=None, projects_count=None, comparison_root=str(comp))
        assert r["status"] in (STATUS_WARNING, STATUS_DANGEROUS)
        assert any("недоступен" in w for w in r["warnings"])

    def test_drift_note_when_code_data_split(self, tmp_path):
        comp = tmp_path / "main" / "comparison"
        comp.mkdir(parents=True)
        r = evaluate_production_data_roots(
            objects_count=3, projects_count=109, comparison_root=str(comp),
            base_dir="/home/coder/projects/PDF-proverka-deploy")
        # base_dir(deploy) != parent(comparison_root)=main → drift note present
        assert r["drift_note"] is not None


class TestApiInfoSafety:
    """(4)(5) /api/info exposes data_roots (paths only), no secrets."""

    def test_api_info_source_exposes_data_roots_no_secrets(self):
        main_path = (Path(__file__).resolve().parent.parent /
                     "backend/app/main.py")
        src = main_path.read_text(encoding="utf-8")
        # api_info returns a data_roots block with the 4 path keys
        assert '"data_roots"' in src
        assert '"comparison_root"' in src
        assert '"audit_data_dir"' in src
        # must NOT dump secrets / whole env / tokens
        for forbidden in ("os.environ)", "PORTAL_SESSION_SECRET", "PORTAL_AUTH_USERS",
                          "dict(os.environ", "**os.environ"):
            assert forbidden not in src

    def test_health_module_no_model_or_job_imports(self):
        """(10) health module imports no Qwen/Gemma/Claude/Opus/job/runner."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/production_root_health.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus", "md_enrichment_jobs",
                     "unified_analysis_jobs", "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "providers")
        offenders = [m for m in imported
                     if any(s in m.lower() for s in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
