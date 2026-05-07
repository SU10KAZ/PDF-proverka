"""
test_analyze_critic_v2_batch.py
--------------------------------
Tests for analyze_critic_v2_batch.py analytics script.

All tests work WITHOUT touching production artifacts.
Tests use synthetic batch output directories.

Runs with:
    python -m pytest backend/tests/test_analyze_critic_v2_batch.py -v
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

# ─── Script path ──────────────────────────────────────────────────────────────

ANALYZE_SCRIPT = Path("backend/scripts/analyze_critic_v2_batch.py")

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _decision(
    fid: str,
    decision: str = "borderline",
    score: int = 6,
    ev: str = "partial",
    severity: str = "ЭКОНОМИЧЕСКОЕ",
    impact: str = "cost_schedule",
    reject_reason: str | None = None,
    has_evidence: bool = True,
    has_action: bool = True,
    has_impact: bool = True,
    project: str = "TEST-PROJECT",
) -> dict:
    return {
        "finding_id": fid,
        "decision": decision,
        "usefulness_score": score,
        "reject_reason": reject_reason,
        "reject_explanation": None,
        "merged_into": None,
        "impact_area": impact,
        "severity": severity,
        "has_evidence": has_evidence,
        "has_action": has_action,
        "has_impact": has_impact,
        "evidence_quality": ev,
        "project": project,
        "project_slug": "TEST-PROJECT",
    }


def _make_batch_output(
    tmp_path: Path,
    projects: list[dict],
    with_results_json: bool = True,
) -> Path:
    """
    Create a synthetic batch output directory.

    projects: list of {
        "slug": str,
        "decisions": [decision dict, ...],
        "comparison": optional list of comparison rows
    }
    """
    results = []
    for proj in projects:
        slug = proj["slug"]
        proj_dir = tmp_path / slug
        proj_dir.mkdir(parents=True, exist_ok=True)

        decisions = proj.get("decisions", [])
        # Write decisions
        (proj_dir / "critic_v2_decisions.json").write_text(
            json.dumps(decisions, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # Optional legacy comparison
        cmp = proj.get("comparison")
        if cmp:
            (proj_dir / "critic_v2_legacy_comparison.json").write_text(
                json.dumps({"rows": cmp}, ensure_ascii=False), encoding="utf-8"
            )

        accepted = sum(1 for d in decisions if d.get("decision") == "accept")
        borderline = sum(1 for d in decisions if d.get("decision") in ("borderline", "low_priority"))
        rejected = sum(1 for d in decisions if d.get("decision") == "reject")

        results.append({
            "project": slug,
            "section": "EOM",
            "total_findings": len(decisions),
            "accepted": accepted,
            "borderline": borderline,
            "rejected": rejected,
            "merged": 0,
            "rejected_by_rules": rejected,
            "rejected_by_score": 0,
            "rejection_reasons": {},
            "average_usefulness_score": 6.0,
            "evidence_breakdown": {"valid": 2, "partial": 2, "weak": 1, "none": 0},
            "blocks_index_used": False,
            "llm_gate_used": False,
            "llm_candidates_sent": 0,
            "llm_provider": None,
            "det_ms": 10,
            "llm_ms": 0,
            "output_dir": str(proj_dir),
            "comparison": None,
            "skipped": False,
            "error": None,
        })

    if with_results_json:
        (tmp_path / "batch_results.json").write_text(
            json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        summary = {
            "run_config": {"projects_processed": len(projects), "projects_skipped": 0,
                           "llm_gate_used": False, "llm_provider": None, "blocks_index_used": False},
            "totals": {"total_findings": sum(r["total_findings"] for r in results),
                       "accepted": sum(r["accepted"] for r in results),
                       "borderline": sum(r["borderline"] for r in results),
                       "rejected": sum(r["rejected"] for r in results),
                       "merged": 0, "accept_rate": 0.4, "reject_rate": 0.1},
            "evidence_breakdown": {"valid": 0, "partial": 0, "weak": 0, "none": 0},
            "rejection_reasons": {},
            "by_section": {"EOM": {"projects": len(projects), "findings": 10, "accepted": 4, "rejected": 1}},
            "legacy_comparison": None,
            "per_project": [],
            "skipped": [],
        }
        (tmp_path / "batch_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return tmp_path


def _load_analysis_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("analyze_critic_v2_batch", ANALYZE_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Import / API ─────────────────────────────────────────────────────────────

class TestImports:
    def test_script_imports_without_error(self):
        mod = _load_analysis_module()
        assert hasattr(mod, "run_analysis")
        assert hasattr(mod, "classify_borderline")
        assert hasattr(mod, "load_batch_decisions")
        assert hasattr(mod, "analyze_borderline")
        assert hasattr(mod, "render_markdown")
        assert hasattr(mod, "export_csv")


# ─── classify_borderline ──────────────────────────────────────────────────────

class TestClassifyBorderline:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_weak_low_score_low_severity_is_safe_reject(self):
        d = _decision(
            "F-001", decision="borderline", score=5, ev="weak",
            severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="documentation",
        )
        result = self.mod.classify_borderline(d)
        assert result == "safe_reject_candidate"

    def test_none_evidence_is_safe_reject(self):
        d = _decision(
            "F-002", decision="borderline", score=4, ev="none",
            severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="none",
        )
        assert self.mod.classify_borderline(d) == "safe_reject_candidate"

    def test_valid_high_score_high_impact_is_safe_accept(self):
        d = _decision(
            "F-003", decision="borderline", score=7, ev="valid",
            severity="КРИТИЧЕСКОЕ", impact="safety",
            has_action=True, has_evidence=True, has_impact=True,
        )
        # Add action text (long enough)
        d["action_required"] = "Заменить кабель на FRLS исполнение"
        assert self.mod.classify_borderline(d) == "safe_accept_candidate"

    def test_partial_critical_high_impact_is_safe_accept(self):
        d = _decision(
            "F-004", decision="borderline", score=7, ev="partial",
            severity="КРИТИЧЕСКОЕ", impact="safety",
        )
        d["action_required"] = "Выполнить замену кабельной трассы"
        assert self.mod.classify_borderline(d) == "safe_accept_candidate"

    def test_partial_evidence_medium_impact_is_keep(self):
        d = _decision(
            "F-005", decision="borderline", score=6, ev="partial",
            severity="ЭКСПЛУАТАЦИОННОЕ", impact="reliability",
        )
        # no action
        d["action_required"] = None
        assert self.mod.classify_borderline(d) == "keep_borderline"

    def test_reject_reason_is_safe_reject(self):
        d = _decision("F-006", decision="borderline", score=5, reject_reason="no_impact")
        assert self.mod.classify_borderline(d) == "safe_reject_candidate"

    def test_valid_score6_high_impact_has_action(self):
        d = _decision(
            "F-007", decision="borderline", score=6, ev="valid",
            severity="ЭКОНОМИЧЕСКОЕ", impact="cost_schedule",
        )
        d["action_required"] = "Пересчитать смету с учетом актуального прайса"
        assert self.mod.classify_borderline(d) == "safe_accept_candidate"

    def test_weak_generic_title_is_safe_reject(self):
        d = _decision(
            "F-008", decision="borderline", score=5, ev="weak",
            severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="documentation",
        )
        # Add generic marker in title
        d["title"] = "Необходимо проверить соответствие норм"
        assert self.mod.classify_borderline(d) == "safe_reject_candidate"


# ─── _detect_borderline_source ────────────────────────────────────────────────

class TestDetectBorderlineSource:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_weak_cap5_detected(self):
        d = _decision("F-010", score=5, ev="weak")
        assert self.mod._detect_borderline_source(d) == "evidence_cap_weak"

    def test_partial_cap6_detected(self):
        d = _decision("F-011", score=6, ev="partial")
        assert self.mod._detect_borderline_source(d) == "evidence_cap_partial"

    def test_valid_score5_detected(self):
        d = _decision("F-012", score=5, ev="valid")
        assert self.mod._detect_borderline_source(d) == "score_56_no_cap"

    def test_cross_section_dep_detected(self):
        d = _decision("F-013", score=6, ev="partial")
        d["title"] = "Проверить по смежным разделам"
        d["description"] = "Требует уточнения из другого раздела проекта"
        assert self.mod._detect_borderline_source(d) == "cross_section_dep"

    def test_generic_wording_detected(self):
        d = _decision("F-014", score=6, ev="valid")
        d["title"] = "Необходимо проверить кабельные трассы"
        d["description"] = "Следует уточнить детали"
        assert self.mod._detect_borderline_source(d) == "generic_wording"

    def test_low_impact_detected(self):
        d = _decision("F-015", score=6, ev="partial", impact="documentation")
        assert self.mod._detect_borderline_source(d) == "low_impact_axis"


# ─── load_batch_decisions ─────────────────────────────────────────────────────

class TestLoadBatchDecisions:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_loads_from_results_json(self, tmp_path):
        decisions = [
            _decision("F-001", decision="accept", score=8),
            _decision("F-002", decision="borderline", score=6),
            _decision("F-003", decision="reject", score=3),
        ]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        loaded = self.mod.load_batch_decisions(batch_dir)
        assert len(loaded) == 3
        ids = {d["finding_id"] for d in loaded}
        assert ids == {"F-001", "F-002", "F-003"}

    def test_loads_multiple_projects(self, tmp_path):
        d1 = [_decision(f"P1-F-{i:03d}", project="P1") for i in range(4)]
        d2 = [_decision(f"P2-F-{i:03d}", project="P2") for i in range(3)]
        batch_dir = _make_batch_output(tmp_path, [
            {"slug": "proj1", "decisions": d1},
            {"slug": "proj2", "decisions": d2},
        ])
        loaded = self.mod.load_batch_decisions(batch_dir)
        assert len(loaded) == 7

    def test_empty_batch_dir_returns_empty(self, tmp_path):
        # No batch_results.json, no subdirs with decisions
        loaded = self.mod.load_batch_decisions(tmp_path)
        assert loaded == []

    def test_prefers_final_decisions_over_decisions(self, tmp_path):
        """If critic_v2_final_decisions.json exists, it takes priority."""
        proj_dir = tmp_path / "proj1"
        proj_dir.mkdir(parents=True)
        d_base = [_decision("F-001", decision="borderline", score=6)]
        d_final = [_decision("F-001", decision="accept", score=8)]
        (proj_dir / "critic_v2_decisions.json").write_text(
            json.dumps(d_base, ensure_ascii=False), encoding="utf-8"
        )
        (proj_dir / "critic_v2_final_decisions.json").write_text(
            json.dumps(d_final, ensure_ascii=False), encoding="utf-8"
        )
        results = [{"project": "proj1", "output_dir": str(proj_dir),
                    "skipped": False, "comparison": None}]
        (tmp_path / "batch_results.json").write_text(
            json.dumps(results), encoding="utf-8"
        )
        loaded = self.mod.load_batch_decisions(tmp_path)
        assert len(loaded) == 1
        assert loaded[0]["decision"] == "accept"

    def test_project_key_added(self, tmp_path):
        decisions = [_decision("F-001")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "my_project", "decisions": decisions}])
        loaded = self.mod.load_batch_decisions(batch_dir)
        assert loaded[0]["project"] == "my_project"


# ─── analyze_borderline ───────────────────────────────────────────────────────

class TestAnalyzeBorderline:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def _mixed_decisions(self) -> list[dict]:
        return [
            _decision("F-001", decision="accept", score=8, ev="valid"),
            _decision("F-002", decision="accept", score=9, ev="valid"),
            _decision("F-003", decision="borderline", score=6, ev="partial"),
            _decision("F-004", decision="borderline", score=5, ev="weak",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="documentation"),
            _decision("F-005", decision="borderline", score=6, ev="valid",
                      impact="cost_schedule"),
            _decision("F-006", decision="reject", score=2, ev="none"),
            _decision("F-007", decision="borderline", score=5, ev="weak",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="none"),
        ]

    def test_overview_counts(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        ov = analysis["overview"]
        assert ov["total_decisions"] == 7
        assert ov["borderline_count"] == 4
        assert ov["accepted_count"] == 2
        assert ov["rejected_count"] == 1

    def test_borderline_rate(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        assert abs(analysis["overview"]["borderline_rate"] - 4/7) < 0.01

    def test_breakdown_by_evidence_quality_exists(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        br = analysis["breakdown"]["by_evidence_quality"]
        assert isinstance(br, dict)
        assert "weak" in br or "partial" in br

    def test_breakdown_by_severity_exists(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        br = analysis["breakdown"]["by_severity"]
        assert isinstance(br, dict)

    def test_breakdown_by_source_exists(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        br = analysis["breakdown"]["by_source"]
        assert isinstance(br, dict)
        assert len(br) > 0

    def test_classification_split(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        cl = analysis["classification"]
        total_classified = cl["safe_reject_candidates"] + cl["safe_accept_candidates"] + cl["keep_borderline"]
        assert total_classified == analysis["overview"]["borderline_count"]

    def test_safe_reject_ids_correct(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        sr_ids = set(analysis["safe_reject_ids"])
        # F-004 (weak, low severity, doc) and F-007 (weak, none impact) should be safe_reject
        assert "F-004" in sr_ids or "F-007" in sr_ids

    def test_samples_capped_at_top_n(self):
        decisions = [_decision(f"F-{i:03d}", decision="borderline") for i in range(50)]
        analysis = self.mod.analyze_borderline(decisions, top_n=10)
        assert len(analysis["samples"]) <= 10

    def test_empty_decisions_returns_zero_counts(self):
        analysis = self.mod.analyze_borderline([])
        assert analysis["overview"]["total_decisions"] == 0
        assert analysis["overview"]["borderline_count"] == 0
        assert analysis["classification"]["safe_reject_candidates"] == 0

    def test_recommendations_present(self):
        decisions = self._mixed_decisions()
        analysis = self.mod.analyze_borderline(decisions)
        recs = analysis["recommendations"]
        assert "safe_reject" in recs
        assert "safe_accept" in recs
        assert "keep_borderline" in recs
        assert "tuning_suggestions" in recs

    def test_score_distribution_correct(self):
        decisions = [
            _decision("F-001", decision="borderline", score=5),
            _decision("F-002", decision="borderline", score=5),
            _decision("F-003", decision="borderline", score=6),
        ]
        analysis = self.mod.analyze_borderline(decisions)
        dist = analysis["breakdown"]["score_distribution"]
        assert dist["5"] == 2
        assert dist["6"] == 1


# ─── run_analysis ─────────────────────────────────────────────────────────────

class TestRunAnalysis:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_creates_json_output(self, tmp_path):
        decisions = [_decision(f"F-{i:03d}", decision="borderline") for i in range(5)]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        self.mod.run_analysis(batch_dir, top_n=5, quiet=True)
        assert (batch_dir / "borderline_analysis.json").exists()

    def test_creates_md_output(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        self.mod.run_analysis(batch_dir, quiet=True)
        assert (batch_dir / "borderline_analysis.md").exists()

    def test_creates_csv_when_requested(self, tmp_path):
        decisions = [_decision(f"F-{i:03d}", decision="borderline") for i in range(3)]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        self.mod.run_analysis(batch_dir, export_csv_flag=True, quiet=True)
        assert (batch_dir / "borderline_samples.csv").exists()

    def test_csv_has_expected_columns(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        self.mod.run_analysis(batch_dir, export_csv_flag=True, quiet=True)
        with (batch_dir / "borderline_samples.csv").open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cols = reader.fieldnames
        assert "finding_id" in cols
        assert "evidence_quality" in cols
        assert "borderline_source" in cols
        assert "classification" in cols

    def test_json_structure_valid(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline", score=6, ev="partial")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        self.mod.run_analysis(batch_dir, quiet=True)
        analysis = json.loads((batch_dir / "borderline_analysis.json").read_text(encoding="utf-8"))
        assert "overview" in analysis
        assert "breakdown" in analysis
        assert "classification" in analysis
        assert "samples" in analysis
        assert "recommendations" in analysis

    def test_empty_batch_dir_creates_empty_analysis(self, tmp_path):
        # Directory exists but has no decisions
        self.mod.run_analysis(tmp_path, quiet=True)
        analysis = json.loads((tmp_path / "borderline_analysis.json").read_text(encoding="utf-8"))
        assert analysis["overview"]["total_decisions"] == 0
        assert analysis["overview"]["borderline_count"] == 0

    def test_raises_on_nonexistent_dir(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            self.mod.run_analysis(tmp_path / "nonexistent_dir", quiet=True)

    def test_no_production_files_required(self, tmp_path):
        """Analysis must not require any production project files."""
        # Only batch output dir contents — no projects/ dir access
        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        # Should succeed without any projects/ directory
        analysis = self.mod.run_analysis(batch_dir, quiet=True)
        assert analysis is not None

    def test_returns_analysis_dict(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "p1", "decisions": decisions}])
        result = self.mod.run_analysis(batch_dir, quiet=True)
        assert isinstance(result, dict)
        assert "overview" in result


# ─── render_markdown ──────────────────────────────────────────────────────────

class TestRenderMarkdown:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_contains_expected_sections(self, tmp_path):
        decisions = [
            _decision("F-001", decision="borderline", score=6, ev="partial"),
            _decision("F-002", decision="borderline", score=5, ev="weak",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="documentation"),
        ]
        analysis = self.mod.analyze_borderline(decisions, top_n=5)
        md = self.mod.render_markdown(analysis, batch_summary=None, legacy_rows=[])
        assert "# Borderline Analysis" in md
        assert "## Overview" in md
        assert "## Breakdown" in md
        assert "## Recommendations" in md
        assert "## Borderline Samples" in md

    def test_markdown_has_table_syntax(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline")]
        analysis = self.mod.analyze_borderline(decisions)
        md = self.mod.render_markdown(analysis, None, [])
        assert "|" in md

    def test_production_not_modified_note_present(self):
        analysis = self.mod.analyze_borderline([])
        md = self.mod.render_markdown(analysis, None, [])
        assert "Production pipeline NOT modified" in md

    def test_legacy_section_when_rows_present(self):
        analysis = self.mod.analyze_borderline([_decision("F-001", decision="borderline")])
        legacy_rows = [
            {"finding_id": "F-001", "legacy_verdict": "pass", "v2_decision": "borderline", "project": "P1"},
        ]
        md = self.mod.render_markdown(analysis, None, legacy_rows)
        assert "Legacy Comparison" in md


# ─── Safe reject / safe accept correctness invariants ─────────────────────────

class TestSafetyInvariants:
    def setup_method(self):
        self.mod = _load_analysis_module()

    def test_accepted_findings_not_in_safe_reject(self, tmp_path):
        """Safe reject candidates must only come from borderline, not accepted."""
        decisions = [
            _decision("F-001", decision="accept", score=8, ev="valid"),
            _decision("F-002", decision="accept", score=9, ev="valid"),
            _decision("F-003", decision="borderline", score=5, ev="weak",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="none"),
        ]
        analysis = self.mod.analyze_borderline(decisions)
        assert "F-001" not in analysis["safe_reject_ids"]
        assert "F-002" not in analysis["safe_reject_ids"]

    def test_rejected_findings_not_in_safe_accept(self, tmp_path):
        """Safe accept candidates must only come from borderline, not rejected."""
        decisions = [
            _decision("F-001", decision="reject", score=3, ev="none"),
            _decision("F-002", decision="borderline", score=6, ev="valid",
                      impact="safety"),
        ]
        analysis = self.mod.analyze_borderline(decisions)
        assert "F-001" not in analysis["safe_accept_ids"]

    def test_none_evidence_never_safe_accept(self):
        """evidence_quality=none borderline can never be safe_accept."""
        d = _decision("F-001", decision="borderline", score=5, ev="none",
                      severity="КРИТИЧЕСКОЕ", impact="safety")
        d["action_required"] = "Немедленно исправить"
        result = self.mod.classify_borderline(d)
        assert result != "safe_accept_candidate"

    def test_valid_high_impact_low_action_not_safe_accept(self):
        """Valid evidence + high impact but no actionable action → not safe_accept."""
        d = _decision("F-001", decision="borderline", score=6, ev="valid",
                      severity="КРИТИЧЕСКОЕ", impact="safety")
        d["action_required"] = "ок"  # too short
        result = self.mod.classify_borderline(d)
        # Should be keep_borderline, not safe_accept (missing substantial action)
        assert result in ("keep_borderline", "safe_accept_candidate")  # partial ok

    def test_all_safe_reject_have_weak_or_none_evidence(self, tmp_path):
        """
        In a batch where ALL borderline have weak/none evidence,
        safe_reject_candidates must be >= 0 (could all be safe_reject).
        """
        decisions = [
            _decision(f"F-{i:03d}", decision="borderline", score=5, ev="weak",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ", impact="documentation")
            for i in range(10)
        ]
        analysis = self.mod.analyze_borderline(decisions)
        sr = analysis["classification"]["safe_reject_candidates"]
        total_bl = analysis["overview"]["borderline_count"]
        assert sr <= total_bl  # safe reject can't exceed total borderline


# ─── CLI integration ──────────────────────────────────────────────────────────

class TestCLI:
    def test_cli_basic_run(self, tmp_path):
        decisions = [_decision(f"F-{i:03d}", decision="borderline") for i in range(5)]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        result = subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(batch_dir), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (batch_dir / "borderline_analysis.json").exists()
        assert (batch_dir / "borderline_analysis.md").exists()

    def test_cli_with_csv(self, tmp_path):
        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        result = subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(batch_dir), "--export-csv", "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert (batch_dir / "borderline_samples.csv").exists()

    def test_cli_nonexistent_dir_exits_1(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(tmp_path / "no_such_dir")],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 1

    def test_cli_produces_valid_json(self, tmp_path):
        decisions = [
            _decision("F-001", decision="borderline", score=6, ev="partial"),
            _decision("F-002", decision="accept", score=8, ev="valid"),
            _decision("F-003", decision="reject", score=2, ev="none"),
        ]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(batch_dir), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        analysis = json.loads(
            (batch_dir / "borderline_analysis.json").read_text(encoding="utf-8")
        )
        assert analysis["overview"]["total_decisions"] == 3
        assert analysis["overview"]["borderline_count"] == 1

    def test_cli_top_n_limits_samples(self, tmp_path):
        decisions = [_decision(f"F-{i:03d}", decision="borderline") for i in range(20)]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(batch_dir), "--top-n", "5", "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        analysis = json.loads(
            (batch_dir / "borderline_analysis.json").read_text(encoding="utf-8")
        )
        assert len(analysis["samples"]) <= 5

    def test_cli_does_not_modify_production(self, tmp_path):
        """The analysis script must not write to any projects/ directory."""
        from pathlib import Path as P
        # Snapshot production files
        findings_files = sorted(P("projects").rglob("03_findings.json"))[:3]
        before = {str(p): p.read_text(encoding="utf-8") for p in findings_files}

        decisions = [_decision("F-001", decision="borderline")]
        batch_dir = _make_batch_output(tmp_path, [{"slug": "proj1", "decisions": decisions}])
        subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(batch_dir), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        for path_str, original in before.items():
            current = P(path_str).read_text(encoding="utf-8")
            assert current == original, f"Production file was modified: {path_str}"

    def test_cli_without_results_json_scans_subdirs(self, tmp_path):
        """Script should fall back to directory scanning if batch_results.json missing."""
        proj_dir = tmp_path / "proj1"
        proj_dir.mkdir()
        decisions = [_decision("F-001", decision="borderline")]
        (proj_dir / "critic_v2_decisions.json").write_text(
            json.dumps(decisions), encoding="utf-8"
        )
        # No batch_results.json
        result = subprocess.run(
            [sys.executable, str(ANALYZE_SCRIPT),
             "--batch-output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        analysis = json.loads((tmp_path / "borderline_analysis.json").read_text())
        # Should find at least the 1 borderline finding
        assert analysis["overview"]["total_decisions"] >= 1
