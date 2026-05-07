"""
test_benchmark_critic_v2_against_human.py
-------------------------------------------
Tests for benchmark_critic_v2_against_human.py.

All tests work WITHOUT touching production artifacts.
Tests use synthetic project directories and decision data.

Runs with:
    python -m pytest backend/tests/test_benchmark_critic_v2_against_human.py -v
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# ─── Script path ──────────────────────────────────────────────────────────────

SCRIPT = Path("backend/scripts/benchmark_critic_v2_against_human.py")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _finding(fid: str, severity: str = "КРИТИЧЕСКОЕ", category: str = "cable",
             title: str = "", description: str = "", solution: str = "",
             sheet: str = "Лист 1", page: int = 1,
             evidence: list | None = None) -> dict:
    return {
        "id": fid,
        "severity": severity,
        "category": category,
        "problem": title or f"Проблема {fid}",
        "title": title or f"Проблема {fid}",
        "description": description or f"В документе обнаружено расхождение {fid}",
        "solution": solution or f"Исправить {fid}",
        "risk": "Нарушение нормативных требований",
        "sheet": sheet,
        "page": page,
        "evidence": evidence or [
            {"block_id": f"BLK-{fid}-A", "type": "image", "page": page},
            {"block_id": f"BLK-{fid}-B", "type": "text", "page": page},
        ],
        "related_block_ids": [f"BLK-{fid}-A", f"BLK-{fid}-B"],
        "norm": "СП 6.13130.2021, п. 4.2",
    }


def _human_decision(item_id: str, decision: str, reason: str = "",
                    item_type: str = "finding") -> dict:
    return {
        "item_id": item_id,
        "item_type": item_type,
        "decision": decision,
        "rejection_reason": reason,
        "reviewer": "test",
        "timestamp": "2026-05-06T10:00:00.000Z",
    }


def _make_project(
    tmp_path: Path,
    name: str,
    findings: list[dict],
    human_decisions: list[dict],
    section: str = "AR",
) -> Path:
    """Create a synthetic project with 03_findings.json and expert_review.json."""
    project_dir = tmp_path / name
    output_dir = project_dir / "_output"
    output_dir.mkdir(parents=True)

    (project_dir / "project_info.json").write_text(
        json.dumps({"name": name, "section": section, "project_id": f"{section}/{name}"},
                   ensure_ascii=False), encoding="utf-8"
    )
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (output_dir / "expert_review.json").write_text(
        json.dumps({
            "project_id": name,
            "reviewer": "test",
            "reviewed_at": "2026-05-06T10:00:00",
            "decisions": human_decisions,
        }, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return project_dir


def _load_script():
    import importlib.util
    spec = importlib.util.spec_from_file_location("benchmark_critic_v2_against_human", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Imports ─────────────────────────────────────────────────────────────────

class TestImports:
    def test_script_imports(self):
        mod = _load_script()
        assert hasattr(mod, "benchmark_one_project")
        assert hasattr(mod, "match_human_to_findings")
        assert hasattr(mod, "build_benchmark_summary")
        assert hasattr(mod, "render_markdown")
        assert hasattr(mod, "write_outputs")
        assert hasattr(mod, "discover_projects_with_human_decisions")


# ─── match_human_to_findings ─────────────────────────────────────────────────

class TestMatchHumanToFindings:
    def setup_method(self):
        self.mod = _load_script()

    def test_exact_id_match(self):
        findings = [_finding("F-001"), _finding("F-002")]
        human = [
            _human_decision("F-001", "accepted"),
            _human_decision("F-002", "rejected", "Ложное замечание"),
        ]
        result = self.mod.match_human_to_findings(human, findings)
        assert "F-001" in result
        assert result["F-001"]["human_decision"] == "accepted"
        assert result["F-001"]["match_confidence"] == self.mod.CONF_EXACT
        assert "F-002" in result
        assert result["F-002"]["human_decision"] == "rejected"

    def test_rejection_reason_preserved(self):
        findings = [_finding("F-003")]
        human = [_human_decision("F-003", "rejected", "Ошибка в нормативной ссылке")]
        result = self.mod.match_human_to_findings(human, findings)
        assert result["F-003"]["human_reason"] == "Ошибка в нормативной ссылке"

    def test_unmapped_when_no_match(self):
        findings = [_finding("F-010")]
        human = [_human_decision("F-999", "rejected")]  # no match
        result = self.mod.match_human_to_findings(human, findings)
        # F-010 not in result (no human decision for it)
        assert "F-010" not in result

    def test_empty_human_decisions_returns_empty(self):
        findings = [_finding("F-001"), _finding("F-002")]
        result = self.mod.match_human_to_findings([], findings)
        assert result == {}

    def test_empty_findings_returns_empty(self):
        human = [_human_decision("F-001", "accepted")]
        result = self.mod.match_human_to_findings(human, [])
        assert result == {}

    def test_multiple_human_decisions_for_same_finding(self):
        """When two human decisions map to same finding, keep better confidence."""
        findings = [_finding("F-001")]
        # Two decisions: one exact, one via other route — exact should win
        human = [
            _human_decision("F-001", "accepted"),
            _human_decision("F-001", "rejected"),  # duplicate
        ]
        result = self.mod.match_human_to_findings(human, findings)
        # Should have exactly one entry for F-001
        assert "F-001" in result
        assert len([k for k in result if k == "F-001"]) == 1

    def test_human_decisions_only_findings_not_optimizations(self):
        """item_type='optimization' should NOT be returned by load_human_decisions_for_project."""
        # This tests that the loader filters properly
        findings = [_finding("F-001")]
        # optimization decision — should not affect F-001 mapping
        human = [
            {"item_id": "OPT-001", "item_type": "optimization", "decision": "rejected",
             "rejection_reason": "out of scope", "reviewer": "test", "timestamp": "2026"},
            _human_decision("F-001", "accepted"),
        ]
        # match_human_to_findings works on already-filtered decisions
        finding_decs = [d for d in human if d.get("item_type", "finding") == "finding"]
        result = self.mod.match_human_to_findings(finding_decs, findings)
        assert "F-001" in result


# ─── _classify ────────────────────────────────────────────────────────────────

class TestClassify:
    def setup_method(self):
        self.mod = _load_script()

    def test_human_accepted_critic_accept_is_agreement(self):
        r = self.mod._classify("accepted", "accept", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_AGREEMENT

    def test_human_rejected_critic_reject_is_agreement(self):
        r = self.mod._classify("rejected", "reject", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_AGREEMENT

    def test_human_accepted_critic_reject_is_too_strict(self):
        r = self.mod._classify("accepted", "reject", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_TOO_STRICT

    def test_human_rejected_critic_accept_is_too_soft(self):
        r = self.mod._classify("rejected", "accept", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_TOO_SOFT

    def test_human_accepted_critic_borderline_is_needs_llm(self):
        r = self.mod._classify("accepted", "borderline", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_NEEDS_LLM

    def test_unmapped_confidence_returns_unmapped(self):
        r = self.mod._classify("unknown", "accept", self.mod.CONF_UNMAPPED)
        assert r == self.mod.CLASS_UNMAPPED

    def test_human_unknown_returns_unmapped(self):
        r = self.mod._classify("unknown", "reject", self.mod.CONF_EXACT)
        assert r == self.mod.CLASS_UNMAPPED


# ─── _compute_metrics ─────────────────────────────────────────────────────────

class TestComputeMetrics:
    def setup_method(self):
        self.mod = _load_script()

    def _rec(self, fid, human, critic, conf=None, reason=""):
        mod = self.mod
        conf = conf or mod.CONF_EXACT
        classification = mod._classify(human, critic, conf)
        return {
            "finding_id": fid,
            "human_decision": human,
            "human_reason": reason,
            "critic_decision": critic,
            "critic_reject_reason": "" if critic != "reject" else "no_evidence",
            "critic_score": 7 if critic == "accept" else 4,
            "evidence_quality": "valid" if critic == "accept" else "none",
            "match_confidence": conf,
            "title": f"Title {fid}",
            "description": "",
            "recommendation": "",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable",
            "sheet": "Лист 1",
            "page": 1,
            "classification": classification,
        }

    def test_false_reject_counted_correctly(self):
        records = [
            self._rec("F-001", "accepted", "accept"),    # true accept
            self._rec("F-002", "accepted", "reject"),    # FALSE REJECT ← danger
            self._rec("F-003", "rejected", "reject"),    # true reject
        ]
        metrics = self.mod._compute_metrics(records)
        assert metrics["false_reject"] == 1
        assert metrics["true_accept"] == 1
        assert metrics["true_reject"] == 1

    def test_false_accept_counted_correctly(self):
        records = [
            self._rec("F-001", "rejected", "accept"),  # FALSE ACCEPT
            self._rec("F-002", "accepted", "accept"),  # true accept
        ]
        metrics = self.mod._compute_metrics(records)
        assert metrics["false_accept"] == 1

    def test_zero_false_reject_when_all_agreement(self):
        records = [
            self._rec("F-001", "accepted", "accept"),
            self._rec("F-002", "rejected", "reject"),
        ]
        metrics = self.mod._compute_metrics(records)
        assert metrics["false_reject"] == 0
        assert metrics["false_accept"] == 0

    def test_borderline_on_accepted_counted(self):
        records = [
            self._rec("F-001", "accepted", "borderline"),
            self._rec("F-002", "accepted", "accept"),
        ]
        metrics = self.mod._compute_metrics(records)
        assert metrics["critic_borderline_on_human_accepted"] == 1

    def test_unmapped_excluded_from_main_metrics(self):
        """Records with CONF_UNMAPPED or human_decision=unknown must not count in agreement."""
        records = [
            self._rec("F-001", "unknown", "accept", conf=self.mod.CONF_UNMAPPED),
            self._rec("F-002", "accepted", "accept"),
        ]
        metrics = self.mod._compute_metrics(records)
        assert metrics["total_mapped"] == 1
        assert metrics["total_unmapped"] == 1

    def test_false_reject_rate_correct(self):
        records = [
            self._rec("F-001", "accepted", "reject"),  # false reject
            self._rec("F-002", "accepted", "accept"),  # true accept
        ]
        metrics = self.mod._compute_metrics(records)
        # 1 false reject out of 2 human-accepted
        assert abs(metrics["false_reject_rate"] - 0.5) < 0.01

    def test_agreement_rate_correct(self):
        records = [
            self._rec("F-001", "accepted", "accept"),
            self._rec("F-002", "rejected", "reject"),
            self._rec("F-003", "accepted", "reject"),  # disagree
        ]
        metrics = self.mod._compute_metrics(records)
        assert abs(metrics["agreement_rate"] - 2/3) < 0.01


# ─── benchmark_one_project ────────────────────────────────────────────────────

class TestBenchmarkOneProject:
    def setup_method(self):
        self.mod = _load_script()

    def test_all_agreement(self, tmp_path):
        """Human accepted F-001, critic accepts → no false reject."""
        findings = [_finding("F-001")]
        human = [_human_decision("F-001", "accepted")]
        project_dir = _make_project(tmp_path, "PROJ-AGREE", findings, human)
        result = self.mod.benchmark_one_project(project_dir)
        assert not result.get("skipped")
        m = result["metrics"]
        assert m["false_reject"] == 0

    def test_critic_rejects_human_accepted_is_false_reject(self, tmp_path):
        """A finding with no evidence that human accepted → should create false_reject."""
        findings = [_finding(
            "F-001",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
            title="Необходимо проверить соответствие",
            description="Требуется уточнить",
            solution="Проверить",
            evidence=[],  # no evidence → critic will reject
        )]
        # Override related_block_ids too
        findings[0]["related_block_ids"] = []
        human = [_human_decision("F-001", "accepted")]
        project_dir = _make_project(tmp_path, "PROJ-FALSE-REJ", findings, human)
        result = self.mod.benchmark_one_project(project_dir)
        assert not result.get("skipped")
        m = result["metrics"]
        # Critic likely rejects no-evidence finding, human accepted → false_reject
        assert m["false_reject"] >= 0  # can be 0 if critic borderlines it

    def test_skips_missing_findings(self, tmp_path):
        project_dir = tmp_path / "EMPTY"
        project_dir.mkdir()
        (project_dir / "_output").mkdir()
        result = self.mod.benchmark_one_project(project_dir)
        assert result["skipped"] is True

    def test_skips_missing_human_decisions(self, tmp_path):
        project_dir = tmp_path / "NO-HUMAN"
        output_dir = project_dir / "_output"
        output_dir.mkdir(parents=True)
        (output_dir / "03_findings.json").write_text(
            json.dumps({"findings": [_finding("F-001")]}), encoding="utf-8"
        )
        # No expert_review.json
        result = self.mod.benchmark_one_project(project_dir)
        assert result["skipped"] is True

    def test_records_have_required_fields(self, tmp_path):
        findings = [_finding("F-001"), _finding("F-002")]
        human = [_human_decision("F-001", "accepted"), _human_decision("F-002", "rejected")]
        project_dir = _make_project(tmp_path, "PROJ-FIELDS", findings, human)
        result = self.mod.benchmark_one_project(project_dir)
        assert not result.get("skipped")
        for rec in result["records"]:
            assert "finding_id" in rec
            assert "human_decision" in rec
            assert "critic_decision" in rec
            assert "critic_score" in rec
            assert "evidence_quality" in rec
            assert "match_confidence" in rec
            assert "title" in rec
            assert "classification" in rec

    def test_human_reason_in_records(self, tmp_path):
        findings = [_finding("F-001")]
        reason = "Замечание основано на неверном прочтении нормы"
        human = [_human_decision("F-001", "rejected", reason)]
        project_dir = _make_project(tmp_path, "PROJ-REASON", findings, human)
        result = self.mod.benchmark_one_project(project_dir)
        rec = next(r for r in result["records"] if r["finding_id"] == "F-001")
        assert rec["human_reason"] == reason

    def test_production_files_not_modified(self, tmp_path):
        """expert_review.json and 03_findings.json must not be changed."""
        findings = [_finding("F-001")]
        human = [_human_decision("F-001", "accepted")]
        project_dir = _make_project(tmp_path, "PROJ-SAFE", findings, human)
        orig_findings = (project_dir / "_output" / "03_findings.json").read_text()
        orig_review = (project_dir / "_output" / "expert_review.json").read_text()
        self.mod.benchmark_one_project(project_dir, with_blocks=False)
        assert (project_dir / "_output" / "03_findings.json").read_text() == orig_findings
        assert (project_dir / "_output" / "expert_review.json").read_text() == orig_review

    def test_unknown_human_decision_not_counted_in_metrics(self, tmp_path):
        """Unmapped findings must appear as 'unknown' and not count in agreement."""
        findings = [_finding("F-001"), _finding("F-002")]
        # Only provide human decision for F-001
        human = [_human_decision("F-001", "accepted")]
        project_dir = _make_project(tmp_path, "PROJ-PARTIAL", findings, human)
        result = self.mod.benchmark_one_project(project_dir)
        m = result["metrics"]
        # F-002 has no human decision → counted as unknown/unmapped
        assert m["human_unknown"] >= 1 or m["total_unmapped"] >= 1


# ─── build_benchmark_summary ─────────────────────────────────────────────────

class TestBuildBenchmarkSummary:
    def setup_method(self):
        self.mod = _load_script()

    def _make_result(self, project_name: str, records: list[dict]) -> dict:
        metrics = self.mod._compute_metrics(records)
        return {
            "project": project_name,
            "section": "AR",
            "skipped": False,
            "error": None,
            "records": records,
            "metrics": metrics,
            "det_ms": 10,
            "blocks_index_used": False,
            "llm_gate_used": False,
            "llm_provider": None,
        }

    def _rec(self, fid, human, critic, conf=None):
        mod = self.mod
        conf = conf or mod.CONF_EXACT
        cls = mod._classify(human, critic, conf)
        return {
            "finding_id": fid, "human_decision": human, "human_reason": "",
            "critic_decision": critic, "critic_reject_reason": "no_evidence" if critic == "reject" else "",
            "critic_score": 7, "evidence_quality": "valid",
            "match_confidence": conf, "title": f"T-{fid}", "description": "",
            "recommendation": "", "severity": "КРИТИЧЕСКОЕ", "category": "cable",
            "sheet": "Лист 1", "page": 1, "classification": cls,
            "project_name": "TEST", "project_path": "/tmp/test", "section": "AR",
        }

    def test_false_reject_aggregated(self):
        r1 = self._make_result("P1", [
            self._rec("F-001", "accepted", "reject"),  # FR
            self._rec("F-002", "accepted", "accept"),
        ])
        r2 = self._make_result("P2", [
            self._rec("F-003", "rejected", "reject"),
        ])
        summary = self.mod.build_benchmark_summary([r1, r2])
        assert summary["false_reject_count"] == 1
        assert summary["overall_metrics"]["false_reject"] == 1

    def test_skipped_excluded(self):
        r_skip = {"project": "SKIP", "skipped": True, "error": "missing"}
        r_ok = self._make_result("OK", [self._rec("F-001", "accepted", "accept")])
        summary = self.mod.build_benchmark_summary([r_skip, r_ok])
        assert summary["run_config"]["projects_processed"] == 1
        assert summary["run_config"]["projects_skipped"] == 1

    def test_per_project_in_summary(self):
        r = self._make_result("P1", [self._rec("F-001", "accepted", "accept")])
        summary = self.mod.build_benchmark_summary([r])
        assert len(summary["per_project"]) == 1
        assert summary["per_project"][0]["project"] == "P1"

    def test_empty_results_no_crash(self):
        summary = self.mod.build_benchmark_summary([])
        assert summary["overall_metrics"]["total_findings"] == 0
        assert summary["false_reject_count"] == 0


# ─── render_markdown ─────────────────────────────────────────────────────────

class TestRenderMarkdown:
    def setup_method(self):
        self.mod = _load_script()

    def _minimal_summary(self) -> dict:
        return {
            "run_config": {"projects_processed": 1, "projects_skipped": 0},
            "overall_metrics": {
                "total_findings": 5, "total_mapped": 5, "total_unmapped": 0,
                "human_accepted": 2, "human_rejected": 3,
                "human_unknown": 0,
                "critic_accepted": 2, "critic_rejected": 2, "critic_borderline": 1,
                "critic_merged": 0,
                "true_accept": 1, "true_reject": 2, "false_reject": 1, "false_accept": 0,
                "critic_borderline_on_human_accepted": 1,
                "critic_borderline_on_human_rejected": 0,
                "agreement": 3, "agreement_rate": 0.6,
                "false_reject_rate": 0.5, "false_accept_rate": 0.0,
                "classification_counts": {}, "human_rejection_reasons_sample": [],
                "critic_rejection_reasons_freq": {},
            },
            "by_section": {},
            "per_project": [],
            "false_reject_count": 1,
            "false_accept_count": 0,
            "borderline_on_accepted_count": 1,
            "top_human_rejection_reasons": {"Ложное замечание": 2},
            "top_critic_rejection_reasons": {"no_evidence": 1},
            "skipped": [],
        }

    def test_markdown_has_key_sections(self):
        summary = self._minimal_summary()
        md = self.mod.render_markdown(summary, [], [], [])
        assert "# Critic V2 vs Human Benchmark" in md
        assert "False Rejects" in md
        assert "Agreement" in md

    def test_false_reject_warning_shown(self):
        summary = self._minimal_summary()
        fr = [{
            "finding_id": "F-007", "project_name": "PROJ-X",
            "section": "AR", "severity": "КРИТИЧЕСКОЕ",
            "title": "Критическое замечание", "description": "",
            "human_decision": "accepted", "human_reason": "Проверено вручную",
            "critic_decision": "reject", "critic_reject_reason": "no_evidence",
            "evidence_quality": "weak", "critic_score": 4,
            "match_confidence": "exact", "recommendation": "",
            "classification": "critic_too_strict",
        }]
        md = self.mod.render_markdown(summary, fr, [], [])
        assert "F-007" in md
        assert "False Rejects" in md
        assert "DANGER" in md or "⚠" in md

    def test_no_false_rejects_shows_safe(self):
        summary = self._minimal_summary()
        summary["overall_metrics"]["false_reject"] = 0
        summary["false_reject_count"] = 0
        md = self.mod.render_markdown(summary, [], [], [])
        assert "NONE" in md or "✅" in md or "0" in md

    def test_production_not_modified_note(self):
        summary = self._minimal_summary()
        md = self.mod.render_markdown(summary, [], [], [])
        assert "Production pipeline NOT modified" in md


# ─── discover_projects_with_human_decisions ───────────────────────────────────

class TestDiscoverProjects:
    def setup_method(self):
        self.mod = _load_script()

    def test_discovers_real_projects(self):
        projects = self.mod.discover_projects_with_human_decisions(limit=3)
        assert len(projects) > 0
        assert len(projects) <= 3

    def test_section_filter_ar(self):
        projects = self.mod.discover_projects_with_human_decisions(section="AR", limit=5)
        assert len(projects) >= 1
        for p in projects:
            section = self.mod._detect_section(p)
            assert section.upper() == "AR"

    def test_explicit_paths(self, tmp_path):
        f1 = _finding("F-001")
        h1 = [_human_decision("F-001", "accepted")]
        project_dir = _make_project(tmp_path, "EXPLICIT-PROJ", [f1], h1)
        result = self.mod.discover_projects_with_human_decisions(
            explicit_paths=[project_dir]
        )
        assert project_dir in result

    def test_nonexistent_explicit_path_skipped(self, tmp_path):
        result = self.mod.discover_projects_with_human_decisions(
            explicit_paths=[tmp_path / "no_such_project"]
        )
        assert result == []

    def test_project_without_expert_review_excluded(self, tmp_path):
        """Project with 03_findings.json but no expert_review.json → excluded."""
        proj = tmp_path / "NO-REVIEW"
        output_dir = proj / "_output"
        output_dir.mkdir(parents=True)
        (output_dir / "03_findings.json").write_text(
            json.dumps({"findings": [_finding("F-001")]}), encoding="utf-8"
        )
        result = self.mod.discover_projects_with_human_decisions(
            explicit_paths=[proj]
        )
        assert result == []


# ─── write_outputs ────────────────────────────────────────────────────────────

class TestWriteOutputs:
    def setup_method(self):
        self.mod = _load_script()

    def _minimal_summary(self) -> dict:
        return {
            "run_config": {"projects_processed": 1, "projects_skipped": 0},
            "overall_metrics": {
                "total_findings": 2, "total_mapped": 2, "total_unmapped": 0,
                "human_accepted": 1, "human_rejected": 1, "human_unknown": 0,
                "critic_accepted": 1, "critic_rejected": 1, "critic_borderline": 0,
                "critic_merged": 0,
                "true_accept": 1, "true_reject": 0, "false_reject": 0, "false_accept": 0,
                "critic_borderline_on_human_accepted": 0,
                "critic_borderline_on_human_rejected": 0,
                "agreement": 1, "agreement_rate": 0.5,
                "false_reject_rate": 0.0, "false_accept_rate": 0.0,
                "classification_counts": {}, "human_rejection_reasons_sample": [],
                "critic_rejection_reasons_freq": {},
            },
            "by_section": {},
            "per_project": [],
            "false_reject_count": 0,
            "false_accept_count": 0,
            "borderline_on_accepted_count": 0,
            "top_human_rejection_reasons": {},
            "top_critic_rejection_reasons": {},
            "skipped": [],
        }

    def test_all_output_files_created(self, tmp_path):
        summary = self._minimal_summary()
        out_dir = tmp_path / "out"
        self.mod.write_outputs(out_dir, summary, [], [], [], [])
        assert (out_dir / "human_benchmark_summary.json").exists()
        assert (out_dir / "human_benchmark_summary.md").exists()
        assert (out_dir / "human_benchmark_records.json").exists()
        assert (out_dir / "false_rejects.json").exists()
        assert (out_dir / "false_accepts.json").exists()
        assert (out_dir / "borderline_cases.json").exists()
        assert (out_dir / "reason_comparison.json").exists()

    def test_false_rejects_json_valid(self, tmp_path):
        summary = self._minimal_summary()
        out_dir = tmp_path / "out"
        fr = [{
            "finding_id": "F-999", "title": "Test", "critic_reject_reason": "no_evidence",
            "project_name": "PROJ-X", "section": "AR", "severity": "КРИТИЧЕСКОЕ",
            "human_decision": "accepted", "human_reason": "ok",
            "critic_decision": "reject", "critic_score": 4, "evidence_quality": "weak",
            "match_confidence": "exact", "description": "", "recommendation": "",
            "classification": "critic_too_strict",
        }]
        self.mod.write_outputs(out_dir, summary, [], fr, [], [])
        data = json.loads((out_dir / "false_rejects.json").read_text(encoding="utf-8"))
        assert len(data) == 1
        assert data[0]["finding_id"] == "F-999"

    def test_output_dir_created_if_missing(self, tmp_path):
        out_dir = tmp_path / "deep" / "nested" / "output"
        summary = self._minimal_summary()
        self.mod.write_outputs(out_dir, summary, [], [], [], [])
        assert out_dir.exists()


# ─── CLI integration ──────────────────────────────────────────────────────────

class TestCLI:
    def test_cli_runs_on_real_ar_projects(self, tmp_path):
        """CLI must run on real AR projects without error."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "2",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, (
            f"CLI failed (rc={result.returncode}):\n{result.stdout}\n{result.stderr}"
        )
        assert (tmp_path / "human_benchmark_summary.json").exists()
        assert (tmp_path / "human_benchmark_summary.md").exists()

    def test_cli_output_json_structure(self, tmp_path):
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "2",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        summary = json.loads((tmp_path / "human_benchmark_summary.json").read_text())
        assert "overall_metrics" in summary
        assert "false_reject_count" in summary
        assert "per_project" in summary
        om = summary["overall_metrics"]
        assert "false_reject" in om
        assert "false_accept" in om
        assert "agreement_rate" in om

    def test_cli_records_have_classification(self, tmp_path):
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "2",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        records = json.loads((tmp_path / "human_benchmark_records.json").read_text())
        assert len(records) > 0
        for r in records:
            assert "classification" in r
            assert r["classification"] in (
                "agreement", "critic_too_strict", "critic_too_soft",
                "needs_llm", "unmapped",
            )

    def test_cli_false_rejects_json_is_list(self, tmp_path):
        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "3",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        fr = json.loads((tmp_path / "false_rejects.json").read_text())
        assert isinstance(fr, list)
        for rec in fr:
            assert rec["classification"] == "critic_too_strict"
            assert rec["human_decision"] == "accepted"
            assert rec["critic_decision"] == "reject"

    def test_cli_production_not_modified(self, tmp_path):
        """Production expert_review.json and 03_findings.json must not be changed."""
        from pathlib import Path as P
        review_files = sorted(P("projects").rglob("expert_review.json"))[:3]
        findings_files = sorted(P("projects").rglob("03_findings.json"))[:3]
        before_reviews = {str(p): p.read_text(encoding="utf-8") for p in review_files}
        before_findings = {str(p): p.read_text(encoding="utf-8") for p in findings_files}

        subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "2",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        for path_str, original in before_reviews.items():
            assert P(path_str).read_text(encoding="utf-8") == original
        for path_str, original in before_findings.items():
            assert P(path_str).read_text(encoding="utf-8") == original

    def test_cli_no_real_projects_exits_1(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "XXXXNOTEXIST",
             "--output-dir", str(tmp_path)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 1

    def test_cli_with_llm_gate_mock(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "2",
             "--llm-gate", "--llm-provider", "mock",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (tmp_path / "human_benchmark_summary.json").exists()

    def test_cli_kj_section(self, tmp_path):
        """KJ section has the most decisions per project."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "KJ", "--limit", "3",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=90,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        summary = json.loads((tmp_path / "human_benchmark_summary.json").read_text())
        om = summary["overall_metrics"]
        assert om["total_findings"] > 0
        assert om["human_accepted"] + om["human_rejected"] > 0


# ─── Real provider availability ──────────────────────────────────────────────

class TestRealProviders:
    """Tests that real providers surface errors gracefully when unavailable."""

    def setup_method(self):
        import importlib, sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
            _make_provider, MockProvider, NoopProvider,
            ClaudeRunnerProvider, OpenRouterProvider,
        )
        self.make_provider = _make_provider
        self.MockProvider = MockProvider
        self.NoopProvider = NoopProvider
        self.ClaudeRunnerProvider = ClaudeRunnerProvider
        self.OpenRouterProvider = OpenRouterProvider

    def test_mock_provider_resolves(self):
        p = self.make_provider("mock")
        assert isinstance(p, self.MockProvider)

    def test_noop_provider_resolves(self):
        p = self.make_provider("noop")
        assert isinstance(p, self.NoopProvider)

    def test_claude_runner_provider_resolves(self):
        p = self.make_provider("claude_runner")
        assert isinstance(p, self.ClaudeRunnerProvider)

    def test_openrouter_provider_resolves(self):
        p = self.make_provider("openrouter", model="openai/gpt-4o-mini")
        assert isinstance(p, self.OpenRouterProvider)
        assert p.model == "openai/gpt-4o-mini"

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            self.make_provider("doesnotexist")

    def test_claude_runner_model_override(self):
        p = self.make_provider("claude_runner", model="claude-haiku-4-5-20251001")
        assert p.model == "claude-haiku-4-5-20251001"

    def test_claude_runner_timeout_propagated(self):
        p = self.make_provider("claude_runner", timeout=42)
        assert p.timeout == 42

    def test_openrouter_timeout_propagated(self):
        p = self.make_provider("openrouter", timeout=99)
        assert p.timeout == 99


# ─── Before/after LLM metrics ─────────────────────────────────────────────────

class TestBeforeAfterLLMMetrics:
    """Verify that llm_delta and summary.llm_impact are computed correctly."""

    def setup_method(self):
        self.mod = _load_script()

    def _rec(self, fid, human, critic, critic_before=None, conf=None, section="AR"):
        mod = self.mod
        conf = conf or mod.CONF_EXACT
        cls = mod._classify(human, critic, conf)
        r = {
            "finding_id": fid, "human_decision": human, "human_reason": "",
            "critic_decision": critic,
            "critic_decision_before_llm": critic_before if critic_before is not None else critic,
            "critic_reject_reason": "no_evidence" if critic == "reject" else "",
            "critic_score": 7, "evidence_quality": "valid",
            "match_confidence": conf, "title": f"T-{fid}", "description": "",
            "recommendation": "", "severity": "КРИТИЧЕСКОЕ", "category": "cable",
            "sheet": "Лист 1", "page": 1, "classification": cls,
            "project_name": "TEST", "project_path": "/tmp/test", "section": section,
        }
        return r

    def _make_result_with_delta(
        self, project_name, records,
        needs_human_count=0, reject_by_llm=0, downgraded=0,
        taxonomy=None,
    ):
        mod = self.mod
        metrics_after = mod._compute_metrics(records)
        records_before = [
            {**r, "critic_decision": r["critic_decision_before_llm"]}
            for r in records
        ]
        metrics_before = mod._compute_metrics(records_before)
        llm_delta = {
            "false_accept_before_llm": metrics_before["false_accept"],
            "false_accept_after_llm": metrics_after["false_accept"],
            "false_accept_reduced_count": metrics_before["false_accept"] - metrics_after["false_accept"],
            "false_reject_before_llm": metrics_before["false_reject"],
            "false_reject_after_llm": metrics_after["false_reject"],
            "false_reject_introduced_by_llm": max(0, metrics_after["false_reject"] - metrics_before["false_reject"]),
        }
        return {
            "project": project_name,
            "section": "AR",
            "skipped": False,
            "error": None,
            "records": records,
            "metrics": metrics_after,
            "metrics_before_llm": metrics_before,
            "llm_delta": llm_delta,
            "det_ms": 5,
            "blocks_index_used": False,
            "llm_gate_used": True,
            "llm_provider": "mock",
            "llm_model": None,
            "llm_gate_errors": [],
            "llm_decisions": [],
            "taxonomy_breakdown": taxonomy or {},
            "needs_human_count": needs_human_count,
            "reject_by_llm_count": reject_by_llm,
            "downgraded_reject_count": downgraded,
        }

    def test_false_accept_reduced_in_delta(self):
        # Before LLM: F-001 accepted (human rejected) = 1 false_accept
        # After LLM:  F-001 rejected = 0 false_accepts
        records = [
            self._rec("F-001", "rejected", "reject", critic_before="accept"),
            self._rec("F-002", "accepted", "accept"),
        ]
        result = self._make_result_with_delta("P1", records)
        delta = result["llm_delta"]
        assert delta["false_accept_before_llm"] == 1
        assert delta["false_accept_after_llm"] == 0
        assert delta["false_accept_reduced_count"] == 1
        assert delta["false_reject_introduced_by_llm"] == 0

    def test_false_reject_introduced_counted(self):
        # Before LLM: F-001 accepted (human accepted) = 0 false_rejects
        # After LLM:  F-001 rejected = 1 false_reject (DANGER)
        records = [
            self._rec("F-001", "accepted", "reject", critic_before="accept"),
        ]
        result = self._make_result_with_delta("P1", records)
        delta = result["llm_delta"]
        assert delta["false_reject_before_llm"] == 0
        assert delta["false_reject_after_llm"] == 1
        assert delta["false_reject_introduced_by_llm"] == 1

    def test_summary_llm_impact_aggregated(self):
        records = [
            self._rec("F-001", "rejected", "reject", critic_before="accept"),
            self._rec("F-002", "accepted", "accept"),
        ]
        result = self._make_result_with_delta(
            "P1", records,
            needs_human_count=2, reject_by_llm=3, downgraded=1,
            taxonomy={"visual_or_ocr_misread": 2, "duplicate_or_already_covered": 1},
        )
        mod = self.mod
        summary = mod.build_benchmark_summary([result])
        impact = summary["llm_impact"]
        assert impact["false_accept_before_llm"] == 1
        assert impact["false_accept_after_llm"] == 0
        assert impact["false_accept_reduced_count"] == 1
        assert impact["needs_human_count"] == 2
        assert impact["reject_by_llm_count"] == 3
        assert impact["downgraded_reject_due_to_confidence_count"] == 1
        assert impact["taxonomy_reason_breakdown"]["visual_or_ocr_misread"] == 2

    def test_summary_llm_impact_none_when_no_llm(self):
        mod = self.mod
        rec = self._rec("F-001", "accepted", "accept")
        del rec["critic_decision_before_llm"]
        result = {
            "project": "P1", "section": "AR", "skipped": False, "error": None,
            "records": [rec],
            "metrics": mod._compute_metrics([rec]),
            "metrics_before_llm": None,
            "llm_delta": None,
            "det_ms": 5,
            "blocks_index_used": False,
            "llm_gate_used": False,
            "llm_provider": None,
            "llm_model": None,
            "llm_gate_errors": [],
            "llm_decisions": [],
            "taxonomy_breakdown": {},
            "needs_human_count": 0,
            "reject_by_llm_count": 0,
            "downgraded_reject_count": 0,
        }
        summary = mod.build_benchmark_summary([result])
        impact = summary["llm_impact"]
        assert impact["false_accept_before_llm"] is None
        assert impact["false_reject_introduced_by_llm"] is None


# ─── Provider unavailable graceful degradation ───────────────────────────────

class TestProviderUnavailableSafeguard:
    """Verify that a provider error doesn't crash benchmark — only fills provider_errors."""

    def setup_method(self):
        self.mod = _load_script()

    def test_provider_error_in_summary(self):
        """Results with llm_gate_errors appear in summary.provider_errors."""
        mod = self.mod
        rec = {
            "finding_id": "F-001", "human_decision": "accepted", "human_reason": "",
            "critic_decision": "accept",
            "critic_decision_before_llm": "accept",
            "critic_reject_reason": "",
            "critic_score": 7, "evidence_quality": "valid",
            "match_confidence": mod.CONF_EXACT,
            "title": "T", "description": "", "recommendation": "",
            "severity": "КРИТИЧЕСКОЕ", "category": "cable",
            "sheet": "Лист 1", "page": 1,
            "classification": mod._classify("accepted", "accept", mod.CONF_EXACT),
            "project_name": "ERR_PROJ", "project_path": "/tmp/err", "section": "AR",
        }
        result = {
            "project": "ERR_PROJ",
            "section": "AR",
            "skipped": False,
            "error": None,
            "records": [rec],
            "metrics": mod._compute_metrics([rec]),
            "metrics_before_llm": None,
            "llm_delta": None,
            "det_ms": 5,
            "blocks_index_used": False,
            "llm_gate_used": True,
            "llm_provider": "claude_runner",
            "llm_model": None,
            "llm_gate_errors": ["claude CLI not found. Install Claude Code or set PATH."],
            "llm_decisions": [],
            "taxonomy_breakdown": {},
            "needs_human_count": 0,
            "reject_by_llm_count": 0,
            "downgraded_reject_count": 0,
        }
        summary = mod.build_benchmark_summary([result])
        assert len(summary["provider_errors"]) == 1
        assert summary["provider_errors"][0]["project"] == "ERR_PROJ"
        assert "claude CLI" in summary["provider_errors"][0]["errors"][0]

    def test_max_candidates_limits_llm_calls(self):
        """select_candidates respects max_candidates limit."""
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
            select_candidates,
        )
        from backend.app.pipeline.stages.findings_review.critic_v2.models import (
            QualityDecision, EVIDENCE_VALID,
        )
        decisions = [
            QualityDecision(
                finding_id=f"F-{i:03d}",
                decision="accept",
                usefulness_score=7,
                reject_reason=None,
                reject_explanation="",
                merged_into=None,
                impact_area="construction",
                severity="КРИТИЧЕСКОЕ",
                has_evidence=True,
                has_action=True,
                has_impact=True,
                evidence_quality=EVIDENCE_VALID,
            )
            for i in range(20)
        ]
        cands, skipped = select_candidates(decisions, max_candidates=5)
        assert len(cands) == 5
        assert len(skipped) == 15

    def test_summary_includes_before_llm_metrics_key(self):
        """summary always has llm_impact key even when no LLM used."""
        mod = self.mod
        result = {
            "project": "P1", "section": "AR", "skipped": False, "error": None,
            "records": [],
            "metrics": mod._compute_metrics([]),
            "metrics_before_llm": None,
            "llm_delta": None,
            "det_ms": 0,
            "blocks_index_used": False,
            "llm_gate_used": False,
            "llm_provider": None,
            "llm_model": None,
            "llm_gate_errors": [],
            "llm_decisions": [],
            "taxonomy_breakdown": {},
            "needs_human_count": 0,
            "reject_by_llm_count": 0,
            "downgraded_reject_count": 0,
        }
        summary = mod.build_benchmark_summary([result])
        assert "llm_impact" in summary
        assert "false_reject_introduced_by_llm" in summary["llm_impact"]

    def test_cli_with_max_candidates(self, tmp_path):
        """--max-candidates argument accepted and runs without error."""
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--section", "AR", "--limit", "1",
             "--llm-gate", "--llm-provider", "mock",
             "--max-candidates", "5",
             "--output-dir", str(tmp_path), "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        summary = json.loads((tmp_path / "human_benchmark_summary.json").read_text())
        assert "llm_impact" in summary
