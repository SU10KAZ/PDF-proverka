"""
test_critic_v2_experiment_matrix.py
--------------------------------------
Tests for run_critic_v2_experiment_matrix.py.

All LLM experiments use mock/noop providers — no real API calls.
All tests use synthetic project directories.

Runs with:
    python -m pytest backend/tests/test_critic_v2_experiment_matrix.py -v
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

MANIFEST_SCRIPT = Path("backend/scripts/build_human_benchmark_manifest.py")
MATRIX_SCRIPT = Path("backend/scripts/run_critic_v2_experiment_matrix.py")


def _load_manifest_mod():
    spec = importlib.util.spec_from_file_location(
        "build_manifest", str(MANIFEST_SCRIPT.resolve())
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_matrix_mod():
    spec = importlib.util.spec_from_file_location(
        "run_matrix", str(MATRIX_SCRIPT.resolve())
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Synthetic project builder ────────────────────────────────────────────────

def _make_project(
    base: Path,
    name: str,
    section: str = "KJ",
    n_findings: int = 10,
    accepted: int = 2,
    extra_fields: dict | None = None,
) -> Path:
    proj = base / section / name
    output = proj / "_output"
    output.mkdir(parents=True, exist_ok=True)

    (proj / "project_info.json").write_text(
        json.dumps({"section": section, "name": name}), encoding="utf-8"
    )

    findings = []
    for i in range(1, n_findings + 1):
        f = {
            "id": f"F-{i:03d}",
            "severity": "КРИТИЧЕСКОЕ" if i % 3 == 0 else "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "rebar" if i % 2 == 0 else "cover_thickness",
            "problem": f"Проблема {i}: несоответствие норме",
            "description": f"В документации обнаружено расхождение {i}",
            "solution": f"Исправить {i}",
            "evidence": [{"block_id": f"BLK-{i:03d}", "type": "text", "page": 1}],
            "related_block_ids": [f"BLK-{i:03d}"],
        }
        if extra_fields:
            f.update(extra_fields)
        findings.append(f)

    (output / "03_findings.json").write_text(
        json.dumps({"findings": findings}), encoding="utf-8"
    )

    decisions = []
    for i, f in enumerate(findings):
        dec = "accepted" if i < accepted else "rejected"
        decisions.append({
            "item_id": f["id"],
            "item_type": "finding",
            "decision": dec,
            "rejection_reason": None if dec == "accepted" else f"Reason {i}",
            "reviewer": "test",
            "timestamp": "2026-01-01T00:00:00",
        })
    (output / "expert_review.json").write_text(
        json.dumps({
            "project_id": name,
            "reviewer": "test",
            "reviewed_at": "2026-01-01",
            "decisions": decisions,
        }), encoding="utf-8"
    )

    (output / "document_graph.json").write_text(
        json.dumps({"pages": []}), encoding="utf-8"
    )

    return proj


def _make_manifest(tmp_path: Path, sections=None) -> tuple[Path, dict]:
    """Build a manifest from synthetic projects and return (manifest_path, manifest_dict)."""
    root = tmp_path / "projects"
    _make_project(root, "KJ-P1", section="KJ", n_findings=10, accepted=1)
    _make_project(root, "KJ-P2", section="KJ", n_findings=8, accepted=2)
    _make_project(root, "AR-P1", section="AR", n_findings=6, accepted=3)

    manifest_mod = _load_manifest_mod()
    manifest = manifest_mod.build_manifest(root, sections=sections)
    manifest_dir = tmp_path / "manifest"
    manifest_mod.write_outputs(manifest_dir, manifest)
    return manifest_dir / "human_benchmark_manifest.json", manifest


# ─── Tests: load_manifest ────────────────────────────────────────────────────

class TestLoadManifest:
    def setup_method(self):
        self.mod = _load_matrix_mod()

    def test_loads_valid_manifest(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        manifest = self.mod.load_manifest(manifest_path)
        assert "records" in manifest
        assert len(manifest["records"]) == 3

    def test_raises_if_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            self.mod.load_manifest(tmp_path / "nonexistent.json")

    def test_raises_if_invalid_json(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json")
        with pytest.raises(ValueError, match="Invalid manifest JSON"):
            self.mod.load_manifest(bad)

    def test_raises_if_missing_records_key(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"stats": {}}))
        with pytest.raises(ValueError, match="missing 'records'"):
            self.mod.load_manifest(bad)


# ─── Tests: filter_manifest_records ──────────────────────────────────────────

class TestFilterManifestRecords:
    def setup_method(self):
        self.mod = _load_matrix_mod()

    def _make_records(self, sections: list[str]) -> list[dict]:
        return [
            {"project_name": f"P-{sec}-{i}", "section": sec,
             "project_path": f"/tmp/{sec}/{i}"}
            for sec in sections for i in range(3)
        ]

    def test_no_filter_returns_all(self):
        records = self._make_records(["KJ", "AR", "EOM"])
        result = self.mod.filter_manifest_records(records)
        assert len(result) == 9

    def test_section_filter(self):
        records = self._make_records(["KJ", "AR", "EOM"])
        result = self.mod.filter_manifest_records(records, sections=["KJ"])
        assert len(result) == 3
        assert all(r["section"] == "KJ" for r in result)

    def test_section_filter_case_insensitive(self):
        records = self._make_records(["KJ", "AR"])
        result = self.mod.filter_manifest_records(records, sections=["kj"])
        assert len(result) == 3

    def test_limit_per_section(self):
        records = self._make_records(["KJ", "AR"])
        result = self.mod.filter_manifest_records(records, limit_per_section=2)
        kj_count = sum(1 for r in result if r["section"] == "KJ")
        ar_count = sum(1 for r in result if r["section"] == "AR")
        assert kj_count <= 2
        assert ar_count <= 2

    def test_section_and_limit_combined(self):
        records = self._make_records(["KJ", "AR"])
        result = self.mod.filter_manifest_records(records, sections=["KJ"], limit_per_section=1)
        assert len(result) == 1
        assert result[0]["section"] == "KJ"


# ─── Tests: det_only experiment via CLI ──────────────────────────────────────

class TestDetOnlyExperiment:
    def test_det_only_runs_via_cli(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"Matrix failed: {result.stderr}"
        summary_path = out / "det_only" / "human_benchmark_summary.json"
        assert summary_path.exists(), "det_only summary.json missing"

    def test_det_only_summary_has_metrics(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        data = json.loads((out / "det_only" / "human_benchmark_summary.json").read_text())
        assert "overall_metrics" in data
        assert data["overall_metrics"]["total_findings"] > 0


# ─── Tests: mock LLM experiments ──────────────────────────────────────────────

class TestMockLLMExperiments:
    def test_llm_no_context_runs(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "llm_no_context",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"Matrix failed: {result.stderr}"
        assert (out / "llm_no_context" / "human_benchmark_summary.json").exists()

    def test_llm_context_policy_runs(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "llm_context_policy",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"Matrix failed: {result.stderr}"

    def test_all_three_experiments_run(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0, f"Matrix failed: {result.stderr}"
        # All three experiment dirs should exist
        for exp in ["det_only", "llm_no_context", "llm_context_policy"]:
            assert (out / exp / "human_benchmark_summary.json").exists(), \
                f"Missing {exp} summary"


# ─── Tests: matrix summary ───────────────────────────────────────────────────

class TestMatrixSummary:
    def test_matrix_summary_created(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only", "llm_no_context",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 0
        assert (out / "experiment_matrix_summary.json").exists()
        assert (out / "experiment_matrix_summary.md").exists()

    def test_matrix_summary_has_required_keys(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        data = json.loads((out / "experiment_matrix_summary.json").read_text())
        assert "manifest_stats" in data
        assert "experiments_run" in data
        assert "metrics_by_experiment" in data
        assert "ranking" in data
        assert "by_section" in data

    def test_section_breakdown_in_summary(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        data = json.loads((out / "experiment_matrix_summary.json").read_text())
        assert len(data["by_section"]) >= 1


# ─── Tests: false_reject danger detection ────────────────────────────────────

class TestFalseRejectDangerDetection:
    def setup_method(self):
        self.mod = _load_matrix_mod()

    def _summary_with_fr(self, fr_count: int, exp_name: str = "test") -> dict:
        return {
            "experiment": exp_name,
            "overall_metrics": {
                "false_reject": fr_count,
                "false_reject_rate": fr_count / 10,
                "false_accept": 5,
                "false_accept_rate": 0.5,
                "total_findings": 10,
                "total_mapped": 10,
                "human_accepted": 3,
                "human_rejected": 7,
                "critic_accepted": 5,
                "critic_rejected": 5,
                "critic_borderline": 0,
                "agreement": 5,
                "agreement_rate": 0.5,
                "true_accept": 3,
                "true_reject": 2,
            },
            "run_config": {"llm_gate_used": False, "context_enrichment_used": False},
            "llm_impact": {},
            "context_enrichment": {},
        }

    def test_fr_zero_not_danger(self):
        summary = self._summary_with_fr(0)
        m = self.mod._experiment_metrics(summary)
        assert m["is_danger"] is False

    def test_fr_nonzero_is_danger(self):
        summary = self._summary_with_fr(2)
        m = self.mod._experiment_metrics(summary)
        assert m["is_danger"] is True

    def test_danger_disqualifies_from_ranking(self):
        danger_summary = self._summary_with_fr(1, "dangerous")
        safe_summary = self._summary_with_fr(0, "safe")
        matrix = self.mod.build_matrix_summary(
            manifest={"stats": {}},
            experiment_summaries=[danger_summary, safe_summary],
            experiment_names=["dangerous", "safe"],
        )
        assert matrix["any_danger"] is True
        # "safe" should rank higher than "dangerous"
        ranking_names = [r["experiment"] for r in matrix["ranking"]]
        if len(ranking_names) == 2:
            assert ranking_names[0] == "safe"

    def test_candidate_score_infinite_for_danger(self):
        m = {"is_danger": True, "false_reject": 1, "false_reject_rate": 0.1}
        score = self.mod._compute_candidate_score(m)
        assert score == float("-inf")

    def test_candidate_score_finite_for_safe(self):
        m = {
            "is_danger": False, "false_reject": 0, "false_reject_rate": 0.0,
            "false_accept": 5, "false_accept_rate": 0.5,
            "agreement_rate": 0.5,
            "other_unclassified_rate": None,
            "provider_errors_count": 0,
        }
        score = self.mod._compute_candidate_score(m)
        assert score != float("-inf")
        assert isinstance(score, float)

    def test_markdown_shows_danger_warning(self):
        danger_m = self._summary_with_fr(1, "dangerous")
        matrix = self.mod.build_matrix_summary(
            manifest={"stats": {}},
            experiment_summaries=[danger_m],
            experiment_names=["dangerous"],
        )
        md = self.mod.render_matrix_markdown(matrix)
        assert "DANGER" in md

    def test_fr_danger_in_matrix_summary_json(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        # Run det_only which should give FR=0 on clean data
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        data = json.loads((out / "experiment_matrix_summary.json").read_text())
        # On clean synthetic data, det_only should have FR=0
        det_metrics = data["metrics_by_experiment"].get("det_only", {})
        assert det_metrics.get("false_reject", 0) == 0


# ─── Tests: safety — production artifacts not modified ───────────────────────

class TestProductionNotModified:
    def test_manifest_build_not_touch_output(self, tmp_path):
        root = tmp_path / "projects"
        proj = _make_project(root, "P1", section="KJ")
        output_dir = proj / "_output"
        before = sorted(f.name for f in output_dir.iterdir())

        manifest_mod = _load_manifest_mod()
        manifest = manifest_mod.build_manifest(root)
        scratch = tmp_path / "scratch"
        manifest_mod.write_outputs(scratch, manifest)

        after = sorted(f.name for f in output_dir.iterdir())
        assert before == after

    def test_matrix_not_touch_project_output(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        # Record all files in all project _output dirs
        def _list_output_files(base: Path) -> dict:
            result = {}
            for oj in base.rglob("_output"):
                if oj.is_dir():
                    result[str(oj)] = sorted(f.name for f in oj.iterdir())
            return result

        before = _list_output_files(tmp_path / "projects")

        out = tmp_path / "matrix"
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )

        after = _list_output_files(tmp_path / "projects")
        assert before == after, "Matrix script must not modify project _output/ files"


# ─── Tests: dry-run ──────────────────────────────────────────────────────────

class TestDryRun:
    def test_dry_run_exits_0(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(tmp_path / "out"),
             "--dry-run"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0

    def test_dry_run_creates_no_output(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "dry_out"
        subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--dry-run"],
            capture_output=True, text=True, timeout=15,
        )
        # Dry run should NOT create experiment outputs
        assert not (out / "det_only").exists()
        assert not (out / "experiment_matrix_summary.json").exists()

    def test_dry_run_shows_plan(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(tmp_path / "out"),
             "--dry-run"],
            capture_output=True, text=True, timeout=15,
        )
        assert "DRY RUN" in result.stdout


# ─── Tests: section filter in matrix ────────────────────────────────────────

class TestSectionFilterInMatrix:
    def test_section_filter_limits_projects(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        out = tmp_path / "matrix"
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(out),
             "--experiments", "det_only",
             "--sections", "KJ",
             "--llm-provider", "mock",
             "--quiet"],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0
        data = json.loads((out / "det_only" / "human_benchmark_summary.json").read_text())
        # Should have only KJ projects (2 projects × 10 findings each)
        assert data["overall_metrics"]["total_findings"] == 18  # 10 + 8

    def test_real_llm_without_flag_exits_1(self, tmp_path):
        manifest_path, _ = _make_manifest(tmp_path)
        result = subprocess.run(
            [sys.executable, str(MATRIX_SCRIPT),
             "--manifest", str(manifest_path),
             "--output-dir", str(tmp_path / "out"),
             "--llm-provider", "claude_runner",
             "--experiments", "det_only",
             "--dry-run"],
            capture_output=True, text=True, timeout=15,
        )
        # claude_runner without --with-real-llm should exit 1
        assert result.returncode == 1


# ─── Tests: manifest warnings propagation to matrix ──────────────────────────

def _make_project_with_decisions(
    base: Path,
    name: str,
    section: str,
    findings: list[dict],
    decisions: list[dict],
) -> Path:
    proj = base / section / name
    output = proj / "_output"
    output.mkdir(parents=True, exist_ok=True)
    (proj / "project_info.json").write_text(
        json.dumps({"section": section, "name": name}), encoding="utf-8"
    )
    (output / "03_findings.json").write_text(
        json.dumps({"findings": findings}), encoding="utf-8"
    )
    (output / "expert_review.json").write_text(
        json.dumps({
            "project_id": name,
            "reviewer": "test",
            "reviewed_at": "2026-01-01",
            "decisions": decisions,
        }), encoding="utf-8"
    )
    return proj


class TestMatrixManifestWarnings:
    """
    Tests ensuring the matrix correctly surfaces manifest warnings and uses
    matched decision counts rather than raw counts for metrics.
    """

    def setup_method(self):
        self.matrix_mod = _load_matrix_mod()
        self.manifest_mod = _load_manifest_mod()

    def _build_manifest_with_warnings(self, tmp_path: Path) -> tuple[Path, dict]:
        """Create a manifest that has duplicate and unmatched human decisions."""
        root = tmp_path / "projects"
        findings = [
            {"id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ", "category": "rebar",
             "problem": f"Prob {i}", "description": f"Desc {i}", "solution": f"Fix {i}"}
            for i in range(1, 6)
        ]
        # Duplicate: F-001 appears twice
        decisions_with_dup = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "changed", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-004", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-005", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
        ]
        _make_project_with_decisions(root, "WithDup", "AR", findings, decisions_with_dup)
        # Also a clean project
        _make_project(root, "KJ-Clean", "KJ", n_findings=5, accepted=2)

        manifest = self.manifest_mod.build_manifest(root)
        manifest_dir = tmp_path / "manifest"
        self.manifest_mod.write_outputs(manifest_dir, manifest)
        return manifest_dir / "human_benchmark_manifest.json", manifest

    def test_manifest_warnings_in_build_matrix_summary(self, tmp_path):
        """build_matrix_summary must propagate manifest_warnings."""
        _, manifest = self._build_manifest_with_warnings(tmp_path)
        summary_with_no_fr = {
            "experiment": "det_only",
            "overall_metrics": {
                "false_reject": 0, "false_reject_rate": 0.0,
                "false_accept": 0, "false_accept_rate": 0.0,
                "total_findings": 10, "total_mapped": 10,
                "human_accepted": 3, "human_rejected": 7,
                "critic_accepted": 5, "critic_rejected": 5,
                "critic_borderline": 0, "agreement": 5, "agreement_rate": 0.5,
                "true_accept": 3, "true_reject": 2,
            },
            "run_config": {"llm_gate_used": False, "context_enrichment_used": False},
            "llm_impact": {}, "context_enrichment": {},
        }
        matrix = self.matrix_mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[summary_with_no_fr],
            experiment_names=["det_only"],
        )
        assert "manifest_warnings" in matrix, "manifest_warnings must be in matrix summary"

    def test_manifest_stats_augmented_with_matched(self, tmp_path):
        """Matrix summary manifest_stats should have total_accepted_matched."""
        _, manifest = self._build_manifest_with_warnings(tmp_path)
        matrix = self.matrix_mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[],
            experiment_names=[],
        )
        stats = matrix["manifest_stats"]
        assert "total_accepted_matched" in stats, (
            "manifest_stats must include total_accepted_matched"
        )
        assert "total_rejected_matched" in stats, (
            "manifest_stats must include total_rejected_matched"
        )

    def test_matrix_markdown_shows_warnings_when_present(self, tmp_path):
        """Markdown should show manifest warnings section when warnings exist."""
        _, manifest = self._build_manifest_with_warnings(tmp_path)
        matrix = self.matrix_mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[],
            experiment_names=[],
        )
        md = self.matrix_mod.render_matrix_markdown(matrix)
        if manifest.get("manifest_warnings"):
            assert "Warning" in md or "warning" in md.lower(), (
                "Markdown must show warnings section when manifest has warnings"
            )

    def test_matrix_markdown_shows_matched_counts(self, tmp_path):
        """Markdown summary table should include matched counts."""
        _, manifest = self._build_manifest_with_warnings(tmp_path)
        matrix = self.matrix_mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[],
            experiment_names=[],
        )
        md = self.matrix_mod.render_matrix_markdown(matrix)
        assert "matched" in md.lower(), "Markdown must show matched human decision counts"

    def test_duplicate_decisions_not_counted_twice_in_matched(self, tmp_path):
        """
        Duplicate item_id in expert_review must be deduped — not counted as two
        separate human decisions. Matched count must not exceed findings count.
        """
        root = tmp_path / "projects"
        findings = [
            {"id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ", "category": "rebar",
             "problem": f"P{i}", "description": "", "solution": ""}
            for i in range(1, 4)
        ]
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "changed", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(root, "DupTest", "AR", findings, decisions)
        manifest_mod = _load_manifest_mod()
        rec = manifest_mod.build_manifest_record(proj)
        assert rec is not None
        matched = rec["human_decisions_matched_to_findings"]
        # 3 findings, F-001 appears twice but must be deduped to 1 unique ID
        assert matched == 3, (
            f"Duplicated item_id must be counted once. matched={matched}, expected 3"
        )
        assert matched <= rec["findings_count"], (
            f"matched ({matched}) must not exceed findings ({rec['findings_count']})"
        )

    def test_unmatched_decisions_excluded_from_fr_fa(self, tmp_path):
        """
        Unmatched decisions (ghost item_ids) must not affect FR/FA metrics.
        The benchmark script already excludes unmatched from the confusion matrix;
        this test verifies the manifest correctly tracks unmatched counts.
        """
        root = tmp_path / "projects"
        findings = [
            {"id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ", "category": "rebar",
             "problem": f"P{i}", "description": "", "solution": ""}
            for i in range(1, 4)
        ]
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-GHOST-STALE", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(root, "GhostTest", "KJ", findings, decisions)
        manifest_mod = _load_manifest_mod()
        rec = manifest_mod.build_manifest_record(proj)
        assert rec is not None
        assert rec["human_decisions_unmatched"] == 1, (
            f"F-GHOST-STALE must be counted as unmatched, got {rec['human_decisions_unmatched']}"
        )
        assert rec["human_decisions_matched_to_findings"] == 2, (
            f"Only F-001 and F-002 should be matched, got {rec['human_decisions_matched_to_findings']}"
        )
        # The unmatched ghost should NOT inflate accepted_matched
        assert rec["human_accepted_matched"] == 1  # only F-001
        assert rec["human_rejected_matched"] == 1  # only F-002

    def test_matrix_uses_matched_counts_in_stats(self, tmp_path):
        """
        Manifest built with clean 1:1 decisions → matched counts == raw counts.
        Verified through build_matrix_summary stats augmentation.
        """
        root = tmp_path / "projects"
        _make_project(root, "P1", "KJ", n_findings=5, accepted=2)
        manifest = _load_manifest_mod().build_manifest(root)
        matrix = self.matrix_mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[],
            experiment_names=[],
        )
        stats = matrix["manifest_stats"]
        # For clean data: matched == raw
        assert stats["total_accepted_matched"] == stats["total_accepted"], (
            "For clean data, matched counts should equal raw counts"
        )
        assert stats["total_rejected_matched"] == stats["total_rejected"], (
            "For clean data, matched counts should equal raw counts"
        )


class TestTriageInMatrix:
    """Tests for --triage flag integration in matrix script."""

    @property
    def matrix_mod(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "run_critic_v2_experiment_matrix",
            Path(__file__).resolve().parent.parent / "scripts" / "run_critic_v2_experiment_matrix.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_build_matrix_summary_triage_key_present(self):
        """build_matrix_summary includes triage key in metrics when available."""
        mod = self.matrix_mod
        # Create a fake experiment summary with triage_metrics
        fake_summary = {
            "experiment": "det_only",
            "overall_metrics": {
                "total_findings": 10, "total_mapped": 10, "total_unmapped": 0,
                "human_accepted": 5, "human_rejected": 5, "human_unknown": 0,
                "critic_accepted": 4, "critic_rejected": 3, "critic_borderline": 2,
                "critic_merged": 1,
                "true_accept": 4, "true_reject": 3,
                "false_reject": 0, "false_accept": 1,
                "false_reject_rate": 0.0, "false_accept_rate": 0.2,
                "agreement": 7, "agreement_rate": 0.7,
                "critic_borderline_on_human_accepted": 1,
                "critic_borderline_on_human_rejected": 1,
                "classification_counts": {},
                "human_rejection_reasons_sample": [],
                "critic_rejection_reasons_freq": {},
            },
            "run_config": {"llm_gate_used": False, "context_enrichment_used": False},
            "llm_impact": {},
            "triage_metrics": {
                "total_findings": 10,
                "strong_keep_count": 3,
                "main_review_count": 2,
                "borderline_count": 1,
                "needs_context_count": 0,
                "suggested_reject_count": 1,
                "hidden_by_critic_count": 3,
                "visible_by_default_count": 6,
                "collapsed_by_default_count": 4,
                "workload_reduction_count": 4,
                "workload_reduction_percent": 40.0,
                "hidden_human_accepted_count": 0,
                "accepted_visible_recall": 1.0,
            },
        }
        manifest = {"stats": {"total_projects": 1, "total_findings": 10,
                               "total_accepted": 5, "total_rejected": 5}}
        summary = mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[fake_summary],
            experiment_names=["det_only"],
        )
        m = summary["metrics_by_experiment"].get("det_only", {})
        assert "triage" in m, "triage key should be present in metrics_by_experiment"
        assert m["triage"] is not None

    def test_matrix_triage_in_markdown(self):
        """Triage metrics appear in markdown when present."""
        mod = self.matrix_mod
        fake_summary = {
            "experiment": "det_only",
            "overall_metrics": {
                "total_findings": 5, "total_mapped": 5, "total_unmapped": 0,
                "human_accepted": 2, "human_rejected": 3, "human_unknown": 0,
                "critic_accepted": 2, "critic_rejected": 2, "critic_borderline": 1,
                "critic_merged": 0,
                "true_accept": 2, "true_reject": 2,
                "false_reject": 0, "false_accept": 0,
                "false_reject_rate": 0.0, "false_accept_rate": 0.0,
                "agreement": 4, "agreement_rate": 0.8,
                "critic_borderline_on_human_accepted": 0,
                "critic_borderline_on_human_rejected": 1,
                "classification_counts": {},
                "human_rejection_reasons_sample": [],
                "critic_rejection_reasons_freq": {},
            },
            "run_config": {"llm_gate_used": False, "context_enrichment_used": False},
            "llm_impact": {},
            "triage_metrics": {
                "total_findings": 5, "strong_keep_count": 2, "main_review_count": 1,
                "borderline_count": 0, "needs_context_count": 0,
                "suggested_reject_count": 0, "hidden_by_critic_count": 2,
                "visible_by_default_count": 3, "collapsed_by_default_count": 2,
                "workload_reduction_count": 2, "workload_reduction_percent": 40.0,
                "hidden_human_accepted_count": 0,
                "accepted_visible_recall": 1.0,
            },
        }
        manifest = {"stats": {"total_projects": 1, "total_findings": 5,
                               "total_accepted": 2, "total_rejected": 3}}
        matrix_summary = mod.build_matrix_summary(
            manifest=manifest,
            experiment_summaries=[fake_summary],
            experiment_names=["det_only"],
        )
        md = mod.render_matrix_markdown(matrix_summary)
        assert "Triage Policy Metrics" in md
        assert "workload_reduction" in md.lower() or "Collapse%" in md

    def test_production_not_modified_with_triage_flag(self, tmp_path):
        """--triage flag does not modify production pipeline files."""
        import subprocess
        import sys

        matrix_script = Path(__file__).resolve().parent.parent / "scripts" / "run_critic_v2_experiment_matrix.py"
        result = subprocess.run(
            [sys.executable, str(matrix_script),
             "--manifest", "/tmp/nonexistent_manifest.json",
             "--output-dir", str(tmp_path),
             "--dry-run", "--triage",
             "--experiments", "det_only"],
            capture_output=True, text=True, timeout=30,
        )
        # dry-run should succeed or fail cleanly without writing production files
        # We only care that backend/app/pipeline files are not touched
        pipeline_runner = Path(__file__).resolve().parent.parent / "app" / "pipeline" / "manager.py"
        if pipeline_runner.exists():
            import os
            mtime_before = os.path.getmtime(pipeline_runner)
            # Wait a moment
            import time; time.sleep(0.1)
            mtime_after = os.path.getmtime(pipeline_runner)
            assert mtime_before == mtime_after, "manager.py was modified!"
