"""
test_batch_critic_v2.py
------------------------
Tests for batch_critic_v2.py batch runner.

All tests work without touching production artifacts.
Tests use fixtures and synthetic project directories.

Runs with:
    python -m pytest backend/tests/test_batch_critic_v2.py -v
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

# ─── Fixtures ─────────────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "findings_review"
BATCH_SCRIPT = Path("backend/scripts/batch_critic_v2.py")


def _make_project(tmp_path: Path, name: str, findings: list[dict],
                  section: str = "EOM", with_blocks: bool = False,
                  with_review: bool = False) -> Path:
    """Create a synthetic project directory with 03_findings.json."""
    project_dir = tmp_path / name
    output_dir = project_dir / "_output"
    output_dir.mkdir(parents=True)

    # project_info.json
    (project_dir / "project_info.json").write_text(
        json.dumps({"name": name, "section": section, "project_id": f"{section}/{name}"},
                   ensure_ascii=False), encoding="utf-8"
    )

    # 03_findings.json
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Optional 02_blocks_analysis.json
    if with_blocks:
        block_ids = list({
            b.get("block_id") or b
            for f in findings
            for b in f.get("evidence", [])
            if isinstance(b, dict) and b.get("block_id")
        })
        (output_dir / "02_blocks_analysis.json").write_text(
            json.dumps({
                "block_analyses": [{"block_id": bid} for bid in block_ids],
            }, ensure_ascii=False), encoding="utf-8"
        )

    # Optional legacy 03_findings_review.json
    if with_review:
        reviews = [
            {"finding_id": f["id"], "verdict": "pass", "details": None,
             "suggested_action": None, "correct_page": None, "correct_sheet": None}
            for f in findings
        ]
        (output_dir / "03_findings_review.json").write_text(
            json.dumps({
                "meta": {"project_id": name, "total_reviewed": len(findings), "verdicts": {"pass": len(findings)}},
                "reviews": reviews,
            }, ensure_ascii=False), encoding="utf-8"
        )

    return project_dir


def _good_finding(fid: str) -> dict:
    return {
        "id": fid,
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable",
        "sheet": "Лист 1",
        "page": 1,
        "problem": f"Кабель {fid} без FR-исполнения — нарушение СП",
        "description": f"На листе 1 кабель {fid} ВВГнг-LS вместо FRLS 4x6",
        "solution": "Заменить на ВВГнг-FRLS 4x6",
        "risk": "Потеря работоспособности при пожаре. Нарушение пожарной безопасности.",
        "norm": "СП 6.13130.2021, п. 4.2",
        "norm_quote": "Кабельные линии систем противопожарной защиты...",
        "evidence": [{"block_id": f"BLK-{fid}-A", "type": "image", "page": 1},
                     {"block_id": f"BLK-{fid}-B", "type": "text", "page": 1}],
        "related_block_ids": [f"BLK-{fid}-A", f"BLK-{fid}-B"],
        "source_block_ids": [f"BLK-{fid}-A"],
    }


def _bad_finding(fid: str) -> dict:
    return {
        "id": fid,
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "documentation",
        "sheet": "Общие данные",
        "page": 1,
        "problem": "Необходимо проверить соответствие",
        "description": "Требуется уточнить актуальность применяемых норм",
        "solution": "Проверить",
        "risk": None,
        "evidence": [],
        "related_block_ids": [],
        "source_block_ids": [],
    }


# ─── Import tests ─────────────────────────────────────────────────────────────

class TestBatchImports:
    def test_batch_script_imports(self):
        """batch_critic_v2.py must be importable without error."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)
        # Just verify it loads without ImportError
        spec.loader.exec_module(mod)
        assert hasattr(mod, "run_one_project")
        assert hasattr(mod, "discover_projects")
        assert hasattr(mod, "build_summary")
        assert hasattr(mod, "compare_with_legacy")


# ─── Project discovery ────────────────────────────────────────────────────────

class TestDiscoverProjects:
    def test_discovers_real_projects(self):
        """discover_projects should find real projects under projects/ root."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        projects = mod.discover_projects(limit=5)
        assert len(projects) > 0
        assert len(projects) <= 5

    def test_section_filter(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        projects = mod.discover_projects(section="EOM", limit=10)
        assert len(projects) >= 1

    def test_empty_result_for_nonexistent_section(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        projects = mod.discover_projects(section="XYZNONEXISTENT")
        assert projects == []


# ─── run_one_project ──────────────────────────────────────────────────────────

class TestRunOneProject:
    def _load_batch(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_good_project_runs_successfully(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(
            tmp_path, "TEST-GOOD",
            [_good_finding(f"F-{i:03d}") for i in range(5)],
        )
        result = mod.run_one_project(project_dir, tmp_path / "out")
        assert not result.get("skipped")
        assert result["total_findings"] == 5
        assert isinstance(result["accepted"], int)
        assert isinstance(result["rejected"], int)

    def test_bad_project_rejects_mostly(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(
            tmp_path, "TEST-BAD",
            [_bad_finding(f"F-{i:03d}") for i in range(5)],
        )
        result = mod.run_one_project(project_dir, tmp_path / "out")
        assert not result.get("skipped")
        assert result["rejected"] >= 3  # mostly bad findings get rejected

    def test_missing_findings_returns_skipped(self, tmp_path):
        mod = self._load_batch()
        project_dir = tmp_path / "EMPTY-PROJECT"
        project_dir.mkdir()
        (project_dir / "_output").mkdir()
        result = mod.run_one_project(project_dir, tmp_path / "out")
        assert result["skipped"] is True
        assert result.get("error") is not None

    def test_artifacts_created(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(
            tmp_path, "TEST-ART",
            [_good_finding("F-001"), _good_finding("F-002")],
        )
        out_dir = tmp_path / "out"
        result = mod.run_one_project(project_dir, out_dir)
        assert not result.get("skipped")
        proj_out = Path(result["output_dir"])
        assert (proj_out / "critic_v2_decisions.json").exists()
        assert (proj_out / "critic_v2_metrics.json").exists()
        assert (proj_out / "critic_v2_accepted.json").exists()
        assert (proj_out / "critic_v2_rejected.json").exists()

    def test_production_artifacts_not_modified(self, tmp_path):
        """Production 03_findings.json and 03_findings_review.json must be untouched."""
        mod = self._load_batch()
        findings = [_good_finding("F-001"), _bad_finding("F-002")]
        project_dir = _make_project(tmp_path, "TEST-PROD", findings, with_review=True)

        original_findings = (project_dir / "_output" / "03_findings.json").read_text(encoding="utf-8")
        original_review = (project_dir / "_output" / "03_findings_review.json").read_text(encoding="utf-8")

        mod.run_one_project(project_dir, tmp_path / "out")

        # Production files must be IDENTICAL after batch run
        assert (project_dir / "_output" / "03_findings.json").read_text(encoding="utf-8") == original_findings
        assert (project_dir / "_output" / "03_findings_review.json").read_text(encoding="utf-8") == original_review

    def test_with_blocks_index(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(
            tmp_path, "TEST-BLOCKS",
            [_good_finding("F-001"), _good_finding("F-002")],
            with_blocks=True,
        )
        result = mod.run_one_project(project_dir, tmp_path / "out", with_blocks=True)
        assert not result.get("skipped")
        assert result["blocks_index_used"] is True

    def test_with_llm_gate_mock(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(
            tmp_path, "TEST-LLM",
            [_good_finding("F-001"), _good_finding("F-002"), _bad_finding("F-003")],
        )
        out_dir = tmp_path / "out"
        result = mod.run_one_project(
            project_dir, out_dir,
            llm_gate=True, llm_provider="mock",
        )
        assert not result.get("skipped")
        assert result["llm_gate_used"] is True
        # LLM-specific artifacts must exist
        proj_out = Path(result["output_dir"])
        assert (proj_out / "critic_v2_llm_decisions.json").exists()
        assert (proj_out / "critic_v2_final_decisions.json").exists()
        assert (proj_out / "critic_v2_borderline.json").exists()


# ─── Legacy comparison ────────────────────────────────────────────────────────

class TestLegacyComparison:
    def _load_batch(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_compare_with_legacy_all_pass(self, tmp_path):
        mod = self._load_batch()
        findings = [_good_finding(f"F-{i:03d}") for i in range(4)]
        project_dir = _make_project(tmp_path, "LEGACY-TEST", findings, with_review=True)
        result = mod.run_one_project(
            project_dir, tmp_path / "out", compare_legacy=True
        )
        assert not result.get("skipped")
        cmp = result.get("comparison")
        assert cmp is not None
        assert cmp["total_compared"] > 0

    def test_compare_artifact_created(self, tmp_path):
        mod = self._load_batch()
        findings = [_good_finding("F-001"), _bad_finding("F-002")]
        project_dir = _make_project(tmp_path, "LEGACY-ART", findings, with_review=True)
        result = mod.run_one_project(
            project_dir, tmp_path / "out", compare_legacy=True
        )
        proj_out = Path(result["output_dir"])
        assert (proj_out / "critic_v2_legacy_comparison.json").exists()

    def test_no_legacy_skips_comparison(self, tmp_path):
        mod = self._load_batch()
        project_dir = _make_project(tmp_path, "NO-LEGACY", [_good_finding("F-001")])
        result = mod.run_one_project(
            project_dir, tmp_path / "out", compare_legacy=True
        )
        # comparison should be None when no legacy review exists
        assert result.get("comparison") is None

    def test_compare_structure(self):
        mod = self._load_batch()
        from backend.app.pipeline.stages.findings_review.critic_v2 import (
            run_critic_v2_offline, QualityDecision, EVIDENCE_VALID
        )
        findings = [_good_finding(f"F-{i:03d}") for i in range(3)]
        det = run_critic_v2_offline(findings)
        legacy_review = {
            "reviews": [
                {"finding_id": f["id"], "verdict": "pass"} for f in findings
            ]
        }
        comparison = mod.compare_with_legacy(det.decisions, legacy_review)
        assert "total_compared" in comparison
        assert "agreement" in comparison
        assert "rows" in comparison
        assert isinstance(comparison["rows"], list)


# ─── build_summary ────────────────────────────────────────────────────────────

class TestBuildSummary:
    def _load_batch(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("batch_critic_v2", BATCH_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_summary_totals(self, tmp_path):
        mod = self._load_batch()
        results = [
            {
                "project": "P1", "section": "EOM",
                "total_findings": 10, "accepted": 7, "borderline": 2,
                "rejected": 1, "merged": 0, "rejected_by_rules": 1, "rejected_by_score": 0,
                "rejection_reasons": {"no_evidence": 1},
                "average_usefulness_score": 7.5,
                "evidence_breakdown": {"valid": 8, "partial": 1, "weak": 0, "none": 1},
                "blocks_index_used": False, "llm_gate_used": False,
                "llm_candidates_sent": 0, "det_ms": 10, "llm_ms": 0,
                "output_dir": str(tmp_path), "comparison": None, "skipped": False, "error": None,
            },
            {
                "project": "P2", "section": "EOM",
                "total_findings": 5, "accepted": 3, "borderline": 1,
                "rejected": 1, "merged": 0, "rejected_by_rules": 1, "rejected_by_score": 0,
                "rejection_reasons": {"ocr_artifact": 1},
                "average_usefulness_score": 8.0,
                "evidence_breakdown": {"valid": 4, "partial": 0, "weak": 1, "none": 0},
                "blocks_index_used": False, "llm_gate_used": False,
                "llm_candidates_sent": 0, "det_ms": 5, "llm_ms": 0,
                "output_dir": str(tmp_path), "comparison": None, "skipped": False, "error": None,
            },
        ]
        summary = mod.build_summary(results, tmp_path, compare_legacy=False)
        assert summary["totals"]["total_findings"] == 15
        assert summary["totals"]["accepted"] == 10
        assert summary["totals"]["rejected"] == 2
        assert "EOM" in summary["by_section"]

    def test_skipped_excluded_from_totals(self, tmp_path):
        mod = self._load_batch()
        results = [
            {
                "project": "P1", "section": "EOM",
                "total_findings": 10, "accepted": 7, "borderline": 2, "rejected": 1,
                "merged": 0, "rejected_by_rules": 1, "rejected_by_score": 0,
                "rejection_reasons": {}, "average_usefulness_score": 7.5,
                "evidence_breakdown": {"valid": 8, "partial": 1, "weak": 0, "none": 1},
                "blocks_index_used": False, "llm_gate_used": False,
                "llm_candidates_sent": 0, "det_ms": 10, "llm_ms": 0,
                "output_dir": str(tmp_path), "comparison": None, "skipped": False, "error": None,
            },
            {
                "project": "SKIP", "skipped": True, "error": "missing file",
                "section": "EOM",
            },
        ]
        summary = mod.build_summary(results, tmp_path, compare_legacy=False)
        assert summary["totals"]["total_findings"] == 10
        assert len(summary["skipped"]) == 1

    def test_summary_saved(self, tmp_path):
        mod = self._load_batch()
        results = []
        summary = mod.build_summary(results, tmp_path, compare_legacy=False)
        assert "totals" in summary
        assert "by_section" in summary
        assert "per_project" in summary


# ─── CLI integration ─────────────────────────────────────────────────────────

class TestCLIBatch:
    def test_cli_section_eom_limit_3(self, tmp_path):
        """CLI must run on 3 EOM projects without error."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "3",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, (
            f"CLI failed (rc={result.returncode}):\n{result.stdout}\n{result.stderr}"
        )
        assert (tmp_path / "batch_summary.json").exists()
        assert (tmp_path / "batch_results.json").exists()

    def test_cli_summary_structure(self, tmp_path):
        """batch_summary.json must have expected keys."""
        subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "2",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        summary = json.loads((tmp_path / "batch_summary.json").read_text(encoding="utf-8"))
        assert "totals" in summary
        assert "by_section" in summary
        assert "rejection_reasons" in summary
        assert "per_project" in summary
        assert "evidence_breakdown" in summary
        assert "run_config" in summary

    def test_cli_per_project_artifacts(self, tmp_path):
        """Each processed project must have its own artifact directory."""
        subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "2",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        results = json.loads((tmp_path / "batch_results.json").read_text(encoding="utf-8"))
        for r in results:
            proj_dir = Path(r["output_dir"])
            assert proj_dir.exists()
            assert (proj_dir / "critic_v2_decisions.json").exists()
            assert (proj_dir / "critic_v2_metrics.json").exists()
            assert (proj_dir / "critic_v2_accepted.json").exists()

    def test_cli_production_not_modified(self, tmp_path):
        """Production files must NOT be modified after batch run."""
        from pathlib import Path as P
        # Collect checksums of production files BEFORE run
        findings_files = sorted(P("projects").rglob("03_findings.json"))[:3]
        before = {str(p): p.read_text(encoding="utf-8") for p in findings_files}

        subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "3",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )

        # Verify unchanged
        for path_str, original in before.items():
            current = P(path_str).read_text(encoding="utf-8")
            assert current == original, f"Production file was modified: {path_str}"

    def test_cli_with_llm_gate_mock(self, tmp_path):
        """CLI with --llm-gate --llm-provider mock must work and create LLM artifacts."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "2",
                "--llm-gate",
                "--llm-provider", "mock",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        results = json.loads((tmp_path / "batch_results.json").read_text(encoding="utf-8"))
        for r in results:
            proj_dir = Path(r["output_dir"])
            assert (proj_dir / "critic_v2_final_decisions.json").exists()
            assert (proj_dir / "critic_v2_llm_decisions.json").exists()

    def test_cli_compare_legacy(self, tmp_path):
        """CLI --compare-legacy must produce comparison artifacts."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "2",
                "--compare-legacy",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        results = json.loads((tmp_path / "batch_results.json").read_text(encoding="utf-8"))
        # At least one project should have legacy comparison (all EOM projects have 03_findings_review.json)
        with_cmp = [r for r in results if r.get("comparison")]
        assert len(with_cmp) >= 1

    def test_cli_with_blocks(self, tmp_path):
        """CLI --with-blocks must enable blocks_index per project."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "2",
                "--with-blocks",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0
        results = json.loads((tmp_path / "batch_results.json").read_text(encoding="utf-8"))
        # At least one project should have blocks index
        with_blocks = [r for r in results if r.get("blocks_index_used")]
        assert len(with_blocks) >= 1

    def test_cli_no_accept_in_zero_evidence_batch(self, tmp_path):
        """Batch over real EOM projects: no accepted finding should have evidence_quality=none."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "EOM",
                "--limit", "3",
                "--output-dir", str(tmp_path),
                "--quiet",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0
        results = json.loads((tmp_path / "batch_results.json").read_text(encoding="utf-8"))
        for r in results:
            proj_dir = Path(r["output_dir"])
            decisions = json.loads((proj_dir / "critic_v2_decisions.json").read_text())
            for d in decisions:
                if d["decision"] == "accept":
                    assert d["evidence_quality"] != "none", (
                        f"Accepted finding {d['finding_id']} has evidence_quality=none "
                        f"in project {r['project']}"
                    )

    def test_cli_invalid_section_exits_1(self, tmp_path):
        """Invalid section must cause exit code 1."""
        result = subprocess.run(
            [
                sys.executable, str(BATCH_SCRIPT),
                "--section", "DOESNOTEXIST999",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 1
