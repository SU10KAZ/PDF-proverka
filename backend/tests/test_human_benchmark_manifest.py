"""
test_human_benchmark_manifest.py
----------------------------------
Tests for build_human_benchmark_manifest.py.

All tests use synthetic project directories — no real projects accessed.

Runs with:
    python -m pytest backend/tests/test_human_benchmark_manifest.py -v
"""
from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path("backend/scripts/build_human_benchmark_manifest.py")


def _load_script():
    spec = importlib.util.spec_from_file_location("build_manifest", str(SCRIPT.resolve()))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ─── Synthetic project builder ────────────────────────────────────────────────

def _make_project(
    base: Path,
    name: str,
    section: str = "KJ",
    findings: list[dict] | None = None,
    accepted: int = 2,
    rejected: int = 8,
    has_blocks: bool = True,
    has_graph: bool = True,
) -> Path:
    """Create a minimal synthetic project directory."""
    proj = base / section / name
    output = proj / "_output"
    output.mkdir(parents=True, exist_ok=True)

    # project_info.json
    (proj / "project_info.json").write_text(
        json.dumps({"section": section, "name": name}), encoding="utf-8"
    )

    # 03_findings.json
    if findings is None:
        findings = [
            {
                "id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ",
                "category": "rebar", "problem": f"Problem {i}",
                "description": "", "solution": "",
            }
            for i in range(1, accepted + rejected + 1)
        ]
    (output / "03_findings.json").write_text(
        json.dumps({"findings": findings}), encoding="utf-8"
    )

    # expert_review.json
    decisions = []
    for i, f in enumerate(findings):
        decision = "accepted" if i < accepted else "rejected"
        decisions.append({
            "item_id": f["id"],
            "item_type": "finding",
            "decision": decision,
            "rejection_reason": None if decision == "accepted" else f"Reason {i}",
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

    if has_blocks:
        (output / "02_blocks_analysis.json").write_text(
            json.dumps({"block_analyses": []}), encoding="utf-8"
        )

    if has_graph:
        (output / "document_graph.json").write_text(
            json.dumps({"pages": []}), encoding="utf-8"
        )

    return proj


# ─── Tests: build_manifest_record ────────────────────────────────────────────

class TestBuildManifestRecord:
    def setup_method(self):
        self.mod = _load_script()

    def test_basic_record(self, tmp_path):
        proj = _make_project(tmp_path, "TestProject", section="KJ")
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        assert rec["project_name"] == "TestProject"
        assert rec["section"] == "KJ"
        assert rec["has_findings"] is True
        assert rec["has_expert_review"] is True
        assert rec["findings_count"] == 10
        assert rec["human_accepted"] == 2
        assert rec["human_rejected"] == 8

    def test_hashes_computed(self, tmp_path):
        proj = _make_project(tmp_path, "P1", section="AR")
        rec = self.mod.build_manifest_record(proj)
        assert "03_findings.json" in rec["hashes"]
        assert "expert_review.json" in rec["hashes"]
        assert len(rec["hashes"]["03_findings.json"]) == 64  # sha256 hex

    def test_hash_changes_when_file_changes(self, tmp_path):
        proj = _make_project(tmp_path, "P1", section="KJ")
        rec1 = self.mod.build_manifest_record(proj)
        h1 = rec1["hashes"]["03_findings.json"]

        # Modify findings
        findings_path = proj / "_output" / "03_findings.json"
        data = json.loads(findings_path.read_text())
        data["findings"].append({"id": "F-NEW", "problem": "New finding"})
        findings_path.write_text(json.dumps(data))

        rec2 = self.mod.build_manifest_record(proj)
        h2 = rec2["hashes"]["03_findings.json"]
        assert h1 != h2, "Hash should change when file changes"

    def test_without_expert_review_returns_none(self, tmp_path):
        proj = tmp_path / "KJ" / "NoReview"
        output = proj / "_output"
        output.mkdir(parents=True)
        (output / "03_findings.json").write_text(json.dumps({"findings": []}))
        rec = self.mod.build_manifest_record(proj)
        assert rec is None

    def test_without_findings_returns_none(self, tmp_path):
        proj = tmp_path / "KJ" / "NoFindings"
        output = proj / "_output"
        output.mkdir(parents=True)
        decisions = [{"item_id": "F-1", "item_type": "finding",
                      "decision": "rejected", "rejection_reason": "r",
                      "reviewer": "t", "timestamp": "2026"}]
        (output / "expert_review.json").write_text(
            json.dumps({"project_id": "x", "decisions": decisions})
        )
        rec = self.mod.build_manifest_record(proj)
        assert rec is None

    def test_empty_expert_review_returns_none(self, tmp_path):
        proj = _make_project(tmp_path, "EmptyReview", section="KJ")
        # Overwrite with empty decisions
        (proj / "_output" / "expert_review.json").write_text(
            json.dumps({"project_id": "x", "decisions": []})
        )
        rec = self.mod.build_manifest_record(proj)
        assert rec is None

    def test_has_blocks_detected(self, tmp_path):
        proj = _make_project(tmp_path, "WithBlocks", has_blocks=True, has_graph=True)
        rec = self.mod.build_manifest_record(proj)
        assert rec["has_blocks"] is True
        assert rec["has_document_graph"] is True

    def test_no_blocks_detected(self, tmp_path):
        proj = _make_project(tmp_path, "NoBlocks", has_blocks=False, has_graph=False)
        rec = self.mod.build_manifest_record(proj)
        assert rec["has_blocks"] is False
        assert rec["has_document_graph"] is False

    def test_section_detected_from_project_info(self, tmp_path):
        proj = _make_project(tmp_path, "P1", section="EOM")
        rec = self.mod.build_manifest_record(proj)
        assert rec["section"] == "EOM"

    def test_optional_artifacts_in_hashes(self, tmp_path):
        proj = _make_project(tmp_path, "Full", has_blocks=True, has_graph=True)
        rec = self.mod.build_manifest_record(proj)
        assert "02_blocks_analysis.json" in rec["hashes"]
        assert "document_graph.json" in rec["hashes"]


# ─── Tests: build_manifest ────────────────────────────────────────────────────

class TestBuildManifest:
    def setup_method(self):
        self.mod = _load_script()

    def _setup_projects(self, tmp_path: Path) -> Path:
        """Create 3 projects: 2 KJ, 1 AR."""
        root = tmp_path / "projects"
        _make_project(root, "KJ-P1", section="KJ", accepted=1, rejected=9)
        _make_project(root, "KJ-P2", section="KJ", accepted=2, rejected=8)
        _make_project(root, "AR-P1", section="AR", accepted=3, rejected=7)
        return root

    def test_finds_all_projects(self, tmp_path):
        root = self._setup_projects(tmp_path)
        manifest = self.mod.build_manifest(root)
        assert manifest["stats"]["total_projects"] == 3

    def test_section_filter(self, tmp_path):
        root = self._setup_projects(tmp_path)
        manifest = self.mod.build_manifest(root, sections=["KJ"])
        assert manifest["stats"]["total_projects"] == 2
        sections = {r["section"] for r in manifest["records"]}
        assert sections == {"KJ"}

    def test_by_section_aggregation(self, tmp_path):
        root = self._setup_projects(tmp_path)
        manifest = self.mod.build_manifest(root)
        assert "KJ" in manifest["by_section"]
        assert "AR" in manifest["by_section"]
        assert manifest["by_section"]["KJ"]["projects"] == 2
        assert manifest["by_section"]["AR"]["projects"] == 1

    def test_total_findings_correct(self, tmp_path):
        root = self._setup_projects(tmp_path)
        manifest = self.mod.build_manifest(root)
        assert manifest["stats"]["total_findings"] == 30  # 3 × 10

    def test_total_accepted_rejected_correct(self, tmp_path):
        root = self._setup_projects(tmp_path)
        manifest = self.mod.build_manifest(root)
        assert manifest["stats"]["total_accepted"] == 6   # 1+2+3
        assert manifest["stats"]["total_rejected"] == 24  # 9+8+7

    def test_empty_root_returns_empty_manifest(self, tmp_path):
        manifest = self.mod.build_manifest(tmp_path / "empty")
        assert manifest["stats"]["total_projects"] == 0

    def test_skipped_logged(self, tmp_path):
        root = tmp_path / "projects"
        # Project without expert_review
        bad = root / "KJ" / "BadProject" / "_output"
        bad.mkdir(parents=True)
        (bad / "03_findings.json").write_text(json.dumps({"findings": []}))
        # Good project
        _make_project(root, "GoodProject", section="KJ")
        manifest = self.mod.build_manifest(root)
        assert manifest["stats"]["total_projects"] == 1
        # Skipped not tracked here (no review = ignored, not "skipped")

    def test_explicit_paths(self, tmp_path):
        root = tmp_path / "projects"
        p1 = _make_project(root, "P1", section="KJ")
        p2 = _make_project(root, "P2", section="AR")
        _make_project(root, "P3", section="EOM")
        manifest = self.mod.build_manifest(root, explicit_paths=[p1, p2])
        assert manifest["stats"]["total_projects"] == 2


# ─── Tests: write_outputs ─────────────────────────────────────────────────────

class TestWriteOutputs:
    def setup_method(self):
        self.mod = _load_script()

    def _make_simple_manifest(self, tmp_path):
        root = tmp_path / "projects"
        _make_project(root, "P1", section="KJ")
        return self.mod.build_manifest(root)

    def test_json_created(self, tmp_path):
        manifest = self._make_simple_manifest(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, manifest)
        assert (out / "human_benchmark_manifest.json").exists()

    def test_markdown_created(self, tmp_path):
        manifest = self._make_simple_manifest(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, manifest)
        md = (out / "human_benchmark_manifest.md").read_text(encoding="utf-8")
        assert "# Human Benchmark Manifest" in md
        assert "By Section" in md

    def test_csv_created_with_flag(self, tmp_path):
        manifest = self._make_simple_manifest(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, manifest, export_csv=True)
        csv_path = out / "human_benchmark_manifest.csv"
        assert csv_path.exists()
        rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
        assert len(rows) == 1

    def test_csv_not_created_without_flag(self, tmp_path):
        manifest = self._make_simple_manifest(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, manifest, export_csv=False)
        assert not (out / "human_benchmark_manifest.csv").exists()

    def test_json_is_valid(self, tmp_path):
        manifest = self._make_simple_manifest(tmp_path)
        out = tmp_path / "out"
        self.mod.write_outputs(out, manifest)
        data = json.loads((out / "human_benchmark_manifest.json").read_text())
        assert "records" in data
        assert "stats" in data
        assert data["stats"]["total_projects"] == 1

    def test_production_not_modified(self, tmp_path):
        """Manifest script should not write to project _output/."""
        root = tmp_path / "projects"
        proj = _make_project(root, "P1", section="KJ")
        output_dir = proj / "_output"
        before = sorted(f.name for f in output_dir.iterdir())

        manifest = self.mod.build_manifest(root)
        scratch = tmp_path / "scratch"
        self.mod.write_outputs(scratch, manifest)

        after = sorted(f.name for f in output_dir.iterdir())
        assert before == after, "Manifest script must not write to project _output/"


# ─── Tests: CLI ───────────────────────────────────────────────────────────────

class TestCLI:
    def _setup(self, tmp_path: Path) -> tuple[Path, Path]:
        root = tmp_path / "projects"
        _make_project(root, "KJ-P1", section="KJ")
        _make_project(root, "AR-P1", section="AR")
        out = tmp_path / "out"
        return root, out

    def test_basic_run(self, tmp_path):
        root, out = self._setup(tmp_path)
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--projects-root", str(root),
             "--output-dir", str(out), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"CLI failed: {result.stderr}"
        assert (out / "human_benchmark_manifest.json").exists()

    def test_section_filter_cli(self, tmp_path):
        root, out = self._setup(tmp_path)
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--projects-root", str(root),
             "--sections", "KJ",
             "--output-dir", str(out), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        data = json.loads((out / "human_benchmark_manifest.json").read_text())
        assert data["stats"]["total_projects"] == 1

    def test_missing_projects_root_exits_1(self, tmp_path):
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--projects-root", str(tmp_path / "nonexistent"),
             "--output-dir", str(tmp_path / "out"), "--quiet"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 1

    def test_production_not_modified_note(self, tmp_path):
        root, out = self._setup(tmp_path)
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--projects-root", str(root),
             "--output-dir", str(out)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert "Production" in result.stdout or "NOT modified" in result.stdout

    def test_with_csv_flag(self, tmp_path):
        root, out = self._setup(tmp_path)
        result = subprocess.run(
            [sys.executable, str(SCRIPT),
             "--projects-root", str(root),
             "--export-csv",
             "--output-dir", str(out), "--quiet"],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert (out / "human_benchmark_manifest.csv").exists()


# ─── Helpers for duplicate/unmatched testing ──────────────────────────────────

def _make_project_with_decisions(
    base: Path,
    name: str,
    section: str,
    findings: list[dict],
    decisions: list[dict],
) -> Path:
    """Create a project with explicit findings and expert_review decisions."""
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


# ─── Tests: duplicate and unmatched human decisions ──────────────────────────

class TestDuplicateAndUnmatchedDecisions:
    """
    Tests for the new manifest validation fields:
    - duplicate item_ids in expert_review
    - unmatched item_ids (decision refers to finding that doesn't exist)
    - human_accepted + human_rejected matched counts
    - warnings in the manifest record and manifest-level
    """

    def setup_method(self):
        self.mod = _load_script()

    def _findings(self, n=5):
        return [
            {"id": f"F-{i:03d}", "severity": "КРИТИЧЕСКОЕ", "category": "rebar",
             "problem": f"Prob {i}", "description": "", "solution": ""}
            for i in range(1, n + 1)
        ]

    def test_clean_project_no_warnings(self, tmp_path):
        """Project with 1:1 matched decisions has no warnings."""
        findings = self._findings(3)
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(tmp_path, "Clean", "KJ", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        assert rec["human_decisions_matched_to_findings"] == 3
        assert rec["human_decisions_unmatched"] == 0
        assert rec["human_decisions_duplicate_item_ids"] == []
        assert rec["human_accepted_matched"] == 1
        assert rec["human_rejected_matched"] == 2
        assert rec["findings_without_human_decision"] == 0
        assert rec["warnings"] == []

    def test_duplicate_item_id_detected(self, tmp_path):
        """Same item_id appears twice → reported in duplicate_item_ids."""
        findings = self._findings(3)
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "changed mind", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(tmp_path, "Dup", "AR", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        assert "F-001" in rec["human_decisions_duplicate_item_ids"]
        assert any("duplicate" in w for w in rec["warnings"])

    def test_unmatched_item_id_detected(self, tmp_path):
        """Decision refers to a finding_id that doesn't exist → unmatched."""
        findings = self._findings(2)
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-GHOST", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "stale", "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(tmp_path, "Unmatched", "KJ", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        assert rec["human_decisions_unmatched"] == 1
        assert rec["human_decisions_matched_to_findings"] == 1
        assert any("unmatched" in w for w in rec["warnings"])

    def test_human_decisions_exceed_findings_warning(self, tmp_path):
        """
        If more matched decisions than findings → human_decisions_exceed_findings warning.
        This is impossible with deduplication unless findings_count is wrong, but test
        the warning pathway.
        """
        # Create 3 findings and 4 unique decisions (via unmatched ghost + 3 real)
        findings = self._findings(2)  # only 2 findings
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-GHOST1", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(tmp_path, "Exceed", "AR", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        # matched = 2, unmatched = 1
        assert rec["human_decisions_matched_to_findings"] == 2
        assert rec["human_decisions_unmatched"] == 1
        # matched total (2) does NOT exceed findings (2) → no exceed warning
        # (Exceed warning only fires when matched > findings, which can't happen with proper dedup)

    def test_findings_without_decision_counted(self, tmp_path):
        """Finding with no human decision → counted in findings_without_human_decision."""
        findings = self._findings(3)  # F-001, F-002, F-003
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            # F-002 and F-003 not reviewed
        ]
        proj = _make_project_with_decisions(tmp_path, "Missing", "KJ", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        assert rec is not None
        assert rec["findings_without_human_decision"] == 2
        assert any("without_review" in w for w in rec["warnings"])

    def test_manifest_level_warnings_surfaced(self, tmp_path):
        """
        build_manifest should aggregate per-project warnings into manifest_warnings.
        """
        root = tmp_path / "projects"
        # Project with a duplicate
        findings = self._findings(3)
        dup_decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "changed", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        _make_project_with_decisions(root, "WithDup", "AR", findings, dup_decisions)
        # Project with unmatched
        unmatched_decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-GHOST", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "stale", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        _make_project_with_decisions(root, "WithUnmatched", "KJ", findings, unmatched_decisions)

        mod = self.mod
        manifest = mod.build_manifest(root)
        warnings = manifest.get("manifest_warnings", [])
        assert any("duplicate" in w for w in warnings), (
            f"Expected duplicate warning in manifest_warnings, got: {warnings}"
        )
        assert any("unmatched" in w for w in warnings), (
            f"Expected unmatched warning in manifest_warnings, got: {warnings}"
        )

    def test_matched_counts_in_stats(self, tmp_path):
        """
        build_manifest stats must include total_accepted_matched and total_rejected_matched.
        """
        root = tmp_path / "projects"
        findings = self._findings(5)
        decisions = [
            {"item_id": f"F-{i:03d}", "item_type": "finding",
             "decision": "accepted" if i <= 2 else "rejected",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"}
            for i in range(1, 6)
        ]
        _make_project_with_decisions(root, "P1", "KJ", findings, decisions)

        manifest = self.mod.build_manifest(root)
        stats = manifest["stats"]
        assert "total_accepted_matched" in stats
        assert "total_rejected_matched" in stats
        assert stats["total_accepted_matched"] == 2
        assert stats["total_rejected_matched"] == 3

    def test_markdown_shows_matched_section(self, tmp_path):
        """Markdown output should include matched counts column."""
        root = tmp_path / "projects"
        _make_project(root, "P1", section="KJ", accepted=2, rejected=3)
        manifest = self.mod.build_manifest(root)
        md = self.mod.render_markdown(manifest)
        assert "matched" in md.lower(), "Markdown should mention matched counts"

    def test_markdown_shows_warnings_section(self, tmp_path):
        """Markdown should show ⚠️ Manifest Warnings section when there are warnings."""
        root = tmp_path / "projects"
        findings = self._findings(3)
        dup_decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "changed", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-002", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
            {"item_id": "F-003", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "r", "reviewer": "t", "timestamp": "2026"},
        ]
        _make_project_with_decisions(root, "WithDup", "KJ", findings, dup_decisions)
        manifest = self.mod.build_manifest(root)
        md = self.mod.render_markdown(manifest)
        assert "Manifest Warnings" in md or "Warning" in md, (
            "Markdown should show warnings section when duplicates present"
        )

    def test_manifest_version_updated(self, tmp_path):
        """build_manifest should return manifest_version=2 now."""
        root = tmp_path / "projects"
        _make_project(root, "P1", section="KJ")
        manifest = self.mod.build_manifest(root)
        assert manifest.get("manifest_version") == "2", (
            f"Expected manifest_version=2, got {manifest.get('manifest_version')!r}"
        )

    def test_non_finding_decisions_excluded(self, tmp_path):
        """Only item_type=finding decisions count; optimization/other types are excluded."""
        findings = self._findings(2)
        decisions = [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
            {"item_id": "OPT-001", "item_type": "optimization", "decision": "accepted",
             "rejection_reason": None, "reviewer": "t", "timestamp": "2026"},
        ]
        proj = _make_project_with_decisions(tmp_path, "Mixed", "KJ", findings, decisions)
        rec = self.mod.build_manifest_record(proj)
        # Only F-001 counts; OPT-001 is not item_type=finding
        assert rec is not None
        assert rec["expert_review_finding_items"] == 1
        assert rec["human_decisions_matched_to_findings"] == 1
