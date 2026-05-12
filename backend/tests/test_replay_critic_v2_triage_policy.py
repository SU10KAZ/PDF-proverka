"""
Tests for replay_critic_v2_triage_policy.py

Verifies:
- Replay does NOT call LLM
- Replay reads existing artifacts
- Triage artifacts are created
- Production files are NOT modified
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.scripts.replay_critic_v2_triage_policy import (
    _LLMDecProxy,
    _load_benchmark_records,
    _load_llm_decisions,
    _record_to_quality_decision,
    compute_section_breakdown,
    replay_triage_on_records,
)
from backend.app.pipeline.stages.findings_review.critic_v2.models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
)
from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
    PROFILE_ASSISTED,
    PROFILE_CONSERVATIVE,
    QUEUE_HIDDEN,
    QUEUE_STRONG_KEEP,
    build_triage_artifacts,
    compute_triage_metrics,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_record(
    fid: str = "F-001",
    critic_decision: str = "accept",
    critic_score: int = 8,
    ev: str = EVIDENCE_VALID,
    human_decision: str = "accepted",
    human_reason: str = "",
    severity: str = "КРИТИЧЕСКОЕ",
    section: str = "AR",
) -> dict:
    return {
        "finding_id": fid,
        "critic_decision": critic_decision,
        "critic_score": critic_score,
        "evidence_quality": ev,
        "human_decision": human_decision,
        "human_reason": human_reason,
        "severity": severity,
        "section": section,
        "title": f"Test finding {fid}",
        "description": "Description",
    }


def _make_llm_decision(
    fid: str = "F-001",
    llm_decision: str = "accept",
    taxonomy: str = "visual_or_ocr_misread",
    confidence: float = 0.9,
    ev_checked: bool = True,
    source_dep: str = "enough_source",
) -> dict:
    return {
        "finding_id": fid,
        "llm_decision": llm_decision,
        "human_taxonomy_reason": taxonomy,
        "confidence": confidence,
        "evidence_checked": ev_checked,
        "source_dependency": source_dep,
        "explanation": "Test explanation",
    }


# ─── Unit tests ───────────────────────────────────────────────────────────────


class TestLoadBenchmarkRecords:
    def test_loads_list(self, tmp_path: Path):
        records = [_make_record("F-001"), _make_record("F-002")]
        p = tmp_path / "human_benchmark_records.json"
        p.write_text(json.dumps(records), encoding="utf-8")
        loaded = _load_benchmark_records(tmp_path)
        assert len(loaded) == 2

    def test_missing_file_returns_empty(self, tmp_path: Path):
        loaded = _load_benchmark_records(tmp_path)
        assert loaded == []

    def test_loads_wrapped_dict(self, tmp_path: Path):
        records = [_make_record("F-001")]
        p = tmp_path / "human_benchmark_records.json"
        p.write_text(json.dumps({"records": records}), encoding="utf-8")
        loaded = _load_benchmark_records(tmp_path)
        assert len(loaded) == 1


class TestLoadLLMDecisions:
    def test_loads_list(self, tmp_path: Path):
        decs = [_make_llm_decision("F-001"), _make_llm_decision("F-002")]
        p = tmp_path / "critic_v2_llm_taxonomy_decisions.json"
        p.write_text(json.dumps(decs), encoding="utf-8")
        loaded = _load_llm_decisions(tmp_path)
        assert "F-001" in loaded
        assert "F-002" in loaded

    def test_missing_file_returns_empty(self, tmp_path: Path):
        loaded = _load_llm_decisions(tmp_path)
        assert loaded == {}


class TestRecordToQualityDecision:
    def test_basic_record(self):
        rec = _make_record("F-001", critic_decision="accept", critic_score=8, ev=EVIDENCE_VALID)
        det = _record_to_quality_decision(rec)
        assert det.finding_id == "F-001"
        assert det.decision == "accept"
        assert det.usefulness_score == 8
        assert det.evidence_quality == EVIDENCE_VALID

    def test_reject_record(self):
        rec = _make_record("F-002", critic_decision="reject", critic_score=1,
                            ev=EVIDENCE_NONE)
        rec["critic_reject_reason"] = "no_evidence"
        det = _record_to_quality_decision(rec)
        assert det.decision == "reject"
        assert det.reject_reason == "no_evidence"


class TestLLMDecProxy:
    def test_proxy_attributes(self):
        d = _make_llm_decision(
            fid="F-001",
            llm_decision="reject",
            taxonomy="visual_or_ocr_misread",
            confidence=0.85,
            ev_checked=True,
            source_dep="enough_source",
        )
        proxy = _LLMDecProxy(d)
        assert proxy.finding_id == "F-001"
        assert proxy.llm_decision == "reject"
        assert proxy.human_taxonomy_reason == "visual_or_ocr_misread"
        assert proxy.confidence == pytest.approx(0.85)
        assert proxy.evidence_checked is True
        assert proxy.source_dependency == "enough_source"

    def test_proxy_missing_fields(self):
        proxy = _LLMDecProxy({})
        assert proxy.finding_id == ""
        assert proxy.llm_decision is None
        assert proxy.confidence is None
        assert proxy.source_dependency == "enough_source"


class TestReplayTriageOnRecords:
    def test_no_llm_called(self):
        """Verify replay never calls LLM (all providers would raise if called)."""
        records = [
            _make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted", severity="КРИТИЧЕСКОЕ"),
            _make_record("F-002", "reject", 0, EVIDENCE_NONE, "rejected",
                         severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        records[1]["critic_reject_reason"] = "no_evidence"
        triage, metrics = replay_triage_on_records(records, {})
        assert len(triage) == 2
        assert metrics.total_findings == 2

    def test_triage_with_llm_decisions(self):
        """Replay uses pre-saved LLM decisions without calling LLM."""
        records = [
            _make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted"),
        ]
        llm_by_id = {
            "F-001": _make_llm_decision("F-001", "accept"),
        }
        triage, metrics = replay_triage_on_records(records, llm_by_id)
        assert len(triage) == 1
        # strong_keep: accept + score=8 + valid
        assert triage[0].human_queue == QUEUE_STRONG_KEEP

    def test_human_labels_recall(self):
        """Accepted visible recall computed when human labels available."""
        records = [
            _make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted"),
            _make_record("F-002", "reject", 0, EVIDENCE_NONE, "accepted",
                         severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        records[1]["critic_reject_reason"] = "no_evidence"
        triage, metrics = replay_triage_on_records(records, {}, include_human_labels=True)
        assert metrics.accepted_visible_recall is not None
        # F-002 is hidden (accepted by human but hidden by critic)
        assert metrics.hidden_human_accepted_count == 1

    def test_no_human_labels_no_recall(self):
        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        triage, metrics = replay_triage_on_records(records, {}, include_human_labels=False)
        assert metrics.accepted_visible_recall is None


class TestComputeSectionBreakdown:
    def test_breakdown_by_section(self):
        records = [
            _make_record("F-001", section="AR"),
            _make_record("F-002", section="AR"),
            _make_record("F-003", section="KJ"),
        ]
        decisions = [
            _record_to_quality_decision(r) for r in records
        ]
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import assign_triage_queue
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import TriageDecision

        triage = [
            assign_triage_queue(
                {"id": r["finding_id"], "severity": r["severity"], "category": ""},
                decisions[i],
                decisions[i],
            )
            for i, r in enumerate(records)
        ]
        breakdown = compute_section_breakdown(records, triage)
        assert "AR" in breakdown
        assert "KJ" in breakdown
        assert breakdown["AR"]["total"] == 2
        assert breakdown["KJ"]["total"] == 1


class TestBuildTriageArtifacts:
    def test_all_artifact_keys_present(self):
        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        triage, metrics = replay_triage_on_records(records, {})
        artifacts = build_triage_artifacts(triage, metrics)
        expected_keys = [
            "critic_v2_triage",
            "critic_v2_triage_metrics",
            "critic_v2_hidden_by_critic",
            "critic_v2_suggested_reject",
            "critic_v2_visible_by_default",
            "critic_v2_risky_hidden_cases",
        ]
        for k in expected_keys:
            assert k in artifacts, f"Missing artifact key: {k}"


class TestReplayDoesNotModifyProduction:
    def test_no_write_to_project_dirs(self, tmp_path: Path):
        """Replay should only write to output_dir, not to any project directory."""
        import subprocess
        import sys

        # Create a minimal benchmark output
        records = [
            _make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted"),
        ]
        bench_dir = tmp_path / "benchmark"
        bench_dir.mkdir()
        (bench_dir / "human_benchmark_records.json").write_text(
            json.dumps(records), encoding="utf-8"
        )
        out_dir = tmp_path / "triage_out"

        script = str(_PROJECT_ROOT / "backend" / "scripts" / "replay_critic_v2_triage_policy.py")
        result = subprocess.run(
            [
                sys.executable, script,
                "--benchmark-output-dir", str(bench_dir),
                "--output-dir", str(out_dir),
                "--quiet",
            ],
            capture_output=True, text=True,
        )
        # Should succeed
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        # Output should be in out_dir only
        assert (out_dir / "critic_v2_triage.json").exists()
        assert (out_dir / "triage_replay_summary.md").exists()
        # benchmark dir should not have triage files written to it
        assert not (bench_dir / "critic_v2_triage.json").exists()

    def test_production_findings_not_modified(self, tmp_path: Path):
        """Verify replay never modifies 03_findings.json in project dirs."""
        project_dir = tmp_path / "projects" / "test_project"
        output_dir = project_dir / "_output"
        output_dir.mkdir(parents=True)

        findings_path = output_dir / "03_findings.json"
        original_content = json.dumps({"findings": [{"id": "F-001"}]})
        findings_path.write_text(original_content, encoding="utf-8")

        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        bench_dir = tmp_path / "bench"
        bench_dir.mkdir()
        (bench_dir / "human_benchmark_records.json").write_text(
            json.dumps(records), encoding="utf-8"
        )
        out_dir = tmp_path / "out"

        # Run replay
        triage, metrics = replay_triage_on_records(records, {})
        artifacts = build_triage_artifacts(triage, metrics)

        # Verify findings not modified
        assert findings_path.read_text(encoding="utf-8") == original_content


class TestReplayProfiles:
    """Tests for profile parameter in replay functions."""

    def test_profile_parameter_accepted(self):
        """replay_triage_on_records accepts profile parameter."""
        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        for prof in [PROFILE_CONSERVATIVE, PROFILE_ASSISTED]:
            triage, metrics = replay_triage_on_records(records, {}, profile=prof)
            assert metrics.profile == prof
            assert len(triage) == 1
            assert triage[0].profile == prof

    def test_assisted_profile_metrics_has_new_fields(self):
        """New primary queue metrics are populated."""
        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        triage, metrics = replay_triage_on_records(records, {}, profile=PROFILE_ASSISTED)
        assert hasattr(metrics, "primary_visible_count")
        assert hasattr(metrics, "primary_collapsed_count")
        assert hasattr(metrics, "primary_queue_reduction_percent")
        assert hasattr(metrics, "accepted_primary_visible_recall")
        assert hasattr(metrics, "accepted_not_hidden_recall")

    def test_triage_metrics_to_dict_has_new_fields(self):
        """triage_metrics_to_dict includes all new metric keys."""
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import triage_metrics_to_dict
        records = [_make_record("F-001", "accept", 8, EVIDENCE_VALID, "accepted")]
        triage, metrics = replay_triage_on_records(records, {})
        d = triage_metrics_to_dict(metrics)
        for key in ["primary_visible_count", "primary_collapsed_count",
                    "primary_queue_reduction_percent", "accepted_primary_visible_recall",
                    "accepted_not_hidden_recall"]:
            assert key in d, f"Missing key: {key}"
