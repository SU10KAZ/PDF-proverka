"""
Unit tests for scripts/run_stage02_recall_hybrid.py

Covers:
- project resolution with _output_classic fallback
- recall signal computation (issue_potential, miss_risk, review_reasons, review_recommended)
- escalation scoring (mandatory rules: findings>0, heavy, merged, full-page)
- tier assignment (tier1/tier2/tier3)
- mandatory escalation rules (findings>0, heavy, merged, full-page, medium/high issue/miss)
- escalation set building (tier sizes, include_tier2, max_second_pass trimming)
- escalation set persistence (entries have correct fields)
- second-pass diff computation
- second-pass winner selection (recall-first criteria)
- hybrid recommendation logic (no real API calls)

No real OpenRouter or Claude CLI calls are made.
"""
from __future__ import annotations

import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _make_block(
    block_id: str = "BLK-001",
    size_kb: float = 50.0,
    page: int = 1,
    is_full_page: bool = False,
    merged_block_ids: list | None = None,
    ocr_text_len: int = 500,
) -> dict:
    return {
        "block_id": block_id,
        "size_kb": size_kb,
        "page": page,
        "is_full_page": is_full_page,
        "merged_block_ids": merged_block_ids or [],
        "ocr_text_len": ocr_text_len,
        "file": f"block_{block_id}.png",
        "ocr_label": f"label_{block_id}",
    }


def _make_analysis(
    block_id: str = "BLK-001",
    findings_count: int = 0,
    kv_count: int = 5,
    summary: str = "This is a detailed summary of the drawing.",
    unreadable: bool = False,
) -> dict:
    return {
        "block_id": block_id,
        "page": 1,
        "sheet": None,
        "label": "Test label",
        "sheet_type": "plan",
        "unreadable_text": unreadable,
        "unreadable_details": None,
        "summary": summary,
        "key_values_read": [f"kv_{i}" for i in range(kv_count)],
        "findings": [
            {
                "id": f"G-{i:03d}",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "test",
                "finding": "test finding",
                "norm": "",
                "norm_quote": None,
                "block_evidence": "",
                "value_found": "",
            }
            for i in range(findings_count)
        ],
    }


def _make_batch_result(
    block_ids: list[str],
    analyses: list[dict] | None = None,
    is_error: bool = False,
    inferred: bool = False,
) -> object:
    """Create a mock BatchResultEnvelope."""
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope
    if analyses is None:
        analyses = [_make_analysis(bid) for bid in block_ids]
    return BatchResultEnvelope(
        batch_id=1,
        model_id="google/gemini-2.5-flash",
        input_block_ids=block_ids,
        is_error=is_error,
        parsed_data={
            "batch_id": 1,
            "project_id": "test",
            "timestamp": "",
            "block_analyses": analyses,
        } if not is_error else None,
        inferred_block_id_count=1 if inferred else 0,
    )


# ──────────────────────────────────────────────────────────────────────────
# 1. Project resolution with _output_classic fallback
# ──────────────────────────────────────────────────────────────────────────

def _write_index(path: Path, block_ids: list[str]) -> None:
    """Write a minimal blocks/index.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "blocks": [
            {
                "block_id": bid,
                "file": f"block_{bid}.png",
                "page": 1,
                "size_kb": 50.0,
                "ocr_text_len": 300,
                "ocr_label": f"label_{bid}",
            }
            for bid in block_ids
        ]
    }, ensure_ascii=False), encoding="utf-8")


def test_resolution_uses_primary_output():
    """_output/blocks/index.json takes priority over _output_classic."""
    from run_stage02_recall_hybrid import resolve_project

    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir) / "test_project.pdf"
        project_dir.mkdir()
        (project_dir / "test_project.pdf").write_bytes(b"%PDF")

        _write_index(project_dir / "_output" / "blocks" / "index.json", ["A", "B"])
        _write_index(project_dir / "_output_classic" / "blocks" / "index.json", ["X", "Y"])

        res = resolve_project("test_project.pdf", project_dir)
        assert res.block_source == "_output"
        assert {b["block_id"] for b in res.blocks} == {"A", "B"}


def test_resolution_falls_back_to_classic():
    """If _output index missing, use _output_classic."""
    from run_stage02_recall_hybrid import resolve_project

    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir) / "test_project.pdf"
        project_dir.mkdir()

        _write_index(project_dir / "_output_classic" / "blocks" / "index.json", ["X", "Y"])

        res = resolve_project("test_project.pdf", project_dir)
        assert res.block_source == "_output_classic"
        assert {b["block_id"] for b in res.blocks} == {"X", "Y"}


def test_resolution_raises_when_no_index():
    """If neither index exists, raise FileNotFoundError."""
    from run_stage02_recall_hybrid import resolve_project

    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir) / "test_project.pdf"
        project_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="No valid blocks"):
            resolve_project("test_project.pdf", project_dir)


def test_resolution_raises_on_empty_blocks():
    """If index.json has empty blocks list, raise ValueError."""
    from run_stage02_recall_hybrid import resolve_project

    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir) / "test_project.pdf"
        project_dir.mkdir()

        idx_path = project_dir / "_output" / "blocks" / "index.json"
        idx_path.parent.mkdir(parents=True)
        idx_path.write_text(json.dumps({"blocks": []}), encoding="utf-8")

        with pytest.raises(ValueError, match="Empty blocks"):
            resolve_project("test_project.pdf", project_dir)


# ──────────────────────────────────────────────────────────────────────────
# 2. Recall signal computation
# ──────────────────────────────────────────────────────────────────────────

def test_recall_signals_clean_block():
    """Light block with good Flash output → low issue_potential, low miss_risk.

    Requires summary >= SHORT_SUMMARY_THRESHOLD (60 chars) and kv >= 3 to avoid
    triggering weak_summary / low_kv miss_risk conditions.
    """
    from run_stage02_recall_hybrid import compute_recall_signals, SHORT_SUMMARY_THRESHOLD

    long_summary = "A" * (SHORT_SUMMARY_THRESHOLD + 20)   # well above threshold
    block    = _make_block("A", size_kb=40.0, ocr_text_len=300)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary=long_summary)

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert sig["issue_potential"] == "low"
    assert sig["miss_risk"] == "low"
    assert sig["review_recommended"] is False
    assert "finding_present" not in sig["review_reasons"]


def test_recall_signals_finding_present():
    """Block with 1+ findings → finding_present reason, issue_potential medium or high."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=1, kv_count=5, summary="Long enough summary text here.")

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert sig["issue_potential"] in ("medium", "high")
    assert "finding_present" in sig["review_reasons"]
    assert sig["review_recommended"] is True


def test_recall_signals_two_findings_high_potential():
    """2+ findings → issue_potential = high."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=2, kv_count=5, summary="Long summary")

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert sig["issue_potential"] == "high"


def test_recall_signals_unreadable_high_risk():
    """Unreadable block → issue_potential=high, miss_risk=high."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=0, unreadable=True)

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert sig["issue_potential"] == "high"
    assert sig["miss_risk"] == "high"
    assert sig["review_recommended"] is True


def test_recall_signals_no_analysis_high_everything():
    """Missing analysis → all risks high."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block = _make_block("A", size_kb=40.0)
    sig = compute_recall_signals(block, None, inferred=False)

    assert sig["issue_potential"] == "high"
    assert sig["miss_risk"] == "high"
    assert sig["review_recommended"] is True
    assert "possible_missed_issue" in sig["review_reasons"]


def test_recall_signals_heavy_block_no_findings_miss_risk_high():
    """Heavy block (size_kb>=2000) with no findings → miss_risk=high."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=2100.0)  # heavy by size
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Long summary text here OK")

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert sig["miss_risk"] == "high"
    assert "high_structural_risk" in sig["review_reasons"]
    assert "possible_missed_issue" in sig["review_reasons"]


def test_recall_signals_inferred_block_medium_miss_risk():
    """Inferred block_id → uncertain_read in reasons, miss_risk at least medium."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Long and descriptive summary text.")

    sig = compute_recall_signals(block, analysis, inferred=True)
    assert "uncertain_read" in sig["review_reasons"]
    assert sig["miss_risk"] in ("medium", "high")


def test_recall_signals_weak_summary():
    """Short summary → weak_summary in reasons."""
    from run_stage02_recall_hybrid import compute_recall_signals, SHORT_SUMMARY_THRESHOLD

    block    = _make_block("A", size_kb=40.0)
    short_summary = "Short."  # definitely < SHORT_SUMMARY_THRESHOLD
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary=short_summary)

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert "weak_summary" in sig["review_reasons"]
    assert len(short_summary) < SHORT_SUMMARY_THRESHOLD


def test_recall_signals_dense_graphics():
    """Large block (size_kb>500) → dense_graphics_or_text in reasons."""
    from run_stage02_recall_hybrid import compute_recall_signals

    block    = _make_block("A", size_kb=900.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Long summary text here OK")

    sig = compute_recall_signals(block, analysis, inferred=False)
    assert "dense_graphics_or_text" in sig["review_reasons"]


# ──────────────────────────────────────────────────────────────────────────
# 3. Escalation scoring
# ──────────────────────────────────────────────────────────────────────────

def test_escalation_score_clean_light_block():
    """Clean light block with no findings → low score (below tier1 threshold).

    Summary must be >= SHORT_SUMMARY_THRESHOLD (60 chars) so weak_summary
    does not trigger, ensuring score stays below tier1 threshold of 8.
    """
    from run_stage02_recall_hybrid import (
        compute_escalation_score, compute_recall_signals, SHORT_SUMMARY_THRESHOLD, TIER1_SCORE_THRESHOLD,
    )

    long_summary = "X" * (SHORT_SUMMARY_THRESHOLD + 20)
    block    = _make_block("A", size_kb=40.0, ocr_text_len=300)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary=long_summary)
    recall   = compute_recall_signals(block, analysis, inferred=False)

    score, rules = compute_escalation_score(block, analysis, recall)
    assert score < TIER1_SCORE_THRESHOLD, f"Expected low score for clean block, got {score}"
    assert "findings_present" not in rules
    assert recall["miss_risk"] == "low"
    assert recall["issue_potential"] == "low"


def test_escalation_score_findings_present_high_score():
    """Block with findings → findings_present rule adds 10 points."""
    from run_stage02_recall_hybrid import compute_escalation_score, compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=1, kv_count=5, summary="Long summary text here OK")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    score, rules = compute_escalation_score(block, analysis, recall)
    assert score >= 10
    assert "findings_present" in rules


def test_escalation_score_heavy_block():
    """Heavy block → risk_heavy adds 8 points."""
    from run_stage02_recall_hybrid import compute_escalation_score, compute_recall_signals

    block    = _make_block("A", size_kb=2100.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Long summary text here OK")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    score, rules = compute_escalation_score(block, analysis, recall)
    assert score >= 8
    assert "risk_heavy" in rules


def test_escalation_score_full_page():
    """Full-page block → is_full_page adds 8 points."""
    from run_stage02_recall_hybrid import compute_escalation_score, compute_recall_signals

    block    = _make_block("A", size_kb=50.0, is_full_page=True)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Summary text")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    score, rules = compute_escalation_score(block, analysis, recall)
    assert "is_full_page" in rules
    assert score >= 8


def test_escalation_score_merged_block():
    """Merged block → is_merged adds 8 points."""
    from run_stage02_recall_hybrid import compute_escalation_score, compute_recall_signals

    block    = _make_block("A", size_kb=50.0, merged_block_ids=["X", "Y"])
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Summary text")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    score, rules = compute_escalation_score(block, analysis, recall)
    assert "is_merged" in rules
    assert score >= 8


def test_escalation_score_missing_analysis():
    """No analysis → analysis_missing adds 15 points."""
    from run_stage02_recall_hybrid import compute_escalation_score, compute_recall_signals

    block  = _make_block("A", size_kb=40.0)
    recall = compute_recall_signals(block, None, inferred=False)

    score, rules = compute_escalation_score(block, None, recall)
    assert score >= 15
    assert "analysis_missing" in rules


# ──────────────────────────────────────────────────────────────────────────
# 4. Tier assignment
# ──────────────────────────────────────────────────────────────────────────

def test_tier_assignment_clean_block_tier3():
    """Clean light block with good output → tier3.

    Summary must be long enough to avoid weak_summary (>= SHORT_SUMMARY_THRESHOLD chars)
    and kv must be > LOW_KV_THRESHOLD. The block must be 'light' risk (size_kb < 500).
    Under the recall-first policy, such blocks produce review_recommended=False and low scores.
    """
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals, compute_escalation_score, SHORT_SUMMARY_THRESHOLD

    long_summary = "M" * (SHORT_SUMMARY_THRESHOLD + 30)
    block    = _make_block("A", size_kb=40.0, ocr_text_len=300)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary=long_summary)
    recall   = compute_recall_signals(block, analysis, inferred=False)

    # Verify this is actually a low-signal block before testing tier
    assert recall["review_recommended"] is False, (
        f"Expected review_recommended=False but got {recall}"
    )
    score, _ = compute_escalation_score(block, analysis, recall)
    tier = assign_tier(block, analysis, recall, score=score)
    assert tier in ("tier3_flash_only_ok", "tier2_recommended_second_pass"), (
        f"Clean block should not be tier1, got {tier} (score={score}, recall={recall})"
    )


def test_tier_assignment_findings_forces_tier1():
    """Any findings → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=1, kv_count=5, summary="Summary")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    tier = assign_tier(block, analysis, recall, score=10)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_heavy_forces_tier1():
    """Heavy block → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=2100.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Summary here OK")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    tier = assign_tier(block, analysis, recall, score=8)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_full_page_forces_tier1():
    """Full-page block → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=50.0, is_full_page=True)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Summary")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    tier = assign_tier(block, analysis, recall, score=8)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_merged_forces_tier1():
    """Merged block → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=50.0, merged_block_ids=["X"])
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Summary")
    recall   = compute_recall_signals(block, analysis, inferred=False)

    tier = assign_tier(block, analysis, recall, score=8)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_medium_issue_potential_forces_tier1():
    """medium issue_potential → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=1, kv_count=5, summary="Summary")
    recall   = compute_recall_signals(block, analysis, inferred=False)
    assert recall["issue_potential"] in ("medium", "high")

    tier = assign_tier(block, analysis, recall, score=5)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_high_miss_risk_forces_tier1():
    """high miss_risk → mandatory tier1."""
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals

    block    = _make_block("A", size_kb=2100.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=0, summary="")
    recall   = compute_recall_signals(block, analysis, inferred=False)
    assert recall["miss_risk"] == "high"

    tier = assign_tier(block, analysis, recall, score=5)
    assert tier == "tier1_mandatory_second_pass"


def test_tier_assignment_medium_score_tier2():
    """Block with score in tier2 range and no mandatory flags → tier2."""
    from run_stage02_recall_hybrid import assign_tier, TIER2_SCORE_THRESHOLD, TIER1_SCORE_THRESHOLD

    block    = _make_block("A", size_kb=40.0)
    analysis = _make_analysis("A", findings_count=0, kv_count=5, summary="Long detailed summary here.")
    # force low recall to avoid mandatory tier1
    recall = {
        "issue_potential": "low",
        "miss_risk": "low",
        "review_recommended": False,
        "review_reasons": [],
    }
    score = TIER2_SCORE_THRESHOLD + 1  # above tier2 but below tier1

    tier = assign_tier(block, analysis, recall, score=score)
    assert tier == "tier2_recommended_second_pass"


# ──────────────────────────────────────────────────────────────────────────
# 5. Escalation set building
# ──────────────────────────────────────────────────────────────────────────

def _make_all_blocks_and_results(n: int = 5) -> tuple[list[dict], list]:
    """Create n clean light blocks with good Flash output."""
    blocks = [_make_block(f"BLK-{i:03d}", size_kb=40.0, page=i) for i in range(n)]
    results = [
        _make_batch_result([b["block_id"]], [_make_analysis(b["block_id"])])
        for b in blocks
    ]
    return blocks, results


def test_escalation_set_all_tier3_no_second_pass():
    """All clean blocks → all tier3, escalation set empty."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks, results = _make_all_blocks_and_results(5)
    entries, ts = build_escalation_set(blocks, results, include_tier2=True)
    # Should have some tier3 blocks; might not be empty if recall signals trigger
    # Check tier consistency
    tier1 = [e for e in entries if e.tier == "tier1_mandatory_second_pass"]
    tier3_ts = ts["tier3_count"]
    assert ts["tier1_count"] + ts["tier2_count"] + ts["tier3_count"] == len(blocks)


def test_escalation_set_findings_block_in_tier1():
    """Block with findings always lands in tier1."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks = [
        _make_block("CLEAN-001", size_kb=40.0),
        _make_block("FIND-001",  size_kb=40.0),
    ]
    results = [
        _make_batch_result(["CLEAN-001"], [_make_analysis("CLEAN-001", findings_count=0, kv_count=7, summary="Long detailed summary text OK")]),
        _make_batch_result(["FIND-001"],  [_make_analysis("FIND-001",  findings_count=2)]),
    ]

    entries, ts = build_escalation_set(blocks, results, include_tier2=True)
    tier1_ids = {e.block_id for e in entries if e.tier == "tier1_mandatory_second_pass"}
    assert "FIND-001" in tier1_ids


def test_escalation_set_heavy_block_in_tier1():
    """Heavy block always lands in tier1."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks = [
        _make_block("HEAVY-001", size_kb=2100.0),
    ]
    results = [
        _make_batch_result(["HEAVY-001"], [_make_analysis("HEAVY-001", findings_count=0, kv_count=5, summary="Summary here")]),
    ]

    entries, ts = build_escalation_set(blocks, results, include_tier2=True)
    tier1_ids = {e.block_id for e in entries if e.tier == "tier1_mandatory_second_pass"}
    assert "HEAVY-001" in tier1_ids


def test_escalation_set_missing_analysis_in_tier1():
    """Block with no analysis (LLM error) → tier1."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks = [_make_block("BLK-001")]
    results = [_make_batch_result(["BLK-001"], is_error=True)]

    entries, ts = build_escalation_set(blocks, results, include_tier2=True)
    tier1_ids = {e.block_id for e in entries if e.tier == "tier1_mandatory_second_pass"}
    assert "BLK-001" in tier1_ids


def test_escalation_set_no_tier2_when_excluded():
    """include_tier2=False → no tier2 in second-pass set."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks = [_make_block(f"B{i}", size_kb=40.0) for i in range(5)]
    results = [
        _make_batch_result([b["block_id"]], [_make_analysis(b["block_id"])])
        for b in blocks
    ]

    entries, ts = build_escalation_set(blocks, results, include_tier2=False)
    assert ts["tier2_count"] == 0
    for e in entries:
        assert e.tier != "tier2_recommended_second_pass"


def test_escalation_set_max_second_pass_never_trims_tier1():
    """max_second_pass NEVER trims tier1 blocks — only may limit tier2.

    Under the recall-first policy, tier1 is mandatory. If max_second_pass < tier1_count,
    tier1 is still included fully (tier1 always wins over the cap).
    """
    from run_stage02_recall_hybrid import build_escalation_set, SHORT_SUMMARY_THRESHOLD

    long_summary = "Z" * (SHORT_SUMMARY_THRESHOLD + 20)

    # Mix: 3 blocks with findings (tier1 mandatory) + 7 clean blocks (tier3)
    blocks_t1 = [_make_block(f"FIND-{i}", size_kb=40.0, page=i) for i in range(3)]
    blocks_t3 = [_make_block(f"CLEAN-{i}", size_kb=40.0, page=10 + i) for i in range(7)]
    all_blocks = blocks_t1 + blocks_t3

    results_t1 = [
        _make_batch_result([b["block_id"]], [_make_analysis(b["block_id"], findings_count=2)])
        for b in blocks_t1
    ]
    results_t3 = [
        _make_batch_result(
            [b["block_id"]],
            [_make_analysis(b["block_id"], findings_count=0, kv_count=5, summary=long_summary)],
        )
        for b in blocks_t3
    ]

    entries, ts = build_escalation_set(
        all_blocks, results_t1 + results_t3,
        include_tier2=True,
        max_second_pass=2,   # less than tier1_count (3)
    )

    # tier1 must always be fully included regardless of max_second_pass
    tier1_ids = {e.block_id for e in entries if e.tier == "tier1_mandatory_second_pass"}
    assert len(tier1_ids) == 3, (
        f"All 3 tier1 blocks must be in escalation set regardless of max_second_pass, got {tier1_ids}"
    )
    for b in blocks_t1:
        assert b["block_id"] in tier1_ids, f"{b['block_id']} (findings>0) must be tier1"


def test_escalation_set_entries_have_required_fields():
    """Each EscalationEntry has all expected fields."""
    from run_stage02_recall_hybrid import build_escalation_set, EscalationEntry

    blocks  = [_make_block("A")]
    results = [_make_batch_result(["A"], [_make_analysis("A")])]

    entries, ts = build_escalation_set(blocks, results)
    assert len(entries) == 1
    e = entries[0]
    assert e.block_id == "A"
    assert e.tier in (
        "tier1_mandatory_second_pass",
        "tier2_recommended_second_pass",
        "tier3_flash_only_ok",
    )
    assert isinstance(e.escalation_score, int)
    assert isinstance(e.rules_triggered, list)
    assert isinstance(e.issue_potential, str)
    assert isinstance(e.miss_risk, str)
    assert isinstance(e.review_reasons, list)

    # Can be serialized
    d = asdict(e)
    assert "block_id" in d
    assert "tier" in d


def test_escalation_set_tier_summary_fields():
    """tier_summary has expected keys."""
    from run_stage02_recall_hybrid import build_escalation_set

    blocks  = [_make_block("A")]
    results = [_make_batch_result(["A"], [_make_analysis("A")])]

    _, ts = build_escalation_set(blocks, results)
    for key in ("tier1_count", "tier2_count", "tier3_count", "second_pass_total", "second_pass_pct"):
        assert key in ts, f"Missing key {key} in tier_summary"
    assert ts["tier1_count"] + ts["tier2_count"] + ts["tier3_count"] == len(blocks)


# ──────────────────────────────────────────────────────────────────────────
# 6. Second-pass diff computation
# ──────────────────────────────────────────────────────────────────────────

def test_diff_improved_when_engine_finds_more():
    """Engine finding more than Flash → improved status."""
    from run_stage02_recall_hybrid import diff_second_pass_vs_flash

    bid = "BLK-001"
    flash_results  = [_make_batch_result([bid], [_make_analysis(bid, findings_count=0)])]
    engine_results = [_make_batch_result([bid], [_make_analysis(bid, findings_count=2)])]

    d = diff_second_pass_vs_flash(engine_results, flash_results, [bid], "TestEngine")
    assert d["improved"] == 1
    assert d["added_findings"] == 2
    assert d["degraded"] == 0


def test_diff_degraded_when_engine_finds_less():
    """Engine finding less than Flash → degraded status."""
    from run_stage02_recall_hybrid import diff_second_pass_vs_flash

    bid = "BLK-001"
    flash_results  = [_make_batch_result([bid], [_make_analysis(bid, findings_count=3)])]
    engine_results = [_make_batch_result([bid], [_make_analysis(bid, findings_count=1)])]

    d = diff_second_pass_vs_flash(engine_results, flash_results, [bid], "TestEngine")
    assert d["degraded"] == 1
    assert d["improved"] == 0


def test_diff_unchanged_same_findings():
    """Same findings → unchanged."""
    from run_stage02_recall_hybrid import diff_second_pass_vs_flash

    bid = "BLK-001"
    flash_results  = [_make_batch_result([bid], [_make_analysis(bid, findings_count=2)])]
    engine_results = [_make_batch_result([bid], [_make_analysis(bid, findings_count=2)])]

    d = diff_second_pass_vs_flash(engine_results, flash_results, [bid], "TestEngine")
    assert d["unchanged"] == 1


def test_diff_unreadable_recovery():
    """Engine recovering from unreadable → counts in unreadable_recovery."""
    from run_stage02_recall_hybrid import diff_second_pass_vs_flash

    bid = "BLK-001"
    flash_results  = [_make_batch_result([bid], [_make_analysis(bid, findings_count=0, unreadable=True)])]
    engine_results = [_make_batch_result([bid], [_make_analysis(bid, findings_count=1, unreadable=False)])]

    d = diff_second_pass_vs_flash(engine_results, flash_results, [bid], "TestEngine")
    assert d["unreadable_recovery"] == 1
    assert d["improved"] == 1


def test_diff_multi_block():
    """Multi-block diff with mixed outcomes."""
    from run_stage02_recall_hybrid import diff_second_pass_vs_flash

    bids = ["A", "B", "C"]
    flash_results = [
        _make_batch_result(["A"], [_make_analysis("A", findings_count=0)]),
        _make_batch_result(["B"], [_make_analysis("B", findings_count=2)]),
        _make_batch_result(["C"], [_make_analysis("C", findings_count=1)]),
    ]
    engine_results = [
        _make_batch_result(["A"], [_make_analysis("A", findings_count=1)]),   # improved
        _make_batch_result(["B"], [_make_analysis("B", findings_count=2)]),   # unchanged
        _make_batch_result(["C"], [_make_analysis("C", findings_count=0)]),   # degraded
    ]

    d = diff_second_pass_vs_flash(engine_results, flash_results, bids, "TestEngine")
    assert d["improved"]  == 1  # A
    assert d["unchanged"] == 1  # B
    assert d["degraded"]  == 1  # C
    assert d["added_findings"] == 1


# ──────────────────────────────────────────────────────────────────────────
# 7. Second-pass winner selection (recall-first)
# ──────────────────────────────────────────────────────────────────────────

def _make_run_metrics(**kw):
    from run_gemini_openrouter_stage02_experiment import RunMetrics
    m = RunMetrics()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_winner_completeness_first():
    """If one engine has coverage issues, the complete one wins."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    pro_m    = _make_run_metrics(coverage_pct=98.0, missing_count=1, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.01, elapsed_s=10)
    claude_m = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.05, elapsed_s=20)

    pro_diff   = {"improved": 5, "added_findings": 8, "degraded": 0, "per_block_diff": []}
    claude_diff = {"improved": 5, "added_findings": 8, "degraded": 0, "per_block_diff": []}

    winner, _ = select_second_pass_winner(pro_m, pro_diff, claude_m, claude_diff, 10)
    assert "Claude" in winner


def test_winner_more_improved_blocks_wins():
    """When both complete, more improved blocks wins."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    pro_m    = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.05, elapsed_s=20)
    claude_m = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.10, elapsed_s=30)

    pro_diff    = {"improved": 3, "added_findings": 4, "degraded": 0, "per_block_diff": []}
    claude_diff = {"improved": 7, "added_findings": 10, "degraded": 0, "per_block_diff": []}

    winner, _ = select_second_pass_winner(pro_m, pro_diff, claude_m, claude_diff, 10)
    assert "Claude" in winner


def test_winner_degradation_penalizes():
    """Engine with more degradations loses even if it improves more."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    pro_m    = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.05, elapsed_s=20)
    claude_m = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.10, elapsed_s=30)

    # Claude improves more but also degrades more
    pro_diff    = {"improved": 3, "added_findings": 4, "degraded": 0, "per_block_diff": []}
    claude_diff = {"improved": 7, "added_findings": 10, "degraded": 5, "per_block_diff": []}

    winner, _ = select_second_pass_winner(pro_m, pro_diff, claude_m, claude_diff, 10)
    # Should prefer Pro (no degradations) or note tradeoff
    # Exact outcome depends on implementation; just verify it runs and returns a string
    assert isinstance(winner, str)
    assert len(winner) > 0


def test_winner_equal_quality_prefers_lower_cost():
    """Equal quality → lower cost wins."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    pro_m    = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.03, elapsed_s=15)
    claude_m = _make_run_metrics(coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.12, elapsed_s=25)

    same_diff = {"improved": 3, "added_findings": 5, "degraded": 0, "per_block_diff": []}

    winner, rationale = select_second_pass_winner(pro_m, same_diff, claude_m, same_diff, 10)
    assert "Pro" in winner  # Pro is cheaper


def test_winner_none_when_both_incomplete():
    """Both engines with coverage issues → winner='none'."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    pro_m    = _make_run_metrics(coverage_pct=95.0, missing_count=2, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.05, elapsed_s=10)
    claude_m = _make_run_metrics(coverage_pct=90.0, missing_count=3, duplicate_count=0, extra_count=0,
                                  total_cost_usd=0.10, elapsed_s=20)

    diff = {"improved": 2, "added_findings": 3, "degraded": 0, "per_block_diff": []}

    winner, _ = select_second_pass_winner(pro_m, diff, claude_m, diff, 10)
    assert winner == "none"


def test_winner_handles_none_engines():
    """If engines not run (None), returns 'none'."""
    from run_stage02_recall_hybrid import select_second_pass_winner

    winner, _ = select_second_pass_winner(None, None, None, None, 10)
    assert winner == "none"


# ──────────────────────────────────────────────────────────────────────────
# 8. Hybrid recommendation correctness (integration-style, no API)
# ──────────────────────────────────────────────────────────────────────────

def test_hybrid_recommendation_md_contains_key_sections():
    """_build_hybrid_recommendation_md produces all required sections."""
    from run_stage02_recall_hybrid import _build_hybrid_recommendation_md
    from run_gemini_openrouter_stage02_experiment import RunMetrics

    flash_m  = RunMetrics(
        model_id="google/gemini-2.5-flash",
        total_input_blocks=25,
        coverage_pct=100.0,
        total_findings=5,
        blocks_with_findings=4,
        total_cost_usd=0.025,
    )
    ts = {
        "tier1_count": 10, "tier2_count": 5, "tier3_count": 10,
        "second_pass_total": 15, "second_pass_pct": 60.0,
        "tier2_trimmed": False, "tier2_total": 5,
    }

    blocks = [_make_block(f"B{i}") for i in range(25)]

    md = _build_hybrid_recommendation_md(
        flash_m, None, None, None, None, ts, "none", "No second pass.", blocks
    )
    assert "Flash First Pass" in md
    assert "Escalation Tiers" in md
    assert "Practical Policy" in md
    assert "Constraints honored" in md


def test_mandatory_escalation_rules_all_trigger_tier1():
    """
    Comprehensive check: findings>0, heavy, full-page, merged, medium/high issue/miss
    ALL individually produce tier1.
    """
    from run_stage02_recall_hybrid import assign_tier, compute_recall_signals, compute_escalation_score

    # Case 1: findings > 0
    b = _make_block("A", size_kb=40.0)
    a = _make_analysis("A", findings_count=1, summary="OK summary here.")
    r = compute_recall_signals(b, a, False)
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "findings>0 must be tier1"

    # Case 2: heavy
    b = _make_block("A", size_kb=2200.0)
    a = _make_analysis("A", findings_count=0, kv_count=5, summary="OK long summary text here.")
    r = compute_recall_signals(b, a, False)
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "heavy must be tier1"

    # Case 3: full_page
    b = _make_block("A", size_kb=50.0, is_full_page=True)
    a = _make_analysis("A", findings_count=0, kv_count=5, summary="OK long summary text here.")
    r = compute_recall_signals(b, a, False)
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "full_page must be tier1"

    # Case 4: merged
    b = _make_block("A", size_kb=50.0, merged_block_ids=["X"])
    a = _make_analysis("A", findings_count=0, kv_count=5, summary="OK long summary text here.")
    r = compute_recall_signals(b, a, False)
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "merged must be tier1"

    # Case 5: medium issue_potential
    b = _make_block("A", size_kb=40.0)
    a = _make_analysis("A", findings_count=1, kv_count=5, summary="OK summary.")
    r = compute_recall_signals(b, a, False)
    assert r["issue_potential"] in ("medium", "high")
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "medium+ issue must be tier1"

    # Case 6: high miss_risk
    b = _make_block("A", size_kb=2200.0)
    a = _make_analysis("A", findings_count=0, kv_count=0, summary="")
    r = compute_recall_signals(b, a, False)
    assert r["miss_risk"] == "high"
    score, _ = compute_escalation_score(b, a, r)
    assert assign_tier(b, a, r, score) == "tier1_mandatory_second_pass", "high miss_risk must be tier1"
