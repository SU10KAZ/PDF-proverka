from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def _obs(block_id: str, mode: str, repeat_idx: int, findings: int, kv: int, summary_len: int = 120):
    from run_pro_second_pass_reevaluation import ProbeObservation

    return ProbeObservation(
        block_id=block_id,
        mode=mode,
        repeat_idx=repeat_idx,
        findings_count=findings,
        kv_count=kv,
        summary_len=summary_len,
        unreadable=False,
        output_tokens=1000,
        reasoning_tokens=500,
        cost_usd=0.05,
        duration_s=12.0,
    )


def test_choose_probe_replacements_uses_mode_specific_ranked_observations():
    from run_pro_second_pass_reevaluation import choose_probe_replacements

    observations = {
        "A": [
            _obs("A", "A", 1, 3, 10),
            _obs("A", "A", 2, 1, 20),
            _obs("A", "A", 3, 2, 15),
            _obs("A", "B", 1, 9, 99),
        ]
    }

    conservative = choose_probe_replacements(observations, mode="A", policy="conservative")
    median = choose_probe_replacements(observations, mode="A", policy="median")
    optimistic = choose_probe_replacements(observations, mode="A", policy="optimistic")

    assert conservative["A"].findings_count == 1
    assert median["A"].findings_count == 2
    assert optimistic["A"].findings_count == 3


def test_build_counterfactual_diff_reclassifies_missing_blocks():
    from run_pro_second_pass_reevaluation import build_counterfactual_diff

    baseline_diff = {
        "engine": "Pro",
        "escalation_set_size": 2,
        "per_block_diff": [
            {
                "block_id": "X",
                "status": "missing_in_engine",
                "flash_findings": 2,
                "engine_findings": 0,
                "delta_findings": -2,
                "flash_kv": 10,
                "engine_kv": 0,
                "delta_kv": -10,
                "flash_unreadable": False,
                "engine_unreadable": False,
                "flash_missing": False,
                "engine_missing": True,
                "flash_summary_len": 100,
                "engine_summary_len": 0,
            },
            {
                "block_id": "Y",
                "status": "degraded",
                "flash_findings": 3,
                "engine_findings": 1,
                "delta_findings": -2,
                "flash_kv": 9,
                "engine_kv": 4,
                "delta_kv": -5,
                "flash_unreadable": False,
                "engine_unreadable": False,
                "flash_missing": False,
                "engine_missing": False,
                "flash_summary_len": 120,
                "engine_summary_len": 110,
            },
        ],
    }
    replacements = {
        "X": _obs("X", "A", 1, 4, 8, summary_len=160),
    }

    diff = build_counterfactual_diff(baseline_diff, replacements)
    by_id = {row["block_id"]: row for row in diff["per_block_diff"]}

    assert by_id["X"]["status"] == "improved"
    assert by_id["X"]["engine_missing"] is False
    assert diff["improved"] == 1
    assert diff["degraded"] == 1
    assert diff["added_findings"] == 2


def test_build_counterfactual_summary_replaces_estimated_cost_slots():
    from run_pro_second_pass_reevaluation import (
        BASELINE_COST_EST_PER_BLOCK,
        build_counterfactual_summary,
    )

    baseline_summary = {
        "total_input_blocks": 2,
        "coverage_pct": 50.0,
        "missing_count": 1,
        "total_findings": 1,
        "total_key_values": 4,
        "blocks_with_findings": 1,
        "total_cost_usd": 0.1000,
        "cost_sources_actual": 1,
        "cost_sources_estimated": 1,
    }
    counterfactual_diff = {
        "engine_total_findings": 5,
        "engine_total_kv": 9,
        "per_block_diff": [
            {"engine_findings": 4},
            {"engine_findings": 1},
        ],
    }
    replacements = {
        "X": _obs("X", "A", 1, 4, 8),
    }

    summary = build_counterfactual_summary(
        baseline_summary,
        counterfactual_diff,
        replacements,
    )

    expected_cost = 0.1000 - BASELINE_COST_EST_PER_BLOCK + 0.05
    assert summary["coverage_pct"] == 100.0
    assert summary["missing_count"] == 0
    assert summary["total_findings"] == 5
    assert summary["blocks_with_findings"] == 2
    assert summary["cost_sources_actual"] == 2
    assert summary["cost_sources_estimated"] == 0
    assert summary["total_cost_usd"] == round(expected_cost, 6)
