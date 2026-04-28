from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def test_retry_healing_turns_off_only_when_initial_healing_is_on():
    from run_pro_second_pass_reeval_small_project import _retry_healing

    assert _retry_healing(True) is False
    assert _retry_healing(False) is False


def test_classify_response_requires_single_matching_block_id():
    from run_pro_second_pass_reeval_small_project import (
        STATUS_MULTI,
        STATUS_SUCCESS,
        STATUS_WRONG_ID,
        _classify_response,
    )

    success = {
        "block_analyses": [
            {"block_id": "A", "summary": "ok", "findings": [], "key_values_read": []}
        ]
    }
    wrong = {
        "block_analyses": [
            {"block_id": "B", "summary": "ok", "findings": [], "key_values_read": []}
        ]
    }
    multi = {
        "block_analyses": [
            {"block_id": "A"},
            {"block_id": "A"},
        ]
    }

    assert _classify_response(success, "A")[0] == STATUS_SUCCESS
    assert _classify_response(wrong, "A")[0] == STATUS_WRONG_ID
    assert _classify_response(multi, "A")[0] == STATUS_MULTI


def test_rank_rows_prefers_complete_then_improved_then_added_then_degraded():
    from run_pro_second_pass_reeval_small_project import rank_rows

    rows = [
        {
            "engine_id": "pro_low_p2",
            "coverage_pct": 100.0,
            "missing_count": 0,
            "duplicate_count": 0,
            "extra_count": 0,
            "improved": 10,
            "added_findings": 15,
            "degraded": 3,
            "total_cost_usd": 0.2,
            "elapsed_s": 100.0,
        },
        {
            "engine_id": "claude_reused",
            "coverage_pct": 100.0,
            "missing_count": 0,
            "duplicate_count": 0,
            "extra_count": 0,
            "improved": 15,
            "added_findings": 46,
            "degraded": 1,
            "total_cost_usd": 0.54,
            "elapsed_s": 260.0,
        },
        {
            "engine_id": "pro_high_p2",
            "coverage_pct": 88.24,
            "missing_count": 2,
            "duplicate_count": 0,
            "extra_count": 0,
            "improved": 20,
            "added_findings": 99,
            "degraded": 0,
            "total_cost_usd": 0.9,
            "elapsed_s": 400.0,
        },
    ]

    ranked = rank_rows(rows)
    assert ranked[0]["engine_id"] == "claude_reused"
    assert ranked[1]["engine_id"] == "pro_low_p2"
