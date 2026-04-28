"""Unit tests for Flash -> Pro selective triage policy.

No real OpenRouter/Gemini calls are made here.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope


def _block(block_id: str, *, size_kb: float = 40.0, page: int = 1, **extra) -> dict:
    out = {
        "block_id": block_id,
        "size_kb": size_kb,
        "page": page,
        "file": f"{block_id}.png",
        "ocr_label": f"label {block_id}",
        "ocr_text_len": extra.pop("ocr_text_len", 300),
    }
    out.update(extra)
    return out


def _analysis(
    block_id: str,
    *,
    findings: int = 0,
    severity: str = "РЕКОМЕНДАТЕЛЬНОЕ",
    kv: int = 5,
    summary_len: int = 90,
    unreadable: bool = False,
) -> dict:
    return {
        "block_id": block_id,
        "page": 1,
        "label": f"label {block_id}",
        "summary": "S" * summary_len,
        "key_values_read": [f"kv_{i}" for i in range(kv)],
        "findings": [
            {
                "id": f"G-{i:03d}",
                "severity": severity,
                "category": "test",
                "finding": "test finding",
                "norm": "",
                "norm_quote": None,
                "block_evidence": "",
                "value_found": "",
            }
            for i in range(findings)
        ],
        "unreadable_text": unreadable,
        "unreadable_details": None,
    }


def _result(block_id: str, analysis: dict | None = None, *, is_error: bool = False) -> BatchResultEnvelope:
    return BatchResultEnvelope(
        batch_id=1,
        model_id="google/gemini-2.5-flash",
        input_block_ids=[block_id],
        is_error=is_error,
        parsed_data=None
        if is_error
        else {
            "batch_id": 1,
            "project_id": "test",
            "timestamp": "",
            "block_analyses": [analysis or _analysis(block_id)],
        },
    )


def test_complex_flash_finding_selected_for_pro():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [_block("B1", size_kb=900)]
    results = [_result("B1", _analysis("B1", findings=1))]

    entries, summary = select_pro_escalation_blocks(blocks, results)

    assert [e.block_id for e in entries] == ["B1"]
    assert "complex_block_with_flash_findings" in entries[0].reasons
    assert summary["selected_for_pro"] == 1


def test_simple_light_finding_stays_flash_only_by_default():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [_block("B1", size_kb=40)]
    results = [_result("B1", _analysis("B1", findings=1, kv=5, summary_len=100))]

    entries, summary = select_pro_escalation_blocks(blocks, results)

    assert entries == []
    assert summary["simple_findings_flash_only"] == 1


def test_include_simple_findings_escalates_light_finding():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [_block("B1", size_kb=40)]
    results = [_result("B1", _analysis("B1", findings=1, kv=5, summary_len=100))]

    entries, _ = select_pro_escalation_blocks(
        blocks,
        results,
        include_simple_findings=True,
    )

    assert [e.block_id for e in entries] == ["B1"]
    assert "all_flash_findings_enabled" in entries[0].reasons


def test_high_value_finding_escalates_even_light_block():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [_block("B1", size_kb=40)]
    results = [_result("B1", _analysis("B1", findings=1, severity="КРИТИЧЕСКОЕ"))]

    entries, _ = select_pro_escalation_blocks(blocks, results)

    assert [e.block_id for e in entries] == ["B1"]
    assert "high_value_flash_finding" in entries[0].reasons


def test_complex_failed_flash_block_selected_for_rescue():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [_block("B1", size_kb=900)]
    results = [_result("B1", is_error=True)]

    entries, _ = select_pro_escalation_blocks(blocks, results)

    assert [e.block_id for e in entries] == ["B1"]
    assert "complex_flash_missing_or_failed" in entries[0].reasons


def test_max_pro_blocks_keeps_highest_priority_entries():
    from run_stage02_flash_pro_triage import select_pro_escalation_blocks

    blocks = [
        _block("FAILED", size_kb=900, page=1),
        _block("COMPLEX", size_kb=900, page=2),
        _block("CRITICAL", size_kb=40, page=3),
    ]
    results = [
        _result("FAILED", is_error=True),
        _result("COMPLEX", _analysis("COMPLEX", findings=1)),
        _result("CRITICAL", _analysis("CRITICAL", findings=1, severity="КРИТИЧЕСКОЕ")),
    ]

    entries, summary = select_pro_escalation_blocks(blocks, results, max_pro_blocks=2)

    assert [e.block_id for e in entries] == ["FAILED", "COMPLEX"]
    assert summary["selected_for_pro_before_cap"] == 3
    assert summary["selected_for_pro"] == 2


def test_budget_stop_logic_blocks_expensive_pro_pass():
    from run_stage02_flash_pro_triage import estimate_triage_cost

    estimate = estimate_triage_cost(
        total_blocks=215,
        pro_blocks=100,
        pro_cost_per_block=0.10,
        max_pro_cost_usd=5.0,
    )

    assert estimate.pro_budget_ok is False
    assert estimate.stop_reason


def test_merge_prefers_successful_pro_for_escalated_block():
    from run_stage02_flash_pro_triage import merge_flash_and_pro_results

    blocks = [_block("B1"), _block("B2")]
    flash = [
        _result("B1", _analysis("B1", findings=1)),
        _result("B2", _analysis("B2", findings=0)),
    ]
    pro = [_result("B1", _analysis("B1", findings=2))]

    rows, summary = merge_flash_and_pro_results(blocks, flash, pro, ["B1"])

    by_id = {r["block_id"]: r for r in rows}
    assert by_id["B1"]["_triage_source"] == "pro_single_block"
    assert len(by_id["B1"]["findings"]) == 2
    assert by_id["B2"]["_triage_source"] == "flash_single_block"
    assert summary["used_pro"] == 1
    assert summary["flash_fallback_after_pro_failure"] == 0


def test_merge_falls_back_to_flash_when_pro_missing():
    from run_stage02_flash_pro_triage import merge_flash_and_pro_results

    blocks = [_block("B1")]
    flash = [_result("B1", _analysis("B1", findings=1))]
    pro = []

    rows, summary = merge_flash_and_pro_results(blocks, flash, pro, ["B1"])

    assert rows[0]["_triage_source"] == "flash_fallback_pro_failed"
    assert len(rows[0]["findings"]) == 1
    assert summary["flash_fallback_after_pro_failure"] == 1


def test_write_stage02_output_creates_standard_artifact(tmp_path):
    from run_stage02_flash_pro_triage import write_stage02_output

    project_dir = tmp_path / "project.pdf"
    artifacts_dir = project_dir / "_experiments" / "stage02_flash_pro_triage" / "run"
    rows = [_analysis("B1", findings=1)]
    summary = {
        "total_blocks": 1,
        "coverage_pct": 100.0,
        "escalation_blocks": 1,
        "used_pro": 1,
        "flash_fallback_after_pro_failure": 0,
    }

    out_path = write_stage02_output(
        project_dir,
        rows,
        summary,
        source_artifacts_dir=artifacts_dir,
    )

    assert out_path.name == "02_blocks_analysis.json"
    data = __import__("json").loads(out_path.read_text(encoding="utf-8"))
    assert data["stage"] == "02_blocks_analysis"
    assert data["meta"]["source"] == "flash_pro_triage"
    assert data["meta"]["blocks_reviewed"] == 1
    assert data["block_analyses"][0]["block_id"] == "B1"


def test_stage_model_ui_hides_direct_gemini_and_allows_flash_pro_pair():
    from webapp.config import (
        AVAILABLE_MODELS,
        FLASH_PRO_TRIAGE_MODEL,
        STAGE_MODEL_RESTRICTIONS,
    )

    visible_ids = {m["id"] for m in AVAILABLE_MODELS}

    assert "gemini-2.5-flash" not in visible_ids
    assert "gemini-3.1-pro-preview" not in visible_ids
    assert "google/gemini-2.5-flash" in visible_ids
    assert "google/gemini-3.1-pro-preview" in visible_ids
    assert FLASH_PRO_TRIAGE_MODEL in STAGE_MODEL_RESTRICTIONS["block_batch"]


def test_before_after_comparison_flags_regressions():
    from compare_stage02_before_after import compare_rows

    before = [
        _analysis("B1", findings=1, kv=8, summary_len=120),
        _analysis("B2", findings=0, kv=4, summary_len=80, unreadable=True),
    ]
    after = [
        _analysis("B1", findings=0, kv=2, summary_len=20),
        {**_analysis("B2", findings=1, kv=6, summary_len=90, unreadable=False), "_triage_source": "pro"},
    ]

    summary, rows = compare_rows(before, after)

    assert summary["delta"]["total_findings"] == 0
    assert summary["pro_rechecked_blocks"] == 1
    b1 = next(r for r in rows if r["block_id"] == "B1")
    assert "findings_disappeared" in b1["review_reasons"]
    assert "kv_collapse" in b1["review_reasons"]
    assert "summary_collapse" in b1["review_reasons"]


def test_before_after_extracts_stage_payload_shape():
    from compare_stage02_before_after import extract_block_analyses, summarize_rows

    payload = {"stage": "02_blocks_analysis", "block_analyses": [_analysis("B1", findings=2)]}
    rows = extract_block_analyses(payload)
    summary = summarize_rows(rows)

    assert len(rows) == 1
    assert summary["total_findings"] == 2
    assert summary["blocks_with_findings"] == 1
