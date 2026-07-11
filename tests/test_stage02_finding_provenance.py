from __future__ import annotations

import json

from backend.app.pipeline.stages.block_analysis.provenance import (
    aggregate_traceability,
    backfill_final_findings_provenance,
    build_finding_provenance,
    detector_for_model,
)
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    build_effective_block_context,
    combine_detector_results,
)


def _write(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_detector_for_model_distinguishes_openrouter_and_codex():
    assert detector_for_model("openai/gpt-5.4") == "gpt_openrouter"
    assert detector_for_model("codex/gpt-5.4") == "codex"
    assert detector_for_model("claude-sonnet-4-6") == "claude"


def test_traceability_merges_independent_gpt_and_codex_detections():
    gpt = {
        "source_finding_ids": ["G-001"],
        "provenance": build_finding_provenance(
            model="openai/gpt-5.4", run_id="gpt-run", raw_finding_id="G-001"
        ),
    }
    codex = {
        "source_finding_ids": ["G-002"],
        "provenance": build_finding_provenance(
            model="codex/gpt-5.4", run_id="codex-run", raw_finding_id="G-002"
        ),
    }

    merged = aggregate_traceability([gpt, codex])

    assert merged["source_finding_ids"] == ["G-001", "G-002"]
    assert merged["provenance"]["found_by"] == ["gpt_openrouter", "codex"]
    assert merged["detector_summary"] == "gpt_codex"


def test_provenance_records_context_source_per_detection():
    provenance = build_finding_provenance(
        model="openai/gpt-5.4",
        run_id="stage01-run",
        raw_finding_id="G-001",
        context_source="structured_singleline",
    )

    assert provenance["context_source"] == "structured_singleline"
    assert provenance["detections"][0]["context_source"] == "structured_singleline"


def test_stage01_context_router_is_unconditional(tmp_path, monkeypatch):
    from backend.app.pipeline.stages.block_grounding import block_source_router

    monkeypatch.setattr(
        block_source_router,
        "resolve_block_source",
        lambda *_args, **_kwargs: ("VECTOR CONTEXT", "raw_vector"),
    )
    block = {"block_id": "B-1", "page": 1}
    text, source = build_effective_block_context(
        block,
        {"_gemma_skipped": "stage_disabled"},
        "page text",
        output_dir=tmp_path,
    )

    assert text == "VECTOR CONTEXT"
    assert source == "raw_vector"


def test_dual_result_keeps_raw_outputs_separate_and_paid_tokens_gpt_only():
    combined = combine_detector_results(
        [
            (
                "openai/gpt-5.4",
                {
                    "ok": True,
                    "parsed": {"findings": [{"finding": "GPT issue"}]},
                    "input_tokens": 100,
                    "output_tokens": 20,
                    "elapsed_ms": 10,
                    "context_source": "raw_vector",
                },
            ),
            (
                "codex/gpt-5.4",
                {
                    "ok": True,
                    "parsed": {"findings": [{"finding": "Codex issue"}]},
                    "input_tokens": 0,
                    "output_tokens": 500,
                    "elapsed_ms": 30,
                    "context_source": "raw_vector",
                },
            ),
        ],
        run_id="dual-run",
    )

    findings = combined["parsed"]["findings"]
    assert [item["_detector_model"] for item in findings] == [
        "openai/gpt-5.4",
        "codex/gpt-5.4",
    ]
    assert combined["detectors_complete"] is True
    assert combined["paid_input_tokens"] == 100
    assert combined["paid_output_tokens"] == 20
    assert combined["context_source"] == "raw_vector"


def test_stage02_model_restrictions_allow_codex_and_dual():
    from backend.app.core import config

    assert config.validate_stage_model_choice("block_batch", config.CODEX_STAGE_MODEL_ID) is None
    assert config.validate_stage_model_choice("block_batch", config.STAGE02_DUAL_MODEL_ID) is None


def test_backfill_credits_only_explicit_source_finding_ids(tmp_path):
    _write(
        tmp_path / "01_blocks_analysis.json",
        {
            "timestamp": "2026-07-11T10:00:00+00:00",
            "stage02_meta": {"model": "openai/gpt-5.4", "run_id": "mixed-run"},
            "block_analyses": [
                {
                    "block_id": "B-1",
                    "findings": [
                        {
                            "id": "G-001",
                            "provenance": build_finding_provenance(
                                model="openai/gpt-5.4",
                                run_id="gpt-run",
                                raw_finding_id="G-001",
                            ),
                        },
                        {
                            "id": "G-002",
                            "provenance": build_finding_provenance(
                                model="codex/gpt-5.4",
                                run_id="codex-run",
                                raw_finding_id="G-002",
                            ),
                        },
                    ],
                }
            ],
        },
    )
    _write(
        tmp_path / "03_findings.json",
        {
            "meta": {},
            "findings": [
                {
                    "id": "F-001",
                    "source_finding_ids": ["G-001", "G-002"],
                    "source_block_ids": ["B-1"],
                },
                {
                    "id": "F-002",
                    "source_block_ids": ["B-1"],
                },
            ],
        },
    )

    report = backfill_final_findings_provenance(tmp_path)
    saved = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))

    assert report["updated"] == 1
    assert saved["findings"][0]["detector_summary"] == "gpt_codex"
    assert saved["findings"][0]["provenance"]["found_by"] == [
        "gpt_openrouter",
        "codex",
    ]
    assert "provenance" not in saved["findings"][1]
    assert saved["meta"]["finding_source_counts"] == {
        "gpt": 0,
        "codex": 0,
        "gpt_codex": 1,
        "other": 0,
        "unattributed": 1,
    }


def test_ui_contains_detector_badge_next_to_finding_number():
    html = open("frontend/index.html", encoding="utf-8").read()
    js = open("frontend/static/js/app.js", encoding="utf-8").read()

    assert "finding-detector-badge" in html
    assert "GPT + Codex" in js
    assert "findingDetectorBadge" in js
    assert "Production Gemma" not in html
    assert "Gemma OCR" not in html
    assert "Production Gemma" not in js
