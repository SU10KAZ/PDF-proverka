from __future__ import annotations

import pytest
import json
from pathlib import Path

from backend.app.models.usage import LLMResult
from backend.app.pipeline.stages.block_analysis.dual_review import (
    apply_normalized_review,
    fallback_dual_review,
    normalize_review_payload,
    review_dual_findings,
)


def _finding(model: str, ref: str, text: str, **extra):
    return {
        "_detector_model": model,
        "_detector_ref": ref,
        "severity": "ЭКСПЛУАТАЦИОННОЕ",
        "category": extra.pop("category", "marking"),
        "finding": text,
        "norm_quote": None,
        "value_found": extra.pop("value_found", ""),
        "recommendation": extra.pop("recommendation", "Исправить обозначение."),
        **extra,
    }


def test_findings_merge_prompts_apply_dual_comparison_contract():
    root = Path(__file__).resolve().parents[1]
    for language in ("ru", "en"):
        prompt = (root / "prompts" / "pipeline" / language / "findings_merge_task.md").read_text(
            encoding="utf-8"
        )
        assert "detector_comparison" in prompt
        assert "source_finding_ids" in prompt
        assert "disputed" in prompt
        assert "gap_search" in prompt


def test_normalize_classifies_match_extension_new_disputed_and_gap():
    findings = [
        _finding("openai/gpt-5.4", "gpt_openrouter:001", "Не указана марка кабеля"),
        _finding("codex/gpt-5.4", "codex:001", "Марка кабеля отсутствует; нужно указать тип и сечение"),
        _finding("openai/gpt-5.4", "gpt_openrouter:002", "Автомат имеет номинал 16 А", value_found="16 А"),
        _finding("codex/gpt-5.4", "codex:002", "На этой линии указан автомат 25 А", value_found="25 А"),
        _finding("codex/gpt-5.4", "codex:003", "Нет номера листа", category="sheet_reference"),
    ]
    payload = {
        "relationships": [
            {
                "gpt_ref": "gpt_openrouter:001",
                "codex_ref": "codex:001",
                "relation": "extension",
                "extends": "codex",
                "confidence": 0.93,
                "reason": "Codex добавил требование о сечении.",
            },
            {
                "gpt_ref": "gpt_openrouter:002",
                "codex_ref": "codex:002",
                "relation": "disputed",
                "extends": "none",
                "confidence": 0.88,
                "reason": "Детекторы прочитали разные номиналы.",
            },
        ],
        "gap_findings": [
            {
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "legend",
                "finding": "Условное обозначение X1 отсутствует в легенде",
                "norm_quote": None,
                "value_found": "X1",
                "recommendation": "Добавить X1 в легенду.",
            }
        ],
        "gap_search": {
            "performed": True,
            "status": "gaps_found",
            "searched_categories": ["маркировки", "легенда"],
            "summary": "Найден один пробел.",
        },
    }

    normalized = normalize_review_payload(
        payload,
        findings,
        reviewer_model="codex/gpt-5.4",
        gap_search_enabled=True,
    )

    assert normalized["report"]["counts"] == {
        "matches": 0,
        "extensions": 1,
        "new": 2,
        "disputed": 1,
        "gap_findings": 1,
    }
    assert normalized["annotations"]["codex:001"]["role"] == "extends"
    assert normalized["annotations"]["gpt_openrouter:001"]["role"] == "base"
    assert normalized["annotations"]["codex:003"]["relation"] == "new"
    assert normalized["report"]["gap_search"]["performed"] is True


def test_gap_search_rejects_probable_duplicate_of_known_finding():
    findings = [
        _finding("openai/gpt-5.4", "gpt_openrouter:001", "Не указана марка кабеля ЩР-1"),
        _finding("codex/gpt-5.4", "codex:001", "Отсутствует марка кабеля ЩР-1"),
    ]
    payload = {
        "relationships": [],
        "gap_findings": [{
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "marking",
            "finding": "Не указана марка кабеля ЩР-1",
            "norm_quote": None,
            "value_found": "ЩР-1",
            "recommendation": "Указать марку кабеля.",
        }],
        "gap_search": {"performed": True, "status": "gaps_found"},
    }

    normalized = normalize_review_payload(
        payload,
        findings,
        reviewer_model="codex/gpt-5.4",
        gap_search_enabled=True,
    )

    assert normalized["gap_findings"] == []
    assert normalized["report"]["gap_search"]["duplicates_rejected"] == 1
    assert normalized["report"]["counts"]["gap_findings"] == 0


def test_apply_review_marks_gap_finding_as_separate_codex_detection():
    findings = [
        _finding("openai/gpt-5.4", "gpt_openrouter:001", "Ошибка GPT"),
        _finding("codex/gpt-5.4", "codex:001", "Ошибка Codex"),
    ]
    normalized = normalize_review_payload(
        {
            "relationships": [],
            "gap_findings": [{
                "severity": "КРИТИЧЕСКОЕ",
                "category": "power",
                "finding": "Не показано резервное питание",
                "norm_quote": None,
                "value_found": "АВР отсутствует",
                "recommendation": "Показать резервный ввод.",
            }],
            "gap_search": {"performed": True, "status": "gaps_found"},
        },
        findings,
        reviewer_model="codex/gpt-5.4",
        gap_search_enabled=True,
    )

    output = apply_normalized_review(
        findings,
        normalized,
        reviewer_model="codex/gpt-5.4",
        run_id="run-1",
    )

    assert len(output) == 3
    gap = output[-1]
    assert gap["_detector_model"] == "codex/gpt-5.4"
    assert gap["_detection_mode"] == "gap_search"
    assert gap["_comparison"]["origin"] == "gap_search"
    assert gap["_detector_ref"] == "codex_gap:001"


def test_fallback_keeps_findings_and_records_review_failure():
    findings = [
        _finding("openai/gpt-5.4", "gpt_openrouter:001", "Не указана марка кабеля"),
        _finding("codex/gpt-5.4", "codex:001", "Марка кабеля не указана"),
    ]

    result = fallback_dual_review(
        findings,
        reviewer_model="codex/gpt-5.4",
        run_id="run-fallback",
        gap_search_enabled=True,
        error="codex unavailable",
    )

    assert len(result["findings"]) == 2
    assert result["report"]["status"] == "fallback"
    assert result["report"]["error"] == "codex unavailable"
    assert result["report"]["gap_search"]["performed"] is False


@pytest.mark.asyncio
async def test_review_dual_findings_runs_one_combined_review_and_gap_call(monkeypatch, tmp_path):
    findings = [
        _finding("openai/gpt-5.4", "gpt_openrouter:001", "Не указана марка кабеля"),
        _finding("codex/gpt-5.4", "codex:001", "Марка кабеля отсутствует"),
    ]
    seen = {}

    async def fake_run(messages, **kwargs):
        seen["messages"] = messages
        seen.update(kwargs)
        return LLMResult(
            text='{"relationships": []}',
            json_data={
                "relationships": [{
                    "gpt_ref": "gpt_openrouter:001",
                    "codex_ref": "codex:001",
                    "relation": "match",
                    "extends": "none",
                    "confidence": 0.96,
                    "reason": "Одна проблема.",
                }],
                "gap_findings": [],
                "gap_search": {
                    "performed": True,
                    "status": "no_new_findings",
                    "searched_categories": ["маркировки"],
                    "summary": "Новых проблем нет.",
                },
            },
            model="codex/gpt-5.4",
            output_tokens=123,
            duration_ms=456,
        )

    monkeypatch.setattr(
        "backend.app.services.llm.codex_runner.run_codex_json_messages",
        fake_run,
    )
    image = tmp_path / "block.png"
    image.write_bytes(b"png")

    result = await review_dual_findings(
        findings,
        block_id="B-1",
        page=3,
        block_context="VECTOR CONTEXT",
        image_path=image,
        reviewer_model="codex/gpt-5.4",
        run_id="run-1",
        project_id="P-1",
        timeout=30,
        gap_search_enabled=True,
    )

    assert result["report"]["counts"]["matches"] == 1
    assert result["report"]["gap_search"]["status"] == "no_new_findings"
    assert result["output_tokens"] == 123
    assert seen["stage"] == "block_analysis_dual_review"
    assert seen["image_paths"] == [image]
    assert "VECTOR CONTEXT" in seen["messages"][1]["content"]


@pytest.mark.asyncio
async def test_stage01_dual_runner_persists_review_contract(monkeypatch, tmp_path):
    from backend.app.core.config import STAGE02_DUAL_MODEL_ID
    from backend.app.pipeline.stages.block_analysis import dual_review
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
    from backend.app.pipeline.stages.block_context.contract import SCHEMA_VERSION
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
        stage02_crop_policy,
    )

    project_dir = tmp_path / "project"
    output_dir = project_dir / "_output"
    blocks_dir = output_dir / "blocks_stage02_100"
    blocks_dir.mkdir(parents=True)
    (blocks_dir / "B-1.png").write_bytes(b"png")
    index = {
        **stage02_crop_policy(),
        "blocks": [{"block_id": "B-1", "page": 1, "file": "B-1.png", "size_kb": 1}],
    }
    (blocks_dir / "index.json").write_text(json.dumps(index), encoding="utf-8")
    (output_dir / "document_graph.json").write_text(
        json.dumps({"pages": [{"page": 1, "sheet_no": "1", "text_blocks": []}]}),
        encoding="utf-8",
    )
    (output_dir / "block_context_summary.json").write_text(
        json.dumps({
            "schema_version": SCHEMA_VERSION,
            "stage": "block_context",
            "reference_catalog": {
                "runtime_source": "pipeline_stage_embedded_catalog",
                "records_total": 1,
            },
            "status": "ok",
            "blocks_total": 1,
            "blocks_ready": 1,
            "blocks_failed": 0,
            "source_counts": {"raw_vector": 1},
            "blocks": [{
                "block_id": "B-1",
                "page": 1,
                "source_kind": "raw_vector",
                "coverage_status": "ready",
                "warnings": [],
            }],
        }),
        encoding="utf-8",
    )

    async def fake_gpt(*_args, **_kwargs):
        return {
            "ok": True,
            "parsed": {"findings": [{
                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                "category": "marking",
                "finding": "GPT finding",
                "norm_quote": None,
                "value_found": "G",
                "recommendation": "Исправить G.",
            }]},
            "input_tokens": 100,
            "output_tokens": 20,
            "elapsed_ms": 10,
            "context_source": "raw_vector",
        }

    async def fake_codex(*_args, **_kwargs):
        return {
            "ok": True,
            "parsed": {"findings": [{
                "severity": "ЭКСПЛУАТАЦИОННОЕ",
                "category": "marking",
                "finding": "Codex finding",
                "norm_quote": None,
                "value_found": "C",
                "recommendation": "Исправить C.",
            }]},
            "input_tokens": 0,
            "output_tokens": 30,
            "elapsed_ms": 15,
            "context_source": "raw_vector",
        }

    async def fake_review(findings, **_kwargs):
        normalized = dual_review.normalize_review_payload(
            {
                "relationships": [{
                    "gpt_ref": "gpt_openrouter:001",
                    "codex_ref": "codex:001",
                    "relation": "extension",
                    "extends": "both",
                    "confidence": 0.9,
                    "reason": "Взаимные дополнения.",
                }],
                "gap_findings": [{
                    "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                    "category": "gap",
                    "finding": "Gap finding",
                    "norm_quote": None,
                    "value_found": "X",
                    "recommendation": "Исправить X.",
                }],
                "gap_search": {"performed": True, "status": "gaps_found"},
            },
            findings,
            reviewer_model="codex/gpt-5.4",
            gap_search_enabled=True,
        )
        return {
            "findings": dual_review.apply_normalized_review(
                findings,
                normalized,
                reviewer_model="codex/gpt-5.4",
                run_id="run-1",
            ),
            "report": normalized["report"],
            "input_tokens": 0,
            "output_tokens": 40,
            "elapsed_ms": 20,
            "raw_content": "{}",
        }

    monkeypatch.setattr(gfo, "call_gpt_for_block", fake_gpt)
    monkeypatch.setattr(gfo, "call_codex_for_block", fake_codex)
    monkeypatch.setattr(gfo, "build_effective_block_context", lambda *_a, **_k: ("CTX", "raw_vector"))
    monkeypatch.setattr(gfo, "load_version_project_info", lambda *_a, **_k: {"project_id": "P-1", "section": "EOM"})
    monkeypatch.setattr(gfo, "STAGE01_DUAL_REVIEW_ENABLED", True)
    monkeypatch.setattr(gfo, "STAGE01_DUAL_GAP_SEARCH_ENABLED", True)
    monkeypatch.setattr(dual_review, "review_dual_findings", fake_review)

    result = await gfo.run_findings_only_for_project(
        project_dir,
        output_dir_override=output_dir,
        model=STAGE02_DUAL_MODEL_ID,
        api_key="test-key",
        timeout_s=5,
        write_run_log=False,
    )

    doc = result["output_doc"]
    meta = doc["stage01_meta"]["dual_review"]
    block = doc["block_analyses"][0]
    assert meta["counts"] == {
        "matches": 0,
        "extensions": 1,
        "new": 1,
        "disputed": 0,
        "gap_findings": 1,
    }
    assert meta["review_calls"] == 1
    assert block["dual_review"]["status"] == "ok"
    assert len(block["findings"]) == 3
    assert block["findings"][-1]["provenance"]["detections"][0]["mode"] == "gap_search"
    assert result["summary"]["api_calls_total"] == 3
