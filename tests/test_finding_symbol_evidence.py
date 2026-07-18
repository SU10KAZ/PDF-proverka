from __future__ import annotations

import json

import pytest

from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
    HOMOGLYPH_TO_CYRILLIC,
    SYMBOL_EVIDENCE_FIELD,
    gate_findings,
    normalize_homoglyph_token,
    observe_symbol_token_evidence,
)


def _source(text: str, page: int, *, kind: str = "image") -> dict:
    return {
        "text": text,
        "page": page,
        "block_kind": kind,
        "source": "pdf_vector_text",
    }


def _observation(item: dict) -> dict:
    return item[SYMBOL_EVIDENCE_FIELD][0]


@pytest.mark.parametrize(
    ("latin", "cyrillic"),
    list(HOMOGLYPH_TO_CYRILLIC.items()),
)
def test_homoglyph_table_normalizes_to_cyrillic(latin: str, cyrillic: str):
    assert normalize_homoglyph_token(latin) == cyrillic
    assert normalize_homoglyph_token(latin.lower()) == cyrillic
    assert normalize_homoglyph_token(cyrillic) == cyrillic


def test_domain_alias_and_internal_punctuation_are_conservative():
    assert normalize_homoglyph_token("УУРиО") == normalize_homoglyph_token("УЧРиО")
    assert normalize_homoglyph_token("П1") != normalize_homoglyph_token("П-1")
    assert normalize_homoglyph_token("П11.1") != normalize_homoglyph_token("П111")
    assert normalize_homoglyph_token("P1") != normalize_homoglyph_token("П1")


def test_homoglyph_mismatch_is_observed_with_raw_vector_offset():
    finding = {
        "problem": "Несоответствие обозначений: D11 против Д11.",
        "page": 8,
    }
    vector_text = "Ведомость дверей\nМарка Д11\n"

    observed, report = observe_symbol_token_evidence(
        [finding],
        vector_sources={"door-table": _source(vector_text, 8, kind="text")},
        enabled=True,
    )

    assert len(observed) == 1
    assert SYMBOL_EVIDENCE_FIELD not in finding, "входной finding не мутируется"
    receipt = _observation(observed[0])
    evidence = receipt["vector_evidence"]
    assert receipt["status"] == "ocr_artifact"
    assert receipt["normalized_token"] == "Д11"
    assert evidence["matched_text"] == "Д11"
    assert evidence["offset_start"] == vector_text.index("Д11")
    assert evidence["offset_end"] == vector_text.index("Д11") + len("Д11")
    assert evidence["evidence_text"] == "Марка Д11"
    assert evidence["offset_basis"] == "block_vector_text_unicode_chars"
    assert report["observation_counts"] == {"ocr_artifact": 1}


def test_real_mixed_alphabets_are_not_labeled_ocr_artifact():
    finding = {
        "problem": "Марки записаны разными алфавитами: Д11 и D11.",
        "page": 8,
    }
    sources = {
        "a": _source("Д11", 8),
        "b": _source("D11", 8),
    }

    observed, report = observe_symbol_token_evidence(
        [finding], vector_sources=sources, enabled=True,
    )

    assert SYMBOL_EVIDENCE_FIELD not in observed[0]
    assert report["annotated_findings"] == 0


def test_other_salient_numeric_difference_is_not_ocr_artifact():
    finding = {
        "problem": "Несоответствие обозначения «Д11, 900» и «D11, 850».",
        "description": "Ширина двери различается: 900 против 850 мм.",
        "page": 8,
    }

    observed, _ = observe_symbol_token_evidence(
        [finding],
        vector_sources={"a": _source("Д11\n900", 8)},
        enabled=True,
    )

    assert SYMBOL_EVIDENCE_FIELD not in observed[0]


def test_live_range_shape_finds_same_mark_without_expanding_range():
    finding = {
        "problem": (
            "Марки дверей записаны вперемешку кириллицей и латиницей: "
            "«Д…» против «D…»."
        ),
        "description": (
            "На планах используются Д11 и Д12, а спецификация распознана как "
            "D11-D17."
        ),
        "evidence": [{"type": "text", "block_id": "table", "page": 8}],
    }

    observed, _ = observe_symbol_token_evidence(
        [finding],
        vector_sources={"table": _source("Д11\nД12\nД17", 8, kind="text")},
        enabled=True,
    )

    receipt = _observation(observed[0])
    assert receipt["status"] == "ocr_artifact"
    assert receipt["normalized_token"] == "Д11"


def test_false_absence_uses_token_next_to_predicate_not_value_found():
    finding = {
        "finding": (
            "На схеме отсутствует позиционное обозначение 4QF31: "
            "после 4QF30 указан 1QF31, затем 4QF32."
        ),
        "value_found": "4QF30, 1QF31, 4QF32",
    }

    observed, report = observe_symbol_token_evidence(
        [finding],
        vector_sources={"panel": _source("4QF30\n4QF31\n4QF32", 4)},
        target_block_id="panel",
        target_page=4,
        enabled=True,
    )

    receipt = _observation(observed[0])
    assert receipt["status"] == "false_absence"
    assert receipt["quoted_tokens"] == ["4QF31"]
    assert receipt["vector_evidence"]["scope"] == "target_block"
    assert report["observation_counts"] == {"false_absence": 1}


def test_false_absence_finds_normalized_alias_in_same_page_neighbor():
    finding = {"finding": "Обозначение «УУРиО» отсутствует."}
    sources = {
        "target": _source("Основная надпись", 5),
        "neighbor": _source("УЧРиО\nОтветственный", 5, kind="text"),
        "other-page": _source("УЧРиО", 6),
    }

    observed, _ = observe_symbol_token_evidence(
        [finding],
        vector_sources=sources,
        target_block_id="target",
        target_page=5,
        enabled=True,
    )

    receipt = _observation(observed[0])
    evidence = receipt["vector_evidence"]
    assert receipt["status"] == "false_absence"
    assert receipt["normalization_applied"] is True
    assert evidence["block_id"] == "neighbor"
    assert evidence["scope"] == "same_page_neighbor"
    assert evidence["offset_start"] == 0


def test_false_absence_does_not_use_substring_or_another_page():
    finding = {"finding": "Обозначение «ПС1» отсутствует."}
    sources = {
        "target": _source("ПС10", 1),
        "another-sheet": _source("ПС1", 2),
    }

    observed, _ = observe_symbol_token_evidence(
        [finding],
        vector_sources=sources,
        target_block_id="target",
        target_page=1,
        enabled=True,
    )

    assert SYMBOL_EVIDENCE_FIELD not in observed[0]


def test_semantic_or_speculative_absence_is_not_token_checked():
    findings = [
        {"finding": "Подвижное соединение не показано."},
        {"finding": "Требуется проверить наличие обозначения «Д16»."},
    ]

    observed, _ = observe_symbol_token_evidence(
        findings,
        vector_sources={"b": _source("Д16", 1)},
        target_page=1,
        enabled=True,
    )

    assert all(SYMBOL_EVIDENCE_FIELD not in item for item in observed)


def test_observer_default_off_and_malformed_sources_are_fail_soft():
    finding = {"problem": "Несоответствие обозначений: D11 против Д11.", "page": 1}

    disabled, disabled_report = observe_symbol_token_evidence(
        [finding],
        vector_sources={"b": _source("Д11", 1)},
    )
    malformed, malformed_report = observe_symbol_token_evidence(
        [finding],
        vector_sources={"b": object()},
        document_graph={"pages": [None, "bad"]},
        enabled=True,
    )

    assert disabled == [finding]
    assert disabled_report["enabled"] is False
    assert malformed == [finding]
    assert malformed_report["annotated_findings"] == 0


def test_observation_does_not_change_publication_decision():
    finding = {
        "severity": "ЭКСПЛУАТАЦИОННОЕ",
        "category": "marking",
        "finding": "Несоответствие обозначений: D11 против Д11.",
        "value_found": "D11 / Д11",
        "claim_type": "contradiction",
        "problem_class": "mark_mismatch",
        "affected_entity": "дверь Д11",
        "evidence_quote": "D11 против Д11",
        "evidence_kind": "block_image",
        "context_status": "sufficient",
        "confidence": 0.95,
        "counterevidence_checked": True,
        "required_context": [],
    }
    baseline = gate_findings([finding])
    observed, _ = observe_symbol_token_evidence(
        [finding],
        vector_sources={"b": _source("Д11", 1)},
        target_block_id="b",
        target_page=1,
        enabled=True,
    )
    with_observation = gate_findings(observed)

    assert len(baseline[0]) == len(with_observation[0]) == 1
    assert len(baseline[1]) == len(with_observation[1]) == 0
    assert baseline[2]["reason_counts"] == with_observation[2]["reason_counts"]
    assert _observation(with_observation[0][0])["status"] == "ocr_artifact"


def test_stage01_production_adapter_preserves_observation():
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        adapt_findings_to_production,
    )

    raw = {
        "finding": "Несоответствие обозначений: D11 против Д11.",
        "severity": "ЭКСПЛУАТАЦИОННОЕ",
        SYMBOL_EVIDENCE_FIELD: [{
            "status": "ocr_artifact",
            "vector_evidence": {
                "block_id": "b",
                "offset_start": 4,
                "offset_end": 7,
                "evidence_text": "Д11",
            },
        }],
    }

    adapted = adapt_findings_to_production(
        raw_findings=[raw],
        block_id="b",
        finding_id_counter=[0],
    )

    assert _observation(adapted[0])["status"] == "ocr_artifact"
    assert _observation(adapted[0])["vector_evidence"]["offset_start"] == 4


def test_targeted_union_runs_observer_only_when_flag_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    from backend.app.core import config
    from backend.app.pipeline.stages.block_grounding import block_source_router
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import (
        combine_findings_with_targeted,
    )

    monkeypatch.setattr(
        config, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", True, raising=False,
    )
    monkeypatch.setattr(
        block_source_router,
        "vector_text_block_index",
        lambda _output_dir: {
            "table": _source("Д11\nД12\nД17", 8, kind="text"),
        },
    )
    (tmp_path / "document_graph.json").write_text(
        json.dumps({
            "pages": [{
                "page": 8,
                "text_blocks": [{"id": "table"}],
                "image_blocks": [],
            }],
        }),
        encoding="utf-8",
    )
    targeted = [("alia_mark_system_audit", {"findings": [{
        "problem": "Марки записаны разными алфавитами: Д11 против D11.",
        "page": 8,
        "evidence": [{"type": "text", "block_id": "table", "page": 8}],
    }]})]

    combined = combine_findings_with_targeted(
        {"findings": []},
        targeted,
        output_dir=tmp_path,
    )

    assert _observation(combined["findings"][0])["status"] == "ocr_artifact"
    observer_meta = combined["meta"]["finding_evidence_observer"]
    assert observer_meta["observe_only"] is True
    assert observer_meta["observation_counts"] == {"ocr_artifact": 1}
