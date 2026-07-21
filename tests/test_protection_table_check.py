from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
    gate_findings,
)
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    combine_detector_results,
)
from backend.app.pipeline.stages.block_analysis.protection_table_check import (
    DETECTOR_MODEL,
    detect_outgoing_setting_findings,
    extract_block_vector_words,
    run_protection_table_detector,
)
from backend.app.pipeline.stages.block_analysis.provenance import detector_for_model


ROOT = Path(__file__).resolve().parents[1]
BLOCK_ID = "blk_5850f08c8fd0407fb58e4271cd648198"


def _word(x: float, y: float, text: str, block: int, line: int, width: float = 10.0):
    return (x, y, x + width, y + 7.0, text, block, line, 0)


def _table_words(*, setting: str = "50", imax: str = "52,6"):
    words = [
        _word(10, 20, "№", 1, 0),
        _word(24, 20, "фидера", 1, 0, 28),
        _word(198, 20, "13", 1, 0),
        _word(238, 20, "14", 1, 0),
        _word(10, 40, "Максимальный", 2, 0, 50),
        _word(64, 40, "ток", 2, 0),
        _word(78, 40, "линии,", 2, 0, 24),
        _word(106, 40, "А", 2, 0),
        _word(196, 40, imax, 2, 0, 18),
        _word(236, 40, "31,5", 2, 0, 18),
        _word(10, 60, "Ток", 3, 0),
        _word(24, 60, "аппарата", 3, 0, 34),
        _word(62, 60, "защиты,", 3, 0, 28),
        _word(94, 60, "А", 3, 0),
        _word(198, 60, setting, 3, 0),
        _word(238, 60, "40", 3, 0),
    ]
    return words


def _package(source_kind: str = "raw_vector"):
    return {
        "source_kind": source_kind,
        "markdown": """## Точный текст блока из вектор-слоя PDF
Послеаварийный режим
Iр = 3667,1 А
ВП 1
3500/5,кл.0.5
3QF
2000А
1 СЕКЦИЯ ШИН РУНН (0,4кВ)
№ фидера
Максимальный ток линии, А
Ток аппарата защиты, А
""",
    }


def test_detector_returns_all_three_exact_numeric_checks():
    result = run_protection_table_detector(
        _package(),
        block_id="B",
        vector_words=_table_words(),
    )

    assert result is not None and result["ok"] is True
    findings = result["parsed"]["findings"]
    assert [item["category"] for item in findings] == [
        "protection_tt_ratio",
        "protection_bus_breaker",
        "protection_outgoing_setting",
    ]
    assert "3500 А < 3667,1 А" in findings[0]["finding"]
    assert "2000 А < 3667,1 А" in findings[1]["finding"]
    assert "50 А < 52,6 А" in findings[2]["finding"]
    assert findings[2]["value_found"] == (
        "фидер 13; Ток аппарата защиты = 50 А; Максимальный ток линии = 52,6 А"
    )
    assert all(item["confidence"] == 1.0 for item in findings)
    assert all(item["counterevidence_checked"] is True for item in findings)


def test_feature_flag_defaults_off():
    from backend.app.core import config

    assert config.STAGE01_PROTECTION_TABLE_CHECK_ENABLED is False


def test_outgoing_values_are_bound_by_x_column_not_flat_text_order():
    words = _table_words(setting="63", imax="52,6")
    # An unrelated large Imax token in another X column must not be paired with 63 A.
    words.append(_word(278, 40, "500", 4, 0, 18))

    assert detect_outgoing_setting_findings(words) == []


def test_scan_is_not_applicable_and_parse_failures_are_silent():
    assert run_protection_table_detector(_package("image_only"), vector_words=[]) is None

    result = run_protection_table_detector(
        _package(),
        vector_words=[("broken",)],
    )
    assert result is not None and result["ok"] is True
    assert [item["category"] for item in result["parsed"]["findings"]] == [
        "protection_tt_ratio",
        "protection_bus_breaker",
    ]


def test_combine_assigns_deterministic_model_and_stable_ref():
    deterministic = run_protection_table_detector(
        _package(),
        vector_words=_table_words(),
    )
    combined = combine_detector_results(
        [
            ("codex/gpt-5.4", {"ok": True, "parsed": {"findings": []}}),
            (DETECTOR_MODEL, deterministic),
        ],
        run_id="run-1",
    )

    findings = combined["parsed"]["findings"]
    assert detector_for_model(DETECTOR_MODEL) == "deterministic"
    assert all(item["_detector_model"] == DETECTOR_MODEL for item in findings)
    assert [item["_detector_ref"] for item in findings] == [
        "deterministic:001",
        "deterministic:002",
        "deterministic:003",
    ]


def test_evidence_gate_does_not_apply_llm_cap_to_deterministic_findings():
    deterministic = run_protection_table_detector(
        _package(),
        vector_words=_table_words(),
    )["parsed"]["findings"]
    tagged = [{**item, "_detector_model": DETECTOR_MODEL} for item in deterministic]
    model_candidates = [
        {
            "severity": "КРИТИЧЕСКОЕ",
            "category": f"model_{index}",
            "finding": f"Legacy model candidate {index}",
            "value_found": str(index),
        }
        for index in range(5)
    ]

    published, deferred, report = gate_findings(
        [*model_candidates, *tagged],
        max_published=1,
    )

    assert sum(item.get("_detector_model") == DETECTOR_MODEL for item in published) == 3
    assert report["deterministic_published"] == 3
    assert report["published"] == 4
    assert len(deferred) == 4


def test_real_grsh_acceptance_block_is_stable_five_of_five():
    candidates = sorted(ROOT.glob(f"projects_v2/**/block_vector_graphs/{BLOCK_ID}.json"))
    if not candidates:
        pytest.skip("local acceptance project EM_1-1 is not available")
    package_path = candidates[0]
    output_dir = package_path.parent.parent
    package = json.loads(package_path.read_text(encoding="utf-8"))
    words = extract_block_vector_words(output_dir, BLOCK_ID)
    if not words:
        pytest.skip("local acceptance PDF/document_graph is not available")

    signatures = []
    for _ in range(5):
        result = run_protection_table_detector(
            package,
            output_dir=output_dir,
            block_id=BLOCK_ID,
            vector_words=words,
        )
        signatures.append([
            (item["category"], item["value_found"], item["finding"])
            for item in result["parsed"]["findings"]
        ])

    assert all(signature == signatures[0] for signature in signatures)
    assert [category for category, _, _ in signatures[0]] == [
        "protection_tt_ratio",
        "protection_bus_breaker",
        "protection_outgoing_setting",
    ]
    assert signatures[0][0][1] == "3500/5,кл.0.5; Iр = 3667,1 А"
    assert signatures[0][1][1] == "3QF · 2000 А; Iр = 3667,1 А"
    assert signatures[0][2][1] == (
        "фидер 13; Ток аппарата защиты = 50 А; Максимальный ток линии = 52,6 А"
    )
