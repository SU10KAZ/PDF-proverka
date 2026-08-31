"""Stage 03 emits candidates; it never publishes normative evidence."""
from __future__ import annotations

import json

from backend.app.pipeline.stages.findings_merge.normative_references import (
    extract_designations,
    harden_finding_normative_references,
    harden_normative_references,
    normalize_designation,
)
from norms._core import extract_norms_from_text


def _harden(finding: dict) -> dict:
    harden_finding_normative_references(finding)
    return finding


def test_document_without_clause_stays_an_unproved_candidate():
    finding = _harden({
        "id": "F-001",
        "norm": "ГОСТ 21.110-2013, требования к оформлению спецификации",
        "norm_quote": None,
    })

    ref = finding["candidate_norm_references"][0]
    assert ref["designation"] == "ГОСТ 21.110-2013"
    assert ref["clause_candidate"] is None
    assert ref["quote_candidate"] is None
    assert finding["norm_references"] == []
    assert finding["norm_quote"] is None
    assert finding["norm_paragraph_state"] == "resolver_pending"


def test_model_clause_and_quote_survive_only_as_unproved_hints():
    finding = _harden({
        "id": "F-001",
        "norm": "СП 30.13330.2020, п. 8.6",
        "norm_quote": "Чужая правдоподобная цитата",
    })

    ref = finding["candidate_norm_references"][0]
    assert ref["clause_candidate"] == "8.6"
    assert ref["quote_candidate"] == "Чужая правдоподобная цитата"
    assert ref["provenance"]["producer"] == "stage03_candidate_contract"
    assert finding["norm_references"] == []
    assert finding["norm_quote"] is None


def test_structured_candidate_has_required_contract_fields():
    finding = _harden({
        "id": "F-002",
        "source_finding_ids": ["G-7"],
        "candidate_norm_references": [{
            "designation": "ГОСТ 21.601-2011",
            "candidate_relevance": 0.83,
            "reason": "Общие данные рабочих чертежей",
            "clause_candidate": "5.1",
            "quote_candidate": "модельная подсказка",
        }],
    })

    ref = finding["candidate_norm_references"][0]
    assert set(("designation", "candidate_relevance", "reason", "provenance")) <= set(ref)
    assert ref["candidate_relevance"] == 0.83
    assert ref["provenance"]["source_finding_ids"] == ["G-7"]


def test_confirmed_alias_and_typo_mappings_are_deterministic():
    expected = {
        "ГОСТ 21.101-2020": "ГОСТ Р 21.101-2020",
        "ГОСТ Р 21.110-2013": "ГОСТ 21.110-2013",
        "ГОСТ 17624-2013": "ГОСТ 17624-2021",
        "ГОСТ 21.608-2020": "ГОСТ 21.608-2021",
        "ГОСТ 9.602-2020": "ГОСТ 9.602-2016",
        "СП 256.132580.2016": "СП 256.1325800.2016",
        "СП 61.13330.2021": "СП 61.13330.2021",
        "ПП РФ № 1479": "ПП РФ №1479",
    }
    assert {source: normalize_designation(source)[0] for source in expected} == expected

    finding = _harden({"id": "F-001", "norm": "ПП РФ № 1479; СП 256.132580.2016"})
    assert [r["designation"] for r in finding["candidate_norm_references"]] == [
        "ПП РФ №1479", "СП 256.1325800.2016",
    ]


def test_multi_norm_legacy_quote_is_not_copied_to_any_candidate():
    finding = _harden({
        "id": "F-010",
        "norm": "ГОСТ 21.110-2013, п. 5.1; ГОСТ 21.601-2011, п. 5.1",
        "norm_quote": "Одна цитата не может принадлежать двум документам",
    })

    refs = finding["candidate_norm_references"]
    assert len(refs) == 2
    assert [ref["clause_candidate"] for ref in refs] == ["5.1", "5.1"]
    assert all(ref["quote_candidate"] is None for ref in refs)
    assert finding["norm_quote"] is None


def test_current_edition_in_legacy_status_note_is_not_a_second_candidate():
    finding = _harden({
        "id": "F-HIST",
        "norm": (
            "ГОСТ Р 21.101-2020 (заменён; актуальная редакция: "
            "ГОСТ Р 21.101-2026), пункт не подтверждён"
        ),
    })
    assert [ref["designation"] for ref in finding["candidate_norm_references"]] == [
        "ГОСТ Р 21.101-2020"
    ]


def test_so_extractor_accepts_standard_and_rejects_russian_prose():
    prose = "Этажи со 117 по 144; высоты со 169 по 200; диапазон со 2-."
    assert extract_designations(prose) == []
    assert extract_norms_from_text(prose) == []
    assert extract_designations("СО 117") == []
    assert extract_norms_from_text("СО 117") == []
    assert extract_designations("СО 153-34.20.501-2003") == ["СО 153-34.20.501-2003"]
    assert extract_norms_from_text("СО 153-34.20.501-2003") == ["СО 153-34.20.501-2003"]
    assert extract_designations("СО-153-34.21.122-2003") == ["СО 153-34.21.122-2003"]
    assert extract_norms_from_text("СО-153-34.21.122-2003") == ["СО-153-34.21.122-2003"]


def test_file_publication_writes_candidate_telemetry(tmp_path):
    path = tmp_path / "03_findings.json"
    path.write_text(json.dumps({
        "meta": {"normative_reference_hardening": {"legacy": True}},
        "findings": [{
            "id": "F-001",
            "norm": "ГОСТ Р 21.110-2013, требования к спецификации",
        }],
    }, ensure_ascii=False), encoding="utf-8")

    report = harden_normative_references(tmp_path)
    output = json.loads(path.read_text(encoding="utf-8"))
    ref = output["findings"][0]["candidate_norm_references"][0]
    assert report == {
        "ok": True, "findings": 1, "candidates": 1,
        "with_clause_candidate": 0, "normalized": 1,
    }
    assert ref["designation"] == "ГОСТ 21.110-2013"
    assert output["findings"][0]["norm_references"] == []
    assert output["meta"]["normative_candidate_contract"]["candidates"] == 1
    assert "normative_reference_hardening" not in output["meta"]
