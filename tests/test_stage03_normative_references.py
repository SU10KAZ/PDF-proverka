"""Stage 03 normative-reference hardening and VK-4-2-RD regressions."""
from __future__ import annotations

import json

from backend.app.pipeline.stages.findings_merge.normative_references import (
    extract_designations,
    harden_finding_normative_references,
    harden_normative_references,
    normalize_designation,
)
from norms._core import extract_norms_from_text


class _FakeNormsApi:
    STATUSES = {
        "ГОСТ Р 21.101-2020": "active",
        "ГОСТ 21.110-2013": "active",
        "СП 256.1325800.2016": "active",
        "СП 30.13330.2020": "active",
        "СП 29.13330.2011": "active",
        "ГОСТ 21.601-2011": "active",
        "ГОСТ 21.1101-2013": "replaced",
    }
    PARAGRAPHS = {
        ("ГОСТ 21.601-2011", "5.1"): (
            "5.1 В состав общих данных по рабочим чертежам систем "
            "водоснабжения и канализации включают данные по водопотреблению."
        ),
        ("СП 29.13330.2011", "8.6"): (
            "8.6 Толщина стяжки с охлаждающими трубами должна составлять 140 мм."
        ),
    }

    def get_norm_status(self, code):
        status = self.STATUSES.get(code)
        return {
            "found": status is not None,
            "authoritative": status is not None,
            "status": status or "unknown",
            "resolution_reason": "exact" if status else "not_in_index",
            "replacement_doc": (
                "ГОСТ Р 21.101-2020" if code == "ГОСТ 21.1101-2013" else None
            ),
        }

    def get_paragraph(self, code, paragraph, max_lines=20):
        text = self.PARAGRAPHS.get((code, paragraph))
        return {
            "found": text is not None,
            "authoritative": text is not None,
            "text": text,
            "matched_code": code,
            "resolution_reason": "exact" if text else "paragraph_not_found",
        }


_DB = {
    "norms": {
        "ГОСТ 21.1101-2013": {
            "status": "replaced",
            "replacement_doc": "ГОСТ Р 21.101-2020",
        }
    },
    "replacements": {"ГОСТ 21.1101-2013": "ГОСТ Р 21.101-2020"},
}


def _harden(finding):
    harden_finding_normative_references(
        finding, norms_api=_FakeNormsApi(), norms_db=_DB
    )
    return finding


def test_norm_without_clause_does_not_receive_invented_clause():
    finding = _harden({
        "id": "F-001",
        "norm": "ГОСТ 21.110-2013, требования к оформлению спецификации",
        "norm_quote": None,
    })

    ref = finding["norm_references"][0]
    assert ref["clause"] is None
    assert ref["quote"] is None
    assert ref["provenance"]["clause_evidence"]["authoritative"] is False
    assert "пункт не подтверждён" in finding["norm"]


def test_unconfirmed_claimed_clause_is_not_published_as_authoritative():
    finding = _harden({
        "id": "F-001",
        "norm": "СП 30.13330.2020, п. 8.6",
        "norm_quote": "Чужая правдоподобная цитата",
    })

    ref = finding["norm_references"][0]
    assert ref["clause"] is None
    assert ref["quote"] is None
    evidence = ref["provenance"]["clause_evidence"]
    assert evidence["claimed_clause"] == "8.6"
    assert evidence["authoritative"] is False
    assert finding["norm_quote"] is None


def test_confirmed_clause_gets_its_own_index_quote():
    finding = _harden({
        "id": "F-001",
        "norm": "ГОСТ 21.601-2011, п. 5.1",
        "norm_quote": "Модель пересказала пункт своими словами",
    })

    ref = finding["norm_references"][0]
    assert ref["clause"] == "5.1"
    assert ref["quote"].startswith("5.1 В состав общих данных")
    assert ref["provenance"]["quote_evidence"]["authoritative"] is True
    assert ref["provenance"]["claimed_quote_matched"] is False
    assert finding["norm_quote"] == ref["quote"]


def test_alias_and_typo_normalization_are_deterministic():
    assert normalize_designation("ГОСТ 21.101-2020")[0] == "ГОСТ Р 21.101-2020"
    assert normalize_designation("ГОСТ Р 21.110-2013")[0] == "ГОСТ 21.110-2013"
    assert normalize_designation("СП 256.132580.2016")[0] == "СП 256.1325800.2016"

    finding = _harden({
        "id": "F-001",
        "norm": "ПП РФ № 1479; СП 256.132580.2016",
    })
    assert [r["norm_designation"] for r in finding["norm_references"]] == [
        "ПП РФ №1479",
        "СП 256.1325800.2016",
    ]


def test_wrong_edition_keeps_cited_designation_and_current_separately():
    finding = _harden({
        "id": "F-001",
        "norm": "ГОСТ 21.1101-2013, п. 5.1",
    })

    ref = finding["norm_references"][0]
    assert ref["norm_designation"] == "ГОСТ 21.1101-2013"
    assert ref["cited_designation"] == "ГОСТ 21.1101-2013"
    assert ref["current_designation"] == "ГОСТ Р 21.101-2020"
    assert ref["status"] == "replaced"
    assert ref["clause"] is None
    assert "заменён; актуальная редакция: ГОСТ Р 21.101-2020" in finding["norm"]


def test_russian_preposition_so_is_not_a_norm_in_either_extractor():
    prose = "Этажи со 117 по 144; высоты со 169 по 200; диапазон со 2-."
    assert extract_designations(prose) == []
    assert extract_norms_from_text(prose) == []
    assert extract_designations("СО 117") == []
    assert extract_norms_from_text("СО 117") == []
    assert extract_designations("СО 153-34.20.501-2003") == [
        "СО 153-34.20.501-2003"
    ]
    assert extract_norms_from_text("СО 153-34.20.501-2003") == [
        "СО 153-34.20.501-2003"
    ]


def test_vk_4_2_gost_quote_is_not_attached_to_two_norms():
    """Real F-010/F-016/F-019/F-024 failure from VK-4-2-RD."""
    foreign_shared_quote = (
        "5.1. В состав общих данных по рабочим чертежам систем "
        "водоснабжения и канализации включают данные по водопотреблению."
    )
    finding = {
        "id": "F-010",
        "norm": (
            "ГОСТ 21.110-2013 (действует), п. 5.1; "
            "ГОСТ 21.601-2011 (действует), п. 5.1"
        ),
        "norm_quote": foreign_shared_quote,
        "source_finding_ids": ["G-003"],
    }
    # Deliberately use the bundled real Norms index: this is a regression over
    # the exact document/clause pairs observed in the production artifact.
    harden_finding_normative_references(finding)

    refs = {ref["norm_designation"]: ref for ref in finding["norm_references"]}
    assert refs["ГОСТ 21.110-2013"]["clause"] is None
    assert refs["ГОСТ 21.110-2013"]["quote"] is None
    assert refs["ГОСТ 21.601-2011"]["clause"] == "5.1"
    assert "В состав общих данных" in refs["ГОСТ 21.601-2011"]["quote"]
    assert refs["ГОСТ 21.110-2013"]["provenance"] is not refs[
        "ГОСТ 21.601-2011"
    ]["provenance"]
    # The unbound compatibility field cannot represent two independent quotes.
    assert finding["norm_quote"] is None


def test_vk_4_2_sp29_sp30_foreign_quote_fails_closed():
    """Real F-018 failure: SP 29 clause 8.6 quote was labelled as SP 30."""
    finding = {
        "id": "F-018",
        "norm": (
            "СП 29.13330.2011, требования к уклонам полов; "
            "СП 30.13330.2020, п. 8.6"
        ),
        "norm_quote": (
            "8.6 Толщина стяжки с охлаждающими трубами в плите катков "
            "с искусственным льдом должна составлять 140 мм."
        ),
        "source_finding_ids": ["G-014"],
    }
    harden_finding_normative_references(finding)

    assert len(finding["norm_references"]) == 2
    assert all(ref["quote"] is None for ref in finding["norm_references"])
    assert all(ref["clause"] is None for ref in finding["norm_references"])
    assert finding["norm_quote"] is None


def test_file_publication_writes_contract_and_telemetry(tmp_path):
    path = tmp_path / "03_findings.json"
    path.write_text(
        json.dumps({
            "meta": {},
            "findings": [{
                "id": "F-001",
                "norm": "ГОСТ Р 21.110-2013, требования к спецификации",
            }],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    report = harden_normative_references(tmp_path)
    output = json.loads(path.read_text(encoding="utf-8"))
    ref = output["findings"][0]["norm_references"][0]
    assert report["ok"] is True
    assert report["references"] == 1
    assert report["normalized"] == 1
    assert ref["norm_designation"] == "ГОСТ 21.110-2013"
    assert output["meta"]["normative_reference_hardening"]["references"] == 1
