"""Bounded deterministic Norm Resolver and norm_verify handoff."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from backend.app.pipeline.stages.norms.resolver import (
    AMBIGUOUS,
    DOCUMENT_MISSING,
    NOT_VERIFIED,
    VERIFIED,
    WRONG_EDITION,
    NormResolver,
    infer_document_date,
)
from backend.app.pipeline.stages.findings_merge.normative_references import (
    harden_finding_normative_references,
)
from norms._core import extract_norms_from_findings, generate_deterministic_checks
from norms._native_verify import verify_paragraphs_native
from norms.runtime import (
    NORMS_STATUS_INDEX_ENV,
    NORMS_TOOLS_ENV,
    NORMS_VAULT_ENV,
)


DOC = "СП 999.1325800.2020"
OLD_DOC = "ГОСТ Р 21.101-2020"
CURRENT_DOC = "ГОСТ Р 21.101-2026"

CLAUSES = {
    (DOC, "1.1"): (
        "1.1 Места прохода кабелей через стены должны быть заделаны "
        "огнестойким материалом с нормируемым пределом огнестойкости."
    ),
    (DOC, "2.1"): (
        "2.1 Расчетные электрические нагрузки следует определять по установленной "
        "мощности и коэффициенту спроса."
    ),
    (DOC, "3.1"): "3.1 Наименование комплекта указывают в основной надписи.",
    (OLD_DOC, "5.1"): (
        "5.1 Общие данные по рабочим чертежам должны содержать ведомость "
        "рабочих чертежей основного комплекта."
    ),
    ("ПУЭ-7", "2.1"): (
        "2.1 Устройства электроустановки должны обеспечивать безопасность эксплуатации."
    ),
}


@pytest.fixture(autouse=True)
def preserve_norms_runtime_environment(monkeypatch):
    """Real-vault regression cases must not leak lazy runtime bootstrap env."""
    root = Path(__file__).resolve().parent.parent / "norms"
    tools = root / "tools"
    monkeypatch.setenv(NORMS_TOOLS_ENV, str(tools))
    monkeypatch.setenv(NORMS_STATUS_INDEX_ENV, str(tools / "status_index.json"))
    monkeypatch.setenv(NORMS_VAULT_ENV, str(root / "vault"))


class FakeNormsApi:
    def __init__(self, *, wrong_lookup_code: str | None = None):
        self.wrong_lookup_code = wrong_lookup_code
        self.statuses = {
            DOC: self._status(DOC, "doc.md"),
            OLD_DOC: self._status(
                OLD_DOC, "old.md", status="replaced",
                replacement_doc=CURRENT_DOC, effective_from="2026-04-01",
            ),
            CURRENT_DOC: self._status(
                CURRENT_DOC, None, source="override_only", effective_from="2026-04-01"
            ),
            "ПУЭ-7": self._status("ПУЭ-7", "pue.md"),
        }

    @staticmethod
    def _status(
        code: str,
        file: str | None,
        *,
        status: str = "active",
        source: str = "vault",
        replacement_doc: str | None = None,
        effective_from: str | None = None,
    ) -> dict:
        return {
            "found": True,
            "authoritative": True,
            "status": status,
            "matched_code": code,
            "source": source,
            "file": file,
            "replacement_doc": replacement_doc,
            "effective_from": effective_from,
            "resolution_reason": "exact",
        }

    def get_norm_status(self, code: str) -> dict:
        return self.statuses.get(code, {
            "found": False,
            "authoritative": False,
            "status": "unknown",
            "matched_code": None,
            "source": "not_found",
            "file": None,
            "resolution_reason": "not_in_index",
        })

    def get_paragraph(self, code: str, paragraph: str, max_lines: int = 50) -> dict:
        text = CLAUSES.get((code, paragraph))
        return {
            "found": text is not None,
            "has_text": text is not None,
            "authoritative": text is not None,
            "text": text,
            "matched_code": self.wrong_lookup_code or code,
            "file": self.get_norm_status(code).get("file"),
            "line": 1,
            "resolution_reason": "exact" if text else "paragraph_not_found",
        }


@pytest.fixture()
def resolver_factory(tmp_path):
    def make(*, clauses: dict = CLAUSES, api: FakeNormsApi | None = None, cache=True):
        vault = tmp_path / "vault"
        vault.mkdir(exist_ok=True)
        for name in ("doc.md", "old.md", "pue.md"):
            path = vault / name
            if not path.exists():
                path.write_text(f"vault document {name}", encoding="utf-8")
        paragraphs = tmp_path / "paragraphs.jsonl"
        paragraphs.write_text("".join(
            json.dumps({
                "code": code,
                "paragraph": paragraph,
                "text": text,
                "file": FakeNormsApi().get_norm_status(code).get("file"),
                "line": 1,
            }, ensure_ascii=False) + "\n"
            for (code, paragraph), text in clauses.items()
        ), encoding="utf-8")
        status_index = tmp_path / "status_index.json"
        status_index.write_text('{"norms": []}', encoding="utf-8")
        return NormResolver(
            norms_api=api or FakeNormsApi(),
            paragraphs_path=paragraphs,
            vault_path=vault,
            status_index_path=status_index,
            cache_path=tmp_path / "cache.json" if cache else None,
        )
    return make


def candidate(code=DOC, clause=None, quote=None):
    return {
        "designation": code,
        "candidate_relevance": 0.8,
        "reason": "кабельная проходка заделана без огнестойкого материала",
        "provenance": {"producer": "test-stage03"},
        "clause_candidate": clause,
        "quote_candidate": quote,
    }


def finding(problem=None, *, severity="ЭКСПЛУАТАЦИОННОЕ"):
    return {
        "id": "F-001",
        "severity": severity,
        "problem": problem or "Проходка кабелей не заделана огнестойким материалом.",
        "solution": "Выполнить огнестойкую заделку кабельной проходки.",
    }


def test_exact_candidate_is_verified_from_vault(resolver_factory):
    result = resolver_factory().resolve_reference(finding(), candidate(clause="1.1"))
    assert result["resolution_status"] == VERIFIED
    assert result["clause"] == "1.1"
    assert result["quote"] == CLAUSES[(DOC, "1.1")]
    assert result["provenance"]["retrieval_strategy"] == "exact_clause_candidate"
    assert result["provenance"]["ai_used"] is False


def test_wrong_candidate_falls_back_inside_same_document(resolver_factory):
    result = resolver_factory().resolve_reference(finding(), candidate(clause="2.1"))
    assert result["resolution_status"] == VERIFIED
    assert result["clause"] == "1.1"
    assert result["provenance"]["retrieval_strategy"] == "same_document_alternative"
    assert result["provenance"]["retrieval"]["document_scope"] == DOC


def test_no_candidate_uses_bounded_retrieval(resolver_factory):
    result = resolver_factory().resolve_reference(finding(), candidate())
    assert result["resolution_status"] == VERIFIED
    assert result["clause"] == "1.1"


def test_ambiguous_and_no_match_fail_closed(resolver_factory):
    ambiguous_clauses = dict(CLAUSES)
    ambiguous_clauses[(DOC, "4.1")] = CLAUSES[(DOC, "1.1")].replace("1.1", "4.1", 1)
    ambiguous = resolver_factory(clauses=ambiguous_clauses).resolve_reference(
        finding(), candidate()
    )
    assert ambiguous["resolution_status"] == AMBIGUOUS
    assert ambiguous["clause"] is None and ambiguous["quote"] is None

    no_match = resolver_factory().resolve_reference(
        {
            "id": "F-NO-MATCH",
            "problem": "Аквариум и декоративные рыбы в интерьере.",
        },
        {**candidate(), "reason": "аквариум рыбы"},
    )
    assert no_match["resolution_status"] == NOT_VERIFIED
    assert no_match["clause"] is None and no_match["quote"] is None

    no_indexed_clauses = resolver_factory(clauses={}).resolve_reference(
        finding(), candidate()
    )
    assert no_indexed_clauses["resolution_status"] == NOT_VERIFIED
    assert no_indexed_clauses["provenance"]["resolver_reason"] == (
        "document_has_no_indexed_clauses"
    )


def test_deterministic_verifier_rejects_quote_from_another_document(resolver_factory):
    api = FakeNormsApi(wrong_lookup_code="СП 30.13330.2020")
    result = resolver_factory(api=api).resolve_reference(finding(), candidate(clause="1.1"))
    assert result["resolution_status"] == NOT_VERIFIED
    assert result["provenance"]["verification"]["reason"] == "quote_from_wrong_document"


def test_missing_document_and_pue_policy_are_explicit(resolver_factory):
    missing = resolver_factory().resolve_reference(
        finding(), candidate(code="СП 404.1325800.2020")
    )
    assert missing["resolution_status"] == DOCUMENT_MISSING

    pue_candidate = {
        **candidate(code="ПУЭ-7", clause="2.1"),
        "reason": "безопасность электроустановки и безопасная эксплуатация",
    }
    pue = resolver_factory().resolve_reference(
        finding("Не обеспечена безопасность эксплуатации электроустановки."),
        pue_candidate,
    )
    assert pue["special_policy"]["status"] == "SPECIAL_POLICY"
    assert pue["provenance"]["ai_used"] is False


def test_edition_is_evaluated_at_document_date(resolver_factory):
    old_candidate = {
        **candidate(code=OLD_DOC, clause="5.1"),
        "reason": "общие данные рабочих чертежей и ведомость чертежей",
    }
    issue_finding = finding("В общих данных нет ведомости рабочих чертежей.")
    historical = resolver_factory().resolve_reference(
        issue_finding, old_candidate, document_date=date(2025, 11, 11)
    )
    assert historical["resolution_status"] == VERIFIED
    assert historical["edition_applicability"] == "historical_applicable"
    assert historical["current_designation"] == CURRENT_DOC

    new_project = resolver_factory().resolve_reference(
        issue_finding, old_candidate, document_date=date(2026, 4, 1)
    )
    assert new_project["resolution_status"] == WRONG_EDITION
    assert new_project["clause"] is None and new_project["quote"] is None

    future_edition = resolver_factory().resolve_reference(
        issue_finding,
        candidate(code=CURRENT_DOC),
        document_date=date(2025, 11, 11),
    )
    assert future_edition["resolution_status"] == WRONG_EDITION
    assert future_edition["edition_applicability"] == "not_yet_effective"


def test_document_date_is_inferred_from_nearest_version_work_dir(tmp_path):
    output = tmp_path / "_versions" / "v2" / "_output"
    work = output.parent / "02_work"
    work.mkdir(parents=True)
    output.mkdir()
    (work / "sheet.md").write_text(
        "Revision / Изменение: 11.11.2025\n", encoding="utf-8"
    )
    assert infer_document_date(output) == date(2025, 11, 11)


def test_multi_norm_status_and_critical_notice_are_finding_level(resolver_factory):
    item = finding(severity="КРИТИЧЕСКОЕ")
    item["candidate_norm_references"] = [
        candidate(clause="1.1"),
        candidate(code="СП 404.1325800.2020"),
    ]
    refs, _ = resolver_factory().resolve_finding(item)
    assert [ref["resolution_status"] for ref in refs] == [VERIFIED, DOCUMENT_MISSING]
    assert item["finding_norm_status"] == "PARTIALLY_VERIFIED"
    assert "critical_norm_notice" not in item

    item2 = finding(severity="КРИТИЧЕСКОЕ")
    item2["candidate_norm_references"] = [candidate(code="СП 404.1325800.2020")]
    resolver_factory().resolve_finding(item2)
    assert item2["finding_norm_status"] == "NOT_VERIFIED"
    assert item2["critical_norm_notice"]


def test_cache_key_invalidates_when_vault_document_changes(resolver_factory):
    first = resolver_factory()
    first.resolve_reference(finding(), candidate(clause="1.1"))
    first.save_cache()

    second = resolver_factory()
    second.resolve_reference(finding(), candidate(clause="1.1"))
    assert second.cache_hits == 1
    (second.vault_path / "doc.md").write_text("changed vault bytes", encoding="utf-8")

    third = resolver_factory()
    third.resolve_reference(finding(), candidate(clause="1.1"))
    assert third.cache_hits == 0 and third.cache_misses == 1
    assert third.ai_calls == 0


def test_resolver_references_feed_norm_verify_independently(tmp_path, monkeypatch):
    findings_path = tmp_path / "03_findings.json"
    findings_path.write_text(json.dumps({"findings": [{
        "id": "F-001",
        "problem": "Две независимые ссылки",
        "norm": None,
        "norm_references": [
            {
                "canonical_designation": DOC,
                "cited_designation": DOC,
                "resolution_status": VERIFIED,
                "edition_applicability": "current",
                "clause": "1.1",
                "quote": CLAUSES[(DOC, "1.1")],
            },
            {
                "canonical_designation": DOC,
                "cited_designation": DOC,
                "resolution_status": VERIFIED,
                "edition_applicability": "current",
                "clause": "2.1",
                "quote": CLAUSES[(DOC, "2.1")],
            },
        ],
    }]}, ensure_ascii=False), encoding="utf-8")

    def fake_status(query):
        return {
            "query": query, "found": True, "authoritative": True,
            "matched_code": DOC, "status": "active", "doc_status": "active",
            "edition_status": "current", "current_version": DOC,
            "replacement_doc": None, "has_text": True, "source": "vault",
            "resolution_reason": "exact", "supported_family": True,
        }

    monkeypatch.setattr("norms.external_provider.resolve_norm_status", fake_status)
    norms_data = extract_norms_from_findings(findings_path)
    det = generate_deterministic_checks(norms_data, project_id="test")
    assert len(det["paragraphs_to_verify"]) == 2
    assert {item["claimed_quote"] for item in det["paragraphs_to_verify"]} == {
        CLAUSES[(DOC, "1.1")], CLAUSES[(DOC, "2.1")],
    }

    monkeypatch.setattr("norms._native_verify._import_norms_api", FakeNormsApi)
    output = verify_paragraphs_native(det["paragraphs_to_verify"], findings_path, tmp_path)
    checks = json.loads(output.read_text(encoding="utf-8"))["paragraph_checks"]
    assert len(checks) == 2
    assert all(check["paragraph_verified"] for check in checks)


def test_historical_edition_does_not_trigger_norm_fix(monkeypatch):
    monkeypatch.setattr("norms.external_provider.resolve_norm_status", lambda query: {
        "query": query, "found": True, "authoritative": True,
        "matched_code": OLD_DOC, "status": "replaced", "doc_status": "replaced",
        "edition_status": "current", "current_version": CURRENT_DOC,
        "replacement_doc": CURRENT_DOC, "has_text": True, "source": "vault",
        "resolution_reason": "exact", "supported_family": True,
    })
    data = {
        "norms": {
            f"{OLD_DOC}, п. 5.1": {
                "cited_as": [OLD_DOC],
                "affected_findings": ["F-1"],
                "contexts": [],
                "finding_norms": {"F-1": f"{OLD_DOC}, п. 5.1"},
                "finding_quotes": {"F-1": CLAUSES[(OLD_DOC, "5.1")]},
                "reference_statuses": {"F-1": VERIFIED},
                "edition_applicability": {"F-1": "historical_applicable"},
                "structured_references": True,
            }
        }
    }
    check = generate_deterministic_checks(data)["checks"][0]
    assert check["status"] == "replaced"
    assert check["status_at_document_date"] == "active"
    assert check["needs_revision"] is False


def test_vk_4_2_gost_quote_cannot_confirm_two_documents():
    foreign_quote = (
        "5.1 В состав общих данных по рабочим чертежам систем "
        "водоснабжения и канализации включают данные по водопотреблению."
    )
    item = {
        "id": "VK-F-010",
        "problem": "В общих данных систем водоснабжения нет данных по водопотреблению.",
        "norm": "ГОСТ 21.110-2013, п. 5.1; ГОСТ 21.601-2011, п. 5.1",
        "norm_quote": foreign_quote,
    }
    harden_finding_normative_references(item)
    refs, _ = NormResolver(cache_path=None).resolve_finding(item)

    assert len(refs) == 2
    assert sum(ref["resolution_status"] == VERIFIED for ref in refs) <= 1
    for ref in refs:
        if ref["resolution_status"] == VERIFIED:
            assert ref["provenance"]["verification"]["matched_code"].replace("_", ".") == ref[
                "canonical_designation"
            ]


def test_vk_4_2_sp29_quote_is_never_published_as_sp30_quote():
    foreign_quote = (
        "8.6 Толщина стяжки с охлаждающими трубами в плите катков "
        "с искусственным льдом должна составлять 140 мм."
    )
    item = {
        "id": "VK-F-018",
        "problem": "Неверно указана толщина стяжки катка с охлаждающими трубами.",
        "norm": "СП 29.13330.2011, п. 8.6; СП 30.13330.2020, п. 8.6",
        "norm_quote": foreign_quote,
    }
    harden_finding_normative_references(item)
    refs, _ = NormResolver(cache_path=None).resolve_finding(item)
    by_code = {ref["canonical_designation"]: ref for ref in refs}

    sp30 = by_code["СП 30.13330.2020"]
    assert sp30.get("quote") != foreign_quote
    if sp30["resolution_status"] == VERIFIED:
        assert sp30["provenance"]["verification"]["matched_code"] == "СП 30.13330.2020"
