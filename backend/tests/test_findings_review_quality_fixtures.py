"""
test_findings_review_quality_fixtures.py
-----------------------------------------
Offline smoke-тест для базы fixtures findings_review.

Цель: проверить структуру и корректность тестовых данных, используемых
для разработки critic v2. Не подключает LLM и не меняет production pipeline.

Запуск:
    python -m pytest backend/tests/test_findings_review_quality_fixtures.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

# ─── Пути ────────────────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "findings_review"

FIXTURE_FILES = {
    "bad": FIXTURES_DIR / "bad_findings.json",
    "good": FIXTURES_DIR / "good_findings.json",
    "borderline": FIXTURES_DIR / "borderline_findings.json",
    "no_evidence": FIXTURES_DIR / "no_evidence_findings.json",
}

DUPLICATE_FIXTURE = FIXTURES_DIR / "duplicate_findings.json"

# Допустимые значения expected_decision для одиночных findings
VALID_SINGLE_DECISIONS = {"accept", "reject", "borderline"}

# Допустимые значения expected_decision для дублей
VALID_DUPLICATE_DECISIONS = {
    "accept", "reject", "borderline",
    "merge_into_F-010", "merge_into_F-020", "merge_into_F-030",
}

# Обязательные поля верхнего уровня в каждом fixture-entry
REQUIRED_TOP_FIELDS = {"id", "category", "input_finding", "expected_decision",
                       "expected_reason", "comment"}

# Обязательные поля в input_finding
REQUIRED_FINDING_FIELDS = {
    "id", "severity", "category", "sheet", "page",
    "problem", "description", "solution", "risk",
    "evidence", "related_block_ids",
}

# Допустимые severity
VALID_SEVERITIES = {
    "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _load_fixture(path: Path) -> list[dict]:
    assert path.exists(), f"Fixture file not found: {path}"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(data, list), f"Expected list in {path.name}"
    return data


def _iter_top_fields(entry: dict, fixture_name: str) -> None:
    fid = entry.get("id", "???")
    missing = REQUIRED_TOP_FIELDS - set(entry.keys())
    assert not missing, (
        f"[{fixture_name}] Entry '{fid}' missing top-level fields: {missing}"
    )


def _iter_finding_fields(entry: dict, fixture_name: str) -> None:
    fid = entry.get("id", "???")
    finding = entry.get("input_finding", {})
    missing = REQUIRED_FINDING_FIELDS - set(finding.keys())
    assert not missing, (
        f"[{fixture_name}] Entry '{fid}' input_finding missing fields: {missing}"
    )


def _check_severity(entry: dict, fixture_name: str) -> None:
    fid = entry.get("id", "???")
    severity = entry.get("input_finding", {}).get("severity", "")
    assert severity in VALID_SEVERITIES, (
        f"[{fixture_name}] Entry '{fid}' has unknown severity: '{severity}'. "
        f"Valid: {VALID_SEVERITIES}"
    )


def _check_decision(entry: dict, fixture_name: str, valid_decisions: set) -> None:
    fid = entry.get("id", "???")
    decision = entry.get("expected_decision", "")
    assert decision in valid_decisions, (
        f"[{fixture_name}] Entry '{fid}' has unknown expected_decision: '{decision}'. "
        f"Valid: {valid_decisions}"
    )


def _check_reason_nonempty(entry: dict, fixture_name: str) -> None:
    fid = entry.get("id", "???")
    reason = entry.get("expected_reason", "")
    assert isinstance(reason, str) and len(reason.strip()) > 0, (
        f"[{fixture_name}] Entry '{fid}' has empty expected_reason"
    )


def _check_evidence_is_list(entry: dict, fixture_name: str) -> None:
    fid = entry.get("id", "???")
    finding = entry.get("input_finding", {})
    evidence = finding.get("evidence", [])
    related = finding.get("related_block_ids", [])
    assert isinstance(evidence, list), (
        f"[{fixture_name}] Entry '{fid}' evidence must be a list"
    )
    assert isinstance(related, list), (
        f"[{fixture_name}] Entry '{fid}' related_block_ids must be a list"
    )


def _check_ids_unique(entries: list[dict], fixture_name: str) -> None:
    ids = [e.get("id") for e in entries]
    seen = set()
    for fid in ids:
        assert fid not in seen, (
            f"[{fixture_name}] Duplicate fixture id: '{fid}'"
        )
        seen.add(fid)


# ─── Tests: individual fixture files ─────────────────────────────────────────

class TestFixtureFilesExist:
    def test_bad_findings_file_exists(self):
        assert FIXTURE_FILES["bad"].exists(), \
            f"Missing: {FIXTURE_FILES['bad']}"

    def test_good_findings_file_exists(self):
        assert FIXTURE_FILES["good"].exists(), \
            f"Missing: {FIXTURE_FILES['good']}"

    def test_borderline_findings_file_exists(self):
        assert FIXTURE_FILES["borderline"].exists(), \
            f"Missing: {FIXTURE_FILES['borderline']}"

    def test_no_evidence_findings_file_exists(self):
        assert FIXTURE_FILES["no_evidence"].exists(), \
            f"Missing: {FIXTURE_FILES['no_evidence']}"

    def test_duplicate_findings_file_exists(self):
        assert DUPLICATE_FIXTURE.exists(), \
            f"Missing: {DUPLICATE_FIXTURE}"

    def test_readme_exists(self):
        assert (FIXTURES_DIR / "README.md").exists(), \
            "Missing: fixtures/findings_review/README.md"

    def test_quality_criteria_doc_exists(self):
        criteria_path = (
            Path(__file__).parent.parent
            / "app" / "pipeline" / "stages" / "findings_review"
            / "CRITIC_QUALITY_CRITERIA.md"
        )
        assert criteria_path.exists(), \
            f"Missing: CRITIC_QUALITY_CRITERIA.md at {criteria_path}"


class TestBadFindings:
    @pytest.fixture(autouse=True)
    def load(self):
        self.entries = _load_fixture(FIXTURE_FILES["bad"])

    def test_not_empty(self):
        assert len(self.entries) >= 5, "bad_findings.json should have ≥5 entries"

    def test_ids_unique(self):
        _check_ids_unique(self.entries, "bad_findings")

    def test_required_top_fields(self):
        for e in self.entries:
            _iter_top_fields(e, "bad_findings")

    def test_required_finding_fields(self):
        for e in self.entries:
            _iter_finding_fields(e, "bad_findings")

    def test_severity_valid(self):
        for e in self.entries:
            _check_severity(e, "bad_findings")

    def test_expected_decision_valid(self):
        for e in self.entries:
            _check_decision(e, "bad_findings", VALID_SINGLE_DECISIONS)

    def test_expected_reason_nonempty(self):
        for e in self.entries:
            _check_reason_nonempty(e, "bad_findings")

    def test_all_decisions_are_reject(self):
        for e in self.entries:
            assert e["expected_decision"] == "reject", (
                f"bad_findings entry '{e['id']}' expected_decision should be 'reject', "
                f"got '{e['expected_decision']}'"
            )

    def test_evidence_fields_are_lists(self):
        for e in self.entries:
            _check_evidence_is_list(e, "bad_findings")


class TestGoodFindings:
    @pytest.fixture(autouse=True)
    def load(self):
        self.entries = _load_fixture(FIXTURE_FILES["good"])

    def test_not_empty(self):
        assert len(self.entries) >= 3, "good_findings.json should have ≥3 entries"

    def test_ids_unique(self):
        _check_ids_unique(self.entries, "good_findings")

    def test_required_top_fields(self):
        for e in self.entries:
            _iter_top_fields(e, "good_findings")

    def test_required_finding_fields(self):
        for e in self.entries:
            _iter_finding_fields(e, "good_findings")

    def test_severity_valid(self):
        for e in self.entries:
            _check_severity(e, "good_findings")

    def test_expected_decision_valid(self):
        for e in self.entries:
            _check_decision(e, "good_findings", VALID_SINGLE_DECISIONS)

    def test_expected_reason_nonempty(self):
        for e in self.entries:
            _check_reason_nonempty(e, "good_findings")

    def test_all_decisions_are_accept(self):
        for e in self.entries:
            assert e["expected_decision"] == "accept", (
                f"good_findings entry '{e['id']}' expected_decision should be 'accept', "
                f"got '{e['expected_decision']}'"
            )

    def test_good_findings_have_evidence(self):
        for e in self.entries:
            finding = e["input_finding"]
            has_evidence = bool(finding.get("evidence")) or bool(finding.get("related_block_ids"))
            assert has_evidence, (
                f"good_findings entry '{e['id']}': a good finding must have evidence "
                f"or related_block_ids"
            )

    def test_evidence_fields_are_lists(self):
        for e in self.entries:
            _check_evidence_is_list(e, "good_findings")


class TestBorderlineFindings:
    @pytest.fixture(autouse=True)
    def load(self):
        self.entries = _load_fixture(FIXTURE_FILES["borderline"])

    def test_not_empty(self):
        assert len(self.entries) >= 2, "borderline_findings.json should have ≥2 entries"

    def test_ids_unique(self):
        _check_ids_unique(self.entries, "borderline_findings")

    def test_required_top_fields(self):
        for e in self.entries:
            _iter_top_fields(e, "borderline_findings")

    def test_required_finding_fields(self):
        for e in self.entries:
            _iter_finding_fields(e, "borderline_findings")

    def test_severity_valid(self):
        for e in self.entries:
            _check_severity(e, "borderline_findings")

    def test_expected_decision_valid(self):
        for e in self.entries:
            _check_decision(e, "borderline_findings", VALID_SINGLE_DECISIONS)

    def test_expected_reason_nonempty(self):
        for e in self.entries:
            _check_reason_nonempty(e, "borderline_findings")

    def test_all_decisions_are_borderline(self):
        for e in self.entries:
            assert e["expected_decision"] == "borderline", (
                f"borderline_findings entry '{e['id']}' expected_decision "
                f"should be 'borderline', got '{e['expected_decision']}'"
            )

    def test_evidence_fields_are_lists(self):
        for e in self.entries:
            _check_evidence_is_list(e, "borderline_findings")


class TestNoEvidenceFindings:
    @pytest.fixture(autouse=True)
    def load(self):
        self.entries = _load_fixture(FIXTURE_FILES["no_evidence"])

    def test_not_empty(self):
        assert len(self.entries) >= 4, "no_evidence_findings.json should have ≥4 entries"

    def test_ids_unique(self):
        _check_ids_unique(self.entries, "no_evidence_findings")

    def test_required_top_fields(self):
        for e in self.entries:
            _iter_top_fields(e, "no_evidence_findings")

    def test_required_finding_fields(self):
        for e in self.entries:
            _iter_finding_fields(e, "no_evidence_findings")

    def test_severity_valid(self):
        for e in self.entries:
            _check_severity(e, "no_evidence_findings")

    def test_expected_decision_valid(self):
        for e in self.entries:
            _check_decision(e, "no_evidence_findings", VALID_SINGLE_DECISIONS)

    def test_expected_reason_nonempty(self):
        for e in self.entries:
            _check_reason_nonempty(e, "no_evidence_findings")

    def test_expected_verdict_present(self):
        for e in self.entries:
            assert "expected_verdict" in e, (
                f"no_evidence_findings entry '{e['id']}' must have 'expected_verdict' field"
            )
            assert isinstance(e["expected_verdict"], str) and e["expected_verdict"], (
                f"no_evidence_findings entry '{e['id']}' expected_verdict is empty"
            )

    def test_evidence_fields_are_lists(self):
        for e in self.entries:
            _check_evidence_is_list(e, "no_evidence_findings")


class TestDuplicateFindings:
    @pytest.fixture(autouse=True)
    def load(self):
        self.sets = _load_fixture(DUPLICATE_FIXTURE)

    def test_not_empty(self):
        assert len(self.sets) >= 2, "duplicate_findings.json should have ≥2 sets"

    def test_ids_unique(self):
        _check_ids_unique(self.sets, "duplicate_findings")

    def test_required_set_fields(self):
        for s in self.sets:
            sid = s.get("id", "???")
            for field in ("id", "description", "findings", "expected_decisions"):
                assert field in s, (
                    f"duplicate set '{sid}' missing field: '{field}'"
                )

    def test_findings_field_is_list(self):
        for s in self.sets:
            assert isinstance(s["findings"], list) and len(s["findings"]) >= 2, (
                f"duplicate set '{s['id']}' must have ≥2 findings"
            )

    def test_expected_decisions_covers_all_findings(self):
        for s in self.sets:
            finding_ids = {f["id"] for f in s["findings"]}
            decision_ids = set(s.get("expected_decisions", {}).keys())
            missing = finding_ids - decision_ids
            assert not missing, (
                f"duplicate set '{s['id']}' expected_decisions missing for: {missing}"
            )

    def test_expected_decisions_not_all_accept(self):
        for s in self.sets:
            decisions = list(s.get("expected_decisions", {}).values())
            all_accept = all(d == "accept" for d in decisions)
            assert not all_accept, (
                f"duplicate set '{s['id']}' — at least one duplicate must be "
                f"merge or reject, not all 'accept'"
            )

    def test_each_finding_has_required_fields(self):
        for s in self.sets:
            for f in s["findings"]:
                missing = REQUIRED_FINDING_FIELDS - set(f.keys())
                assert not missing, (
                    f"duplicate set '{s['id']}' finding '{f.get('id')}' "
                    f"missing fields: {missing}"
                )
            for f in s["findings"]:
                severity = f.get("severity", "")
                assert severity in VALID_SEVERITIES, (
                    f"duplicate set '{s['id']}' finding '{f.get('id')}' "
                    f"unknown severity: '{severity}'"
                )


class TestFixtureCoverage:
    """Проверяет, что база fixtures покрывает все ключевые категории дефектов."""

    def test_bad_covers_generic_wording(self):
        entries = _load_fixture(FIXTURE_FILES["bad"])
        cats = {e["category"] for e in entries}
        assert "generic_wording" in cats, \
            "bad_findings should include 'generic_wording' category"

    def test_bad_covers_speculation(self):
        entries = _load_fixture(FIXTURE_FILES["bad"])
        cats = {e["category"] for e in entries}
        assert "speculation" in cats, \
            "bad_findings should include 'speculation' category"

    def test_bad_covers_no_evidence(self):
        entries = _load_fixture(FIXTURE_FILES["bad"])
        cats = {e["category"] for e in entries}
        assert "no_evidence" in cats, \
            "bad_findings should include 'no_evidence' category"

    def test_bad_covers_no_impact(self):
        entries = _load_fixture(FIXTURE_FILES["bad"])
        cats = {e["category"] for e in entries}
        assert "no_impact" in cats, \
            "bad_findings should include 'no_impact' category"

    def test_good_covers_calculation(self):
        entries = _load_fixture(FIXTURE_FILES["good"])
        cats = {e["category"] for e in entries}
        assert "calculation_with_fact" in cats, \
            "good_findings should include 'calculation_with_fact' category"

    def test_good_covers_declaration_vs_fact(self):
        entries = _load_fixture(FIXTURE_FILES["good"])
        cats = {e["category"] for e in entries}
        assert "declaration_vs_fact" in cats, \
            "good_findings should include 'declaration_vs_fact' category"

    def test_no_evidence_covers_phantom_block(self):
        entries = _load_fixture(FIXTURE_FILES["no_evidence"])
        cats = {e["category"] for e in entries}
        assert "phantom_block_all_invalid" in cats, \
            "no_evidence_findings should include 'phantom_block_all_invalid' category"

    def test_borderline_covers_interdisciplinary(self):
        entries = _load_fixture(FIXTURE_FILES["borderline"])
        cats = {e["category"] for e in entries}
        assert "cross_section_dependency" in cats, \
            "borderline_findings should include 'cross_section_dependency' category"
