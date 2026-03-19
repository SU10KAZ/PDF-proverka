"""Тесты для norm_contract — классификация и обогащение norm полей."""
import json
import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from norm_contract import (
    classify_norm_status,
    classify_norm_quote_status,
    compute_norm_confidence,
    compute_norm_policy_class,
    enrich_findings_from_norm_checks,
    should_review_norm,
    NORM_CONFIDENCE_THRESHOLDS,
)


# ─── classify_norm_status ──────────────────────────────────────────────────

class TestClassifyNormStatus:
    def test_exact_quote(self):
        f = {"norm": "СП 256, п. 15.3", "norm_quote": "Точная цитата из нормы длиннее десяти символов", "norm_confidence": 0.9}
        assert classify_norm_status(f) == "exact_quote"

    def test_paraphrased(self):
        f = {"norm": "СП 256, п. 15.3", "norm_quote": "Приблизительная цитата из нормы", "norm_confidence": 0.7}
        assert classify_norm_status(f) == "paraphrased"

    def test_norm_detected_no_quote(self):
        f = {"norm": "СП 256, п. 15.3", "norm_quote": None, "norm_confidence": 0.7}
        assert classify_norm_status(f) == "norm_detected_no_quote"

    def test_no_norm_cited(self):
        f = {"norm": "", "norm_quote": None}
        assert classify_norm_status(f) == "no_norm_cited"

    def test_no_norm_field(self):
        f = {}
        assert classify_norm_status(f) == "no_norm_cited"

    def test_invalid_reference(self):
        f = {"norm": "ГОСТ 13109-97", "norm_verification": {"status": "replaced"}}
        assert classify_norm_status(f) == "invalid_reference"

    def test_not_found(self):
        f = {"norm": "ГОСТ 99999-2099", "norm_verification": {"status": "not_found"}}
        assert classify_norm_status(f) == "not_found"


# ─── classify_norm_quote_status ────────────────────────────────────────────

class TestClassifyNormQuoteStatus:
    def test_exact(self):
        f = {"norm_quote": "Длинная точная цитата из пункта нормы", "norm_confidence": 0.9}
        assert classify_norm_quote_status(f) == "exact"

    def test_approximate(self):
        f = {"norm_quote": "Приблизительная цитата", "norm_confidence": 0.6}
        assert classify_norm_quote_status(f) == "approximate"

    def test_missing(self):
        f = {"norm_quote": None}
        assert classify_norm_quote_status(f) == "missing"

    def test_empty_string(self):
        f = {"norm_quote": ""}
        assert classify_norm_quote_status(f) == "missing"


# ─── compute_norm_confidence ───────────────────────────────────────────────

class TestComputeNormConfidence:
    def test_no_norm_zero(self):
        f = {"norm": ""}
        assert compute_norm_confidence(f) == 0.0

    def test_active_verification_boosts(self):
        f = {"norm": "СП 256", "norm_confidence": 0.5,
             "norm_verification": {"status": "active"}}
        conf = compute_norm_confidence(f)
        assert conf >= 0.6  # boosted to at least 0.6

    def test_replaced_caps(self):
        f = {"norm": "ГОСТ 13109-97", "norm_confidence": 0.9,
             "norm_verification": {"status": "replaced"}}
        conf = compute_norm_confidence(f)
        assert conf <= 0.3  # capped

    def test_paragraph_verified_high(self):
        f = {"norm": "СП 256", "norm_confidence": 0.5,
             "norm_verification": {"paragraph_verified": True}}
        conf = compute_norm_confidence(f)
        assert conf >= 0.9

    def test_actual_quote_found_boosts(self):
        f = {"norm": "СП 256", "norm_confidence": 0.5, "norm_quote": None,
             "norm_verification": {"actual_quote": "Реальная цитата из нормы"}}
        conf = compute_norm_confidence(f)
        assert conf >= 0.75

    def test_raw_conf_preserved_when_no_verification(self):
        f = {"norm": "ПУЭ-7", "norm_confidence": 0.65}
        conf = compute_norm_confidence(f)
        assert conf == 0.65


# ─── enrich_findings_from_norm_checks ──────────────────────────────────────

class TestEnrichFindings:
    def test_enriches_verification(self):
        findings = [
            {"id": "F-001", "norm": "СП 256.1325800.2016, п. 15.3",
             "norm_quote": None, "norm_confidence": 0.6},
        ]
        norm_checks = {
            "checks": [
                {"doc_number": "СП 256.1325800.2016", "status": "active",
                 "edition_status": "active", "verified_via": "deterministic",
                 "needs_revision": False, "affected_findings": ["F-001"]},
            ],
            "paragraph_checks": [],
        }
        stats = enrich_findings_from_norm_checks(findings, norm_checks)
        assert stats["enriched_verification"] > 0
        assert findings[0]["norm_verification"]["status"] == "active"
        assert findings[0]["norm_status"] == "norm_detected_no_quote"

    def test_enriches_quote_from_paragraph_check(self):
        findings = [
            {"id": "F-001", "norm": "СП 256, п. 15.3",
             "norm_quote": None, "norm_confidence": 0.5},
        ]
        norm_checks = {
            "checks": [],
            "paragraph_checks": [
                {"finding_id": "F-001", "norm": "СП 256",
                 "paragraph_verified": True,
                 "actual_quote": "Кабельные линии должны проектироваться с учётом..."},
            ],
        }
        stats = enrich_findings_from_norm_checks(findings, norm_checks)
        assert stats["enriched_quote"] > 0
        assert findings[0]["norm_quote"] is not None
        assert findings[0]["norm_confidence"] >= 0.9

    def test_does_not_overwrite_existing_quote(self):
        findings = [
            {"id": "F-001", "norm": "СП 256",
             "norm_quote": "Существующая хорошая цитата", "norm_confidence": 0.9},
        ]
        norm_checks = {
            "checks": [],
            "paragraph_checks": [
                {"finding_id": "F-001", "norm": "СП 256",
                 "paragraph_verified": True,
                 "actual_quote": "Другая цитата"},
            ],
        }
        enrich_findings_from_norm_checks(findings, norm_checks)
        assert findings[0]["norm_quote"] == "Существующая хорошая цитата"

    def test_norm_status_set(self):
        findings = [
            {"id": "F-001", "norm": "СП 256", "norm_quote": None, "norm_confidence": 0.6},
            {"id": "F-002", "norm": "", "norm_confidence": 0.0},
        ]
        enrich_findings_from_norm_checks(findings, {"checks": [], "paragraph_checks": []})
        assert findings[0]["norm_status"] == "norm_detected_no_quote"
        assert findings[1]["norm_status"] == "no_norm_cited"


# ─── should_review_norm ────────────────────────────────────────────────────

class TestNormPolicyClass:
    def test_critical_required(self):
        assert compute_norm_policy_class({"severity": "КРИТИЧЕСКОЕ"}) == "required"

    def test_economic_required(self):
        assert compute_norm_policy_class({"severity": "ЭКОНОМИЧЕСКОЕ"}) == "required"

    def test_exploitation_recommended(self):
        assert compute_norm_policy_class({"severity": "ЭКСПЛУАТАЦИОННОЕ"}) == "recommended"

    def test_recommendation_optional(self):
        assert compute_norm_policy_class({"severity": "РЕКОМЕНДАТЕЛЬНОЕ"}) == "optional"

    def test_check_optional(self):
        assert compute_norm_policy_class({"severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ"}) == "optional"

    def test_enrichment_sets_policy(self):
        findings = [{"id": "F-1", "norm": "", "severity": "КРИТИЧЕСКОЕ", "norm_confidence": 0}]
        enrich_findings_from_norm_checks(findings, {"checks": [], "paragraph_checks": []})
        assert findings[0]["norm_policy_class"] == "required"


class TestShouldReviewNorm:
    def test_no_norm_optional_skip(self):
        """no_norm_cited + optional severity → не проверять."""
        f = {"norm": "", "norm_status": "no_norm_cited", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"}
        assert should_review_norm(f) is False

    def test_no_norm_required_review(self):
        """no_norm_cited + required severity → проверять."""
        f = {"norm": "", "norm_status": "no_norm_cited", "severity": "КРИТИЧЕСКОЕ"}
        assert should_review_norm(f) is True

    def test_no_norm_economic_review(self):
        """no_norm_cited + ЭКОНОМИЧЕСКОЕ → проверять."""
        f = {"norm": "", "norm_status": "no_norm_cited", "severity": "ЭКОНОМИЧЕСКОЕ"}
        assert should_review_norm(f) is True

    def test_no_norm_skip(self):
        """no_norm_cited + РЕКОМЕНДАТЕЛЬНОЕ → не проверять (backward compat alias)."""
        f = {"norm": "", "norm_status": "no_norm_cited", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"}
        assert should_review_norm(f) is False

    def test_invalid_reference_review(self):
        """invalid_reference → обязательно проверять."""
        f = {"norm": "ГОСТ 13109-97", "norm_status": "invalid_reference", "severity": "РЕКОМЕНДАТЕЛЬНОЕ"}
        assert should_review_norm(f) is True

    def test_critical_low_conf_review(self):
        """КРИТИЧЕСКОЕ с conf < 0.7 → проверять."""
        f = {"norm": "ПУЭ-7, п. 3.1.8", "norm_status": "norm_detected_no_quote",
             "severity": "КРИТИЧЕСКОЕ", "norm_confidence": 0.6}
        assert should_review_norm(f) is True

    def test_critical_high_conf_skip(self):
        """КРИТИЧЕСКОЕ с conf >= 0.7 → не проверять."""
        f = {"norm": "ПУЭ-7, п. 3.1.8", "norm_status": "norm_detected_no_quote",
             "severity": "КРИТИЧЕСКОЕ", "norm_confidence": 0.75}
        assert should_review_norm(f) is False

    def test_recommendation_low_conf_skip(self):
        """РЕКОМЕНДАТЕЛЬНОЕ с conf 0.5 → не проверять (порог 0.5)."""
        f = {"norm": "ГОСТ 21.101", "norm_status": "norm_detected_no_quote",
             "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "norm_confidence": 0.5}
        assert should_review_norm(f) is False

    def test_recommendation_very_low_conf_review(self):
        """РЕКОМЕНДАТЕЛЬНОЕ с conf < 0.5 → проверять."""
        f = {"norm": "ГОСТ 21.101", "norm_status": "norm_detected_no_quote",
             "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "norm_confidence": 0.3}
        assert should_review_norm(f) is True

    def test_thresholds_differentiated(self):
        """Пороги различаются по severity."""
        assert NORM_CONFIDENCE_THRESHOLDS["КРИТИЧЕСКОЕ"] > NORM_CONFIDENCE_THRESHOLDS["РЕКОМЕНДАТЕЛЬНОЕ"]


# ─── Revision logic ───────────────────────────────────────────────────────

class TestRevisionLogic:
    def test_not_found_needs_revision(self):
        """status=not_found → needs_revision=True в deterministic checks."""
        from norms import generate_deterministic_checks

        # Минимальный norms_data с нормой, которой нет в DB
        norms_data = {
            "norms": {
                "ГОСТ 99999-2099": {
                    "cited_as": ["ГОСТ 99999-2099"],
                    "affected_findings": ["F-001"],
                },
            },
        }
        result = generate_deterministic_checks(norms_data, project_id="test")
        checks = result["checks"]
        assert len(checks) == 1
        # Unknown norms = needs_revision=True (до WebSearch)
        assert checks[0]["needs_revision"] is True
