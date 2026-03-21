"""Sorting tests for findings API."""

from unittest.mock import patch

from webapp.services.findings_service import get_findings


def test_same_severity_prefers_higher_practicality():
    findings = {
        "findings": [
            {
                "id": "F-001",
                "severity": "КРИТИЧЕСКОЕ",
                "category": "documentation",
                "quality": {"practicality_score": 20},
            },
            {
                "id": "F-002",
                "severity": "КРИТИЧЕСКОЕ",
                "category": "evacuation",
                "quality": {"practicality_score": 95},
            },
        ]
    }

    with patch("webapp.services.findings_service._load_json", return_value=findings), patch(
        "webapp.services.findings_service._enrich_sheet_page"
    ):
        result = get_findings("test")

    assert result is not None
    assert [item["id"] for item in result.findings] == ["F-002", "F-001"]
