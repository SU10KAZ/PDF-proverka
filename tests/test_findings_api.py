"""Тесты для API замечаний — пагинация и фильтрация."""
import json
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from webapp.services.findings_service import get_findings


@pytest.fixture
def mock_findings():
    """Mock данные замечаний."""
    findings = [
        {"id": f"F-{i:03d}", "severity": sev, "category": "cable", "sheet": "3"}
        for i, sev in enumerate([
            "КРИТИЧЕСКОЕ", "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ",
            "РЕКОМЕНДАТЕЛЬНОЕ", "РЕКОМЕНДАТЕЛЬНОЕ", "РЕКОМЕНДАТЕЛЬНОЕ",
            "ЭКСПЛУАТАЦИОННОЕ", "ЭКСПЛУАТАЦИОННОЕ",
        ], 1)
    ]
    return {"findings": findings}


def test_get_findings_no_pagination(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test")
        assert result is not None
        assert result.total == 8
        assert len(result.findings) == 8
        assert result.filtered_total == 8


def test_get_findings_with_limit(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test", limit=3)
        assert len(result.findings) == 3
        assert result.total == 8
        assert result.filtered_total == 8


def test_get_findings_with_offset(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test", offset=5)
        assert len(result.findings) == 3  # 8 - 5 = 3
        assert result.filtered_total == 8


def test_get_findings_with_limit_and_offset(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test", limit=2, offset=2)
        assert len(result.findings) == 2
        assert result.filtered_total == 8


def test_get_findings_severity_filter(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test", severity="КРИТИЧЕСКОЕ")
        assert len(result.findings) == 2
        assert result.filtered_total == 2
        assert result.total == 8


def test_get_findings_filter_then_paginate(mock_findings):
    with patch("webapp.services.findings_service._load_json", return_value=mock_findings), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("test", severity="РЕКОМЕНДАТЕЛЬНОЕ", limit=2, offset=0)
        assert len(result.findings) == 2
        assert result.filtered_total == 3
        assert result.total == 8


def test_get_findings_returns_none_for_missing():
    with patch("webapp.services.findings_service._load_json", return_value=None), \
         patch("webapp.services.findings_service._enrich_sheet_page"):
        result = get_findings("nonexistent")
        assert result is None
