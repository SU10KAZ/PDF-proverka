"""Тесты для norms.py — детерминированная верификация."""
import json
import pytest
import sys
from pathlib import Path
from unittest.mock import patch
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from norms import generate_deterministic_checks, validate_norm_checks


@pytest.fixture
def mock_norms_db():
    """Mock norms_db.json через patch."""
    db = {
        "meta": {"stale_after_days": 30},
        "norms": {
            "СП 256.1325800.2016": {
                "status": "active",
                "title": "Электроустановки жилых и общественных зданий",
                "last_verified": datetime.now().isoformat(),
                "replaced_by": None,
            },
            "СП 31-110-2003": {
                "status": "replaced",
                "title": "Проектирование и монтаж электроустановок",
                "last_verified": datetime.now().isoformat(),
                "replaced_by": "СП 256.1325800.2016",
            },
        },
        "replacements": {},
    }
    return db


def test_deterministic_checks_active_norm(mock_norms_db):
    norms_data = {
        "norms": {
            "СП 256.1325800.2016": {
                "cited_as": ["СП 256.1325800.2016"],
                "affected_findings": ["F-001"],
            },
        },
    }
    with patch("norms._core.load_norms_db", return_value=mock_norms_db), \
         patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["checks"]) == 1
    check = result["checks"][0]
    assert check["status"] == "active"


def test_deterministic_checks_replaced_norm(mock_norms_db):
    norms_data = {
        "norms": {
            "СП 31-110-2003": {
                "cited_as": ["СП 31-110-2003"],
                "affected_findings": ["F-002"],
            },
        },
    }
    with patch("norms._core.load_norms_db", return_value=mock_norms_db), \
         patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["checks"]) == 1
    check = result["checks"][0]
    assert check["status"] == "replaced"


def test_deterministic_checks_unknown_norm(mock_norms_db):
    norms_data = {
        "norms": {
            "ГОСТ 99999-2099": {
                "cited_as": ["ГОСТ 99999-2099"],
                "affected_findings": ["F-003"],
            },
        },
    }
    with patch("norms._core.load_norms_db", return_value=mock_norms_db), \
         patch("norms._core.load_norms_paragraphs", return_value={"paragraphs": {}}):
        result = generate_deterministic_checks(norms_data, project_id="test")
    assert len(result["unknown_norms"]) >= 1


def test_validate_norm_checks(tmp_path):
    checks_data = {
        "meta": {},
        "checks": [
            {"norm": "СП 256", "status": "active", "verified_via": "cache",
             "needs_revision": False},
            {"norm": "Unknown", "status": "unknown", "verified_via": "none",
             "needs_revision": False},
        ],
    }
    checks_path = tmp_path / "norm_checks.json"
    checks_path.write_text(json.dumps(checks_data, ensure_ascii=False), encoding="utf-8")
    result = validate_norm_checks(checks_path)
    assert result["total_checks"] == 2
