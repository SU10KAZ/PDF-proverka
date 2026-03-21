"""Тесты для paragraph cache (norms.py).

Проверяет upsert, read, merge, защиту от неподтверждённых цитат.
"""
import json
import pytest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime


# ─── Фикстуры ────────────────────────────────────────────────
@pytest.fixture
def empty_pdb():
    """Пустой paragraph database."""
    return {
        "meta": {
            "description": "Test",
            "last_updated": None,
            "total_paragraphs": 0,
        },
        "paragraphs": {},
    }


@pytest.fixture
def populated_pdb():
    """Paragraph database с несколькими записями."""
    return {
        "meta": {
            "description": "Test",
            "last_updated": "2026-01-01T00:00:00",
            "total_paragraphs": 2,
        },
        "paragraphs": {
            "СП 256.1325800.2016, п. 15.3": {
                "norm": "СП 256.1325800.2016",
                "quote": "Сечение нулевых защитных проводников должно быть не менее...",
                "verified_at": "2026-01-01T00:00:00",
                "verified_via": "websearch",
                "confidence": 0.95,
                "source_project": "test-project",
            },
            "СП 54.13330.2022, п. 4.6": {
                "norm": "СП 54.13330.2022",
                "quote": "Высота жилых помещений от пола до потолка должна быть не менее 2,5 м",
                "verified_at": "2026-01-01T00:00:00",
                "verified_via": "manual",
                "confidence": 1.0,
                "source_project": "test-project-2",
            },
        },
    }


# ─── get_paragraph ──────────────────────────────────────────
class TestGetParagraph:
    def test_get_existing(self, populated_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=populated_pdb):
            from norms import get_paragraph
            result = get_paragraph("СП 256.1325800.2016, п. 15.3")
            assert result is not None
            assert "quote" in result
            assert result["confidence"] == 0.95

    def test_get_nonexistent(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb):
            from norms import get_paragraph
            result = get_paragraph("несуществующий")
            assert result is None


# ─── upsert_paragraph ───────────────────────────────────────
class TestUpsertParagraph:
    def test_add_new(self, empty_pdb):
        saved = {}
        def mock_save(pdb):
            saved["pdb"] = pdb
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb), \
             patch("norms._core.save_norms_paragraphs", side_effect=mock_save):
            from norms import upsert_paragraph
            result = upsert_paragraph(
                paragraph_key="СП 1.2.3, п. 4.5",
                quote="Тестовая цитата",
                norm_key="СП 1.2.3",
                verified=True,
            )
            assert result == "added"
            assert "СП 1.2.3, п. 4.5" in saved["pdb"]["paragraphs"]

    def test_skip_unverified(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb):
            from norms import upsert_paragraph
            result = upsert_paragraph(
                paragraph_key="СП 1.2.3, п. 4.5",
                quote="Неподтверждённая цитата",
                verified=False,
            )
            assert result == "skipped"

    def test_skip_empty_quote(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb):
            from norms import upsert_paragraph
            result = upsert_paragraph(
                paragraph_key="СП 1.2.3",
                quote="",
                verified=True,
            )
            assert result == "skipped"

    def test_skip_duplicate(self, populated_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=populated_pdb):
            from norms import upsert_paragraph
            result = upsert_paragraph(
                paragraph_key="СП 256.1325800.2016, п. 15.3",
                quote="Сечение нулевых защитных проводников должно быть не менее...",
                verified=True,
            )
            assert result == "skipped"

    def test_no_downgrade_confidence(self, populated_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=populated_pdb):
            from norms import upsert_paragraph
            # Попытка записать с более низким confidence
            result = upsert_paragraph(
                paragraph_key="СП 54.13330.2022, п. 4.6",
                quote="Другая цитата",
                confidence=0.5,
                verified=True,
            )
            assert result == "skipped"


# ─── merge_paragraph_checks ──────────────────────────────────
class TestMergeParagraphChecks:
    def test_merge_verified(self, empty_pdb):
        saved = {}
        def mock_save(pdb):
            saved["pdb"] = pdb
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb), \
             patch("norms._core.save_norms_paragraphs", side_effect=mock_save):
            from norms import merge_paragraph_checks
            checks = [
                {
                    "paragraph_verified": True,
                    "norm": "СП 1.2.3",
                    "paragraph_key": "СП 1.2.3, п. 1",
                    "actual_quote": "Цитата номер один",
                    "confidence": 0.9,
                },
                {
                    "paragraph_verified": True,
                    "norm": "ГОСТ 4.5.6",
                    "paragraph_key": "ГОСТ 4.5.6, п. 2",
                    "actual_quote": "Цитата номер два",
                },
            ]
            result = merge_paragraph_checks(checks, project_id="test")
            assert result["added"] == 2
            assert result["skipped"] == 0

    def test_merge_skips_unverified(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb), \
             patch("norms._core.save_norms_paragraphs"):
            from norms import merge_paragraph_checks
            checks = [
                {
                    "paragraph_verified": False,
                    "norm": "СП 1.2.3",
                    "actual_quote": "Неподтверждённая цитата",
                },
            ]
            result = merge_paragraph_checks(checks)
            assert result["skipped"] == 1
            assert result["added"] == 0

    def test_merge_empty_list(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb):
            from norms import merge_paragraph_checks
            result = merge_paragraph_checks([])
            assert result["added"] == 0


# ─── paragraph_cache_stats ────────────────────────────────────
class TestParagraphCacheStats:
    def test_stats_populated(self, populated_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=populated_pdb):
            from norms import paragraph_cache_stats
            result = paragraph_cache_stats()
            assert result["total"] == 2
            assert result["empty_quote"] == 0
            assert "websearch" in result["by_verified_via"]
            assert "manual" in result["by_verified_via"]

    def test_stats_empty(self, empty_pdb):
        with patch("norms._core.load_norms_paragraphs", return_value=empty_pdb):
            from norms import paragraph_cache_stats
            result = paragraph_cache_stats()
            assert result["total"] == 0
