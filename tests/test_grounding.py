"""Тесты для grounding_service."""
import pytest
from webapp.services.grounding_service import (
    _tokenize,
    _compute_overlap,
    _finding_is_well_grounded,
    compute_grounding_candidates,
)


def test_tokenize_basic():
    tokens = _tokenize("Кабель ВВГнг 3x2.5 на стр. 12")
    assert "кабель" in tokens
    assert "ввгнг" in tokens
    # Короткие слова (< 3 символов) отфильтрованы
    assert "на" not in tokens
    assert "стр" in tokens


def test_tokenize_empty():
    assert _tokenize("") == []
    assert _tokenize("  ") == []


def test_compute_overlap_identical():
    tokens = ["кабель", "ввгнг", "сечение"]
    score = _compute_overlap(tokens, tokens)
    assert score == 1.0


def test_compute_overlap_disjoint():
    a = ["кабель", "ввгнг"]
    b = ["труба", "вентиляция"]
    score = _compute_overlap(a, b)
    assert score == 0.0


def test_compute_overlap_partial():
    a = ["кабель", "ввгнг", "сечение"]
    b = ["кабель", "ввгнг", "лоток"]
    score = _compute_overlap(a, b)
    assert 0.0 < score < 1.0


def test_compute_overlap_empty():
    assert _compute_overlap([], ["кабель"]) == 0.0
    assert _compute_overlap(["кабель"], []) == 0.0


def test_finding_is_well_grounded_with_image_evidence():
    f = {"evidence": [{"type": "image", "block_id": "b1"}]}
    assert _finding_is_well_grounded(f) is True


def test_finding_is_well_grounded_with_related():
    f = {"related_block_ids": ["b1"]}
    assert _finding_is_well_grounded(f) is True


def test_finding_is_not_grounded():
    f = {"problem": "test", "evidence": [], "related_block_ids": []}
    assert _finding_is_well_grounded(f) is False


def test_finding_is_not_grounded_no_fields():
    f = {"problem": "test"}
    assert _finding_is_well_grounded(f) is False


def test_compute_grounding_candidates_basic():
    findings = [
        {
            "id": "F-001",
            "problem": "Кабель ВВГнг сечение не соответствует",
            "description": "Сечение кабеля ВВГнг занижено",
            "evidence": [],
            "related_block_ids": [],
        },
    ]
    blocks = [
        {
            "block_id": "block_001",
            "page": 3,
            "summary": "Спецификация кабелей ВВГнг с сечениями",
            "findings": [],
            "key_values_read": [],
        },
        {
            "block_id": "block_002",
            "page": 5,
            "summary": "План вентиляции первого этажа",
            "findings": [],
            "key_values_read": [],
        },
    ]
    result = compute_grounding_candidates(findings, blocks)
    assert len(result) == 1
    f = result[0]
    assert "grounding_candidates" in f
    assert f["grounding_candidates"][0]["block_id"] == "block_001"
    assert f["related_block_ids"] == ["block_001"]


def test_compute_grounding_skips_well_grounded():
    findings = [
        {
            "id": "F-001",
            "problem": "test",
            "evidence": [{"type": "image", "block_id": "b1"}],
            "related_block_ids": ["b1"],
        },
    ]
    blocks = [{"block_id": "b1", "page": 1, "summary": "test", "findings": [], "key_values_read": []}]
    result = compute_grounding_candidates(findings, blocks)
    assert "grounding_candidates" not in result[0]


def test_compute_grounding_page_bonus():
    findings = [
        {
            "id": "F-001",
            "problem": "Кабель ВВГнг",
            "page": 3,
            "evidence": [],
            "related_block_ids": [],
        },
    ]
    blocks = [
        {"block_id": "b1", "page": 3, "summary": "Кабель ВВГнг спецификация", "findings": [], "key_values_read": []},
        {"block_id": "b2", "page": 7, "summary": "Кабель ВВГнг спецификация", "findings": [], "key_values_read": []},
    ]
    result = compute_grounding_candidates(findings, blocks)
    candidates = result[0].get("grounding_candidates", [])
    assert len(candidates) >= 1
    # b1 (same page) should rank higher
    assert candidates[0]["block_id"] == "b1"
