"""Тесты для task_builder — формирование контекста блоков."""
import json
import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from webapp.services.task_builder import _build_structured_block_context


def _make_graph(pages):
    return {"pages": pages}


def test_build_context_includes_text_block_ids():
    """text_block_id должен появляться в контексте."""
    graph = _make_graph([
        {
            "page": 1,
            "sheet_no": "1",
            "text_blocks": [
                {"id": "RUXD-WP4R-6C3", "text": "Спецификация кабелей"},
            ],
            "image_blocks": [
                {"id": "block_001", "type": "схема", "ocr": ""},
            ],
        },
    ])
    block_ids = ["block_001"]
    block_pages = [1]

    result = _build_structured_block_context(graph, block_ids, block_pages)
    assert "RUXD-WP4R-6C3" in result


def test_build_context_includes_image_blocks():
    """image_block info должна быть в контексте."""
    graph = _make_graph([
        {
            "page": 2,
            "sheet_no": "2",
            "text_blocks": [],
            "image_blocks": [
                {"id": "block_010", "type": "план", "ocr": "План вентиляции"},
            ],
        },
    ])
    block_ids = ["block_010"]
    block_pages = [2]

    result = _build_structured_block_context(graph, block_ids, block_pages)
    assert "block_010" in result


def test_build_context_empty_graph():
    """Пустой граф — пустой контекст."""
    graph = _make_graph([])
    result = _build_structured_block_context(graph, [], [])
    assert result == "" or result.strip() == ""
