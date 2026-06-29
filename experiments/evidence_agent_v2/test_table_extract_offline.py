"""Офлайн-тесты извлечения таблиц."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from experiments.evidence_agent_v2 import table_extract as te


def test_is_table_block_by_label():
    assert te.is_table_block({"label": "Ведомость отверстий с марками"})
    assert te.is_table_block({"label": "Спецификация элементов перемычек"})
    assert not te.is_table_block({"label": "План типового этажа с осями"})


def test_is_table_block_by_sheet_type():
    assert te.is_table_block({"sheet_type": "table_legend"})


def test_extract_uses_existing_key_values():
    out = te.extract_table_values(
        {"block_id": "B1", "key_values_read": ["85: 900мм", "86: 1380мм"]}, Path("/tmp"))
    assert out.status == "extracted" and out.source == "blocks_analysis"
    assert "85: 900мм" in out.values


def test_extract_without_source_needs_vision():
    out = te.extract_table_values({"block_id": "B2", "label": "Ведомость"}, Path("/tmp/nonexistent"))
    assert out.status == "needs_vision"


def test_rows_from_words_groups_by_y():
    words = [
        {"text": "Марка", "bbox": [0, 0, 10, 5]},
        {"text": "85", "bbox": [20, 0, 30, 5]},
        {"text": "900", "bbox": [0, 30, 10, 35]},
    ]
    rows = te._rows_from_words(words)
    assert any("Марка" in r and "85" in r for r in rows)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
