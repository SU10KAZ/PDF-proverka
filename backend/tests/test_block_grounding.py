"""Тесты ядра Value Grounding (Phase 1)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.grounding import (
    concrete_class_canon,
    extract_concrete_classes,
    ground_block,
    vector_usable,
)

_VEC = "Бетон В40 F100 W4, колонна Км-400-1200-2800-1, отм. +12.500, размеры 5000 3000"


def test_concrete_canon_dot_zero_is_class():
    # «В4.0» (int=4, frac=0) — это В40 (нет десятичных классов по ГОСТ 26633)
    assert concrete_class_canon("4", "0") == "В40"
    assert concrete_class_canon("40", None) == "В40"


def test_extract_concrete_classes():
    assert "В40" in extract_concrete_classes("бетон В40 F100")
    assert "В40" in extract_concrete_classes("бетон В4.0 F100")  # опечатка → канон В40
    assert "В30" in extract_concrete_classes("B30 W6")           # латинская B


def test_ground_detects_glyph_error_v40():
    # КЛЮЧЕВОЙ кейс: вектор=В40, gemma прочла В4.0 → корректировка
    g = ground_block(gemma_text="класс бетона В4.0 F100 W4", pdfplumber_text=_VEC)
    assert g["vector_usable"] is True
    assert g["value_source"] == "vector"
    corr = [c for c in g["corrections"] if c["field"] == "concrete_class"]
    assert corr, "должна быть корректировка класса бетона"
    assert corr[0]["gemma_value"] in {"В4.0", "В4,0"}
    assert corr[0]["grounded_value"] == "В40"


def test_ground_no_correction_when_gemma_agrees():
    g = ground_block(gemma_text="бетон В40 F100", pdfplumber_text=_VEC)
    assert not [c for c in g["corrections"] if c["field"] == "concrete_class"]


def test_ground_latin_b_variant():
    g = ground_block(gemma_text="concrete B40", pdfplumber_text=_VEC)
    # B40 — латинская запись того же класса → НЕ ошибка (содержит цифры 40)
    assert not [c for c in g["corrections"] if c.get("gemma_value") == "B40" and c["grounded_value"] != "В40"]


def test_number_space_normalization_recall():
    g = ground_block(gemma_text="габарит 24775", pdfplumber_text="общий размер 24 775 мм по фасаду 12345")
    # «24 775» из вектора склеивается → 24775 совпадает с gemma
    assert g["gemma_number_recall_vs_vector"] is not None


def test_vector_unusable_short():
    assert vector_usable("abc") is False
    g = ground_block(gemma_text="В4.0", pdfplumber_text="")
    assert g["vector_usable"] is False
    assert g["value_source"] == "gemma_only"
    # Доменное правило срабатывает БЕЗ эталона: «В4.0» невозможен по ГОСТ → «В40».
    assert any(c["grounded_value"] == "В40" and c["scope"] == "domain_rule"
               for c in g["corrections"])


def test_domain_rule_only_valid_collapse():
    # «В7.0» → «В70» НЕ валиден (макс В60) → не корректируем (консервативно)
    g = ground_block(gemma_text="В7.0", pdfplumber_text="")
    assert not g["corrections"]
    # «В1.0» → «В10» валиден
    g2 = ground_block(gemma_text="отметка В1.0 уровень", pdfplumber_text="")
    assert any(c["grounded_value"] == "В10" for c in g2["corrections"])


def test_vector_garbled_rejected():
    assert vector_usable("№№№@@@###$$$%%%^^^&&&***((()))" * 3) is False


def test_tile_grid_covers_large_block():
    from backend.app.pipeline.stages.block_grounding.qwen_grounding import tile_grid
    tiles = tile_grid([0, 0, 18000, 6000], tile=2600, overlap=350)
    assert len(tiles) >= 8                       # большой блок → много тайлов
    assert all(t[2] <= 18000 and t[3] <= 6000 for t in tiles)  # не выходят за bbox
    assert tiles[0][0] == 0 and tiles[0][1] == 0


def test_tile_grid_cap():
    from backend.app.pipeline.stages.block_grounding.qwen_grounding import tile_grid
    assert len(tile_grid([0, 0, 99000, 99000], tile=1000, overlap=100, max_tiles=10)) == 10


def test_extract_values_from_parsed_and_raw():
    from backend.app.pipeline.stages.block_grounding.qwen_grounding import _extract_values

    class _R:
        def __init__(self, parsed=None, raw=""):
            self.parsed = parsed
            self.full_raw_response = raw
            self.raw_response_excerpt = raw

    assert _extract_values(_R(parsed={"values": ["10A", "В40", 250]})) == ["10A", "В40", "250"]
    assert "ВРУ-1" in _extract_values(_R(raw='прочее {"values": ["ВРУ-1", "QF5"]} хвост'))
    assert _extract_values(_R()) == []


def test_select_qwen_blocks_priority_and_cap():
    from backend.app.pipeline.stages.block_grounding.runner import _select_qwen_blocks
    cands = [{"block_id": f"b{i}", "width": w} for i, w in enumerate([8000, 7000, 3000, 2000, 500])]
    # crop выключен (crop_min=0) → только крупные, mode=tiled, по убыванию ширины
    sel = _select_qwen_blocks(cands, tile_min=6000, crop_min=0, max_blocks=10)
    assert [c["block_id"] for c in sel] == ["b0", "b1"]
    assert all(c["mode"] == "tiled" for c in sel)
    # crop включён → средние добавляются ПОСЛЕ крупных, мелочь (<crop_min) отброшена
    sel2 = _select_qwen_blocks(cands, tile_min=6000, crop_min=1500, max_blocks=10)
    modes = {c["block_id"]: c["mode"] for c in sel2}
    assert modes["b0"] == "tiled" and modes["b2"] == "crop"
    assert "b4" not in modes  # 500px < crop_min
    # cap соблюдается, крупные имеют приоритет
    capped = _select_qwen_blocks(cands, tile_min=6000, crop_min=1500, max_blocks=2)
    assert [c["block_id"] for c in capped] == ["b0", "b1"]


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
