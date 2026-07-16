"""Тесты сверки MD-зеркала с вектор-слоем (md_mirror_reconcile) — ветвление форматов MD.

Новый формат портала (*_results.md) читается нативным парсером results_md,
старый Chandra-формат — прежним кодом. Проверяем:
  1. Разбор ТЕКСТ-блоков обоих форматов в единый вид {block_id: текст}.
  2. IMAGE-блоки нового формата НЕ участвуют в сверке (их тело — структурное
     описание, не транскрипция).
  3. Детект нового формата по имени файла И по содержимому.
  4. Паритет end-to-end: одна и та же OCR-ошибка (потеря десятичной точки)
     даёт одинаковую подсветку и на старом, и на новом формате MD.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.md_mirror_reconcile import (
    _parse_chandra_text_blocks,
    _parse_md_text_blocks,
    _results_md_text_blocks,
    build_reconcile_annotation,
    reconcile_block,
)

BLK_TEXT = "blk_" + "a" * 32
BLK_IMAGE = "blk_" + "b" * 32

# Новый формат портала vibe: шапка + Page/BLOCK #N, IMAGE со структурой секций.
NEW_MD = f"""\
# Document: TEST_V1.pdf

Path: ЭОМ / TEST / TEST_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: TEST | Stage: Р | Object: Объект, по адресу: г. Москва | Organization: ОРГ

---

## Page 1

### BLOCK #1 [TEXT]: {BLK_TEXT}

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok1)
> **Stamp:** Code: TEST | Stage: Р | Sheet: 2 | Object: Объект | Name: План | Organization: ОРГ | Revisions:

Кабель 3x15 сеч.

### BLOCK #2 [IMAGE]: {BLK_IMAGE}

> **Created:** 2026-07-07 15:23:00 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok2)
> **Stamp:** Code: TEST | Stage: Р | Sheet: 2 | Object: Объект | Name: План | Organization: ОРГ | Revisions:

**[IMAGE]** | Type: План | Axes: Оси: А-Б | Zone: — | Level: 1

**Summary:** План этажа

**Description:** Кабель 3x18 вдоль оси А

**Entities:** 3x18

**Verification:** ок
"""

# Старый Chandra-формат: та же полезная нагрузка ТЕКСТ-блока.
OLD_MD = f"""\
## СТРАНИЦА 1

### BLOCK [TEXT]: {BLK_TEXT}

Кабель 3x15 сеч.

### BLOCK [IMAGE]: {BLK_IMAGE}

[IMAGE] План с кабелем 3x18
"""


# ── Разбор ТЕКСТ-блоков ──────────────────────────────────────────────────────

class TestParseNewFormat:
    def test_by_name(self, tmp_path):
        p = tmp_path / "TEST_V1_results.md"
        p.write_text(NEW_MD, encoding="utf-8")
        blocks = _parse_md_text_blocks(p)
        assert blocks == {BLK_TEXT: "Кабель 3x15 сеч."}

    def test_by_content_with_legacy_name(self, tmp_path):
        # новый формат внутри файла со «старым» именем — детект по содержимому
        p = tmp_path / "TEST_V1_document.md"
        p.write_text(NEW_MD, encoding="utf-8")
        blocks = _parse_md_text_blocks(p)
        assert BLK_TEXT in blocks

    def test_image_blocks_excluded(self):
        blocks = _results_md_text_blocks(NEW_MD)
        assert BLK_IMAGE not in blocks
        assert list(blocks) == [BLK_TEXT]

    def test_body_without_meta_quotes(self):
        blocks = _results_md_text_blocks(NEW_MD)
        assert "**Created:**" not in blocks[BLK_TEXT]
        assert "Stamp" not in blocks[BLK_TEXT]


class TestParseOldFormatUnchanged:
    def test_routed_to_chandra_parser(self, tmp_path):
        p = tmp_path / "TEST_V1_document.md"
        p.write_text(OLD_MD, encoding="utf-8")
        blocks = _parse_md_text_blocks(p)
        assert blocks == _parse_chandra_text_blocks(OLD_MD)
        assert blocks[BLK_TEXT].strip() == "Кабель 3x15 сеч."

    def test_old_image_blocks_excluded(self):
        blocks = _parse_chandra_text_blocks(OLD_MD)
        assert BLK_IMAGE not in blocks

    def test_old_format_not_misdetected_as_new(self, tmp_path):
        from backend.app.services.common.results_md import is_results_md_text
        assert not is_results_md_text(OLD_MD)


# ── Ядро сверки не зависит от формата ────────────────────────────────────────

def test_reconcile_block_lost_decimal_point():
    hl = reconcile_block("Кабель 3x15 сеч.", "3x1.5")
    assert hl == [{"md": "3x15", "vector": "3x1.5",
                   "kind": "потеря десятичной точки", "verdict": "вектор верен"}]


# ── End-to-end: подсветка одинакова на старом и новом формате ────────────────

@pytest.fixture()
def pdf_and_graph(tmp_path):
    """Мини-PDF с вектор-словом «3x1.5» + document_graph с текст-блоком на всю страницу."""
    fitz = pytest.importorskip("fitz")
    pdf_path = tmp_path / "doc.pdf"
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((100, 100), "3x1.5")
    doc.save(str(pdf_path))
    doc.close()
    dg = {"pages": [{"page_index": 0, "sheet_no": "2",
                     "text_blocks": [{"id": BLK_TEXT, "coords_norm": [0, 0, 1, 1]}]}]}
    dg_path = tmp_path / "document_graph.json"
    dg_path.write_text(json.dumps(dg, ensure_ascii=False), encoding="utf-8")
    return pdf_path, dg_path


def _annotation_for(md_name: str, md_text: str, tmp_path, pdf_and_graph) -> str:
    pdf_path, dg_path = pdf_and_graph
    md_path = tmp_path / md_name
    md_path.write_text(md_text, encoding="utf-8")
    return build_reconcile_annotation(str(md_path), str(pdf_path), str(dg_path))


def test_annotation_new_format(tmp_path, pdf_and_graph):
    ann = _annotation_for("TEST_V1_results.md", NEW_MD, tmp_path, pdf_and_graph)
    assert "В MD «3x15»" in ann
    assert "«3x1.5»" in ann
    assert "вектор верен" in ann
    assert "Лист 2" in ann
    # значение из IMAGE-блока не должно попасть в сверку
    assert "3x18" not in ann


def test_annotation_parity_old_vs_new(tmp_path, pdf_and_graph):
    ann_new = _annotation_for("TEST_V1_results.md", NEW_MD, tmp_path, pdf_and_graph)
    ann_old = _annotation_for("TEST_V1_document.md", OLD_MD, tmp_path, pdf_and_graph)
    assert ann_new == ann_old
    assert ann_old  # не пустая: старый путь работает как раньше


def test_annotation_fail_soft_missing_files(tmp_path):
    assert build_reconcile_annotation(
        str(tmp_path / "нет.md"), str(tmp_path / "нет.pdf"),
        str(tmp_path / "нет.json")) == ""
