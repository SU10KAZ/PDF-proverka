"""Тесты нативного парсера results_md.py (новый MD-формат портала vibe).

Фикстура повторяет реальные грабли формата: пустой Sheet («Sheet:  |»),
вложенные двоеточия в Object/Axes, IMAGE-секции, хвостовой «Revisions: »,
задвоенные маркеры списков. Дополнительно — сверка на реальных выгрузках из
experiments/новая структура./*.zip, если они есть на диске (skip иначе).
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from backend.app.services.common.results_md import (
    ResultsMdDocument,
    is_results_md_name,
    is_results_md_text,
    parse_results_md,
    parse_stamp_line,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLES_DIR = REPO_ROOT / "experiments" / "новая структура."

SAMPLE = """\
# Document: ПД-00542664-АР1.2-1_V1.pdf

Path: АР / ПД-00542664-АР1.2-1 / ПД-00542664-АР1.2-1_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Object: «Застройка», по адресу: г. Москва, кв. 3 | Organization: ЮНИПРО

---

## Page 1

### BLOCK #1 [TEXT]: blk_8db07fd5e0a24e12b549745faa1ad4f0

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/_lkFrM2goEJnxe6ScFhb)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet:  | Object: «Застройка», по адресу: г. Москва, кв. 3 | Name:  | Organization: ЮНИПРО | Revisions:

ООО «ЮНИПРО»

#### Рабочая документация

- - с заданием на проектирование
1. 1. Общие указания

## Page 2

### BLOCK #2 [TEXT]: blk_d130bc9d42364116af2e047aadf48a48

> **Created:** 2026-07-07 15:19:40 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/bnWfCZBJEJxhE0CIFgEA)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet: 2 | Object: «Застройка» | Name: Кладочный план 1-го этажа | Organization: ЮНИПРО | Revisions:

| Лист | Наименование |
|---|---|
| 1 | Общие данные |

### BLOCK #3 [IMAGE]: blk_a31258a5e5fa40e681c23ec1cf59a07d

> **Created:** 2026-07-07 15:22:41 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/DcX3vJyomTOEmRFOTHVm)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet: 2 | Object: «Застройка» | Name: Кладочный план 1-го этажа | Organization: ЮНИПРО | Revisions:

**[IMAGE]** | Type: План | Axes: Оси: А, Б; 1, 2 | Level: Этаж 1

**Summary:** Кладочный план первого этажа.

**Description:** 1) Стены из блоков.
2) Перемычки по серии.

**Entities:** стены, перемычки, проёмы

**Verification:** Сверить толщины стен с АР.
"""


@pytest.fixture(scope="module")
def doc() -> ResultsMdDocument:
    return parse_results_md(SAMPLE)


class TestDetect:
    def test_detects_new_format(self):
        assert is_results_md_text(SAMPLE) is True

    def test_rejects_old_chandra_format(self):
        assert is_results_md_text("## СТРАНИЦА 1\n\nтекст\n") is False

    def test_rejects_empty(self):
        assert is_results_md_text("") is False

    def test_name_detect(self):
        assert is_results_md_name("X_results.md") is True
        assert is_results_md_name("X_document.md") is False


class TestHeader:
    def test_document_name(self, doc):
        assert doc.document_name == "ПД-00542664-АР1.2-1_V1.pdf"

    def test_path_and_discipline_hint(self, doc):
        assert doc.path.startswith("АР / ")
        assert doc.discipline_hint == "АР"

    def test_generated(self, doc):
        assert doc.generated == "2026-07-15 05:51:33 UTC"

    def test_doc_stamp_without_sheet(self, doc):
        assert doc.stamp["code"] == "ПД-00542664-АР1.2-1"
        assert doc.stamp["stage"] == "Р"
        # вложенное двоеточие в Object не ломает разбор
        assert doc.stamp["object"].endswith("г. Москва, кв. 3")
        assert "sheet" not in doc.stamp


class TestPagesAndBlocks:
    def test_pages(self, doc):
        assert doc.page_numbers == [1, 2]
        assert len(doc.page(2).blocks) == 2

    def test_blocks_order_and_ids(self, doc):
        assert [b.ordinal for b in doc.blocks] == [1, 2, 3]
        assert doc.blocks[0].block_id == "blk_8db07fd5e0a24e12b549745faa1ad4f0"
        assert doc.blocks[0].page == 1 and doc.blocks[2].page == 2

    def test_types(self, doc):
        assert [b.block_type for b in doc.blocks] == ["text", "text", "image"]

    def test_crop_url_and_token(self, doc):
        b = doc.blocks[0]
        assert b.crop_url == "https://vibe.cloud-ip.cc/api/crops/_lkFrM2goEJnxe6ScFhb"
        assert b.crop_token == "_lkFrM2goEJnxe6ScFhb"

    def test_created(self, doc):
        assert doc.blocks[1].created == "2026-07-07 15:19:40 UTC"

    def test_body_without_meta_quotes(self, doc):
        b = doc.blocks[0]
        assert b.body.startswith("ООО «ЮНИПРО»")
        assert "**Created:**" not in b.body
        # задвоенные маркеры — сырое содержимое, парсер не трогает
        assert "- - с заданием" in b.body

    def test_table_preserved(self, doc):
        assert "| Лист | Наименование |" in doc.blocks[1].body

    def test_body_lines_tracked(self, doc):
        assert doc.blocks[0].header_line > 0
        assert doc.blocks[0].body_start_line > doc.blocks[0].header_line


class TestBlockStamp:
    def test_empty_sheet_and_name(self, doc):
        b = doc.blocks[0]
        assert b.stamp["sheet"] == "" and b.sheet is None
        assert b.stamp["name"] == "" and b.sheet_name is None
        assert b.stamp["revisions"] == ""

    def test_filled_sheet(self, doc):
        b = doc.blocks[1]
        assert b.sheet == "2"
        assert b.sheet_name == "Кладочный план 1-го этажа"

    def test_sheet_map_keyed_by_pdf_page(self, doc):
        sm = doc.sheet_map()
        assert sm[1] == {"sheet": None, "name": None}
        assert sm[2] == {"sheet": "2", "name": "Кладочный план 1-го этажа"}


class TestImageBlocks:
    def test_image_meta_with_nested_colons(self, doc):
        b = doc.blocks[2]
        assert b.image_meta["type"] == "План"
        # «Axes: Оси: А, Б; 1, 2» — вложенное двоеточие не съедает Level
        assert b.image_meta["axes"] == "Оси: А, Б; 1, 2"
        assert b.image_meta["level"] == "Этаж 1"

    def test_image_sections(self, doc):
        s = doc.blocks[2].image_sections
        assert s["summary"] == "Кладочный план первого этажа."
        assert s["description"].startswith("1) Стены")
        assert s["entities"] == "стены, перемычки, проёмы"
        assert s["verification"].startswith("Сверить")

    def test_text_block_has_no_image_meta(self, doc):
        assert doc.blocks[0].image_meta == {}
        assert doc.blocks[0].image_sections == {}


class TestStampLineParsing:
    def test_split_by_known_keys_not_colon(self):
        st = parse_stamp_line(
            "Code: X | Stage: Р | Sheet: 7 | Object: адрес: г. Москва | "
            "Name: План | Organization: ООО | Revisions: "
        )
        assert st["object"] == "адрес: г. Москва"
        assert st["sheet"] == "7"
        assert st["revisions"] == ""

    def test_empty_line(self):
        assert parse_stamp_line("") == {}

    def test_partial_keys(self):
        st = parse_stamp_line("Code: X | Organization: Y")
        assert st == {"code": "X", "organization": "Y"}


class TestFullPages:
    def test_fills_gaps_from_pdf_page_count(self, doc):
        assert doc.full_page_numbers(5) == [1, 2, 3, 4, 5]

    def test_without_pdf_count_uses_max_seen(self, doc):
        assert doc.full_page_numbers() == [1, 2]


class TestTolerance:
    def test_unknown_block_type_kept(self):
        text = SAMPLE + (
            "\n### BLOCK #4 [STAMP]: blk_ffffffffffffffffffffffffffffffff\n\n"
            "> **Created:** 2026-07-07 15:22:50 UTC\n\nштамп\n"
        )
        d = parse_results_md(text)
        assert d.blocks[-1].block_type == "stamp"
        assert d.blocks[-1].body == "штамп"

    def test_block_without_meta_quotes(self):
        text = (
            "# Document: x.pdf\n\n## Page 1\n\n"
            "### BLOCK #1 [TEXT]: blk_00000000000000000000000000000000\n\n"
            "просто текст\n"
        )
        d = parse_results_md(text)
        assert d.blocks[0].crop_url is None
        assert d.blocks[0].body == "просто текст"

    def test_empty_input(self):
        d = parse_results_md("")
        assert d.blocks == [] and d.pages == []


# ─── Сверка на реальных выгрузках (если ZIP лежат на диске) ──────────────────

def _real_zip_pairs():
    """(results.md, blocks.json) из ZIP в experiments/новая структура./"""
    if not SAMPLES_DIR.is_dir():
        return []
    pairs = []
    for zpath in sorted(SAMPLES_DIR.glob("*.zip")):
        try:
            zf = zipfile.ZipFile(zpath)
        except zipfile.BadZipFile:
            continue
        names = zf.namelist()
        md = next((n for n in names if n.endswith("_results.md")), None)
        bj = next((n for n in names if n.endswith("_blocks.json")), None)
        if md and bj:
            pairs.append((zpath.name, zf.read(md).decode("utf-8"), json.loads(zf.read(bj))))
    return pairs


@pytest.mark.parametrize("zname,md_text,blocks_json",
                         _real_zip_pairs() or [pytest.param(None, None, None,
                                               marks=pytest.mark.skip(reason="реальные ZIP не найдены"))])
def test_real_samples_match_blocks_json(zname, md_text, blocks_json):
    """MD-парсер 1:1 с blocks.json той же генерации: id/ordinal/page/type."""
    doc = parse_results_md(md_text)
    assert is_results_md_text(md_text)
    ti = [b for b in blocks_json["blocks"] if b["block_type"] in ("text", "image")]
    assert len(doc.blocks) == len(ti)
    assert len(doc.pages) == len(blocks_json["pages"])
    bj_map = {b["block_id"]: b for b in ti}
    assert {b.block_id for b in doc.blocks} == set(bj_map)
    for b in doc.blocks:
        ref = bj_map[b.block_id]
        assert b.ordinal == ref["ordinal"], b.block_id
        assert b.page == ref["page_label"], b.block_id
        assert b.block_type == ref["block_type"], b.block_id
    # все блоки несут полный штамп и crop-токен
    assert all(set(b.stamp) == {"code", "stage", "sheet", "object",
                                "name", "organization", "revisions"}
               for b in doc.blocks)
    assert all(b.crop_token for b in doc.blocks)
