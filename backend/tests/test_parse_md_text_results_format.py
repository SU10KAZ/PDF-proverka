"""Тесты ветки нового формата (*_results.md) в project_service.parse_md_text.

Прод-путь просмотра документа (GET /api/document/{id}/pages|/page/{n} через
read_canary._v2_parse_pages): на новом формате parse_md_text раньше возвращал
None (нет маркера `## СТРАНИЦА N`) → фронт получал 404. Проверяем:

- новый формат разбирается: страницы = `## Page N` (ключ листа = страница PDF),
  Sheet/Name из штампа — подписи sheet_info/sheet_label;
- контракт dict байт-в-байт как у старого пути (ключи страниц/блоков,
  type в верхнем регистре, image_type/axes/brief/description/entities);
- старый Chandra-формат идёт прежним кодом без изменений;
- сверка на реальной выгрузке из experiments/новая структура./ (skip, если нет).
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from backend.app.services.common.project_service import parse_md_text
from backend.app.services.common.results_md import parse_results_md

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_ZIP = REPO_ROOT / "experiments" / "новая структура." / "ПД-00542664-АР1.2-1_V1 (4).zip"

NEW_FORMAT_SAMPLE = """\
# Document: ПД-00542664-АР1.2-1_V1.pdf

Path: АР / ПД-00542664-АР1.2-1 / ПД-00542664-АР1.2-1_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Object: «Застройка», по адресу: г. Москва | Organization: ЮНИПРО

---

## Page 1

### BLOCK #1 [TEXT]: blk_8db07fd5e0a24e12b549745faa1ad4f0

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/_lkFrM2goEJnxe6ScFhb)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet:  | Object: «Застройка» | Name:  | Organization: ЮНИПРО | Revisions:

ООО «ЮНИПРО»

Рабочая документация

## Page 2

### BLOCK #2 [TEXT]: blk_d130bc9d42364116af2e047aadf48a48

> **Created:** 2026-07-07 15:19:40 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/bnWfCZBJEJxhE0CIFgEA)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet: 2 | Object: «Застройка» | Name: Кладочный план 1-го этажа | Organization: ЮНИПРО | Revisions:

1. Кладку вести по СП 70.13330.2012

### BLOCK #3 [IMAGE]: blk_aa07fd5e0a24e12b549745faa1ad4f00

> **Created:** 2026-07-07 15:19:41 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/cnWfCZBJEJxhE0CIFgEB)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet: 2 | Object: «Застройка» | Name: Кладочный план 1-го этажа | Organization: ЮНИПРО | Revisions:

**[IMAGE]** | Type: План | Axes: Оси: 1-8, А-Д | Zone: Секция 1 | Level: Этаж 1

**Summary:** Кладочный план 1-го этажа секции 1.

**Description:** План с размерами кладки, проёмами и марками перемычек.

**Entities:** стены, перемычки ПР-1, проёмы

**Verification:** Штамп читается.
"""

OLD_FORMAT_SAMPLE = """\
## СТРАНИЦА 1

**Лист:** 1
**Наименование листа:** Общие данные

### BLOCK [TEXT]: p1_b1

Общие указания по проекту.

### BLOCK [IMAGE]: p1_b2

**[ИЗОБРАЖЕНИЕ]** | Тип: План | Оси: 1-5
**Краткое описание:** План этажа.
**Описание:** Подробный план с размерами.
**Текст на чертеже:** Пм-1, Пм-2
**Сущности:** стены, окна

## СТРАНИЦА 3

Текст без блоков.
"""


# ── Новый формат ─────────────────────────────────────────────────────────────

class TestParseMdTextResultsFormat:
    def test_contract_shape(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="АР/тест", md_file="x_results.md")
        assert parsed is not None
        assert set(parsed.keys()) == {"project_id", "md_file", "total_pages", "pages"}
        assert parsed["project_id"] == "АР/тест"
        assert parsed["md_file"] == "x_results.md"
        assert parsed["total_pages"] == 2
        for page in parsed["pages"]:
            assert set(page.keys()) == {
                "page_num", "sheet_info", "sheet_label",
                "text_blocks", "image_blocks", "blocks",
            }

    def test_pages_keyed_by_pdf_page(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="p", md_file="f")
        assert [p["page_num"] for p in parsed["pages"]] == [1, 2]

    def test_sheet_from_stamp_is_label_only(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="p", md_file="f")
        p1, p2 = parsed["pages"]
        # пустой Sheet штампа (титул) → None, как у старого пути без **Лист:**
        assert p1["sheet_info"] is None
        assert p1["sheet_label"] is None
        assert p2["sheet_info"] == "2"
        assert p2["sheet_label"] == "Кладочный план 1-го этажа"

    def test_block_counters(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="p", md_file="f")
        p1, p2 = parsed["pages"]
        assert (p1["text_blocks"], p1["image_blocks"]) == (1, 0)
        assert (p2["text_blocks"], p2["image_blocks"]) == (1, 1)

    def test_text_block_contract(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="p", md_file="f")
        block = parsed["pages"][0]["blocks"][0]
        assert block["block_id"] == "blk_8db07fd5e0a24e12b549745faa1ad4f0"
        assert block["type"] == "TEXT"  # верхний регистр, как в старом контракте
        assert "ООО «ЮНИПРО»" in block["content"]
        # мета-цитаты (> **Created:** …) в content не попадают
        assert "**Created:**" not in block["content"]

    def test_image_block_fields_mapping(self):
        parsed = parse_md_text(NEW_FORMAT_SAMPLE, project_id="p", md_file="f")
        image = parsed["pages"][1]["blocks"][1]
        assert image["type"] == "IMAGE"
        assert image["image_type"] == "План"
        assert image["axes"] == "Оси: 1-8, А-Д"
        assert image["brief"] == "Кладочный план 1-го этажа секции 1."
        assert image["description"] == "План с размерами кладки, проёмами и марками перемычек."
        assert image["entities"] == "стены, перемычки ПР-1, проёмы"
        # эквивалента «Текст на чертеже» в новом формате нет — ключ отсутствует
        assert "text_on_drawing" not in image
        # raw content сохраняется, как и в старом пути
        assert "**Summary:**" in image["content"]

    def test_empty_and_garbage_return_none(self):
        assert parse_md_text("", project_id="p", md_file="f") is None
        assert parse_md_text("просто текст без маркеров", project_id="p", md_file="f") is None


# ── Старый формат: поведение прежнее ─────────────────────────────────────────

class TestParseMdTextOldFormatUnchanged:
    def test_old_format_full_contract(self):
        parsed = parse_md_text(OLD_FORMAT_SAMPLE, project_id="p", md_file="doc.md")
        assert parsed == {
            "project_id": "p",
            "md_file": "doc.md",
            "total_pages": 2,
            "pages": [
                {
                    "page_num": 1,
                    "sheet_info": "1",
                    "sheet_label": "Общие данные",
                    "text_blocks": 1,
                    "image_blocks": 1,
                    "blocks": [
                        {
                            "block_id": "p1_b1",
                            "type": "TEXT",
                            "content": "Общие указания по проекту.",
                        },
                        {
                            "block_id": "p1_b2",
                            "type": "IMAGE",
                            "image_type": "План",
                            "axes": "1-5",
                            "brief": "План этажа.",
                            "description": "Подробный план с размерами.",
                            "text_on_drawing": "Пм-1, Пм-2",
                            "entities": "стены, окна",
                            "content": (
                                "**[ИЗОБРАЖЕНИЕ]** | Тип: План | Оси: 1-5\n"
                                "**Краткое описание:** План этажа.\n"
                                "**Описание:** Подробный план с размерами.\n"
                                "**Текст на чертеже:** Пм-1, Пм-2\n"
                                "**Сущности:** стены, окна"
                            ),
                        },
                    ],
                },
                {
                    "page_num": 3,
                    "sheet_info": None,
                    "sheet_label": None,
                    "text_blocks": 0,
                    "image_blocks": 0,
                    "blocks": [],
                },
            ],
        }

    def test_old_format_not_detected_as_new(self):
        # `## СТРАНИЦА N` не должен уходить в ветку results_md
        parsed = parse_md_text(OLD_FORMAT_SAMPLE, project_id="p", md_file="f")
        assert parsed["pages"][0]["page_num"] == 1
        assert parsed["pages"][0]["blocks"][0]["block_id"] == "p1_b1"


# ── Реальная выгрузка (skip, если архива нет) ────────────────────────────────

@pytest.mark.skipif(not SAMPLE_ZIP.exists(), reason="реальная выгрузка недоступна")
class TestParseMdTextRealSample:
    def _load_md(self) -> str:
        with zipfile.ZipFile(SAMPLE_ZIP) as z:
            name = next(n for n in z.namelist() if n.endswith("_results.md"))
            return z.read(name).decode("utf-8")

    def test_real_sample_parses_consistently(self):
        md = self._load_md()
        parsed = parse_md_text(md, project_id="АР/реальный", md_file="r_results.md")
        assert parsed is not None
        doc = parse_results_md(md)
        assert parsed["total_pages"] == len(doc.pages)
        assert [p["page_num"] for p in parsed["pages"]] == [p.number for p in doc.pages]
        # суммарное число блоков совпадает со сквозным списком парсера
        total_blocks = sum(len(p["blocks"]) for p in parsed["pages"])
        assert total_blocks == len(doc.blocks)
        # sheet_info/sheet_label согласованы с sheet_map (ключ = страница PDF)
        smap = doc.sheet_map()
        for page in parsed["pages"]:
            assert page["sheet_info"] == smap[page["page_num"]]["sheet"]
            assert page["sheet_label"] == smap[page["page_num"]]["name"]

    def test_real_sample_read_canary_light_shape(self):
        """Форма, которую строит read_canary.v2_document_pages (pages_light)."""
        md = self._load_md()
        parsed = parse_md_text(md, project_id="АР/реальный", md_file="r_results.md")
        for p in parsed["pages"]:
            light = {
                "page_num": p["page_num"], "sheet_info": p["sheet_info"],
                "sheet_label": p["sheet_label"], "text_blocks": p["text_blocks"],
                "image_blocks": p["image_blocks"],
            }
            assert isinstance(light["page_num"], int)
