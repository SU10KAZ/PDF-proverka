"""Ветка нового формата (*_results.md) в block_markdown.

Проверяет, что центральный разборщик блоков понимает новый формат портала
vibe (## Page N + ### BLOCK #N [TYPE]: blk_<hex>) через results_md,
а поведение на старом Chandra-формате не изменилось.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from backend.app.pipeline.stages.crop_blocks.block_markdown import (
    extract_block_sections,
    extract_chandra_block_description,
    iter_block_headers,
    parse_block_header,
    strip_gemma_enrichment_sections,
)


BLK_TEXT_1 = "blk_8db07fd5e0a24e12b549745faa1ad4f0"
BLK_IMAGE_2 = "blk_8423257fa0bb471b93c66815351510be"
BLK_TEXT_3 = "blk_d130bc9d42364116af2e047aadf48a48"

RESULTS_SAMPLE = f"""\
# Document: ПД-TEST-АР_V1.pdf

Path: АР / ПД-TEST-АР / ПД-TEST-АР_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ПД-TEST-АР | Stage: Р | Object: Жилой дом по адресу: г. Москва | Organization: ОРГ

---

## Page 1

### BLOCK #1 [TEXT]: {BLK_TEXT_1}

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/_lkFrM2goEJnxe6ScFhb)
> **Stamp:** Code: ПД-TEST-АР | Stage: Р | Sheet:  | Object: Жилой дом | Name:  | Organization: ОРГ | Revisions:

Титульный лист. Рабочая документация.

## Page 2

### BLOCK #2 [IMAGE]: {BLK_IMAGE_2}

> **Created:** 2026-07-07 15:26:09 UTC
> **Crop:** [Crop](https://portal.example/api/crops/si1PAZWzO_wlY3-rOEMt)
> **Stamp:** Code: ПД-TEST-АР | Stage: Р | Sheet: 2 | Object: Жилой дом | Name: Кладочный план 1-го этажа | Organization: ОРГ | Revisions:

**[IMAGE]** | Type: Схема | Axes: А, Б | Level: 1 этаж

**Summary:** Узел примыкания плавающего пола к стене.

**Description:** Две схемы устройства гидроизоляции пола и стены.

**Entities:** ЯД-ИЗ-СУЩНОСТЕЙ, Сп-1

**Verification:** ЯД-ИЗ-ВЕРИФИКАЦИИ

### BLOCK #3 [TEXT]: {BLK_TEXT_3}

> **Created:** 2026-07-07 15:19:40 UTC
> **Crop:** [Crop](https://portal.example/api/crops/bnWfCZBJEJxhE0CIFgEA)
> **Stamp:** Code: ПД-TEST-АР | Stage: Р | Sheet: 2 | Object: Жилой дом | Name: Кладочный план 1-го этажа | Organization: ОРГ | Revisions:

Примечания к плану.
"""

# Старый Chandra-формат (как в test_block_markdown_chandra.py) — регресс-контроль.
CHANDRA_SAMPLE = """\
## СТРАНИЦА 1

### BLOCK [IMAGE]: DETAIL-1
**[ИЗОБРАЖЕНИЕ]** | Тип: Схема

**Краткое описание:** Узел примыкания плавающего пола к стене.

**Описание:** Две схемы устройства гидроизоляции пола и стены.

### BLOCK [TEXT]: NOTES-2
Примечания.
"""


# ── parse_block_header ───────────────────────────────────────────────────────

def test_parse_block_header_new_format():
    header = parse_block_header(f"### BLOCK #2 [IMAGE]: {BLK_IMAGE_2}\n")

    assert header is not None
    assert header.type == "IMAGE"
    assert header.id == BLK_IMAGE_2
    assert header.ordinal == 2


def test_parse_block_header_old_format_unchanged():
    header = parse_block_header("### BLOCK [TEXT]: NOTES-2")

    assert header is not None
    assert header.type == "TEXT"
    assert header.id == "NOTES-2"
    assert header.ordinal is None


def test_parse_block_header_rejects_garbage():
    assert parse_block_header("### BLOCK #x [TEXT]: blk_deadbeef") is None
    assert parse_block_header("## Page 3") is None


# ── iter_block_headers / extract_block_sections ─────────────────────────────

def test_iter_block_headers_new_format():
    headers = list(iter_block_headers(RESULTS_SAMPLE))

    assert [h.ordinal for h in headers] == [1, 2, 3]
    assert [h.type for h in headers] == ["TEXT", "IMAGE", "TEXT"]
    assert [h.id for h in headers] == [BLK_TEXT_1, BLK_IMAGE_2, BLK_TEXT_3]


def test_extract_block_sections_new_format_fields():
    sections = extract_block_sections(RESULTS_SAMPLE)

    assert [s.id for s in sections] == [BLK_TEXT_1, BLK_IMAGE_2, BLK_TEXT_3]
    assert [s.ordinal for s in sections] == [1, 2, 3]
    assert [s.page for s in sections] == [1, 2, 2]
    assert [s.sheet for s in sections] == [None, "2", "2"]
    assert sections[0].body_clean == "Титульный лист. Рабочая документация."
    assert sections[2].body_clean == "Примечания к плану."
    # body_clean БЕЗ мета-цитат, сырой body — С ними
    assert "> **Created:**" not in sections[0].body_clean
    assert "> **Created:**" in sections[0].body


def test_extract_block_sections_new_format_offsets_roundtrip():
    sections = extract_block_sections(RESULTS_SAMPLE)

    for s in sections:
        assert s.text == RESULTS_SAMPLE[s.header_start:s.body_end]
        assert s.body == RESULTS_SAMPLE[s.header_end:s.body_end]
        assert s.body_start == s.header_end
    # границы: блок #1 обрывается на "## Page 2", блок #2 — на заголовке #3
    assert "## Page 2" not in sections[0].text
    assert BLK_TEXT_3 not in sections[1].text
    assert "Узел примыкания" in sections[1].body


def test_extract_block_sections_old_format_unchanged():
    sections = extract_block_sections(CHANDRA_SAMPLE)

    assert [s.id for s in sections] == ["DETAIL-1", "NOTES-2"]
    assert [s.type for s in sections] == ["IMAGE", "TEXT"]
    # поля нового формата на старом пути не заполняются
    assert all(s.ordinal is None for s in sections)
    assert all(s.page is None for s in sections)
    assert all(s.sheet is None for s in sections)
    assert all(s.body_clean is None for s in sections)
    for s in sections:
        assert s.text == CHANDRA_SAMPLE[s.header_start:s.body_end]


# ── strip_gemma_enrichment_sections ──────────────────────────────────────────

def test_strip_enrichment_noop_on_new_format_without_tail():
    assert strip_gemma_enrichment_sections(RESULTS_SAMPLE) == RESULTS_SAMPLE


def test_strip_enrichment_removes_tail_in_new_format():
    enriched = RESULTS_SAMPLE + (
        "\n**[ENRICHED google/gemma-test @ 2026-07-01T00:00:00]**\n"
        "- **Тип блока:** ЯД-ИЗ-ОБОГАЩЕНИЯ\n"
    )

    stripped = strip_gemma_enrichment_sections(enriched)

    assert "ENRICHED" not in stripped
    assert "ЯД-ИЗ-ОБОГАЩЕНИЯ" not in stripped
    assert "Примечания к плану." in stripped
    assert "Узел примыкания" in stripped


# ── extract_chandra_block_description (маппинг нового формата) ───────────────

def test_block_description_maps_new_format_fields():
    description = extract_chandra_block_description(RESULTS_SAMPLE, BLK_IMAGE_2)

    assert description is not None
    assert description.block_id == BLK_IMAGE_2
    assert description.block_type == "Схема"
    assert description.short_description == "Узел примыкания плавающего пола к стене."
    assert description.description == "Две схемы устройства гидроизоляции пола и стены."
    # Entities/Verification игнорируются (аналог «Текст на чертеже»/«Сущности»)
    assert "ЯД-" not in description.classification_text


def test_block_description_new_format_ignores_text_blocks_and_unknown_ids():
    assert extract_chandra_block_description(RESULTS_SAMPLE, BLK_TEXT_1) is None
    assert extract_chandra_block_description(RESULTS_SAMPLE, "blk_0000000000000000") is None


def test_block_description_new_format_cuts_enriched_tail():
    # ENRICHED-хвост прилипает к последней секции — в контракт попадать не должен
    block_tail = RESULTS_SAMPLE.replace(
        "**Description:** Две схемы устройства гидроизоляции пола и стены.",
        "**Description:** Две схемы устройства гидроизоляции пола и стены.\n\n"
        "**[ENRICHED google/gemma-test @ 2026-07-01T00:00:00]**\n"
        "- **Тип блока:** ЯД-ИЗ-ОБОГАЩЕНИЯ",
    )

    description = extract_chandra_block_description(block_tail, BLK_IMAGE_2)

    assert description is not None
    assert description.description == "Две схемы устройства гидроизоляции пола и стены."
    assert "ЯД-" not in description.classification_text


def test_block_description_old_format_unchanged():
    description = extract_chandra_block_description(CHANDRA_SAMPLE, "DETAIL-1")

    assert description is not None
    assert description.block_type == "Схема"
    assert description.short_description == "Узел примыкания плавающего пола к стене."


# ── Смоук на реальной выгрузке портала (если архив на месте) ────────────────

_REAL_ZIP = Path(__file__).resolve().parent.parent / (
    "experiments/новая структура./ПД-00542664-АР1.2-1_V1 (4).zip"
)


@pytest.mark.skipif(not _REAL_ZIP.exists(), reason="реальная выгрузка недоступна")
def test_real_results_md_smoke():
    with zipfile.ZipFile(_REAL_ZIP) as zf:
        text = zf.read("ПД-00542664-АР1.2-1_V1_results.md").decode("utf-8")

    sections = extract_block_sections(text)

    assert sections, "в реальной выгрузке должны находиться блоки"
    assert all(s.id.startswith("blk_") for s in sections)
    assert [s.ordinal for s in sections] == sorted(s.ordinal for s in sections)
    assert all((s.page or 0) >= 1 for s in sections)
    for s in sections[:20]:
        assert s.text == text[s.header_start:s.body_end]
    # strip на неэнриченном тексте — no-op
    assert strip_gemma_enrichment_sections(text) == text
    # хотя бы один IMAGE-блок мапится в контракт описания
    image_ids = [s.id for s in sections if s.type == "IMAGE"]
    assert image_ids
    described = [extract_chandra_block_description(text, bid) for bid in image_ids[:5]]
    assert any(d is not None and d.classification_text for d in described)
