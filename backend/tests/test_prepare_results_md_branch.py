"""Ветка нового MD-формата портала (*_results.md) в сборщиках задач Stage 01
и codex targeted-пассов.

Проверяем два инварианта:
1. Старый Chandra-формат («## СТРАНИЦА N» / «**Лист:**») обрабатывается
   прежним кодом — поведение и тексты промптов не меняются.
2. Новый формат ветвится в начале функции: страница = `## Page N` (номер
   страницы PDF), лист — подпись из sheet_map (может отсутствовать).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.pipeline.stages.prepare.task_builder import (
    _extract_page_context_for_blocks,
    _extract_page_to_sheet_map,
)
import backend.app.pipeline.stages.prepare.codex_targeted_findings as ctf


# ── Фикстуры форматов ────────────────────────────────────────────────────────

OLD_MD = """\
## СТРАНИЦА 1

**Лист:** 1
**Наименование листа:** Общие данные

### BLOCK [TEXT]: page_1_text
Общие указания. Кабель ВВГнг(А)-LS.

### BLOCK [IMAGE]: page_1_img_0
Описание плана первого этажа.

## СТРАНИЦА 2

**Лист:** 2

### BLOCK [TEXT]: page_2_text
Спецификация оборудования.

### BLOCK [IMAGE]: page_2_img_0
Описание однолинейной схемы ВРУ.
"""

NEW_MD = """\
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

Титульный лист. Рабочая документация.

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

## Page 3

### BLOCK #4 [IMAGE]: blk_ffffffffffffffffffffffffffffffff

> **Created:** 2026-07-07 15:23:00 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/zzz)
> **Stamp:** Code: ПД-00542664-АР1.2-1 | Stage: Р | Sheet: 3 | Object: «Застройка» | Name: Схемы перемычек | Organization: ЮНИПРО | Revisions:

**[IMAGE]** | Type: Схема

**Summary:** Схемы перемычек.
"""

IMG_P2 = "blk_a31258a5e5fa40e681c23ec1cf59a07d"
IMG_P3 = "blk_ffffffffffffffffffffffffffffffff"


@pytest.fixture()
def old_md_path(tmp_path) -> str:
    p = tmp_path / "old_document.md"
    p.write_text(OLD_MD, encoding="utf-8")
    return str(p)


@pytest.fixture()
def new_md_path(tmp_path) -> str:
    p = tmp_path / "ПД-00542664-АР1.2-1_V1_results.md"
    p.write_text(NEW_MD, encoding="utf-8")
    return str(p)


# ── _extract_page_to_sheet_map ───────────────────────────────────────────────

class TestSheetMap:
    def test_old_format_unchanged(self, old_md_path):
        assert _extract_page_to_sheet_map(old_md_path) == {1: "1", 2: "2"}

    def test_new_format_keyed_by_pdf_page(self, new_md_path):
        # Страница 1: Sheet пуст → в маппинг не попадает (как пустой «**Лист:**» в старом пути)
        assert _extract_page_to_sheet_map(new_md_path) == {2: "2", 3: "3"}

    def test_new_format_detected_by_text_without_suffix(self, tmp_path):
        # Имя без *_results.md, но содержимое нового формата → детект по тексту
        p = tmp_path / "arbitrary_name.md"
        p.write_text(NEW_MD, encoding="utf-8")
        assert _extract_page_to_sheet_map(str(p)) == {2: "2", 3: "3"}

    def test_missing_file(self):
        assert _extract_page_to_sheet_map("(нет)") == {}


# ── _extract_page_context_for_blocks ─────────────────────────────────────────

class TestPageContextOldFormat:
    def test_old_format_unchanged(self, old_md_path):
        ctx = _extract_page_context_for_blocks(old_md_path, ["page_1_img_0"], [1])
        assert "## СТРАНИЦА 1" in ctx
        assert "**Лист:** 1" in ctx
        assert "**Наименование листа:** Общие данные" in ctx
        assert "### Текст на странице:" in ctx
        assert "Кабель ВВГнг(А)-LS" in ctx
        assert "### OCR-описания блоков:" in ctx
        assert "Описание плана первого этажа" in ctx
        # Нерелевантная страница не попадает
        assert "## СТРАНИЦА 2" not in ctx
        assert "Описание однолинейной схемы ВРУ" not in ctx

    def test_old_format_lazy_mode(self, old_md_path):
        ctx = _extract_page_context_for_blocks(old_md_path, ["page_2_img_0"], [])
        assert "## СТРАНИЦА 2" in ctx
        assert "## СТРАНИЦА 1" not in ctx


class TestPageContextNewFormat:
    def test_filter_by_pages(self, new_md_path):
        ctx = _extract_page_context_for_blocks(new_md_path, [IMG_P2], [2])
        assert "## СТРАНИЦА 2" in ctx
        assert "**Лист:** 2" in ctx
        assert "**Наименование листа:** Кладочный план 1-го этажа" in ctx
        # TEXT-блок страницы включён вместе с заголовком (block_id виден)
        assert "### Текст на странице:" in ctx
        assert f"### BLOCK #2 [TEXT]: blk_d130bc9d42364116af2e047aadf48a48" in ctx
        assert "| 1 | Общие данные |" in ctx
        # IMAGE-описание — только для блока пакета
        assert "### OCR-описания блоков:" in ctx
        assert f"### BLOCK #3 [IMAGE]: {IMG_P2}" in ctx
        assert "Кладочный план первого этажа" in ctx
        # Другие страницы отфильтрованы
        assert "## СТРАНИЦА 1" not in ctx
        assert "## СТРАНИЦА 3" not in ctx
        assert "Схемы перемычек." not in ctx
        # Мета-цитаты в контекст не утекают
        assert "> **Created:**" not in ctx
        assert "> **Stamp:**" not in ctx

    def test_lazy_mode_by_image_block(self, new_md_path):
        # Без списка страниц: релевантны только страницы с IMAGE-блоками пакета
        ctx = _extract_page_context_for_blocks(new_md_path, [IMG_P3], [])
        assert "## СТРАНИЦА 3" in ctx
        assert "**Лист:** 3" in ctx
        assert "Схемы перемычек." in ctx
        assert "## СТРАНИЦА 1" not in ctx
        assert "## СТРАНИЦА 2" not in ctx

    def test_foreign_image_excluded_on_target_page(self, new_md_path):
        # Целевая страница 2, но блок пакета — с другой страницы:
        # текст и мета страницы 2 включаются, чужой IMAGE — нет
        ctx = _extract_page_context_for_blocks(new_md_path, [IMG_P3], [2])
        assert "## СТРАНИЦА 2" in ctx
        assert "### Текст на странице:" in ctx
        assert "### OCR-описания блоков:" not in ctx
        assert "Кладочный план первого этажа" not in ctx

    def test_no_match_returns_empty(self, new_md_path):
        assert _extract_page_context_for_blocks(new_md_path, ["blk_" + "0" * 32], []) == ""

    def test_empty_inputs_return_empty(self, new_md_path):
        assert _extract_page_context_for_blocks(new_md_path, [], []) == ""


# ── codex targeted: подсказка заголовка страницы в JSON-инструкции ──────────

class TestCodexTargetedPageHeaderHint:
    def _user_content(self, messages: list[dict[str, str]]) -> str:
        return messages[-1]["content"]

    def test_old_format_keeps_stranitsa_hint(self, old_md_path, tmp_path, monkeypatch):
        monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(tmp_path))
        msgs = ctf._ss_messages({"section": "SS"}, "p1", Path(old_md_path))
        user = self._user_content(msgs)
        assert "по ближайшему заголовку «## СТРАНИЦА N» выше цитируемых строк" in user
        assert "«## Page N»" not in user

    def test_new_format_uses_page_hint(self, new_md_path, tmp_path, monkeypatch):
        monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(tmp_path))
        msgs = ctf._ss_messages({"section": "SS"}, "p1", Path(new_md_path), results_md=True)
        user = self._user_content(msgs)
        assert "по ближайшему заголовку «## Page N» выше цитируемых строк" in user
        assert "«## СТРАНИЦА N»" not in user

    def test_md_is_results_format_detection(self, old_md_path, new_md_path, tmp_path):
        assert ctf._md_is_results_format(Path(new_md_path)) is True
        assert ctf._md_is_results_format(Path(old_md_path)) is False
        # детект по тексту, если имя не *_results.md
        p = tmp_path / "renamed.md"
        p.write_text(NEW_MD, encoding="utf-8")
        assert ctf._md_is_results_format(p) is True
        assert ctf._md_is_results_format(tmp_path / "missing.md") is False

    def test_build_passes_new_format_threads_flag(self, new_md_path, tmp_path, monkeypatch):
        monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr(ctf, "_get_md_file_path", lambda info, pid: new_md_path)
        passes = ctf.build_targeted_findings_passes({"section": "SS"}, "p1")
        assert [p.stage for p in passes] == ["alia_ss_lowcurrent_audit", "alia_docnorm_audit"]
        for p in passes:
            assert "«## Page N»" in self._user_content(p.messages)
            assert "«## СТРАНИЦА N»" not in self._user_content(p.messages)

    def test_build_passes_old_format_unchanged(self, old_md_path, tmp_path, monkeypatch):
        monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(tmp_path))
        monkeypatch.setattr(ctf, "_get_md_file_path", lambda info, pid: old_md_path)
        passes = ctf.build_targeted_findings_passes({"section": "AR"}, "p1")
        assert [p.stage for p in passes] == ["alia_ar_masonry_audit", "alia_docnorm_audit"]
        for p in passes:
            assert "«## СТРАНИЦА N»" in self._user_content(p.messages)
            assert "«## Page N»" not in self._user_content(p.messages)
