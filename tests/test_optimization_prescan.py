import json

from backend.app.pipeline.stages.optimization.prescan import (
    build_optimization_prescan_section_from_text,
    scan_optimization_opportunities,
)


def test_prescan_finds_large_mounting_position_and_repeated_family():
    md_text = """
## СТРАНИЦА 23
**Лист:** 1

| Поз. | Наименование и техническая характеристика | Тип, марка | Код продукции | Поставщик | Ед. измерения | Кол. | Масса |
|---|---|---|---|---|---|---|---|
| | Комплект настенного кронштейна | K17.33F | | SPL | компл. | 415 | |
| | Радиатор стальной панельный Тип 22 300/900 | OVL-22-3-09 | | SPL | шт. | 13 | |
| | Радиатор стальной панельный Тип 22 300/1000 | OVL-22-3-10 | | SPL | шт. | 17 | |
| | Радиатор стальной панельный Тип 22 300/1400 | OVL-22-3-14 | | SPL | шт. | 57 | |
| | Радиатор стальной панельный Тип 22 300/1500 | OVL-22-3-15 | | SPL | шт. | 34 | |
| | Радиатор стальной панельный Тип 22 300/1600 | OVL-22-3-16 | | SPL | шт. | 13 | |
"""

    opportunities = scan_optimization_opportunities(md_text, section="OV")

    assert any(item.type == "faster_install" and "кронштейна" in item.title for item in opportunities)
    assert any(
        item.type == "simpler_design"
        and item.lens == "repeatable_spec_family"
        and "радиатор" in item.title.lower()
        for item in opportunities
    )


def test_prescan_new_results_format_matches_old_and_maps_pages_sheets():
    """Новый формат портала (*_results.md): те же кандидаты, что и старый.

    Ключ листа = страница PDF (`## Page 23`); sheet — подпись из штампа блока.
    """
    table = """| Поз. | Наименование и техническая характеристика | Тип, марка | Код продукции | Поставщик | Ед. измерения | Кол. | Масса |
|---|---|---|---|---|---|---|---|
| | Комплект настенного кронштейна | K17.33F | | SPL | компл. | 415 | |
| | Радиатор стальной панельный Тип 22 300/900 | OVL-22-3-09 | | SPL | шт. | 13 | |
| | Радиатор стальной панельный Тип 22 300/1000 | OVL-22-3-10 | | SPL | шт. | 17 | |
| | Радиатор стальной панельный Тип 22 300/1400 | OVL-22-3-14 | | SPL | шт. | 57 | |
| | Радиатор стальной панельный Тип 22 300/1500 | OVL-22-3-15 | | SPL | шт. | 34 | |
| | Радиатор стальной панельный Тип 22 300/1600 | OVL-22-3-16 | | SPL | шт. | 13 | |"""

    old_md = f"\n## СТРАНИЦА 23\n**Лист:** 1\n\n{table}\n"
    new_md = f"""# Document: ОВ-ТЕСТ_V1.pdf

Path: ОВ / ОВ-ТЕСТ / ОВ-ТЕСТ_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Object: Тестовый объект | Organization: Орг

---

## Page 23

### BLOCK #1 [TEXT]: blk_00000000000000000000000000000023

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p23)
> **Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Sheet: 1 | Object: Тестовый объект | Name: Спецификация | Organization: Орг | Revisions:

{table}

### BLOCK #2 [IMAGE]: blk_000000000000000000000000000000f1

> **Created:** 2026-07-07 15:23:00 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_img)
> **Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Sheet: 1 | Object: Тестовый объект | Name: Спецификация | Organization: Орг | Revisions:

**[IMAGE]** | Type: План | Axes: Оси: 1-5 | Zone: — | Level: Этаж 1

**Summary:** План размещения радиаторов.
**Description:** Схематичный план без спецификационных таблиц.
"""

    old_ops = scan_optimization_opportunities(old_md, section="OV")
    new_ops = scan_optimization_opportunities(new_md, section="OV")

    # Набор кандидатов (линза + заголовок) идентичен старому формату.
    assert {(item.lens, item.title) for item in new_ops} == {
        (item.lens, item.title) for item in old_ops
    }

    bracket = next(item for item in new_ops if "кронштейна" in item.title)
    # Страница = физическая страница PDF из `## Page 23`.
    assert bracket.related_pages == [23]
    # Лист — подпись из штампа блока (Sheet: 1).
    assert bracket.related_sheets == ["1"]

    assert any(
        item.lens == "repeatable_spec_family" and "радиатор" in item.title.lower()
        for item in new_ops
    )


def test_prescan_new_results_format_empty_sheet_is_tolerated():
    """Пустой Sheet в штампе (титулы и т.п.) не ломает прескан: sheet = ''."""
    md_text = """# Document: ОВ-ТЕСТ_V1.pdf

## Page 5

### BLOCK #1 [TEXT]: blk_00000000000000000000000000000005

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p5)
> **Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Sheet:  | Object: Тестовый объект | Name:  | Organization: Орг | Revisions:

| Поз. | Наименование и техническая характеристика | Тип, марка | Код продукции | Поставщик | Ед. измерения | Кол. |
|---|---|---|---|---|---|---|
| | Комплект настенного кронштейна | K17.33F | | SPL | компл. | 415 |
"""

    opportunities = scan_optimization_opportunities(md_text, section="OV")

    bracket = next(item for item in opportunities if "кронштейна" in item.title)
    assert bracket.related_pages == [5]
    assert bracket.related_sheets == []


def test_prescan_section_includes_blockers_from_findings():
    md_text = """
## СТРАНИЦА 10
**Лист:** 7

| Поз. | Наименование и техническая характеристика | Тип, марка | Код продукции | Поставщик | Ед. измерения | Кол. |
|---|---|---|---|---|---|---|
| 12 | Кабель силовой ППГнг(А)-FRHF 5x10 | ППГнг-FRHF | | Кабэкс | м | 420 |
"""
    findings = {
        "findings": [
            {
                "id": "F-001",
                "severity": "КРИТИЧЕСКОЕ",
                "page": 10,
                "problem": "Для кабельной линии не подтверждён предел огнестойкости и способ прокладки.",
            }
        ]
    }

    section = build_optimization_prescan_section_from_text(
        md_text,
        section="EOM",
        vendor_list_text="| Кабельная продукция | **Кабэкс** | Электрокабель |",
        findings_data=json.dumps(findings, ensure_ascii=False),
    )

    assert "Автопрескан оптимизаций из MD" in section
    assert "F-001:КРИТИЧЕСКОЕ" in section
    assert "не предлагай дешёвый аналог" in section


def test_prescan_skips_room_area_but_keeps_finish_area():
    md_text = """
## СТРАНИЦА 5
**Лист:** 3

| № пом. | Наименование | Площадь, м2 | Кат. пом. |
|---|---|---|---|
| 1 | Вестибюль | 125.4 | |
| 2 | Насосная | 84.0 | |

| Тип | Описание отделки стен | Площадь, м2 | Примечание |
|---|---|---|---|
| Ш-2 | Цементная штукатурка по сетке армирующей с грунтовкой | 134.86 | |
"""

    opportunities = scan_optimization_opportunities(md_text, section="AR")
    titles = "\n".join(item.title for item in opportunities)

    assert "Вестибюль" not in titles
    assert "Насосная" not in titles
    assert "штукатурка" in titles.lower()
