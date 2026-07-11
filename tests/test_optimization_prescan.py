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
