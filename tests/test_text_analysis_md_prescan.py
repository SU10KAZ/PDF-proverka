import json
from pathlib import Path

from backend.app.pipeline.stages.text_analysis.md_prescan import (
    augment_text_analysis_file,
    scan_md_text,
)


def test_scan_md_text_detects_high_risk_patterns():
    md = """
## СТРАНИЦА 3
### BLOCK [TEXT]: TEXT-P3
Рабочая документация выполнена по СП 60.1330.2020 и СП 18.1330.2022.
Расчетные параметры внутреннего воздуха:
- жилая комната closets: +24°C.
Магистраль принята по ГОСТ 2822-75.
После монтажа трубопроводы подвергаются гидравлическому испытанию давлением
не менее 20 кПа.ст. в самой нижней точке системы.
ГИЛЬЗЫ ДЛЯ ТРУБОПРОВОДОВ
\\phi 108 \\times 2,2 - Для труб из сшитого полиэтилена.
Тип системы (1 - Жилая часть, 2- ПОН, 3 - Теплоснабжение)
Т11.3 — подающий трубопровод помещений аренды.

## СТРАНИЦА 4
### BLOCK [TEXT]: TEXT-P4
| У1.1 | Вестибюль | КЭВ- 9ПЗ011Е | 0,2 |
| У2.1 | Управляющая компания | КЭВ- 12ПЗ011Е | 0,2 |
Установленная мощность электродвигателей, кВт

## СТРАНИЦА 26
### BLOCK [TEXT]: TEXT-P26
| | Кран шаровой резьбовой BVR-R DN15 | BVR-R DN15 | 065B8307FG | Ридан | шт. | 5 |
| | Кран шаровой с вн./ вн. резьбой | BVR-R DN15 | 065B8307R | Ридан | шт. | 9 |
| | Тепловая завеса КЭВ-9П3011Е | | | | шт. | 2 | Закупается арендатором |
"""

    keys = {item.key: item for item in scan_md_text(md)}

    assert "hydraulic_test_20_kpa" in keys
    assert "closets_temperature_label" in keys
    assert "gost_2822_75" in keys
    assert "bvr_r_dn15_duplicate_codes" in keys
    assert "tenant_supply_air_curtains_ambiguous" in keys
    assert "pexa_sleeve_108x22" in keys
    assert "system_type_legend_mismatch" in keys
    assert keys["bvr_r_dn15_duplicate_codes"].related_block_ids == ["TEXT-P26"]


def test_scan_md_text_new_results_format_matches_old_behavior():
    """Новый формат портала (*_results.md) даёт те же паттерны, что и старый.

    Контент страниц идентичен старому тесту; меняется только обёртка
    (`## Page N` + `### BLOCK #N [TEXT]: blk_…` вместо `## СТРАНИЦА N`).
    """
    page3 = """Рабочая документация выполнена по СП 60.1330.2020 и СП 18.1330.2022.
Расчетные параметры внутреннего воздуха:
- жилая комната closets: +24°C.
Магистраль принята по ГОСТ 2822-75.
После монтажа трубопроводы подвергаются гидравлическому испытанию давлением
не менее 20 кПа.ст. в самой нижней точке системы.
ГИЛЬЗЫ ДЛЯ ТРУБОПРОВОДОВ
\\phi 108 \\times 2,2 - Для труб из сшитого полиэтилена.
Тип системы (1 - Жилая часть, 2- ПОН, 3 - Теплоснабжение)
Т11.3 — подающий трубопровод помещений аренды."""
    page4 = """| У1.1 | Вестибюль | КЭВ- 9ПЗ011Е | 0,2 |
| У2.1 | Управляющая компания | КЭВ- 12ПЗ011Е | 0,2 |
Установленная мощность электродвигателей, кВт"""
    page26 = """| | Кран шаровой резьбовой BVR-R DN15 | BVR-R DN15 | 065B8307FG | Ридан | шт. | 5 |
| | Кран шаровой с вн./ вн. резьбой | BVR-R DN15 | 065B8307R | Ридан | шт. | 9 |
| | Тепловая завеса КЭВ-9П3011Е | | | | шт. | 2 | Закупается арендатором |"""

    old_md = (
        "\n## СТРАНИЦА 3\n### BLOCK [TEXT]: TEXT-P3\n" + page3
        + "\n\n## СТРАНИЦА 4\n### BLOCK [TEXT]: TEXT-P4\n" + page4
        + "\n\n## СТРАНИЦА 26\n### BLOCK [TEXT]: TEXT-P26\n" + page26 + "\n"
    )
    stamp = (
        "> **Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Sheet: {sheet} | Object: Тестовый объект"
        " | Name: Общие данные | Organization: Орг | Revisions: "
    )
    new_md = f"""# Document: ОВ-ТЕСТ_V1.pdf

Path: ОВ / ОВ-ТЕСТ / ОВ-ТЕСТ_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Object: Тестовый объект | Organization: Орг

---

## Page 3

### BLOCK #1 [TEXT]: blk_00000000000000000000000000000003

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p3)
{stamp.format(sheet=3)}

{page3}

## Page 4

### BLOCK #2 [TEXT]: blk_00000000000000000000000000000004

> **Created:** 2026-07-07 15:23:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p4)
{stamp.format(sheet=4)}

{page4}

## Page 26

### BLOCK #3 [TEXT]: blk_00000000000000000000000000000026

> **Created:** 2026-07-07 15:24:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p26)
{stamp.format(sheet=26)}

{page26}
"""

    old_keys = {item.key: item for item in scan_md_text(old_md)}
    new_keys = {item.key: item for item in scan_md_text(new_md)}

    # Тот же набор паттернов, что и в старом формате.
    assert set(new_keys) == set(old_keys)
    assert "hydraulic_test_20_kpa" in new_keys
    assert "bvr_r_dn15_duplicate_codes" in new_keys
    # Номера страниц нового формата = страницы PDF (1-based, из `## Page N`).
    assert "стр. 26" in new_keys["bvr_r_dn15_duplicate_codes"].source
    # related_block_ids указывают на блоки нового формата (blk_<hex>).
    assert new_keys["bvr_r_dn15_duplicate_codes"].related_block_ids == [
        "blk_00000000000000000000000000000026"
    ]


def test_augment_text_analysis_file_new_results_format_backfills_blk_ids(tmp_path: Path):
    md_path = tmp_path / "doc_results.md"
    md_path.write_text(
        """# Document: ОВ-ТЕСТ_V1.pdf

Path: ОВ / ОВ-ТЕСТ / ОВ-ТЕСТ_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Object: Тестовый объект | Organization: Орг

---

## Page 3

### BLOCK #1 [TEXT]: blk_000000000000000000000000000000a3

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://portal.example/api/crops/tok_p3)
> **Stamp:** Code: ОВ-ТЕСТ | Stage: Р | Sheet: 3 | Object: Тестовый объект | Name: Общие данные | Organization: Орг | Revisions:

После монтажа трубопроводы подвергаются гидравлическому испытанию давлением
равным 1,5 Ррб, но не менее 20 кПа.ст. в самой нижней точке системы.
""",
        encoding="utf-8",
    )
    output_path = tmp_path / "02_text_analysis.json"
    output_path.write_text(
        json.dumps(
            {
                "stage": "02_text_analysis",
                "project_id": "P",
                "text_source": "md",
                "timestamp": "2026-07-10T00:00:00+03:00",
                "project_params": {},
                "normative_refs_found": [],
                "text_findings": [
                    {
                        "id": "T-001",
                        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                        "category": "documentation",
                        "source": "MD стр. 3 / общие указания",
                        "finding": "Тестовое замечание без block id.",
                        "norm": "",
                        "norm_quote": None,
                        "related_block_ids": [],
                    }
                ],
                "items_verified_from_blocks": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = augment_text_analysis_file(output_path, md_path)
    data = json.loads(output_path.read_text(encoding="utf-8"))

    assert summary["added"] == 1
    assert summary["backfilled"] == 1
    assert data["text_findings"][0]["related_block_ids"] == [
        "blk_000000000000000000000000000000a3"
    ]
    assert data["text_findings"][1]["id"] == "T-002"
    assert "20 кПа" in data["text_findings"][1]["finding"]


def test_augment_text_analysis_file_adds_missing_and_backfills_existing(tmp_path: Path):
    md_path = tmp_path / "document.md"
    md_path.write_text(
        """
## СТРАНИЦА 3
### BLOCK [TEXT]: TEXT-P3
После монтажа трубопроводы подвергаются гидравлическому испытанию давлением
равным 1,5 Ррб, но не менее 20 кПа.ст. в самой нижней точке системы.
""",
        encoding="utf-8",
    )
    output_path = tmp_path / "02_text_analysis.json"
    output_path.write_text(
        json.dumps(
            {
                "stage": "02_text_analysis",
                "project_id": "P",
                "text_source": "md",
                "timestamp": "2026-07-10T00:00:00+03:00",
                "project_params": {},
                "normative_refs_found": [],
                "text_findings": [
                    {
                        "id": "T-001",
                        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                        "category": "documentation",
                        "source": "MD стр. 3 / общие указания",
                        "finding": "Тестовое замечание без block id.",
                        "norm": "",
                        "norm_quote": None,
                        "related_block_ids": [],
                    }
                ],
                "items_verified_from_blocks": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = augment_text_analysis_file(output_path, md_path)
    data = json.loads(output_path.read_text(encoding="utf-8"))

    assert summary["added"] == 1
    assert summary["backfilled"] == 1
    assert data["text_findings"][0]["related_block_ids"] == ["TEXT-P3"]
    assert data["text_findings"][1]["id"] == "T-002"
    assert "20 кПа" in data["text_findings"][1]["finding"]
    assert (tmp_path / "01_text_prescan.json").exists()

