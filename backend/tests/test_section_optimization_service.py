from types import SimpleNamespace

from backend.app.services.section_optimization_service import (
    build_replication_signals,
    build_section_optimization,
    cluster_accepted_optimizations,
    group_shared_specification_items,
    parse_specification_markdown,
)


SPEC_MD = """
## СТРАНИЦА 1
**Лист:** 1

| Изм. | Дата |
|---|---|
| 1 | 01.01.2026 |

## СТРАНИЦА 7
**Лист:** СО1
**Наименование листа:** Спецификация оборудования

### BLOCK [TEXT]: ABC
| Поз. | Наименование и техническая характеристика | Тип, марка | Поставщик | Единица измерения | Количество | Примечание |
|---|---|---|---|---|---|---|
| | Кабельно-проводниковая продукция | | | | | |
| 1 | Кабель силовой огнестойкий 3x2,5 | ППГнг(А)-FRHF | Конкорд или аналог | м | 120 | ОКЛ |

###### Электроустановочные изделия
| Датчик движения IP44 180 градусов | MD 180 Basic | ESYLUX или аналог | шт. | 3 | проверить дальность |
|---|---|---|---|---|---|
| Выключатель одноклавишный IP44 | BA10-041B | Systeme Electric | шт. | 4 | |
"""

FORM7_MD = """
## СТРАНИЦА 11
**Лист:** КЖ-7
**Наименование листа:** Спецификация элементов

| Поз. | Обозначение | Наименование | Кол. | Масса ед., кг | Примечание |
|---|---|---|---|---|---|
| | | Изделия металлические | | | |
| М1 | ГОСТ 23118-2019 | Балка стальная Б1 | 2 | 125,5 | оцинкованная |
"""

FORM7_EQUIPMENT_MD = """
## СТРАНИЦА 12
**Лист:** ЭОМ.СО
**Наименование листа:** Спецификация оборудования, изделий и материалов

| Позиция | Наименование и техническая характеристика | Тип, марка, обозначение документа, опросного листа | Код оборудования, изделия, материала | Завод-изготовитель | Ед. изм. | Количество | Масса единицы, кг | Примечание |
|---|---|---|---|---|---|---|---|---|
| | Щитовое электрооборудование | | | | | | | |
| ВРУ-1П | Шкаф управления вентилятором 15 кВт | ЩУВ-15-03-R3 | 27.12.31.000 | Рубеж | шт. | 1 | 42,5 | ДВ1 |
"""

CUSTOM_SPEC_MD = """
## СТРАНИЦА 4
**Лист:** 2

##### СПЕЦИФИКАЦИЯ ОСВЕТИТЕЛЬНОГО ОБОРУДОВАНИЯ
| № п/п | Условноеобозначение | Внешнийвид | Наименование ОП | Тип ИС | Оптика | Управление | Цвет | Аксессуар | Мощность | Кол-во,шт. | Потребляемаямощность |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | L1 | ● | Светильник Marko, h=4м | 3000K | Diffuse | DALI | RAL9011 | опора 4м | 30 | 6 | 180 |
"""


def _parse(project_id: str, project_name: str = "Проект") -> list[dict]:
    return parse_specification_markdown(
        SPEC_MD,
        project_id=project_id,
        project_name=project_name,
        version_id="v002",
        md_file="document.md",
    )


def test_parser_reads_only_specification_tables_and_keeps_provenance():
    rows = _parse("EOM/P1")

    assert len(rows) == 3
    assert {row["page"] for row in rows} == {7}
    assert all(row["sheet"] == "СО1" for row in rows)
    assert all(row["project_id"] == "EOM/P1" for row in rows)
    assert all(row["version_id"] == "v002" for row in rows)
    assert rows[0]["position"] == "1"
    assert rows[0]["quantity"] == "120"
    assert rows[1]["category"] == "Электроустановочные изделия"
    assert rows[1]["name"].startswith("Датчик движения")


def test_parser_keeps_form7_designation_mass_and_section():
    rows = parse_specification_markdown(
        FORM7_MD,
        project_id="KJ/P1",
        project_name="Корпус 1",
        version_id="v1",
    )

    assert len(rows) == 1
    assert rows[0]["category"] == "Изделия металлические"
    assert rows[0]["position"] == "М1"
    assert rows[0]["designation"] == "ГОСТ 23118-2019"
    assert rows[0]["mass"] == "125,5"


def test_parser_reads_all_nine_form7_equipment_columns():
    rows = parse_specification_markdown(
        FORM7_EQUIPMENT_MD,
        project_id="EOM/P1",
        project_name="Корпус 1",
        version_id="v2",
    )

    assert len(rows) == 1
    assert rows[0]["category"] == "Щитовое электрооборудование"
    assert rows[0]["position"] == "ВРУ-1П"
    assert rows[0]["name"] == "Шкаф управления вентилятором 15 кВт"
    assert rows[0]["type_mark"] == "ЩУВ-15-03-R3"
    assert rows[0]["code"] == "27.12.31.000"
    assert rows[0]["manufacturer"] == "Рубеж"
    assert rows[0]["unit"] == "шт."
    assert rows[0]["quantity"] == "1"
    assert rows[0]["mass"] == "42,5"
    assert rows[0]["note"] == "ДВ1"


def test_parser_normalizes_custom_specification_into_form7_columns():
    rows = parse_specification_markdown(
        CUSTOM_SPEC_MD,
        project_id="EOM/P2",
        project_name="Наружное освещение",
        version_id="v1",
    )

    assert len(rows) == 1
    assert rows[0]["position"] == "1"
    assert rows[0]["name"] == "Светильник Marko, h=4м"
    assert rows[0]["type_mark"] == "L1"
    assert rows[0]["unit"] == "шт."
    assert rows[0]["quantity"] == "6"
    assert rows[0]["note"] == "опора 4м"


def test_shared_groups_require_two_projects_and_sum_compatible_quantities():
    rows = _parse("P1", "Корпус 1") + _parse("P2", "Корпус 2")
    groups = group_shared_specification_items(rows)

    cable = next(group for group in groups if "Кабель силовой" in group["name"])
    assert cable["project_count"] == 2
    assert cable["total_quantity"] == 240
    assert cable["unit"] == "м"
    assert set(cable["project_ids"]) == {"P1", "P2"}


def test_similar_accepted_optimizations_are_only_merge_candidates():
    common = {
        "current": "Кабель закупается отдельно по каждому корпусу у разных поставщиков",
        "proposed": "Объединить закупку кабеля по корпусам и запросить единое коммерческое предложение",
        "spec_items": ["Кабель силовой огнестойкий ППГнг 3x2,5"],
    }
    clusters = cluster_accepted_optimizations([
        {"project_id": "P1", "id": "OPT-001", **common},
        {"project_id": "P2", "id": "OPT-004", **common},
    ])

    assert len(clusters) == 1
    assert clusters[0]["project_count"] == 2
    assert set(clusters[0]["item_refs"]) == {"P1:OPT-001", "P2:OPT-004"}
    assert clusters[0]["title"].startswith("Унифицировать: Кабель")
    assert clusters[0]["match_basis"] == "совпадение позиций спецификации"
    assert clusters[0]["match_score"] == 1
    assert clusters[0]["representative_proposal"] == common["proposed"]
    assert clusters[0]["graphics_recommended"] is False


def test_section_payload_includes_only_expert_accepted_optimizations():
    projects = [
        SimpleNamespace(project_id="P1", name="Корпус 1", section="EOM", version_id="v1", block_count=0),
        SimpleNamespace(project_id="P2", name="Корпус 2", section="EOM", version_id="v1", block_count=0),
        SimpleNamespace(project_id="P3", name="Корпус 3", section="EOM", version_id="v1", block_count=0),
        SimpleNamespace(project_id="AR1", name="АР", section="AR", version_id="v1", block_count=0),
    ]

    def loader(project):
        accepted_id = "OPT-001"
        return {
            "version_id": "v003",
            "md_text": SPEC_MD,
            "md_file": "document.md",
            "optimization": {
                "items": [
                    {
                        "id": accepted_id,
                        "current": "Раздельная закупка кабеля по корпусам разными поставщиками",
                        "proposed": "Объединить закупку кабеля по корпусам в единый договор поставки",
                        "spec_items": ["Кабель ППГнг 3x2,5"],
                        "type": "cheaper_analog",
                    },
                    {"id": "OPT-002", "current": "A", "proposed": "B", "spec_items": []},
                ]
            },
            "expert_review": {
                "decisions": ([
                    {"item_id": accepted_id, "item_type": "optimization", "decision": "accepted"},
                    {"item_id": "OPT-002", "item_type": "optimization", "decision": "rejected"},
                ] if project.project_id in {"P1", "P2"} else [])
            },
            "graphic_blocks": 5,
            "error": None,
        }

    payload = build_section_optimization("EOM", projects=projects, loader=loader)

    assert payload["meta"]["project_count"] == 3
    assert payload["meta"]["specification_rows"] == 9
    assert payload["meta"]["optimization_items"] == 6
    assert payload["meta"]["accepted_optimizations"] == 2
    assert payload["meta"]["graphic_blocks_available"] == 15
    assert {item["project_id"] for item in payload["accepted_optimizations"]} == {"P1", "P2"}
    assert all(item["id"] == "OPT-001" for item in payload["accepted_optimizations"])
    assert not any(signal["kind"] == "consolidated_procurement" for signal in payload["signals"])
    assert all(signal["kind"] == "replicate_accepted_optimization" for signal in payload["signals"])
    assert payload["meta"]["accepted_merge_candidates"] == 1
    assert payload["meta"]["replication_candidates"] == 1
    assert payload["signals"][0]["source_project_ids"] == ["P1", "P2"]
    assert payload["signals"][0]["target_project_ids"] == ["P3"]
    assert payload["capabilities"]["targeted_graphics_agent"] is True


def test_replication_keeps_fire_class_and_ip_constraints():
    accepted = [{
        "source_ref": "P1:OPT-001",
        "project_id": "P1",
        "id": "OPT-001",
        "current": "Применена труба НГ",
        "proposed": "Заменить производителя без изменения пожарного исполнения",
        "spec_items": ["Поз. 1 — Труба гибкая ПЛЛ НГ 25мм IP44, 100 м"],
    }]
    rows = [
        {
            "row_id": "SPEC-NG",
            "project_id": "P2",
            "name": "Труба гибкая ПЛЛ НГ 25мм",
            "type_mark": "IP44",
        },
        {
            "row_id": "SPEC-HF",
            "project_id": "P3",
            "name": "Труба гибкая ПЛЛ HF 25мм",
            "type_mark": "IP44",
        },
        {
            "row_id": "SPEC-IP55",
            "project_id": "P4",
            "name": "Труба гибкая ПЛЛ НГ 25мм",
            "type_mark": "IP55",
        },
    ]

    signals = build_replication_signals(rows, accepted)

    assert len(signals) == 1
    assert signals[0]["target_project_ids"] == ["P2"]
    assert signals[0]["target_row_ids"] == ["SPEC-NG"]


def test_graphics_are_requested_only_for_geometry_sensitive_merge_candidates():
    common = {
        "current": "Раздельные решения по корпусам",
        "proposed": "Унифицировать однолинейную схему подключения щита",
        "spec_items": ["Щит распределительный этажный ЩЭ-4"],
    }
    clusters = cluster_accepted_optimizations([
        {"project_id": "P1", "id": "OPT-001", **common},
        {"project_id": "P2", "id": "OPT-002", **common},
    ])

    assert len(clusters) == 1
    assert clusters[0]["graphics_recommended"] is True
