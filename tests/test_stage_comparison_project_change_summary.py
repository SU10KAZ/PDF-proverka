from __future__ import annotations

import copy

import pytest

from backend.app.services.stage_comparison import project_change_summary as summary


def evidence(
    evidence_id: str, text: str, *, before: str | None = None,
    after: str | None = None, status: str = "CHANGED",
) -> dict:
    item = {
        "evidence_id": evidence_id,
        "source_status": status,
        "summary": text,
        "before": before,
        "after": after,
        "reason": "Факт подтверждён текстом.",
        "left_fragment_ids": [f"L-{evidence_id}"],
        "right_fragment_ids": [f"R-{evidence_id}"],
        "left_pages": [2],
        "right_pages": [3],
        "left_anchors": [],
        "right_anchors": [],
    }
    hint, category = summary.deterministic_class_hint(item, {
        "left_labels": ["Лист 1 — Однолинейная схема ВРУ-А"],
        "right_labels": ["Лист 1 — Однолинейная схема ВРУ-А"],
    })
    item["deterministic_class_hint"] = hint
    item["deterministic_category_hint"] = category
    return item


def source_group(items: list[dict], *, left: str = "Лист 1 — Однолинейная схема ВРУ-А",
                 right: str = "Лист 1 — Однолинейная схема ВРУ-А") -> dict:
    group = {
        "group_id": "group-1",
        "left_pages": [2], "right_pages": [3],
        "left_labels": [left], "right_labels": [right],
        "atomic_evidence": items,
        "source_group_sha256": "source-sha",
    }
    group["pair_precheck"] = summary.precheck_sheet_purpose(group)
    return group


def classify(text: str, *, before: str | None = None, after: str | None = None,
             label: str = "Лист 1 — Однолинейная схема ВРУ-А") -> tuple[str, str]:
    item = {"summary": text, "before": before, "after": after, "source_status": "CHANGED"}
    return summary.deterministic_class_hint(item, {
        "left_labels": [label], "right_labels": [label],
    })


@pytest.mark.parametrize("text", [
    "Изменены сведения о СРО и номер свидетельства.",
    "ГИП Иванов заменён на Петрова.",
    "Год изменён с 2022 на 2026.",
    "Изменился номер страницы 12 на 14.",
    "Схема ВРУ-3 разделена на два листа.",
])
def test_service_structure_examples(text: str):
    assert classify(text)[0] == "SERVICE_STRUCTURE"


@pytest.mark.parametrize(("text", "category"), [
    ("Площадь помещения изменена с 25,4 м2 до 25,9 м2.", "areas"),
    ("Этажность корпуса 4 увеличена с 10 до 16 этажей.", "floors"),
    ("Суммарная электрическая нагрузка изменена с 120 до 160 кВт.", "electrical_load"),
    ("Метод расчёта по кратности заменён расчётом по вредностям.", "calculation_method"),
])
def test_project_change_examples(text: str, category: str):
    assert classify(text) == ("PROJECT_CHANGE", category)


def test_pure_equipment_rename_is_service_structure():
    assert classify("ВРУ-1 переименовано в ВРУ-А.")[0] == "SERVICE_STRUCTURE"


def test_rename_with_changed_parameters_is_project_change():
    result = classify(
        "ВРУ-1 переименовано в ВРУ-А, мощность изменена с 80 до 120 кВт.",
        before="ВРУ-1, 80 кВт", after="ВРУ-А, 120 кВт",
    )
    assert result == ("PROJECT_CHANGE", "electrical_load")


def test_twenty_area_changes_become_one_backend_counted_change():
    items = [evidence(
        f"area-{index}", f"Площадь помещения {index} изменена.",
        before=f"{index},0 м2", after=f"{index},5 м2",
    ) for index in range(1, 21)]
    group = source_group(items)
    model_items = [{
        "class": "PROJECT_CHANGE", "category": "areas",
        "title": "Скорректированы площади помещений",
        "evidence_ids": [item["evidence_id"] for item in items],
    }]
    normalized = summary.validate_group_response(
        {"group_id": group["group_id"], "items": model_items}, group,
    )
    result = summary.build_group_summary(
        group, normalized, aggregation_status="ai_aggregated", error=None,
    )
    assert len(result["project_changes"]) == 1
    assert result["project_changes"][0]["count"] == 20
    assert result["project_changes"][0]["title"] == "Скорректированы площади 20 помещений."


def test_eleven_vru_consumers_become_one_change_with_all_details():
    items = [evidence(
        f"load-{index}", f"Добавлен потребитель ВРУ-А: нагрузка {index}.",
        after=f"Потребитель {index}", status="ADDED",
    ) for index in range(1, 12)]
    group = source_group(items)
    result = summary.build_group_summary(group, [{
        "class": "PROJECT_CHANGE", "category": "consumer_composition",
        "title": "Расширен состав потребителей ВРУ-А",
        "evidence_ids": [item["evidence_id"] for item in items],
    }], aggregation_status="ai_aggregated", error=None)
    assert len(result["project_changes"]) == 1
    change = result["project_changes"][0]
    assert change["count"] == 11
    assert len(change["details"]) == 11
    assert set(change["evidence_ids"]) == {item["evidence_id"] for item in items}


def test_wrong_purpose_pair_requires_review_and_never_creates_project_change():
    group = source_group(
        [evidence("load-1", "Добавлен потребитель ВРУ-А: ворота.", status="ADDED")],
        left="Лист 8 — Молниезащита и заземление",
        right="Лист 7 — Однолинейная схема ВРУ-А",
    )
    assert group["pair_precheck"]["status"] == summary.PAIR_REVIEW_REQUIRED
    result = summary.build_wrong_pair_summary(group)
    assert result["project_changes"] == []
    assert len(result["review"]) == 1
    assert result["review"][0]["count"] == 1


def test_uncertain_evidence_must_remain_review():
    item = evidence("uncertain-1", "OCR не позволяет прочитать значение.", status="UNCERTAIN")
    item["deterministic_class_hint"] = "REVIEW"
    item["deterministic_category_hint"] = "uncertain"
    group = source_group([item])
    with pytest.raises(summary.SummaryValidationError, match="uncertain_must_remain_review"):
        summary.validate_group_response({"group_id": "group-1", "items": [{
            "class": "PROJECT_CHANGE", "category": "other_project",
            "title": "Изменено значение", "evidence_ids": ["uncertain-1"],
        }]}, group)
    fallback = summary.deterministic_fallback_items(group)
    assert fallback[0]["class"] == "REVIEW"


def test_invalid_ai_group_fallback_exposes_everything_only_as_review():
    items = [
        evidence("area-1", "Площадь помещения изменена.", before="10 м2", after="11 м2"),
        evidence("sro-1", "Изменены сведения о СРО."),
    ]
    fallback = summary.deterministic_fallback_items(source_group(items))
    assert fallback == [{
        "class": "REVIEW", "category": "uncertain",
        "title": "Классификация группы требует проверки",
        "evidence_ids": ["area-1", "sro-1"],
    }]


def test_model_cannot_invent_aggregate_count_or_evidence():
    items = [evidence("area-1", "Площадь помещения изменена.", before="10 м2", after="11 м2")]
    group = source_group(items)
    with pytest.raises(summary.SummaryValidationError, match="unsupported_title"):
        summary.validate_group_response({"group_id": "group-1", "items": [{
            "class": "PROJECT_CHANGE", "category": "areas",
            "title": "Скорректированы площади 20 помещений",
            "evidence_ids": ["area-1"],
        }]}, group)
    with pytest.raises(summary.SummaryValidationError, match="hallucinated_evidence"):
        summary.validate_group_response({"group_id": "group-1", "items": [{
            "class": "PROJECT_CHANGE", "category": "areas",
            "title": "Скорректированы площади помещений",
            "evidence_ids": ["invented"],
        }]}, group)


def test_every_atomic_evidence_must_be_preserved_exactly_once():
    items = [evidence("area-1", "Площадь помещения изменена."), evidence(
        "area-2", "Площадь другого помещения изменена.",
    )]
    group = source_group(items)
    with pytest.raises(summary.SummaryValidationError, match="incomplete_evidence_coverage"):
        summary.validate_group_response({"group_id": "group-1", "items": [{
            "class": "PROJECT_CHANGE", "category": "areas",
            "title": "Скорректирована площадь помещения", "evidence_ids": ["area-1"],
        }]}, group)


def test_single_group_id_typo_can_recover_only_through_valid_evidence_ids():
    item = evidence("area-1", "Площадь помещения изменена.", before="10 м2", after="11 м2")
    group = source_group([item])
    payload = {"groups": [{
        "group_id": "mistyped-id", "items": [{
            "class": "PROJECT_CHANGE", "category": "areas",
            "title": "Скорректирована площадь помещения", "evidence_ids": ["area-1"],
        }],
    }]}
    with pytest.raises(summary.SummaryValidationError, match="unexpected_or_duplicate_group"):
        summary.validate_response(payload, [group])
    recovered = summary.validate_response(
        payload, [group], recover_single_group_id=True,
    )
    assert recovered[0][0]["evidence_ids"] == ["area-1"]


def test_semantic_outer_groups_can_only_flatten_with_exact_evidence_coverage():
    items = [
        evidence("area-1", "Площадь помещения изменена.", before="10 м2", after="11 м2"),
        evidence("load-1", "Электрическая нагрузка изменена.", before="10 кВт", after="12 кВт"),
    ]
    group = source_group(items)
    payload = {"groups": [
        {"group_id": "g_areas", "items": [{
            "class": "PROJECT_CHANGE", "category": "areas",
            "title": "Скорректирована площадь помещения", "evidence_ids": ["area-1"],
        }]},
        {"group_id": "g_loads", "items": [{
            "class": "PROJECT_CHANGE", "category": "electrical_load",
            "title": "Изменена электрическая нагрузка", "evidence_ids": ["load-1"],
        }]},
    ]}
    recovered = summary.validate_response(
        payload, [group], recover_single_group_id=True,
    )
    assert [item["evidence_ids"] for item in recovered[0]] == [["area-1"], ["load-1"]]


def test_summary_build_does_not_mutate_stage4_payload():
    final = {
        "source_signature": "stage4-sha",
        "sheet_groups": [{
            "id": "link-1", "left_pages": [1], "right_pages": [1],
            "left_labels": ["Страница 1"], "right_labels": ["Страница 1"],
            "changed": [{
                "summary": "Год изменён с 2022 на 2026.",
                "before": "2022", "after": "2026",
                "left_fragment_ids": ["L1"], "right_fragment_ids": ["R1"],
            }], "removed": [], "added": [], "uncertain": [],
        }],
    }
    original = copy.deepcopy(final)
    groups = summary.build_source_groups(final)
    summary.source_signature(final, groups)
    assert final == original


def test_contents_group_is_always_documentation_structure():
    assert classify(
        "Добавлен отдельный лист ВРУ-ИТП.", label="Лист 1 — Содержание тома",
    ) == ("SERVICE_STRUCTURE", "documentation_structure")


def test_unconfirmed_change_register_claim_stays_review():
    item = {
        "source_status": "REMOVED",
        "summary": "Запись об изменении этажности корпуса 4 отсутствует справа.",
        "before": "В соответствии с изменениями в проектных решениях этажность корпуса 4 до 16 этажей.",
        "after": None,
    }
    assert summary.deterministic_class_hint(item, {
        "left_labels": ["Страница 3"], "right_labels": ["Страница 3"],
    }) == ("REVIEW", "floors")


@pytest.mark.parametrize("text", [
    "Добавлена экспликация помещений этажа.",
    "В таблице появилась новая строка обозначений.",
    "Изменена легенда условных обозначений.",
    "Удалено примечание к плану.",
    "Изменена маркировка двери Д-1 на Д-2.",
    "Добавлено обозначение утепления наружной стены.",
    "Изменён номер позиции оборудования.",
    "Добавлена категория «Технические помещения».",
    "Добавлена выноска к узлу.",
    "Изменён заголовок таблицы.",
    "Добавлена расшифровка марки.",
    "Изменено форматирование таблицы.",
    "Изменена нумерация помещений.",
    "Добавлены обозначения марок помещения, пола и двери.",
    "Удалены обозначения утепления наружной стены.",
])
def test_drawing_information_defaults_to_service_structure(text: str):
    assert classify(text)[0] == "SERVICE_STRUCTURE"


@pytest.mark.parametrize(("text", "category"), [
    ("Добавлена дверь Д-17.", "other_project"),
    ("Удалена лестница между первым и вторым этажами.", "other_project"),
    ("Ширина проёма изменена с 900 до 1200 мм.", "dimensions"),
    ("Площадь помещения изменена с 18 до 21 м2.", "areas"),
    ("Электрическая нагрузка изменена с 20 до 25 кВт.", "electrical_load"),
    ("Изменён принцип работы системы вентиляции.", "system_configuration"),
    ("Заменён материал наружной стены.", "materials"),
    ("Перенесено оборудование вентиляционной установки.", "other_project"),
    ("Добавлено помещение электрощитовой.", "other_project"),
])
def test_actual_object_or_parameter_change_stays_project(text: str, category: str):
    assert classify(text) == ("PROJECT_CHANGE", category)


def test_actual_parameter_in_explication_table_stays_project_change():
    assert classify(
        "В таблице экспликации площадь помещения изменена с 10 до 12 м2.",
        before="10 м2", after="12 м2",
    ) == ("PROJECT_CHANGE", "areas")


def test_unchanged_dimensions_inside_renamed_note_do_not_make_it_project_change():
    assert classify(
        "Изменено обозначение помещений: «кладовых» заменено на «кладковых».",
        before="Перегородки кладовых высотой 2,85 м и 3,0 м.",
        after="Перегородки кладковых высотой 2,85 м и 3,0 м.",
    )[0] == "SERVICE_STRUCTURE"


def test_added_table_row_without_confirmed_object_is_service_structure():
    assert classify("Добавлена строка ПОН: 2, 18,20.")[0] == "SERVICE_STRUCTURE"


def test_ambiguous_element_change_stays_review():
    assert classify("Скорректирован элемент на плане.") == ("REVIEW", "uncertain")


def test_validator_reclassifies_drawing_only_model_item_as_service():
    item = evidence("explication-1", "Добавлена экспликация помещений этажа.", status="ADDED")
    group = source_group([item])
    normalized = summary.validate_group_response({"group_id": "group-1", "items": [{
        "class": "PROJECT_CHANGE", "category": "room_composition",
        "title": "Добавлена экспликация помещений этажа",
        "evidence_ids": ["explication-1"],
    }]}, group)
    assert normalized[0]["class"] == "SERVICE_STRUCTURE"
    assert normalized[0]["category"] == "documentation_structure"


def test_validator_reclassifies_actual_door_model_item_as_project():
    item = evidence("door-1", "Добавлена дверь Д-17.", status="ADDED")
    group = source_group([item])
    normalized = summary.validate_group_response({"group_id": "group-1", "items": [{
        "class": "SERVICE_STRUCTURE", "category": "documentation_structure",
        "title": "Добавлена дверь Д-17", "evidence_ids": ["door-1"],
    }]}, group)
    assert normalized[0]["class"] == "PROJECT_CHANGE"
    assert normalized[0]["category"] == "other_project"


def test_validator_keeps_mixed_project_and_drawing_aggregate_in_review():
    project = evidence("door-1", "Добавлена дверь Д-17.", status="ADDED")
    drawing = evidence("mark-1", "Добавлена маркировка двери Д-17.", status="ADDED")
    group = source_group([project, drawing])
    normalized = summary.validate_group_response({"group_id": "group-1", "items": [{
        "class": "SERVICE_STRUCTURE", "category": "documentation_structure",
        "title": "Добавлена дверь Д-17 и ее маркировка",
        "evidence_ids": ["door-1", "mark-1"],
    }]}, group)
    assert normalized[0]["class"] == "REVIEW"
    assert normalized[0]["category"] == "uncertain"
