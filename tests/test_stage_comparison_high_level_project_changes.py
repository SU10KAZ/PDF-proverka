from __future__ import annotations

import copy

import pytest

from backend.app.services.stage_comparison import high_level_project_changes as high
from backend.app.services.stage_comparison import project_change_summary as stage5


def atomic(
    evidence_id: str, summary: str, *, before: str | None = None,
    after: str | None = None, status: str = "CHANGED",
    hint: str = "PROJECT_CHANGE", hint_category: str = "uncertain",
) -> dict:
    return {
        "evidence_id": evidence_id, "source_status": status,
        "summary": summary, "before": before, "after": after,
        "reason": "Факт подтверждён переданным текстом.",
        "left_fragment_ids": [f"L-{evidence_id}"] if before else [],
        "right_fragment_ids": [f"R-{evidence_id}"] if after else [],
        "left_pages": [2] if before else [], "right_pages": [3] if after else [],
        "left_anchors": [], "right_anchors": [],
        "deterministic_class_hint": hint,
        "deterministic_category_hint": hint_category,
    }


def stage5_summary(
    evidence: list[dict], *, stage5_class: str = "PROJECT_CHANGE",
    category: str = "uncertain", pair_status: str = stage5.PAIR_OK,
    group_id: str = "sheet-1", aggregation_status: str = "ai_aggregated",
) -> dict:
    bucket = {
        "PROJECT_CHANGE": "project_changes",
        "SERVICE_STRUCTURE": "service_structure",
        "REVIEW": "review",
    }[stage5_class]
    group = {
        "group_id": group_id, "left_pages": [2], "right_pages": [3],
        "left_labels": ["Лист 2 — Схема"], "right_labels": ["Лист 3 — Схема"],
        "pair_precheck": {"status": pair_status},
        "aggregation_status": aggregation_status,
        "project_changes": [], "service_structure": [], "review": [],
        "atomic_evidence": evidence,
    }
    group[bucket] = [{
        "id": "stage5-item", "title": "Сводный факт.", "category": category,
        "evidence_ids": [item["evidence_id"] for item in evidence],
        "count": len(evidence), "details": evidence,
    }]
    return {
        "version": stage5.VERSION, "kind": stage5.KIND,
        "pair_id": "pair", "source_signature": "stage5-source",
        "status": "completed", "sheet_groups": [group],
    }


def combine(*summaries: dict) -> dict:
    result = copy.deepcopy(summaries[0])
    result["sheet_groups"] = [
        group for summary in summaries for group in summary["sheet_groups"]
    ]
    return result


def synthesize(summary: dict) -> dict:
    groups = high.build_semantic_groups(summary)
    resolved, ai_required = high.deterministic_decisions(groups)
    decisions = resolved + [high.fallback_decision(group, "test_no_ai") for group in ai_required]
    return high.build_artifact(
        pair_id="pair", generated_at="2026-01-01T00:00:00+00:00",
        source_signature_value=high.source_signature(summary, groups),
        project_summary=summary, semantic_groups=groups, decisions=decisions,
    )


def test_multiple_areas_become_one_backend_counted_change():
    evidence = [atomic(
        f"area-{index}", f"Площадь помещения {index}.А.1 изменена с {index},0 до {index},5 м².",
        before=f"{index}.А.1 Помещение {index},0 м²",
        after=f"{index}.А.1 Помещение {index},5 м²", hint_category="areas",
    ) for index in range(1, 21)]
    artifact = synthesize(stage5_summary(evidence, category="areas"))
    assert len(artifact["high_level_changes"]) == 1
    change = artifact["high_level_changes"][0]
    assert change["type"] == "PARAMETER_SET_CHANGED"
    assert change["count"] == 20
    assert change["title"] == "Скорректированы площади 20 помещений."


def test_multiple_load_values_become_one_parameter_set_change():
    evidence = [atomic(
        f"load-{index}", f"Расчётная нагрузка потребителя {index} изменена.",
        before=f"Потребитель {index}: {index} кВт", after=f"Потребитель {index}: {index + 1} кВт",
        hint_category="electrical_load",
    ) for index in range(1, 6)]
    artifact = synthesize(stage5_summary(evidence, category="electrical_load"))
    assert [item["type"] for item in artifact["high_level_changes"]] == ["PARAMETER_SET_CHANGED"]
    assert artifact["high_level_changes"][0]["title"] == "Скорректированы расчётные нагрузки группы потребителей."


def test_paraphrase_has_no_high_level_change():
    item = atomic(
        "para", "Формулировка перефразирована, смысл решения не изменился.",
        before="Питание выполняется от ТП.", after="Электропитание предусмотрено от ТП.",
    )
    artifact = synthesize(stage5_summary([item]))
    assert artifact["high_level_changes"] == []
    assert artifact["non_material_review"][0]["type"] == "NO_HIGH_LEVEL_CHANGE"


def test_spelling_correction_inside_same_material_spec_is_not_material_change():
    item = atomic(
        "spelling", "антипиррированными → антипирированными.",
        before="Бруски антипиррированные 50х50, шаг 600; утеплитель НГ 50 мм",
        after="Бруски антипирированные 50х50, шаг 600; утеплитель НГ 50 мм",
        hint="UNCLASSIFIED", hint_category="uncertain",
    )
    artifact = synthesize(stage5_summary(
        [item], stage5_class="REVIEW", category="materials",
    ))
    assert artifact["high_level_changes"] == []
    assert artifact["non_material_review"][0]["reason"] == "NO_SEMANTIC_CHANGE"


def test_service_only_is_suppressed_and_collapsed():
    item = atomic(
        "service", "Изменён номер листа с 2 на 3.", before="Лист 2", after="Лист 3",
        hint="SERVICE_STRUCTURE", hint_category="documentation_structure",
    )
    artifact = synthesize(stage5_summary(
        [item], stage5_class="SERVICE_STRUCTURE", category="documentation_structure",
    ))
    assert artifact["high_level_changes"] == []
    assert artifact["service_structure_summary"]["collapsed"] is True
    assert artifact["service_structure_summary"]["evidence_ids"] == ["service"]


def test_detail_increase_is_neutral_not_project_change():
    item = atomic(
        "detail", "В РД подробно перечислены устройства той же системы вентиляции.",
        before="Система вентиляции.", after="Система вентиляции: вентилятор, клапан, датчик.",
        hint_category="system_configuration",
    )
    artifact = synthesize(stage5_summary([item], category="system_configuration"))
    assert artifact["high_level_changes"] == []
    assert artifact["detail_level_increased"][0]["type"] == "DETAIL_LEVEL_INCREASED"


def test_actual_system_operation_principle_change_is_detected():
    item = atomic(
        "operation", "Изменён принцип резервирования и логика переключения системы.",
        before="Резервирование без автоматического переключения.",
        after="Автоматическое переключение на резервный источник.",
        hint_category="system_configuration",
    )
    artifact = synthesize(stage5_summary([item], category="system_configuration"))
    assert artifact["high_level_changes"][0]["type"] == "SYSTEM_OPERATION_CHANGED"


def test_formula_or_method_change_is_calculation_approach():
    item = atomic(
        "formula", "Формула расчёта изменена: применён другой коэффициент.",
        before="Q = A × 0,8", after="Q = A × 1,2", hint_category="calculation_method",
    )
    artifact = synthesize(stage5_summary([item], category="calculation_method"))
    assert artifact["high_level_changes"][0]["type"] == "CALCULATION_APPROACH_CHANGED"


def test_room_purpose_change_is_space_program():
    item = atomic(
        "purpose", "Изменено назначение помещения.",
        before="1.А.1 Кладовая", after="1.А.1 Электрощитовая", hint_category="room_composition",
    )
    artifact = synthesize(stage5_summary([item], category="room_composition"))
    assert artifact["high_level_changes"][0]["type"] == "SPACE_PROGRAM_CHANGED"


def test_ambiguous_project_review_is_material():
    item = atomic(
        "ambiguous", "Мощность могла измениться, значение OCR не распознано.",
        before="Мощность 10 кВт", status="UNCERTAIN", hint="REVIEW",
        hint_category="electrical_load",
    )
    artifact = synthesize(stage5_summary([item], stage5_class="REVIEW"))
    assert artifact["high_level_changes"] == []
    assert artifact["material_review"][0]["status"] == high.REVIEW_REQUIRED


def test_irrelevant_review_is_non_material():
    item = atomic(
        "weak", "Исправлена пунктуация в заголовке.",
        before="Общие данные", after="Общие данные.", status="UNCERTAIN",
        hint="REVIEW", hint_category="uncertain",
    )
    artifact = synthesize(stage5_summary([item], stage5_class="REVIEW"))
    assert artifact["material_review"] == []
    assert artifact["non_material_review"][0]["evidence_ids"] == ["weak"]


def _one_sided_ai_group() -> dict:
    item = atomic(
        "stairs", "Добавлена технологическая лестница 7.ПОН.2.",
        after="7.ПОН.2 Лестница технологическая", status="ADDED",
        hint_category="room_composition",
    )
    groups = high.build_semantic_groups(stage5_summary([item], category="room_composition"))
    return next(group for group in groups if group["route"] == "AI_REVIEW")


def test_ai_validator_rejects_unsupported_number():
    group = _one_sided_ai_group()
    with pytest.raises(high.HighLevelValidationError, match="unsupported_claim"):
        high.validate_ai_response({"groups": [{
            "group_id": group["group_id"], "decision": "REAL_CHANGE",
            "type": "SPACE_PROGRAM_CHANGED", "title": "Добавлены 20 лестниц",
            "reason": "Текст подтверждает добавление лестницы",
            "evidence_ids": group["evidence_ids"],
        }]}, [group])


def test_ai_validator_rejects_invented_entity():
    group = _one_sided_ai_group()
    with pytest.raises(high.HighLevelValidationError, match="unsupported_claim"):
        high.validate_ai_response({"groups": [{
            "group_id": group["group_id"], "decision": "REAL_CHANGE",
            "type": "SPACE_PROGRAM_CHANGED", "title": "Добавлена лестница 9.ПОН.9",
            "reason": "Текст подтверждает добавление лестницы",
            "evidence_ids": group["evidence_ids"],
        }]}, [group])


def test_ai_validator_cannot_publish_one_sided_presence_as_real_change():
    group = _one_sided_ai_group()
    with pytest.raises(high.HighLevelValidationError, match="one_sided_presence_cannot_publish"):
        high.validate_ai_response({"groups": [{
            "group_id": group["group_id"], "decision": "REAL_CHANGE",
            "type": "SPACE_PROGRAM_CHANGED", "title": "Добавлена технологическая лестница 7.ПОН.2",
            "reason": "Справа присутствует технологическая лестница 7.ПОН.2",
            "evidence_ids": group["evidence_ids"],
        }]}, [group])


def test_ai_validator_requires_exact_evidence_coverage():
    group = _one_sided_ai_group()
    with pytest.raises(high.HighLevelValidationError, match="incomplete_or_hallucinated_evidence"):
        high.validate_ai_response({"groups": [{
            "group_id": group["group_id"], "decision": "INSUFFICIENT_CONTEXT",
            "type": "UNRESOLVED_HIGH_LEVEL_CHANGE", "title": "Требуется проверка",
            "reason": "Недостаточно контекста", "evidence_ids": ["invented"],
        }]}, [group])


def test_every_published_change_has_text_provenance():
    item = atomic(
        "area", "Площадь помещения 1.А.1 изменена.",
        before="1.А.1 10 м²", after="1.А.1 11 м²", hint_category="areas",
    )
    artifact = synthesize(stage5_summary([item], category="areas"))
    change = artifact["high_level_changes"][0]
    assert change["evidence_sources"] == ["TEXT"]
    assert change["evidence_ids"] == ["area"]
    assert change["details"][0]["left_fragment_ids"] == ["L-area"]


def test_uncertain_sheet_link_can_never_publish_strong_change():
    item = atomic(
        "link", "Расчётная нагрузка изменена.", before="10 кВт", after="20 кВт",
        hint_category="electrical_load",
    )
    summary = stage5_summary(
        [item], category="electrical_load", pair_status=stage5.PAIR_REVIEW_REQUIRED,
    )
    artifact = synthesize(summary)
    assert artifact["high_level_changes"] == []
    assert artifact["material_review"][0]["reason"] == high.SOURCE_LINK_UNCERTAIN


def test_ai_validator_refuses_real_change_from_uncertain_link():
    item = atomic("link", "Добавлена лестница.", after="Лестница", status="ADDED")
    group = high.build_semantic_groups(stage5_summary(
        [item], pair_status=stage5.PAIR_REVIEW_REQUIRED,
    ))[0]
    with pytest.raises(high.HighLevelValidationError, match="source_link_uncertain_cannot_publish"):
        high.validate_ai_response({"groups": [{
            "group_id": group["group_id"], "decision": "REAL_CHANGE",
            "type": group["candidate_type"], "title": "Добавлена лестница",
            "reason": "Лестница присутствует справа", "evidence_ids": group["evidence_ids"],
        }]}, [group])


def test_cross_sheet_same_version_counterpart_suppresses_false_removal():
    removed = atomic(
        "removed", "Утепление толщиной 50 мм отсутствует справа.",
        before="У1 Утепление минеральной ватой 50 мм", status="REMOVED",
        hint_category="materials",
    )
    same = atomic(
        "same", "Утепление У1 толщиной 50 мм совпадает.",
        before="У1 Утепление минеральной ватой 50 мм",
        after="У1 50 Утепление минеральной ватой 50 мм",
        status="UNCERTAIN", hint="REVIEW",
    )
    summary = combine(
        stage5_summary([removed], category="materials", group_id="sheet-1"),
        stage5_summary([same], stage5_class="REVIEW", group_id="sheet-2"),
    )
    artifact = synthesize(summary)
    assert artifact["high_level_changes"] == []
    counterpart = next(
        item for item in artifact["non_material_review"] if "removed" in item["evidence_ids"]
    )
    assert counterpart["reason"] == "CROSS_SHEET_COUNTERPART"


def test_one_sided_added_object_is_not_automatically_published():
    artifact = synthesize(stage5_summary(
        _one_sided_ai_group()["atomic_evidence"], category="room_composition",
    ))
    assert artifact["high_level_changes"] == []
    assert artifact["material_review"][0]["decision_source"] == "FAIL_CLOSED"


def test_existing_stage5_payload_is_not_mutated():
    summary = stage5_summary([atomic(
        "area", "Площадь изменена.", before="10 м²", after="11 м²", hint_category="areas",
    )], category="areas")
    original = copy.deepcopy(summary)
    synthesize(summary)
    assert summary == original


def test_non_artifact_or_old_run_returns_no_public_stage53_view():
    assert high.public_view(None) is None
    assert high.public_view({"version": 1, "kind": stage5.KIND}) is None


def test_service_evidence_cannot_be_promoted_by_final_validator():
    item = atomic(
        "service", "Изменён заголовок листа.", before="План", after="План этажа",
        hint="SERVICE_STRUCTURE", hint_category="documentation_structure",
    )
    summary = stage5_summary(
        [item], stage5_class="SERVICE_STRUCTURE", category="documentation_structure",
    )
    artifact = synthesize(summary)
    fake = copy.deepcopy(artifact)
    promoted = copy.deepcopy(fake["service_structure_summary"]["items"][0])
    promoted["type"] = "DESIGN_PRINCIPLE_CHANGED"
    promoted["title"] = "Изменён принцип проектного решения."
    fake["high_level_changes"] = [promoted]
    with pytest.raises(high.HighLevelValidationError, match="service_promoted_to_project_change"):
        high.validate_final_artifact(fake, summary)


def test_artifact_contract_is_ready_for_future_graphic_evidence():
    artifact = synthesize(stage5_summary([atomic(
        "area", "Площадь изменена.", before="10 м²", after="11 м²", hint_category="areas",
    )], category="areas"))
    assert artifact["evidence_sources"] == ["TEXT"]
    assert artifact["constraints"]["graphic_evidence_supported_by_contract"] is True
