import pytest

from backend.app.services.section_optimization_dependency_service import build_dependency_assessment, update_dependency_interface


def test_dependency_assessment_keeps_matching_interfaces_unknown_until_evidenced():
    result = build_dependency_assessment({
        "action": "Заменить кабель и тип крепления лотка",
        "subjects": [{"name": "Кабель силовой"}],
    })

    by_id = {item["interface_id"]: item for item in result["interfaces"]}
    assert {"power_supply", "protection", "automation", "fire_safety", "supports", "penetrations", "access"} <= set(by_id)
    assert all(item["status"] == "unknown" for item in by_id.values())
    assert all(item["required_evidence"] for item in by_id.values())


def test_dependency_assessment_does_not_treat_unclassified_as_no_impact():
    result = build_dependency_assessment({"action": "Унифицировать изделие", "subjects": []})

    assert result["interfaces"] == [{
        "interface_id": "adjacent_systems",
        "title": "Интерфейсы со смежными системами",
        "status": "unknown",
        "relation": "impact_not_classified",
        "required_evidence": "Указать затрагиваемые системы или обосновать отсутствие влияния",
        "basis": "Тип изменения пока не классифицирован",
    }]


def test_dependency_resolution_requires_evidence_and_keeps_other_unknowns():
    assessment = build_dependency_assessment({"action": "Заменить кабель", "subjects": []})
    with pytest.raises(ValueError, match="доказательство"):
        update_dependency_interface(assessment, "power_supply", "no_impact", [])
    updated = update_dependency_interface(
        assessment, "power_supply", "confirmed_impact", ["P2:v3:однолинейная схема"], "Пересчитать нагрузку",
    )
    assert next(item for item in updated["interfaces"] if item["interface_id"] == "power_supply")["status"] == "confirmed_impact"
    assert updated["status"] == "requires_review"
