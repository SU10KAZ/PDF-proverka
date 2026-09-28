from backend.app.services.section_optimization_passport_service import build_engineering_passport


def test_passport_preserves_action_scope_parameters_and_constraints():
    passport = build_engineering_passport(
        {"title": "Тиражировать крепление", "representative_proposal": "Применить типовой кронштейн"},
        [{
            "source_ref": "P1:OPT-1",
            "project_id": "P1",
            "version_id": "v1",
            "current": "Индивидуальное крепление",
            "proposed": "Типовой кронштейн",
            "risks": "Проверить основание",
            "norm": "СП 1",
        }],
        [{
            "project_id": "P2",
            "project_name": "Корпус 2",
            "version_id": "v2",
            "rows": [{
                "row_id": "ROW-2",
                "page": 8,
                "sheet": "КМ-8",
                "name": "Кронштейн",
                "type_mark": "КР-1",
                "quantity": "12",
                "mass": "3,5",
            }],
        }],
    )

    assert passport["action"] == "Применить типовой кронштейн"
    assert passport["scope"] == {"source_projects": ["P1"], "target_projects": ["P2"]}
    assert passport["subjects"][0]["type_mark"] == "КР-1"
    assert passport["targets"][0]["rows"][0]["parameters"]["mass"] == "3,5"
    assert {item["kind"] for item in passport["constraints"]} == {"risk", "norm"}
    assert "unit" in passport["targets"][0]["rows"][0]["missing_parameters"]
