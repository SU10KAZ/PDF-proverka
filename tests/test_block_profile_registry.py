from __future__ import annotations

import json

from backend.app.pipeline.stages.block_grounding import block_profile_registry
from backend.app.pipeline.stages.block_grounding.block_profile_registry import (
    artifact_path,
    load_prepared_package,
    make_package,
    select_reference,
)


def test_reference_is_selected_by_exact_discipline_and_profile():
    reference = select_reference(
        "hvac_floor_plan", "ОВ", source_kind="structured_hvac"
    )

    assert reference["discipline"] == "ОВ"
    assert reference["profile_id"] == "hvac_floor_plan"
    assert reference["block_id"]
    assert "профил" in reference["selection"]


def test_package_keeps_graph_gate_reference_and_llm_text():
    graph = {
        "profile_id": "vk_floor_plan",
        "nodes": [{"id": "n1", "label": "В1", "node_type": "riser"}],
        "validation": {"nodes_total": 1},
        "readiness": {"complete": True, "reasons": []},
    }
    package = make_package(
        block_id="B-1",
        page=3,
        source_kind="structured_water",
        discipline="ВК",
        user_text="точный текст для модели",
        markdown="# Описание ВК",
        graph=graph,
        gate={"use": True, "complete": True},
    )

    assert package["graph"] is graph
    assert package["profile_id"] == "vk_floor_plan"
    assert package["reference"]["block_id"]
    assert package["user_text"] == "точный текст для модели"
    assert package["gate"]["use"] is True


def test_reference_prefers_matching_subtype_inside_one_profile():
    cctv = select_reference(
        "discipline_floor_plan", "СС", source_kind="structured_alia_scheme",
        graph={"validation": {"subtype": "cctv"}},
        classification={"description": "План размещения камер видеонаблюдения на этаже."},
    )
    trays = select_reference(
        "discipline_floor_plan", "СС", source_kind="structured_alia_scheme",
        graph={"validation": {"subtype": "cable_tray"}},
        classification={"description": "План размещения кабельных лотков слаботочных систем."},
    )

    assert cctv["subtype"] == "cctv"
    assert trays["subtype"] == "cable_tray"
    assert cctv["block_id"] != trays["block_id"]
    assert cctv["selection_mode"] == "dynamic_similarity"
    assert cctv["candidate_count"] > 1


def test_reference_uses_semantics_and_keeps_exact_profile_boundary():
    reference = select_reference(
        "electrical_installation_detail", "ЭОМ", source_kind="structured_electrical",
        graph={"validation": {"subtype": "grounding_welds"}},
        classification={"description": "Узел сварного соединения стальной полосы заземления."},
    )

    assert reference["block_id"] == "9HTP-UNPC-EQV"
    assert reference["profile_id"] == "electrical_installation_detail"
    assert reference["discipline"] == "ЭОМ"
    assert reference["subtype"] == "grounding_welds"
    assert reference["match_factors"]["semantic"] > 0
    assert reference["match_factors"]["subtype"] == 1
    assert "Сравнено эталонов" in reference["explanation"]


def test_door_scheme_prefers_door_view_over_canonical_door_table():
    reference = select_reference(
        "ar_opening_drawing", "АР", source_kind="structured_architecture",
        graph={
            "nodes": (
                [{"node_type": "opening"} for _ in range(20)]
                + [{"node_type": "fire_rating"} for _ in range(6)]
            ),
            "containers": [{}],
            "validation": {
                "subtype": "архитектурный блок",
                "physical_line_segments_total": 1422,
            },
        },
        classification={
            "description": (
                "Фронтальные виды дверных блоков и люков с габаритными размерами, "
                "маркировкой и отметкой чистого пола. Дополнительно показаны разрезы люков."
            )
        },
        current_block_id="9TU6-AR4J-CAP",
    )

    assert reference["block_id"] == "DVTT-3GXK-J4P"
    assert reference["subtype"] == "door_window_sketch"
    assert any(item["block_id"] == "9EXP-KHJC-PMW" for item in reference["alternatives"])


def test_reference_falls_back_to_canonical_without_current_block_features():
    reference = select_reference(
        "hvac_floor_plan", "ОВ", source_kind="structured_hvac"
    )

    assert reference["selection_mode"] == "canonical_profile_fallback"
    assert reference["match_score"] is None
    assert "Недостаточно признаков" in reference["explanation"]


def test_package_survives_dynamic_reference_selection_error(monkeypatch):
    def broken_semantic_comparison(*_args, **_kwargs):
        raise ValueError("повреждённый корпус")

    monkeypatch.setattr(
        block_profile_registry, "_semantic_similarities", broken_semantic_comparison
    )
    package = make_package(
        block_id="SAFE-1",
        page=1,
        source_kind="structured_hvac",
        discipline="ОВ",
        profile_id="hvac_floor_plan",
        classification={"description": "План системы вентиляции"},
        graph={"profile_id": "hvac_floor_plan", "nodes": []},
        user_text="контекст",
    )

    assert package["graph"] is not None
    assert package["reference"]["block_id"]
    assert (
        package["reference"]["selection_mode"]
        == "canonical_fallback_after_selection_error"
    )
    assert "Векторный граф блока сохранён" in package["reference"]["explanation"]


def test_prepared_package_round_trip(tmp_path):
    package = make_package(
        block_id="B/2",
        page=1,
        source_kind="structured_hvac",
        discipline="ОВ",
        profile_id="hvac_floor_plan",
        user_text="контекст",
        graph={"profile_id": "hvac_floor_plan", "nodes": []},
    )
    path = artifact_path(tmp_path, "B/2")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(package, ensure_ascii=False), encoding="utf-8")

    loaded = load_prepared_package(tmp_path, "B/2")

    assert loaded is not None
    assert loaded["block_id"] == "B/2"
    assert loaded["profile_id"] == "hvac_floor_plan"


def test_prepared_package_loads_safe_graph_sidecar(tmp_path):
    package = make_package(
        block_id="GALLERY-1",
        page=1,
        source_kind="structured_hvac",
        discipline="ОВ",
        profile_id="hvac_floor_plan",
        user_text="контекст",
        graph={"profile_id": "hvac_floor_plan", "nodes": [{"id": "n1"}]},
    )
    path = artifact_path(tmp_path, "GALLERY-1")
    sidecar = path.parent / "_graphs" / "GALLERY-1.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(json.dumps(package["graph"], ensure_ascii=False), encoding="utf-8")
    package["graph"] = None
    package["graph_artifact"] = "_graphs/GALLERY-1.json"
    path.write_text(json.dumps(package, ensure_ascii=False), encoding="utf-8")

    loaded = load_prepared_package(tmp_path, "GALLERY-1")

    assert loaded is not None
    assert loaded["graph"]["nodes"] == [{"id": "n1"}]


def test_prepared_package_rejects_graph_sidecar_outside_artifact_dir(tmp_path):
    package = make_package(
        block_id="GALLERY-2",
        page=1,
        source_kind="structured_hvac",
        discipline="ОВ",
        profile_id="hvac_floor_plan",
        user_text="контекст",
        graph={"profile_id": "hvac_floor_plan", "nodes": []},
    )
    path = artifact_path(tmp_path, "GALLERY-2")
    path.parent.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(package["graph"]), encoding="utf-8")
    package["graph"] = None
    package["graph_artifact"] = "../outside.json"
    path.write_text(json.dumps(package), encoding="utf-8")

    assert load_prepared_package(tmp_path, "GALLERY-2") is None
