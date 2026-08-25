"""G2.4.2 deterministic TEXT entity ↔ GRAPHIC entity bridge checks."""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import high_level_project_changes as high
from backend.app.services.stage_comparison.unified_entity_bridge.entity_bridge import (
    BRIDGE_VERSION,
    KIND,
    SCHEMA_VERSION,
    BridgeValidationError,
    build_entity_links,
    graphic_entities_from_system_graph,
    schema_path,
    validate_entity_links,
)
from backend.app.services.stage_comparison.unified_entity_bridge.entity_normalizer import (
    NORMALIZER_VERSION,
    normalize_entity_name,
)


ROOT = Path(__file__).resolve().parents[1]
RIGHT_GRAPH_PATH = ROOT / "experiments/g2_dense_sectioned_board/right_system_graph.json"
STAGE53_PATH = (
    ROOT
    / "comparison/sessions/121d764109184c13/pairs/p26c08b83a6"
    / "high_level_project_changes.json"
)
AR_STAGE53_PATH = (
    ROOT
    / "comparison/sessions/121d764109184c13/pairs/p570d156f57"
    / "high_level_project_changes.json"
)
REFERENCE_LINKS_PATH = (
    ROOT
    / "backend/app/services/stage_comparison/unified_entity_bridge/entity_links.json"
)


def _text(entity_id: str, name: str, **overrides) -> dict:
    payload = {
        "id": entity_id,
        "name": name,
        "normalized_name": overrides.pop("normalized_name", None),
        "type": overrides.pop("type", "EQUIPMENT"),
        "sheet": overrides.pop("sheet", "ЭОМ-1"),
        "page": overrides.pop("page", 1),
        "fragments": overrides.pop("fragments", [f"fragment-{entity_id}"]),
        "values": overrides.pop("values", {}),
    }
    payload.update(overrides)
    return payload


def _graphic(
    entity_id: str,
    *,
    canonical_identity: str | None = None,
    labels: list[str] | None = None,
    node_type: str = "LOAD",
    attributes: dict | None = None,
    graph_scope: dict | None = None,
    **overrides,
) -> dict:
    payload = {
        "id": entity_id,
        "node_type": node_type,
        "canonical_identity": canonical_identity,
        "labels": labels or [],
        "attributes": attributes or {},
        "evidence": [{"kind": "token", "source": "controlled-test"}],
        "graph_scope": graph_scope or {},
    }
    payload.update(overrides)
    return payload


def _single_link(text: dict, graphic: dict) -> dict:
    result = build_entity_links([text], [graphic])
    assert validate_entity_links(result) is result
    assert len(result["links"]) == 1
    return result["links"][0]


def test_normalizer_preserves_original_and_unifies_supported_designations():
    originals = ["ВРУ-А", "ВРУ А", "VRU-A"]
    normalized = [normalize_entity_name(value) for value in originals]

    assert [item["original"] for item in normalized] == originals
    assert {item["canonical"] for item in normalized} == {"VRU_A"}
    assert normalize_entity_name("ЩР-1")["canonical"] == "PANEL_1"
    assert normalize_entity_name("ЩР1")["canonical"] == "PANEL_1"
    assert normalize_entity_name("Помещение 101")["canonical"] == "ROOM_101"
    assert normalize_entity_name("ROOM-101")["canonical"] == "ROOM_101"
    assert normalize_entity_name("вручную")["canonical"] == "ВРУЧНУЮ"
    assert normalize_entity_name("ВРУ автостоянки")["canonical"] == (
        "ВРУ_АВТОСТОЯНКИ"
    )


def test_exact_canonical_identity_vru_is_high():
    link = _single_link(
        _text("text-vru-a", "ВРУ-А"),
        _graphic("node-vru-a", canonical_identity="VRU-A"),
    )

    assert link["relation"] == "SAME_ENTITY"
    assert link["confidence"] == "HIGH"
    assert link["evidence"][0]["rule"] == "EXACT_CANONICAL_IDENTITY_MATCH"
    assert {item["canonical"] for item in link["evidence"][0]["normalization"]} == {
        "VRU_A"
    }


def test_normalized_designation_panel_is_high():
    link = _single_link(
        _text("text-panel-1", "ЩР-1"),
        _graphic("node-panel-1", labels=["ЩР1"]),
    )

    assert link["relation"] == "SAME_ENTITY"
    assert link["confidence"] == "HIGH"
    assert link["evidence"][0]["rule"] == "NORMALIZED_DESIGNATION_MATCH"


def test_local_qf_without_context_is_unknown():
    link = _single_link(
        _text("text-qf1", "QF1", type="OUTGOING_DEVICE"),
        _graphic(
            "node-qf1",
            canonical_identity="QF1",
            node_type="OUTGOING_DEVICE",
        ),
    )

    assert link["relation"] == "UNKNOWN"
    assert link["confidence"] == "UNKNOWN"
    assert link["evidence"][0]["rule"] == "LOCAL_DESIGNATION_REQUIRES_CONTEXT"


def test_local_qf_with_parent_context_is_high():
    link = _single_link(
        _text(
            "text-qf1",
            "QF1",
            type="OUTGOING_DEVICE",
            values={"parent_group": "BUS1"},
        ),
        _graphic(
            "node-qf1",
            canonical_identity="QF1",
            node_type="OUTGOING_DEVICE",
            attributes={"section": "BUS1"},
        ),
    )

    assert link["relation"] == "SAME_ENTITY"
    assert link["confidence"] == "HIGH"
    assert link["evidence"][0]["rule"] == "DESIGNATION_CONTEXT_MATCH"
    assert link["evidence"][0]["context"] == {"parent_group": ["BUS_1"]}


def test_fire_pump_requires_explicit_role_and_remains_possible():
    text = _text(
        "text-fire-pump",
        "Насос пожарный",
        values={"functional_role": "пожарный насос"},
    )
    graphic = _graphic(
        "node-fire-pump",
        canonical_identity="FIRE_PUMP",
        attributes={"functional_role": "FIRE_PUMP"},
    )

    link = _single_link(text, graphic)
    without_role = build_entity_links(
        [_text("text-fire-pump", "Насос пожарный")], [graphic]
    )

    assert link["relation"] == "POSSIBLE_ENTITY"
    assert link["confidence"] == "MEDIUM"
    assert link["evidence"][0]["rule"] == "FUNCTIONAL_ROLE_MATCH"
    assert without_role["links"] == []
    assert without_role["diagnostics"]["unresolved_text_entity_ids"] == [
        "text-fire-pump"
    ]


def test_functional_role_cannot_override_designation_conflict():
    link = _single_link(
        _text(
            "text-vru-a",
            "ВРУ-А",
            values={"functional_role": "FIRE_PUMP"},
        ),
        _graphic(
            "node-vru-b",
            canonical_identity="ВРУ-Б",
            attributes={"functional_role": "FIRE_PUMP"},
        ),
    )

    assert link["relation"] == "UNKNOWN"
    assert link["evidence"][0]["rule"] == "CANONICAL_IDENTITY_CONFLICT"


def test_conflicting_vru_designations_are_unknown_not_same():
    link = _single_link(
        _text("text-vru-a", "ВРУ-А"),
        _graphic("node-vru-b", canonical_identity="ВРУ-Б"),
    )

    assert link["relation"] == "UNKNOWN"
    assert link["evidence"][0]["rule"] == "CANONICAL_IDENTITY_CONFLICT"
    assert link["evidence"][0]["outcome"] == "CONFLICT"


def test_one_text_to_two_graph_nodes_does_not_choose_first():
    result = build_entity_links(
        [_text("text-vru-a", "ВРУ-А")],
        [
            _graphic("node-vru-a-1", canonical_identity="VRU-A"),
            _graphic("node-vru-a-2", canonical_identity="VRU-A"),
        ],
    )

    assert len(result["links"]) == 2
    assert {link["relation"] for link in result["links"]} == {"UNKNOWN"}
    assert {
        link["evidence"][-1]["context"]["direction"] for link in result["links"]
    } == {"ONE_TEXT_TO_MULTIPLE_GRAPHIC"}


def test_two_text_entities_to_one_graph_node_are_unknown():
    result = build_entity_links(
        [
            _text("text-vru-a-primary", "ВРУ-А"),
            _text("text-vru-a-duplicate", "VRU A"),
        ],
        [_graphic("node-vru-a", canonical_identity="VRU-A")],
    )

    assert len(result["links"]) == 2
    assert {link["relation"] for link in result["links"]} == {"UNKNOWN"}
    assert {
        link["evidence"][-1]["context"]["direction"] for link in result["links"]
    } == {"MULTIPLE_TEXT_TO_ONE_GRAPHIC"}


def test_same_page_and_bbox_are_not_identity_evidence():
    result = build_entity_links(
        [_text("text-vru-a", "ВРУ-А", page=7, bbox=[1, 2, 3, 4])],
        [
            _graphic(
                "node-panel-1",
                canonical_identity="PANEL-1",
                graph_scope={"page_index": 7},
                bbox=[1, 2, 3, 4],
            )
        ],
    )

    assert result["links"] == []
    assert result["diagnostics"]["unresolved_text_entity_ids"] == ["text-vru-a"]


def test_context_and_node_type_conflicts_fail_closed():
    context_link = _single_link(
        _text("text-vru-a", "ВРУ-А", values={"system": "SYS-A"}),
        _graphic(
            "node-vru-a",
            canonical_identity="VRU-A",
            attributes={"system": "SYS-B"},
        ),
    )
    type_link = _single_link(
        _text("text-load", "ВРУ-А", type="LOAD"),
        _graphic(
            "node-outgoing",
            canonical_identity="VRU-A",
            node_type="OUTGOING_DEVICE",
        ),
    )

    assert context_link["relation"] == type_link["relation"] == "UNKNOWN"
    assert context_link["evidence"][0]["rule"] == "CONTEXT_CONFLICT"
    assert type_link["evidence"][0]["rule"] == "ENTITY_TYPE_CONFLICT"


def test_different_sheet_is_not_an_identity_conflict():
    link = _single_link(
        _text("text-vru-a", "ВРУ-А", sheet="П-ЭОМ-7"),
        _graphic(
            "node-vru-a",
            canonical_identity="VRU-A",
            graph_scope={"sheet": "РД-ЭОМ-29"},
        ),
    )

    assert link["relation"] == "SAME_ENTITY"
    assert link["confidence"] == "HIGH"


def test_contract_schema_and_diagnostics_are_versioned_and_strict():
    schema = json.loads(schema_path().read_text(encoding="utf-8"))
    result = build_entity_links(
        [_text("text-vru-a", "ВРУ-А")],
        [_graphic("node-vru-a", canonical_identity="VRU-A")],
    )

    assert schema["title"] == "Deterministic TEXT/GRAPHIC Entity Links"
    assert schema["properties"]["schema_version"]["const"] == SCHEMA_VERSION
    assert result["schema_version"] == SCHEMA_VERSION
    assert result["kind"] == KIND
    assert result["bridge_version"] == BRIDGE_VERSION
    assert result["normalizer_version"] == NORMALIZER_VERSION
    assert result["diagnostics"]["relation_counts"] == {
        "SAME_ENTITY": 1,
        "POSSIBLE_ENTITY": 0,
        "UNKNOWN": 0,
    }

    invalid = copy.deepcopy(result)
    invalid["diagnostics"]["relation_counts"]["SAME_ENTITY"] = 0
    with pytest.raises(BridgeValidationError, match="diagnostics"):
        validate_entity_links(invalid)


def test_reference_entity_links_artifact_validates():
    artifact = json.loads(REFERENCE_LINKS_PATH.read_text(encoding="utf-8"))

    assert validate_entity_links(artifact) is artifact
    assert artifact["diagnostics"]["confidence_counts"]["HIGH"] == 3
    assert {link["text_entity_id"] for link in artifact["links"]} == {
        "text-vru-a",
        "text-panel-1",
        "text-room-101",
    }


def test_matcher_and_system_graph_adapter_do_not_mutate_inputs():
    graph = json.loads(RIGHT_GRAPH_PATH.read_text(encoding="utf-8"))
    original_graph = copy.deepcopy(graph)
    graphic_entities = graphic_entities_from_system_graph(graph)
    original_entities = copy.deepcopy(graphic_entities)
    text_entities = [_text("text-vru-a", "ВРУ-А", type="LOAD")]
    original_text = copy.deepcopy(text_entities)

    build_entity_links(text_entities, graphic_entities)

    assert graph == original_graph
    assert graphic_entities == original_entities
    assert text_entities == original_text
    assert len(graphic_entities) == len(graph["nodes"])
    assert graphic_entities[0]["graph_scope"]["schema_version"] == "system-graph.v1"


def test_real_ios_entities_have_no_high_link_in_current_grsh_graph():
    graph = json.loads(RIGHT_GRAPH_PATH.read_text(encoding="utf-8"))
    graphic_entities = graphic_entities_from_system_graph(graph)
    # The spelling and location are literal Stage 5.3 evidence from p26c08b83a6.
    text_entities = [
        _text(
            "stage53-vru-a",
            "ВРУ-А",
            type="LOAD",
            sheet="Лист 1 — Содержание тома",
            page=4,
            fragments=["txt_f31d470d8a5b17da", "txt_b6c4adf81e6b0f74"],
        ),
        _text(
            "stage53-panel-6",
            "ЩР-6",
            type="PANEL",
            sheet="Лист 1 — Содержание тома",
            page=4,
            fragments=["txt_63a519a22fad5c82"],
        ),
    ]

    result = build_entity_links(text_entities, graphic_entities)

    assert result["diagnostics"]["confidence_counts"]["HIGH"] == 0
    assert result["diagnostics"]["confidence_counts"]["UNKNOWN"] == 2
    assert result["diagnostics"]["unresolved_text_entity_ids"] == [
        "stage53-panel-6",
        "stage53-vru-a",
    ]
    assert {link["graphic_entity_id"] for link in result["links"]} == {
        "LOAD:1QF8@909",
        "LOAD:2QF5@1550",
    }


def test_real_ar_room_designations_are_not_paired_without_ar_graph_entities():
    artifact = json.loads(AR_STAGE53_PATH.read_text(encoding="utf-8"))
    details = []
    for bucket in (
        "high_level_changes",
        "detail_level_increased",
        "material_review",
        "non_material_review",
    ):
        for change in artifact.get(bucket, []):
            details.extend(change.get("details", []))
    evidence_text = "\n".join(
        "\n".join(str(detail.get(key) or "") for key in ("summary", "before", "after"))
        for detail in details
    )
    room_ids = sorted(
        set(re.findall(r"(?i)помещение\s+([0-9]+(?:[.][а-яa-z0-9]+)*)", evidence_text))
    )
    text_entities = [
        _text(f"stage53-room-{room_id}", f"Помещение {room_id}", type="ROOM")
        for room_id in room_ids
    ]

    result = build_entity_links(text_entities, [])

    assert len(room_ids) == 26
    assert result["links"] == []
    assert result["diagnostics"]["text_entity_count"] == 26
    assert result["diagnostics"]["confidence_counts"]["UNKNOWN"] == 0
    assert len(result["diagnostics"]["unresolved_text_entity_ids"]) == 26


def test_real_grsh_qf1_without_parent_context_is_unknown():
    graph = json.loads(RIGHT_GRAPH_PATH.read_text(encoding="utf-8"))
    result = build_entity_links(
        [_text("grsh-qf1", "QF1", type="INPUT_DEVICE")],
        graphic_entities_from_system_graph(graph),
    )

    assert len(result["links"]) == 1
    assert result["links"][0]["graphic_entity_id"] == "INPUT1"
    assert result["links"][0]["relation"] == "UNKNOWN"
    assert result["links"][0]["evidence"][0]["rule"] == (
        "LOCAL_DESIGNATION_REQUIRES_CONTEXT"
    )


def test_old_stage53_artifact_opens_unchanged_after_bridge_use():
    artifact = json.loads(STAGE53_PATH.read_text(encoding="utf-8"))
    original = copy.deepcopy(artifact)

    public = high.public_view(artifact)
    build_entity_links(
        [_text("text-vru-a", "ВРУ-А")],
        [_graphic("node-vru-a", canonical_identity="VRU-A")],
    )

    assert public is not None
    assert public["schema_version"] == "1.0"
    assert public["kind"] == high.KIND
    assert artifact == original
