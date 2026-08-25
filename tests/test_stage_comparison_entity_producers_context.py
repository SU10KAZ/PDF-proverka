"""G2.4.3 TEXT_ENTITIES, GRAPH_ENTITIES, and artifact bridge checks."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION as SYSTEM_GRAPH_SCHEMA_VERSION,
    make_edge,
    make_node,
)
from backend.app.services.stage_comparison.unified_entity_bridge.entity_bridge import (
    ARTIFACT_SCHEMA_VERSION,
    BridgeValidationError,
    build_entity_links_from_artifacts,
    entity_links_are_stale,
    validate_entity_links_artifact,
)
from backend.app.services.stage_comparison.unified_entity_bridge.graph_entity_adapter import (
    ADAPTER_VERSION,
    build_graph_entities,
    is_stale as graph_entities_are_stale,
    schema_path as graph_schema_path,
    validate_graph_entities,
)
from backend.app.services.stage_comparison.unified_entity_bridge.text_entity_producer import (
    PRODUCER_VERSION,
    build_text_entities,
    is_stale as text_entities_are_stale,
    schema_path as text_schema_path,
    validate_text_entities,
)


ROOT = Path(__file__).resolve().parents[1]
RIGHT_GRAPH_PATH = ROOT / "experiments/g2_dense_sectioned_board/right_system_graph.json"
IOS_STAGE53_PATH = (
    ROOT
    / "comparison/sessions/121d764109184c13/pairs/p26c08b83a6"
    / "high_level_project_changes.json"
)
AR_STAGE53_PATH = (
    ROOT
    / "comparison/sessions/121d764109184c13/pairs/p570d156f57"
    / "high_level_project_changes.json"
)


def _detail(evidence_id: str, text: str, **overrides) -> dict:
    payload = {
        "evidence_id": evidence_id,
        "summary": text,
        "before": None,
        "after": None,
        "stage5_title": text,
        "group_id": "sheet-1",
        "left_pages": [1],
        "right_pages": [2],
        "left_fragment_ids": [f"left-{evidence_id}"],
        "right_fragment_ids": [f"right-{evidence_id}"],
    }
    payload.update(overrides)
    return payload


def _stage53(*details: dict, pair_id: str = "pair-1") -> dict:
    change = {
        "change_id": "change-1",
        "sheet_groups": ["sheet-1"],
        "details": list(details),
    }
    return {
        "version": 1,
        "schema_version": "1.0",
        "kind": "stage_comparison_high_level_project_changes",
        "pair_id": pair_id,
        "source_signature": "stage53-source-signature",
        "high_level_changes": [change],
        "detail_level_increased": [],
        "material_review": [],
        "non_material_review": [],
        "unresolved": [],
        "service_structure_summary": {"items": []},
    }


def _node(node_id: str, node_type: str, label: str, **overrides) -> dict:
    return make_node(
        node_id,
        node_type,
        confidence=overrides.pop("confidence", 0.95),
        evidence=[
            {
                "kind": "token",
                "role": "identity",
                "value": label,
                "source_tokens": [label],
            }
        ],
        bbox=overrides.pop("bbox", [1, 2, 3, 4]),
        source_tokens=[label],
        label=label,
        **overrides,
    )


def _edge(edge_id: str, edge_type: str, source: str, target: str) -> dict:
    return make_edge(
        edge_id,
        edge_type,
        source,
        target,
        confidence=0.95,
        evidence=[
            {
                "kind": "relation",
                "role": edge_type,
                "source_tokens": [source, target],
            }
        ],
        source_bbox=[1, 2, 3, 4],
        target_bbox=[1, 2, 3, 4],
        source_tokens=[source, target],
    )


def _graph(nodes: list[dict], edges: list[dict] | None = None, **overrides) -> dict:
    payload = {
        "schema_version": SYSTEM_GRAPH_SCHEMA_VERSION,
        "profile_id": "controlled",
        "block": {"block_id": "block-1", "page_index": 7},
        "discipline": "EOM",
        "profile": {"id": "controlled"},
        "nodes": nodes,
        "edges": edges or [],
        "quality": {"backbone_recovered": True},
        "provenance": {
            "profile": "controlled",
            "profile_version": "controlled-v1",
            "vector_evidence": {"extraction_version": "vector-evidence-v1"},
        },
    }
    payload.update(overrides)
    return payload


def _text_by_name(artifact: dict, canonical_name: str) -> list[dict]:
    return [
        entity
        for entity in artifact["entities"]
        if entity["canonical_name"] == canonical_name
    ]


def test_text_producer_builds_vru_deterministically_and_preserves_evidence():
    source = _stage53(
        _detail("ev-1", "Изменена схема ВРУ-А."),
        _detail("ev-2", "ВРУ А сохранено в перечне."),
    )

    first = build_text_entities(source)
    second = build_text_entities(copy.deepcopy(source))
    entities = _text_by_name(first, "VRU_A")

    assert validate_text_entities(first) is first
    assert first == second
    assert len(entities) == 1
    assert entities[0]["evidence_ids"] == ["ev-1", "ev-2"]
    assert entities[0]["fragment_ids"] == [
        "left-ev-1",
        "left-ev-2",
        "right-ev-1",
        "right-ev-2",
    ]
    assert entities[0]["entity_id"].startswith("txt_ent_")
    assert first["producer_version"] == PRODUCER_VERSION
    assert first["quality_report"]["duplicated"] >= 1


def test_text_producer_drops_service_words_and_does_not_create_every_token():
    source = _stage53(_detail("ev-service", "Проект, этаж, лист и система."))
    artifact = build_text_entities(
        source,
        {
            "ev-service": {
                "entities": ["проект", "этаж", "лист", "система"]
            }
        },
    )

    assert artifact["entities"] == []
    assert artifact["quality_report"]["total_source_candidates"] == 4
    assert artifact["quality_report"]["dropped_noise"] == 4


def test_text_producer_keeps_same_name_separate_for_different_context():
    source = _stage53(
        _detail("ev-s1", "QF1 изменён."),
        _detail("ev-s2", "QF1 изменён повторно."),
    )
    artifact = build_text_entities(
        source,
        {
            "ev-s1": {"section": "SECTION_1"},
            "ev-s2": {"section": "SECTION_2"},
        },
    )

    qf1 = _text_by_name(artifact, "QF_1")
    assert len(qf1) == 2
    assert {entity["parent_context"]["section"] for entity in qf1} == {
        "SECTION_1",
        "SECTION_2",
    }
    assert artifact["quality_report"]["unresolved_context"] == 0


def test_text_producer_source_signature_marks_changed_stage53_stale():
    source = _stage53(_detail("ev-1", "ВРУ-А изменено."))
    artifact = build_text_entities(source)
    changed = copy.deepcopy(source)
    changed["high_level_changes"][0]["details"][0]["summary"] = "ВРУ-Б изменено."

    assert text_entities_are_stale(artifact, source) is False
    assert text_entities_are_stale(artifact, changed) is True

    index = {"ev-1": {"section": "S1"}}
    indexed_artifact = build_text_entities(source, index)
    assert text_entities_are_stale(indexed_artifact, source, index) is False
    assert text_entities_are_stale(
        indexed_artifact, source, {"ev-1": {"section": "S2"}}
    ) is True


def test_graph_adapter_one_node_produces_one_entity_with_provenance():
    graph = _graph(
        [_node("panel-1", "LOAD", "VRU-A", canonical_identity="VRU-A", section="S1")]
    )
    artifact = build_graph_entities([graph])
    entity = artifact["entities"][0]

    assert validate_graph_entities(artifact) is artifact
    assert artifact["adapter_version"] == ADAPTER_VERSION
    assert artifact["quality_report"]["source_nodes"] == 1
    assert artifact["quality_report"]["produced_entities"] == 1
    assert entity["canonical_name"] == "VRU_A"
    assert entity["graph_node_ids"] == ["panel-1"]
    assert entity["evidence_refs"] == [{"kind": "NODE", "id": "panel-1"}]
    assert entity["provenance"]["graph_digest"] == artifact["source_graphs"][0]["graph_digest"]


def test_graph_adapter_merges_only_explicit_representation_pair():
    nodes = [
        _node("out-1", "OUTGOING_DEVICE", "1QF8", canonical_identity="VRU-A", section="S1"),
        _node("load-1", "LOAD", "VRU-A", canonical_identity="VRU-A", section="S1"),
    ]
    edge = _edge("terminal-1", "TERMINATES_AT", "out-1", "load-1")
    artifact = build_graph_entities([_graph(nodes, [edge])])

    assert artifact["quality_report"]["source_nodes"] == 2
    assert artifact["quality_report"]["produced_entities"] == 1
    assert artifact["quality_report"]["duplicated"] == 1
    assert artifact["entities"][0]["graph_node_ids"] == ["load-1", "out-1"]
    assert artifact["entities"][0]["provenance"]["aggregation_rule"] == (
        "TERMINATES_AT_REPRESENTATION_PAIR"
    )
    assert "terminal-1" in artifact["entities"][0]["edge_ids"]


def test_graph_adapter_never_merges_same_label_across_section_or_role():
    different_sections = _graph(
        [
            _node("load-s1", "LOAD", "QF1", canonical_identity="QF1", section="S1"),
            _node("load-s2", "LOAD", "QF1", canonical_identity="QF1", section="S2"),
        ]
    )
    different_roles_without_relation = _graph(
        [
            _node("out-s1", "OUTGOING_DEVICE", "VRU-A", canonical_identity="VRU-A", section="S1"),
            _node("load-s1", "LOAD", "VRU-A", canonical_identity="VRU-A", section="S1"),
        ]
    )

    section_artifact = build_graph_entities([different_sections])
    role_artifact = build_graph_entities([different_roles_without_relation])

    assert len(section_artifact["entities"]) == 2
    assert {entity["section_context"] for entity in section_artifact["entities"]} == {
        "S1",
        "S2",
    }
    assert len(role_artifact["entities"]) == 2
    assert role_artifact["quality_report"]["duplicated"] == 0


def test_invalid_graph_fails_closed_to_no_high_entity():
    invalid = _graph([_node("node-1", "LOAD", "VRU-A")])
    invalid["schema_version"] = "unsupported"

    artifact = build_graph_entities([invalid])

    assert artifact["entities"] == []
    assert artifact["quality_report"]["invalid_source_graphs"] == 1
    assert artifact["quality_report"]["dropped_noise"] == 1


def test_graph_adapter_signature_includes_graph_and_profile_versions():
    graph = _graph([_node("node-1", "LOAD", "VRU-A")])
    artifact = build_graph_entities([graph])
    changed = copy.deepcopy(graph)
    changed["provenance"]["profile_version"] = "controlled-v2"

    assert graph_entities_are_stale(artifact, [graph]) is False
    assert graph_entities_are_stale(artifact, [changed]) is True


def test_artifact_bridge_vru_alias_is_high_and_signatures_are_preserved():
    text = build_text_entities(_stage53(_detail("ev-vru", "ВРУ-А изменено.")))
    graph_source = _graph(
        [_node("vru", "LOAD", "VRU-A", canonical_identity="VRU-A", section="S1")]
    )
    graphic = build_graph_entities([graph_source])

    result = build_entity_links_from_artifacts(text, graphic)

    assert validate_entity_links_artifact(result) is result
    assert result["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert result["source_signatures"] == {
        "text_entities": text["source_signature"],
        "graph_entities": graphic["source_signature"],
    }
    assert result["diagnostics"]["confidence_counts"]["HIGH"] == 1
    assert result["links"][0]["relation"] == "SAME_ENTITY"
    assert entity_links_are_stale(result, text, graphic) is False


def test_artifact_bridge_panel_separator_alias_is_high():
    text = build_text_entities(_stage53(_detail("ev-panel", "ЩР-1 изменён.")))
    graphic = build_graph_entities(
        [_graph([_node("panel-1", "LOAD", "ЩР1", section="S1")])]
    )

    result = build_entity_links_from_artifacts(text, graphic)

    assert len(result["links"]) == 1
    assert result["links"][0]["relation"] == "SAME_ENTITY"
    assert result["links"][0]["confidence"] == "HIGH"


def test_artifact_bridge_qf_requires_unique_context_and_cardinality():
    source = _stage53(_detail("ev-qf", "QF1 изменён."))
    without_context = build_text_entities(source)
    with_context = build_text_entities(
        source, {"ev-qf": {"system": "EOM", "section": "S1"}}
    )
    one_graph = build_graph_entities(
        [_graph([_node("qf-s1", "INPUT_DEVICE", "QF1", canonical_identity="QF1", section="S1")])]
    )
    two_graphs = build_graph_entities(
        [
            _graph(
                [
                    _node("qf-s1", "INPUT_DEVICE", "QF1", canonical_identity="QF1", section="S1"),
                    _node("qf-s2", "INPUT_DEVICE", "QF1", canonical_identity="QF1", section="S2"),
                ]
            )
        ]
    )

    unknown = build_entity_links_from_artifacts(without_context, one_graph)
    high = build_entity_links_from_artifacts(with_context, one_graph)
    ambiguous = build_entity_links_from_artifacts(without_context, two_graphs)

    assert unknown["links"][0]["relation"] == "UNKNOWN"
    assert high["links"][0]["relation"] == "SAME_ENTITY"
    assert high["links"][0]["confidence"] == "HIGH"
    assert len(ambiguous["links"]) == 2
    assert {link["relation"] for link in ambiguous["links"]} == {"UNKNOWN"}


def test_artifact_bridge_conflict_missing_graph_and_text_only_rooms_fail_closed():
    text = build_text_entities(
        _stage53(
            _detail("ev-vru", "ВРУ-А изменено."),
            _detail("ev-room", "Помещение 101 добавлено."),
        )
    )
    conflict_graph = build_graph_entities(
        [_graph([_node("vru-b", "LOAD", "ВРУ-Б", canonical_identity="ВРУ-Б")])]
    )
    empty_graph = build_graph_entities([])

    conflict = build_entity_links_from_artifacts(text, conflict_graph)
    text_only = build_entity_links_from_artifacts(text, empty_graph)

    assert conflict["links"][0]["relation"] == "UNKNOWN"
    assert conflict["links"][0]["evidence"][0]["rule"] == (
        "CANONICAL_IDENTITY_CONFLICT"
    )
    assert text_only["links"] == []
    assert len(text_only["diagnostics"]["unresolved_text_entity_ids"]) == 2
    assert text_only["diagnostics"]["confidence_counts"]["HIGH"] == 0


def test_artifact_bridge_rejects_stale_sources_instead_of_reusing_links():
    source = _stage53(_detail("ev-vru", "ВРУ-А изменено."))
    text = build_text_entities(source)
    graph_source = _graph([_node("vru", "LOAD", "VRU-A", canonical_identity="VRU-A")])
    graphic = build_graph_entities([graph_source])
    changed_source = copy.deepcopy(source)
    changed_source["source_signature"] = "new-stage53-signature"

    with pytest.raises(BridgeValidationError, match="TEXT_ENTITIES stale"):
        build_entity_links_from_artifacts(
            text,
            graphic,
            current_stage53_artifact=changed_source,
            current_system_graphs=[graph_source],
        )


def test_entity_artifact_schemas_are_versioned():
    text_schema = json.loads(text_schema_path().read_text(encoding="utf-8"))
    graph_schema = json.loads(graph_schema_path().read_text(encoding="utf-8"))

    assert text_schema["properties"]["schema_version"]["const"] == "text-entities.v1"
    assert graph_schema["properties"]["schema_version"]["const"] == "graph-entities.v2"


def test_graph_entity_ids_ignore_node_and_edge_array_order():
    graph = _graph(
        [
            _node("section", "BUS_SECTION", "РП1", canonical_identity="SECTION_1"),
            _node("load", "LOAD", "ЩР1", canonical_identity="PANEL_1", section="section"),
        ],
        [_edge("feed", "FEEDS", "section", "load")],
    )
    permuted = copy.deepcopy(graph)
    permuted["nodes"].reverse()
    permuted["edges"].reverse()

    first = build_graph_entities([graph])
    second = build_graph_entities([permuted])
    first_ids = {
        tuple(entity["graph_node_ids"]): entity["entity_id"]
        for entity in first["entities"]
    }
    second_ids = {
        tuple(entity["graph_node_ids"]): entity["entity_id"]
        for entity in second["entities"]
    }

    assert first_ids == second_ids
    assert first["source_signature"] != second["source_signature"]
    section = next(
        entity for entity in first["entities"] if entity["graph_node_ids"] == ["section"]
    )
    assert section["external_connections"] == [
        {
            "edge_id": "feed",
            "edge_type": "FEEDS",
            "direction": "OUTGOING",
            "neighbour_node_id": "load",
        }
    ]


def test_real_ios_vru_a_deduplicates_representations_but_stays_ambiguous():
    stage53 = json.loads(IOS_STAGE53_PATH.read_text(encoding="utf-8"))
    system_graph = json.loads(RIGHT_GRAPH_PATH.read_text(encoding="utf-8"))
    text = build_text_entities(stage53)
    graphic = build_graph_entities([system_graph])
    result = build_entity_links_from_artifacts(text, graphic)
    text_vru_a = _text_by_name(text, "VRU_A")
    graph_vru_a = [
        entity for entity in graphic["entities"] if entity["canonical_name"] == "VRU_A"
    ]
    vru_links = [
        link
        for link in result["links"]
        if link["text_entity_id"] == text_vru_a[0]["entity_id"]
    ]
    panel_6 = _text_by_name(text, "PANEL_6")[0]

    assert text["quality_report"] == {
        "source_evidence": 96,
        "total_source_candidates": 46,
        "produced_entities": 19,
        "dropped_noise": 0,
        "ambiguous": 0,
        "duplicated": 27,
        "unresolved_context": 0,
    }
    assert graphic["quality_report"]["source_nodes"] == 73
    assert graphic["quality_report"]["produced_entities"] == 52
    assert graphic["quality_report"]["duplicated"] == 21
    assert len(text_vru_a) == 1
    assert len(graph_vru_a) == 2
    assert all(len(entity["graph_node_ids"]) == 2 for entity in graph_vru_a)
    assert len(vru_links) == 2
    assert {link["relation"] for link in vru_links} == {"UNKNOWN"}
    assert panel_6["entity_id"] in result["diagnostics"]["unresolved_text_entity_ids"]
    assert not any(link["text_entity_id"] == panel_6["entity_id"] for link in result["links"])


def test_real_graph_qf1_is_unknown_without_context_and_high_with_bus1():
    system_graph = json.loads(RIGHT_GRAPH_PATH.read_text(encoding="utf-8"))
    graphic = build_graph_entities([system_graph])
    source = _stage53(_detail("ev-qf-real", "QF1 изменён."))
    without_context = build_text_entities(source)
    with_context = build_text_entities(
        source, {"ev-qf-real": {"section": "BUS1"}}
    )

    unknown = build_entity_links_from_artifacts(without_context, graphic)
    high = build_entity_links_from_artifacts(with_context, graphic)

    assert len(unknown["links"]) == 1
    assert unknown["links"][0]["relation"] == "UNKNOWN"
    assert unknown["links"][0]["evidence"][0]["rule"] == (
        "LOCAL_DESIGNATION_REQUIRES_CONTEXT"
    )
    assert len(high["links"]) == 1
    assert high["links"][0]["relation"] == "SAME_ENTITY"
    assert high["links"][0]["confidence"] == "HIGH"


def test_real_ar_produces_26_rooms_and_no_synthetic_graphic_links():
    stage53 = json.loads(AR_STAGE53_PATH.read_text(encoding="utf-8"))
    text = build_text_entities(stage53)
    graphic = build_graph_entities([])
    result = build_entity_links_from_artifacts(text, graphic)
    rooms = [entity for entity in text["entities"] if entity["entity_type"] == "ROOM"]

    assert len(rooms) == 26
    assert text["quality_report"]["produced_entities"] == 27
    assert graphic["entities"] == []
    assert result["links"] == []
    assert result["diagnostics"]["confidence_counts"]["HIGH"] == 0
    assert len(result["diagnostics"]["unresolved_text_entity_ids"]) == 27
