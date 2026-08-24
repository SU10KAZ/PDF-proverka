"""G2.3 negative, contract, and mandatory real-graph comparisons."""
from __future__ import annotations

import copy
import inspect
import json
from pathlib import Path

from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION,
    make_edge,
    make_node,
    validate_system_graph,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
    validate_comparison_result,
)


ROOT = Path(__file__).resolve().parents[3]


def _evidence(token: str) -> list[dict]:
    return [
        {
            "kind": "token",
            "role": "test_grounding",
            "value": token,
            "bbox": [10.0, 10.0, 20.0, 20.0],
            "source_tokens": [token],
        }
    ]


def _node(
    node_id: str,
    node_type: str,
    *,
    canonical: str | None = None,
    label: str | None = None,
    confidence: float = 0.9,
    section: str | None = None,
    attrs: dict | None = None,
    source_representation: str | None = None,
) -> dict:
    extra = {
        "label": label or node_id,
        "canonical_identity": canonical,
        "attrs": attrs or {},
    }
    if section is not None:
        extra["section"] = section
    if node_type == "SOURCE":
        extra["source_role"] = "UPSTREAM_SUPPLY"
        extra["source_representation"] = source_representation or "UNKNOWN_SOURCE"
    return make_node(
        node_id,
        node_type,
        confidence=confidence,
        evidence=_evidence(label or node_id),
        bbox=[10.0, 10.0, 20.0, 20.0],
        source_tokens=[label or node_id],
        **extra,
    )


def _edge(edge_type: str, source: str, target: str, nodes: dict[str, dict]) -> dict:
    return make_edge(
        f"{edge_type}:{source}->{target}",
        edge_type,
        source,
        target,
        confidence=0.9,
        evidence=_evidence(f"{source}->{target}"),
        source_bbox=nodes[source]["bbox"],
        target_bbox=nodes[target]["bbox"],
        source_tokens=[source, target],
    )


def _graph(nodes: list[dict], edge_specs=(), *, block_id="test") -> dict:
    index = {node["id"]: node for node in nodes}
    edges = [_edge(edge_type, source, target, index) for edge_type, source, target in edge_specs]
    graph = {
        "schema_version": SCHEMA_VERSION,
        "profile_id": "test_profile",
        "block": {"block_id": block_id, "page_index": 0, "bbox_visual_pt": [0, 0, 100, 100]},
        "profile": {"id": "test_profile", "profile_confidence": 0.9},
        "nodes": nodes,
        "edges": edges,
        "quality": {
            "source_confidence": 0.9,
            "bus_confidence": 0.9,
            "section_confidence": 0.9,
            "identity_coverage": 0.9,
        },
        "provenance": {"profile_version": "test", "manual_cases": False},
    }
    assert validate_system_graph(graph)["valid"]
    return graph


def _basic_path(*, representation="UPSTREAM_TP_CONNECTION", expanded=False, block_id="test"):
    nodes = [
        _node(
            "SOURCE",
            "SOURCE",
            canonical="SOURCE_PATH#BUS",
            label="SOURCE",
            source_representation=representation,
            attrs={"section": "BUS"},
        ),
        _node("INPUT", "INPUT_DEVICE", canonical="INPUT#BUS", section="BUS"),
        _node("BUS", "BUS_SECTION", canonical="SECTION#1", label="SECTION"),
    ]
    edges = []
    if expanded:
        nodes.append(
            _node(
                "PATH",
                "SERVICE_GROUP",
                canonical="SOURCE_PATH_ELEMENT#BUS",
                section="BUS",
                attrs={"subclass": "BUSWAY"},
            )
        )
        edges.append(("FEEDS", "SOURCE", "PATH"))
        edges.append(("FEEDS", "PATH", "INPUT"))
    else:
        edges.append(("FEEDS", "SOURCE", "INPUT"))
    edges.append(("FEEDS", "INPUT", "BUS"))
    return _graph(nodes, edges, block_id=block_id)


def _types(result: dict) -> list[str]:
    return [change["type"] for change in result["changes"]]


def test_identical_graph_with_different_bbox_is_no_change():
    left = _basic_path(block_id="left")
    right = copy.deepcopy(left)
    right["block"]["block_id"] = "right"
    for index, node in enumerate(right["nodes"], 1):
        node["bbox"] = [index * 100.0, 2.0, index * 100.0 + 20.0, 22.0]
    for edge in right["edges"]:
        edge["bbox"] = [500.0, 500.0, 900.0, 900.0]

    result = compare_system_graphs(left, right)

    assert result["status"] == "NO_CHANGE"
    assert result["changes"] == []
    assert result["provenance"]["bbox_identity"] is False


def test_same_function_with_renamed_labels_is_no_change():
    left = _graph(
        [
            _node("BUS", "BUS_SECTION", canonical="SECTION#1"),
            _node(
                "METER",
                "METERING_GROUP",
                label="Wh1",
                section="BUS",
                attrs={"member_count": 1},
            ),
        ],
        (("MEASURES", "METER", "BUS"),),
        block_id="left",
    )
    right = copy.deepcopy(left)
    right["block"]["block_id"] = "right"
    right["nodes"][1]["label"] = "НАРТИС"
    right["nodes"][1]["source_tokens"] = ["НАРТИС"]

    result = compare_system_graphs(left, right)

    assert result["status"] == "NO_CHANGE"
    assert result["functional_groups"]["status"] == "FUNCTIONS_PRESERVED"
    assert result["changes"] == []


def test_one_source_expanded_to_subgraph_is_detail_not_added_node():
    left = _basic_path(
        representation="UPSTREAM_TP_CONNECTION", block_id="left"
    )
    right = _basic_path(
        representation="TRANSFORMER_EXPLICIT", expanded=True, block_id="right"
    )

    result = compare_system_graphs(left, right)

    assert result["backbone"]["status"] == "BACKBONE_PRESERVED"
    assert _types(result) == ["DETAIL_LEVEL_INCREASED"]
    assert result["matching"]["detail_matches"][0]["right_nodes"] == [
        "SOURCE",
        "PATH",
        "INPUT",
        "BUS",
    ]
    assert result["matching"]["detail_matches"][0]["cardinality"] == "one_to_many"


def test_new_branch_is_node_added():
    bus_left = _node("BUS", "BUS_SECTION", canonical="SECTION#1")
    existing_left = _node(
        "OUT-A", "OUTGOING_DEVICE", canonical="LOAD-A", section="BUS"
    )
    left = _graph(
        [bus_left, existing_left],
        (("FEEDS", "BUS", "OUT-A"),),
        block_id="left",
    )
    right = copy.deepcopy(left)
    right["block"]["block_id"] = "right"
    added = _node("OUT-B", "OUTGOING_DEVICE", canonical="LOAD-B", section="BUS")
    right["nodes"].append(added)
    index = {node["id"]: node for node in right["nodes"]}
    right["edges"].append(_edge("FEEDS", "BUS", "OUT-B", index))

    result = compare_system_graphs(left, right)

    assert "NODE_ADDED" in _types(result)
    added_change = next(change for change in result["changes"] if change["type"] == "NODE_ADDED")
    assert added_change["right_nodes"] == ["OUT-B"]


def test_removed_node_is_node_removed():
    left = _graph(
        [
            _node("BUS", "BUS_SECTION", canonical="SECTION#1"),
            _node("LOAD-A", "LOAD", canonical="LOAD-A"),
        ],
        block_id="left",
    )
    right = _graph(
        [_node("BUS", "BUS_SECTION", canonical="SECTION#1")], block_id="right"
    )

    result = compare_system_graphs(left, right)

    assert "NODE_REMOVED" in _types(result)
    removed = next(change for change in result["changes"] if change["type"] == "NODE_REMOVED")
    assert removed["left_nodes"] == ["LOAD-A"]


def test_edge_type_change_is_connection_changed():
    nodes = [
        _node("A", "LOAD", canonical="A"),
        _node("B", "LOAD", canonical="B"),
    ]
    left = _graph(nodes, (("FEEDS", "A", "B"),), block_id="left")
    right = _graph(
        copy.deepcopy(nodes),
        (("PROTECTS_OR_SWITCHES", "A", "B"),),
        block_id="right",
    )

    result = compare_system_graphs(left, right)

    assert _types(result) == ["CONNECTION_CHANGED"]
    change = result["changes"][0]
    assert change["evidence"]["left"]["edge_ids"]
    assert change["evidence"]["right"]["edge_ids"]


def test_weak_identity_stays_uncertain():
    left = _graph(
        [_node("UNKNOWN-L", "UNKNOWN_NODE", label="left", confidence=0.3)],
        block_id="left",
    )
    right = _graph(
        [_node("UNKNOWN-R", "UNKNOWN_NODE", label="right", confidence=0.3)],
        block_id="right",
    )

    result = compare_system_graphs(left, right)

    assert result["status"] == "UNCERTAIN"
    assert _types(result) == ["UNCERTAIN_STRUCTURAL_CHANGE"]
    assert not ({"NODE_ADDED", "NODE_REMOVED"} & set(_types(result)))


def test_real_grsh_comparison_has_expected_structural_semantics():
    artifact_dir = ROOT / "experiments/g2_dense_sectioned_board"
    left = json.loads((artifact_dir / "left_system_graph.json").read_text(encoding="utf-8"))
    right = json.loads((artifact_dir / "right_system_graph.json").read_text(encoding="utf-8"))

    result = compare_system_graphs(left, right)
    types = _types(result)

    assert result["validation"]["valid"] is True
    assert result["backbone"]["status"] == "BACKBONE_PRESERVED"
    assert types.count("DETAIL_LEVEL_INCREASED") == 2
    assert types.count("NODE_TYPE_CHANGED") == 1
    assert types.count("GROUP_COUNT_CHANGED") == 1
    assert "NODE_ADDED" not in types
    assert "NODE_REMOVED" not in types
    type_change = next(change for change in result["changes"] if change["type"] == "NODE_TYPE_CHANGED")
    assert type_change["evidence"]["reason"]["left_effective_type"] == "CIRCUIT_BREAKER"
    assert type_change["evidence"]["reason"]["right_effective_type"] == "SWITCH_DISCONNECTOR"
    count_change = next(change for change in result["changes"] if change["type"] == "GROUP_COUNT_CHANGED")
    assert count_change["evidence"]["reason"]["left_count"] == 30
    assert count_change["evidence"]["reason"]["right_count"] == 27
    preserved = {item["function"] for item in result["functional_groups"]["preserved"]}
    assert {"METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP"} <= preserved
    assert result["provenance"]["geometry_identity_weight"] == 0.0


def test_every_change_is_grounded_for_future_ledger_wrapping():
    artifact_dir = ROOT / "experiments/g2_dense_sectioned_board"
    left = json.loads((artifact_dir / "left_system_graph.json").read_text(encoding="utf-8"))
    right = json.loads((artifact_dir / "right_system_graph.json").read_text(encoding="utf-8"))

    result = compare_system_graphs(left, right)

    assert validate_comparison_result(result)["valid"] is True
    for change in result["changes"]:
        assert {"type", "confidence", "evidence", "summary"} <= change.keys()
        assert change["left_nodes"] or change["right_nodes"]
        assert (
            change["evidence"]["left"]["source_tokens"]
            or change["evidence"]["right"]["source_tokens"]
        )


def test_comparator_has_no_manual_case_or_integration_dependencies():
    import backend.app.pipeline.stages.block_grounding.graph_identity_matcher as matcher
    import backend.app.pipeline.stages.block_grounding.system_graph_comparator as comparator

    source = inspect.getsource(matcher) + inspect.getsource(comparator)
    for forbidden in (
        "QF3",
        "QS1",
        "ТП1",
        "ТП2",
        "blk_039909",
        "blk_2d72",
        "fitz",
        "graphic_comparison",
    ):
        assert forbidden not in source
