"""G2.4.4 explicit side, scope join, and graphic coverage checks."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION as SYSTEM_GRAPH_SCHEMA_VERSION,
    make_node,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
)
from backend.app.services.stage_comparison.graphic_comparison.graphic_change_ledger_adapter import (
    adapt_system_graph_comparison_to_ledger,
)
from backend.app.services.stage_comparison.unified_entity_bridge.comparison_scope import (
    build_scope_join,
    graphic_page_to_canonical_index,
    pdf_page_to_canonical_index,
    query_text_scope,
    schema_path as scope_schema_path,
    scope_join_is_stale,
)
from backend.app.services.stage_comparison.unified_entity_bridge.graphic_coverage import (
    build_graphic_coverage,
    coverage,
    graphic_coverage_is_stale,
    schema_path as coverage_schema_path,
)
from backend.app.services.stage_comparison.unified_entity_bridge.side_entity_contract import (
    build_side_entity_links,
    build_side_graph_entities,
    graph_schema_path,
    links_schema_path,
    query_text_entity_side,
)
from backend.app.services.stage_comparison.unified_entity_bridge.text_entity_producer import (
    build_text_entities,
)


ROOT = Path(__file__).resolve().parents[1]
LEFT_GRAPH_PATH = ROOT / "experiments/g2_dense_sectioned_board/left_system_graph.json"
RIGHT_GRAPH_PATH = ROOT / "experiments/g2_dense_sectioned_board/right_system_graph.json"
COMPARISON_PATH = ROOT / "experiments/g2_system_graph_comparator/comparison_result.json"
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


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _stage53(
    *,
    text: str = "ВРУ-А изменено.",
    left_page: int = 1,
    right_page: int = 1,
    source_status: str = "CHANGED",
    pair_status: str = "PAIR_OK",
    pair_id: str = "pair-scope",
) -> dict:
    before = None if source_status == "ADDED" else text
    after = None if source_status == "REMOVED" else text
    detail = {
        "evidence_id": "ev-scope",
        "summary": text,
        "before": before,
        "after": after,
        "stage5_title": text,
        "group_id": "sheet-scope",
        "left_pages": [left_page],
        "right_pages": [right_page],
        "left_fragment_ids": [] if before is None else ["fragment-left"],
        "right_fragment_ids": [] if after is None else ["fragment-right"],
        "source_status": source_status,
        "pair_status": pair_status,
    }
    return {
        "version": 1,
        "schema_version": "1.0",
        "kind": "stage_comparison_high_level_project_changes",
        "pair_id": pair_id,
        "source_signature": "controlled-stage53",
        "high_level_changes": [
            {
                "change_id": "change-scope",
                "sheet_groups": ["sheet-scope"],
                "details": [detail],
            }
        ],
        "detail_level_increased": [],
        "material_review": [],
        "non_material_review": [],
        "unresolved": [],
        "service_structure_summary": {"items": []},
    }


def _node(node_id: str, label: str) -> dict:
    return make_node(
        node_id,
        "LOAD",
        confidence=0.95,
        evidence=[
            {
                "kind": "token",
                "role": "identity",
                "value": label,
                "source_tokens": [label],
            }
        ],
        bbox=[1, 2, 3, 4],
        source_tokens=[label],
        label=label,
        canonical_identity=label,
        section="S1",
    )


def _graph(block_id: str, page_index: int, nodes: list[dict]) -> dict:
    return {
        "schema_version": SYSTEM_GRAPH_SCHEMA_VERSION,
        "profile_id": "controlled",
        "block": {"block_id": block_id, "page_index": page_index},
        "discipline": "EOM",
        "profile": {"id": "controlled"},
        "nodes": nodes,
        "edges": [],
        "quality": {"backbone_recovered": True},
        "provenance": {
            "profile": "controlled",
            "profile_version": "controlled-v1",
            "vector_evidence": {"extraction_version": "vector-evidence-v1"},
        },
    }


def _mode1_ledger(
    left_block: str,
    right_block: str,
    *,
    left_page: int = 0,
    right_page: int = 0,
) -> dict:
    def block(block_id: str, page_index: int) -> dict:
        return {
            "block_id": block_id,
            "page_index": page_index,
            "block_type": "generic_graphic",
            "bbox_visual_pt": [0.0, 0.0, 10.0, 10.0],
        }

    return {
        "schema_version": "graphic-change-ledger.v1",
        "comparison_scope": {
            "left_blocks": [block(left_block, left_page)],
            "right_blocks": [block(right_block, right_page)],
        },
        "route": "MODE_1_APPLICABLE",
        "mode": "MODE_1",
        "policy": {},
        "quality": {},
        "changes": [],
        "diagnostics": {},
    }


def _mode1_group(
    left_block: str,
    right_block: str,
    *,
    left_page: int = 0,
    right_page: int = 0,
) -> dict:
    return {
        "block_pairs": [
            {
                "ledger": _mode1_ledger(
                    left_block,
                    right_block,
                    left_page=left_page,
                    right_page=right_page,
                ),
                "comparison_result": None,
            }
        ]
    }


def _base(stage: dict, left_graphs=(), right_graphs=()):
    text = build_text_entities(stage)
    graphs = build_side_graph_entities(
        left_graphs=list(left_graphs), right_graphs=list(right_graphs)
    )
    links = build_side_entity_links(text, graphs)
    return text, graphs, links


@pytest.fixture(scope="module")
def real_ios():
    stage = _read(IOS_STAGE53_PATH)
    left = _read(LEFT_GRAPH_PATH)
    right = _read(RIGHT_GRAPH_PATH)
    comparison = _read(COMPARISON_PATH)
    ledger = adapt_system_graph_comparison_to_ledger(comparison, left, right)
    text, graphs, links = _base(stage, [left], [right])
    groups = [
        {
            "block_pairs": [
                {"ledger": ledger, "comparison_result": comparison}
            ]
        }
    ]
    scopes = build_scope_join(stage, text, graphs, groups)
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    return stage, left, right, comparison, text, graphs, links, groups, scopes, manifest


def test_page_base_is_explicit_and_text_page_one_does_not_match_graphic_index_one():
    stage = _stage53(left_page=1, right_page=1)
    text, graphs, _ = _base(stage)
    wrong_page_group = _mode1_group(
        "left-page-two", "right-page-two", left_page=1, right_page=1
    )

    scopes = build_scope_join(stage, text, graphs, [wrong_page_group])

    assert pdf_page_to_canonical_index(1) == 0
    assert graphic_page_to_canonical_index(1) == 1
    text_scope = next(item for item in scopes["scopes"] if item["text_scope"])
    assert text_scope["status"] == "UNRESOLVED_SCOPE"
    assert text_scope["graphic_scope_group"] is None


def test_one_sheet_with_multiple_explicit_block_children_is_valid():
    stage = _stage53()
    text, graphs, _ = _base(stage)
    group = {
        "block_pairs": [
            _mode1_group("left-a", "right-a")["block_pairs"][0],
            _mode1_group("left-b", "right-b")["block_pairs"][0],
        ]
    }

    scopes = build_scope_join(stage, text, graphs, [group])
    resolved = next(item for item in scopes["scopes"] if item["status"] == "RESOLVED")

    assert scopes["diagnostics"]["resolved_scopes"] == 1
    assert len(resolved["child_block_scopes"]) == 2
    assert len({item["scope_ref"] for item in resolved["child_block_scopes"]}) == 2


def test_block_on_other_page_does_not_join_and_ambiguous_groups_fail_closed():
    stage = _stage53()
    text, graphs, _ = _base(stage)
    other_page = build_scope_join(
        stage,
        text,
        graphs,
        [_mode1_group("left-other", "right-other", left_page=2, right_page=2)],
    )
    ambiguous = build_scope_join(
        stage,
        text,
        graphs,
        [
            _mode1_group("left-a", "right-a"),
            _mode1_group("left-b", "right-b"),
        ],
    )

    assert next(item for item in other_page["scopes"] if item["text_scope"])[
        "status"
    ] == "UNRESOLVED_SCOPE"
    ambiguous_text = next(item for item in ambiguous["scopes"] if item["text_scope"])
    assert ambiguous_text["status"] == "UNRESOLVED_SCOPE"
    assert ambiguous_text["reason_codes"] == ["multiple_graphic_scope_groups_on_sheet"]


def test_left_and_right_links_are_independent_but_same_side_cardinality_fails_closed():
    stage = _stage53()
    text = build_text_entities(stage)
    left = _graph("left-one", 0, [_node("left-vru", "VRU-A")])
    right = _graph("right-one", 0, [_node("right-vru", "ВРУ-А")])
    graphs = build_side_graph_entities(left_graphs=[left], right_graphs=[right])
    links = build_side_entity_links(text, graphs)
    entity_id = text["entities"][0]["entity_id"]

    assert links["diagnostics"]["LEFT"]["confidence_counts"]["HIGH"] == 1
    assert links["diagnostics"]["RIGHT"]["confidence_counts"]["HIGH"] == 1
    assert query_text_entity_side(stage, text, links, entity_id, "LEFT")["match"] == "HIGH"
    assert query_text_entity_side(stage, text, links, entity_id, "RIGHT")["match"] == "HIGH"

    two_left = _graph(
        "left-two",
        0,
        [_node("left-vru-a", "VRU-A"), _node("left-vru-b", "VRU-A")],
    )
    ambiguous_graphs = build_side_graph_entities(
        left_graphs=[two_left], right_graphs=[right]
    )
    ambiguous_links = build_side_entity_links(text, ambiguous_graphs)

    assert ambiguous_links["diagnostics"]["LEFT"]["confidence_counts"]["HIGH"] == 0
    assert ambiguous_links["diagnostics"]["LEFT"]["confidence_counts"]["UNKNOWN"] == 2
    assert ambiguous_links["diagnostics"]["RIGHT"]["confidence_counts"]["HIGH"] == 1


def test_text_side_presence_and_thin_text_scope_query_are_evidence_backed():
    stage = _stage53(source_status="ADDED", pair_status="PAIR_REVIEW_REQUIRED")
    text, graphs, links = _base(stage)
    entity_id = text["entities"][0]["entity_id"]

    left = query_text_entity_side(stage, text, links, entity_id, "LEFT")
    right = query_text_entity_side(stage, text, links, entity_id, "RIGHT")
    scope = query_text_scope(stage, "sheet-scope")

    assert left["presence"] == "ABSENT"
    assert right["presence"] == "PRESENT"
    assert scope["status"] == "CHECK_BLOCKED"
    assert scope["pair_review_required"] is True
    assert "pair_review_required" in scope["reason_codes"]


def test_no_system_graph_is_not_checked_and_unsupported_dimension_is_not_applicable():
    stage = _stage53()
    text, graphs, links = _base(stage)
    scopes = build_scope_join(stage, text, graphs, [])
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, [])
    scope_ref = next(item["scope_ref"] for item in scopes["scopes"] if item["text_scope"])

    assert coverage(manifest, scope_ref, None, "STRUCTURE")["state"] == "NOT_CHECKED"
    assert "no_system_graph_for_sheet" in coverage(
        manifest, scope_ref, None, "STRUCTURE"
    )["reason_codes"]
    assert coverage(manifest, scope_ref, None, "PARAMETER")["state"] == (
        "NOT_APPLICABLE"
    )


def test_mode1_local_delta_does_not_claim_system_graph_semantic_coverage():
    stage = _stage53()
    text, graphs, links = _base(stage)
    groups = [_mode1_group("left-local", "right-local")]
    scopes = build_scope_join(stage, text, graphs, groups)
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    scope_ref = next(
        item["scope_ref"] for item in scopes["scopes"] if item["status"] == "RESOLVED"
    )
    result = coverage(manifest, scope_ref, None, "STRUCTURE")

    assert result["state"] == "NOT_APPLICABLE"
    assert result["reason_codes"] == [
        "mode1_local_graphic_delta_not_semantic_coverage"
    ]


def test_low_quality_mode2_is_check_blocked(real_ios):
    stage, left, right, _, _, _, _, _, _, _ = real_ios
    low_left = copy.deepcopy(left)
    low_right = copy.deepcopy(right)
    low_left["quality"]["identity_coverage"] = 0.1
    low_right["quality"]["identity_coverage"] = 0.1
    comparison = compare_system_graphs(low_left, low_right)
    ledger = adapt_system_graph_comparison_to_ledger(
        comparison, low_left, low_right
    )
    text, graphs, links = _base(stage, [low_left], [low_right])
    groups = [
        {
            "block_pairs": [
                {"ledger": ledger, "comparison_result": comparison}
            ]
        }
    ]
    scopes = build_scope_join(stage, text, graphs, groups)
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    scope_ref = next(
        item["scope_ref"] for item in scopes["scopes"] if item["status"] == "RESOLVED"
    )
    result = coverage(manifest, scope_ref, None, "STRUCTURE")

    assert result["state"] == "CHECK_BLOCKED"
    assert "left_identity_coverage_below_threshold" in result["reason_codes"]
    assert "right_identity_coverage_below_threshold" in result["reason_codes"]


def test_same_inputs_produce_identical_side_scope_and_coverage_artifacts(real_ios):
    stage, left, right, _, text, graphs, links, groups, scopes, manifest = real_ios

    graphs_again = build_side_graph_entities(left_graphs=[left], right_graphs=[right])
    links_again = build_side_entity_links(text, graphs_again)
    scopes_again = build_scope_join(stage, text, graphs_again, groups)
    manifest_again = build_graphic_coverage(
        stage, text, graphs_again, links_again, scopes_again, groups
    )

    assert graphs_again == graphs
    assert links_again == links
    assert scopes_again == scopes
    assert manifest_again == manifest
    assert scope_join_is_stale(scopes, stage, text, graphs, groups) is False
    assert graphic_coverage_is_stale(
        manifest, stage, text, graphs, links, scopes, groups
    ) is False

    changed_stage = copy.deepcopy(stage)
    changed_stage["source_signature"] = "changed"
    assert scope_join_is_stale(scopes, changed_stage, text, graphs, groups) is True

    changed_groups = copy.deepcopy(groups)
    changed_groups[0]["block_pairs"][0]["ledger"]["diagnostics"][
        "coverage_probe"
    ] = "changed"
    assert scope_join_is_stale(scopes, stage, text, graphs, changed_groups) is True
    assert graphic_coverage_is_stale(
        manifest, stage, text, graphs, links, scopes, changed_groups
    ) is True


def test_real_ios_scope_and_subject_coverage_are_conservative(real_ios):
    _, _, _, comparison, text, graphs, links, _, scopes, manifest = real_ios
    resolved = next(item for item in scopes["scopes"] if item["status"] == "RESOLVED")
    graph_checked = [
        item
        for item in manifest["coverage"]
        if item["scope_ref"] == resolved["scope_ref"]
        and item["subject"]["kind"] == "GRAPH_ENTITY"
        and item["dimension"] == "STRUCTURE"
        and item["state"] == "CHECKED"
    ]
    graph_total = [
        item
        for item in manifest["coverage"]
        if item["scope_ref"] == resolved["scope_ref"]
        and item["subject"]["kind"] == "GRAPH_ENTITY"
        and item["dimension"] == "STRUCTURE"
    ]

    assert scopes["diagnostics"] == {
        "text_sheet_groups": 5,
        "graphic_scope_groups": 1,
        "resolved_scopes": 1,
        "unresolved_scopes": 4,
        "resolved_child_block_scopes": 1,
    }
    assert links["diagnostics"]["LEFT"]["confidence_counts"]["HIGH"] == 0
    assert links["diagnostics"]["RIGHT"]["confidence_counts"]["HIGH"] == 1
    assert 0 < len(graph_checked) < len(graph_total)
    assert len(graph_checked) <= 2 * (
        comparison["comparison_quality"]["matched_nodes"]
        + sum(len(item["left_nodes"]) for item in comparison["matching"]["detail_matches"])
    )
    assert text["quality_report"]["produced_entities"] == 19
    assert graphs["diagnostics"]["LEFT"]["entities"] == 56
    assert graphs["diagnostics"]["RIGHT"]["entities"] == 52


def test_real_ar_without_graphs_has_only_not_checked_semantic_coverage():
    stage = _read(AR_STAGE53_PATH)
    text, graphs, links = _base(stage)
    scopes = build_scope_join(stage, text, graphs, [])
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, [])
    semantic_scope_records = [
        item
        for item in manifest["coverage"]
        if item["subject"]["kind"] == "SCOPE"
        and item["dimension"] in {"STRUCTURE", "CONNECTION", "TYPE", "QUANTITY"}
    ]

    assert len(text["entities"]) == 27
    assert graphs["sides"]["LEFT"]["entities"] == []
    assert graphs["sides"]["RIGHT"]["entities"] == []
    assert scopes["diagnostics"]["resolved_scopes"] == 0
    assert {item["state"] for item in semantic_scope_records} == {"NOT_CHECKED"}
    assert manifest["summary"]["by_state"]["CHECKED"] == 0


def test_g244_contract_schemas_are_versioned():
    schemas = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (
            graph_schema_path(),
            links_schema_path(),
            scope_schema_path(),
            coverage_schema_path(),
        )
    ]

    assert [schema["properties"]["schema_version"]["const"] for schema in schemas] == [
        "side-graph-entities.v1",
        "side-entity-links.v1",
        "text-graphic-scope-join.v1",
        "graphic-coverage.v1",
    ]
