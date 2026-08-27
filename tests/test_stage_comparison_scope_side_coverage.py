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
    produce_graphic_scope_groups,
    query_text_scope,
    schema_path as scope_schema_path,
    scope_join_is_stale,
)
from backend.app.services.stage_comparison.unified_entity_bridge.graphic_coverage import (
    GraphicCoverageValidationError,
    build_graphic_coverage,
    coverage,
    graphic_coverage_is_stale,
    saved_coverage_bundle_is_stale,
    schema_path as coverage_schema_path,
    validate_graphic_coverage,
)
from backend.app.services.stage_comparison.unified_entity_bridge.parent_page_relation import (
    RELATION_AMBIGUOUS,
    RELATION_PROVEN,
    RELATION_UNPROVEN,
    make_parent_page_relation,
    schema_path as parent_relation_schema_path,
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
CORRECT_SIDES_IOS = ROOT / "experiments/g2_4_4_3_correct_sides/ios"
LEFT_GRAPH_PATH = CORRECT_SIDES_IOS / "left_system_graph.json"
RIGHT_GRAPH_PATH = CORRECT_SIDES_IOS / "right_system_graph.json"
COMPARISON_PATH = CORRECT_SIDES_IOS / "comparison_result.json"
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


def _document(code: str) -> dict:
    return {
        "document_code": code,
        "version_id": "v001",
        "storage_identity": None,
        "source_path": None,
        "provenance": "ARTIFACT",
    }


PAIR_DOCUMENTS = {
    "LEFT": _document("CONTROLLED_LEFT"),
    "RIGHT": _document("CONTROLLED_RIGHT"),
}


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
    left_document: dict | None = None,
    right_document: dict | None = None,
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
                "left_document": left_document or PAIR_DOCUMENTS["LEFT"],
                "right_document": right_document or PAIR_DOCUMENTS["RIGHT"],
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


def _processing(manifest: dict, scope_ref: str, dimension: str) -> dict:
    return next(
        item
        for item in manifest["scope_processing"]
        if item["scope_ref"] == scope_ref and item["dimension"] == dimension
    )


def _semantic_states(manifest: dict) -> list[tuple]:
    return sorted(
        (
            item["subject"]["kind"],
            item["subject"]["id"],
            item["dimension"],
            item["side"],
            item["state"],
        )
        for item in manifest["coverage"]
    )


def _build_for_pairs(real_ios, pairs: list[dict]):
    stage, _, _, _, text, graphs, links, _, _, _ = real_ios
    groups = [{"block_pairs": pairs}]
    scopes = build_scope_join(
        stage, text, graphs, groups, pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(
        stage, text, graphs, links, scopes, groups
    )
    return scopes, manifest


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
    groups[0]["block_pairs"][0].update(
        {
            "left_document": PAIR_DOCUMENTS["LEFT"],
            "right_document": PAIR_DOCUMENTS["RIGHT"],
        }
    )
    scopes = build_scope_join(
        stage, text, graphs, groups, pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    return stage, left, right, comparison, text, graphs, links, groups, scopes, manifest


def test_real_ios_uses_accepted_left_to_right_truth_fixture(real_ios):
    _, left, right, comparison, *_ = real_ios

    assert comparison["left_graph"]["block_id"] == (
        "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"
    )
    assert comparison["right_graph"]["block_id"] == (
        "blk_039909ec039649a1b8209f059c95167b"
    )
    assert (len(left["nodes"]), len(left["edges"])) == (73, 99)
    assert (len(right["nodes"]), len(right["edges"])) == (82, 111)
    assert left["quality"]["outgoing_devices"] == 27
    assert right["quality"]["outgoing_devices"] == 30


@pytest.mark.parametrize(
    "binding_state",
    [
        "DOCUMENT_BINDING_PROVEN",
        "DOCUMENT_BINDING_UNPROVEN",
        "DOCUMENT_BINDING_AMBIGUOUS",
        "DOCUMENT_BINDING_MISMATCH",
        "DOCUMENT_BINDING_ERROR",
    ],
)
def test_coverage_validator_requires_proven_binding_for_checked_records(
    real_ios, binding_state
):
    manifest = copy.deepcopy(real_ios[-1])
    assert any(item["state"] == "CHECKED" for item in manifest["coverage"])
    manifest["source_artifacts"]["document_binding"]["state"] = binding_state

    if binding_state == "DOCUMENT_BINDING_PROVEN":
        assert validate_graphic_coverage(manifest) is manifest
    else:
        with pytest.raises(
            GraphicCoverageValidationError,
            match="CHECKED record requires proven document binding",
        ):
            validate_graphic_coverage(manifest)


def test_parent_relation_revocation_makes_scope_and_coverage_stale(real_ios):
    stage, _, _, _, text, graphs, links, original_groups, _, _ = real_ios
    groups = copy.deepcopy(original_groups)
    left_excerpt = _document("LEFT_EXCERPT")
    right_excerpt = _document("RIGHT_EXCERPT")
    for pair in groups[0]["block_pairs"]:
        pair["left_document"] = left_excerpt
        pair["right_document"] = right_excerpt
    left_proven = make_parent_page_relation(
        state=RELATION_PROVEN,
        excerpt={"document": left_excerpt, "page_index_0based": 0},
        parent={"document": PAIR_DOCUMENTS["LEFT"], "page_index_0based": 0},
        reason_codes=["external_deterministic_page_proof"],
        evidence={"producer_id": "test-proof-producer", "evidence_id": "left-proof"},
    )
    right_proven = make_parent_page_relation(
        state=RELATION_PROVEN,
        excerpt={"document": right_excerpt, "page_index_0based": 0},
        parent={"document": PAIR_DOCUMENTS["RIGHT"], "page_index_0based": 0},
        reason_codes=["external_deterministic_page_proof"],
        evidence={"producer_id": "test-proof-producer", "evidence_id": "right-proof"},
    )
    proven_relations = [left_proven, right_proven]
    scopes = build_scope_join(
        stage,
        text,
        graphs,
        groups,
        pair_documents=PAIR_DOCUMENTS,
        parent_page_relations=proven_relations,
    )
    manifest = build_graphic_coverage(
        stage,
        text,
        graphs,
        links,
        scopes,
        groups,
        parent_page_relations=proven_relations,
    )
    assert scopes["document_binding"]["state"] == "DOCUMENT_BINDING_PROVEN"
    assert manifest["summary"]["by_state"]["CHECKED"] > 0

    left_revoked = make_parent_page_relation(
        state=RELATION_UNPROVEN,
        excerpt={"document": left_excerpt, "page_index_0based": 0},
        parent={"document": PAIR_DOCUMENTS["LEFT"], "page_index_0based": 0},
        reason_codes=["external_proof_revoked"],
    )
    current_relations = [left_revoked, right_proven]

    assert scope_join_is_stale(
        scopes,
        stage,
        text,
        graphs,
        groups,
        parent_page_relations=current_relations,
    )
    assert graphic_coverage_is_stale(
        manifest,
        stage,
        text,
        graphs,
        links,
        scopes,
        groups,
        parent_page_relations=current_relations,
    )
    assert saved_coverage_bundle_is_stale(
        manifest,
        text,
        graphs,
        links,
        scopes,
        parent_page_relations=current_relations,
    )

    assert scope_join_is_stale(
        scopes,
        stage,
        text,
        graphs,
        groups,
        parent_page_relations=None,
    )
    assert scope_join_is_stale(
        scopes,
        stage,
        text,
        graphs,
        groups,
        parent_page_relations=[],
    )
    assert graphic_coverage_is_stale(
        manifest,
        stage,
        text,
        graphs,
        links,
        scopes,
        groups,
        parent_page_relations=None,
    )
    assert saved_coverage_bundle_is_stale(
        manifest,
        text,
        graphs,
        links,
        scopes,
        parent_page_relations=None,
    )


def test_page_base_is_explicit_and_text_page_one_does_not_match_graphic_index_one():
    stage = _stage53(left_page=1, right_page=1)
    text, graphs, _ = _base(stage)
    wrong_page_group = _mode1_group(
        "left-page-two", "right-page-two", left_page=1, right_page=1
    )

    scopes = build_scope_join(
        stage, text, graphs, [wrong_page_group], pair_documents=PAIR_DOCUMENTS
    )

    assert pdf_page_to_canonical_index(1) == 0
    assert graphic_page_to_canonical_index(1) == 1
    text_scope = next(item for item in scopes["scopes"] if item["text_scope"])
    assert text_scope["status"] == "UNRESOLVED_SCOPE"
    assert text_scope["graphic_scope_group"] is None


@pytest.mark.parametrize("text_page,graphic_index", [(1, 0), (2, 1), (3, 2)])
def test_canonical_page_matrix(text_page, graphic_index):
    assert pdf_page_to_canonical_index(text_page) == graphic_index
    assert graphic_page_to_canonical_index(graphic_index) == graphic_index


def test_excerpt_local_page_zero_is_not_parent_page_one_without_relation():
    stage = _stage53(left_page=1, right_page=1)
    text, graphs, _ = _base(stage)
    excerpt = _document("LEFT_EXCERPT")
    group = _mode1_group(
        "left-excerpt",
        "right-full",
        left_document=excerpt,
    )

    scopes = build_scope_join(
        stage,
        text,
        graphs,
        [group],
        pair_documents=PAIR_DOCUMENTS,
    )
    text_scope = next(item for item in scopes["scopes"] if item["text_scope"])

    assert text_scope["status"] == "UNRESOLVED_SCOPE"
    assert "graphic_page_identity_unresolved" in text_scope["reason_codes"]
    assert "left:proven_parent_page_relation_absent" in text_scope["reason_codes"]


def test_explicit_proven_parent_relation_resolves_parent_page():
    stage = _stage53(left_page=1, right_page=1)
    text, graphs, _ = _base(stage)
    excerpt = _document("LEFT_EXCERPT")
    group = _mode1_group(
        "left-excerpt",
        "right-full",
        left_document=excerpt,
    )
    relation = make_parent_page_relation(
        state=RELATION_PROVEN,
        excerpt={"document": excerpt, "page_index_0based": 0},
        parent={"document": PAIR_DOCUMENTS["LEFT"], "page_index_0based": 0},
        reason_codes=["external_deterministic_page_proof"],
        evidence={"producer_id": "test-proof-producer", "evidence_id": "proof-1"},
    )

    scopes = build_scope_join(
        stage,
        text,
        graphs,
        [group],
        pair_documents=PAIR_DOCUMENTS,
        parent_page_relations=[relation],
    )
    resolved = next(item for item in scopes["scopes"] if item["status"] == "RESOLVED")

    assert resolved["reason_codes"] == ["proven_parent_page_relation_unique"]
    assert scopes["document_binding"]["state"] == "DOCUMENT_BINDING_PROVEN"


@pytest.mark.parametrize("relation_state", [RELATION_UNPROVEN, RELATION_AMBIGUOUS])
def test_non_proven_parent_relation_keeps_scope_unresolved(relation_state):
    stage = _stage53(left_page=1, right_page=1)
    text, graphs, _ = _base(stage)
    excerpt = _document("LEFT_EXCERPT")
    group = _mode1_group(
        "left-excerpt",
        "right-full",
        left_document=excerpt,
    )
    relation = make_parent_page_relation(
        state=relation_state,
        excerpt={"document": excerpt, "page_index_0based": 0},
        parent={"document": PAIR_DOCUMENTS["LEFT"], "page_index_0based": 0},
        reason_codes=["external_proof_not_conclusive"],
    )

    scopes = build_scope_join(
        stage,
        text,
        graphs,
        [group],
        pair_documents=PAIR_DOCUMENTS,
        parent_page_relations=[relation],
    )
    text_scope = next(item for item in scopes["scopes"] if item["text_scope"])

    assert text_scope["status"] == "UNRESOLVED_SCOPE"
    assert f"left:parent_page_relation_{relation_state.lower()}" in text_scope[
        "reason_codes"
    ]


def test_ambiguous_document_binding_has_distinct_coverage_reason():
    stage = _stage53()
    text, graphs, links = _base(stage)
    groups = [
        {
            "block_pairs": [
                _mode1_group(
                    "left-a",
                    "right-a",
                    left_document=_document("LEFT_A"),
                    right_document=_document("RIGHT_A"),
                )["block_pairs"][0],
                _mode1_group(
                    "left-b",
                    "right-b",
                    left_document=_document("LEFT_B"),
                    right_document=_document("RIGHT_B"),
                )["block_pairs"][0],
            ]
        }
    ]

    scopes = build_scope_join(stage, text, graphs, groups)
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)

    assert scopes["document_binding"]["state"] == "DOCUMENT_BINDING_AMBIGUOUS"
    assert any(
        "document_binding_ambiguous" in item["reason_codes"]
        for item in manifest["scope_processing"]
    )


def test_one_sheet_with_multiple_explicit_block_children_is_valid():
    stage = _stage53()
    text, graphs, _ = _base(stage)
    group = {
        "block_pairs": [
            _mode1_group("left-a", "right-a")["block_pairs"][0],
            _mode1_group("left-b", "right-b")["block_pairs"][0],
        ]
    }

    scopes = build_scope_join(
        stage, text, graphs, [group], pair_documents=PAIR_DOCUMENTS
    )
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
        pair_documents=PAIR_DOCUMENTS,
    )
    ambiguous = build_scope_join(
        stage,
        text,
        graphs,
        [
            _mode1_group("left-a", "right-a"),
            _mode1_group("left-b", "right-b"),
        ],
        pair_documents=PAIR_DOCUMENTS,
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
    scopes = build_scope_join(
        stage, text, graphs, [], pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, [])
    scope_ref = next(item["scope_ref"] for item in scopes["scopes"] if item["text_scope"])

    text_entity_id = text["entities"][0]["entity_id"]
    assert coverage(manifest, scope_ref, text_entity_id, "STRUCTURE")["state"] == (
        "NOT_CHECKED"
    )
    assert "no_graphic_block_scope_on_sheet" in _processing(
        manifest, scope_ref, "STRUCTURE"
    )["reason_codes"]
    assert coverage(manifest, scope_ref, text_entity_id, "PARAMETER")["state"] == (
        "NOT_APPLICABLE"
    )


def test_mode1_local_delta_does_not_claim_system_graph_semantic_coverage():
    stage = _stage53()
    text, graphs, links = _base(stage)
    groups = [_mode1_group("left-local", "right-local")]
    scopes = build_scope_join(
        stage, text, graphs, groups, pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    scope_ref = next(
        item["scope_ref"] for item in scopes["scopes"] if item["status"] == "RESOLVED"
    )
    text_entity_id = text["entities"][0]["entity_id"]
    result = coverage(manifest, scope_ref, text_entity_id, "STRUCTURE", "LEFT")

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
    groups[0]["block_pairs"][0].update(
        {
            "left_document": PAIR_DOCUMENTS["LEFT"],
            "right_document": PAIR_DOCUMENTS["RIGHT"],
        }
    )
    scopes = build_scope_join(
        stage, text, graphs, groups, pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, groups)
    scope_ref = next(
        item["scope_ref"] for item in scopes["scopes"] if item["status"] == "RESOLVED"
    )
    graph_entity_id = graphs["sides"]["LEFT"]["entities"][0]["entity_id"]
    result = coverage(manifest, scope_ref, graph_entity_id, "STRUCTURE", "LEFT")

    assert result["state"] == "CHECK_BLOCKED"
    assert "left_identity_coverage_below_threshold" in result["reason_codes"]
    assert "right_identity_coverage_below_threshold" in result["reason_codes"]


def test_same_inputs_produce_identical_side_scope_and_coverage_artifacts(real_ios):
    stage, left, right, _, text, graphs, links, groups, scopes, manifest = real_ios

    graphs_again = build_side_graph_entities(left_graphs=[left], right_graphs=[right])
    links_again = build_side_entity_links(text, graphs_again)
    scopes_again = build_scope_join(
        stage, text, graphs_again, groups, pair_documents=PAIR_DOCUMENTS
    )
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
    assert links["diagnostics"]["LEFT"]["confidence_counts"]["HIGH"] == 1
    assert links["diagnostics"]["RIGHT"]["confidence_counts"]["HIGH"] == 0
    assert 0 < len(graph_checked) < len(graph_total)
    assert len(graph_checked) <= 2 * (
        comparison["comparison_quality"]["matched_nodes"]
        + sum(len(item["left_nodes"]) for item in comparison["matching"]["detail_matches"])
    )
    assert text["quality_report"]["produced_entities"] == 19
    assert graphs["diagnostics"]["LEFT"]["entities"] == 52
    assert graphs["diagnostics"]["RIGHT"]["entities"] == 56


def test_matched_sections_with_unresolved_neighbours_are_not_connection_or_structure_checked(
    real_ios,
):
    _, _, _, _, _, graphs, _, _, _, manifest = real_ios
    names = {
        side: {
            entity["entity_id"]: entity["canonical_name"]
            for entity in graphs["sides"][side]["entities"]
        }
        for side in ("LEFT", "RIGHT")
    }
    records = [
        item
        for item in manifest["coverage"]
        if item["subject"]["kind"] == "GRAPH_ENTITY"
        and names[item["side"]].get(item["subject"]["id"])
        in {"SECTION_1", "SECTION_2"}
        and item["dimension"] in {"CONNECTION", "STRUCTURE"}
    ]

    assert len(records) == 8
    assert {item["state"] for item in records} == {"NOT_CHECKED"}
    assert {tuple(item["reason_codes"]) for item in records} == {
        ("NEIGHBOUR_IDENTITY_UNRESOLVED",)
    }


def test_real_ios_has_zero_checked_graph_subjects_with_unresolved_neighbours(real_ios):
    _, _, _, comparison, _, graphs, _, _, _, manifest = real_ios
    checked = [
        item
        for item in manifest["coverage"]
        if item["subject"]["kind"] == "GRAPH_ENTITY"
        and item["dimension"] in {"CONNECTION", "STRUCTURE"}
        and item["state"] == "CHECKED"
    ]
    violations = []
    for side in ("LEFT", "RIGHT"):
        side_key = "left_id" if side == "LEFT" else "right_id"
        detail_key = "left_nodes" if side == "LEFT" else "right_nodes"
        high = {
            item[side_key]
            for item in comparison["matching"]["matches"]
            if item["decision"] == "HIGH_MATCH"
        }
        high.update(
            node_id
            for item in comparison["matching"]["detail_matches"]
            if item["match_confidence"] >= 0.85
            for node_id in item[detail_key]
        )
        entities = {
            item["entity_id"]: item for item in graphs["sides"][side]["entities"]
        }
        for record in checked:
            if record["side"] != side:
                continue
            entity = entities[record["subject"]["id"]]
            neighbours = {
                edge["neighbour_node_id"]
                for edge in entity["external_connections"]
            }
            if not set(entity["graph_node_ids"]) <= high or not neighbours <= high:
                violations.append(record["coverage_id"])

    assert checked
    assert violations == []


def test_same_block_in_two_good_pairs_is_order_and_hash_independent(real_ios):
    good = copy.deepcopy(real_ios[7][0]["block_pairs"][0])
    duplicate = copy.deepcopy(good)
    duplicate["ledger"]["diagnostics"]["coverage_provenance_salt"] = "alpha"
    _, first = _build_for_pairs(real_ios, [good, duplicate])

    changed_good = copy.deepcopy(good)
    changed_duplicate = copy.deepcopy(duplicate)
    changed_good["ledger"]["diagnostics"]["coverage_provenance_salt"] = "beta"
    changed_duplicate["ledger"]["diagnostics"]["coverage_provenance_salt"] = "gamma"
    _, second = _build_for_pairs(real_ios, [changed_duplicate, changed_good])

    assert _semantic_states(first) == _semantic_states(second)
    relevant = [
        item
        for item in first["coverage"]
        if item["subject"]["kind"] == "GRAPH_ENTITY"
        and item["dimension"] in {"STRUCTURE", "CONNECTION", "TYPE"}
    ]
    assert relevant
    assert {item["state"] for item in relevant} == {"CHECK_BLOCKED"}
    assert all(
        "MULTIPLE_RELEVANT_BLOCK_PAIRS" in item["reason_codes"]
        for item in relevant
    )


def test_good_and_blocked_pairs_with_same_block_propagate_blocked_independent_of_order(
    real_ios,
):
    stage, left, right, _, _, _, _, groups, _, _ = real_ios
    good = copy.deepcopy(groups[0]["block_pairs"][0])
    low_left = copy.deepcopy(left)
    low_right = copy.deepcopy(right)
    low_left["quality"]["identity_coverage"] = 0.1
    low_right["quality"]["identity_coverage"] = 0.1
    blocked_comparison = compare_system_graphs(low_left, low_right)
    blocked_ledger = adapt_system_graph_comparison_to_ledger(
        blocked_comparison, low_left, low_right
    )
    blocked = {
        "ledger": blocked_ledger,
        "comparison_result": blocked_comparison,
        "left_document": PAIR_DOCUMENTS["LEFT"],
        "right_document": PAIR_DOCUMENTS["RIGHT"],
    }

    _, first = _build_for_pairs(real_ios, [good, blocked])
    _, second = _build_for_pairs(real_ios, [blocked, good])

    assert _semantic_states(first) == _semantic_states(second)
    observable_subjects = [
        item
        for item in first["coverage"]
        if item["dimension"] in {"STRUCTURE", "CONNECTION", "TYPE"}
    ]
    assert observable_subjects
    assert "CHECKED" not in {item["state"] for item in observable_subjects}
    assert "CHECK_BLOCKED" in {item["state"] for item in observable_subjects}


def test_mode1_and_mode2_same_block_is_order_independent(real_ios):
    left = real_ios[1]
    right = real_ios[2]
    mode2 = copy.deepcopy(real_ios[7][0]["block_pairs"][0])
    mode1 = _mode1_group(
        left["block"]["block_id"],
        right["block"]["block_id"],
        left_page=left["block"]["page_index"],
        right_page=right["block"]["page_index"],
    )["block_pairs"][0]

    _, first = _build_for_pairs(real_ios, [mode1, mode2])
    _, second = _build_for_pairs(real_ios, [mode2, mode1])

    assert _semantic_states(first) == _semantic_states(second)
    graph_structure = [
        item
        for item in first["coverage"]
        if item["subject"]["kind"] == "GRAPH_ENTITY"
        and item["dimension"] == "STRUCTURE"
    ]
    assert graph_structure
    assert {item["state"] for item in graph_structure} == {"CHECK_BLOCKED"}


def test_individual_entity_quantity_is_not_applicable(real_ios):
    manifest = real_ios[-1]
    quantity = [
        item
        for item in manifest["coverage"]
        if item["dimension"] == "QUANTITY"
    ]

    assert quantity
    assert {item["state"] for item in quantity} == {"NOT_APPLICABLE"}
    assert all(
        item["reason_codes"] == ["quantity_not_observable_for_individual_entity"]
        or item["reason_codes"] == ["dimension_not_observable_on_either_side"]
        for item in quantity
    )


def test_scope_only_semantic_query_is_rejected(real_ios):
    scopes = real_ios[-2]
    manifest = real_ios[-1]
    scope_ref = scopes["scopes"][0]["scope_ref"]

    with pytest.raises(GraphicCoverageValidationError, match="requires.*subject_id"):
        coverage(manifest, scope_ref, None, "STRUCTURE")


def test_node_permutation_keeps_entity_ids_and_semantic_coverage(real_ios):
    stage, left, right, _, text, graphs, _, groups, _, manifest = real_ios
    permuted_left = copy.deepcopy(left)
    permuted_right = copy.deepcopy(right)
    permuted_left["nodes"].reverse()
    permuted_left["edges"].reverse()
    permuted_right["nodes"].reverse()
    permuted_right["edges"].reverse()
    permuted_graphs = build_side_graph_entities(
        left_graphs=[permuted_left], right_graphs=[permuted_right]
    )
    permuted_links = build_side_entity_links(text, permuted_graphs)
    permuted_scopes = build_scope_join(
        stage, text, permuted_graphs, groups, pair_documents=PAIR_DOCUMENTS
    )
    permuted_manifest = build_graphic_coverage(
        stage,
        text,
        permuted_graphs,
        permuted_links,
        permuted_scopes,
        groups,
    )

    for side in ("LEFT", "RIGHT"):
        original_ids = {
            tuple(item["graph_node_ids"]): item["entity_id"]
            for item in graphs["sides"][side]["entities"]
        }
        permuted_ids = {
            tuple(item["graph_node_ids"]): item["entity_id"]
            for item in permuted_graphs["sides"][side]["entities"]
        }
        assert original_ids == permuted_ids
    assert _semantic_states(manifest) == _semantic_states(permuted_manifest)


def test_saved_bundle_stale_check_needs_no_raw_comparison_or_graph_inputs(real_ios):
    _, _, _, _, text, graphs, links, _, scopes, manifest = real_ios

    assert saved_coverage_bundle_is_stale(
        manifest, text, graphs, links, scopes
    ) is False
    changed_graphs = copy.deepcopy(graphs)
    changed_graphs["source_signature"] = "0" * 64
    assert saved_coverage_bundle_is_stale(
        manifest, text, changed_graphs, links, scopes
    ) is True


def test_production_scope_group_producer_groups_all_pairs_by_canonical_page_pair():
    pairs = [
        _mode1_group("left-a", "right-a")["block_pairs"][0],
        _mode1_group("left-b", "right-b")["block_pairs"][0],
        _mode1_group(
            "left-c", "right-c", left_page=1, right_page=2
        )["block_pairs"][0],
    ]

    groups = produce_graphic_scope_groups(list(reversed(pairs)))

    assert sorted(len(group["block_pairs"]) for group in groups) == [1, 2]


def test_real_ar_without_graphs_has_only_not_checked_semantic_coverage():
    stage = _read(AR_STAGE53_PATH)
    text, graphs, links = _base(stage)
    scopes = build_scope_join(
        stage, text, graphs, [], pair_documents=PAIR_DOCUMENTS
    )
    manifest = build_graphic_coverage(stage, text, graphs, links, scopes, [])
    semantic_scope_records = [
        item
        for item in manifest["scope_processing"]
        if item["dimension"] in {"STRUCTURE", "CONNECTION", "TYPE"}
    ]

    assert len(text["entities"]) == 27
    assert graphs["sides"]["LEFT"]["entities"] == []
    assert graphs["sides"]["RIGHT"]["entities"] == []
    assert scopes["diagnostics"]["resolved_scopes"] == 0
    assert {item["processing_state"] for item in semantic_scope_records} == {
        "SCOPE_NOT_PROCESSED"
    }
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
        "side-graph-entities.v2",
        "side-entity-links.v1",
        "text-graphic-scope-join.v3",
        "graphic-coverage.v4",
    ]
    parent_schema = json.loads(parent_relation_schema_path().read_text(encoding="utf-8"))
    assert parent_schema["properties"]["relation_version"]["const"] == (
        "parent-page-relation.v1"
    )
    assert "parent_page_relations" in schemas[2]["properties"][
        "source_artifacts"
    ]["required"]
