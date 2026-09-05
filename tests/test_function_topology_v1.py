"""Controls for the FUNCTION TOPOLOGY V1 aggregation layer.

Two kinds, for the reason V2's controls give: most are unit tests on synthetic
graphs, because a rule that only holds on one corpus is not a rule; a few read
the frozen control sheet, and those are skipped rather than failed where the
corpus is not present.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_topology_v1 import (
    aggregation,
    binding,
    contract,
    controls,
    facts,
    signature,
)
from experiments.pdf_evidence_v1.contract import ContractViolation
from experiments.pdf_evidence_v2.contract import (
    BUS,
    ELECTRICAL_CONNECTION,
    EQUIPMENT,
    FEEDER,
    JUNCTION_DOT,
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    NO_CLAIM,
    PROVEN_CONNECTION,
    RUNS_ALONG_SINGLE_CONDUCTOR,
    TERMINAL,
    TopologyEdge,
    TopologyNode,
    UNDIRECTED,
)
from experiments.pdf_evidence_v2.topology import PageTopology, assign_islands


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_no_vocabulary_value_states_absence():
    vocabularies = (
        contract.AGGREGATION_CHANNEL + contract.BOUNDARY_STATUS
        + contract.LABEL_OWNERSHIP + contract.BINDING_CHANNEL
        + contract.BINDING_STATUS + contract.BINDING_CAUSE
        + contract.SIGNATURE_TIERS + contract.REPRESENTATION_CLASS
    )
    for value in vocabularies:
        for term in ("ABSENT", "ABSENCE", "REMOVED", "DELETED", "MISSING", "NOT_FOUND"):
            assert term not in value.upper(), value


def test_only_a_drawn_relation_may_prove_a_binding():
    assert contract.PROVING_BINDING_CHANNELS == (contract.MARK_BOUND_TO_MEMBER_NODE,)
    assert contract.SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH not in contract.PROVING_BINDING_CHANNELS


def test_a_sheet_scoped_channel_may_not_prove_a_binding():
    row = contract.ScopeBinding(
        binding_id="b", pair_id="p", project="P", side="LEFT", scope_id="s",
        function_id="f", fragment_id=None, physical_page=1, primary_mark="ВРУ1",
        binding_status=contract.PROVEN_BINDING,
        binding_channel=contract.SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH,
        subgraph_id="fts_x", evidence_refs=("sheet_mark:ВРУ1",),
    )
    with pytest.raises(ContractViolation):
        contract.assert_binding_evidence([row])


def test_an_unbound_row_may_not_name_a_subgraph():
    row = contract.ScopeBinding(
        binding_id="b", pair_id="p", project="P", side="LEFT", scope_id="s",
        function_id="f", fragment_id=None, physical_page=1, primary_mark="ВРУ1",
        binding_status=contract.NO_BINDING, subgraph_id="fts_x",
    )
    with pytest.raises(ContractViolation):
        contract.assert_binding_evidence([row])


def test_a_resemblance_key_is_refused_in_a_produced_value():
    with pytest.raises(ContractViolation):
        contract.assert_no_similarity_evidence({"rows": [{"match_score": 0.9}]})
    with pytest.raises(ContractViolation):
        contract.assert_no_similarity_evidence({"confidence": "HIGH"})
    contract.assert_no_similarity_evidence({"member_node_count": 30, "subgraph_id": "x"})


def test_aggregation_must_be_held_together_by_its_own_edges():
    subgraph = contract.FunctionTopologySubgraph(
        subgraph_id="fts_a", document="D", physical_page=1,
        aggregation_channel=contract.CONNECTED_COMPONENT,
        boundary_status=contract.PARTIAL,
        member_node_ids=("n1", "n2"), member_edge_ids=(),
    )
    with pytest.raises(ContractViolation):
        contract.assert_aggregation_evidence([subgraph], {})
    joined = contract.FunctionTopologySubgraph(
        subgraph_id="fts_b", document="D", physical_page=1,
        aggregation_channel=contract.CONNECTED_COMPONENT,
        boundary_status=contract.PARTIAL,
        member_node_ids=("n1", "n2"), member_edge_ids=("e1",),
    )
    contract.assert_aggregation_evidence([joined], {"e1": ("n1", "n2")})


def test_an_edge_leaving_the_members_is_refused():
    subgraph = contract.FunctionTopologySubgraph(
        subgraph_id="fts_a", document="D", physical_page=1,
        aggregation_channel=contract.CONNECTED_COMPONENT,
        boundary_status=contract.PARTIAL,
        member_node_ids=("n1", "n2"), member_edge_ids=("e1",),
    )
    with pytest.raises(ContractViolation):
        contract.assert_aggregation_evidence([subgraph], {"e1": ("n1", "n9")})


def test_a_label_may_not_reach_outside_the_group_it_names():
    subgraph = contract.FunctionTopologySubgraph(
        subgraph_id="fts_a", document="D", physical_page=1,
        aggregation_channel=contract.CONNECTED_COMPONENT,
        boundary_status=contract.PROVEN,
        member_node_ids=("n1",), member_edge_ids=(), function_marks=("ГPЩ1",),
    )
    with pytest.raises(ContractViolation):
        contract.assert_label_never_aggregates(
            [subgraph], {"n1": "fts_a", "n2": "fts_b"}, {"ГPЩ1": ["n1", "n2"]},
        )
    contract.assert_label_never_aggregates(
        [subgraph], {"n1": "fts_a"}, {"ГPЩ1": ["n1"]},
    )


def test_a_group_may_not_span_two_pages():
    subgraph = contract.FunctionTopologySubgraph(
        subgraph_id="fts_a", document="D", physical_page=1,
        aggregation_channel=contract.CONNECTED_COMPONENT,
        boundary_status=contract.PARTIAL,
        member_node_ids=("n1", "n2"), member_edge_ids=(),
    )
    with pytest.raises(ContractViolation):
        contract.assert_single_page_membership(
            [subgraph], {"n1": 1, "n2": 2}, {"n1": "D", "n2": "D"},
        )


# ---------------------------------------------------------------------------
# synthetic drawings
# ---------------------------------------------------------------------------


def _node(node_id, kind, x=0.0, y=0.0, labels=(), symbol=None, page=1):
    return TopologyNode(
        node_id=node_id, document="D", physical_page=page, node_kind=kind,
        bbox=(x, y, x + 1.0, y + 1.0), anchor=(x, y), labels=tuple(labels),
        symbol_signature=symbol,
    )


def _edge(edge_id, left, right, kind=ELECTRICAL_CONNECTION, page=1,
          claim=PROVEN_CONNECTION, channel=None, evidence=JUNCTION_DOT):
    return TopologyEdge(
        edge_id=edge_id, document="D", physical_page=page,
        from_node_id=left, to_node_id=right, edge_kind=kind,
        connection_claim=claim, direction_status=UNDIRECTED,
        junction_evidence=evidence if kind != LABEL_CONNECTION else None,
        binding_channel=channel, geometry_refs=("synthetic",),
    )


def _page(nodes, edges, page=1, sheet_labels=()):
    topology = PageTopology(document="D", page=page, nodes=list(nodes), edges=list(edges))
    assign_islands(topology)
    data = types.SimpleNamespace(
        labels=[{"label_id": f"l{index}", "text": text}
                for index, text in enumerate(sheet_labels)],
        strokes=types.SimpleNamespace(edges=[0] * 100),
    )
    return types.SimpleNamespace(topology=topology, data=data, page=page)


def _board(page=1, feeders=3, offset=0.0, order=None, mark="ГРЩ1"):
    """One bus, ``feeders`` feeders, each through its own device."""
    nodes = [_node("bus", BUS, offset, offset, page=page)]
    edges = []
    indices = list(range(feeders)) if order is None else list(order)
    for position, index in enumerate(indices):
        nodes.append(_node(f"f{index}", FEEDER, offset + index, offset, page=page))
        nodes.append(_node(f"q{index}", EQUIPMENT, offset + index, offset + 1, symbol="sym_a", page=page))
        nodes.append(_node(f"t{index}", TERMINAL, offset + index, offset + 2, page=page))
        edges.append(_edge(f"e{position}a", "bus", f"f{index}", page=page))
        edges.append(_edge(f"e{position}b", f"f{index}", f"q{index}", page=page))
        edges.append(_edge(f"e{position}c", f"q{index}", f"t{index}", page=page))
        anchor = _node(f"l{index}", LABEL_ANCHOR, offset + index, offset,
                       labels=(f"{mark}-РП1-{index + 1} ППГнг(А)-HF 5х150мм²",), page=page)
        nodes.append(anchor)
        edges.append(_edge(f"e{position}d", f"l{index}", f"f{index}",
                           kind=LABEL_CONNECTION, channel=RUNS_ALONG_SINGLE_CONDUCTOR,
                           page=page))
    return nodes, edges


def test_one_bus_and_its_feeders_become_one_proven_aggregate():
    nodes, edges = _board(feeders=4)
    page = aggregation.aggregate_page(_page(nodes, edges))
    assert len(page.subgraphs) == 1
    only = page.subgraphs[0]
    assert only.boundary_status == contract.PROVEN
    assert len(only.feeder_node_ids) == 4
    assert "ГPЩ1" in only.function_marks


def test_two_disconnected_buses_on_one_page_stay_two_aggregates():
    left_nodes, left_edges = _board(feeders=2, mark="ГРЩ1")
    right_nodes, right_edges = _board(feeders=2, offset=500.0, mark="ГРЩ2")
    right_nodes = [
        _node(node.node_id + "_r", node.node_kind, node.bbox[0], node.bbox[1],
              labels=node.labels, symbol=node.symbol_signature)
        for node in right_nodes
    ]
    right_edges = [
        _edge(edge.edge_id + "_r", edge.from_node_id + "_r", edge.to_node_id + "_r",
              kind=edge.edge_kind, channel=edge.binding_channel)
        for edge in right_edges
    ]
    page = aggregation.aggregate_page(_page(left_nodes + right_nodes, left_edges + right_edges))
    proven = [item for item in page.subgraphs if item.boundary_status == contract.PROVEN]
    assert len(proven) == 2
    members = [set(item.member_node_ids) for item in proven]
    assert not members[0] & members[1]


def test_the_page_is_never_a_reason_to_join_two_drawings():
    left_nodes, left_edges = _board(feeders=2, mark="ГРЩ1")
    right_nodes, right_edges = _board(feeders=2, offset=500.0, mark="ГРЩ2")
    right_nodes = [
        _node(node.node_id + "_r", node.node_kind, node.bbox[0], node.bbox[1],
              labels=node.labels, symbol=node.symbol_signature)
        for node in right_nodes
    ]
    right_edges = [
        _edge(edge.edge_id + "_r", edge.from_node_id + "_r", edge.to_node_id + "_r",
              kind=edge.edge_kind, channel=edge.binding_channel)
        for edge in right_edges
    ]
    result = _page(left_nodes + right_nodes, left_edges + right_edges)
    page = aggregation.aggregate_page(result)
    measured = controls.page_controls(page, result)
    assert measured.get("A_aggregates_spanning_two_islands", 0) == 0
    assert measured.get("B_sheet_wide_aggregate_on_a_multi_island_page", 0) == 0
    assert measured.get("E_nodes_claimed_by_two_aggregates", 0) == 0


def test_a_consumer_named_on_one_wire_does_not_contest_the_board():
    nodes, edges = _board(feeders=3)
    nodes.append(_node("lx", LABEL_ANCHOR, 9, 9, labels=("ХМ1",)))
    edges.append(_edge("ex", "lx", "t0", kind=LABEL_CONNECTION,
                       channel=RUNS_ALONG_SINGLE_CONDUCTOR))
    page = aggregation.aggregate_page(_page(nodes, edges))
    assert page.subgraphs[0].boundary_status == contract.PROVEN


def test_two_boards_welded_by_a_tie_are_ambiguous_rather_than_merged_silently():
    nodes, edges = _board(feeders=2, mark="ГРЩ1")
    extra_nodes, extra_edges = _board(feeders=2, offset=500.0, mark="ГРЩ2")
    extra_nodes = [
        _node(node.node_id + "_r", node.node_kind, node.bbox[0], node.bbox[1],
              labels=node.labels, symbol=node.symbol_signature)
        for node in extra_nodes
    ]
    extra_edges = [
        _edge(edge.edge_id + "_r", edge.from_node_id + "_r", edge.to_node_id + "_r",
              kind=edge.edge_kind, channel=edge.binding_channel)
        for edge in extra_edges
    ]
    tie = _edge("tie", "bus", "bus_r")
    page = aggregation.aggregate_page(
        _page(nodes + extra_nodes, edges + extra_edges + [tie]))
    assert len(page.subgraphs) == 1
    only = page.subgraphs[0]
    assert only.boundary_status == contract.AMBIGUOUS
    assert "bus_bearing_halves_joined_by_one_link=1" in only.notes
    # nothing is dropped: an ambiguous reading keeps every member it has
    assert set(only.bus_node_ids) == {"bus", "bus_r"}


def test_a_mark_that_names_two_aggregates_owns_neither():
    left_nodes, left_edges = _board(feeders=2, mark="ГРЩ1")
    right_nodes, right_edges = _board(feeders=2, offset=500.0, mark="ГРЩ1")
    right_nodes = [
        _node(node.node_id + "_r", node.node_kind, node.bbox[0], node.bbox[1],
              labels=node.labels, symbol=node.symbol_signature)
        for node in right_nodes
    ]
    right_edges = [
        _edge(edge.edge_id + "_r", edge.from_node_id + "_r", edge.to_node_id + "_r",
              kind=edge.edge_kind, channel=edge.binding_channel)
        for edge in right_edges
    ]
    page = aggregation.aggregate_page(_page(left_nodes + right_nodes, left_edges + right_edges))
    assert page.mark_ownership["ГPЩ1"] == contract.REPEATED_LABEL_ACROSS_SUBGRAPHS
    for item in page.subgraphs:
        assert "ГPЩ1" not in item.function_marks


def test_an_alignment_only_label_never_owns_anything():
    nodes, edges = _board(feeders=2)
    nodes.append(_node("la", LABEL_ANCHOR, 9, 9, labels=("АУКРМ №1",)))
    edges.append(_edge("ea", "la", "f0", kind=LABEL_CONNECTION, claim=NO_CLAIM))
    page = aggregation.aggregate_page(_page(nodes, edges))
    assert "AYKPM1" not in page.mark_ownership
    for item in page.subgraphs:
        assert not any(mark.startswith("AYKPM") for mark in item.function_marks)


# ---------------------------------------------------------------------------
# identity
# ---------------------------------------------------------------------------


def _signature(result, tier=contract.SHAPE_ONLY):
    page = aggregation.aggregate_page(result)
    rows = signature.annotate(page, result)
    board = max(rows, key=lambda row: row["shape"]["proven_edge_count"])
    return board["signatures"][tier]


def test_a_signature_survives_new_coordinates():
    plain = _page(*_board(feeders=3))
    moved = _page(*_board(feeders=3, offset=917.5))
    assert _signature(plain) == _signature(moved)


def test_a_signature_survives_a_permuted_feeder_order():
    plain = _page(*_board(feeders=4))
    shuffled = _page(*_board(feeders=4, order=[3, 1, 0, 2]))
    assert _signature(plain) == _signature(shuffled)


def test_a_signature_survives_the_move_to_another_sheet():
    here = _page(*_board(feeders=3, page=1), page=1)
    there = _page(*_board(feeders=3, page=7), page=7)
    assert _signature(here) == _signature(there)


def test_a_signature_carries_no_page_or_node_identifier():
    result = _page(*_board(feeders=3))
    page = aggregation.aggregate_page(result)
    rows = signature.annotate(page, result)
    signature.assert_layout_independent(rows)


def test_a_bigger_board_gets_a_different_signature():
    small = _page(*_board(feeders=3))
    large = _page(*_board(feeders=9))
    assert _signature(small) != _signature(large)


# ---------------------------------------------------------------------------
# binding
# ---------------------------------------------------------------------------


def _passport(page=1, marks=("ВРУ1",), title="Однолинейная схема ВРУ1"):
    return {
        "source_sheet": {"physical_page": page, "title": title, "side": "RIGHT"},
        "stable_entities": list(marks),
        "systems": list(marks),
        "function_class": "POWER_SUPPLY",
        "component_role": "POWER_SUPPLY",
    }


def test_a_passport_without_a_mark_is_unknown_rather_than_unbound():
    nodes, edges = _board(feeders=2)
    page = aggregation.aggregate_page(_page(nodes, edges))
    row = binding.bind_function(
        pair_id="p", project="P", side="RIGHT", function_id="f", fragment_id=None,
        scope_id="s", passport=_passport(marks=(), title=""), page=page,
        page_has_strokes=True,
    )
    assert row.binding_status in {contract.UNKNOWN, contract.NO_BINDING}


def test_a_mark_printed_only_in_the_title_reaches_partial_and_no_further():
    nodes, edges = _board(feeders=3, mark="ГРЩ9")
    result = _page(nodes, edges, sheet_labels=("Однолинейная схема ВРУ1",))
    page = aggregation.aggregate_page(result)
    row = binding.bind_function(
        pair_id="p", project="P", side="RIGHT", function_id="f", fragment_id=None,
        scope_id="s", passport=_passport(marks=("ВРУ1",)), page=page,
        page_has_strokes=True,
    )
    assert row.binding_status == contract.PARTIAL_BINDING
    assert row.binding_channel == contract.SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH
    assert row.cause == contract.MARK_NOT_ON_A_CONDUCTOR
    contract.assert_binding_evidence([row])


def test_a_mark_along_a_member_conductor_reaches_proven():
    nodes, edges = _board(feeders=3, mark="ВРУ1")
    result = _page(nodes, edges, sheet_labels=("Однолинейная схема ВРУ1",))
    page = aggregation.aggregate_page(result)
    row = binding.bind_function(
        pair_id="p", project="P", side="RIGHT", function_id="f", fragment_id=None,
        scope_id="s", passport=_passport(marks=("ВРУ1",)), page=page,
        page_has_strokes=True,
    )
    assert row.binding_status == contract.PROVEN_BINDING
    assert row.binding_channel == contract.MARK_BOUND_TO_MEMBER_NODE
    contract.assert_binding_evidence([row])


def test_a_page_without_a_vector_layer_states_a_mechanism_and_no_verdict():
    row = binding.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", fragment_id=None,
        scope_id="s", passport=_passport(), page=None, page_has_strokes=False,
    )
    assert row.binding_status == contract.NO_BINDING
    assert row.cause == contract.NO_VECTOR_LAYER
    assert row.subgraph_id is None


def test_many_branches_on_one_aggregate_is_not_a_cross_version_merge():
    rows = [
        contract.ScopeBinding(
            binding_id=f"b{index}", pair_id="p", project="P", side="RIGHT",
            scope_id="s", function_id=f"f{index}", fragment_id=None, physical_page=1,
            primary_mark="ВРУ1", binding_status=contract.PROVEN_BINDING,
            binding_channel=contract.MARK_BOUND_TO_MEMBER_NODE,
            subgraph_id="fts_one", evidence_refs=("node:n1",),
        )
        for index in range(3)
    ]
    measured = binding.granularity_notes(rows)
    assert measured["subgraphs_carrying_several_functions"] == 1
    assert "merge" in measured["distinction"]


# ---------------------------------------------------------------------------
# facts
# ---------------------------------------------------------------------------


def test_a_fact_row_states_only_what_is_drawn():
    result = _page(*_board(feeders=3))
    page = aggregation.aggregate_page(result)
    rows = facts.page_facts(page, result)
    board = max(rows, key=lambda row: row["feeder_count"])
    assert board["bus_exists"] is True
    assert board["feeder_count"] == 3
    assert board["arrow_proven_inbound_edge_count"] == 0
    assert board["arrow_proven_outbound_edge_count"] == 0
    assert board["direction_evidence_available"] is False
    shape = facts.comparable_fact_shape(board)
    assert set(shape) == {
        "owner_marks", "bus_exists", "feeder_count", "equipment_count",
        "free_ended_feeder_count", "folded_branch_labels",
    }


# ---------------------------------------------------------------------------
# the frozen control sheet
# ---------------------------------------------------------------------------


def _corpus_available() -> bool:
    try:
        from experiments.function_lineage_v3 import corpus as frozen_corpus

        paths = frozen_corpus.document_paths("p19cd7f695a", "RIGHT")
        return Path(paths["pdf"]).is_file() and Path(paths["markdown"]).is_file()
    except Exception:
        return False


@pytest.mark.skipif(not _corpus_available(), reason="frozen corpus is not present")
def test_the_control_sheet_yields_exactly_one_board_level_aggregate():
    from experiments.function_lineage_v3 import corpus as frozen_corpus
    from experiments.pdf_evidence_v1 import extraction as v1_extraction
    from experiments.pdf_evidence_v2 import pipeline as v2_pipeline

    paths = frozen_corpus.document_paths("p19cd7f695a", "RIGHT")
    body = frozen_corpus.markdown_pages(paths["markdown"])
    profile = v1_extraction.document_profile(str(paths["pdf"]), body)
    result = v2_pipeline.analyse("IOS1.1/RIGHT", str(paths["pdf"]), 20, profile)
    page = aggregation.aggregate_page(result)
    proven = [item for item in page.subgraphs if item.boundary_status == contract.PROVEN]
    assert len(proven) == 1
    board = proven[0]
    assert len(board.bus_node_ids) >= 3
    assert len(board.feeder_node_ids) >= 300
    assert "ГPЩ1" in board.function_marks
    texts = aggregation.bound_texts_by_node(result.topology)
    cable_feeders = [
        node_id for node_id, values in texts.items() if any("мм" in text for text in values)
    ]
    assert cable_feeders
    assert all(
        page.subgraph_of_node.get(node_id) == board.subgraph_id
        for node_id in cable_feeders
    )
