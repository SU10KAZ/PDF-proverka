"""Controls for the PDF Evidence V2 topology layer.

The tests are deliberately of two kinds.  Most are unit tests on synthetic
geometry, because a rule that only works on one corpus is not a rule.  A few
read the frozen control sheet, because the whole track turns on whether a real
single-line diagram survives the rules — and those are skipped, not failed,
where the corpus is not present.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

# ``tests/experiments/`` shadows the repository package once pytest puts the
# test directory first on the path; V1's controls do the same thing for the
# same reason.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.pdf_evidence_v2 import (
    binding,
    conductors,
    contract,
    identity,
    strokes,
    symbols,
    topology,
)
from experiments.pdf_evidence_v1.contract import ContractViolation


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_the_claim_vocabulary_has_no_absence():
    assert contract.CONNECTION_CLAIM == ("PROVEN_CONNECTION", "NO_CLAIM")
    for value in contract.CONNECTION_CLAIM + contract.DIRECTION_STATUS:
        for term in ("ABSENT", "REMOVED", "DELETED"):
            assert term not in value.upper()


def test_only_an_arrowhead_may_prove_a_direction():
    assert contract.DIRECTION_EVIDENCE == ("ARROWHEAD",)


def test_binding_channels_contain_no_proximity_rule():
    assert contract.BINDING_CHANNEL == (
        "RUNS_ALONG_SINGLE_CONDUCTOR",
        "INSIDE_SINGLE_SYMBOL_BOX",
        "INSIDE_SINGLE_TABLE_CELL",
    )


def _edge(**kwargs):
    base = dict(
        edge_id="e", document="d", physical_page=1,
        from_node_id="a", to_node_id="b",
        edge_kind=contract.ELECTRICAL_CONNECTION,
        connection_claim=contract.PROVEN_CONNECTION,
        direction_status=contract.UNDIRECTED,
    )
    base.update(kwargs)
    return contract.TopologyEdge(**base)


def test_a_connection_without_drawn_evidence_is_refused():
    with pytest.raises(ContractViolation):
        contract.assert_connection_evidence([_edge(geometry_refs=("g",))])
    contract.assert_connection_evidence(
        [_edge(junction_evidence=contract.JUNCTION_DOT, geometry_refs=("g",))])


def test_a_direction_without_an_arrowhead_is_refused():
    with pytest.raises(ContractViolation):
        contract.assert_direction_evidence([
            _edge(direction_status=contract.PROVEN, direction_evidence=None)])


def test_the_guard_refuses_absence_words_in_any_produced_value():
    with pytest.raises(ContractViolation):
        contract.assert_no_absence_vocabulary({"note": "the conductor was REMOVED"})


def test_the_contract_document_is_exempt_from_its_own_vocabulary_guard():
    contract.assert_no_absence_vocabulary(contract.contract_document())


# ---------------------------------------------------------------------------
# welding and dashes
# ---------------------------------------------------------------------------


def test_collinear_strokes_of_one_colour_weld_and_two_colours_do_not():
    segments = np.array([
        [0.0, 10.0, 20.0, 10.0],
        [20.0, 10.0, 40.0, 10.0],
        [0.0, 10.2, 40.0, 10.2],
    ])
    edges, axis, colour, pattern, pieces, slanted = strokes.weld(
        segments, ["black", "black", "cyan"])
    assert len(edges) == 2
    assert sorted(colour) == ["black", "cyan"]
    assert pieces[list(colour).index("black")] == 2


def test_a_regular_dash_run_becomes_one_edge_and_an_irregular_one_does_not():
    regular = np.array([[x, 5.0, x + 6.0, 5.0] for x in (0.0, 9.0, 18.0, 27.0, 36.0)])
    edges, _, _, pattern, _, _ = strokes.weld(regular, ["c"] * len(regular))
    assert len(edges) == 1
    assert pattern == [contract.PROVEN_DASHED]

    irregular = np.array([[0.0, 5.0, 8.0, 5.0], [12.0, 5.0, 40.0, 5.0]])
    edges, _, _, pattern, _, _ = strokes.weld(irregular, ["c", "c"])
    assert len(edges) == 2
    assert pattern == [contract.SOLID, contract.SOLID]


def test_slanted_strokes_are_kept_and_never_welded():
    segments = np.array([[0.0, 0.0, 10.0, 10.0]])
    edges, _, _, _, _, slanted = strokes.weld(segments, ["black"])
    assert len(edges) == 0
    assert len(slanted) == 1


def test_scanline_slivers_cluster_back_into_one_dot():
    rects = np.array([[10.0, 20.0 + index * 0.3, 15.0, 20.3 + index * 0.3] for index in range(16)])
    blobs = strokes.cluster_blobs(rects, ["black"] * len(rects))
    assert len(blobs) == 1
    assert blobs[0].is_dot_shaped


def test_a_widening_profile_is_an_arrow_and_a_disc_is_not():
    arrow = strokes.Blob(bbox=(0, 0, 6, 6), colour="black", slivers=6,
                         profile=(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    disc = strokes.Blob(bbox=(0, 0, 6, 6), colour="black", slivers=6,
                        profile=(1.0, 4.0, 6.0, 6.0, 4.0, 1.0))
    assert arrow.widens_monotonically()
    assert not disc.widens_monotonically()


# ---------------------------------------------------------------------------
# symbols
# ---------------------------------------------------------------------------


def test_one_block_drawn_twice_shares_a_signature_and_stays_two_clusters():
    shape = np.array([[0.0, 0.0, 4.0, 4.0], [4.0, 4.0, 0.0, 8.0], [0.0, 8.0, 0.0, 0.0]])
    offset = shape + np.array([100.0, 0.0, 100.0, 0.0])
    clusters = symbols.build_clusters(
        np.zeros((0, 4)), np.zeros(0, dtype=bool), np.zeros(0), np.vstack([shape, offset]))
    assert len(clusters) == 2
    assert clusters[0].signature == clusters[1].signature
    assert clusters[0].index != clusters[1].index


def test_a_signature_is_blind_to_position_and_scale_and_not_to_shape():
    triangle = np.array([[0.0, 0.0, 4.0, 4.0], [4.0, 4.0, 0.0, 8.0], [0.0, 8.0, 0.0, 0.0]])
    scaled = triangle * 3.0
    square = np.array([[0.0, 0.0, 4.0, 0.0], [4.0, 0.0, 4.0, 4.0],
                       [4.0, 4.0, 0.0, 4.0], [0.0, 4.0, 0.0, 0.0]])
    assert (
        symbols._signature(triangle, (0, 0, 4, 8), 3)
        == symbols._signature(scaled, (0, 0, 12, 24), 3)
    )
    assert (
        symbols._signature(triangle, (0, 0, 4, 8), 3)
        != symbols._signature(square, (0, 0, 4, 4), 4)
    )


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


class _FakeStrokes:
    def __init__(self, edges):
        self.edges = np.asarray(edges, dtype=float)
        self.page = 1

    @property
    def horizontal_mask(self):
        return self.edges[:, 1] == self.edges[:, 3]

    def geometry_ref(self, index):
        return f"g{index}"


class _FakePage:
    def __init__(self, edges):
        self.document = "d"
        self.page = 1
        self.strokes = _FakeStrokes(edges)
        self.regions = []


def _facts(page, conductor, junctions):
    return conductors.EdgeFacts(
        nature=[contract.SCHEMATIC_CONDUCTOR] * len(page.strokes.edges),
        region=np.zeros(len(page.strokes.edges), dtype=np.int64),
        conductor=np.asarray(conductor, dtype=bool),
        conductor_evidence=[contract.JUNCTION_DOT] * len(page.strokes.edges),
        junctions=junctions,
    )


def test_two_pieces_meeting_alone_are_one_wire_and_three_are_a_branch():
    page = _FakePage([[0, 0, 10, 0], [10, 0, 20, 0]])
    junction = conductors.Junction("j", (10.0, 0.0), contract.COINCIDENT_ENDPOINTS, (0, 1))
    run_of_edge, _ = topology.build_runs(page, _facts(page, [True, True], [junction]))
    assert len(set(run_of_edge.values())) == 1

    page = _FakePage([[0, 0, 10, 0], [10, 0, 20, 0], [10, 0, 10, 10]])
    junction = conductors.Junction("j", (10.0, 0.0), contract.COINCIDENT_ENDPOINTS, (0, 1, 2))
    run_of_edge, _ = topology.build_runs(page, _facts(page, [True] * 3, [junction]))
    assert len(set(run_of_edge.values())) == 3


def test_a_run_carrying_three_dots_is_a_bus_and_two_is_not():
    page = _FakePage([[0, 0, 90, 0], [10, 0, 10, 20], [40, 0, 40, 20], [70, 0, 70, 20]])
    junctions = [
        conductors.Junction(f"j{index}", (float(x), 0.0), contract.JUNCTION_DOT, (0, index + 1))
        for index, x in enumerate((10.0, 40.0, 70.0))
    ]
    graph = topology.build_page(page, _facts(page, [True] * 4, junctions))
    kinds = {node.node_kind for node in graph.nodes}
    assert contract.BUS in kinds
    buses = [node for node in graph.nodes if node.node_kind == contract.BUS]
    assert len(buses) == 1

    graph = topology.build_page(page, _facts(page, [True] * 4, junctions[:2]))
    assert not [node for node in graph.nodes if node.node_kind == contract.BUS]


def test_independent_drawings_on_one_page_get_different_islands():
    page = _FakePage([[0, 0, 30, 0], [10, 0, 10, 20], [200, 0, 230, 0], [210, 0, 210, 20]])
    junctions = [
        conductors.Junction("j0", (10.0, 0.0), contract.JUNCTION_DOT, (0, 1)),
        conductors.Junction("j1", (210.0, 0.0), contract.JUNCTION_DOT, (2, 3)),
    ]
    graph = topology.build_page(page, _facts(page, [True] * 4, junctions))
    assert graph.counters["islands"] >= 2


# ---------------------------------------------------------------------------
# the frozen control sheet
# ---------------------------------------------------------------------------


def _control():
    from experiments.function_lineage_v3 import corpus as frozen

    paths = frozen.document_paths("p19cd7f695a", "RIGHT")
    if not paths["pdf"].is_file():
        pytest.skip("frozen corpus is not present")
    from experiments.pdf_evidence_v1 import extraction
    from experiments.pdf_evidence_v2 import pipeline

    profile = extraction.document_profile(str(paths["pdf"]), frozen.markdown_pages(paths["markdown"]))
    return pipeline.analyse("IOS1.1/RIGHT", str(paths["pdf"]), 20, profile)


@pytest.mark.slow
def test_the_control_sheet_yields_a_bus_with_feeders_reaching_it():
    result = _control()
    buses = [node for node in result.topology.nodes if node.node_kind == contract.BUS]
    assert buses, "the single-line diagram must produce at least one bus"
    adjacency = identity.electrical_adjacency(result.topology)
    distance = identity.hops_to_bus(result.topology, adjacency)
    named = identity.bound_marks(result.topology)
    feeders = [
        node_id for node_id, texts in named.items()
        if any(text.startswith("ГРЩ1-") for text in texts)
    ]
    assert feeders, "the feeder cable marks must bind to a conductor"
    assert all(distance.get(node_id) is not None for node_id in feeders)


@pytest.mark.slow
def test_the_control_sheet_refuses_every_furniture_stroke():
    result = _control()
    nature = np.asarray(result.facts.nature)
    for value in (contract.TABLE_GRID, contract.FRAME, contract.TEXT_UNDERLINE):
        assert not (nature == value)[result.facts.conductor].any()


@pytest.mark.slow
def test_the_control_sheet_refuses_the_direction_word():
    from experiments.pdf_evidence_v2 import direction

    result = _control()
    trap = direction.keyword_trap(result.topology)
    assert trap["nodes_a_keyword_rule_would_direct"] > 0
    assert trap["edges_this_layer_directs_from_a_keyword"] == 0
    assert all(
        edge.direction_status != contract.PROVEN or edge.direction_evidence == contract.ARROWHEAD
        for edge in result.topology.edges
    )
