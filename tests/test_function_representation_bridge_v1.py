"""Controls for the FUNCTION REPRESENTATION BRIDGE V1 layer.

Two kinds, for the reason the previous tracks' controls give: most are unit
tests on synthetic pages, because a rule that only holds on one corpus is not a
rule; a few read the frozen control sheet, and those are skipped rather than
failed where the corpus is not present.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_representation_bridge_v1 import (
    assembly,
    assembly_facts,
    bridge,
    contract,
    controls,
    membership,
    representation,
    signature,
)
from experiments.function_topology_v1 import aggregation as topology_aggregation
from experiments.pdf_evidence_v1.contract import ContractViolation
from experiments.pdf_evidence_v2.contract import (
    BUS,
    ELECTRICAL_CONNECTION,
    EQUIPMENT,
    FEEDER,
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    PROVEN_CONNECTION,
    RUNS_ALONG_SINGLE_CONDUCTOR,
    TERMINAL,
    TopologyEdge,
    TopologyNode,
    UNDIRECTED,
)
from experiments.pdf_evidence_v2.topology import PageTopology, assign_islands


# ---------------------------------------------------------------------------
# a synthetic page
# ---------------------------------------------------------------------------


class _Region:
    def __init__(self, region_id, kind, bbox, rows=(), columns=()):
        self.region_id = region_id
        self.kind = kind
        self.bbox = list(bbox)
        self.rows = list(rows)
        self.columns = list(columns)


def _node(node_id, kind, page=1, symbol=None, labels=(), refs=()):
    return TopologyNode(
        node_id=node_id, document="DOC/LEFT", physical_page=page, node_kind=kind,
        bbox=(0.0, 0.0, 1.0, 1.0), anchor=(0.5, 0.5), symbol_signature=symbol,
        labels=tuple(labels), evidence_refs=tuple(refs),
    )


def _edge(edge_id, source, target, kind=ELECTRICAL_CONNECTION, claim=PROVEN_CONNECTION):
    return TopologyEdge(
        edge_id=edge_id, document="DOC/LEFT", physical_page=1,
        from_node_id=source, to_node_id=target, edge_kind=kind,
        connection_claim=claim, direction_status=UNDIRECTED,
    )


def _page_result(nodes, edges, labels, regions, ownership, conductors=4):
    topology = PageTopology(document="DOC/LEFT", page=1, nodes=list(nodes), edges=list(edges))
    assign_islands(topology)
    data = SimpleNamespace(
        document="DOC/LEFT", page=1, width=1000.0, height=800.0,
        regions=list(regions), labels=list(labels),
        strokes=SimpleNamespace(edges=np.zeros((conductors, 4))),
    )
    facts = SimpleNamespace(conductor=np.ones(conductors, dtype=bool))
    return SimpleNamespace(
        data=data, facts=facts, topology=topology, page=1, v1_ownership=dict(ownership))


def _label(label_id, text, bbox=(0.0, 0.0, 10.0, 10.0)):
    return {
        "label_id": label_id, "text": text, "bbox": list(bbox),
        "size": 4.0, "vertical": False, "decoding": "DECODED_NATIVE",
        "provenance": "NATIVE_PDF_TEXT",
    }


def _own(kind, region_id=None, cell=None):
    return {"ownership": kind, "region_id": region_id, "cell": list(cell) if cell else None,
            "applicability": "FRAGMENT_LOCAL"}


def _table_page():
    """A ruled lattice with a printed header row and one riser grid without one."""
    labels = [
        _label("l:p0001:t00000", "ВРУ1"),
        _label("l:p0001:t00001", "Рр,кВт"),
        _label("l:p0001:t00002", "449.3"),
        _label("l:p0001:t00003", "1 этаж"),
    ]
    ownership = {
        "l:p0001:t00000": _own("TABLE_CELL", "reg_0001_00002", (0, 0)),
        "l:p0001:t00001": _own("TABLE_CELL", "reg_0001_00002", (1, 0)),
        "l:p0001:t00002": _own("TABLE_CELL", "reg_0001_00002", (1, 0)),
        "l:p0001:t00003": _own("TABLE_CELL", "reg_0001_00009", (1, 0)),
    }
    regions = [
        _Region("reg_0001_00002", "TABLE", (0, 0, 60, 100), rows=(0, 10, 20), columns=(0, 60)),
        _Region("reg_0001_00009", "TABLE", (100, 0, 400, 300), rows=(0, 10, 20), columns=(0, 300)),
    ]
    return _page_result([], [], labels, regions, ownership, conductors=0)


def _schematic_page():
    """A bus, a breaker and two feeders, with a mark bound to one of them."""
    nodes = [
        _node("n:0001:r00001", BUS),
        _node("n:0001:r00002", EQUIPMENT, symbol="sym_a"),
        _node("n:0001:r00003", FEEDER),
        _node("n:0001:r00004", FEEDER),
        _node("n:0001:a00001", LABEL_ANCHOR, labels=("ГРЩ1",), refs=("label:l:p0001:t00010",)),
    ]
    edges = [
        _edge("e1", "n:0001:r00001", "n:0001:r00002"),
        _edge("e2", "n:0001:r00002", "n:0001:r00003"),
        _edge("e3", "n:0001:r00001", "n:0001:r00004"),
        _edge("e4", "n:0001:a00001", "n:0001:r00003", kind=LABEL_CONNECTION),
    ]
    labels = [
        _label("l:p0001:t00010", "ГРЩ1"),
        _label("l:p0001:t00011", "ППГнг(А)-HF 5х185"),
    ]
    ownership = {
        "l:p0001:t00010": _own("CONNECTED_CALLOUT", "reg_0001_00050"),
        "l:p0001:t00011": _own("CONNECTED_CALLOUT", "reg_0001_00050"),
    }
    regions = [_Region("reg_0001_00050", "EDGE_GROUP", (0, 0, 100, 100))]
    return _page_result(nodes, edges, labels, regions, ownership)


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_no_vocabulary_value_states_absence():
    vocabularies = (
        contract.REPRESENTATION_TYPE + contract.ASSEMBLY_CHANNEL + contract.ASSEMBLY_KIND
        + contract.MEMBERSHIP_STATUS + contract.MEMBERSHIP_CHANNEL
        + contract.MEMBERSHIP_CAUSE + contract.SCOPE_COMPOSITION + contract.FACT_KEYS
        + contract.SIGNATURE_TIERS + contract.BRIDGE_COVERAGE_CLASS
    )
    for value in vocabularies:
        for term in ("ABSENT", "ABSENCE", "REMOVED", "DELETED", "MISSING", "NOT_FOUND"):
            assert term not in value.upper(), value


def test_contract_document_passes_its_own_guards():
    document = contract.contract_document()
    contract.assert_no_absence_vocabulary(document)
    contract.assert_no_similarity_evidence(document)


def test_a_resemblance_key_is_refused():
    with pytest.raises(ContractViolation):
        contract.assert_no_similarity_evidence({"rows": [{"match_score": 0.9}]})


def test_a_ranking_key_is_refused_even_when_nested():
    with pytest.raises(ContractViolation):
        contract.assert_no_similarity_evidence({"a": {"b": [{"confidence": "HIGH"}]}})


def test_an_assembly_without_a_drawn_container_is_refused():
    item = contract.FunctionalAssembly(
        assembly_id="fasm_x", document="DOC/LEFT", pair_id="p", side="LEFT",
        physical_page=1, assembly_channel=contract.DRAWN_STROKE_GROUP,
        representation_type=contract.TEXT, assembly_kind=contract.UNKNOWN,
        membership_status=contract.PARTIAL, member_label_ids=("l:p0001:t00000",),
    )
    with pytest.raises(ContractViolation):
        contract.assert_assembly_is_a_drawn_container([item])


def test_a_weak_channel_may_not_be_proven():
    item = contract.FunctionalAssembly(
        assembly_id="fasm_x", document="DOC/LEFT", pair_id="p", side="LEFT",
        physical_page=1, assembly_channel=contract.DRAWN_TABLE_LATTICE,
        representation_type=contract.TABLE, assembly_kind=contract.PANEL,
        membership_status=contract.PROVEN, source_region_ids=("reg_0001_00002",),
        table_ids=("reg_0001_00002",), member_label_ids=("l:p0001:t00000",),
    )
    with pytest.raises(ContractViolation):
        contract.assert_assembly_is_a_drawn_container([item])


def test_one_printed_string_may_not_belong_to_two_assemblies():
    shared = ("l:p0001:t00000",)
    items = [
        contract.FunctionalAssembly(
            assembly_id=f"fasm_{index}", document="DOC/LEFT", pair_id="p", side="LEFT",
            physical_page=1, assembly_channel=contract.DRAWN_STROKE_GROUP,
            representation_type=contract.TEXT, assembly_kind=contract.UNKNOWN,
            membership_status=contract.PARTIAL, source_region_ids=(f"reg_{index}",),
            member_label_ids=shared)
        for index in range(2)
    ]
    with pytest.raises(ContractViolation):
        contract.assert_one_owner_per_label(items)


def test_a_sheet_wide_assembly_is_refused_when_the_page_has_others():
    items = [
        contract.FunctionalAssembly(
            assembly_id="fasm_a", document="DOC/LEFT", pair_id="p", side="LEFT",
            physical_page=1, assembly_channel=contract.DRAWN_STROKE_GROUP,
            representation_type=contract.TEXT, assembly_kind=contract.UNKNOWN,
            membership_status=contract.PARTIAL, source_region_ids=("reg_a",),
            member_label_ids=("a", "b", "c")),
        contract.FunctionalAssembly(
            assembly_id="fasm_b", document="DOC/LEFT", pair_id="p", side="LEFT",
            physical_page=1, assembly_channel=contract.DRAWN_STROKE_GROUP,
            representation_type=contract.TEXT, assembly_kind=contract.UNKNOWN,
            membership_status=contract.PARTIAL, source_region_ids=("reg_b",),
            member_label_ids=("d",)),
    ]
    with pytest.raises(ContractViolation):
        contract.assert_no_sheet_wide_assembly(items, {("DOC/LEFT", 1): 3})


def test_a_kind_the_layer_refuses_to_decide_may_not_be_emitted():
    item = contract.FunctionalAssembly(
        assembly_id="fasm_x", document="DOC/LEFT", pair_id="p", side="LEFT",
        physical_page=1, assembly_channel=contract.DRAWN_TABLE_LATTICE,
        representation_type=contract.TABLE, assembly_kind=contract.RISER_GROUP,
        membership_status=contract.PARTIAL, table_ids=("reg_a",),
        member_label_ids=("a",))
    with pytest.raises(ContractViolation):
        contract.assert_closed_vocabularies([item], [], [])


def test_a_fact_without_evidence_is_refused():
    fact = contract.AssemblyFact(
        assembly_id="fasm_x", key="printed_string_count", value=3,
        source_representation=contract.TABLE, applicability="FRAGMENT_LOCAL",
        provenance="NATIVE_PDF_TEXT", evidence_refs=())
    with pytest.raises(ContractViolation):
        contract.assert_closed_vocabularies([], [], [fact])


def test_a_proven_membership_on_a_capped_channel_is_refused():
    row = contract.AssemblyMembership(
        membership_id="fmem_x", pair_id="p", project="P", side="LEFT",
        function_id="f", scope_id=None, fragment_id=None, physical_page=1,
        primary_mark="ГРЩ1", membership_status=contract.PROVEN,
        membership_channel=contract.SHEET_MARK_WITH_ONE_ASSEMBLY,
        assembly_id="fasm_x", evidence_refs=("x",))
    with pytest.raises(ContractViolation):
        contract.assert_membership_evidence([row])


def test_an_unjoined_membership_may_not_name_an_assembly():
    row = contract.AssemblyMembership(
        membership_id="fmem_x", pair_id="p", project="P", side="LEFT",
        function_id="f", scope_id=None, fragment_id=None, physical_page=1,
        primary_mark=None, membership_status=contract.UNKNOWN,
        assembly_id="fasm_x")
    with pytest.raises(ContractViolation):
        contract.assert_membership_evidence([row])


def test_a_signature_carrying_an_address_is_refused():
    with pytest.raises(ContractViolation):
        contract.assert_signature_representation_neutral(
            [{"signatures": {"NAMES_ONLY": "asig_a"},
              "ingredients": {"owner_designation": "n:0001:r00001"}}])


# ---------------------------------------------------------------------------
# containers
# ---------------------------------------------------------------------------


def test_a_lattice_with_a_printed_header_row_becomes_a_table_container():
    page = representation.read_page("p", "LEFT", _table_page())
    tables = [item for item in page.containers
              if item.channel == contract.DRAWN_TABLE_LATTICE]
    assert [item.container_id for item in tables] == ["reg_0001_00002"]
    assert tables[0].column_captions == ("ВРУ1",)


def test_a_grid_whose_first_row_prints_nothing_is_not_a_table():
    page = representation.read_page("p", "LEFT", _table_page())
    assert "reg_0001_00009" not in {item.container_id for item in page.containers}
    assert page.counters["lattices_refused_for_lacking_a_header_row"] == 1


def test_a_header_row_must_start_at_the_first_column():
    assert representation._header_row_columns({(0, 0): ["a"], (0, 1): ["b"]}) == [0, 1]
    assert representation._header_row_columns({(0, 1): ["a"], (0, 3): ["b"]}) is None
    assert representation._header_row_columns({(1, 0): ["a"]}) is None


def test_a_string_bound_to_a_conductor_is_handed_to_the_schematic():
    page = representation.read_page("p", "LEFT", _schematic_page())
    schematic = [item for item in page.containers
                 if item.channel == contract.PROVEN_CONNECTED_COMPONENT]
    stroke = [item for item in page.containers
              if item.channel == contract.DRAWN_STROKE_GROUP]
    assert schematic and "l:p0001:t00010" in schematic[0].label_ids
    assert all("l:p0001:t00010" not in item.label_ids for item in stroke)


def test_no_printed_string_belongs_to_two_containers_of_one_page():
    page = representation.read_page("p", "LEFT", _schematic_page())
    built = assembly.build_page(page)
    contract.assert_one_owner_per_label(built)
    seen: set[str] = set()
    for item in built:
        assert not (seen & set(item.member_label_ids))
        seen |= set(item.member_label_ids)


# ---------------------------------------------------------------------------
# assemblies
# ---------------------------------------------------------------------------


def test_a_schematic_with_a_bus_is_a_board():
    page = representation.read_page("p", "LEFT", _schematic_page())
    built = assembly.build_page(page)
    schematic = [item for item in built
                 if item.assembly_channel == contract.PROVEN_CONNECTED_COMPONENT]
    assert schematic and schematic[0].assembly_kind == contract.BOARD
    assert schematic[0].membership_status == contract.PROVEN


def test_a_one_caption_table_is_a_panel_and_names_itself():
    page = representation.read_page("p", "LEFT", _table_page())
    built = assembly.build_page(page)
    table = [item for item in built if item.representation_type == contract.TABLE][0]
    assert table.assembly_kind == contract.PANEL
    assert table.owner_designation == "ВРУ1"
    assert "BPY1" in table.named_designations


def test_a_multi_caption_table_names_nothing_and_is_a_schedule():
    result = _table_page()
    result.data.labels.append(_label("l:p0001:t00004", "Поз."))
    result.data.labels.append(_label("l:p0001:t00005", "Наименование"))
    result.v1_ownership["l:p0001:t00004"] = _own("TABLE_CELL", "reg_0001_00009", (0, 0))
    result.v1_ownership["l:p0001:t00005"] = _own("TABLE_CELL", "reg_0001_00009", (0, 1))
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    schedule = [item for item in built if "reg_0001_00009" in item.table_ids][0]
    assert schedule.assembly_kind == contract.SYSTEM_GROUP
    assert schedule.owner_designation is None


def test_an_assembly_identifier_does_not_depend_on_the_order_of_the_page():
    page = representation.read_page("p", "LEFT", _table_page())
    first = [item.assembly_id for item in assembly.build_page(page)]
    page.containers.reverse()
    second = [item.assembly_id for item in assembly.build_page(page)]
    assert sorted(first) == sorted(second)


# ---------------------------------------------------------------------------
# facts
# ---------------------------------------------------------------------------


def test_a_ruled_cell_attaches_a_quantity_to_the_thing_it_names():
    result = _table_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, result)
    quantities = [fact for fact in facts if fact.key == "quantity_facets"]
    assert quantities and quantities[0].value["demand_active_power_kw"] == [449.3]


def test_a_loose_number_is_not_attached_to_anything():
    """The same number printed with no container attributes to no assembly."""
    result = _table_page()
    result.data.labels.append(_label("l:p0001:t00050", "Рр=449,3 кВт"))
    result.v1_ownership["l:p0001:t00050"] = _own("NO_OWNERSHIP")
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, result)
    for fact in facts:
        assert "l:p0001:t00050" not in fact.evidence_refs


def test_the_latin_current_row_is_read_through_the_production_parser():
    assert assembly_facts.cell_quantity_facets({(1, 0): ("Ip,A", "717.3")}) == {
        "maximum_calculated_current_a": [717.3]
    }


def test_a_surname_from_the_title_block_is_not_a_cable():
    assert assembly_facts.cable_facets(["САФИН", "ДЖАМИЛОВ", "Лист"]) == []


def test_a_cable_needs_its_conductors_and_its_section():
    rows = assembly_facts.cable_facets(["ППГнг(А)-HF 5х185"])
    assert len(rows) == 1
    assert rows[0]["cores"] == 5 and rows[0]["section_mm2"] == 185.0


def test_every_fact_carries_a_representation_and_evidence():
    result = _schematic_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, result)
    assert facts
    for fact in facts:
        assert fact.source_representation in contract.REPRESENTATION_TYPE
        assert fact.evidence_refs
        assert fact.key in contract.FACT_KEYS


# ---------------------------------------------------------------------------
# signatures
# ---------------------------------------------------------------------------


def test_a_signature_survives_a_change_of_page_and_of_identifiers():
    page = representation.read_page("p", "LEFT", _table_page())
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, _table_page())
    first = signature.annotate(built, facts)

    moved = _table_page()
    moved.data.page = 7
    moved.topology.page = 7
    for label in moved.data.labels:
        label["bbox"] = [value + 500 for value in label["bbox"]]
    moved_page = representation.read_page("p", "LEFT", moved)
    moved_page.physical_page = 7
    moved_built = assembly.build_page(moved_page)
    moved_facts = assembly_facts.page_facts(moved_page, moved_built, moved)
    second = signature.annotate(moved_built, moved_facts)
    assert (
        first[0]["signatures"][contract.NAMES_ONLY]
        == second[0]["signatures"][contract.NAMES_ONLY]
    )


def test_signature_ingredients_carry_no_address():
    page = representation.read_page("p", "LEFT", _schematic_page())
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, _schematic_page())
    rows = signature.annotate(built, facts)
    contract.assert_signature_representation_neutral(rows)


# ---------------------------------------------------------------------------
# membership
# ---------------------------------------------------------------------------


def _passport(**fields):
    base = {
        "source_sheet": {"physical_page": 1, "title": ""},
        "function_class": "ELECTRICAL_DISTRIBUTION",
    }
    base.update(fields)
    return base


def test_a_value_printed_in_one_container_joins_the_function_to_it():
    result = _table_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    row = membership.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", scope_id=None,
        fragment_id=None, passport=_passport(stable_entities=["Рр,кВт"]),
        page=page, assemblies=built)
    assert row.membership_status == contract.PARTIAL
    assert row.membership_channel == contract.DOCUMENTED_VALUE_IN_ONE_ASSEMBLY


def test_a_value_printed_in_two_containers_joins_nothing():
    result = _table_page()
    result.data.labels.append(_label("l:p0001:t00006", "Рр,кВт"))
    result.v1_ownership["l:p0001:t00006"] = _own("TABLE_CELL", "reg_0001_00009", (0, 0))
    result.data.labels.append(_label("l:p0001:t00007", "заголовок"))
    result.v1_ownership["l:p0001:t00007"] = _own("TABLE_CELL", "reg_0001_00009", (0, 1))
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    row = membership.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", scope_id=None,
        fragment_id=None, passport=_passport(stable_entities=["Рр,кВт"]),
        page=page, assemblies=built)
    assert row.membership_status == contract.UNKNOWN
    assert row.assembly_id is None


def test_a_short_value_may_not_join_by_being_contained():
    result = _table_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    row = membership.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", scope_id=None,
        fragment_id=None, passport=_passport(systems=["Рр"]),
        page=page, assemblies=built, minimum_chars=8)
    assert row.membership_status == contract.UNKNOWN


def test_the_page_alone_never_joins_a_function():
    result = _table_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    row = membership.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", scope_id=None,
        fragment_id=None, passport=_passport(systems=["не напечатанная строка"]),
        page=page, assemblies=built)
    assert row.membership_status == contract.UNKNOWN
    assert row.assembly_id is None
    assert row.cause in contract.MEMBERSHIP_CAUSE


def test_a_mark_bound_to_a_conductor_proves_the_membership():
    result = _schematic_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    passport = _passport(source_sheet={"physical_page": 1, "title": "Схема ГРЩ1"})
    row = membership.bind_function(
        pair_id="p", project="P", side="LEFT", function_id="f", scope_id=None,
        fragment_id=None, passport=passport, page=page, assemblies=built)
    assert row.membership_status == contract.PROVEN
    assert row.membership_channel == contract.PROVEN_TOPOLOGY_OWNERSHIP
    contract.assert_membership_evidence([row])


# ---------------------------------------------------------------------------
# the bridge
# ---------------------------------------------------------------------------


def _membership(side, function_id, assembly_id, status=contract.PARTIAL):
    return contract.AssemblyMembership(
        membership_id=f"fmem_{side}_{function_id}", pair_id="p", project="P",
        side=side, function_id=function_id, scope_id=None, fragment_id=None,
        physical_page=1, primary_mark=None, membership_status=status,
        membership_channel=contract.DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
        assembly_id=assembly_id, evidence_refs=("value:x",))


def _task(left_id, right_id):
    return {
        "task_id": "task_1", "pair_id": "p", "corpus": "P", "scope_id": "scope_1",
        "relation_types": ["CONTINUED_1_TO_1"],
        "candidates": [{"component_mapping": [
            {"left_function_id": left_id, "right_function_id": right_id}]}],
    }


def _tiny_assembly(assembly_id, side, representation_type=contract.TABLE):
    return contract.FunctionalAssembly(
        assembly_id=assembly_id, document=f"P/{side}", pair_id="p", side=side,
        physical_page=1, assembly_channel=contract.DRAWN_TABLE_LATTICE,
        representation_type=representation_type, assembly_kind=contract.PANEL,
        membership_status=contract.PARTIAL, table_ids=("reg_a",),
        member_label_ids=("l:p0001:t00000",))


def test_a_task_with_facts_on_one_side_only_is_not_a_disagreement():
    coverage = bridge.coverage_audit(
        [_task("fl", "fr")],
        [_membership("LEFT", "fl", "fasm_l")],
        [_tiny_assembly("fasm_l", "LEFT")],
        {("p", "LEFT"): {}, ("p", "RIGHT"): {}},
    )
    assert coverage["by_coverage_class"] == {"ASSEMBLY_FACTS_LEFT_ONLY": 1}
    contract.assert_no_absence_vocabulary(coverage)


def test_both_sides_are_counted_only_when_both_reach_an_assembly():
    coverage = bridge.coverage_audit(
        [_task("fl", "fr")],
        [_membership("LEFT", "fl", "fasm_l"), _membership("RIGHT", "fr", "fasm_r")],
        [_tiny_assembly("fasm_l", "LEFT"), _tiny_assembly("fasm_r", "RIGHT")],
        {("p", "LEFT"): {}, ("p", "RIGHT"): {}},
    )
    assert coverage["by_coverage_class"] == {"ASSEMBLY_FACTS_BOTH_SIDES": 1}
    assert coverage["by_representation_pair"] == {"TABLE_TO_TABLE": 1}


def test_two_counts_that_agree_are_not_a_meeting():
    schematic = contract.FunctionalAssembly(
        assembly_id="fasm_s", document="P/LEFT", pair_id="p", side="LEFT",
        physical_page=1, assembly_channel=contract.PROVEN_CONNECTED_COMPONENT,
        representation_type=contract.SCHEMATIC, assembly_kind=contract.BOARD,
        membership_status=contract.PROVEN, topology_subgraph_ids=("fts_a",),
        member_node_ids=("n:0001:r00001",))
    table = _tiny_assembly("fasm_t", "RIGHT")
    facts = [
        contract.AssemblyFact("fasm_s", "outgoing_branch_designations", [],
                              contract.SCHEMATIC, "FRAGMENT_LOCAL", "NATIVE_PDF_VECTOR", ("x",)),
        contract.AssemblyFact("fasm_s", "feeder_count", 30,
                              contract.SCHEMATIC, "FRAGMENT_LOCAL", "NATIVE_PDF_VECTOR", ("x",)),
        contract.AssemblyFact("fasm_t", "table_row_leaders", [],
                              contract.TABLE, "FRAGMENT_LOCAL", "NATIVE_PDF_TEXT", ("x",)),
        contract.AssemblyFact("fasm_t", "table_row_count", 30,
                              contract.TABLE, "FRAGMENT_LOCAL", "NATIVE_PDF_TEXT", ("x",)),
    ]
    audit = bridge.normalization_audit([schematic, table], facts)
    assert audit["pairs_meeting_on_printed_designations"] == 0
    assert audit["pairs_whose_counts_coincide_without_a_shared_designation"] == 1


# ---------------------------------------------------------------------------
# controls
# ---------------------------------------------------------------------------


def test_the_safety_table_reports_zero_on_a_clean_page():
    result = _schematic_page()
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, result)
    table = controls.safety_table(built, [], {("p", "LEFT"): {1: page}}, facts)
    assert all(value == 0 for value in table["safety"].values())
    assert table["frozen_layers"]["v2_topology_rules_changed"] == 0


def test_two_assemblies_of_one_page_share_no_printed_string():
    result = _table_page()
    result.data.labels.append(_label("l:p0001:t00008", "Поз."))
    result.v1_ownership["l:p0001:t00008"] = _own("TABLE_CELL", "reg_0001_00009", (0, 0))
    page = representation.read_page("p", "LEFT", result)
    built = assembly.build_page(page)
    control = controls.control_b_several_assemblies_on_one_page(built)
    assert control["printed_strings_claimed_by_two_assemblies"] == 0


# ---------------------------------------------------------------------------
# the frozen control sheet
# ---------------------------------------------------------------------------


def _control_sheet():
    from experiments.function_lineage_v3 import corpus as frozen_corpus
    from experiments.pdf_evidence_v1 import extraction as v1_extraction
    from experiments.pdf_evidence_v2 import pipeline as v2_pipeline

    paths = frozen_corpus.document_paths("p19cd7f695a", "RIGHT")
    if not Path(paths["pdf"]).is_file():
        pytest.skip("the frozen corpus is not present")
    profile = v1_extraction.document_profile(
        str(paths["pdf"]), frozen_corpus.markdown_pages(paths["markdown"]))
    return v2_pipeline.analyse("IOS1.1/RIGHT", str(paths["pdf"]), 20, profile)


def test_the_control_sheet_carries_nine_named_parameter_blocks():
    result = _control_sheet()
    page = representation.read_page("p19cd7f695a", "RIGHT", result)
    built = assembly.build_page(page)
    panels = [item for item in built if item.assembly_kind == contract.PANEL]
    assert len(panels) == 9
    assert {item.owner_designation for item in panels} >= {"ВРУ1", "ВРУ3", "ШУ-ХЦ"}


def test_the_control_sheet_attaches_its_numbers_through_ruled_cells():
    result = _control_sheet()
    page = representation.read_page("p19cd7f695a", "RIGHT", result)
    built = assembly.build_page(page)
    facts = assembly_facts.page_facts(page, built, result)
    by_assembly = {item.assembly_id: item for item in built}
    quantities = {
        by_assembly[fact.assembly_id].owner_designation: fact.value
        for fact in facts if fact.key == "quantity_facets"
        and by_assembly[fact.assembly_id].assembly_kind == contract.PANEL
    }
    assert quantities.get("ВРУ1", {}).get("demand_active_power_kw") == [414.5, 449.3]


def test_the_control_sheet_keeps_its_board_aggregate_whole():
    result = _control_sheet()
    page = representation.read_page("p19cd7f695a", "RIGHT", result)
    built = assembly.build_page(page)
    boards = [item for item in built if item.assembly_kind == contract.BOARD]
    assert boards
    assert max(len(item.member_node_ids) for item in boards) > 1000
