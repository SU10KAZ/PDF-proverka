"""Controls for the FUNCTION ASSEMBLY MEMBERSHIP CERTIFICATE V1 layer.

Synthetic pages, because a rule that only holds on one corpus is not a rule.
Every channel has a positive case and the negative case that must refuse it;
the contract guards are exercised directly; and the decoy machinery is checked
to count a stranger's certificate and nothing else.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_assembly_membership_v1 import (
    certificate,
    contract,
    controls,
    evidence,
    gate,
    scopes,
)
from experiments.function_representation_bridge_v1.contract import (
    DRAWN_STROKE_GROUP,
    DRAWN_TABLE_LATTICE,
    FunctionalAssembly,
    PROVEN_CONNECTED_COMPONENT,
)
from experiments.function_topology_v1.contract import COMMON_OWNER_LABEL
from experiments.pdf_evidence_v1.contract import ContractViolation

PAIR, PROJECT, SIDE = "p_test", "TEST", "LEFT"
DOC = f"{PROJECT}/{SIDE}"


# ---------------------------------------------------------------------------
# a synthetic page
# ---------------------------------------------------------------------------


def _label(label_id, text, cell=None, region=None, bbox=(0, 0, 10, 4)):
    return {
        "label_id": label_id, "text": text, "bbox": list(bbox), "provenance": "NATIVE_PDF_TEXT",
        "ownership": "TABLE_CELL" if cell else ("DIRECT_CONTAINMENT" if region else "NO_OWNERSHIP"),
        "region_id": region, "cell": list(cell) if cell else None,
    }


def _assembly(assembly_id, channel, labels=(), nodes=(), owner=None, page=1, kind="UNKNOWN"):
    return FunctionalAssembly(
        assembly_id=assembly_id, document=DOC, pair_id=PAIR, side=SIDE, physical_page=page,
        assembly_channel=channel,
        representation_type={PROVEN_CONNECTED_COMPONENT: "SCHEMATIC", DRAWN_TABLE_LATTICE: "TABLE"}.get(channel, "TEXT"),
        assembly_kind=kind, membership_status="PROVEN" if channel == PROVEN_CONNECTED_COMPONENT else "PARTIAL",
        source_region_ids=() if channel == PROVEN_CONNECTED_COMPONENT else (f"reg_{assembly_id}",),
        topology_subgraph_ids=(f"fts_{assembly_id}",) if channel == PROVEN_CONNECTED_COMPONENT else (),
        table_ids=(f"reg_{assembly_id}",) if channel == DRAWN_TABLE_LATTICE else (),
        member_label_ids=tuple(labels), member_node_ids=tuple(nodes), owner_designation=owner,
    )


def _page(labels, nodes_of_mark=None, ownership=None, sheet_marks=(), page=1):
    aggregation = SimpleNamespace(
        nodes_of_mark=dict(nodes_of_mark or {}),
        mark_ownership=dict(ownership or {mark: COMMON_OWNER_LABEL for mark in (nodes_of_mark or {})}),
        sheet_marks=set(sheet_marks), subgraphs=[],
    )
    return SimpleNamespace(
        document=DOC, pair_id=PAIR, side=SIDE, physical_page=page,
        labels_by_id={row["label_id"]: row for row in labels}, aggregation=aggregation,
        containers=[], printed_strings=len(labels),
    )


def _passport(title, page=1, **fields):
    base = {
        "function_id": "f1", "function_class": "ELECTRICAL_DISTRIBUTION",
        "source_sheet": {"title": title, "physical_page": page, "side": SIDE},
        "stable_entities": [], "consumers": [], "equipment_roles": [],
        "systems": ["электроснабжение"], "function_fragment_ids": ["frag_1"],
    }
    base.update(fields)
    return base


def _certify(passport, page, assemblies, fragments=(), facts=None, **kw):
    return certificate.certify_function(
        pair_id=PAIR, project=PROJECT, side=SIDE, function_id="f1", scope_id="s1",
        fragment_ids=("frag_1",), passport=passport, fragments=list(fragments),
        page=page, assemblies=assemblies, facts_by_assembly=facts or {}, **kw,
    )


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_no_vocabulary_value_states_absence():
    vocabularies = (
        contract.CERTIFICATE_STATUS + contract.CERTIFICATE_CHANNEL + contract.CERTIFICATE_CAUSE
        + contract.SCOPE_CAUSE + contract.ASSEMBLY_COMPOSITION + contract.RELATION_KIND
    )
    for value in vocabularies:
        for term in ("ABSENT", "ABSENCE", "REMOVED", "DELETED", "MISSING", "NOT_FOUND"):
            assert term not in value.upper(), value


def test_contract_document_passes_its_own_guards():
    document = contract.contract_document()
    contract.assert_no_absence_vocabulary(document)
    contract.assert_no_similarity_evidence(document)


def test_a_certificate_on_a_support_channel_is_refused():
    row = contract.MembershipCertificate(
        certificate_id="c", pair_id=PAIR, project=PROJECT, side=SIDE, function_id="f1",
        scope_id=None, fragment_ids=(), physical_page=1, primary_mark="ЩО2",
        status=contract.CERTIFIED, channel=contract.MARK_BOUND_TO_ONE_MEMBER,
        relation_kind=contract.IS_DRAWN_AS, assembly_id="a", certified_assembly_ids=("a",),
        structural_basis=("x",), evidence_refs=("y",))
    with pytest.raises(ContractViolation):
        contract.assert_certificate_evidence([row])


def test_a_certificate_without_a_basis_is_refused():
    row = contract.MembershipCertificate(
        certificate_id="c", pair_id=PAIR, project=PROJECT, side=SIDE, function_id="f1",
        scope_id=None, fragment_ids=(), physical_page=1, primary_mark="ЩО2",
        status=contract.CERTIFIED, channel=contract.TOPOLOGY_OWNER_MARK_ON_MEMBERS,
        relation_kind=contract.IS_DRAWN_AS, assembly_id="a", certified_assembly_ids=("a",))
    with pytest.raises(ContractViolation):
        contract.assert_certificate_evidence([row])


def test_a_contradiction_needs_two_proofs():
    row = contract.MembershipCertificate(
        certificate_id="c", pair_id=PAIR, project=PROJECT, side=SIDE, function_id="f1",
        scope_id=None, fragment_ids=(), physical_page=1, primary_mark="ЩО2",
        status=contract.CONTRADICTORY, conflict={"evidence_refs": ["one"]})
    with pytest.raises(ContractViolation):
        contract.assert_certificate_evidence([row])


def test_a_certified_container_must_lie_on_the_function_page():
    row = contract.MembershipCertificate(
        certificate_id="c", pair_id=PAIR, project=PROJECT, side=SIDE, function_id="f1",
        scope_id=None, fragment_ids=(), physical_page=1, primary_mark="ЩО2",
        status=contract.CERTIFIED, channel=contract.TOPOLOGY_OWNER_MARK_ON_MEMBERS,
        relation_kind=contract.IS_DRAWN_AS, assembly_id="a", certified_assembly_ids=("a",),
        structural_basis=("x",), evidence_refs=("y",))
    with pytest.raises(ContractViolation):
        contract.assert_certified_container_lies_on_the_function_page(
            [row], {"a": (DOC, 2)})


# ---------------------------------------------------------------------------
# channel 1 — the mark bound to members of an island
# ---------------------------------------------------------------------------


def test_a_mark_bound_to_two_members_of_one_island_certifies():
    labels = [_label("l1", "ЩО-2"), _label("l2", "ЩО-2 гр.1")]
    page = _page(labels, nodes_of_mark={"ЩO2": ["n1", "n2"]})
    island = _assembly("isl", PROVEN_CONNECTED_COMPONENT, labels=("l1", "l2"), nodes=("n1", "n2", "n3"), kind="BOARD")
    row = _certify(_passport("Однолинейная схема ЩО-2"), page, [island])
    assert row.status == contract.CERTIFIED
    assert row.channel == contract.TOPOLOGY_OWNER_MARK_ON_MEMBERS
    assert row.relation_kind == contract.IS_DRAWN_AS
    assert row.assembly_id == "isl"


def test_a_mark_bound_along_one_feeder_of_a_board_named_otherwise_only_supports():
    labels = [_label("l1", "ГРЩ1"), _label("l2", "ГРЩ1 с.ш.2"), _label("l3", "ХМ1")]
    page = _page(labels, nodes_of_mark={"ГPЩ1": ["n1", "n2"], "XM1": ["n3"]})
    island = _assembly("isl", PROVEN_CONNECTED_COMPONENT, labels=("l1", "l2", "l3"),
                       nodes=("n1", "n2", "n3"), owner="ГРЩ1", kind="BOARD")
    row = _certify(_passport("Схема ХМ-1"), page, [island])
    assert row.status == contract.PARTIAL
    assert row.channel == contract.MARK_BOUND_TO_ONE_MEMBER
    assert row.cause == contract.MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER


def test_a_mark_bound_to_one_member_certifies_when_the_island_names_nothing_else():
    labels = [_label("l1", "ВРУ1 АВР ППУ IP31"), _label("l2", "ППГнг(А)-HF 5х185")]
    page = _page(labels, nodes_of_mark={"BPY1": ["n1"], "IP31": ["n1"], "HF5": ["n2"]})
    island = _assembly("isl", PROVEN_CONNECTED_COMPONENT, labels=("l1", "l2"), nodes=("n1", "n2"))
    row = _certify(_passport("Однолинейная схема ВРУ-1 (конец)"), page, [island])
    assert row.status == contract.CERTIFIED
    assert "one member" in row.structural_basis[0]


def test_a_mark_bound_in_two_islands_is_ambiguous():
    labels = [_label("l1", "ЩО-2"), _label("l2", "ЩО-2")]
    page = _page(labels, nodes_of_mark={"ЩO2": ["n1", "n2"]},
                 ownership={"ЩO2": "REPEATED_LABEL_ACROSS_SUBGRAPHS"})
    a = _assembly("isl_a", PROVEN_CONNECTED_COMPONENT, labels=("l1",), nodes=("n1",))
    b = _assembly("isl_b", PROVEN_CONNECTED_COMPONENT, labels=("l2",), nodes=("n2",))
    row = _certify(_passport("Схема ЩО-2"), page, [a, b])
    assert row.status == contract.AMBIGUOUS
    assert row.candidate_assembly_ids == ("isl_a", "isl_b")
    assert row.assembly_id is None


# ---------------------------------------------------------------------------
# channel 2 — a captioned lattice
# ---------------------------------------------------------------------------


def _captioned_table(caption="ВРУ1", value="449.3"):
    labels = [
        _label("t0", caption, cell=(0, 0), region="reg_tab"),
        _label("t1", "Рр,кВт", cell=(1, 0), region="reg_tab"),
        _label("t2", value, cell=(1, 1), region="reg_tab"),
        _label("free", "Примечание"),
    ]
    table = _assembly("tab", DRAWN_TABLE_LATTICE, labels=("t0", "t1", "t2"), owner=caption, kind="PANEL")
    return labels, table


def test_a_caption_naming_the_scope_with_a_documented_value_inside_certifies():
    labels, table = _captioned_table()
    page = _page(labels)
    passport = _passport("Схема ВРУ-1", stable_entities=["449.3"])
    row = _certify(passport, page, [table])
    assert row.status == contract.CERTIFIED
    assert row.channel == contract.CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE
    assert row.relation_kind == contract.IS_DOCUMENTED_BY


def test_a_caption_alone_does_not_certify():
    labels, table = _captioned_table()
    page = _page(labels)
    row = _certify(_passport("Схема ВРУ-1"), page, [table])
    assert row.status == contract.PARTIAL
    assert row.cause == contract.NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER


def test_the_caption_cell_itself_is_not_a_documented_value():
    labels, table = _captioned_table()
    page = _page(labels)
    row = _certify(_passport("Схема ВРУ-1", stable_entities=["ВРУ1 распределительное"]), page, [table])
    assert row.status != contract.CERTIFIED


def test_two_lattices_captioned_with_the_mark_are_ambiguous():
    labels, table = _captioned_table()
    labels += [_label("u0", "ВРУ1", cell=(0, 0), region="reg_tab2"), _label("u1", "449.3", cell=(1, 0), region="reg_tab2")]
    table2 = _assembly("tab2", DRAWN_TABLE_LATTICE, labels=("u0", "u1"), owner="ВРУ1", kind="PANEL")
    page = _page(labels)
    row = _certify(_passport("Схема ВРУ-1", stable_entities=["449.3"]), page, [table, table2])
    assert row.status == contract.AMBIGUOUS


# ---------------------------------------------------------------------------
# channel 3 — the mark printed inside one container
# ---------------------------------------------------------------------------


def test_a_mark_inside_one_box_with_a_documented_value_certifies():
    labels = [_label("b1", "ЩО-2", region="reg_box"), _label("b2", "Рр=15,0 кВт", region="reg_box"),
              _label("free", "Однолинейная схема ЩО-2")]
    box = _assembly("box", DRAWN_STROKE_GROUP, labels=("b1", "b2"))
    page = _page(labels, sheet_marks={"ЩO2"})
    row = _certify(_passport("Однолинейная схема ЩО-2", stable_entities=["Рр=15,0 кВт"]), page, [box])
    assert row.status == contract.CERTIFIED
    assert row.channel == contract.MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE


def test_a_mark_printed_only_in_the_title_lies_outside_every_container():
    labels = [_label("b2", "Рр=15,0 кВт", region="reg_box"), _label("free", "Однолинейная схема ЩО-2")]
    box = _assembly("box", DRAWN_STROKE_GROUP, labels=("b2",))
    page = _page(labels, sheet_marks={"ЩO2"})
    row = _certify(_passport("Однолинейная схема ЩО-2"), page, [box])
    assert row.status == contract.UNKNOWN
    assert row.cause == contract.MARK_LIES_OUTSIDE_EVERY_CONTAINER


def test_a_mark_printed_in_two_boxes_is_ambiguous():
    labels = [_label("b1", "ЩО-2", region="reg_a"), _label("b2", "Рр=15,0 кВт", region="reg_a"),
              _label("c1", "от ЩО-2", region="reg_b"), _label("c2", "Рр=15,0 кВт", region="reg_b")]
    a = _assembly("box_a", DRAWN_STROKE_GROUP, labels=("b1", "b2"))
    b = _assembly("box_b", DRAWN_STROKE_GROUP, labels=("c1", "c2"))
    page = _page(labels)
    row = _certify(_passport("Схема ЩО-2", stable_entities=["Рр=15,0 кВт"]), page, [a, b])
    assert row.status == contract.AMBIGUOUS


# ---------------------------------------------------------------------------
# channel 4 — the fragment's raw evidence rows
# ---------------------------------------------------------------------------


def _fragment(*rows):
    return {"fragment_id": "frag_1", "evidence_snippets": list(rows)}


def test_two_evidence_rows_inside_one_container_certify():
    labels = [_label("t1", "Квартиры 1к", cell=(1, 0), region="reg_tab"),
              _label("t2", "Квартиры пентхаусы", cell=(2, 0), region="reg_tab"),
              _label("t0", "Наименование", cell=(0, 0), region="reg_tab")]
    table = _assembly("tab", DRAWN_TABLE_LATTICE, labels=("t0", "t1", "t2"), kind="SYSTEM_GROUP")
    page = _page(labels)
    fragment = _fragment("Квартиры 1к | 13 | 14,0", "Квартиры пентхаусы | 7 | 35,0")
    row = _certify(_passport("Расчёт нагрузок"), page, [table], fragments=[fragment])
    assert row.status == contract.CERTIFIED
    assert row.channel == contract.FRAGMENT_EVIDENCE_IN_ONE_CONTAINER
    assert row.relation_kind == contract.HOLDS_SCOPE_EVIDENCE
    assert row.located_segments == 2


def test_one_evidence_row_is_too_few():
    labels = [_label("t1", "Квартиры 1к", cell=(1, 0), region="reg_tab"), _label("t0", "Наименование", cell=(0, 0), region="reg_tab")]
    table = _assembly("tab", DRAWN_TABLE_LATTICE, labels=("t0", "t1"))
    page = _page(labels)
    row = _certify(_passport("Расчёт нагрузок"), page, [table], fragments=[_fragment("Квартиры 1к | 13")])
    assert row.status == contract.PARTIAL
    assert row.cause == contract.TOO_FEW_LOCATED_SEGMENTS


def test_evidence_rows_split_over_two_containers_do_not_certify():
    labels = [_label("t1", "Квартиры 1к", cell=(1, 0), region="reg_a"),
              _label("u1", "Квартиры пентхаусы", cell=(1, 0), region="reg_b")]
    a = _assembly("tab_a", DRAWN_TABLE_LATTICE, labels=("t1",))
    b = _assembly("tab_b", DRAWN_TABLE_LATTICE, labels=("u1",))
    page = _page(labels)
    row = _certify(_passport("Расчёт"), page, [a, b],
                   fragments=[_fragment("Квартиры 1к", "Квартиры пентхаусы", "Квартиры 3к")])
    assert row.status == contract.PARTIAL
    assert row.cause == contract.EVIDENCE_SPANS_SEVERAL_CONTAINERS
    assert set(row.candidate_assembly_ids) == {"tab_a", "tab_b"}


def test_a_row_printed_in_two_containers_votes_for_neither():
    labels = [_label("t1", "Квартиры 1к", cell=(1, 0), region="reg_a"),
              _label("u1", "Квартиры 1к", cell=(1, 0), region="reg_b"),
              _label("t2", "Квартиры пентхаусы", cell=(2, 0), region="reg_a"),
              _label("t3", "Квартиры таунхаусы", cell=(3, 0), region="reg_a")]
    a = _assembly("tab_a", DRAWN_TABLE_LATTICE, labels=("t1", "t2", "t3"))
    b = _assembly("tab_b", DRAWN_TABLE_LATTICE, labels=("u1",))
    page = _page(labels)
    row = _certify(_passport("Расчёт"), page, [a, b],
                   fragments=[_fragment("Квартиры 1к", "Квартиры пентхаусы", "Квартиры таунхаусы")])
    assert row.status == contract.CERTIFIED
    assert row.assembly_id == "tab_a"
    assert row.located_segments == 2


def test_rows_printed_outside_every_container_do_not_join():
    labels = [_label("free1", "Квартиры 1к"), _label("free2", "Квартиры пентхаусы"),
              _label("b1", "ЩО-2", region="reg_box")]
    box = _assembly("box", DRAWN_STROKE_GROUP, labels=("b1",))
    page = _page(labels)
    row = _certify(_passport("Расчёт"), page, [box], fragments=[_fragment("Квартиры 1к", "Квартиры пентхаусы")])
    assert row.status == contract.UNKNOWN
    assert row.cause == contract.LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER


def test_the_bag_of_words_field_never_votes():
    labels = [_label("b1", "Электроснабжение", region="reg_box"), _label("b2", "ЩО-2", region="reg_box")]
    box = _assembly("box", DRAWN_STROKE_GROUP, labels=("b1", "b2"))
    page = _page(labels)
    row = _certify(_passport("Схема ЩО-2", systems=["Электроснабжение"]), page, [box])
    assert row.status != contract.CERTIFIED
    assert row.cause == contract.NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER


# ---------------------------------------------------------------------------
# contradiction
# ---------------------------------------------------------------------------


def test_two_proofs_on_containers_the_drawing_names_differently_contradict():
    labels = [
        _label("l1", "ЩО-2"), _label("l2", "ЩО-2 гр.1"),
        _label("t0", "ЩО-5", cell=(0, 0), region="reg_tab"),
        _label("t1", "Квартиры 1к", cell=(1, 0), region="reg_tab"),
        _label("t2", "Квартиры пентхаусы", cell=(2, 0), region="reg_tab"),
    ]
    island = _assembly("isl", PROVEN_CONNECTED_COMPONENT, labels=("l1", "l2"), nodes=("n1", "n2"), owner="ЩО-2")
    table = _assembly("tab", DRAWN_TABLE_LATTICE, labels=("t0", "t1", "t2"), owner="ЩО-5", kind="PANEL")
    page = _page(labels, nodes_of_mark={"ЩO2": ["n1", "n2"]})
    row = _certify(_passport("Схема ЩО-2"), page, [island, table],
                   fragments=[_fragment("Квартиры 1к", "Квартиры пентхаусы")])
    assert row.status == contract.CONTRADICTORY
    assert row.cause == contract.NAMED_CONTAINERS_DISAGREE
    assert row.assembly_id is None


def test_a_single_printed_quantity_against_a_single_documented_one_contradicts():
    labels, table = _captioned_table(value="35.0")
    page = _page(labels)
    passport = _passport("Схема ВРУ-1", stable_entities=["35.0"],
                         downstream=["Основные параметры: Рр=20,0 кВт"])
    facts = {"tab": {"quantity_facets": {"demand_active_power_kw": [35.0]}}}
    row = _certify(passport, page, [table], facts=facts)
    assert row.status == contract.CONTRADICTORY
    assert row.cause == contract.QUANTITY_VALUES_DISAGREE


def test_a_gap_on_one_side_is_never_a_contradiction():
    labels, table = _captioned_table(value="35.0")
    page = _page(labels)
    passport = _passport("Схема ВРУ-1", stable_entities=["35.0"])
    facts = {"tab": {"quantity_facets": {"demand_active_power_kw": [35.0]}}}
    row = _certify(passport, page, [table], facts=facts)
    assert row.status == contract.CERTIFIED


# ---------------------------------------------------------------------------
# the page is the domain, never the evidence
# ---------------------------------------------------------------------------


def test_a_page_without_containers_says_nothing():
    row = _certify(_passport("Схема ЩО-2"), _page([_label("free", "ЩО-2")]), [])
    assert row.status == contract.UNKNOWN
    assert row.cause == contract.NO_CONTAINER_ON_THE_SHEET


def test_a_single_container_on_the_page_is_not_a_reason_to_join():
    labels = [_label("b1", "Рр=15,0 кВт", region="reg_box"), _label("free", "ЩО-2")]
    box = _assembly("box", DRAWN_STROKE_GROUP, labels=("b1",))
    row = _certify(_passport("Схема ЩО-2"), _page(labels, sheet_marks={"ЩO2"}), [box])
    assert row.status == contract.UNKNOWN


# ---------------------------------------------------------------------------
# scopes and assemblies
# ---------------------------------------------------------------------------


def _row(function_id, status, assembly=None, scope="s1", candidates=()):
    return contract.MembershipCertificate(
        certificate_id=f"c_{function_id}", pair_id=PAIR, project=PROJECT, side=SIDE,
        function_id=function_id, scope_id=scope, fragment_ids=(), physical_page=1, primary_mark=None,
        status=status, channel=contract.TOPOLOGY_OWNER_MARK_ON_MEMBERS if status == contract.CERTIFIED else None,
        relation_kind=contract.IS_DRAWN_AS if status == contract.CERTIFIED else None,
        assembly_id=assembly, certified_assembly_ids=(assembly,) if assembly and status == contract.CERTIFIED else (),
        candidate_assembly_ids=tuple(candidates) or ((assembly,) if assembly else ()),
        structural_basis=("x",) if status == contract.CERTIFIED else (),
        evidence_refs=("y",) if status == contract.CERTIFIED else (),
    )


def test_a_scope_certifies_when_every_component_does_and_records_how_many_containers():
    rows = [_row("f1", contract.CERTIFIED, "a"), _row("f2", contract.CERTIFIED, "b")]
    lifted = scopes.lift_to_scopes(rows, {"s1": ["k1", "k2"]}, {(PAIR, "f1"): "k1", (PAIR, "f2"): "k2"})
    assert lifted[0]["status"] == contract.CERTIFIED
    assert lifted[0]["cause"] == contract.COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS
    assert lifted[0]["container_count"] == 2


def test_an_ambiguous_component_makes_the_scope_ambiguous():
    rows = [_row("f1", contract.CERTIFIED, "a"), _row("f2", contract.AMBIGUOUS, candidates=("a", "b"))]
    lifted = scopes.lift_to_scopes(rows, {"s1": ["k1", "k2"]}, {(PAIR, "f1"): "k1", (PAIR, "f2"): "k2"})
    assert lifted[0]["status"] == contract.AMBIGUOUS


def test_several_certified_scopes_on_one_assembly_are_a_composition_not_a_defect():
    rows = [_row("f1", contract.CERTIFIED, "a", scope="s1"), _row("f2", contract.CERTIFIED, "a", scope="s2")]
    assembly = _assembly("a", PROVEN_CONNECTED_COMPONENT, labels=("l1",), nodes=("n1",))
    composition = scopes.assembly_composition(rows, [assembly])
    assert composition["by_composition"][contract.MULTI_SCOPE] == 1
    assert composition["rows"][0]["certified_scope_ids"] == ["s1", "s2"]


# ---------------------------------------------------------------------------
# decoys and the gate
# ---------------------------------------------------------------------------


def test_the_decoy_partner_sits_on_another_page_with_another_mark():
    ordered = [
        ("f1", _passport("Схема ЩО-2", page=1)),
        ("f2", _passport("Схема ЩО-2", page=2)),
        ("f3", _passport("Схема ЩО-3", page=3)),
    ]
    partner = controls._partner(ordered, 0)
    assert partner is not None and partner[0] == "f3"


def test_the_gate_reads_both_sides_from_certificates_only():
    tasks = [{
        "task_id": "t1", "pair_id": PAIR, "corpus": PROJECT, "scope_id": "s1",
        "relation_types": ["CONTINUED_1_TO_1"],
        "candidates": [{"component_mapping": [{"left_function_id": "L", "right_function_id": "R"}]}],
    }]
    left = _row("L", contract.CERTIFIED, "a")
    right = contract.MembershipCertificate(
        certificate_id="c_R", pair_id=PAIR, project=PROJECT, side="RIGHT", function_id="R",
        scope_id=None, fragment_ids=(), physical_page=1, primary_mark=None,
        status=contract.PARTIAL, channel=contract.MARK_BOUND_TO_ONE_MEMBER, assembly_id="b",
        candidate_assembly_ids=("b",))
    scope_rows = [{"scope_id": "s1", "status": contract.CERTIFIED}]
    result = gate.phase1_gate(tasks, [left, right], scope_rows, [], {"false_certificates_total": 0})
    assert result["by_coverage_class"][gate.CERTIFIED_LEFT_ONLY] == 1
    assert result["meaningful_certified_coverage"] is False


# ---------------------------------------------------------------------------
# the frozen control sheet, when the corpus is present
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not (Path(__file__).resolve().parents[1] / "comparison" / "ai_sheet_matcher"
         / "20260905_function_assembly_membership_v1" / "verdict.json").is_file(),
    reason="frozen artifacts are not present",
)
def test_the_frozen_artifact_keeps_the_nine_topology_certificates_and_no_false_one():
    import json
    root = Path(__file__).resolve().parents[1] / "comparison" / "ai_sheet_matcher" / "20260905_function_assembly_membership_v1"
    verdict = json.loads((root / "verdict.json").read_text(encoding="utf-8"))
    assert verdict["proven_before"] == 9
    assert verdict["certified_after"] >= 9
    assert verdict["false_certificates_on_decoys"] == 0
    assert verdict["model_calls"] == 0
    assert verdict["deploy"] is False and verdict["pushed"] is False
