"""Controls for the UNIFIED ENGINEERING EVIDENCE V1 contract and its consumers.

Synthetic facts and pages: the contract guards must refuse what the layers
beneath refuse, the producers must keep applicability and certification
honest, and the profiles must never read a gap as a contradiction.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_assembly_membership_v1 import contract as membership_contract
from experiments.pdf_evidence_v1.contract import ContractViolation
from experiments.unified_evidence_v1 import contract, producers, profiles, reassessment, synthesis

PAIR, PROJECT = "p_test", "TEST"


def _fact(**overrides):
    base = dict(
        fact_id="uef_x", field="printed_string", normalized_value="ввод 1", source_representation=contract.NATIVE_PDF_TEXT,
        producer="pdf_evidence_v1", pair_id=PAIR, document=f"{PROJECT}/LEFT", side="LEFT", physical_page=1,
        applicability=contract.UNKNOWN, claim_semantics=contract.POSITIVE_PRESENCE,
        provenance_grade=contract.EXACT_GEOMETRY, provenance_refs=("label:l1",),
    )
    base.update(overrides)
    return contract.UnifiedFact(**base)


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_no_vocabulary_value_states_absence():
    for value in contract.FIELDS + contract.APPLICABILITY + contract.CLAIM_SEMANTICS + contract.PROVENANCE_GRADE:
        for term in ("ABSENT", "ABSENCE", "REMOVED", "DELETED", "MISSING", "NOT_FOUND"):
            assert term not in value.upper(), value


def test_contract_document_passes_its_own_guards():
    document = contract.contract_document()
    contract.assert_no_absence_vocabulary(document)
    contract.assert_no_similarity_evidence(document)


def test_presence_requires_exact_provenance():
    with pytest.raises(ContractViolation):
        contract.assert_fact_contract([_fact(provenance_grade=contract.PAGE_ONLY)])


def test_a_fact_without_provenance_is_refused():
    with pytest.raises(ContractViolation):
        contract.assert_fact_contract([_fact(provenance_refs=())])


def test_a_function_local_fact_must_name_a_function():
    with pytest.raises(ContractViolation):
        contract.assert_fact_contract([_fact(applicability=contract.FUNCTION_LOCAL)])


def test_certification_needs_a_certificate():
    fact = _fact(certified_function_ids=("f1",), certified_assembly_id="a", applicability=contract.ASSEMBLY_LOCAL,
                 container={"kind": "ASSEMBLY", "id": "a"})
    with pytest.raises(ContractViolation):
        contract.assert_fact_contract([fact], certified_pairs={})
    contract.assert_fact_contract([fact], certified_pairs={(PAIR, "LEFT", "f1"): ("a",)})


def test_a_resemblance_key_is_refused_in_a_produced_payload():
    with pytest.raises(ContractViolation):
        contract.assert_no_similarity_evidence({"rows": [{"similarity": 0.9}]})


# ---------------------------------------------------------------------------
# producers keep certification and applicability honest
# ---------------------------------------------------------------------------


def _certificate(function_id, assembly_id, status=membership_contract.CERTIFIED, scope="s1"):
    return membership_contract.MembershipCertificate(
        certificate_id=f"c_{function_id}", pair_id=PAIR, project=PROJECT, side="LEFT", function_id=function_id,
        scope_id=scope, fragment_ids=(), physical_page=1, primary_mark="ЩO2", status=status,
        channel=membership_contract.TOPOLOGY_OWNER_MARK_ON_MEMBERS if status == membership_contract.CERTIFIED else None,
        relation_kind=membership_contract.IS_DRAWN_AS if status == membership_contract.CERTIFIED else None,
        assembly_id=assembly_id if status == membership_contract.CERTIFIED else None,
        certified_assembly_ids=(assembly_id,) if status == membership_contract.CERTIFIED else (),
        structural_basis=("x",), evidence_refs=("node:n1",))


def test_attach_names_a_function_only_through_a_certificate():
    index = producers.Certificates([_certificate("f1", "a"), _certificate("f2", "b", status=membership_contract.PARTIAL)])
    fact = index.attach(_fact(applicability=contract.ASSEMBLY_LOCAL, container={"kind": "ASSEMBLY", "id": "a"}), "a")
    assert fact.certified_function_ids == ("f1",) and fact.certified_assembly_id == "a"
    other = index.attach(_fact(applicability=contract.ASSEMBLY_LOCAL, container={"kind": "ASSEMBLY", "id": "b"}), "b")
    assert other.certified_function_ids == () and other.certified_assembly_id is None


def test_two_certified_scopes_on_one_assembly_leave_the_scope_unset():
    index = producers.Certificates([_certificate("f1", "a", scope="s1"), _certificate("f2", "a", scope="s2")])
    fact = index.attach(_fact(applicability=contract.ASSEMBLY_LOCAL, container={"kind": "ASSEMBLY", "id": "a"}), "a")
    assert set(fact.certified_function_ids) == {"f1", "f2"}
    assert fact.certified_function_scope_id is None
    assert any("certified_scopes=2" in note for note in fact.notes)


def test_a_stamp_string_stays_sheet_shared():
    page = SimpleNamespace(
        document=f"{PROJECT}/LEFT", pair_id=PAIR, side="LEFT", physical_page=1,
        labels_by_id={"l1": {"label_id": "l1", "text": "Однолинейная схема ЩО-2", "bbox": [0, 0, 10, 4],
                             "provenance": "NATIVE_PDF_TEXT", "ownership": "STAMP_ZONE", "region_id": None, "cell": None}},
        aggregation=None, containers=[], printed_strings=1,
    )
    state = {"pages": {(PAIR, "LEFT"): {1: page}}, "assemblies_map": {(PAIR, "LEFT"): {1: []}}}
    facts = producers.native_text_facts(state, producers.Certificates([]))
    strings = [fact for fact in facts if fact.field == "printed_string"]
    assert strings and strings[0].applicability == contract.SHEET_SHARED
    assert strings[0].container == {"kind": "STAMP", "id": "stamp:1"}
    marks = [fact for fact in facts if fact.field == "designation"]
    assert [fact.normalized_value for fact in marks] == ["ЩO2"]


# ---------------------------------------------------------------------------
# profiles never read a gap as a contradiction
# ---------------------------------------------------------------------------


def _function_fact(side, function_id, field, value, basis=synthesis.DECLARED, corroborated=False):
    return synthesis.FunctionFact(
        fact_id=f"fnf_{side}_{function_id}_{field}_{value}", pair_id=PAIR, side=side, function_id=function_id,
        scope_id="s1", field=field, value=value, basis=basis,
        claim_semantics=contract.POSITIVE_PRESENCE if basis == synthesis.CERTIFIED else contract.SUPPORT_ONLY,
        applicability=contract.FUNCTION_LOCAL, provenance_refs=("x",), natively_corroborated=corroborated,
    )


def _candidate(candidate_id, left, right):
    return {"candidate_id": candidate_id, "relation_type": "CONTINUED_1_TO_1",
            "component_mapping": [{"left_function_id": left, "right_function_id": right}]}


def test_shared_positive_facts_and_a_missing_field_is_unknown():
    facts = {
        (PAIR, "LEFT", "L"): [_function_fact("LEFT", "L", "board_mark", "ЩO2", synthesis.SHEET),
                              _function_fact("LEFT", "L", "floors", "1 этаж")],
        (PAIR, "RIGHT", "R"): [_function_fact("RIGHT", "R", "board_mark", "ЩO2", synthesis.SHEET)],
    }
    profile = profiles.profile_candidate(_candidate("c1", "L", "R"), PAIR, facts)
    assert profile["shared_count"] == 1
    assert profile["explicit_contradictions"] == []
    assert profile["unknown_fields"]["left_only"] == ["floors"]


def test_two_explicit_single_marks_that_differ_contradict():
    facts = {
        (PAIR, "LEFT", "L"): [_function_fact("LEFT", "L", "board_mark", "ЩO2", synthesis.SHEET)],
        (PAIR, "RIGHT", "R"): [_function_fact("RIGHT", "R", "board_mark", "ЩO3", synthesis.SHEET)],
    }
    profile = profiles.profile_candidate(_candidate("c1", "L", "R"), PAIR, facts)
    assert profile["explicit_contradictions"] == [{"field": "board_mark", "facet": "board_mark", "left": "ЩO2", "right": "ЩO3"}]


def test_a_multi_valued_quantity_never_contradicts():
    facts = {
        (PAIR, "LEFT", "L"): [_function_fact("LEFT", "L", "electrical_quantities", {"facet": "demand_active_power_kw", "value": 20.0}),
                              _function_fact("LEFT", "L", "electrical_quantities", {"facet": "demand_active_power_kw", "value": 35.0})],
        (PAIR, "RIGHT", "R"): [_function_fact("RIGHT", "R", "electrical_quantities", {"facet": "demand_active_power_kw", "value": 41.0})],
    }
    profile = profiles.profile_candidate(_candidate("c1", "L", "R"), PAIR, facts)
    assert profile["explicit_contradictions"] == []


def test_coverage_class_reads_certified_facts_only():
    facts = {
        (PAIR, "LEFT", "L"): [_function_fact("LEFT", "L", "bus_facts", {"bus_count": 2}, synthesis.CERTIFIED)],
        (PAIR, "RIGHT", "R"): [_function_fact("RIGHT", "R", "board_mark", "ЩO2", synthesis.SHEET)],
    }
    task = {"task_id": "t1", "pair_id": PAIR, "corpus": PROJECT, "scope_id": "s1", "relation_types": ["CONTINUED_1_TO_1"],
            "candidates": [_candidate("c1", "L", "R")], "references": [{"candidate_ids": ["c1"]}]}
    result = profiles.profile_tasks([task], facts)
    assert result["by_coverage_class"][profiles.CERTIFIED_LEFT_ONLY] == 1
    assert result["rows"][0]["reference_candidate_ids"] == ["c1"]


def test_reassessment_counts_new_evidence_without_removing_candidates():
    facts = {
        (PAIR, "LEFT", "L"): [_function_fact("LEFT", "L", "floors", "1 этаж", corroborated=True)],
        (PAIR, "RIGHT", "R1"): [_function_fact("RIGHT", "R1", "floors", "1 этаж", corroborated=True)],
        (PAIR, "RIGHT", "R2"): [_function_fact("RIGHT", "R2", "floors", "2 этаж", corroborated=True)],
    }
    task = {"task_id": "t1", "pair_id": PAIR, "corpus": PROJECT, "scope_id": "s1", "relation_types": ["CONTINUED_1_TO_1"],
            "candidates": [_candidate("c1", "L", "R1"), _candidate("c2", "L", "R2")], "references": [{"candidate_ids": ["c1"]}]}
    result = reassessment.reassess(profiles.profile_tasks([task], facts))
    assert result["totals"]["candidates"] == 2
    assert result["totals"]["multi_candidate_tasks_discriminated_by_new_evidence_alone"] == 1
    assert result["references"]["reference_supported_by_new_evidence"] == 1
    assert result["selector_changed"] is False


# ---------------------------------------------------------------------------
# the frozen artifact, when present
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not (Path(__file__).resolve().parents[1] / "comparison" / "ai_sheet_matcher" / "20260905_unified_evidence_v1" / "verdict.json").is_file(),
    reason="frozen artifacts are not present",
)
def test_the_frozen_artifact_asserts_no_gap_and_calls_no_model():
    import json
    root = Path(__file__).resolve().parents[1] / "comparison" / "ai_sheet_matcher" / "20260905_unified_evidence_v1"
    verdict = json.loads((root / "verdict.json").read_text(encoding="utf-8"))
    census = json.loads((root / "unified_evidence_census.json").read_text(encoding="utf-8"))
    assert verdict["model_calls"] == 0 and verdict["deploy"] is False and verdict["pushed"] is False
    assert census["facts_asserting_a_gap"] == 0
    assert census["by_claim_semantics"].get("POSITIVE_PRESENCE", 0) > 0
