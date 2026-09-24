"""Phase D: deterministic validator + expander — every code, fail-closed fallbacks (0 model calls)."""
from __future__ import annotations

import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator import engine as E
from backend.app.services.project_change_consolidator.validator import validate_call
from backend.tests.project_change_consolidator import responses as R
from backend.tests.project_change_consolidator import synthetic as syn

LOCS = {"R-7": ["Здание 7", "Коридоры"], "R-9": ["Здание 9", "Коридоры"], "R-11": ["Здание 11"],
        "R-P": ["Насосная"], "R-Q": ["Насосная"], "R-OTHER": ["Общие решения"], "R-D": ["Листы"],
        "R-ALL": ["Здания 7, 9 и 11"]}


def build(tmp_path, *, leak_cards=None):
    a = syn.building_card("A1", 7, 11)
    blk = a["evidence_items"][1]
    h_other = syn.hint("H001", "Трубопроводы В5 под потолком коридоров",
                       "Схема здания 7 показывает трубопроводы в стяжке; противоречие 44,5", [blk])
    h_pump = syn.hint("H001", "Подача насосной установки В5", "Подача 17,3 м3/ч не подтверждена; противоречие 17,3",
                      [syn.evidence("NEW", 50, "n30-В5", "Установки В5 подача 17,3 м3/ч напор 44,5 м", "TABLE")])
    legend = syn.card("D1", subject="Условные обозначения арматуры на схеме",
                      summary="Изменены условные обозначения листа.", old="Легенда листа содержала смесители.",
                      new="Легенда листа не содержит смесителей.")
    cards = [(a, "R-7"), (syn.building_card("B1", 9, 12), "R-9"), (syn.building_card("C1", 11, 13), "R-11"),
             (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P"), (syn.pump_card("P2", "В8", 31, "12,5", "17,3"), "R-Q"),
             (legend, "R-D")]
    bundle = syn.bundle_of(cards, {"R-OTHER": [h_other], "R-9": [h_pump], "R-ALL": []}, region_locations=LOCS,
                           package_dir=tmp_path / "source")
    prepared = E.prepare(bundle)
    calls = {tuple(c.members): c for c in prepared.calls}
    return prepared, calls[("A1", "B1", "C1")], calls[("P1", "P2")], calls[("D1",)]


def run(prepared, call, resp):
    record = {"status": "RESPONDED", "raw_response": resp, "raw_response_sha256": C.sha256_json(resp)}
    return validate_call(prepared, call, record, consolidator_run_id="0" * 32)


def failed(out):
    return [c["code"] for c in out.checks if c["result"] == "FAIL"]


@pytest.fixture
def fx(tmp_path):
    return build(tmp_path)


# ------------------------------------------------------------------ positive paths
def test_valid_merge_expands_values_evidence_and_lineage(fx):
    prepared, bld, _, _ = fx
    out = run(prepared, bld, R.merge(bld.payload))
    assert out.status == "ACCEPTED" and failed(out) == [] and len(out.consolidated) == 1
    card = out.consolidated[0]
    assert card["change_summary"] == "Одно решение на нескольких листах: в стяжке → под потолком."
    assert card["changed_parameters"][0]["old_value"] == "в стяжке"
    assert [s["ref"] for s in card["changed_parameters"][0]["source_values"]] == ["A1#p1", "B1#p1", "C1#p1"]
    assert [e["ref"] for e in card["evidence_items"]] == ["A1#e1", "A1#e2", "B1#e1", "B1#e2", "C1#e1", "C1#e2"]
    assert card["lineage"]["member_ids"] == ["A1", "B1", "C1"]
    assert card["lineage"]["input_cluster_sha256"] == bld.payload_sha256
    assert "{{" not in card["change_summary"]


def test_conflict_from_another_region_is_preserved_with_provenance(fx):
    prepared, bld, _, _ = fx
    resp = R.merge(bld.payload, material={("R-OTHER", "H001")})
    out = run(prepared, bld, resp)
    assert failed(out) == []
    hint = out.consolidated[0]["open_conflicts"][0]["hint"]
    assert (hint["region_id"], hint["hint_id"], hint["hint_ref"]) == ("R-OTHER", "H001", "R-OTHER/H001")
    assert hint["evidence_items"][0]["ref"] == "R-OTHER/H001#h1"
    assert "44,5" in hint["missing_proof_or_conflict"]


def test_similar_numbers_different_systems_keep_separate(fx):
    prepared, _, pumps, _ = fx
    out = run(prepared, pumps, R.keep_all(pumps.payload))
    assert out.status == "ACCEPTED" and out.consolidated == []
    assert sorted(p["projectchange_id"] for p in out.pass_through) == ["P1", "P2"]
    assert all(p["decision"] == "KEEP_SEPARATE" and p["card"]["projectchange_id"] == p["projectchange_id"]
               for p in out.pass_through)


def test_material_hint_on_kept_card_becomes_deterministic_annotation(fx):
    prepared, _, pumps, _ = fx
    out = run(prepared, pumps, R.keep_all(pumps.payload, material={("R-9", "H001")}))
    notes = {p["projectchange_id"]: p["conflict_annotations"] for p in out.pass_through}
    assert notes["P1"][0]["hint"]["hint_ref"] == "R-9/H001" and "17,3" in notes["P1"][0]["text"]
    assert notes["P2"][0]["hint"]["hint_ref"] == "R-9/H001"


def test_uncertain_never_merges_and_links_members(fx):
    prepared, _, pumps, _ = fx
    resp = R.keep_all(pumps.payload)
    resp["groups"] = [R.group("G1", "UNCERTAIN", ["P1", "P2"], uncertainty="разные системы, но одинаковые значения")]
    resp["conflict_dispositions"] = R._dispositions(pumps.payload, resp["groups"])
    out = run(prepared, pumps, resp)
    assert failed(out) == [] and out.consolidated == []
    assert {p["projectchange_id"]: p["possible_same_event"] for p in out.pass_through} == {"P1": ["P2"], "P2": ["P1"]}


def test_needs_source_merge_is_treated_as_uncertain(fx):
    prepared, bld, _, _ = fx
    resp = R.merge(bld.payload, flags=["NEEDS_SOURCE"])
    out = run(prepared, bld, resp)
    assert out.consolidated == [] and "M4" in [c["code"] for c in out.checks]
    assert all(p["decision"] == "UNCERTAIN" for p in out.pass_through)


def test_composite_card_is_a_barrier(fx):
    prepared, bld, _, _ = fx
    ok = R.keep_all(bld.payload)
    ok["groups"][0].update(flags=["COMPOSITE_CARD"], channel="REVIEW")
    out = run(prepared, bld, ok)
    first = next(p for p in out.pass_through if p["projectchange_id"] == "A1")
    assert first["channel"] == "REVIEW" and "COMPOSITE_CARD" in first["flags"] and out.consolidated == []
    out = run(prepared, bld, R.merge(bld.payload, flags=["COMPOSITE_CARD"]))
    assert failed(out) == ["M1"] and out.consolidated == []
    assert all(p["decision"] == "FALLBACK" for p in out.pass_through)


def test_documentary_change_goes_to_documentary_channel(fx):
    prepared, _, _, single = fx
    out = run(prepared, single, R.keep_all(single.payload, channel="DOCUMENTARY_CHANGE", flags=["DOCUMENTARY_SUSPECTED"]))
    assert failed(out) == [] and out.pass_through[0]["channel"] == "DOCUMENTARY_CHANGE"


def test_provider_failure_falls_back_without_using_the_answer(fx):
    prepared, bld, _, _ = fx
    out = validate_call(prepared, bld, {"status": "PROVIDER_ERROR", "error_code": "schema_invalid",
                                        "raw_response": R.merge(bld.payload)}, consolidator_run_id="0" * 32)
    assert out.status == "PROVIDER_FAILED" and out.consolidated == []
    assert all(p["decision"] == "PROVIDER_FAILED" and p["channel"] == "NOT_ASSESSED" for p in out.pass_through)


# ------------------------------------------------------------------ cluster-level failures (S)
@pytest.mark.parametrize("mutate,code", [
    (lambda r: r.pop("conflict_dispositions"), "S1"),
    (lambda r: r.update(cluster_id="CL-000000000000"), "S2"),
    (lambda r: r["groups"].append(R.group("G9", "KEEP_SEPARATE", ["A1"])), "S3"),     # duplicate member
    (lambda r: r["groups"][0]["member_refs"].append("P1"), "S3"),                       # foreign card
    (lambda r: r["recompositions"].clear(), "S4"),
    (lambda r: r["groups"][0]["relations"].__setitem__(0, {"card_ref": "B1", "relation": "ANCHOR"}), "S6"),
])
def test_structure_failures_fall_back_for_the_whole_cluster(fx, mutate, code):
    prepared, bld, _, _ = fx
    out = run(prepared, bld, R.edit(R.merge(bld.payload), mutate))
    assert out.status == "CLUSTER_FALLBACK" and out.reason == f"CLUSTER_INVALID:{code}"
    assert out.consolidated == [] and sorted(p["projectchange_id"] for p in out.pass_through) == ["A1", "B1", "C1"]


def test_merge_forbidden_in_singleton_review(fx):
    prepared, _, _, single = fx
    resp = R.keep_all(single.payload)
    resp["groups"][0]["decision"] = "UNCERTAIN"
    assert run(prepared, single, resp).reason in ("CLUSTER_INVALID:S4", "CLUSTER_INVALID:S5")


# ------------------------------------------------------------------ group-level failures
def _rec(fn):
    return lambda r: fn(r["recompositions"][0])


@pytest.mark.parametrize("mutate,code", [
    (_rec(lambda x: x["parameters"][0]["sources"].__setitem__(0, "A1#p9")), "R2"),
    (_rec(lambda x: x["scope"].update(scope_type="SYSTEM_WIDE", scope_basis="EXPLICIT_GENERAL_STATEMENT",
                                      basis_evidence_refs=["A1#e9"])), "R3"),       # bad evidence ref
    (lambda r: r["conflict_dispositions"][0]["hint_key"].update(region_id="R-9"), "R4"),   # bad hint identity
    (_rec(lambda x: x.update(change_summary="См. blk_0123456789abcdef.")), "R6"),
    (_rec(lambda x: x["manifestations"][0].update(location_ids=["L99"])), "R7"),
    (_rec(lambda x: x["parameters"][0]["sources"].pop()), "C1"),
    (_rec(lambda x: x.update(change_summary="Подача увеличена до 99,9 м3/ч.")), "C4"),        # invented number
    (_rec(lambda x: x.update(new_state="Добавлена система К77.")), "C4"),                      # invented designation
    (_rec(lambda x: x.update(system_designations=["В9"])), "C5"),
    (_rec(lambda x: x["manifestations"].pop()), "C6"),
    (_rec(lambda x: x.update(change_summary="")), "C7"),
    (lambda r: r["conflict_dispositions"].clear(), "K1"),
    (lambda r: r["conflict_dispositions"][0].update(disposition="MATERIAL_REPRESENTED"), "K2"),
    (lambda r: r["conflict_dispositions"][0].update(reason=""), "K5"),
    (_rec(lambda x: x["scope"].update(scope_type="SYSTEM_WIDE")), "P2"),
    (_rec(lambda x: x["scope"].update(scope_basis="SINGLE_MANIFESTATION")), "P3"),
    (_rec(lambda x: x["scope"].update(scope_type="DESIGN_BASIS", scope_basis="CALCULATION_BASIS")), "P4"),
    (_rec(lambda x: x["scope"].update(affected_location_ids=[])), "P1"),
    (lambda r: r["groups"][0].update(merge_basis=[]), "M5"),
])
def test_group_failures_fall_back_to_original_cards(fx, mutate, code):
    prepared, bld, _, _ = fx
    out = run(prepared, bld, R.edit(R.merge(bld.payload), mutate))
    assert failed(out) == [code], out.checks
    assert out.consolidated == [] and out.rollbacks[0]["code"] == code
    assert sorted(p["projectchange_id"] for p in out.pass_through) == ["A1", "B1", "C1"]
    assert all(p["decision"] == "FALLBACK" and p["channel"] == "NOT_ASSESSED" and p["conflict_annotations"] == []
               for p in out.pass_through)


def test_c2_and_c4b_on_parameter_status(fx):
    prepared, _, pumps, _ = fx
    resp = R.merge(pumps.payload)
    resp["recompositions"][0]["parameters"] = [
        {"param_key": "P1", "name": "Подача", "sources": ["P1#p1", "P2#p1"], "status": "LOCATION_VARIANT",
         "applies_to_location_ids": []}]
    out = run(prepared, pumps, resp)
    assert failed(out) == ["C2"]  # identical values cannot be a LOCATION_VARIANT


def test_placeholder_only_for_consistent_parameters(fx):
    import copy

    prepared, _, pumps, _ = fx
    call = copy.deepcopy(pumps)
    p2 = next(c for c in call.payload["cards"] if c["card_ref"] == "P2")["parameters"][0]
    p2.update(new_value="20,0", location="Насосная второй зоны")
    resp = R.merge(call.payload)
    rec = resp["recompositions"][0]
    rec["parameters"] = [{"param_key": "P1", "name": "Подача", "sources": ["P1#p1", "P2#p1"],
                          "status": "LOCATION_VARIANT", "applies_to_location_ids": []}]
    for m in rec["manifestations"]:
        m["local_param_keys"] = ["P1"]
    rec["change_summary"] = "Подача насосных установок изменена по зонам."
    assert failed(run(prepared, call, resp)) == []
    rec["change_summary"] = "Подача изменена до {{P1.new}}."
    assert failed(run(prepared, call, resp)) == ["C4b"]
    rec["parameters"][0]["status"] = "CONFLICT"
    rec["change_summary"] = "Подача насосных установок изменена."
    assert failed(run(prepared, call, resp)) == ["C2"]  # CONFLICT needs a MEMBER_DISAGREEMENT open conflict
    rec["open_conflicts"] = [{"source": "MEMBER_DISAGREEMENT", "hint_key": {"region_id": "", "hint_id": ""},
                              "member_param_refs": ["P1#p1", "P2#p1"], "statement": "Значения подачи расходятся.",
                              "affected_param_keys": ["P1"]}]
    assert failed(run(prepared, call, resp)) == ["K3"]  # a conflict on a parameter must be declared
    rec["claim_status"] = "CONTAINS_UNRESOLVED_CONFLICT"
    assert failed(run(prepared, call, resp)) == []


def test_system_wide_by_all_units_requires_every_unit(fx):
    prepared, bld, _, _ = fx
    units = bld.payload["location_units"]["units"]
    assert units, "registry is available in the fixture"
    resp = R.merge(bld.payload)
    rec = resp["recompositions"][0]
    rec["scope"].update(scope_type="SYSTEM_WIDE", scope_basis="ALL_LOCATION_UNITS_COVERED")
    rec["scope"]["affected_location_ids"] = sorted({lid for u in units for lid in u["location_ids"][:1]})
    assert failed(run(prepared, bld, resp)) == []
    rec["scope"]["affected_location_ids"] = units[0]["location_ids"][:1]
    assert failed(run(prepared, bld, resp)) == ["P2"]


def test_system_wide_by_general_statement_needs_text_evidence(fx):
    prepared, bld, _, _ = fx
    resp = R.merge(bld.payload)
    resp["recompositions"][0]["scope"].update(scope_type="SYSTEM_WIDE", scope_basis="EXPLICIT_GENERAL_STATEMENT",
                                              basis_evidence_refs=["A1#e2"])
    out = run(prepared, bld, resp)
    assert failed(out) == []
    assert out.consolidated[0]["scope"]["basis_evidence"][0]["block_id"] == "n11-note"


def test_documentary_relation_must_point_to_an_engineering_card(fx):
    prepared, _, pumps, _ = fx
    resp = R.keep_all(pumps.payload)
    resp["groups"][0].update(channel="DOCUMENTARY_CHANGE", related_engineering_card_ref="P2")
    assert failed(run(prepared, pumps, resp)) == []
    resp["groups"][1]["channel"] = "REVIEW"
    assert failed(run(prepared, pumps, resp)) == ["M2"]


def test_uncertain_needs_a_reason(fx):
    prepared, _, pumps, _ = fx
    resp = R.keep_all(pumps.payload)
    resp["groups"] = [R.group("G1", "UNCERTAIN", ["P1", "P2"])]
    resp["conflict_dispositions"] = R._dispositions(pumps.payload, resp["groups"])
    assert failed(run(prepared, pumps, resp)) == ["M3"]


def test_source_package_mismatch_rejects_the_merge(fx):
    prepared, bld, _, _ = fx
    prepared.cards["B1"].card["evidence_items"][0]["bbox"] = [0.2, 0.2, 0.3, 0.3]
    out = run(prepared, bld, R.merge(bld.payload))
    assert failed(out) == ["R8"] and out.consolidated == []
