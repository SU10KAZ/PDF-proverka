"""Phase C: model payload + provider-neutral engine with a fake provider (0 model calls)."""
from __future__ import annotations

import hashlib

import jsonschema
import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator import engine as E
from backend.app.services.project_change_consolidator import payload as PL
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, build_codex_payload
from backend.tests.project_change_consolidator import synthetic as syn

LOCS = {"R-7": ["Здание 7", "Коридоры"], "R-9": ["Здание 9", "Коридоры"], "R-11": ["Здание 11"],
        "R-P": ["Насосная", "Здания 7, 9 и 11"], "R-OTHER": ["Общие решения"]}


def _three_buildings_and_pump(extra_hints=None, pump=None):
    cards = [(syn.building_card("A1", 7, 11), "R-7"), (syn.building_card("B1", 9, 12), "R-9"),
             (syn.building_card("C1", 11, 13), "R-11"), (pump or syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")]
    return syn.bundle_of(cards, extra_hints, region_locations=LOCS)


def _keep_all(call_id, pair_id, data, schema, images):
    return {"cluster_id": data["cluster_id"], "recompositions": [], "conflict_dispositions": [],
            "groups": [{"group_id": f"G{i}", "decision": "KEEP_SEPARATE", "member_refs": [c["card_ref"]],
                        "relations": [], "merge_basis": [], "distinguishing_check": "разные", "uncertainty_reason": "",
                        "flags": [], "channel": "ENGINEERING_CHANGE", "related_engineering_card_ref": ""}
                       for i, c in enumerate(data["cards"], 1)]}


# --------------------------------------------------------------------------- payload
def test_location_registry_prefers_buildings_and_fails_closed_below_two_units():
    reg = PL.location_registry(syn.semantic_map_of({"a": ["Здания 1, 2 и 3", "Секции 1.1, 1.2"], "b": ["Здание 3"]}))
    assert reg["available"] and reg["kind"] == "здани" and reg["units"] == ["здани 1", "здани 2", "здани 3"]
    reg = PL.location_registry(syn.semantic_map_of({"a": ["Здание 1"], "b": ["Насосная"]}))
    assert not reg["available"] and reg["units"] == [] and reg["unavailable_reason"]


def test_cluster_payload_validates_and_resolves_locations():
    prepared = E.prepare(_three_buildings_and_pump())
    call = next(c for c in prepared.calls if c.mode == C.MODE_CLUSTER)
    assert call.status == E.READY and sorted(call.members) == ["A1", "B1", "C1"]
    p = call.payload
    jsonschema.Draft202012Validator(C.INPUT_SCHEMA).validate(p)
    catalog = {x["location_id"]: x for x in p["location_catalog"]}
    corridors = [x for x in catalog.values() if x["text"] == "Коридоры"]
    assert len(corridors) == 1 and len(corridors[0]["origins"]) >= 3  # one location, several origins
    for card in p["cards"]:
        assert all(lid in catalog for lid in card["location_ids"])
    units = {u["label"]: u["location_ids"] for u in p["location_units"]["units"]}
    assert set(units) == {"здани 7", "здани 9", "здани 11"}
    assert all(catalog[lid]["text"] for lid in units["здани 9"])
    assert p["pair_features"] and all(not r["same_region"] for r in p["pair_features"])


def test_payload_carries_only_the_cluster_cards():
    prepared = E.prepare(_three_buildings_and_pump())
    call = next(c for c in prepared.calls if c.mode == C.MODE_CLUSTER)
    text = str(call.payload)
    assert "P1" not in [c["card_ref"] for c in call.payload["cards"]]
    assert "Насосная установка В5" not in text


def test_repeated_hint_id_from_two_regions_is_two_hints():
    blk = syn.building_card("A1", 7, 11)["evidence_items"][1]
    h_a = syn.hint("H001", "Трубопроводы В5 под потолком коридоров", "Противоречие прокладки 44,5 в стяжке", [blk])
    h_b = syn.hint("H001", "Трубопроводы В5 под потолком коридоров", "Другое противоречие 44,5 прокладки", [blk])
    prepared = E.prepare(_three_buildings_and_pump({"R-OTHER": [h_a], "R-9": [h_b]}))
    call = next(c for c in prepared.calls if c.mode == C.MODE_CLUSTER)
    keys = [(h["hint_key"]["region_id"], h["hint_key"]["hint_id"]) for h in call.payload["hints"]]
    assert ("R-OTHER", "H001") in keys and ("R-9", "H001") in keys
    assert set(call.hint_refs) >= {"R-OTHER/H001", "R-9/H001"}


def test_fragments_are_truncated():
    long = syn.building_card("A1", 7, 11)
    long["evidence_items"][0]["relevant_fragment"] = "Примечание " + "очень длинный текст " * 60
    prepared = E.prepare(syn.bundle_of([(long, "R-7"), (syn.building_card("B1", 9, 12), "R-9")], region_locations=LOCS))
    ev = prepared.calls[0].payload["cards"][0]["evidence"][0]
    assert len(ev["fragment"]) <= C.FRAGMENT_CHARS


def test_label_leak_guard_refuses_the_call():
    leaky = syn.building_card("B1", 9, 12)
    leaky["change_summary"] += " (см. SF-123)"
    bundle = syn.bundle_of([(syn.building_card("A1", 7, 11), "R-7"), (leaky, "R-9")], region_locations=LOCS)
    prepared = E.prepare(bundle)
    assert [c.status for c in prepared.calls] == [E.NOT_SENT]
    assert prepared.calls[0].reason == "LABEL_LEAK_GUARD" and prepared.ready_calls == []


def test_over_budget_payload_is_not_sent(monkeypatch):
    monkeypatch.setattr(PL, "PAYLOAD_BUDGET_CHARS", 500)
    prepared = E.prepare(_three_buildings_and_pump())
    assert prepared.calls and all(c.status == E.NOT_SENT and c.reason == "PAYLOAD_OVER_BUDGET" for c in prepared.calls)


def test_input_freeze_is_deterministic():
    a = E.prepare(_three_buildings_and_pump()).input_freeze()
    b = E.prepare(_three_buildings_and_pump()).input_freeze()
    assert a["input_freeze_sha256"] == b["input_freeze_sha256"]
    assert a["contracts"]["prompt_sha256"] == C.CONSOLIDATOR_PROMPT_SHA256


# --------------------------------------------------------------------------- engine
def test_model_visible_text_equals_the_v3_transport_text():
    prepared = E.prepare(_three_buildings_and_pump())
    call = prepared.ready_calls[0]
    text, paths, _ = build_codex_payload(prepared.prompt, call.payload, [])
    assert paths == [] and hashlib.sha256(text.encode()).hexdigest() == call.model_visible_sha256


def test_exact_calls_stage_images_and_record_before_return(monkeypatch):
    import backend.app.services.project_change_v3.provider as prov

    monkeypatch.setattr(prov, "get_provider", lambda: (_ for _ in ()).throw(AssertionError("get_provider used")))
    prepared = E.prepare(_three_buildings_and_pump())
    fake = FakeProvider(handlers={C.STAGE: _keep_all})
    seen = []
    records = E.execute(prepared, fake, max_calls=12, on_record=seen.append)
    assert len(fake.calls) == len(prepared.ready_calls) == len(records) == len(seen)
    assert all(c["stage"] == "CONSOLIDATE" and c["images"] == 0 for c in fake.calls)
    assert all(r["status"] == "RESPONDED" and r["raw_response_sha256"] and r["automatic_retries"] == 0 for r in seen)


def test_provider_error_is_not_retried():
    prepared = E.prepare(_three_buildings_and_pump())

    def boom(**_):
        raise ProviderError("provider_timeout", "slow")

    fake = FakeProvider(handlers={C.STAGE: boom})
    records = E.execute(prepared, fake, max_calls=12, on_record=lambda r: None)
    assert len(fake.calls) == len(prepared.ready_calls)
    assert all(r["status"] == "PROVIDER_ERROR" and r["error_code"] == "provider_timeout" and r["attempt"] == 1
               for r in records)


def test_call_plan_above_limit_sends_nothing():
    prepared = E.prepare(_three_buildings_and_pump())
    fake = FakeProvider(handlers={C.STAGE: _keep_all})
    with pytest.raises(E.CallPlanExceeded):
        E.execute(prepared, fake, max_calls=0, on_record=lambda r: None)
    assert fake.calls == []
    plan = prepared.call_plan(0)
    assert plan["within_limit"] is False and plan["automatic_retries"] == 0


def test_stop_request_cancels_before_send():
    prepared = E.prepare(_three_buildings_and_pump())
    fake = FakeProvider(handlers={C.STAGE: _keep_all})
    records = E.execute(prepared, fake, max_calls=12, on_record=lambda r: None, should_stop=lambda: True)
    assert fake.calls == [] and all(r["status"] == "CANCELLED_BEFORE_SEND" for r in records)
