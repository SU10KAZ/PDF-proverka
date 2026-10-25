"""Full comparison coverage, exact admission, conservative plan selection."""
import copy
from itertools import product

import pytest

from backend.app.services.project_change_v3 import contracts, mapper_portions as mp, portion_planning as pp
from backend.app.services.project_change_v3.mapper_planning import MapperPlanner


def source(old=8, new=24):
    return [{"side": side, "physical_page": n, "blocks": [], "weight": 10}
            for side, count in (("OLD", old), ("NEW", new)) for n in range(1, count + 1)]


def select(pages, *, budget=100, exact=None, ratios=pp.RATIOS):
    def measurement(portion):
        packet = mp.portion_structure(pages, portion)
        cost = sum(p["weight"] for p in packet)
        fits = cost <= budget and (exact is None or exact(portion))
        return {"fits": fits, "errors": [] if fits else ["final packet exceeds budget"],
                "estimated_input_tokens": cost}
    return pp.select_plan(pages, mp.new_units(pages, None),
                          fits=lambda rows: sum(p["weight"] for p in rows) <= budget,
                          measure=measurement, weights={(p["side"], p["physical_page"]): p["weight"] for p in pages},
                          source_budget=budget, ratios=ratios)


def test_balance_reduces_repeated_input_and_keeps_every_pair_exactly_once():
    pages = source()
    before = copy.deepcopy(pages)
    result = select(pages)
    rows = {r["name"]: r for r in result.report["candidates"]}
    baseline, winner = rows["legacy"], rows[result.report["selected"]]
    assert baseline["calls"] == 12
    assert winner["calls"] == 8 and winner["estimated_input_tokens"] < baseline["estimated_input_tokens"]
    covered = [pair for p in result.portions for pair in product(p.old_pages or range(1, 9), p.new_pages)]
    assert len(covered) == len(set(covered)) == 8 * 24
    assert pages == before


@pytest.mark.parametrize("counts", [(1, 17), (7, 1), (13, 19)])
def test_unequal_pages_and_volumes_preserve_full_coverage(counts):
    pages = source(*counts)
    for i, p in enumerate(pages):
        p["weight"] = 1 + (i * 17) % 40
    result = select(pages)
    mp.validate_portion_coverage(pages, result.portions)
    again = select(pages)
    assert [p.summary() for p in result.portions] == [p.summary() for p in again.portions]


def test_exact_final_check_can_reject_every_balanced_plan_and_retain_legacy():
    result = select(source(), exact=lambda p: p.old_pages is None)
    assert result.report["selected"] == "legacy"
    assert any(row["status"] == "rejected" for row in result.report["candidates"])


def test_one_impossible_pair_refuses_the_plan():
    pages = source(2, 3)
    pages[0]["weight"] = 95
    with pytest.raises(mp.PortioningImpossible):
        select(pages)


def test_no_new_comparisons_are_added_when_planning_a_miner_region():
    pages = source(8, 24)
    pages = [p for p in pages if (p["side"] == "OLD" and p["physical_page"] in {2, 6, 8})
             or (p["side"] == "NEW" and p["physical_page"] in {4, 11, 24})]
    result = select(pages, budget=40)
    mp.validate_portion_coverage(pages, result.portions)
    assert {n for p in result.portions for n in p.new_pages} == {4, 11, 24}


def test_identical_or_more_expensive_candidates_do_not_replace_baseline():
    result = select(source(1, 2))
    assert result.report["selected"] == "legacy"


def test_real_payload_report_profiles_and_request_keys(tmp_path):
    pages = source(2, 3)
    for p in pages:
        p["blocks"] = [{"structured_md": "Таблица полностью сохранена", "modality": "TEXT"}]
    astra = MapperPlanner("pair", pages, contracts.MODEL_PROFILES["astra"])
    opus = MapperPlanner("pair", pages, contracts.MODEL_PROFILES["opus55"])
    a = astra.plan()
    assert a.report["selected"] == "single"
    assert a.report["model_calls"] == 0 and a.report["miner_plan"]["calls"] is None
    first = astra.verify([])[0]
    assert first["request_key"] != opus.verify([])[0]["request_key"]
    pages[0]["blocks"][0]["structured_md"] += " изменено"
    assert MapperPlanner("pair", pages, contracts.MODEL_PROFILES["astra"]).verify([])[0]["request_key"] != first["request_key"]


def test_real_balancing_counts_exact_packets_without_rewriting_md(monkeypatch):
    from backend.app.services.project_change_v3 import transport
    pages = source(8, 24)
    for p in pages:
        # Distinct strings so cross-page dictionary deduplication cannot mask cost.
        p["blocks"] = [{"modality": "TEXT", "structured_md": f"{p['side']} {p['physical_page']} " + "Ж" * 3000}]
    before = copy.deepcopy(pages)
    monkeypatch.setattr(transport, "CODEX_MAPPER_INPUT_TOKENS", 37000)
    planner = MapperPlanner("P", pages, contracts.MODEL_PROFILES["astra"])
    selected = planner.plan()
    rows = {r["name"]: r for r in selected.report["candidates"]}
    best, legacy = rows[selected.report["selected"]], rows["legacy"]
    assert best["calls"] <= legacy["calls"]
    packets = planner.verify(selected.portions)
    assert sum(p["estimated_input_tokens"] for p in packets) == best["estimated_input_tokens"]
    assert pages == before
    shadow = planner.plan(mode="shadow")
    assert shadow.report["selected"] == "legacy"
    assert len(shadow.portions) == legacy["calls"]


@pytest.mark.parametrize("budget", [18000, 25000, 37000])
def test_report_baseline_is_the_existing_production_probe(monkeypatch, budget):
    from backend.app.services.project_change_v3 import engine, transport

    pages = source(4, 9)
    for p in pages:
        p["blocks"] = [{"modality": "TEXT", "structured_md": f"{p['side']} {p['physical_page']} " + "Ж" * 2000}]
    monkeypatch.setattr(transport, "CODEX_MAPPER_INPUT_TOKENS", budget)
    profile = contracts.MODEL_PROFILES["astra"]
    planner = MapperPlanner("P", pages, profile)
    with contracts.use_profile(profile):
        expected = mp.plan_portions(pages, planner.units, lambda rows: engine.mapper_portions_mod_fits("P", rows))
    legacy = planner.plan(mode="legacy", allow_single=False)
    assert [p.summary() for p in legacy.portions] == [p.summary() for p in expected]
    balanced = planner.plan(mode="balanced", allow_single=False)
    baseline, = [r for r in balanced.report["candidates"] if r["name"] == "legacy"]
    assert baseline["calls"] == len(expected)
