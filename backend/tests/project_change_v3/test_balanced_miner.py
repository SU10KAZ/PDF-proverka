from itertools import product

from backend.app.services.project_change_v3 import mapper_portions as mp, miner_plan


def test_miner_saves_input_without_expanding_the_original_rectangle():
    region = {"region_id": "R", "scope": "system", "old_pages": list(range(1, 9)),
              "new_pages": list(range(20, 44)), "important_text_blocks": [],
              "important_table_blocks": [], "important_graphic_blocks": []}

    def measure(r):
        cost = 10 * (len(r["old_pages"]) + len(r["new_pages"]))
        return {"estimated_input_tokens": cost, "input_budget_tokens": 100,
                "fits": cost <= 100, "errors": ["over budget"] if cost > 100 else []}

    legacy = mp.split_region_for_envelope(region, lambda r: measure(r)["fits"])
    balanced, report = miner_plan.balance_region(region, legacy, measure, mode="balanced")
    assert len(legacy) == 12 and len(balanced) == 8
    assert report["selected"] != "legacy"
    pairs = [pair for part in balanced for pair in product(part["old_pages"], part["new_pages"])]
    assert len(pairs) == len(set(pairs)) == 8 * 24
    assert set(pairs) == set(product(region["old_pages"], region["new_pages"]))
    shadow, _ = miner_plan.balance_region(region, legacy, measure, mode="shadow")
    assert shadow == legacy


def test_a_fitting_region_keeps_its_id_and_evidence():
    region = {"region_id": "R", "old_pages": [1], "new_pages": [99]}
    parts, _ = miner_plan.balance_region(region, [region],
                                         lambda r: {"estimated_input_tokens": 42}, mode="balanced")
    assert parts == [region]
