"""Phase B: deterministic candidate prefilter (never decides a merge; 0 model calls)."""
from __future__ import annotations

import re
from pathlib import Path

from backend.app.services.project_change_consolidator import features as F
from backend.app.services.project_change_consolidator.prefilter import MAX_CLUSTER_CARDS, build_plan
from backend.tests.project_change_consolidator import synthetic as syn

PACKAGE = Path(__file__).resolve().parents[2] / "app" / "services" / "project_change_consolidator"


def test_location_units_are_syntactic_and_do_not_capture_counts():
    assert F.location_units("Здание 7 (16 эт.)") == {"здани 7"}
    assert F.location_units("корпуса №7, 16 этажей") == {"корпус 7"}
    assert F.location_units("зданий 7, 9 и 11") == {"здани 7", "здани 9", "здани 11"}
    assert F.location_units("корпуса 3 и 3.1") == {"корпус 3", "корпус 3.1"}
    assert F.location_units("Секция 7.2, этажи 1–6") == {"секци 7.2"}
    assert F.location_units("Этажи 1–6") == set()


def test_designations_and_numbers():
    found = F.designations("системы В8.1 и 3В1.1, установка COR-3")
    assert {"В8.1", "3В1.1"} <= found and any(d.endswith("R-3") for d in found)  # Latin C/O folded to Cyrillic
    assert "ДУ150" not in F.designations("труба Ду150")
    assert F.numbers("−2,100 и 1,800") == ["2.100", "1.800"]
    assert F.distinctive("2.9") and F.distinctive("41") and not F.distinctive("2")


def _plan(cards_regions, hints_by_region=None):
    result, miner = syn.result_of(cards_regions, hints_by_region)
    hints = result["unresolved_hints"]
    return build_plan(result["projectchanges"], [r for _, r in cards_regions], hints), result


def test_same_decision_across_three_regions_forms_one_cluster():
    plan, _ = _plan([(syn.building_card("A1", 7, 11), "R-7"), (syn.building_card("B1", 9, 12), "R-9"),
                     (syn.building_card("C1", 11, 13), "R-11"), (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")])
    clusters = [sorted(plan.card_ids[i] for i in c) for c in plan.call_clusters()]
    assert ["A1", "B1", "C1"] in clusters


def test_same_region_pair_never_forms_an_edge_even_with_identical_note():
    plan, _ = _plan([(syn.building_card("A1", 7, 11), "R-SAME"), (syn.building_card("A2", 7, 11), "R-SAME")])
    assert plan.candidate_edges == set()
    assert plan.call_clusters() == []


def test_cluster_cap_is_never_exceeded():
    cards = [(syn.building_card(f"X{i}", 20 + i, 40 + i), f"R-{i}") for i in range(12)]
    plan, _ = _plan(cards)
    assert all(2 <= len(c) <= MAX_CLUSTER_CARDS for c in plan.call_clusters())
    assert sum(len(c) for c in plan.clusters) == 12  # exact partition


def test_similar_values_in_different_systems_are_only_candidates():
    plan, _ = _plan([(syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-1"),
                     (syn.pump_card("P2", "В8", 31, "12,5", "17,3"), "R-2")])
    # the prefilter may show them together; deciding is the Consolidator's job
    assert (0, 1) in plan.candidate_edges


def test_thresholds_are_percentiles_of_the_run():
    base = [(syn.building_card("A1", 7, 11), "R-7"), (syn.building_card("B1", 9, 12), "R-9"),
            (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")]
    plan1, _ = _plan(base)
    plan2, _ = _plan(base + [(syn.pump_card("P2", "Т7", 33, "3,0", "4,5"), "R-Q")])
    assert plan1.thresholds != plan2.thresholds


def test_hint_from_another_region_is_attached_by_shared_evidence():
    shared = syn.evidence("NEW", 31, "n11-note", syn.NOTE)
    h = syn.hint("H001", "Трубопроводы В5 под потолком коридоров", "Схема здания 7 показывает трубопроводы в стяжке; противоречие 44,5",
                 [shared])
    a = syn.building_card("A1", 7, 11)
    plan, result = _plan([(a, "R-7"), (syn.building_card("B1", 9, 12), "R-9")], {"R-OTHER": [h]})
    attached = {(plan.card_ids[x.card_index], x.hint_index) for x in plan.attachments}
    assert ("A1", 0) in attached  # the only hint lives in region R-OTHER


def test_hint_cap_per_card_is_six_best_first():
    a = syn.building_card("A1", 7, 11)
    blk = a["evidence_items"][1]
    hints = [syn.hint(f"H{i:03d}", "Трубопроводы В5 под потолком", f"вопрос {i} прокладки коридоров", [blk]) for i in range(1, 9)]
    plan, _ = _plan([(a, "R-7"), (syn.building_card("B1", 9, 12), "R-9")], {"R-H": hints})
    mine = [x for x in plan.attachments if x.card_index == 0]
    assert len(mine) == 6 and plan.attachments_before_cap >= 8
    assert [x.rank_in_card for x in mine] == [1, 2, 3, 4, 5, 6]


def test_singleton_review_batch_selects_documentary_and_conflict_singletons():
    legend = syn.card("D1", subject="Условные обозначения арматуры на схеме", summary="Изменены условные обозначения листа.",
                      old="Легенда листа содержала смесители.", new="Легенда листа не содержит смесителей.")
    plan, _ = _plan([(legend, "R-D"), (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")])
    assert "D1" in [plan.card_ids[i] for i in plan.singleton_review]


def test_prefilter_is_deterministic():
    cards = [(syn.building_card("A1", 7, 11), "R-7"), (syn.building_card("B1", 9, 12), "R-9"),
             (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")]
    p1, _ = _plan(cards)
    p2, _ = _plan(cards)
    assert p1.to_json(["x"])["prefilter_sha256"] == p2.to_json(["x"])["prefilter_sha256"]


FORBIDDEN_LITERALS = [r"p290a06df79", r"\bSF-\d{3}\b", r"PCA-A-R\d{3}", r"a3672d0c", r"a631b49a",
                      r"Садовническ", r"Балчуг", r"корпус[а-я]*\s*(?:№\s*)?(?:3\.1|4)\b", r"2,6\s*→\s*2,9", r"ВСХНд"]


def test_no_dev5_literals_in_package_source():
    for path in PACKAGE.rglob("*"):
        if path.suffix not in {".py", ".txt"}:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_LITERALS:
            assert not re.search(pattern, text), (path.name, pattern)
