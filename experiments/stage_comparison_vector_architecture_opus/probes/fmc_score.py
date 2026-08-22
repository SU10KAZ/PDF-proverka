#!/usr/bin/env python3
"""FMC probe step 11 — score v0.1 on the failure-mode corpus.

Two scores per pair:

1. STATUS  — does comparator.status match the human verdict?
2. FACT RECALL — are the load-bearing tokens of the Russian expert sentence physically present
   anywhere in the comparator output (differences / text.added / text.removed / value_changes)?
   A fact that is not in the payload cannot be turned into the sentence by any downstream model,
   so this is an upper bound on what a report generator could say.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_score
"""
from __future__ import annotations

import json
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"
DESC = ART / "fmc_descriptions"

# tokens that MUST survive into the machine output for the human sentence to be derivable
KEY_FACTS: dict[str, list[str]] = {
    "fmc_eom_text_as_paths": ["13АВ-РД-ГРЩ2-ПА"],
    "fmc_kj_spec_table_reflow": [],           # correct answer is "nothing changed"
    "fmc_kj_steel_table_shift": [],           # correct answer is "nothing changed"
    "fmc_eom_room_schedule_values": ["17,68", "17,37", "497,57", "497,24"],
    "fmc_eom_tray_plan_geometry": ["h=2650", "h=3350"],
    "fmc_eom_notes_reflow": [],
    "fmc_eom_qr_stamp_only": [],
    "fmc_ov_raster_retile": [],
    "fmc_tx_raster_scan": [],
    "fmc_ss_a4_to_a3_reissue": ["V21", "0.338", "0.819"],
    "fmc_ov_block_split_widened": ["ø19,1", "Подъём"],
    "fmc_eom_drawing_list_rows": ["43", "44", "45", "46", "План кабельных лотков"],
    "fmc_eom_layout_reorg_mismatch": [],      # correct answer is "not comparable"
    "fmc_eom_cable_table_values": ["4х(1х185)+1х95", "5х35"],
    "fmc_gp_section_hatch_dims": ["Р4.2", "0,12", "асфальт"],
    "fmc_ov_page_shift_geometry": ["757", "692", "PatAIR"],
    "fmc_ar_hatch_sections": ["+4,935", "У2", "Ш1"],
    "fmc_vk_spec_positions": ["81", "86"],
    "fmc_km_broken_text_swap": ["+52,400", "+55,850"],
    "fmc_eom_rotated_labels": ["C25", "40"],
    "fmc_crop_mismatch_same_sheet": [],
}


def haystack(cmp: dict) -> str:
    parts = list(cmp.get("differences", []))
    t = cmp.get("text", {})
    parts += list(t.get("added", [])) + list(t.get("removed", []))
    for v in t.get("value_changes", []):
        parts += [str(v.get("left")), str(v.get("right"))]
    return " | ".join(parts)


def main() -> None:
    from .fmc_io import read_json
    manifest = {p["pair_id"]: p for p in read_json(ART / "fmc_pairs.json")["pairs"]}
    results = {r["pair_id"]: r for r in read_json(ART / "fmc_v01_results.json")}
    rows = []
    for pid, pair in manifest.items():
        r = results.get(pid)
        if r is None:
            continue
        cmp = read_json(DESC / f"{pid}_comparison.json")
        hay = haystack(cmp)
        keys = KEY_FACTS.get(pid, [])
        found = [k for k in keys if k in hay]
        missing = [k for k in keys if k not in hay]
        status_ok = r["status"] == r["human_expected"]
        rows.append(
            {
                "pair_id": pid,
                "discipline": pair["discipline"],
                "change_class": pair["change_class"],
                "human_status": r["human_expected"],
                "v01_status": r["status"],
                "status_correct": status_ok,
                "key_facts": keys,
                "facts_found": found,
                "facts_missing": missing,
                "fact_recall": (len(found) / len(keys)) if keys else None,
                "geometry_similarity": r["geometry_similarity"],
                "text_similarity": r["text_similarity"],
                "topology_similarity": r["topology_similarity"],
                "left_quality": r["left_quality"],
                "right_quality": r["right_quality"],
                "n_differences": r["n_differences"],
                "human_expected_ru": r["human_expected_ru"],
            }
        )
    n = len(rows)
    ok = sum(1 for r in rows if r["status_correct"])
    with_keys = [r for r in rows if r["key_facts"]]
    full_recall = sum(1 for r in with_keys if r["fact_recall"] == 1.0)
    zero_recall = sum(1 for r in with_keys if r["fact_recall"] == 0.0)
    # constant baselines
    from collections import Counter

    human = Counter(r["human_status"] for r in rows)
    best_const = human.most_common(1)[0]
    summary = {
        "pairs": n,
        "status_correct": ok,
        "status_accuracy": round(ok / n, 4),
        "best_constant_baseline": {"answer": best_const[0], "hits": best_const[1], "accuracy": round(best_const[1] / n, 4)},
        "human_status_distribution": dict(human),
        "v01_status_distribution": dict(Counter(r["v01_status"] for r in rows)),
        "pairs_with_key_facts": len(with_keys),
        "pairs_with_full_fact_recall": full_recall,
        "pairs_with_zero_fact_recall": zero_recall,
        "mean_fact_recall": round(sum(r["fact_recall"] for r in with_keys) / len(with_keys), 4) if with_keys else None,
        "rows": rows,
    }
    (ART / "fmc_score.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, ensure_ascii=False, indent=1))
    print()
    print(f"{'pair':34} {'human':30} {'v0.1':30} ok  recall  missing")
    for r in rows:
        rec = "-" if r["fact_recall"] is None else f"{r['fact_recall']:.2f}"
        print(f"{r['pair_id']:34} {r['human_status']:30} {r['v01_status']:30} "
              f"{'Y' if r['status_correct'] else 'N'}  {rec:>5}  {','.join(r['facts_missing'])[:50]}")


if __name__ == "__main__":
    main()
