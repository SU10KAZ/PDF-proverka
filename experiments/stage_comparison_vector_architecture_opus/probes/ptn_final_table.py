"""Consolidate every ptn_ measurement into one table: would a per-motif count sentence fire,
and would it be right?

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_final_table
Writes artifacts/ptn_final_table.json (+ Markdown on stdout)
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

TRUTH = {
    "ar_plan": ("same PDF bytes (O1)", "no graphic change"),
    "ar_wall_sections": ("same PDF bytes (O1)", "no graphic change"),
    "ss_plan_dense": ("NEAR_IDENTICAL", "no graphic change"),
    "ss_simple_node": ("IDENTICAL", "no graphic change"),
    "ss_table_graphic": ("NEAR_IDENTICAL", "no graphic change"),
    "vk_plan": ("NEAR_IDENTICAL", "no graphic change"),
    "vk_node_plan": ("NEAR_IDENTICAL", "no graphic change"),
    "vk_nodes": ("STRUCTURE_SAME_VALUES_CHANGED", "extra notes area on the right"),
    "ss_scheme_text_changed": ("STRUCTURE_SAME_VALUES_CHANGED", "text only: 15 cameras and 5 OSPD on BOTH sides"),
    "eom_singleline_changed": ("STRUCTURE_CHANGED", "2 -> 4 apparatus rows; 14 -> 14 crossed-square markers; 20 -> 20 terminal circles"),
}
RECUT = {"ar_wall_sections", "vk_node_plan", "vk_nodes"}


def main() -> None:
    diff = json.load(open(ART / "ptn_pair_diff.json", encoding="utf-8"))
    recut = json.load(open(ART / "ptn_recut_diff.json", encoding="utf-8"))
    tol = json.load(open(ART / "ptn_tolerant_match_twopass.json", encoding="utf-8"))
    rows = {}
    print("| pair | ground truth | S0 changed/app/dis | S1 changed/app/dis | S5c changed | S6 changed | verdict |")
    print("|---|---|---|---|---|---|---|")
    for pid, (verdict, truth) in TRUTH.items():
        s0 = diff[pid]["sigs"]["S0"]
        s1 = diff[pid]["sigs"]["S1"]
        s5c = diff[pid]["sigs"]["S5c"]
        s6 = tol[pid]
        note = ""
        if pid in RECUT:
            r = recut[pid]
            s1 = {"changed_n": r["S1"]["changed"], "appeared_n": r["S1"]["appeared"], "disappeared_n": r["S1"]["disappeared"]}
            s5c = {"changed_n": r["S5c"]["changed"]}
            s6 = {"clusters_changed": r["S6_twopass"]["changed"]}
            note = " (uncapped re-extraction)"
        rows[pid] = {
            "human_verdict": verdict, "ground_truth": truth,
            "S0": [s0["changed_n"], s0["appeared_n"], s0["disappeared_n"]],
            "S1": [s1["changed_n"], s1["appeared_n"], s1["disappeared_n"]],
            "S5c_changed": s5c["changed_n"],
            "S6_twopass_changed": s6["clusters_changed"],
            "note": note.strip(),
        }
        print(f"| {pid} | {truth}{note} | {s0['changed_n']}/{s0['appeared_n']}/{s0['disappeared_n']} | "
              f"{s1['changed_n']}/{s1['appeared_n']}/{s1['disappeared_n']} | {s5c['changed_n']} | "
              f"{s6['clusters_changed']} | {verdict} |")
    # graphics-unchanged controls: the 7 pairs with no graphic change PLUS ss_scheme_text_changed,
    # whose graphics are identical on both sides (15 cameras / 5 OSPD each) - only the text differs.
    unchanged = [p for p, (v, t) in TRUTH.items() if t == "no graphic change"] + ["ss_scheme_text_changed"]
    summary = {
        "unchanged_pairs": len(unchanged),
        "pairs_where_S0_emits_a_false_repeated_pattern_line": sum(
            1 for p in unchanged if sum(rows[p]["S0"]) > 0),
        "pairs_where_S1_emits_a_false_count_change": sum(1 for p in unchanged if rows[p]["S1"][0] > 0),
        "pairs_where_S1_emits_any_false_line": sum(1 for p in unchanged if sum(rows[p]["S1"]) > 0),
        "pairs_where_S6_emits_a_false_count_change": sum(1 for p in unchanged if rows[p]["S6_twopass_changed"] > 0),
    }
    print()
    print(json.dumps(summary, indent=1))
    with open(ART / "ptn_final_table.json", "w", encoding="utf-8") as handle:
        json.dump({"rows": rows, "summary": summary}, handle, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
