#!/usr/bin/env python3
"""Probe HYBRID-7: evidence for the deterministic/AI boundary.

(a) correspondence space: how many segment-to-segment assignments exist per pair
    (the thing an LLM must never be asked to do);
(b) how many of the deterministic `value_changes` correspondences are made over
    text the extractor itself flagged undecodable, or across wildly different
    strings — i.e. machine-made guesses shipped to the model as facts;
(c) title-block / stamp leakage: change events that are frame text, not design;
(d) crop-truncation predicate: right-only strings that are prefixes of left-only
    strings (the ss_table_graphic case) — a deterministic trigger for a Vision
    question rather than a reported "added position".

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p7_boundary_evidence
"""
from __future__ import annotations

import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

STAMP = (
    "Согласовано", "Подп. и дата", "Взам. инв. №", "Инв. № подл.", "ГАП", "ГКП",
    "Гл.спец", "Разраб", "Пров.", "Н.контр", "Утв.", "Стадия", "Листов", "Лист",
    "Изм.", "Кол.уч", "№ док", "Дата", "Формат", "Копировал",
)


def load(pid, side):
    return json.loads((TA / "descriptions" / pid / side / "vector_block.json").read_text("utf-8"))


def cmpj(pid):
    return json.loads((TA / "comparisons" / pid / "comparison.json").read_text("utf-8"))


def garbage(s: str) -> bool:
    return any(ord(c) < 32 and not c.isspace() for c in s)


def is_stamp(s: str) -> bool:
    t = s.strip()
    return any(t.startswith(w) or w in t for w in STAMP)


def main() -> None:
    pairs = json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]
    res = {}
    for p in pairs:
        pid = p["pair_id"]
        l, r = load(pid, "left"), load(pid, "right")
        c = cmpj(pid)
        ls = l["topology"]["segments_total"]
        rs = r["topology"]["segments_total"]
        vc = c["text"]["value_changes"]
        vc_garbage = [v for v in vc if garbage(v["left"]) or garbage(v["right"])]
        vc_wild = [
            v for v in vc
            if not garbage(v["left"]) and not garbage(v["right"])
            and max(len(v["left"]), len(v["right"])) > 0
            and min(len(v["left"]), len(v["right"])) / max(len(v["left"]), len(v["right"])) < 0.5
        ]
        added, removed = c["text"]["added"], c["text"]["removed"]
        stamp_events = [s for s in added + removed if is_stamp(s)]
        # crop-truncation predicate
        trunc = []
        for a in added:
            for d in removed:
                if a and d and a != d and (d.startswith(a) or a.startswith(d)):
                    trunc.append([d, a])
                    break
        res[pid] = {
            "segments": [ls, rs],
            "assignment_space_log10": round(math.log10(max(ls, 1)) + math.log10(max(rs, 1)), 2),
            "n_value_changes": len(vc),
            "value_changes_over_undecodable_text": len(vc_garbage),
            "value_changes_between_dissimilar_strings": len(vc_wild),
            "value_changes_examples_wild": [[v["left"], v["right"]] for v in vc_wild[:4]],
            "n_text_events": len(added) + len(removed),
            "stamp_frame_events": len(stamp_events),
            "stamp_frame_pct": round(100.0 * len(stamp_events) / max(len(added) + len(removed), 1), 1),
            "stamp_examples": sorted(set(stamp_events))[:6],
            "prefix_truncation_pairs": len(trunc),
            "prefix_truncation_examples": trunc[:4],
        }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_boundary_evidence.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(f"{'pair':24s} {'segsL/R':>14} {'log10 space':>11} {'vc':>4} {'vc_garb':>8} {'vc_wild':>8} "
          f"{'events':>7} {'stamp':>6} {'stamp%':>7} {'trunc':>6}")
    for k, v in res.items():
        print(f"{k:24s} {str(v['segments']):>14} {v['assignment_space_log10']:>11} {v['n_value_changes']:>4} "
              f"{v['value_changes_over_undecodable_text']:>8} {v['value_changes_between_dissimilar_strings']:>8} "
              f"{v['n_text_events']:>7} {v['stamp_frame_events']:>6} {v['stamp_frame_pct']:>7} {v['prefix_truncation_pairs']:>6}")
    print()
    for k in ("ss_table_graphic", "eom_singleline_changed", "vk_nodes"):
        print("==", k, json.dumps(res[k]["prefix_truncation_examples"], ensure_ascii=False)[:400])
        print("   wild:", json.dumps(res[k]["value_changes_examples_wild"], ensure_ascii=False)[:400])
        print("   stamp:", json.dumps(res[k]["stamp_examples"], ensure_ascii=False)[:300])


if __name__ == "__main__":
    main()
