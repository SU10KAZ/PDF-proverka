"""falsify_ probe, attack B: how stable is v0.1 normalized geometry across
instances of the SAME symbol on the SAME real page?

If the backbone is universal, two instances of one symbol must score ~1.0 on the
same order-independent directional segment coverage the comparator uses.
This probe takes the biggest same-descriptor instance groups mined by
falsify_symbol_collisions.py and reports the full pairwise similarity
distribution inside each group.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_same_symbol_spread \
      --pdf <path> --page <i> --min-seg 6 --min-cycles 1 --min-size 5 --max-size 80
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_architecture_opus.probes.falsify_symbol_collisions import (
    ART,
    ROOT,
    _segments,
    components,
    descriptor,
    shape_similarity,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--page", type=int, required=True)
    ap.add_argument("--min-seg", type=int, default=6)
    ap.add_argument("--max-seg", type=int, default=200)
    ap.add_argument("--min-size", type=float, default=5.0)
    ap.add_argument("--max-size", type=float, default=80.0)
    ap.add_argument("--min-cycles", type=int, default=1)
    ap.add_argument("--groups", type=int, default=6)
    ap.add_argument("--cap", type=int, default=24)
    ap.add_argument("--out", default="falsify_same_symbol_spread.json")
    args = ap.parse_args()

    path = ROOT / args.pdf if not Path(args.pdf).is_absolute() else Path(args.pdf)
    doc = fitz.open(path)
    page = doc[args.page]
    comps = components(_segments(page))
    kept = []
    for comp in comps:
        d = descriptor(comp)
        if not (args.min_seg <= d["n_segments"] <= args.max_seg):
            continue
        if not (args.min_size <= max(d["w"], d["h"]) <= args.max_size):
            continue
        if d["cycles"] < args.min_cycles:
            continue
        kept.append(d)
    by_key = collections.defaultdict(list)
    for d in kept:
        by_key[d["l3_key"]].append(d)
    groups = sorted(by_key.items(), key=lambda kv: -len(kv[1]))[: args.groups]
    rows = []
    for key, group in groups:
        group = group[: args.cap]
        sims = []
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                sims.append(shape_similarity(group[i]["norm_segments"], group[j]["norm_segments"]))
        if not sims:
            continue
        rows.append(
            {
                "l3_key": key,
                "instances_compared": len(group),
                "pairs": len(sims),
                "min": round(min(sims), 4),
                "median": round(statistics.median(sims), 4),
                "mean": round(statistics.fmean(sims), 4),
                "max": round(max(sims), 4),
                "pairs_below_0_985": sum(1 for s in sims if s < 0.985),
                "pairs_below_0_90": sum(1 for s in sims if s < 0.90),
                "share_below_0_985": round(sum(1 for s in sims if s < 0.985) / len(sims), 4),
                "example_bboxes": [g["bbox"] for g in group[:4]],
                "n_segments": group[0]["n_segments"],
            }
        )
    payload = {
        "pdf": str(path.relative_to(ROOT)) if str(path).startswith(str(ROOT)) else str(path),
        "page_index": args.page,
        "filters": vars(args),
        "note": "shape_similarity is the same order-independent directional segment "
        "coverage idea comparator._directional_segment_coverage uses, on "
        "component-normalized coordinates; 1.0 means the two instances are the "
        "same normalized shape.",
        "groups": rows,
    }
    (ART / args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print("%-6s %-6s %8s %8s %8s %8s %10s" % ("segs", "inst", "min", "median", "mean", "max", "<0.985"))
    for r in rows:
        print(
            "%-6d %-6d %8.4f %8.4f %8.4f %8.4f %9.1f%%"
            % (r["n_segments"], r["instances_compared"], r["min"], r["median"], r["mean"], r["max"],
               100 * r["share_below_0_985"])
        )
    print("wrote", ART / args.out)


if __name__ == "__main__":
    main()
