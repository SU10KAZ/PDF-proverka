#!/usr/bin/env python3
"""TCF probe 2 — what the 8000-segment topology cap does.

For every block where `segments_capped` is true, recompute the topology with the
Track A cap (8000) and with an effectively unlimited cap, and measure:
  * the nine counts the comparator averages, and the comparator topology
    similarity of the real left/right pair, capped vs uncapped;
  * the selection bias of "keep the longest segments": length cut-off, share of
    total length retained, occupied-cell coverage of a 40x40 normalized grid,
    axis-aligned share, and the share of segments that lie inside a text bbox
    (detail linework) among retained vs dropped.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p2_cap
"""
from __future__ import annotations

import json
import math
import pathlib
import statistics
import time

from experiments.stage_comparison_vector_blocks.comparator import _topology_diff
from experiments.stage_comparison_vector_blocks.extractor import _all_segments
from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p2_cap.json")
BIG_CAP = 5_000_000
GRID = 40


def selection_profile(description: dict, cap: int) -> dict:
    segments = _all_segments(description["geometry"]["primitives"])
    ordered = sorted(segments, key=lambda item: item["length"], reverse=True)
    kept, dropped = ordered[:cap], ordered[cap:]
    text_boxes = [t["bbox_norm"] for t in description["texts"]]

    def cells(items):
        out = set()
        for s in items:
            for p in (s["p1"], s["p2"]):
                out.add((min(GRID - 1, max(0, int(p[0] * GRID))), min(GRID - 1, max(0, int(p[1] * GRID)))))
        return out

    def axis_share(items):
        if not items:
            return 0.0
        n = 0
        for s in items:
            dx, dy = abs(s["p2"][0] - s["p1"][0]), abs(s["p2"][1] - s["p1"][1])
            if min(dx, dy) <= 1e-4:
                n += 1
        return round(n / len(items), 4)

    def in_text_share(items, sample=4000):
        step = max(1, len(items) // sample)
        picked = items[::step]
        if not picked or not text_boxes:
            return None
        n = 0
        for s in picked:
            mx = (s["p1"][0] + s["p2"][0]) / 2
            my = (s["p1"][1] + s["p2"][1]) / 2
            if any(b[0] <= mx <= b[2] and b[1] <= my <= b[3] for b in text_boxes):
                n += 1
        return round(n / len(picked), 4)

    total_length = sum(s["length"] for s in segments)
    return {
        "segments_total": len(segments),
        "kept": len(kept),
        "dropped": len(dropped),
        "length_cutoff": kept[-1]["length"] if kept else 0.0,
        "median_length_all": round(statistics.median(s["length"] for s in segments), 6),
        "median_length_kept": round(statistics.median(s["length"] for s in kept), 6) if kept else 0,
        "median_length_dropped": round(statistics.median(s["length"] for s in dropped), 6) if dropped else 0,
        "length_share_kept": round(sum(s["length"] for s in kept) / total_length, 4) if total_length else 0,
        "cells_all": len(cells(segments)),
        "cells_kept": len(cells(kept)),
        "cell_coverage_kept": round(len(cells(kept)) / max(1, len(cells(segments))), 4),
        "axis_aligned_share_kept": axis_share(kept),
        "axis_aligned_share_dropped": axis_share(dropped),
        "inside_text_bbox_share_kept": in_text_share(kept),
        "inside_text_bbox_share_dropped": in_text_share(dropped),
        "long_share_kept": round(sum(1 for s in kept if s["length"] >= 0.1) / max(1, len(kept)), 4),
        "long_share_all": round(sum(1 for s in segments if s["length"] >= 0.1) / max(1, len(segments)), 4),
    }


def main() -> None:
    out: dict = {"probe": "tcf_p2_cap", "big_cap": BIG_CAP, "blocks": {}, "pairs": {}}
    cache: dict[tuple[str, str, str], dict] = {}
    for pair_dir in sorted(ROOT.iterdir()):
        pair = pair_dir.name
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            description = json.loads(path.read_text(encoding="utf-8"))
            if not description["topology"]["segments_capped"]:
                continue
            primitives = description["geometry"]["primitives"]
            row = {"selection": selection_profile(description, 8_000)}
            for label, cap in (("cap8000", 8_000), ("uncapped", BIG_CAP)):
                t0 = time.time()
                topo = tcf_topo.topology(primitives, 0.0025, cap)
                topo.pop("_dropped_segment_lengths", None)
                topo["_seconds"] = round(time.time() - t0, 1)
                cache[(pair, side, label)] = topo
                row[label] = {k: topo[k] for k in tcf_topo.COMPARATOR_KEYS}
                row[label]["seconds"] = topo["_seconds"]
                row[label]["collapsed_segments"] = topo["_collapsed_segments"]
                row[label]["crossings_truncated"] = topo["crossings_truncated"]
            out["blocks"][f"{pair}/{side}"] = row
            print(f"{pair}/{side} cap8000={row['cap8000']} uncapped={row['uncapped']}", flush=True)
    for pair_dir in sorted(ROOT.iterdir()):
        pair = pair_dir.name
        if (pair, "left", "cap8000") in cache and (pair, "right", "cap8000") in cache:
            out["pairs"][pair] = {
                "similarity_cap8000": _topology_diff(
                    cache[(pair, "left", "cap8000")], cache[(pair, "right", "cap8000")]
                )["similarity"],
                "similarity_uncapped": _topology_diff(
                    cache[(pair, "left", "uncapped")], cache[(pair, "right", "uncapped")]
                )["similarity"],
                "similarity_cap_vs_uncapped_same_block": _topology_diff(
                    cache[(pair, "left", "cap8000")], cache[(pair, "left", "uncapped")]
                )["similarity"],
            }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\npair\tsim_cap8000\tsim_uncapped\tself_cap_vs_uncapped")
    for pair, row in out["pairs"].items():
        print(f"{pair}\t{row['similarity_cap8000']:.4f}\t{row['similarity_uncapped']:.4f}\t{row['similarity_cap_vs_uncapped_same_block']:.4f}")


if __name__ == "__main__":
    main()
