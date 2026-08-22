"""S6: tolerant motif matching (raster prototype + Jaccard leader clustering, D4-invariant).

Contrast with S1..S5c, which are exact hashes: S6 accepts small dimensional jitter.
Both sides of a pair are clustered TOGETHER, then instances are counted per side, so a
count delta is a real per-motif "12 -> 14" statement.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_tolerant_match
Writes artifacts/ptn_tolerant_match.json
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
GRID = int(os.environ.get("PTN_GRID", "20"))
THRESH = float(os.environ.get("PTN_JACCARD", "0.62"))


def rasterize(points_segments, grid: int = GRID) -> frozenset:
    xs = [p[0] for seg in points_segments for p in seg]
    ys = [p[1] for seg in points_segments for p in seg]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    scale = max(x1 - x0, y1 - y0, 1e-9)
    cells = set()
    for a, b in points_segments:
        ax, ay = (a[0] - x0) / scale, (a[1] - y0) / scale
        bx, by = (b[0] - x0) / scale, (b[1] - y0) / scale
        steps = max(2, int(math.hypot(bx - ax, by - ay) * grid * 2))
        for k in range(steps + 1):
            t = k / steps
            cx = min(grid - 1, int((ax + (bx - ax) * t) * grid))
            cy = min(grid - 1, int((ay + (by - ay) * t) * grid))
            cells.add((cx, cy))
    return frozenset(cells)


def d4_rasters(segs) -> list[frozenset]:
    out = []
    for variant in M._d4_variants(segs):
        out.append(rasterize(variant))
    return out


def jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / (len(a) + len(b) - inter)


def cluster(items: list[dict], thresh: float = THRESH):
    """Greedy leader clustering. items: {'rasters': [...], 'side': 'left'|'right', 'aspect': f}"""
    leaders: list[dict] = []
    for item in items:
        best, best_j = None, thresh
        for leader in leaders:
            if abs(math.log((item["aspect"] + 1e-6) / (leader["aspect"] + 1e-6))) > 0.25:
                continue
            j = max(jaccard(r, leader["raster"]) for r in item["rasters"])
            if j >= best_j:
                best, best_j = leader, j
        if best is None:
            leaders.append({"raster": item["rasters"][0], "aspect": item["aspect"],
                            "left": 0, "right": 0, "nseg": item["nseg"], "diag": item["diag"],
                            "members": []})
            best = leaders[-1]
        best[item["side"]] += 1
        best["members"].append(item["idx"])
    return leaders


def cluster_two_pass(items: list[dict], thresh: float = THRESH):
    """Deterministic variant: build prototypes from a canonically ordered union,
    then assign EVERY item to its best-matching prototype (not the first above threshold)."""
    order = sorted(range(len(items)), key=lambda i: (-items[i]["nseg"], -items[i]["diag"], items[i]["idx"]))
    protos: list[dict] = []
    for i in order:
        item = items[i]
        best, best_j = None, thresh
        for proto in protos:
            if abs(math.log((item["aspect"] + 1e-6) / (proto["aspect"] + 1e-6))) > 0.25:
                continue
            j = max(jaccard(r, proto["raster"]) for r in item["rasters"])
            if j >= best_j:
                best, best_j = proto, j
        if best is None:
            protos.append({"raster": item["rasters"][0], "aspect": item["aspect"],
                           "left": 0, "right": 0, "nseg": item["nseg"], "diag": item["diag"]})
    for item in items:
        best, best_j = None, -1.0
        for proto in protos:
            if abs(math.log((item["aspect"] + 1e-6) / (proto["aspect"] + 1e-6))) > 0.25:
                continue
            j = max(jaccard(r, proto["raster"]) for r in item["rasters"])
            if j > best_j:
                best, best_j = proto, j
        if best is not None and best_j >= thresh:
            best[item["side"]] += 1
    return protos


def motif_items(pair: str, side: str) -> list[dict]:
    desc = M.load_description(TRACK_A / pair / side / "vector_block.json")
    bundle = M.build_motifs(desc, unit="cc_split")
    segments = bundle["segments"]
    items = []
    for idx, m in enumerate(bundle["motifs"]):
        segs = [(segments[i]["p0"], segments[i]["p1"]) for i in m["seg_indexes"]]
        x0, y0, x1, y1 = m["bbox"]
        w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
        items.append(
            {
                "rasters": d4_rasters(segs),
                "aspect": min(w, h) / max(w, h),
                "side": side,
                "nseg": m["nseg"],
                "diag": round(m["diag"], 2),
                "idx": idx,
            }
        )
    return items


def main() -> None:
    pairs = json.load(open(ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json",
                           encoding="utf-8"))["pairs"]
    only = set(sys.argv[1:]) or None
    out = {}
    for pair in pairs:
        pid = pair["pair_id"]
        if only and pid not in only:
            continue
        t0 = time.time()
        items = motif_items(pid, "left") + motif_items(pid, "right")
        leaders = (cluster_two_pass(items) if os.environ.get("PTN_TWOPASS") == "1" else cluster(items))
        rows = [
            {"left": leader["left"], "right": leader["right"], "nseg": leader["nseg"], "diag": leader["diag"]}
            for leader in leaders
            if max(leader["left"], leader["right"]) >= 2
        ]
        rows.sort(key=lambda r: -(r["left"] + r["right"]))
        changed = [r for r in rows if r["left"] != r["right"]]
        out[pid] = {
            "human_expected": pair["human_expected"],
            "motifs_left": sum(1 for i in items if i["side"] == "left"),
            "motifs_right": sum(1 for i in items if i["side"] == "right"),
            "clusters_repeated": len(rows),
            "clusters_changed": len(changed),
            "clusters_appeared": sum(1 for r in rows if r["left"] == 0),
            "clusters_disappeared": sum(1 for r in rows if r["right"] == 0),
            "max_abs_delta": max((abs(r["left"] - r["right"]) for r in changed), default=0),
            "top": rows[:20],
            "changed_top": sorted(changed, key=lambda r: -abs(r["left"] - r["right"]))[:20],
            "elapsed_s": round(time.time() - t0, 1),
            "grid": GRID,
            "jaccard_threshold": THRESH,
        }
        print(pid, "clusters", len(rows), "changed", len(changed), "appeared", out[pid]["clusters_appeared"],
              "disappeared", out[pid]["clusters_disappeared"], f'{out[pid]["elapsed_s"]}s', flush=True)
    path = OUT / ("ptn_tolerant_match_twopass.json" if os.environ.get("PTN_TWOPASS") == "1" else "ptn_tolerant_match.json")
    if only:
        path = OUT / "ptn_tolerant_match_subset.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=1)
    print(path)


if __name__ == "__main__":
    main()
