"""Re-run the S1 exact-hash diff and the S6 two-pass tolerant diff on RECUT (uncapped)
descriptions, to test whether the storage cap was the source of the count differences.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut_diff <pair_id> ...
Writes artifacts/ptn_recut_diff.json
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_tolerant_match as T  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
RECUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn/recut"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def side_data(pair: str, side: str):
    desc = M.load_description(RECUT / pair / side / "vector_block.json")
    bundle = M.build_motifs(desc, unit="cc_split")
    return desc, bundle


def s6_items(bundle, side):
    segments = bundle["segments"]
    items = []
    for idx, m in enumerate(bundle["motifs"]):
        segs = [(segments[i]["p0"], segments[i]["p1"]) for i in m["seg_indexes"]]
        x0, y0, x1, y1 = m["bbox"]
        w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
        items.append({"rasters": T.d4_rasters(segs), "aspect": min(w, h) / max(w, h),
                      "side": side, "nseg": m["nseg"], "diag": round(m["diag"], 2), "idx": idx})
    return items


def main() -> None:
    out = {}
    for pair in sys.argv[1:]:
        t0 = time.time()
        dl, bl = side_data(pair, "left")
        dr, br = side_data(pair, "right")
        row = {"prims_left": len(dl["geometry"]["primitives"]), "prims_right": len(dr["geometry"]["primitives"]),
               "motifs_left": len(bl["motifs"]), "motifs_right": len(br["motifs"])}
        for name, fn in (("S1", M.geom_core), ("S5c", M.geom_core_rot)):
            cl = Counter(M._hash((name, fn(m, bl["segments"]))) for m in bl["motifs"])
            cr = Counter(M._hash((name, fn(m, br["segments"]))) for m in br["motifs"])
            keys = {k for k in set(cl) | set(cr) if max(cl.get(k, 0), cr.get(k, 0)) >= 2}
            changed = [(k, cl[k], cr[k]) for k in keys if k in cl and k in cr and cl[k] != cr[k]]
            row[name] = {"groups": len(keys), "changed": len(changed),
                         "appeared": sum(1 for k in keys if k not in cl),
                         "disappeared": sum(1 for k in keys if k not in cr),
                         "max_delta": max((abs(a - b) for _k, a, b in changed), default=0),
                         "top_changed": sorted(changed, key=lambda t: -abs(t[1] - t[2]))[:8]}
        items = s6_items(bl, "left") + s6_items(br, "right")
        protos = T.cluster_two_pass(items)
        rows = [p for p in protos if max(p["left"], p["right"]) >= 2]
        changed = [p for p in rows if p["left"] != p["right"]]
        row["S6_twopass"] = {"clusters": len(rows), "changed": len(changed),
                             "appeared": sum(1 for p in rows if p["left"] == 0),
                             "disappeared": sum(1 for p in rows if p["right"] == 0),
                             "max_delta": max((abs(p["left"] - p["right"]) for p in changed), default=0),
                             "top_changed": [[p["left"], p["right"], p["nseg"], p["diag"]]
                                             for p in sorted(changed, key=lambda p: -abs(p["left"] - p["right"]))[:10]]}
        row["elapsed_s"] = round(time.time() - t0, 1)
        out[pair] = row
        print(pair, "S1 changed", row["S1"]["changed"], "S5c changed", row["S5c"]["changed"],
              "S6 changed", row["S6_twopass"]["changed"], f'{row["elapsed_s"]}s', flush=True)
    with open(OUT / "ptn_recut_diff.json", "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=1)
    print(OUT / "ptn_recut_diff.json")


if __name__ == "__main__":
    main()
