# -*- coding: utf-8 -*-
"""Is G1-a evidence, or an identity?

`v03_objects.build_objects` reads exactly these per-segment fields:
    p0, p1, len            (chains, primitives, dash runs, descriptors)
    w, color               ONLY when style_split=True — the default is False
It never reads `path`, `op`, `closed` or `fill`.

So the test is arithmetic, not statistical: does the rewrite change the multiset of
(p0, p1) at all?  If it does not, the object layer cannot possibly change, and the
203/203 result carries no information about stability.
"""
from __future__ import annotations
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import advB_rw as R

SEED = 20260823
NAMES = ["A1_path_split", "A2_path_merge", "A5_order_shuffle", "A8_lineweight",
         "A4_circle_to_bezier", "A6_round_0.25"]


def geom_key(segs):
    return sorted((round(s["p0"][0], 9), round(s["p0"][1], 9),
                   round(s["p1"][0], 9), round(s["p1"][1], 9)) for s in segs)


def main():
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 20 <= b["n_seg"] <= 20000]
    out = {"n_blocks": 0, "per_rewrite": {n: {"geometry_identical": 0, "n": 0,
                                              "style_changed": 0, "path_changed": 0}
                                          for n in NAMES + ["X1_split_at_0.37", "X7_rect_to_lines",
                                                            "X8_lines_to_rect", "X2_reverse_vertices"]}}
    for rec in blocks:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            continue
        ex = G.extract(pb)
        if not ex.segments:
            continue
        out["n_blocks"] += 1
        base = geom_key(ex.segments)
        fns = {n: G.REWRITES[n] for n in NAMES}
        fns["X1_split_at_0.37"] = R._split_at([0.37], 0.5)
        fns["X7_rect_to_lines"] = R.rw_rect_to_lines
        fns["X8_lines_to_rect"] = R.rw_lines_to_rect
        fns["X2_reverse_vertices"] = R.rw_reverse_vertices
        for n, fn in fns.items():
            try:
                segs = fn(ex.segments, random.Random(SEED))
            except Exception:
                continue
            d = out["per_rewrite"][n]
            d["n"] += 1
            k2 = geom_key(segs)
            if n == "X2_reverse_vertices":
                # direction flip: compare as unordered endpoint pairs
                norm = lambda L: sorted(tuple(sorted([(a, b), (c, e)])) for a, b, c, e in L)
                if norm(base) == norm(k2):
                    d["geometry_identical"] += 1
            elif base == k2:
                d["geometry_identical"] += 1
            sk = lambda L: sorted(repr((s["w"], s["color"])) for s in L)
            if sk(segs) != sk(ex.segments):
                d["style_changed"] += 1
            if sorted(s["path"] for s in segs) != sorted(s["path"] for s in ex.segments):
                d["path_changed"] += 1
        if out["n_blocks"] % 25 == 0:
            print(out["n_blocks"], flush=True)
    for n, d in out["per_rewrite"].items():
        d["geometry_identical_share"] = round(d["geometry_identical"] / max(1, d["n"]), 4)
    json.dump(out, open(G.ART / "advB_tautology.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
