# -*- coding: utf-8 -*-
"""advC: how much of the ledger's 'zero false records' is the L_min threshold and the
boundary drop, rather than the evidence?  Sweeps both on the SAME raw ledger."""
from __future__ import annotations
import json, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G
import grp_match as M
import loc_common as L
import ldg_ledger as LDG


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    outdir = ART / "advC_runs"; outdir.mkdir(exist_ok=True)
    out = open(outdir / f"thresh_{shard}.jsonl", "w", encoding="utf-8")
    for i, p in enumerate(pairs):
        if i % nsh != shard:
            continue
        row = {"pair_id": p["pair_id"], "expected": p["expected_verdict"],
               "human_objects": p.get("expected_changed_objects")}
        t0 = time.time()
        try:
            exA, exB = side(p["side_a"]), side(p["side_b"])
            if not exA.segments or not exB.segments:
                raise RuntimeError("no vector geometry on one side")
            clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
            base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
            sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), (base[0], base[1]), (float(sd[0]), float(sd[1])),
                     (base[0] + float(sd[0]), base[1] + float(sd[1]))}
            dx, dy, score = M.register(exA.segments, exB.segments, seeds)
            LA, LB, meta = L.layers(exA, exB)
            raw = LDG.raw_ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
            S = raw["S"]
            row["S"] = round(S, 4)
            row["n_raw"] = raw["n_records"]
            row["raw_len_sum"] = round(sum(r["change_len"] for r in raw["records"]), 1)
            row["raw_at_boundary"] = sum(1 for r in raw["records"] if r["at_boundary"])
            grid = {}
            for name, lmin in (("L0", 0.0), ("L3pt", 3.0), ("L1S", S), ("L2S", 2.0 * S),
                               ("Lprod", max(2.0 * S, 3.0)), ("L4S", 4.0 * S)):
                for db in (True, False):
                    b = LDG.build(exA, exB, off=(dx, dy), led=raw,
                                  min_change_len_pt=lmin, drop_boundary=db)
                    grid[f"{name}|bdrop={int(db)}"] = len(b["changes"])
            row["grid"] = grid
        except Exception as e:
            row["error"] = type(e).__name__ + ": " + str(e)[:100]
        row["t_sec"] = round(time.time() - t0, 1)
        out.write(json.dumps(row, ensure_ascii=False) + "\n"); out.flush()
        print(json.dumps(row, ensure_ascii=False), flush=True)
    out.close()


main()
