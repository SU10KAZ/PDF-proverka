# -*- coding: utf-8 -*-
"""advC: does the ledger's ink-similarity scalar agree with the plain ink balance?"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G
import grp_match as M
import loc_common as L
import ldg_ledger as LDG

pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
out = []
for p in pairs:
    row = {"pair_id": p["pair_id"], "expected": p["expected_verdict"]}
    try:
        a, b = p["side_a"], p["side_b"]
        exA = G.F.extract_block(str(G.ROOT / a["pdf"]), a["page_index"], a["coords_px"],
                                a["page_px"][0], a["page_px"][1])
        exB = G.F.extract_block(str(G.ROOT / b["pdf"]), b["page_index"], b["coords_px"],
                                b["page_px"][0], b["page_px"][1])
        if not exA.segments or not exB.segments:
            raise RuntimeError("no vector geometry")
        inkA = sum(s["len"] for s in exA.segments)
        inkB = sum(s["len"] for s in exB.segments)
        clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
        base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
        sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
        seeds = {(0.0, 0.0), base, (float(sd[0]), float(sd[1])),
                 (base[0] + float(sd[0]), base[1] + float(sd[1]))}
        dx, dy, score = M.register(exA.segments, exB.segments, seeds)
        LA, LB, meta = L.layers(exA, exB)
        raw = LDG.raw_ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
        rec_lost = sum(r["len_lost"] for r in raw["records"])
        rec_new = sum(r["len_new"] for r in raw["records"])
        row.update({
            "ink_a": round(inkA, 1), "ink_b": round(inkB, 1),
            "ink_delta_share_a": round((inkA - inkB) / max(inkA, 1e-9), 4),
            "scalar_ink_similarity": raw["scalar"]["ink_similarity"],
            "unmatched_share_a": raw["scalar"]["unmatched_share_a"],
            "raw_len_lost": round(rec_lost, 1), "raw_len_new": round(rec_new, 1),
            "raw_lost_share_of_ink_a": round(rec_lost / max(inkA, 1e-9), 4),
            "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
            "reg_score": round(score, 4)})
    except Exception as e:
        row["error"] = type(e).__name__ + ": " + str(e)[:90]
    out.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
(ART / "advC_ink_balance.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                           encoding="utf-8")
