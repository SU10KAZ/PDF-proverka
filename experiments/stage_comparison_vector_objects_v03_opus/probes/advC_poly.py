# -*- coding: utf-8 -*-
"""advC: does `polygon_pt` change any published record on the real benchmark?"""
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
import ctr_common as C


def ring_display(side, ex):
    rj = json.load(open(G.ROOT / side["result_json"], encoding="utf-8"))
    blk = None
    for pg in rj.get("pages") or []:
        for b in pg.get("blocks") or []:
            if b.get("id") == side["block_id"]:
                blk = b
    if not blk:
        return None
    pts = blk.get("polygon_points") or blk.get("polygon") or blk.get("points")
    if not pts:
        return None
    cp = side["coords_px"]; fd = ex.frame["clip_display"]
    sx = (fd[2] - fd[0]) / max(cp[2] - cp[0], 1e-9)
    sy = (fd[3] - fd[1]) / max(cp[3] - cp[1], 1e-9)
    return [[fd[0] + (q[0] - cp[0]) * sx, fd[1] + (q[1] - cp[1]) * sy] for q in pts]


def clip(ex, ring):
    keep = []
    for s in ex.segments:
        mx = (s["p0"][0] + s["p1"][0]) / 2.0; my = (s["p0"][1] + s["p1"][1]) / 2.0
        if C.point_in_ring(mx, my, ring):
            keep.append(s)
    ex.segments = keep
    return ex


pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
sel = [p for p in pairs
       if p["side_a"].get("shape_type") == "polygon" or p["side_b"].get("shape_type") == "polygon"]
out = []
for p in sel:
    row = {"pair_id": p["pair_id"], "expected": p["expected_verdict"],
           "shapes": [p["side_a"].get("shape_type"), p["side_b"].get("shape_type")]}
    try:
        def build(do_clip):
            exs = []
            for tag in ("side_a", "side_b"):
                s = p[tag]
                e = G.F.extract_block(str(G.ROOT / s["pdf"]), s["page_index"], s["coords_px"],
                                      s["page_px"][0], s["page_px"][1])
                if do_clip and s.get("shape_type") == "polygon":
                    r = ring_display(s, e)
                    if r:
                        e = clip(e, r)
                exs.append(e)
            exA, exB = exs
            if not exA.segments or not exB.segments:
                raise RuntimeError("empty side")
            ca, cb = exA.frame["clip_display"], exB.frame["clip_display"]
            base = (ca[0] - cb[0], ca[1] - cb[1])
            sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), base, (float(sd[0]), float(sd[1])),
                     (base[0] + float(sd[0]), base[1] + float(sd[1]))}
            dx, dy, sc = M.register(exA.segments, exB.segments, seeds)
            LA, LB, meta = L.layers(exA, exB)
            raw = LDG.raw_ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
            b = LDG.build(exA, exB, off=(dx, dy), led=raw)
            return {"n_seg": [len(exA.segments), len(exB.segments)],
                    "ink": [round(sum(s["len"] for s in exA.segments), 1),
                            round(sum(s["len"] for s in exB.segments), 1)],
                    "n_raw": raw["n_records"], "n_pub": len(b["changes"]),
                    "phrases": [x["text"] for x in LDG.phrases(b)]}
        row["rect_clip"] = build(False)
        row["poly_clip"] = build(True)
    except Exception as e:
        row["error"] = type(e).__name__ + ": " + str(e)[:100]
    out.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
(ART / "advC_polygon.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                       encoding="utf-8")
