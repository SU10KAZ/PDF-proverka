# -*- coding: utf-8 -*-
"""advB attack #2 — rewrites NOT in set A, against claim G1-a (203/203, layer unchanged).

Reports, per block per rewrite: d_obj, boundary_identical, churn 1:1 — exactly the
three numbers G1-a is stated in, computed by the SAME functions grp used.
"""
from __future__ import annotations
import json, math, os, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import advB_rw as R
import v03_objects as O

SEED = 20260823


def boundary_identity(layer_a, layer_b, segs_b):
    right = set()
    for o in layer_b.objects:
        src = []
        for gi in o["segments"]:
            src.extend(segs_b[gi].get("src") or [gi])
        right.add(frozenset(src))
    hit = sum(1 for o in layer_a.objects if frozenset(o["segments"]) in right)
    return hit / max(1, len(layer_a.objects))


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments:
        return None
    segs0 = G.rw_identity(ex.segments, random.Random(SEED))
    L0 = G.layer_of(segs0, ex.texts)
    fr = ex.frame["clip_display"]
    cx, cy = (fr[0] + fr[2]) / 2, (fr[1] + fr[3]) / 2
    S0 = L0.S

    rewrites = {
        "X1_split_at_0.37": (R._split_at([0.37], max(0.5, 0.05 * S0)), None),
        "X1b_split_at_0.29_0.71": (R._split_at([0.29, 0.71], max(0.5, 0.05 * S0)), None),
        "X2_reverse_vertices": (R.rw_reverse_vertices, None),
        "X3_mirror_x": (R.mirror_x(cx), lambda x, y: (2 * cx - x, y)),
        "X4_rotate_7": (R.rotate_about(cx, cy, 7.0), None),
        "X5_rotate_30": (R.rotate_about(cx, cy, 30.0), None),
        "X6_rotate_45": (R.rotate_about(cx, cy, 45.0), None),
        "X6b_rotate_90": (R.rotate_about(cx, cy, 90.0), None),
        "X7_rect_to_lines": (R.rw_rect_to_lines, None),
        "X8_lines_to_rect": (R.rw_lines_to_rect, None),
    }
    out = {"block_id": rec["block_id"], "doc_id": rec["doc_id"], "version": rec["version"],
           "discipline": rec["discipline"], "cls": rec["cls"], "bucket": rec["bucket"],
           "n_seg": len(ex.segments), "n_text": len(ex.texts),
           "S": round(S0, 4), "scale_source": L0.scale_source,
           "n_obj": len(L0.objects), "rewrites": {}}
    for name, (fn, tfn) in rewrites.items():
        t0 = time.time()
        try:
            segs = fn(ex.segments, random.Random(SEED))
        except Exception as e:
            out["rewrites"][name] = {"error": repr(e)}
            continue
        texts = ex.texts
        deg = None
        if name.startswith("X4") or name.startswith("X5") or name.startswith("X6"):
            deg = float(name.split("_")[-1])
        if deg is not None:
            a = math.radians(deg); ca, sa = math.cos(a), math.sin(a)
            texts = R.map_texts(ex.texts, lambda x, y: (cx + ca * (x - cx) - sa * (y - cy),
                                                        cy + sa * (x - cx) + ca * (y - cy)))
        if name == "X3_mirror_x":
            texts = R.map_texts(ex.texts, lambda x, y: (2 * cx - x, y))
        # bite: how many segments the rewrite actually touched
        if name.startswith("X1"):
            bite = sum(1 for s in ex.segments if s["len"] >= max(0.5, 0.05 * S0))
        elif name == "X2_reverse_vertices":
            bite = len(ex.segments)
        elif name in ("X3_mirror_x",) or deg is not None:
            bite = len(ex.segments)
        elif name == "X7_rect_to_lines":
            bite = sum(1 for s in ex.segments if s.get("op") in ("re", "qu"))
        else:
            bite = sum(1 for a, b in zip(segs, G.rw_identity(ex.segments, None))
                       if a.get("op") != b.get("op"))
        L = G.layer_of(segs, texts)
        rows = G.churn_exact(L0, segs0, L, segs)
        cl = G.classify_churn(rows)
        out["rewrites"][name] = {
            "bite": bite, "n_seg_out": len(segs), "n_obj": len(L.objects),
            "d_obj": len(L.objects) - len(L0.objects),
            "S_out": round(L.S, 4),
            "boundary_identical": round(boundary_identity(L0, L, segs), 5),
            "churn": {k: round(v, 5) for k, v in cl.items()},
            "sec": round(time.time() - t0, 2),
        }
    return out


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    max_seg = int(sys.argv[3]) if len(sys.argv) > 3 else 20000
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 20 <= b["n_seg"] <= max_seg]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"advB/rw_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True, parents=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception as e:
                r = {"block_id": rec["block_id"], "error": repr(e)}
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n"); fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)} {rec['block_id'][:12]} n_seg={rec['n_seg']}", flush=True)


if __name__ == "__main__":
    main()
