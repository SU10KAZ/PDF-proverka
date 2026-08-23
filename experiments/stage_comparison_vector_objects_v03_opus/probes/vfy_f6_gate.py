# -*- coding: utf-8 -*-
"""VERIFY: corpus-wide size of the degenerate-rect path-gate defect.

fitz.Rect.intersects() is False for EMPTY rects (zero width or zero height).  Every
purely horizontal or vertical single-line path has such a rect, so v03_foundation
discards it as "outside_clip".  Measured here: segments and ink length lost per block.
"""
from __future__ import annotations
import json, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np
import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT / "experiments/stage_comparison_vector_objects_v03_opus/probes"))
import vfy_common as C
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"


def pick(seed=555, n=500):
    rows = [json.loads(l) for l in open(ART/"vfy_corpus.jsonl", encoding="utf-8")]
    cand = []
    for r in rows:
        if not r["pdf_exists"]: continue
        for p in r["pages"]:
            if not p["rect"] or not p["n_img"] or not p["w"]: continue
            for bid, co in zip(p["bid"], p["coords"]):
                if co is None: continue
                cand.append({"pdf": r["pdf"], "doc": r["doc"], "disc": r["disc"], "pi": p["pn"]-1,
                             "bid": bid, "coords": co, "pw": p["w"], "ph": p["h"], "rot": p["pdf_rot"]})
    rnd = random.Random(seed); rnd.shuffle(cand)
    out, cnt = [], Counter()
    for c in cand:
        if cnt[c["doc"]] >= 4: continue
        out.append(c); cnt[c["doc"]] += 1
        if len(out) >= n: break
    return out, len(cand)


def main():
    bl, total = pick()
    print("pool", total, "sampled", len(bl))
    rows = []
    t0 = time.time()
    for i, b in enumerate(bl):
        o = dict(b)
        try:
            d = fitz.open(b["pdf"]); pg = d[b["pi"]]
            cd, cp, fwd, derot, sx, sy = C.frame(pg, b["coords"], b["pw"], b["ph"])
            dr = pg.get_drawings()
            ka, _, sa = C.segments(pg, cd, cp, fwd, drawings=dr, path_gate="overlap")
            kb, _, sb = C.segments(pg, cd, cp, fwd, drawings=dr, path_gate="intersects")
            o["n_overlap"] = len(ka); o["n_intersects"] = len(kb)
            o["len_overlap"] = sum(s["len"] for s in ka)
            o["len_intersects"] = sum(s["len"] for s in kb)
            o["empty_rect_in_clip"] = sa["empty_rect_in_clip"]
            d.close()
        except Exception as e:
            o["err"] = f"{type(e).__name__}: {e}"
        rows.append(o)
        if i % 50 == 0: print(f"  {i}/{len(bl)} t={time.time()-t0:.0f}s", flush=True)
    ok = [r for r in rows if "err" not in r]
    nz = [r for r in ok if r["n_overlap"] > 0]
    lost_n = [1-r["n_intersects"]/r["n_overlap"] for r in nz]
    lost_l = [1-r["len_intersects"]/max(1e-9, r["len_overlap"]) for r in nz]
    bydisc = defaultdict(list)
    for r, v in zip(nz, lost_l): bydisc[r["disc"]].append(v)
    summ = {
        "n_sampled": len(rows), "n_ok": len(ok), "n_with_geometry_overlap": len(nz),
        "blocks_empty_under_module_but_not_mine": sum(1 for r in ok if r["n_overlap"] > 0 and r["n_intersects"] == 0),
        "blocks_empty_under_both": sum(1 for r in ok if r["n_overlap"] == 0),
        "lost_segments_share": {"median": float(np.median(lost_n)), "mean": float(np.mean(lost_n)),
                                "p90": float(np.percentile(lost_n, 90)), "max": float(max(lost_n))},
        "lost_ink_length_share": {"median": float(np.median(lost_l)), "mean": float(np.mean(lost_l)),
                                  "p90": float(np.percentile(lost_l, 90)), "max": float(max(lost_l)),
                                  "share_blocks_gt10pct": float(np.mean([x > .1 for x in lost_l])),
                                  "share_blocks_gt50pct": float(np.mean([x > .5 for x in lost_l]))},
        "lost_ink_length_by_discipline": {k: {"n": len(v), "median": float(np.median(v))}
                                          for k, v in sorted(bydisc.items())},
        "errors": dict(Counter(r["err"].split(":")[0] for r in rows if "err" in r)),
    }
    json.dump({"summary": summ, "rows": rows}, open(ART/"vfy_f6_gate.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1, default=str)
    print(json.dumps(summ, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
