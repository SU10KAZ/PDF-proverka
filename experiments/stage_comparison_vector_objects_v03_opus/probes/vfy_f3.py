# -*- coding: utf-8 -*-
"""VERIFY F3: isotropy, re-measured on an independently mined pair set with a
degenerate-safe extractor (the module's path gate drops axis-aligned lines, which are
exactly the segments anisotropic scaling does NOT rotate -> fnd's F3' ran on a biased
subset).  Reports both the fnd-style filtered pairs and ALL pairs.
"""
from __future__ import annotations
import json, math, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import fitz
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import vfy_common as C

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"
MAXSEG = 30000
TOLS = (0.0025, 0.005, 0.01)


def iou(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    ua = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - inter
    return inter/ua if ua > 0 else 0.0


def mine_pairs(seed=246813, n_keep=200):
    rows = [json.loads(l) for l in open(ART / "vfy_corpus.jsonl", encoding="utf-8")]
    g = defaultdict(list)
    for r in rows:
        if r["pdf_exists"]:
            g[(r["obj"], r["disc"], r["doc"])].append(r)
    pairs = []
    for k, vs in g.items():
        if len(vs) < 2: continue
        vs = sorted(vs, key=lambda x: x["ver"])
        A, B = vs[0], vs[-1]
        pa = {p["pn"]: p for p in A["pages"]}
        pb = {p["pn"]: p for p in B["pages"]}
        for pn in set(pa) & set(pb):
            x, y = pa[pn], pb[pn]
            if not (x["rect"] and y["rect"] and x["w"] and y["w"]): continue
            na = [(bid, co, [co[0]/x["w"], co[1]/x["h"], co[2]/x["w"], co[3]/x["h"]])
                  for bid, co in zip(x["bid"], x["coords"]) if co]
            nb = [(bid, co, [co[0]/y["w"], co[1]/y["h"], co[2]/y["w"], co[3]/y["h"]])
                  for bid, co in zip(y["bid"], y["coords"]) if co]
            used = set()
            for bid, co, nn in na:
                best, bj = None, 0.6
                for j, (bid2, co2, nn2) in enumerate(nb):
                    if j in used: continue
                    v = iou(nn, nn2)
                    if v > bj: bj, best = v, j
                if best is None: continue
                used.add(best)
                bid2, co2, nn2 = nb[best]
                wa, ha = co[2]-co[0], co[3]-co[1]
                wb, hb = co2[2]-co2[0], co2[3]-co2[1]
                if wa < 40 or ha < 40 or wb < 40 or hb < 40: continue
                asp_a, asp_b = wa/ha, wb/hb
                d_asp = abs(asp_a-asp_b)/max(asp_a, asp_b)
                pairs.append({"doc": k[2], "disc": k[1], "pn": pn, "iou": bj,
                              "d_aspect": d_asp,
                              "A": {"pdf": A["pdf"], "pi": pn-1, "bid": bid, "coords": co,
                                    "pw": x["w"], "ph": x["h"]},
                              "B": {"pdf": B["pdf"], "pi": pn-1, "bid": bid2, "coords": co2,
                                    "pw": y["w"], "ph": y["h"]}})
    rnd = random.Random(seed)
    rnd.shuffle(pairs)
    cnt = Counter(); out = []
    for p in pairs:
        if cnt[p["doc"]] >= 3: continue
        out.append(p); cnt[p["doc"]] += 1
        if len(out) >= n_keep: break
    return out, len(pairs)


def get_segs(spec, gate="overlap"):
    d = fitz.open(spec["pdf"]); pg = d[spec["pi"]]
    cd, cp, fwd, derot, sx, sy = C.frame(pg, spec["coords"], spec["pw"], spec["ph"])
    kept, dropped, st = C.segments(pg, cd, cp, fwd, path_gate=gate)
    d.close()
    return kept, cd


def norm(segs, cd, mode):
    w = max(cd.width, 1e-9); h = max(cd.height, 1e-9)
    if mode == "isotropic": fx = fy = 1.0/max(w, h)
    elif mode == "anisotropic": fx, fy = 1.0/w, 1.0/h
    else: fx = fy = 1.0
    out = []
    for s in segs:
        x0 = (s["p0"][0]-cd.x0)*fx; y0 = (s["p0"][1]-cd.y0)*fy
        x1 = (s["p1"][0]-cd.x0)*fx; y1 = (s["p1"][1]-cd.y0)*fy
        L = math.hypot(x1-x0, y1-y0)
        if L <= 0: continue
        out.append(((x0+x1)/2, (y0+y1)/2, L, math.atan2(y1-y0, x1-x0) % math.pi))
    return out


def match_rate(A, B, tol):
    if not A or not B: return None
    Ba = np.array([[b[0], b[1]] for b in B])
    Bl = np.array([b[2] for b in B]); Bd = np.array([b[3] for b in B])
    from scipy.spatial import cKDTree
    t = cKDTree(Ba)
    hit = 0
    for cx, cy, L, d in A:
        idx = t.query_ball_point((cx, cy), tol)
        for j in idx:
            if abs(Bl[j]-L) <= max(tol, 0.1*L):
                dd = abs(Bd[j]-d)
                dd = min(dd, math.pi-dd)
                if dd <= math.radians(5):
                    hit += 1; break
    return hit/len(A)


def run(p, gate="overlap"):
    o = {"doc": p["doc"], "disc": p["disc"], "pn": p["pn"], "d_aspect": p["d_aspect"],
         "iou": p["iou"], "gate": gate}
    try:
        sa, ca = get_segs(p["A"], gate); sb, cb = get_segs(p["B"], gate)
        o["nA"], o["nB"] = len(sa), len(sb)
        if not (20 <= len(sa) <= MAXSEG) or not (20 <= len(sb) <= MAXSEG):
            o["err"] = "size"; return o
        wa, ha = ca.width, ca.height
        # analytic angle error of a 45-degree segment under x/w,y/h into B's frame
        k = (cb.width/ca.width) / (cb.height/ca.height)
        o["angle_err_45_deg"] = abs(45.0 - math.degrees(math.atan2(1.0, k)))
        o["share_axis_aligned_A"] = float(np.mean([
            min(abs(math.degrees(math.atan2(s["p1"][1]-s["p0"][1], s["p1"][0]-s["p0"][0])) % 180 - a)
                for a in (0, 90, 180)) < 1.0 for s in sa]))
        res = {}
        for mode in ("isotropic", "anisotropic", "points"):
            na = norm(sa, ca, mode); nb = norm(sb, cb, mode)
            scale = max(wa, ha) if mode == "points" else 1.0
            res[mode] = {str(t): match_rate(na, nb, t*scale) for t in TOLS}
        o["match"] = res
    except Exception as e:
        o["err"] = f"{type(e).__name__}: {e}"
    return o


def main():
    pairs, total = mine_pairs()
    print("mined", total, "kept", len(pairs))
    rows = []
    t0 = time.time()
    for i, p in enumerate(pairs):
        rows.append(run(p, "overlap"))
        if i % 20 == 0: print(f"  {i}/{len(pairs)} t={time.time()-t0:.0f}s", flush=True)
    # module-gate control on the same pairs
    rows_gate = []
    for i, p in enumerate(pairs):
        rows_gate.append(run(p, "intersects"))
        if i % 40 == 0: print(f"  gate {i}/{len(pairs)}", flush=True)

    def summarize(rr, filt=None):
        ok = [r for r in rr if "err" not in r and r.get("match")]
        if filt: ok = [r for r in ok if filt(r)]
        s = {"n": len(ok)}
        if not ok: return s
        s["d_aspect_median"] = float(np.median([r["d_aspect"] for r in ok]))
        s["d_aspect_p90"] = float(np.percentile([r["d_aspect"] for r in ok], 90))
        ae = [r["angle_err_45_deg"] for r in ok]
        s["angle_err_45"] = {"median": float(np.median(ae)), "p90": float(np.percentile(ae, 90)),
                             "max": float(max(ae))}
        s["share_axis_aligned_A_median"] = float(np.median([r["share_axis_aligned_A"] for r in ok]))
        for mode in ("isotropic", "anisotropic", "points"):
            s[mode] = {t: float(np.mean([r["match"][mode][t] for r in ok if r["match"][mode][t] is not None]))
                       for t in map(str, TOLS)}
        return s

    out = {
        "n_pairs_mined": total,
        "all_pairs": summarize(rows),
        "fnd_style_filtered_d_aspect_ge_2pct": summarize(rows, lambda r: r["d_aspect"] >= 0.02),
        "module_gate_all_pairs": summarize(rows_gate),
        "module_gate_filtered": summarize(rows_gate, lambda r: r["d_aspect"] >= 0.02),
        "tols": TOLS,
    }
    json.dump({"summary": out, "rows": rows, "rows_module_gate": rows_gate},
              open(ART / "vfy_f3.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=str)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
