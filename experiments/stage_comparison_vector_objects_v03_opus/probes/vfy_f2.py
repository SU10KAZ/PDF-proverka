# -*- coding: utf-8 -*-
"""VERIFY F2: does the ink filter eat VISIBLE geometry?

Raster arbitration per block (independent extraction, degenerate-safe path gate):
  ink            = dark pixels of the render
  mask_kept      = pixels drawn by segments that survive the ink filter
  mask_dropped   = pixels drawn by segments the ink filter removes
  ONLY-DROPPED   = ink & ~dilate(mask_kept) & dilate(mask_dropped)
                   -> visible ink that ONLY the removed paths explain.
That share, relative to all ink of the block, is the loss the filter causes.
"""
from __future__ import annotations
import json, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import fitz
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import vfy_common as C

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"
DPI = 200
DARK = 250


def pool(seed=13579, n_random=140):
    rows = [json.loads(l) for l in open(ART / "vfy_corpus.jsonl", encoding="utf-8")]
    cand = []
    for r in rows:
        if not r["pdf_exists"]:
            continue
        for p in r["pages"]:
            if not p["rect"] or not p["n_img"] or not p["w"]:
                continue
            for bid, co in zip(p["bid"], p["coords"]):
                if co is None or co[2] - co[0] < 60 or co[3] - co[1] < 60:
                    continue
                cand.append({"pdf": r["pdf"], "doc": r["doc"], "disc": r["disc"], "ver": r["ver"],
                             "pi": p["pn"] - 1, "bid": bid, "coords": co,
                             "pw": p["w"], "ph": p["h"], "rot": p["pdf_rot"]})
    rnd = random.Random(seed)
    rnd.shuffle(cand)
    out, cnt = [], Counter()
    for c in cand:
        if cnt[c["doc"]] >= 2:
            continue
        out.append(dict(c, strat="random"))
        cnt[c["doc"]] += 1
        if len(out) >= n_random:
            break
    return out


def run(b):
    o = dict(b)
    try:
        doc = fitz.open(b["pdf"]); page = doc[b["pi"]]
        cd, cp, fwd, derot, sx, sy = C.frame(page, b["coords"], b["pw"], b["ph"])
        if cd.width < 1 or cd.height < 1:
            o["err"] = "degenerate"; doc.close(); return o
        if (cd.width * DPI / 72) * (cd.height * DPI / 72) > 45e6:
            o["err"] = "too_big"; doc.close(); return o
        dr = page.get_drawings()
        kept, dropped, st = C.segments(page, cd, cp, fwd, drawings=dr, path_gate="overlap",
                                       drop_invisible=True)
        o["n_kept"] = len(kept); o["n_dropped"] = len(dropped)
        o["drop_share_segments"] = len(dropped) / max(1, len(kept) + len(dropped))
        o["rules"] = dict(Counter(s["rule"] for s in dropped))
        if not kept and not dropped:
            o["err"] = "no_geometry"; doc.close(); return o
        gray, s, pix = C.render(page, cd, dpi=DPI)
        ink = gray < DARK
        n_ink = int(ink.sum())
        o["n_ink_px"] = n_ink
        o["ink_share_of_crop"] = float(ink.mean())
        if n_ink == 0:
            o["err"] = "blank"; doc.close(); return o
        mk = C.dilate(C.seg_mask(kept, cd, s, ink.shape), 2)
        md = C.dilate(C.seg_mask(dropped, cd, s, ink.shape), 2)
        only_dropped = ink & ~mk & md
        o["ink_only_dropped_share"] = float(only_dropped.sum() / n_ink)
        o["ink_covered_kept_share"] = float((ink & mk).sum() / n_ink)
        o["ink_covered_dropped_share"] = float((ink & md).sum() / n_ink)
        o["dropped_paints_share"] = float((ink & md).sum() / max(1, int(md.sum())))
        o["kept_paints_share"] = float((ink & mk).sum() / max(1, int(mk.sum())))
        # per-rule breakdown of only-dropped ink
        byrule = {}
        for rule in set(s["rule"] for s in dropped):
            sub = [s for s in dropped if s["rule"] == rule]
            m = C.dilate(C.seg_mask(sub, cd, s, ink.shape), 2)
            byrule[rule] = float((ink & ~mk & m).sum() / n_ink)
        o["only_dropped_by_rule"] = byrule
        doc.close()
    except Exception as e:
        o["err"] = f"{type(e).__name__}: {e}"
    return o


def main():
    bl = pool()
    print("sampled", len(bl))
    rows = []
    t0 = time.time()
    for i, b in enumerate(bl):
        rows.append(run(b))
        if i % 10 == 0:
            print(f"  {i}/{len(bl)} t={time.time()-t0:.0f}s", flush=True)
    ok = [r for r in rows if "err" not in r]
    withdrop = [r for r in ok if r["n_dropped"] > 0]
    def q(v, p):
        return float(np.percentile(v, p)) if v else None
    od = [r["ink_only_dropped_share"] for r in withdrop]
    ds = [r["drop_share_segments"] for r in ok]
    rule_tot = Counter()
    for r in ok:
        for k, v in (r.get("rules") or {}).items():
            rule_tot[k] += v
    summ = {
        "n_sampled": len(rows), "n_ok": len(ok), "n_with_dropped": len(withdrop),
        "drop_share_segments": {"median": q(ds, 50), "mean": float(np.mean(ds)) if ds else None,
                                "p90": q(ds, 90), "max": max(ds) if ds else None,
                                "share_blocks_gt20pct": float(np.mean([x > .2 for x in ds])) if ds else None},
        "ink_only_dropped_share": {"median": q(od, 50), "mean": float(np.mean(od)) if od else None,
                                   "p75": q(od, 75), "p90": q(od, 90), "p99": q(od, 99),
                                   "max": max(od) if od else None,
                                   "share_blocks_gt1pct": float(np.mean([x > .01 for x in od])) if od else None,
                                   "share_blocks_gt5pct": float(np.mean([x > .05 for x in od])) if od else None},
        "rules_segments": dict(rule_tot),
        "worst": sorted([{k: r[k] for k in ("bid", "disc", "doc", "ink_only_dropped_share",
                                            "drop_share_segments", "n_kept", "n_dropped")}
                         for r in withdrop], key=lambda x: -x["ink_only_dropped_share"])[:15],
        "errors": dict(Counter(r["err"].split(":")[0] for r in rows if "err" in r)),
        "dpi": DPI,
    }
    json.dump({"summary": summ, "rows": rows}, open(ART / "vfy_f2.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1, default=str)
    print(json.dumps(summ, ensure_ascii=False, indent=1)[:3000])


if __name__ == "__main__":
    main()
