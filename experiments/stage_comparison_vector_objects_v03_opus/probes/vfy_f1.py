# -*- coding: utf-8 -*-
"""VERIFY F1: coordinate system / /Rotate / CropBox, with RASTER GROUND TRUTH.

Two metrics per block:
  precision  = share of extracted segments lying on dark pixels of the render
  recall     = share of dark pixels of the render NOT explained by any extracted
               segment, text box or raster image (the metric fnd never measured)

Three extraction variants: correct+overlap gate (mine), correct+fitz.intersects gate
(module replica), naive (no derotation).
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
DPI = 150
DARK = 250


def pick(n_per_rot=(("0", 40), ("90", 25), ("270", 25), ("180", 99)), n_cropbox=30, seed=90210):
    rows = [json.loads(l) for l in open(ART / "vfy_corpus.jsonl", encoding="utf-8")]
    pool = defaultdict(list)
    cbpool = []
    for r in rows:
        if not r["pdf_exists"]:
            continue
        for p in r["pages"]:
            if not p["rect"] or not p["n_img"] or not p["w"] or not p["h"]:
                continue
            cb = p["cb"]
            cb_off = abs(cb[0]) > 1e-6 or abs(cb[1]) > 1e-6
            for bid, co, sh in zip(p["bid"], p["coords"], p["shape"]):
                if co is None:
                    continue
                if (co[2] - co[0]) < 20 or (co[3] - co[1]) < 20:
                    continue
                rec = {"pdf": r["pdf"], "doc": r["doc"], "ver": r["ver"], "disc": r["disc"],
                       "pi": p["pn"] - 1, "bid": bid, "coords": co, "pw": p["w"], "ph": p["h"],
                       "rot": p["pdf_rot"], "shape": sh, "cb_off": cb_off,
                       "cb": cb, "mb": p["mb"], "rect": p["rect"]}
                pool[str(p["pdf_rot"])].append(rec)
                if cb_off:
                    cbpool.append(rec)
    rnd = random.Random(seed)
    out, seen = [], Counter()
    for rot, k in n_per_rot:
        cand = pool.get(rot, [])
        rnd.shuffle(cand)
        cnt = Counter()
        for c in cand:
            if cnt[c["doc"]] >= 2:
                continue
            out.append(dict(c, strat=f"rot{rot}"))
            cnt[c["doc"]] += 1
            if len(cnt) and sum(cnt.values()) >= k:
                break
    rnd.shuffle(cbpool)
    cnt = Counter()
    for c in cbpool:
        if cnt[c["doc"]] >= 2:
            continue
        out.append(dict(c, strat="cropbox_offset"))
        cnt[c["doc"]] += 1
        if sum(cnt.values()) >= n_cropbox:
            break
    return out


def on_ink(segs, clip_d, s, ink_d, shape):
    hits = tot = 0
    H, W = shape
    for sg in segs:
        for t in (0.0, 0.25, 0.5, 0.75, 1.0):
            x = (sg["p0"][0] + t * (sg["p1"][0] - sg["p0"][0]) - clip_d.x0) * s
            y = (sg["p0"][1] + t * (sg["p1"][1] - sg["p0"][1]) - clip_d.y0) * s
            xi, yi = int(x), int(y)
            if 0 <= yi < H and 0 <= xi < W and ink_d[yi, xi]:
                hits += 1
                break
        tot += 1
    return (hits / tot) if tot else None


def run_block(b):
    out = dict(b)
    try:
        doc = fitz.open(b["pdf"])
        page = doc[b["pi"]]
        clip_d, clip_p, fwd, derot, sx, sy = C.frame(page, b["coords"], b["pw"], b["ph"])
        out["clip_display"] = [clip_d.x0, clip_d.y0, clip_d.x1, clip_d.y1]
        out["clip_page"] = [clip_p.x0, clip_p.y0, clip_p.x1, clip_p.y1]
        out["scale"] = [sx, sy]
        out["aniso_scale_ratio"] = sx / sy if sy else None
        if clip_d.width < 1 or clip_d.height < 1:
            out["err"] = "degenerate_clip"; doc.close(); return out
        area_px = (clip_d.width * DPI / 72) * (clip_d.height * DPI / 72)
        if area_px > 40e6:
            out["err"] = "too_big"; doc.close(); return out
        gray, s, pix = C.render(page, clip_d, dpi=DPI)
        ink = gray < DARK
        out["render_size"] = [pix.width, pix.height]
        out["ink_share"] = float(ink.mean())
        dr = page.get_drawings()
        ink_d = C.dilate(ink, 1)
        res = {}
        for name, kw in (("mine", dict(path_gate="overlap")),
                         ("module_gate", dict(path_gate="intersects")),
                         ("naive", dict(path_gate="overlap", naive=True))):
            kept, dropped, st = C.segments(page, clip_d, clip_p, fwd, drawings=dr, **kw)
            r = {"n_seg": len(kept), "n_dropped": len(dropped),
                 "ink_len": sum(x["len"] for x in kept), "stats": st}
            r["prec_on_ink"] = on_ink(kept, clip_d, s, ink_d, ink.shape) if kept else None
            res[name] = r
            if name in ("mine", "module_gate"):
                m = C.seg_mask(kept, clip_d, s, ink.shape)
                res[name]["mask"] = m
        # recall: dark pixels not explained
        txt = page.get_text("dict", clip=clip_p)
        expl_extra = np.zeros(ink.shape, dtype=bool)
        for blk in txt.get("blocks") or []:
            for ln in blk.get("lines") or []:
                for sp in ln.get("spans") or []:
                    bb = fitz.Rect(sp["bbox"]) * fwd
                    bb.normalize()
                    x0 = int(max(0, (bb.x0 - clip_d.x0) * s)); x1 = int(min(ink.shape[1], (bb.x1 - clip_d.x0) * s) + 1)
                    y0 = int(max(0, (bb.y0 - clip_d.y0) * s)); y1 = int(min(ink.shape[0], (bb.y1 - clip_d.y0) * s) + 1)
                    if x1 > x0 and y1 > y0:
                        expl_extra[y0:y1, x0:x1] = True
        try:
            infos = page.get_image_info(hashes=False, xrefs=True)
        except Exception:
            infos = []
        for inf in infos or []:
            bb = fitz.Rect(inf.get("bbox")) * fwd
            bb.normalize()
            x0 = int(max(0, (bb.x0 - clip_d.x0) * s)); x1 = int(min(ink.shape[1], (bb.x1 - clip_d.x0) * s) + 1)
            y0 = int(max(0, (bb.y0 - clip_d.y0) * s)); y1 = int(min(ink.shape[0], (bb.y1 - clip_d.y0) * s) + 1)
            if x1 > x0 and y1 > y0:
                expl_extra[y0:y1, x0:x1] = True
        out["text_img_cover"] = float(expl_extra.mean())
        n_ink = int(ink.sum())
        out["n_ink_px"] = n_ink
        for name in ("mine", "module_gate"):
            m = res[name].pop("mask")
            md = C.dilate(m, 2)
            unexp = ink & ~md & ~expl_extra
            res[name]["unexplained_ink"] = float(unexp.sum() / n_ink) if n_ink else None
            unexp2 = ink & ~md
            res[name]["unexplained_ink_geom_only"] = float(unexp2.sum() / n_ink) if n_ink else None
        out["res"] = res
        doc.close()
    except Exception as e:
        out["err"] = f"{type(e).__name__}: {e}"
    return out


def main():
    blocks = pick()
    print("sampled", len(blocks), Counter(b["strat"] for b in blocks))
    rows = []
    t0 = time.time()
    for i, b in enumerate(blocks):
        rows.append(run_block(b))
        if i % 10 == 0:
            print(f"  {i}/{len(blocks)} t={time.time()-t0:.0f}s", flush=True)
    ok = [r for r in rows if "err" not in r]
    summ = {}
    for strat in sorted({r["strat"] for r in rows}):
        sub = [r for r in ok if r["strat"] == strat and r["res"]["mine"]["n_seg"] > 0]
        if not sub:
            summ[strat] = {"n": 0}; continue
        def med(f):
            v = [f(r) for r in sub if f(r) is not None]
            return float(np.median(v)) if v else None
        summ[strat] = {
            "n_total": len([r for r in rows if r["strat"] == strat]),
            "n_with_geom": len(sub),
            "prec_mine": med(lambda r: r["res"]["mine"]["prec_on_ink"]),
            "prec_naive": med(lambda r: r["res"]["naive"]["prec_on_ink"]),
            "unexplained_mine": med(lambda r: r["res"]["mine"]["unexplained_ink"]),
            "unexplained_module_gate": med(lambda r: r["res"]["module_gate"]["unexplained_ink"]),
            "seg_mine_median": med(lambda r: r["res"]["mine"]["n_seg"]),
            "seg_module_gate_median": med(lambda r: r["res"]["module_gate"]["n_seg"]),
            "seg_ratio_module_over_mine": med(lambda r: r["res"]["module_gate"]["n_seg"] / max(1, r["res"]["mine"]["n_seg"])),
            "inklen_ratio_module_over_mine": med(lambda r: r["res"]["module_gate"]["ink_len"] / max(1e-9, r["res"]["mine"]["ink_len"])),
            "empty_rect_paths_in_clip_median": med(lambda r: r["res"]["mine"]["stats"]["empty_rect_in_clip"]),
        }
    json.dump({"summary": summ, "dpi": DPI, "rows": rows}, open(ART / "vfy_f1.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1, default=str)
    print(json.dumps(summ, ensure_ascii=False, indent=1))
    print("errors", Counter(r.get("err", "").split(":")[0] for r in rows if "err" in r))


if __name__ == "__main__":
    main()
