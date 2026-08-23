# -*- coding: utf-8 -*-
"""VERIFY F1c: stored crop PNG (what the human actually saw) vs my own render.

fnd's sample was 196 rot0 / 4 rot270.  Here the sample is stratified by /Rotate so the
rotated pages carry statistical weight.
"""
from __future__ import annotations
import json, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np
import fitz
from PIL import Image
from scipy.ndimage import binary_dilation

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"
DARK = 250


def index_stored():
    rows = [json.loads(l) for l in open(ART/"vfy_corpus.jsonl", encoding="utf-8")]
    out = []
    for r in rows:
        if not r["pdf_exists"]: continue
        base = Path(r["result_json"]).parent.parent / "03_analysis/latest/blocks_stage02_100"
        idx = base/"index.json"
        if not idx.exists(): continue
        try:
            data = json.load(open(idx, encoding="utf-8"))
        except Exception:
            continue
        blocks = data.get("blocks") or []
        bym = {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
        for p in r["pages"]:
            if not p["rect"] or not p["w"]: continue
            for bid, co in zip(p["bid"], p["coords"]):
                e = bym.get(bid)
                if not e or co is None: continue
                png = base/str(e.get("file") or "")
                if not png.exists(): continue
                out.append({"pdf": r["pdf"], "doc": r["doc"], "disc": r["disc"], "pi": p["pn"]-1,
                            "bid": bid, "coords": co, "pw": p["w"], "ph": p["h"],
                            "rot": p["pdf_rot"], "png": str(png), "src": e.get("source"),
                            "crop_px": e.get("crop_px")})
    return out


def ink(a):
    g = np.asarray(Image.open(a).convert("L"), dtype=np.uint8)
    return g < DARK


def main(n_per_rot=(("0", 120), ("90", 60), ("270", 60), ("180", 20))):
    pool = index_stored()
    print("blocks with stored PNG:", len(pool))
    byrot = defaultdict(list)
    for b in pool: byrot[str(b["rot"])].append(b)
    print({k: len(v) for k, v in byrot.items()})
    rnd = random.Random(2718)
    sample = []
    for rot, k in n_per_rot:
        v = byrot.get(rot, [])[:]
        rnd.shuffle(v)
        cnt = Counter()
        for c in v:
            if cnt[c["doc"]] >= 3: continue
            sample.append(c); cnt[c["doc"]] += 1
            if sum(cnt.values()) >= k: break
    print("sampled", len(sample))
    rows = []
    for i, b in enumerate(sample):
        o = dict(b)
        try:
            o["crop_px_equals_coords"] = (b["crop_px"] is not None and
                                          all(abs(float(x)-float(y)) <= 0.5
                                              for x, y in zip(b["crop_px"], b["coords"])))
            d = fitz.open(b["pdf"]); pg = d[b["pi"]]
            sx = pg.rect.width/b["pw"]; sy = pg.rect.height/b["ph"]
            c = b["coords"]
            clip = fitz.Rect(c[0]*sx, c[1]*sy, c[2]*sx, c[3]*sy)
            ls = max(clip.width, clip.height)
            if ls < 1: o["err"] = "zero"; rows.append(o); d.close(); continue
            rs = max(0.5, min(8.0, max(100/72.0, 800/ls)))
            pix = pg.get_pixmap(matrix=fitz.Matrix(rs, rs), clip=clip, alpha=False)
            arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            mine = (arr[:, :, :3].mean(axis=2) if pix.n >= 3 else arr[:, :, 0]) < DARK
            stored = ink(b["png"])
            o["size_mine"] = [pix.width, pix.height]; o["size_stored"] = [stored.shape[1], stored.shape[0]]
            H = min(mine.shape[0], stored.shape[0]); W = min(mine.shape[1], stored.shape[1])
            o["size_rel_diff"] = max(abs(mine.shape[0]-stored.shape[0])/max(1, stored.shape[0]),
                                     abs(mine.shape[1]-stored.shape[1])/max(1, stored.shape[1]))
            a = mine[:H, :W]; bm = stored[:H, :W]
            ad = binary_dilation(a, np.ones((3, 3), bool)); bd = binary_dilation(bm, np.ones((3, 3), bool))
            inter = int(((a & bd) | (bm & ad)).sum())/2
            union = int((a | bm).sum())
            o["ink_iou_strict"] = float(inter/union) if union else None
            # exactly fnd's metric: IoU of the two 1-px dilated masks
            u = int((ad | bd).sum())
            o["ink_iou"] = float(int((ad & bd).sum())/u) if u else None
            o["ink_share_mine"] = float(a.mean()); o["ink_share_stored"] = float(bm.mean())
            d.close()
        except Exception as e:
            o["err"] = f"{type(e).__name__}: {e}"
        rows.append(o)
        if i % 30 == 0: print(f"  {i}/{len(sample)}", flush=True)
    ok = [r for r in rows if r.get("ink_iou") is not None]
    import numpy as _np
    summ = {"n_pool": len(pool), "n_sampled": len(rows), "n_ok": len(ok),
            "pool_by_rot": {k: len(v) for k, v in byrot.items()},
            "crop_px_equals_coords_all": all(r.get("crop_px_equals_coords") for r in rows if r.get("crop_px")),
            "crop_px_mismatch": sum(1 for r in rows if r.get("crop_px") and not r["crop_px_equals_coords"])}
    for rot in ("0", "90", "270", "180"):
        sub = [r["ink_iou"] for r in ok if str(r["rot"]) == rot]
        if not sub: continue
        summ[f"rot{rot}"] = {"n": len(sub), "median_iou": float(np.median(sub)),
                            "share_ge_0.60": float(np.mean([x >= .6 for x in sub])),
                            "share_lt_0.30": float(np.mean([x < .3 for x in sub])),
                            "p10": float(np.percentile(sub, 10))}
    allv = [r["ink_iou"] for r in ok]
    strict = [r["ink_iou_strict"] for r in ok if r.get("ink_iou_strict") is not None]
    summ["all"] = {"n": len(allv), "median_iou": float(np.median(allv)),
                   "share_ge_0.60": float(np.mean([x >= .6 for x in allv])),
                   "share_lt_0.30": float(np.mean([x < .3 for x in allv])),
                   "median_iou_strict": float(np.median(strict)),
                   "metric": "fnd metric = IoU of 1-px-dilated masks; strict = dilated-match over plain union"}
    json.dump({"summary": summ, "rows": rows}, open(ART/"vfy_f7_storedcrop.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1, default=str)
    print(json.dumps(summ, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
