# -*- coding: utf-8 -*-
"""VERIFY: is render_block() really equivalent to production crop_from_pdf()?

Calls the REAL production function from backend, the module's render_block, and my own
independent implementation, and compares PNG sha256.  Also probes the default-argument
path, which production and the module do NOT share.
"""
from __future__ import annotations
import hashlib, json, random, sys, tempfile, time
from collections import Counter, defaultdict
from pathlib import Path

import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "experiments/stage_comparison_vector_objects_v03_opus/probes"))
ART = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts"
TMP = ART / "vfy_tmp"
TMP.mkdir(parents=True, exist_ok=True)

from backend.app.pipeline.stages.crop_blocks.blocks import crop_from_pdf  # production
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def mine_render(pdf, pi, coords, pw, ph, out, dpi=0, target_px=0, min_long_side=0):
    d = fitz.open(pdf); pg = d[pi]
    sx = pg.rect.width/pw; sy = pg.rect.height/ph
    x1, y1, x2, y2 = coords
    clip = fitz.Rect(x1*sx, y1*sy, x2*sx, y2*sy)
    ls = max(clip.width, clip.height)
    if dpi > 0:
        rs = dpi/72.0
        if min_long_side > 0: rs = max(rs, min_long_side/ls)
    else:
        rs = (target_px or 1500)/ls
    rs = max(0.5, min(8.0, rs))
    pix = pg.get_pixmap(matrix=fitz.Matrix(rs, rs), clip=clip, alpha=False)
    pix.save(str(out)); d.close()
    return pix.width, pix.height


def pick(seed=31337, per_rot=25):
    rows = [json.loads(l) for l in open(ART/"vfy_corpus.jsonl", encoding="utf-8")]
    pool = defaultdict(list)
    for r in rows:
        if not r["pdf_exists"]: continue
        for p in r["pages"]:
            if not p["rect"] or not p["n_img"] or not p["w"]: continue
            for bid, co in zip(p["bid"], p["coords"]):
                if co is None: continue
                pool[str(p["pdf_rot"])].append({"pdf": r["pdf"], "pi": p["pn"]-1, "bid": bid,
                                                "coords": co, "pw": p["w"], "ph": p["h"],
                                                "rot": p["pdf_rot"], "doc": r["doc"]})
    rnd = random.Random(seed); out = []
    for rot, v in pool.items():
        rnd.shuffle(v)
        cnt = Counter()
        for c in v:
            if cnt[c["doc"]] >= 2: continue
            out.append(c); cnt[c["doc"]] += 1
            if sum(cnt.values()) >= per_rot: break
    return out


def main():
    blocks = pick()
    print("sampled", len(blocks), Counter(b["rot"] for b in blocks))
    rows = []
    for i, b in enumerate(blocks):
        o = {k: b[k] for k in ("bid", "rot", "doc", "pi", "coords")}
        for tag, kw in (("prod_profile", dict(dpi=100, min_long_side=800)),
                        ("defaults", dict(dpi=0, min_long_side=0))):
            pa = TMP/f"a_{i}.png"; pb = TMP/f"b_{i}.png"; pc = TMP/f"c_{i}.png"
            try:
                wa, ha = crop_from_pdf(Path(b["pdf"]), b["pi"]+1, list(b["coords"]),
                                       b["pw"], b["ph"], pa, **kw)
                pix = F.render_block(b["pdf"], b["pi"], b["coords"], b["pw"], b["ph"],
                                     out_png=pb, **kw)
                wc, hc = mine_render(b["pdf"], b["pi"], b["coords"], b["pw"], b["ph"], pc, **kw)
                o[tag] = {"prod_size": [wa, ha], "mod_size": [pix.width, pix.height],
                          "mine_size": [wc, hc],
                          "sha_prod_vs_module": sha(pa) == sha(pb),
                          "sha_prod_vs_mine": sha(pa) == sha(pc)}
            except Exception as e:
                o[tag] = {"err": f"{type(e).__name__}: {e}"}
            finally:
                F.clear_caches()
                for p in (pa, pb, pc):
                    if p.exists(): p.unlink()
        rows.append(o)
        if i % 20 == 0: print(f"  {i}/{len(blocks)}", flush=True)
    summ = {}
    for tag in ("prod_profile", "defaults"):
        ok = [r for r in rows if "err" not in r[tag]]
        summ[tag] = {
            "n": len(ok),
            "n_err": len(rows)-len(ok),
            "module_equals_production": sum(r[tag]["sha_prod_vs_module"] for r in ok),
            "mine_equals_production": sum(r[tag]["sha_prod_vs_mine"] for r in ok),
            "size_mismatch_module": sum(r[tag]["prod_size"] != r[tag]["mod_size"] for r in ok),
        }
    json.dump({"summary": summ, "rows": rows}, open(ART/"vfy_f4_render.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1, default=str)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
