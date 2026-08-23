# -*- coding: utf-8 -*-
"""R4 — FALSE RELATION RATE, measured the only honest way: by looking.

For every type, >=30 relations are sampled across blocks/disciplines and rendered as
a tile: the whole local neighbourhood in grey, object A in red, object B in blue, the
evidence point in green.  The tiles are laid out 3x3 on contact sheets which a human
(the probe author) grades one by one.  The grades go into rel_r4_grades.json by hand;
this script only samples and draws, so the sampling stays reproducible and the grading
stays separable from it.

Usage:  rel_r4_visual.py sample      -> artifacts/rel_r4_sample.json + rel_visual/*.png
"""
from __future__ import annotations
import json, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
from PIL import Image, ImageDraw

SEED = 20260823
PER_TYPE = 30
TILE = 500
GRID = 3
OUT = C.ART / "rel_visual"


def tile_for(ex, layer, rel, size=TILE):
    objs = layer.objects
    a = rel["a"]
    bb = list(objs[a]["bbox"])
    tb = None
    if rel["type"] == "LABEL_ANCHOR":
        t = ex.texts[rel["text_ix"]]
        tb = list(t["bbox"])
    else:
        tb = list(objs[rel["b"]]["bbox"])
    u = [min(bb[0], tb[0]), min(bb[1], tb[1]), max(bb[2], tb[2]), max(bb[3], tb[3])]
    w, h = max(u[2] - u[0], 1e-3), max(u[3] - u[1], 1e-3)
    pad = max(0.35 * max(w, h), 2.0 * (layer.S or 1.0), 3.0)
    fr = [u[0] - pad, u[1] - pad, u[2] + pad, u[3] + pad]
    fw, fh = fr[2] - fr[0], fr[3] - fr[1]
    k = size / max(fw, fh)
    ox = (size - fw * k) / 2
    oy = (size - fh * k) / 2
    img = Image.new("RGB", (size, size), "white")
    dr = ImageDraw.Draw(img)

    def P(p):
        return (ox + (p[0] - fr[0]) * k, oy + (p[1] - fr[1]) * k)

    ain = set(objs[a]["segments"])
    bin_ = set(objs[rel["b"]]["segments"]) if rel["type"] != "LABEL_ANCHOR" else set()
    for s in ex.segments:
        p0, p1 = s["p0"], s["p1"]
        if max(p0[0], p1[0]) < fr[0] or min(p0[0], p1[0]) > fr[2]:
            continue
        if max(p0[1], p1[1]) < fr[1] or min(p0[1], p1[1]) > fr[3]:
            continue
        i = s["i"]
        if i in ain:
            col, wdt = (220, 30, 30), 3
        elif i in bin_:
            col, wdt = (20, 70, 220), 3
        else:
            col, wdt = (185, 185, 185), 1
        dr.line([P(p0), P(p1)], fill=col, width=wdt)
    for t in ex.texts:
        b = t["bbox"]
        if b[2] < fr[0] or b[0] > fr[2] or b[3] < fr[1] or b[1] > fr[3]:
            continue
        is_anchor = (rel["type"] == "LABEL_ANCHOR" and t is ex.texts[rel["text_ix"]]) or \
                    (rel["type"] == "LEADER_TO" and (t.get("text") or "").strip() ==
                     (rel.get("text") or "").strip() and rel.get("text"))
        col = (20, 70, 220) if is_anchor else (120, 190, 120)
        dr.rectangle([P((b[0], b[1])), P((b[2], b[3]))], outline=col, width=2 if is_anchor else 1)
        if is_anchor:
            dr.text((P((b[0], b[1]))[0], max(0, P((b[0], b[1]))[1] - 12)),
                    (t.get("text") or "")[:24], fill=(20, 70, 220))
    if rel.get("at"):
        x, y = P(rel["at"])
        dr.ellipse([x - 6, y - 6, x + 6, y + 6], outline=(0, 170, 0), width=3)
    return img


def sheet(tiles, labels, path):
    n = len(tiles)
    cols = GRID
    rows = (n + cols - 1) // cols
    hdr = 18
    img = Image.new("RGB", (cols * TILE, rows * (TILE + hdr)), "white")
    dr = ImageDraw.Draw(img)
    for i, (t, lab) in enumerate(zip(tiles, labels)):
        cx, cy = (i % cols) * TILE, (i // cols) * (TILE + hdr)
        dr.text((cx + 4, cy + 3), lab, fill=(0, 0, 0))
        img.paste(t, (cx, cy + hdr))
        dr.rectangle([cx, cy, cx + TILE - 1, cy + TILE + hdr - 1], outline=(0, 0, 0))
    img.save(path)


def main():
    OUT.mkdir(exist_ok=True)
    rng = random.Random(SEED)
    smp = json.load(open(C.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    pool = [r for r in smp if 60 <= r["n_seg"] <= 25000]
    rng.shuffle(pool)
    by_disc: dict = {}
    for r in pool:
        by_disc.setdefault(r["discipline"], []).append(r)
    order = []
    while any(by_disc.values()):
        for d in sorted(by_disc):
            if by_disc[d]:
                order.append(by_disc[d].pop())
    want = {t: PER_TYPE for t in R.REL_TYPES}
    picked: dict = {t: [] for t in R.REL_TYPES}
    used_blocks = 0
    for rec in order:
        if all(len(picked[t]) >= want[t] for t in R.REL_TYPES):
            break
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            continue
        try:
            ex = C.G.extract(pb)
        except Exception:
            continue
        if not ex.segments:
            continue
        L = C.O.build_objects(ex)
        rels = R.build_relations(L, ex)
        used_blocks += 1
        byt: dict = {}
        for j, r in enumerate(rels):
            byt.setdefault(r["type"], []).append(j)
        for t in R.REL_TYPES:
            need = want[t] - len(picked[t])
            if need <= 0 or t not in byt:
                continue
            # at most 3 per block per type, so one drawing cannot define the rate
            take = min(3, need, len(byt[t]))
            for j in rng.sample(byt[t], take):
                r = rels[j]
                try:
                    img = tile_for(ex, L, r)
                except Exception:
                    continue
                idx = len(picked[t])
                name = f"{t}_{idx:02d}"
                picked[t].append({
                    "id": name, "type": t, "block": rec["block_id"],
                    "doc": rec["doc_id"], "version": rec["version"],
                    "discipline": rec["discipline"], "cls": rec["cls"],
                    "n_seg": rec["n_seg"], "S": round(L.S, 3),
                    "a_cls": L.objects[r["a"]]["cls"],
                    "b_cls": (L.objects[r["b"]]["cls"] if r["type"] != "LABEL_ANCHOR" else "TEXT"),
                    "a_nseg": L.objects[r["a"]]["n_seg"],
                    "b_nseg": (L.objects[r["b"]]["n_seg"] if r["type"] != "LABEL_ANCHOR" else None),
                    "detail": {k: v for k, v in r.items()
                               if k in ("d_pt", "gap_pt", "tol_pt", "delta_pt", "axis",
                                        "text", "shelf", "resolved", "mode", "group_size")},
                    "_img": img,
                })
        C.F.clear_caches()
    meta = {}
    for t in R.REL_TYPES:
        rows = picked[t]
        meta[t] = [{k: v for k, v in r.items() if k != "_img"} for r in rows]
        for s in range(0, len(rows), GRID * GRID):
            chunk = rows[s:s + GRID * GRID]
            sheet([c["_img"] for c in chunk],
                  [f"{c['id']} {c['discipline']}/{c['cls']} {c['a_cls']}->{c['b_cls']} "
                   f"{json.dumps(c['detail'], ensure_ascii=False)[:60]}" for c in chunk],
                  OUT / f"{t}_sheet{s // (GRID * GRID)}.png")
    json.dump({"seed": SEED, "blocks_used": used_blocks,
               "n": {t: len(picked[t]) for t in R.REL_TYPES}, "items": meta},
              open(C.ART / "rel_r4_sample.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({t: len(picked[t]) for t in R.REL_TYPES}), "blocks", used_blocks)


if __name__ == "__main__":
    main()
