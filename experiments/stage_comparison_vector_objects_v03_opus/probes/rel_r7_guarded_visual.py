# -*- coding: utf-8 -*-
"""R7 — the decisive control for the rectangle->circle defect: the SAME visual audit,
with the guard (``arc_min_pts=6``) on.

R4 graded the relations of the layer as it stands.  R6 showed that 76.9 % of INSIDE /
CONTAINS edges disappear when rectangles stop being turned into their circumscribed
circles.  If the defect is really what my eye was seeing, the false rate of INSIDE must
collapse on the guarded layer, on the same blocks and the same sampling rule.

Usage: rel_r7_guarded_visual.py
"""
from __future__ import annotations
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
import rel_r4_visual as V

GUARD = {"arc_min_pts": 6}
TYPES = ["INSIDE", "CONTAINS", "PART_OF", "ADJACENT"]
PER_TYPE = 18
SEED = 20260823
OUT = C.ART / "rel_visual_guarded"


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
    picked: dict = {t: [] for t in TYPES}
    for rec in order:
        if all(len(picked[t]) >= PER_TYPE for t in TYPES):
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
        L = C.O.build_objects(ex, **GUARD)
        rels = R.build_relations(L, ex)
        byt: dict = {}
        for j, r in enumerate(rels):
            byt.setdefault(r["type"], []).append(j)
        for t in TYPES:
            need = PER_TYPE - len(picked[t])
            if need <= 0 or t not in byt:
                continue
            for j in rng.sample(byt[t], min(3, need, len(byt[t]))):
                r = rels[j]
                try:
                    img = V.tile_for(ex, L, r)
                except Exception:
                    continue
                picked[t].append({
                    "id": f"{t}_{len(picked[t]):02d}", "type": t, "block": rec["block_id"],
                    "doc": rec["doc_id"], "version": rec["version"],
                    "discipline": rec["discipline"], "cls": rec["cls"],
                    "a_cls": L.objects[r["a"]]["cls"],
                    "b_cls": (L.objects[r["b"]]["cls"] if t != "LABEL_ANCHOR" else "TEXT"),
                    "detail": {k: v for k, v in r.items()
                               if k in ("gap_pt", "tol_pt", "mode")},
                    "_img": img})
        C.F.clear_caches()
    meta = {}
    for t in TYPES:
        rows = picked[t]
        meta[t] = [{k: v for k, v in r.items() if k != "_img"} for r in rows]
        for s in range(0, len(rows), 9):
            chunk = rows[s:s + 9]
            V.sheet([c["_img"] for c in chunk],
                    [f"{c['id']} {c['discipline']}/{c['cls']} {c['a_cls']}->{c['b_cls']} "
                     f"{json.dumps(c['detail'], ensure_ascii=False)[:50]}" for c in chunk],
                    OUT / f"{t}_sheet{s // 9}.png")
    json.dump({"seed": SEED, "guard": GUARD,
               "n": {t: len(picked[t]) for t in TYPES}, "items": meta},
              open(C.ART / "rel_r7_sample.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({t: len(picked[t]) for t in TYPES}))


if __name__ == "__main__":
    main()
