# -*- coding: utf-8 -*-
"""Stratified block sample for the `grp` probe.

Strata: density bucket x discipline, over the `cns` census of REAL prepared graphic
blocks whose version has a document.pdf.  Deterministic (seed in the artifact).
"""
from __future__ import annotations
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G

SEED = 20260823
BUCKETS = [("sparse", 20, 500), ("medium", 500, 5000),
           ("dense", 5000, 50000), ("very_dense", 50000, 10**9)]
KEEP_CLS = {"drawing", "stamp", "table", "legend_notes", "vector_raster_mix"}


def bucket(n):
    for name, lo, hi in BUCKETS:
        if lo <= n < hi:
            return name
    return None


def main():
    G.result_json_for("x", "y")           # warm the index
    have = G._RESULT_INDEX
    pool: dict[tuple, list] = {}
    for r in G.block_records():
        if r["cls"] not in KEEP_CLS:
            continue
        b = bucket(r["n_seg"])
        if b is None:
            continue
        if f"{r['doc_id']}|{r['version']}" not in have:
            continue
        pool.setdefault((r["discipline"], b, r["cls"]), []).append(r)

    rng = random.Random(SEED)
    per_stratum = {"sparse": 3, "medium": 3, "dense": 3, "very_dense": 2}
    chosen, seen_doc = [], {}
    for key in sorted(pool):
        disc, b, cls = key
        rows = sorted(pool[key], key=lambda r: r["block_id"])
        rng.shuffle(rows)
        k = per_stratum[b] if cls == "drawing" else max(1, per_stratum[b] - 1)
        taken = 0
        for r in rows:
            if taken >= k:
                break
            # at most 3 blocks from one document keeps a single big album from dominating
            c = seen_doc.get(r["doc_id"], 0)
            if c >= 3:
                continue
            seen_doc[r["doc_id"]] = c + 1
            r = dict(r)
            r["bucket"] = b
            chosen.append(r)
            taken += 1

    out = {
        "seed": SEED, "n": len(chosen),
        "buckets": {b: sum(1 for r in chosen if r["bucket"] == b) for b, _, _ in BUCKETS},
        "disciplines": {},
        "classes": {},
        "blocks": chosen,
    }
    for r in chosen:
        out["disciplines"][r["discipline"]] = out["disciplines"].get(r["discipline"], 0) + 1
        out["classes"][r["cls"]] = out["classes"].get(r["cls"], 0) + 1
    p = G.ART / "grp_sample.json"
    json.dump(out, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "blocks"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
