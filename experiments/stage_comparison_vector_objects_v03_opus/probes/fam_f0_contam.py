# -*- coding: utf-8 -*-
"""F0 — how contaminated is the A4 rewrite of `grp_common` (the defect `cf` found)?

`grp_common._closed_circles(min_pts=5)` accepts every closed chain of >= 4 segments
whose points fit a circle -- the four corners of a rectangle do.  So A4* silently
replaced rectangles by circles: the "representation rewrite" changed the picture.
Here we count, per real block, how many chains the loose detector accepts that the
strict one (`v03_counterfactual._circles_strict`) rejects, and what share of the
block's ink they carry.
Usage: fam_f0_contam.py [n] [out.json]
"""
from __future__ import annotations
import json, statistics, sys, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import v03_counterfactual as CF


def one(b):
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments or len(ex.segments) > 60000:
        return None
    loose = G._closed_circles(ex.segments)
    strict = CF._circles_strict(ex.segments)
    sset = {tuple(ch) for ch, *_ in strict}
    bad = [ch for ch, *_ in loose if tuple(ch) not in sset]
    tot = sum(s["len"] for s in ex.segments) or 1.0
    bad_len = sum(ex.segments[k]["len"] for ch in bad for k in ch)
    ok_len = sum(ex.segments[k]["len"] for ch, *_ in strict for k in ch)
    n4 = sum(1 for ch in bad if len(ch) <= 5)
    return {"block_id": b["block_id"], "discipline": b["discipline"], "cls": b["cls"],
            "n_seg": len(ex.segments), "n_loose": len(loose), "n_strict": len(strict),
            "n_false_circles": len(bad), "n_false_short": n4,
            "false_share_of_loose": round(len(bad) / max(len(loose), 1), 4),
            "false_ink_share": round(bad_len / tot, 6),
            "true_ink_share": round(ok_len / tot, 6)}


def _w(b):
    try:
        return one(b)
    except Exception:
        return {"block_id": b["block_id"], "error": traceback.format_exc().splitlines()[-1]}


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 250
    out = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f0_contam.json")
    smp = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))["blocks"][:n]
    smp = [b for b in smp if b.get("n_seg", 0) <= 60000]
    rows = []
    for k, b in enumerate(smp):
        r = _w(b)
        if r:
            rows.append(r)
        if k % 20 == 0:
            print(k, len(smp), flush=True)
    good = [r for r in rows if "n_loose" in r]
    withc = [r for r in good if r["n_loose"] > 0]
    summ = {
        "n_blocks": len(good),
        "n_blocks_with_loose_circles": len(withc),
        "n_blocks_with_false_circles": sum(1 for r in good if r["n_false_circles"] > 0),
        "share_blocks_with_false_circles": round(
            sum(1 for r in good if r["n_false_circles"] > 0) / max(len(good), 1), 4),
        "loose_total": sum(r["n_loose"] for r in good),
        "strict_total": sum(r["n_strict"] for r in good),
        "false_total": sum(r["n_false_circles"] for r in good),
        "false_share_of_loose_aggregate": round(
            sum(r["n_false_circles"] for r in good) / max(sum(r["n_loose"] for r in good), 1), 4),
        "false_ink_share_median": round(statistics.median(
            [r["false_ink_share"] for r in withc] or [0]), 6),
        "false_ink_share_p90": round(G.pct([r["false_ink_share"] for r in withc] or [0], 0.9), 6),
        "false_ink_share_max": round(max([r["false_ink_share"] for r in good] or [0]), 6),
    }
    json.dump({"summary": summ, "rows": rows}, open(out, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(summ, indent=1))


if __name__ == "__main__":
    main()
