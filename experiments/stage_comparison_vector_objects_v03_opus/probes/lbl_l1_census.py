# -*- coding: utf-8 -*-
"""L1 — LABEL CENSUS.  On what share of real prepared blocks / objects is a text
anchor available at all, and how often is it UNIQUE inside the block?

Two populations, both real prepared image-blocks:
  bench  — every side of every benchmark pair (mine_pairs.json)
  corpus — the stratified sample of the `grp` probe (grp_sample.json)

Nothing here is a counterfactual: pure [REAL] census.
Usage: lbl_l1_census.py [workers]
"""
from __future__ import annotations
import json, os, sys, time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G

MAX_SEG = 200000          # cns: 1.16 % of the corpus is heavier; skipped with a reason


def one(rec):
    out = {"key": rec["key"], "pop": rec["pop"], "discipline": rec.get("discipline"),
           "cls": rec.get("cls"), "bucket": rec.get("bucket")}
    try:
        ex = G.F.extract_block(rec["pdf"], rec["page_index"], rec["coords_px"],
                               rec["page_px"][0], rec["page_px"][1])
        if not ex.segments:
            out["skip"] = "no vector geometry"
            return out
        if len(ex.segments) > MAX_SEG:
            out["skip"] = f"too heavy ({len(ex.segments)} segments)"
            return out
        t0 = time.time()
        Lay = G.layer_of(ex.segments, ex.texts)
        recs = L.label_census(Lay, ex.texts)
        out.update({"n_seg": len(ex.segments), "n_text": len(ex.texts),
                    "n_obj": len(Lay.objects), "S": round(Lay.S, 4),
                    "scale_source": Lay.scale_source, "t_sec": round(time.time() - t0, 2)})
        agg = {}
        for k in L.K_LADDER:
            kk = f"k{k}"
            st = defaultdict(int)
            gaps = []
            for r in recs:
                st[r[kk]["state"]] += 1
                if "gap_over_S" in r[kk]:
                    gaps.append(r[kk]["gap_over_S"])
            agg[kk] = dict(st)
            agg[kk]["median_gap_over_S"] = round(sorted(gaps)[len(gaps) // 2], 3) if gaps else None
        out["by_k"] = agg
        # per class of object, at the working radius 1.6*S
        by_cls = defaultdict(lambda: defaultdict(int))
        for r in recs:
            by_cls[r["cls"]][r["k1.6"]["state"]] += 1
        out["by_obj_cls"] = {c: dict(v) for c, v in by_cls.items()}
        out["n_marks_in_block"] = len(L.block_mark_index(ex.texts))
        out["n_unique_marks"] = sum(1 for v in L.block_mark_index(ex.texts).values() if v == 1)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        G.F._DRAW_CACHE.clear()
        G.F.clear_caches()
    return out


def tasks():
    T = []
    pairs = json.load(open(L.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    for p in pairs:
        for side in ("side_a", "side_b"):
            s = p[side]
            T.append({"pop": "bench", "key": f"{p['pair_id']}/{side}",
                      "discipline": p["discipline"], "cls": None, "bucket": None,
                      "pdf": str(L.ROOT / s["pdf"]), "page_index": s["page_index"],
                      "coords_px": s["coords_px"], "page_px": s["page_px"]})
    smp = json.load(open(L.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    for r in smp:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        T.append({"pop": "corpus", "key": f"{r['doc_id']}|{r['version']}|{r['block_id']}",
                  "discipline": r["discipline"], "cls": r["cls"], "bucket": r["bucket"],
                  "pdf": pb.pdf_path, "page_index": pb.page_index,
                  "coords_px": list(pb.coords_px), "page_px": [pb.page_px_w, pb.page_px_h]})
    return T


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    T = tasks()
    print(f"{len(T)} blocks", flush=True)
    rows = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futs = {exe.submit(one, t): t["key"] for t in T}
        for i, f in enumerate(as_completed(futs)):
            rows.append(f.result())
            if (i + 1) % 25 == 0:
                print(f"  {i+1}/{len(T)}", flush=True)
    json.dump({"k_ladder": list(L.K_LADDER), "mark_rule": "token with a letter AND a digit",
               "rows": rows},
              open(L.ART / "lbl_census.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    ok = [r for r in rows if "by_k" in r]
    print("done", len(ok), "of", len(rows))


if __name__ == "__main__":
    main()
