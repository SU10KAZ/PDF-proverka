# -*- coding: utf-8 -*-
"""Same replay as loc_c1_rescore.py, but work-ordered and resumable.

Rows already present in artifacts/loc_runs/rescore*.jsonl are skipped, the remaining
work is ordered by (noise priority, measured cost of the first run) so that coverage
grows breadth-first instead of getting stuck in the densest carrier.

    python probes/loc_c1_rescore2.py <shard> <nshards>
"""
from __future__ import annotations
import glob
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402
from loc_c1_sens import plan, touched_ink  # noqa: E402

NOISE_PRIO = {"none": 0, "round025": 1, "jitter03": 2, "jitter10": 3}
# optional filter: replay only these noise modes.  The heavy tail of the grid costs
# hours and the report's main tables use `none` and `round025`.
import os                                              # noqa: E402
ONLY = set((os.environ.get("LOC_ONLY_NOISE") or "").split(",")) - {""}


def worklist():
    done = set()
    for f in glob.glob(str(ART / "loc_runs" / "rescore*.jsonl")):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            done.add((r["block_id"], r["inst"], r.get("noise")))
    work = []
    for f in sorted(glob.glob(str(ART / "loc_runs" / "sens_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if "score" not in r or r.get("cf_class") != "C":
                continue
            s = r["score"]
            if s["L2_localised"] and s["L3_right_type"] and s["L4_right_object"]:
                continue
            key = (r["block_id"], r["inst"], r["noise"])
            if key in done:
                continue
            if ONLY and r["noise"] not in ONLY:
                continue
            work.append((NOISE_PRIO.get(r["noise"], 9), r["t_sec"], key))
    work.sort()
    return [w[2] for w in work]


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    W = [w for i, w in enumerate(worklist()) if i % nsh == shard]
    carriers = {r["block_id"]: r for r in pick_carriers()}
    P = {(f"{c}@{tag}" if tag else c): (c, kw) for (c, kw, tag) in plan()}
    by_block: dict[str, list] = {}
    for (bid, inst, noise) in W:
        by_block.setdefault(bid, []).append((inst, noise))
    out = open(ART / "loc_runs" / f"rescore4_{shard}.jsonl", "w", encoding="utf-8")
    for bid, todo in by_block.items():
        r = carriers[bid]
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            ol = O.build_objects(ex)
        except Exception as e:
            print("CARRIER FAIL", bid, repr(e), flush=True)
            continue
        if not ex.segments:
            continue
        ink_total = sum(s["len"] for s in ex.segments)
        base = {"block_id": bid, "doc_id": r["doc_id"], "version": r["version"],
                "discipline": r["discipline"], "bucket": r["bucket"], "cls": r["cls"],
                "n_seg": len(ex.segments), "ink_total_pt": round(ink_total, 2),
                "n_obj": len(ol.objects), "S": round(ol.S, 3)}
        made = {}
        for inst in sorted({i for (i, _n) in todo}):
            cf, kw = P[inst]
            try:
                made[inst] = C.apply(ex, ol, cf, **kw)
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst, "skip": repr(e)},
                                     ensure_ascii=False) + "\n")
        for inst, noise in todo:
            if inst not in made:
                continue
            ex2, man = made[inst]
            t0 = time.time()
            try:
                exB = L.noisy(ex2, noise, seed=20260823)
                LA, LB, meta = L.layers(ex, exB)
                led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                sc = L.score_against_manifest(led, man)
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst, "noise": noise,
                                      "error": repr(e)}, ensure_ascii=False) + "\n")
                continue
            tk_len, tk_n = touched_ink(ex, man)
            to = (man.get("touched_objects") or [{}])[0]
            out.write(json.dumps({
                **base, "inst": inst, "noise": noise,
                "cf_id": man.get("cf_id"), "cf_class": man.get("cf_class"),
                "expected": man.get("expected_verdict"),
                "size_bucket": (man.get("params") or {}).get("size_bucket"),
                "frac_of_diag": (man.get("params") or {}).get("frac_of_diag"),
                "obj_area_frac": to.get("area_frac_of_block"),
                "obj_n_seg": to.get("n_seg"), "obj_diag_pt": to.get("diag_pt"),
                "touched_ink_pt": tk_len, "touched_n_seg": tk_n,
                "touched_ink_frac": round(tk_len / max(ink_total, 1e-9), 8),
                "counters_invariant": bool(man.get("counters_invariant")),
                "scalar": led["scalar"], "counts": led["counts"],
                "verdict_scalar_999": L.scalar_verdict(led, 0.999),
                "verdict_scalar_9999": L.scalar_verdict(led, 0.9999),
                "verdict_counts": L.counts_verdict(led),
                "score": sc, "t_sec": round(time.time() - t0, 2),
            }, ensure_ascii=False) + "\n")
            out.flush()
        print("done", bid, len(todo), flush=True)
    out.close()


if __name__ == "__main__":
    main()
