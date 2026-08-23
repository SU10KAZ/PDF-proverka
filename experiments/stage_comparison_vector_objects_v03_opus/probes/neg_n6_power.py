# -*- coding: utf-8 -*-
"""N6 — the power control that makes the zeros of N1/N2/N4 mean something.

The GATEFIX lesson applies to negative controls too: "we fired on nothing" is not a
result unless the same comparator, at the same settings, on the SAME blocks, fires on
a real change.  Otherwise a comparator that always answers NO_GRAPHIC_CHANGE scores a
perfect negative-control sheet.

So: class C (a genuine single-object edit) on every carrier that N1/N2/N4 used, with
the identical config.  Stratified by object size, because that is what sets the floor.
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

JOBS = [("C1_remove_object", {"bucket": "tiny"}),
        ("C1_remove_object", {"bucket": "small"}),
        ("C1_remove_object", {"bucket": "large"}),
        ("C2_add_object", {"bucket": "small"}),
        ("C3_move_object", {"bucket": "small", "frac": 0.02}),
        ("C6_reshape_object", {"bucket": "small"})]
L_GRID = [0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0]


def run(shard=0, of=1):
    t0 = time.time()
    rows, skips = [], []
    for i, c in enumerate(N.carriers()):
        if i % of != shard:
            continue
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "reason": str(e)}); continue
        la = O.build_objects(ex)
        for cid, prm in JOBS:
            tag = cid + "/" + str(prm.get("bucket"))
            try:
                ex2, man = CF.apply(ex, la, cid, key=key, **prm)
            except CF.CFNotApplicable as e:
                skips.append({"carrier": key, "cf": tag, "reason": str(e)}); continue
            except Exception as e:
                skips.append({"carrier": key, "cf": tag, "reason": f"ERR {e}",
                              "tb": traceback.format_exc()[-300:]}); continue
            try:
                r = N.full_compare2(ex, ex2, shared_scale=True)
            except Exception as e:
                skips.append({"carrier": key, "cf": tag, "reason": f"CMP {e}"}); continue
            lay_a, lay_b, off, rws, cfg = r["_la"], r["_lb"], r["_off"], r["_rows"], r["_cfg"]
            sweep = {}
            for Lm in L_GRID:
                v = N.ledger_at(ex, ex2, lay_a, lay_b, off, Lm, u_share=0.35,
                                L_min_S=0.0, rows=rws)
                sweep[f"L{Lm}"] = {"n": v["n_entries"], "nb": v["n_border_entries"]}
            to = man.get("touched_objects") or [{}]
            rows.append({"carrier": key, "discipline": c["discipline"], "cls": c["cls"],
                         "bucket": c["bucket"], "n_seg": len(ex.segments),
                         "cf_id": cid, "variant": tag, "params": prm,
                         "obj_area_frac": to[0].get("area_frac"),
                         "obj_len_pt": to[0].get("seg_len"),
                         "obj_n_seg": to[0].get("n_seg"),
                         "res": {k: v for k, v in r.items() if not k.startswith("_")},
                         "sweep": sweep})
        print(f"[{i+1}] {key}", flush=True)
    name = "neg_n6_power.json" if of == 1 else f"neg_runs/neg_n6_{shard}of{of}.json"
    N.dump(name, {"schema": "neg-n6-1", "rows": rows, "skips": skips,
                  "L_grid": L_GRID, "sec": round(time.time() - t0, 1)})


if __name__ == "__main__":
    a = sys.argv[1:]
    run(int(a[0]) if a else 0, int(a[1]) if len(a) > 1 else 1)
