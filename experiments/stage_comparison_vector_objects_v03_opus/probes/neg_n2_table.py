# -*- coding: utf-8 -*-
"""N2 — table-only negative controls, grid census, and the row add/remove boundary."""
from __future__ import annotations
import json, random, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_tablecf as TB        # noqa: E402
import grp_common as G          # noqa: E402
import v03_counterfactual as CF # noqa: E402
import v03_objects as O         # noqa: E402

CF_TABLE = ["D4_table_values", "D5_table_row_text"]
SEED = 20260823


def table_sample(n_extra=90):
    """stamp / table blocks from the census, spread over disciplines and documents."""
    rows = [r for r in G.block_records()
            if r.get("cls") in ("stamp", "table") and r.get("n_seg", 0) >= 60
            and not r.get("dup")]
    rng = random.Random(SEED)
    rng.shuffle(rows)
    per_doc: dict[str, int] = {}
    per_disc: dict[str, int] = {}
    out = []
    for r in rows:
        if per_doc.get(r["doc_id"], 0) >= 2:
            continue
        if per_disc.get(r["discipline"], 0) >= max(6, n_extra // 5):
            continue
        per_doc[r["doc_id"]] = per_doc.get(r["doc_id"], 0) + 1
        per_disc[r["discipline"]] = per_disc.get(r["discipline"], 0) + 1
        out.append(r)
        if len(out) >= n_extra:
            break
    return out


def run():
    t_all = time.time()
    # ---------------- part 1: D4 / D5 (text inside cells) -----------------------
    d_rows, skips = [], []
    for c in N.carriers():
        key = N.carrier_key(c)
        try:
            ex = N.carrier_extract(c)
        except Exception as e:
            skips.append({"carrier": key, "cf": "*", "reason": str(e)}); continue
        la = O.build_objects(ex)
        for cid in CF_TABLE:
            try:
                ex2, man = CF.apply(ex, la, cid, key=key)
            except CF.CFNotApplicable as e:
                skips.append({"carrier": key, "cf": cid, "reason": str(e)}); continue
            gid = N.geometry_identical(ex, ex2)
            r_sh = N.full_compare2(ex, ex2, shared_scale=True)
            r_no = N.full_compare2(ex, ex2, shared_scale=False)
            d_rows.append({"carrier": key, "discipline": c["discipline"], "cls": c["cls"],
                           "n_seg": len(ex.segments), "n_text": len(ex.texts),
                           "cf_id": cid, "geometry_identical": gid,
                           "shared": {k: v for k, v in r_sh.items() if not k.startswith("_")},
                           "own_scale": {k: v for k, v in r_no.items() if not k.startswith("_")}})
        print("D45", key, flush=True)

    # ---------------- part 2+3: grid census and row edits ------------------------
    census, row_rows = [], []
    pool = [{"block_id": c["block_id"], "doc_id": c["doc_id"], "version": c["version"],
             "discipline": c["discipline"], "cls": c["cls"], "source": "carrier"}
            for c in N.carriers() if c["cls"] in ("stamp", "table")]
    pool += [{**r, "source": "census"} for r in table_sample()]
    seen = set()
    for i, r in enumerate(pool):
        k = f"{r['doc_id']}|{r['version']}|{r['block_id']}"
        if k in seen:
            continue
        seen.add(k)
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            if pb is None:
                skips.append({"block": k, "reason": "no prepared block"}); continue
            ex = G.extract(pb)
        except Exception as e:
            skips.append({"block": k, "reason": f"extract: {e}"}); continue
        if not ex.segments:
            skips.append({"block": k, "reason": "no vector geometry"}); continue
        lay = O.build_objects(ex)
        rul = TB.rulings(ex)
        fr = [float(v) for v in ex.frame["clip_display"]]
        w = max(fr[2] - fr[0], 1e-6)
        n_rule_obj = sum(1 for o in lay.objects
                         if o["cls"] == "linear" and o["diag"] >= 0.30 * w)
        census.append({"block": k, "discipline": r["discipline"], "cls": r["cls"],
                       "source": r["source"], "n_seg": len(ex.segments),
                       "n_text": len(ex.texts), "n_obj": len(lay.objects),
                       "counts": lay.counts(), "n_h_rules": len(rul),
                       "n_long_linear_obj": n_rule_obj,
                       "obj_per_1k_seg": round(1000 * len(lay.objects) / max(1, len(ex.segments)), 2),
                       "S": round(lay.S, 3)})
        for cid, prm in TB.VARIANTS:
            try:
                ex2, man = TB.apply(ex, cid, k, **prm)
            except CF.CFNotApplicable as e:
                skips.append({"block": k, "cf": cid, "reason": str(e)}); continue
            except Exception as e:
                skips.append({"block": k, "cf": cid, "reason": f"ERROR {e}",
                              "tb": traceback.format_exc()[-300:]}); continue
            try:
                res = N.full_compare2(ex, ex2, shared_scale=True)
            except Exception as e:
                skips.append({"block": k, "cf": cid, "reason": f"CMP {e}"}); continue
            row_rows.append({"block": k, "discipline": r["discipline"], "cls": r["cls"],
                             "cf_id": cid, "manifest": man,
                             "n_obj_before": len(lay.objects),
                             "res": {kk: v for kk, v in res.items() if not kk.startswith("_")}})
        if (i + 1) % 10 == 0:
            print(f"grid {i+1}/{len(pool)}", flush=True)

    N.dump("neg_n2_table.json", {"schema": "neg-n2-1",
                                 "d45": d_rows, "grid_census": census,
                                 "row_edits": row_rows, "skips": skips,
                                 "sec": round(time.time() - t_all, 1)})


if __name__ == "__main__":
    run()
