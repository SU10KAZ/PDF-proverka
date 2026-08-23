# -*- coding: utf-8 -*-
"""M5 — WHY alignment refuses.  The main CF run lost the estimator's `reason` field on
exactly the rows that failed (the report harness crashed before writing it), so the
refusals are re-run here and the reason recorded together with the object counts, which
is what actually explains them.

    python probes/mov_m5_reason.py
Writes artifacts/mov_unavail_reasons.json
"""
from __future__ import annotations
import glob, json, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC       # noqa: E402
import grp_common as G        # noqa: E402
import v03_objects as O       # noqa: E402
import v03_counterfactual as C  # noqa: E402

ART = MC.ART


def main():
    rows = []
    for f in sorted(glob.glob(str(ART / "mov_runs" / "cf_*.jsonl"))):
        for l in open(f, encoding="utf-8"):
            r = json.loads(l)
            if r.get("status") == "ALIGNMENT_UNAVAILABLE" and not r.get("chained"):
                rows.append(r)
    out = []
    seen = set()
    for r in rows:
        key = (r["block_id"], r["tag"])
        if key in seen:
            continue
        seen.add(key)
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        ex = G.extract(pb)
        L = O.build_objects(ex)
        rec = {"tag": r["tag"], "block_id": r["block_id"], "discipline": r["discipline"],
               "n_seg": r["n_seg"], "n_obj_a": len(L.objects)}
        try:
            ex2, man = C.apply(ex, L, r["cf_id"], **r["params"])
            o, rep, LA, LB = MC.compare(ex, ex2, modes=("strict",))
            e = o.get("estimate") or {}
            rec.update({"status": o.get("status"), "reason": o.get("reason"),
                        "n_obj_b": o.get("n_obj_b"),
                        "obj_ratio": round((o.get("n_obj_b") or 0) / max(1, len(L.objects)), 4),
                        "n_anchor_a": e.get("n_anchor_a"), "n_anchor_b": e.get("n_anchor_b"),
                        "inliers": e.get("inliers"), "inlier_ratio": e.get("inlier_ratio")})
        except Exception as exc:
            rec["error"] = repr(exc)
        out.append(rec)
        print(rec, flush=True)
    from collections import Counter
    agg = {"n": len(out),
           "by_reason": Counter(x.get("reason") for x in out).most_common(),
           "by_tag": Counter(x["tag"] for x in out).most_common(),
           "obj_ratio_median": sorted(x["obj_ratio"] for x in out if "obj_ratio" in x)[len(out) // 2]
           if out else None}
    json.dump({"schema": "mov-unavail-1", "agg": agg, "rows": out},
              open(ART / "mov_unavail_reasons.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(agg, ensure_ascii=False))


if __name__ == "__main__":
    main()
