# -*- coding: utf-8 -*-
"""M4b [CF] — crop-border attribution: rule v1 vs rule v2, on the same clusters.

v1 (what mov_common.compare shipped): a cluster is a finding unless its bbox leaves the
    frame intersection; a cluster matched as a translated copy is a finding regardless.
v2: a cluster is a finding only if it stays `pad` inside the intersection AND contains no
    segment that the FRAME ITSELF cut (v03_foundation's per-segment `border` flag).

Both verdicts come from ONE comparison run, so the difference is the rule, not the run.

    python probes/mov_m4b_fix.py --shard i --of k
Writes artifacts/mov_runs/b4b_<i>.jsonl
"""
from __future__ import annotations
import argparse, json, math, sys, time, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC          # noqa: E402
import grp_common as G           # noqa: E402
import v03_objects as O          # noqa: E402
import v03_counterfactual as C   # noqa: E402
import cf_build_set as CB        # noqa: E402

ART = MC.ART
OUT = ART / "mov_runs"
MIN_SHARE = 1e-4
PADS = [0.0, 0.5, 1.0, 2.0, 4.0]

JOBS = [("B3_crop_jitter", {"frac": 0.0025}), ("B3_crop_jitter", {"frac": 0.02}),
        ("B3_crop_jitter", {"frac": 0.05}), ("B3_crop_jitter", {"frac": 0.10}),
        ("B4_aspect", {"frac": 0.05}), ("B4_aspect", {"frac": 0.14}),
        ("B1_translate", {"frac": 0.02}),
        ("C1_remove_object", {"bucket": "large"}),
        ("C3_move_object", {"bucket": "large", "frac": 0.005}),
        ("C3_move_object", {"bucket": "small", "frac": 0.005}),
        ("C2_add_object", {"bucket": "large"})]


def _touch(a, b, pad=1.0):
    return not (a[2] < b[0] - pad or b[2] < a[0] - pad or a[3] < b[1] - pad or b[3] < a[1] - pad)


def rule_findings(ik, tot_a, tot_b, rule, pad):
    """Reproduce the ledger from the packed clusters under one rule."""
    out = []
    for c in ik["clusters_a"]:
        if c["ink_len"] <= 0:
            continue
        share = c["ink_len"] / max(tot_a, 1e-9)
        if rule == "v1":
            ok = c["inside_intersection"] or bool(c["moved"])
        else:
            ok = (c.get("margin_pt", 0.0) >= pad) and not c.get("border_seg")
        if ok and share > MIN_SHARE:
            out.append({"type": "MOVED_INK" if c["moved"] else "REMOVED_INK",
                        "bbox": c["bbox"], "share": share, "ink_len": c["ink_len"],
                        "moved": c["moved"]})
    for c in ik["clusters_b"]:
        if c["moved_from"] is not None or c["ink_len"] <= 0:
            continue
        share = c["ink_len"] / max(tot_b, 1e-9)
        if rule == "v1":
            ok = c["inside_intersection"]
        else:
            ok = (c.get("margin_pt", 0.0) >= pad) and not c.get("border_seg")
        if ok and share > MIN_SHARE:
            out.append({"type": "ADDED_INK", "bbox": c["bbox"], "share": share,
                        "ink_len": c["ink_len"], "moved": None})
    return out


def run_carrier(rec):
    rows = []
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return [{"carrier": rec["block_id"], "error": "block not found"}]
    ex = G.extract(pb)
    if not ex.segments:
        return [{"carrier": rec["block_id"], "error": "no vector geometry"}]
    L = O.build_objects(ex)
    n_bd = sum(1 for s in ex.segments if s.get("border"))
    base = {"block_id": rec["block_id"], "discipline": rec["discipline"],
            "bucket": rec["bucket"], "n_seg": len(ex.segments), "n_obj": len(L.objects),
            "S": round(L.S, 3), "n_border_seg": n_bd,
            "border_seg_share": round(n_bd / len(ex.segments), 5)}
    for cf, kw in JOBS:
        t0 = time.time()
        tag = cf + "@" + "@".join(str(v) for v in kw.values())
        row = dict(base, cf_id=cf, tag=tag, params=kw)
        try:
            ex2, man = C.apply(ex, L, cf, **kw)
            out, rep, LA, LB = MC.compare(ex, ex2, modes=("strict",))
            row["status"] = out.get("status")
            if out.get("status") == "ALIGNMENT_UNAVAILABLE":
                rows.append(row); continue
            ik = (out.get("ink") or {}).get("strict") or {}
            if "skipped" in ik:
                row["skipped"] = ik["skipped"]; rows.append(row); continue
            tot_a = sum(s["len"] for s in ex.segments)
            tot_b = sum(s["len"] for s in ex2.segments)
            truth = man.get("change_bbox_pt") or (man.get("touched_objects") or [{}])[0].get("bbox_pt")
            row["truth_bbox"] = truth
            row["expected"] = C.CF_SPECS[cf]["expected"]
            row["v1"] = {}
            row["v2"] = {}
            for rule, pads in (("v1", [0.0]), ("v2", PADS)):
                for pad in pads:
                    f = rule_findings(ik, tot_a, tot_b, rule, pad)
                    hit = [x for x in f if truth and _touch(x["bbox"], truth)]
                    d = {"n": len(f), "n_on_truth": len(hit),
                         "n_elsewhere": len(f) - len(hit),
                         "share_sum": round(sum(x["share"] for x in f), 6),
                         "types": sorted({x["type"] for x in f}),
                         "top": sorted(f, key=lambda x: -x["share"])[:4]}
                    (row["v1"] if rule == "v1" else row["v2"])[f"pad{pad}"] = d
            row["ink_v2_agg"] = ik.get("v2")
            row["ink_v1_agg"] = {k: ik.get(k) for k in
                                 ("lost_ink_share_a", "new_ink_share_b", "moved_ink_share_a",
                                  "border_ink_share_a", "border_ink_share_b")}
        except C.CFNotApplicable as e:
            row["skipped"] = str(e)
        except Exception as e:
            row["error"] = repr(e); row["trace"] = traceback.format_exc()[-700:]
        row["t_sec"] = round(time.time() - t0, 2)
        rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    a = ap.parse_args()
    carriers = CB.pick_carriers()
    mine = [c for i, c in enumerate(carriers) if i % a.of == a.shard]
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / f"b4b_{a.shard}.jsonl", "w", encoding="utf-8") as fh:
        for k, rec in enumerate(mine):
            t0 = time.time()
            for row in run_carrier(rec):
                fh.write(json.dumps(row, ensure_ascii=False, default=float) + "\n")
            fh.flush()
            print(f"[b4b.{a.shard}] {k+1}/{len(mine)} {rec['block_id'][:12]} "
                  f"{rec['discipline']} {rec['n_seg']} seg {round(time.time()-t0,1)}s", flush=True)


if __name__ == "__main__":
    main()
