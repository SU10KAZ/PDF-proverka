# -*- coding: utf-8 -*-
"""M2 / M3 — [CF] separating "the whole block moved" from "one object moved".

    python probes/mov_m2_cf.py --shard i --of k

Every instance: a REAL prepared block (carriers of the `cf` probe), one counterfactual
from `v03_counterfactual`, then the `mov` comparison.  Ground truth comes from the
counterfactual manifest, never from a label.
Writes artifacts/mov_runs/cf_<i>.jsonl
"""
from __future__ import annotations
import argparse, json, math, sys, time, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC          # noqa: E402
import mov_align as MA           # noqa: E402
import grp_common as G           # noqa: E402
import v03_objects as O          # noqa: E402
import v03_counterfactual as C   # noqa: E402
import cf_build_set as CB        # noqa: E402

ART = MC.ART
OUT = ART / "mov_runs"
MIN_SHARE = 1e-4          # ink share below which a cluster is not counted as a finding
DELTAS = [0.0002, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05]


def plan():
    P = []
    for cf in ("A1_path_split", "A5_order_shuffle", "A6_round_0.1", "A6_round_0.25",
               "D1_text_edit", "D3_label_rename"):
        P.append((cf, {}, cf))
    for f in (0.005, 0.02, 0.10):
        P.append(("B1_translate", {"frac": f}, f"B1_translate@{f}"))
    for k in (0.95, 1.05, 1.2):
        P.append(("B2_scale", {"k": k}, f"B2_scale@{k}"))
    for f in (0.0025, 0.005, 0.02, 0.05, 0.10):
        P.append(("B3_crop_jitter", {"frac": f}, f"B3_crop_jitter@{f}"))
    for f in (0.05, 0.14):
        P.append(("B4_aspect", {"frac": f}, f"B4_aspect@{f}"))
    for a in (90, 270):
        P.append(("B5_rotate_page", {"add": a}, f"B5_rotate_page@{a}"))
    for b in ("tiny", "small", "large"):
        P.append(("C1_remove_object", {"bucket": b}, f"C1_remove_object@{b}"))
        for f in DELTAS:
            P.append(("C3_move_object", {"bucket": b, "frac": f}, f"C3_move_object@{b}@{f}"))
    return P


COMBO = [("C3+B1", "large", 0.01, ("B1_translate", {"frac": 0.02})),
         ("C3+B1", "small", 0.01, ("B1_translate", {"frac": 0.02})),
         ("C3+B1", "large", 0.0025, ("B1_translate", {"frac": 0.10})),
         ("C3+B2", "large", 0.01, ("B2_scale", {"k": 1.05})),
         ("C3+B2", "small", 0.02, ("B2_scale", {"k": 1.2}))]


def _bbox_iou(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    ua = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def _touches(a, b, pad=0.0):
    return not (a[2] < b[0] - pad or b[2] < a[0] - pad or a[3] < b[1] - pad or b[3] < a[1] - pad)


def score(out, man, ex, ex2):
    """Compare the produced ledger with the manifest ground truth."""
    ik = (out.get("ink") or {}).get("strict") or {}
    led = out.get("ledger_unified", [])
    tot_a = sum(s["len"] for s in ex.segments) or 1.0
    findings = [l for l in led if l["type"] in ("MOVED_INK", "REMOVED_INK", "ADDED_INK")
                and l["ink_len"] / tot_a > MIN_SHARE]
    truth_bbox = man.get("change_bbox_pt") or (man.get("touched_objects") or [{}])[0].get("bbox_pt")
    exp = man.get("expected_ledger") or []
    exp_move = [e for e in exp if e["type"] == "MOVED_OBJECT"]
    r = {"n_findings": len(findings),
         "types": sorted({l["type"] for l in findings}),
         "moved_ink_share": ik.get("moved_ink_share_a"),
         "lost_ink_share": ik.get("lost_ink_share_a"),
         "new_ink_share": ik.get("new_ink_share_b"),
         "border_ink_share_a": ik.get("border_ink_share_a"),
         "border_ink_share_b": ik.get("border_ink_share_b"),
         "n_border_clusters": ik.get("n_border_clusters"),
         "block_transformed": any(l["type"] == "BLOCK_TRANSFORMED" for l in led)}
    if exp_move and truth_bbox:
        d_true = math.hypot(exp_move[0]["dx_pt"], exp_move[0]["dy_pt"])
        r["truth_d_pt"] = round(d_true, 4)
        hit = None
        for l in findings:
            if l["type"] != "MOVED_INK":
                continue
            if not _touches(l["bbox_pt"], truth_bbox, 1.0):
                continue
            err = math.hypot(l["dx_pt"] - exp_move[0]["dx_pt"], l["dy_pt"] - exp_move[0]["dy_pt"])
            if hit is None or err < hit[0]:
                hit = (err, l)
        r["move_detected"] = bool(hit is not None and hit[0] <= max(0.25, 0.05 * d_true))
        r["move_vector_err_pt"] = round(hit[0], 4) if hit else None
        r["moved_ink_len"] = round(hit[1]["ink_len"], 3) if hit else None
        # localisation: any finding at the right place, even if not named a move
        r["localised"] = any(_touches(l["bbox_pt"], truth_bbox, 1.0) for l in findings)
        r["n_findings_elsewhere"] = sum(1 for l in findings
                                        if not _touches(l["bbox_pt"], truth_bbox, 1.0))
    elif truth_bbox:
        r["localised"] = any(_touches(l["bbox_pt"], truth_bbox, 1.0) for l in findings)
        r["n_findings_elsewhere"] = sum(1 for l in findings
                                        if not _touches(l["bbox_pt"], truth_bbox, 1.0))
    return r


def transform_error(out, cf_id, params, man):
    g = out.get("global_transform") or out.get("transform") or {}
    s, th = g.get("s"), g.get("theta")
    tx, ty = g.get("tx"), g.get("ty")
    e = {"s": s, "theta": th, "tx": round(float(tx or 0), 4), "ty": round(float(ty or 0), 4)}
    comp = man.get("compensation") or {}
    if cf_id == "B1_translate":
        d = man["delta"]
        e["t_err_pt"] = round(math.hypot(tx - d["dx_pt"], ty - d["dy_pt"]), 5)
        e["s_err"] = round(abs(s - 1.0), 8)
        e["theta_ok"] = (th == 0)
    elif cf_id == "B2_scale":
        k, cx, cy = comp["k"], comp["cx"], comp["cy"]
        e["t_err_pt"] = round(math.hypot(tx - cx * (1 - k), ty - cy * (1 - k)), 5)
        e["s_err"] = round(abs(s - k), 8)
        e["theta_ok"] = (th == 0)
    elif cf_id in ("B3_crop_jitter", "B4_aspect") or cf_id.startswith(("A", "C", "D")):
        e["t_err_pt"] = round(math.hypot(tx, ty), 5)
        e["s_err"] = round(abs(s - 1.0), 8)
        e["theta_ok"] = (th == 0)
    elif cf_id == "B5_rotate_page":
        e["theta_ok"] = th in (90, 270)
        e["s_err"] = round(abs(s - 1.0), 8)
    return e


def run_carrier(rec):
    rows = []
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return [{"carrier": rec["block_id"], "error": "block not found"}]
    ex = G.extract(pb)
    if not ex.segments:
        return [{"carrier": rec["block_id"], "error": "no vector geometry"}]
    L = O.build_objects(ex)
    base = {"block_id": rec["block_id"], "doc_id": rec["doc_id"], "version": rec["version"],
            "discipline": rec["discipline"], "bucket": rec["bucket"], "cls": rec.get("cls"),
            "n_seg": len(ex.segments), "n_obj": len(L.objects), "S": round(L.S, 3)}
    jobs = [(cf, kw, tag, None) for cf, kw, tag in plan()]
    for name, bucket, frac, (cf2, kw2) in COMBO:
        jobs.append(("C3_move_object", {"bucket": bucket, "frac": frac},
                     f"{name}@{bucket}@{frac}@{cf2}{list(kw2.values())[0]}", (cf2, kw2)))
    for cf, kw, tag, chain in jobs:
        t0 = time.time()
        row = dict(base, cf_id=cf, tag=tag, params=kw)
        try:
            ex2, man = C.apply(ex, L, cf, **kw)
            if chain:
                ex2b, man2 = C.apply(ex2, None, chain[0], **chain[1])
                row["chained"] = chain[0]
                ex2 = ex2b
                man = dict(man)
                man["chained_delta"] = man2.get("delta")
                # the truth bbox of the moved object is expressed in the pre-chain frame;
                # the chained transform is global, so the ledger is compared after
                # compensation and the bbox stays valid.
            out, rep, LA, LB = MC.compare(ex, ex2, modes=("strict",))
            row["status"] = out.get("status")
            row["verdict"] = out.get("verdict")
            row["n_obj_b"] = out.get("n_obj_b")
            row["transform"] = transform_error(out, cf, kw, man)
            row["estimate"] = {k: out.get("estimate", {}).get(k)
                               for k in ("inliers", "inlier_ratio", "n_candidates",
                                         "theta_free_deg", "s_free", "ambiguous")}
            row["obj_counts"] = out.get("counts")
            row["residual"] = out.get("residual")
            row["score"] = score(out, man, ex, ex2)
            row["expected"] = C.CF_SPECS[cf]["expected"] if not chain else "GRAPHIC_CHANGE"
        except C.CFNotApplicable as e:
            row["skipped"] = str(e)
        except Exception as e:
            row["error"] = repr(e)
            row["trace"] = traceback.format_exc()[-800:]
        row["t_sec"] = round(time.time() - t0, 2)
        rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    carriers = CB.pick_carriers()
    if a.limit:
        carriers = carriers[: a.limit]
    mine = [c for i, c in enumerate(carriers) if i % a.of == a.shard]
    OUT.mkdir(parents=True, exist_ok=True)
    fh = open(OUT / f"cf_{a.shard}.jsonl", "w", encoding="utf-8")
    for k, rec in enumerate(mine):
        t0 = time.time()
        for row in run_carrier(rec):
            fh.write(json.dumps(row, ensure_ascii=False, default=float) + "\n")
        fh.flush()
        print(f"[{a.shard}] {k+1}/{len(mine)} {rec['block_id'][:14]} {rec['discipline']} "
              f"{rec['n_seg']} seg  {round(time.time()-t0,1)}s", flush=True)
    fh.close()


if __name__ == "__main__":
    main()
