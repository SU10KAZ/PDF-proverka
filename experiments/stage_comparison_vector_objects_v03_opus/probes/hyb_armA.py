# -*- coding: utf-8 -*-
"""`hyb` ARM A — the deterministic vector object comparator on the counterfactual cases.

Identical code path to loc_c4_real.py (same registration, same shared scale, same ledger),
only the inputs differ: both sides are read from the materialised counterfactual PDFs, so
arm A sees exactly the two documents arms B and C see.

Real pairs are NOT recomputed here: loc_c4_real.py already ran this same ledger on them
(loc_real_pairs.json) and loc L17 measured the comparator to be bit-reproducible over
1 315 repeated rows.

    python3 probes/hyb_armA.py [case_id ...]
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H         # noqa: E402
import v03_foundation as F     # noqa: E402
import grp_match as M          # noqa: E402
import loc_common as L         # noqa: E402


def extract(sd):
    p = Path(sd["pdf"])
    pdf = str(p if p.is_absolute() else (H.ROOT / p))
    return F.extract_block(pdf, sd["page_index"], sd["coords_px"],
                           sd["page_px"][0], sd["page_px"][1])


def run_pair(sa, sb):
    exA, exB = extract(sa), extract(sb)
    if not exA.segments or not exB.segments:
        return {"error": "no vector geometry on one side",
                "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments)}
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    seeds = {(0.0, 0.0), (base[0], base[1])}
    dx, dy, score = M.register(exA.segments, exB.segments, seeds)
    LA, LB, meta = L.layers(exA, exB)
    led = L.ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
    recs = led["records"]
    return {"n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
            "n_text_a": len(exA.texts), "n_text_b": len(exB.texts),
            "reg_offset": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
            "S_shared": round(meta["S_shared"], 3),
            "scalar": led["scalar"], "counts": led["counts"],
            "verdict_scalar_999": L.scalar_verdict(led, 0.999),
            "verdict_counts": L.counts_verdict(led),
            "n_records": led["n_records"], "n_records_interior": led["n_records_interior"],
            "changed_len_total": led["changed_len_total"],
            "clip_a": [round(v, 2) for v in clipA], "clip_b": [round(v, 2) for v in clipB],
            "records": [{"type": r["type"], "bbox_pt": [round(v, 2) for v in r["bbox_pt"]],
                         "change_len": round(r["change_len"], 2),
                         "len_lost": round(r["len_lost"], 2), "len_new": round(r["len_new"], 2),
                         "at_boundary": bool(r["at_boundary"]),
                         "objects_a": [{k: o[k] for k in ("object_id", "cls", "label",
                                                          "share_of_object")}
                                       for o in (r.get("objects_a") or [])[:4]],
                         "objects_b": [{k: o[k] for k in ("object_id", "cls", "label",
                                                          "share_of_object")}
                                       for o in (r.get("objects_b") or [])[:4]]}
                        for r in recs[:60]]}


def main():
    want = set(sys.argv[1:])
    cases = H.load("hyb_cf_cases.json")["cases"]
    out_path = H.ART / "hyb_armA_cf.jsonl"
    done = set()
    if out_path.exists() and not want:
        for line in open(out_path, encoding="utf-8"):
            done.add(json.loads(line)["case_id"])
    sink = open(out_path, "a", encoding="utf-8")
    for c in cases:
        cid = c["cand_id"]
        if (want and cid not in want) or (not want and cid in done):
            continue
        t0 = time.time()
        row = {"case_id": cid, "source": "CF", "cf_id": c["cf_id"], "cf_class": c["cf_class"],
               "mode": c["mode"], "truth": c["expected_verdict"],
               "discipline": c["carrier"]["discipline"],
               "change_bbox_pt": c.get("change_bbox_pt"),
               "manifest": c["manifest"]}
        try:
            row.update(run_pair(c["left"], c["right"]))
        except Exception as e:                                # noqa: BLE001
            row["error"] = repr(e)
        row["t_sec"] = round(time.time() - t0, 1)
        sink.write(json.dumps(row, ensure_ascii=False) + "\n")
        sink.flush()
        print(cid, row.get("n_records"), row.get("error", ""), row["t_sec"], flush=True)


if __name__ == "__main__":
    main()
