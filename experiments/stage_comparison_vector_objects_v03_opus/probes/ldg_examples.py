# -*- coding: utf-8 -*-
"""Real ledgers for the report: 2 real pairs with a change, 2 negative controls (the
ledger must be EMPTY) and 2 class-C counterfactuals.

    python probes/ldg_examples.py   ->  artifacts/ldg_ledger_examples.json
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import grp_match as M           # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
import ldg_ledger as LDG        # noqa: E402

REAL_CHANGE = ["AR-55eda7fb", "EOM-c50e2170"]
REAL_QUIET = ["AR-b38a7dbc", "SS-392b7bd3"]
CF_CASES = [("6WNV-DWDA-4CQ", "C9_add_branch", {}),
            ("blk_fafeb59601b84a839e0616f5fcda4f18", "C10_remove_opening", {}),
            ("7PJD-33QQ-W44", "C2x2", {})]


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


def real_case(p):
    exA, exB = side(p["side_a"]), side(p["side_b"])
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
    seeds = {(0.0, 0.0), base, (float(sd[0]), float(sd[1])),
             (base[0] + float(sd[0]), base[1] + float(sd[1]))}
    dx, dy, sc = M.register(exA.segments, exB.segments, seeds)
    LA, LB, meta = L.layers(exA, exB)
    ldg = LDG.build(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
    return ldg, {"registration_offset_pt": [round(dx, 3), round(dy, 3)],
                 "registration_score": round(sc, 4),
                 "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                 "S_pt": ldg["S"], "L_min_pt": ldg["L_min_pt"],
                 "ink_similarity": ldg["scalar"]["ink_similarity"]}


def main():
    pairs = {p["pair_id"]: p for p in
             json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]}
    out = {"note": "GraphicChangeLedger v0.3 (probe ldg). [REAL] = cross-revision pair of "
                   "prepared graphic blocks; the corpus has no P->RD pairs (probe pd). "
                   "[CF] = controlled counterfactual on a real prepared block.",
           "contract": {"record": ["type", "welded", "object_before", "object_after",
                                   "evidence[]"],
                        "evidence_kinds": sorted(LDG.GEOMETRIC_EVIDENCE),
                        "forbidden_as_evidence": ["text", "label", "ocr", "table cell value"]},
           "cases": []}
    for pid in REAL_CHANGE + REAL_QUIET:
        t0 = time.time()
        p = pairs[pid]
        ldg, meta = real_case(p)
        out["cases"].append({
            "case": pid, "source": "REAL", "discipline": p["discipline"],
            "classes": p["classes"], "expected_verdict": p["expected_verdict"],
            "human_label_ru": p["human_expected_ru"], "geometry": meta,
            "ledger": {"changes": ldg["changes"]},
            "phrases_ru": [q["text"] for q in LDG.phrases(ldg)],
            "validator_violations": LDG.validate(ldg),
            "t_sec": round(time.time() - t0, 2)})
        print("real", pid, len(ldg["changes"]), flush=True)
    import ldg_run as R
    carriers = {c["block_id"]: c for c in R.pick_carriers()}
    for blk, cf, kw in CF_CASES:
        t0 = time.time()
        r = carriers[blk]
        ex = G.extract(G.prepared_block(r["doc_id"], r["version"], blk))
        ol = O.build_objects(ex)
        if cf == "C2x2":
            ex2, man = R._c2_same_object(ex, ol)
        else:
            ex2, man = C.apply(ex, ol, cf, **kw)
        exB = L.noisy(ex2, "round025", seed=20260823)      # export rewrite on side B
        LA, LB, meta = L.layers(ex, exB)
        ldg = LDG.build(ex, exB, LA=LA, LB=LB, meta=meta)
        out["cases"].append({
            "case": f"{blk}|{man['cf_id']}", "source": "CF", "discipline": r["discipline"],
            "cf_id": man["cf_id"], "cf_class": man["cf_class"],
            "export_noise_on_B": "A6_round_0.25",
            "ground_truth": {"change_bbox_pt": man.get("change_bbox_pt"),
                             "change_bboxes_pt": man.get("change_bboxes_pt"),
                             "expected_ledger": man.get("expected_ledger"),
                             "params": man.get("params")},
            "geometry": {"n_seg_a": len(ex.segments), "n_seg_b": len(exB.segments),
                         "S_pt": ldg["S"], "L_min_pt": ldg["L_min_pt"],
                         "ink_similarity": ldg["scalar"]["ink_similarity"]},
            "ledger": {"changes": ldg["changes"]},
            "phrases_ru": [q["text"] for q in LDG.phrases(ldg)],
            "validator_violations": LDG.validate(ldg),
            "t_sec": round(time.time() - t0, 2)})
        print("cf", blk, cf, len(ldg["changes"]), flush=True)
    json.dump(out, open(ART / "ldg_ledger_examples.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("written", ART / "ldg_ledger_examples.json")


if __name__ == "__main__":
    main()
