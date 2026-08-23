# -*- coding: utf-8 -*-
"""C4 — the ledger on the REAL benchmark pairs [REAL].

Registration is the two-stage translation search grp_g2_churn.py already measured
(equal physical scale, PDF points).  No counterfactual is involved: this is the
external-validity half of the probe.

    python probes/loc_c4_real.py [pair_id ...]
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G      # noqa: E402
import grp_match as M       # noqa: E402
import loc_common as L      # noqa: E402


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


def main():
    want = set(sys.argv[1:])
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    # incremental: every finished pair is appended immediately, so an interrupted run
    # (the first one was killed by the machine running out of memory) is resumable
    jl = ART / "loc_runs" / "real_rows.jsonl"
    done = set()
    if jl.exists():
        for line in open(jl, encoding="utf-8"):
            done.add(json.loads(line)["pair_id"])
    sink = open(jl, "a", encoding="utf-8")
    out = []
    for p in pairs:
        if want and p["pair_id"] not in want:
            continue
        if not want and p["pair_id"] in done:
            continue
        t0 = time.time()
        row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
               "classes": p["classes"], "expected": p["expected_verdict"],
               "expected_changed_objects": p.get("expected_changed_objects"),
               "label_confidence": p["label_confidence"],
               "human": p["human_expected_ru"]}
        try:
            exA, exB = side(p["side_a"]), side(p["side_b"])
            if not exA.segments or not exB.segments:
                raise RuntimeError("no vector geometry on one side")
            clipA = exA.frame["clip_display"]
            clipB = exB.frame["clip_display"]
            base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
            sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), (base[0], base[1]), (float(sd[0]), float(sd[1])),
                     (base[0] + float(sd[0]), base[1] + float(sd[1]))}
            dx, dy, score = M.register(exA.segments, exB.segments, seeds)
            LA, LB, meta = L.layers(exA, exB)
            led = L.ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
            recs = led["records"]
            row.update({
                "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                "reg_offset": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
                "S_a": round(meta["S_a"], 2), "S_b": round(meta["S_b"], 2),
                "S_shared": round(meta["S_shared"], 2),
                "scalar": led["scalar"], "counts": led["counts"],
                "verdict_scalar_999": L.scalar_verdict(led, 0.999),
                "verdict_scalar_9999": L.scalar_verdict(led, 0.9999),
                "verdict_counts": L.counts_verdict(led),
                "n_records": led["n_records"],
                "n_records_interior": led["n_records_interior"],
                "changed_len_total": led["changed_len_total"],
                "records_top": [{k: v for k, v in r.items() if k != "objects_b"}
                                for r in recs[:12]],
                # geometry of every record, so that a change of the reporting rule
                # (boundary attribution, threshold) is replayable offline
                "rec_all": [[r["type"], [round(v, 2) for v in r["bbox_pt"]],
                             round(r["change_len"], 2), 1 if r["at_boundary"] else 0,
                             round(r["len_lost"], 2), round(r["len_new"], 2)]
                            for r in recs[:400]],
                "clip_a": [round(v, 2) for v in exA.frame["clip_display"]],
                "clip_b": [round(v, 2) for v in exB.frame["clip_display"]],
                "rec_len_all": [round(r["change_len"], 2) for r in recs[:400]],
                "rec_len_interior": [round(r["change_len"], 2) for r in recs[:400]
                                     if not r["at_boundary"]],
                "t_sec": round(time.time() - t0, 1),
            })
        except Exception as e:
            row["error"] = repr(e)
        print(row["pair_id"], row.get("n_records"), row.get("error", ""), flush=True)
        sink.write(json.dumps(row, ensure_ascii=False) + "\n")
        sink.flush()
        out.append(row)
    prev = {}
    fp = ART / "loc_real_pairs.json"
    if fp.exists():
        prev = {r["pair_id"]: r for r in json.load(open(fp, encoding="utf-8"))["pairs"]}
    for line in open(jl, encoding="utf-8"):
        r = json.loads(line)
        prev[r["pair_id"]] = r
    for r in out:
        prev[r["pair_id"]] = r
    json.dump({"note": "cross-revision pairs only; the corpus has no P->RD pairs (probe pd)",
               "pairs": list(prev.values())},
              open(fp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
