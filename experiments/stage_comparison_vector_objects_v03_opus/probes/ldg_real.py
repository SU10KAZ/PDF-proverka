# -*- coding: utf-8 -*-
"""[REAL] driver for probe `ldg`: the ledger contract and the phrases on the benchmark.

    python probes/ldg_real.py <shard> <nshards>

Registration is the same two-stage translation search `loc_c4_real.py` used, so the only
difference against the `loc` numbers is what is written down, not what is compared.
"""
from __future__ import annotations
import json
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G      # noqa: E402
import grp_match as M       # noqa: E402
import loc_common as L      # noqa: E402
import ldg_ledger as LDG    # noqa: E402


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    outdir = ART / "ldg_runs"
    outdir.mkdir(exist_ok=True)
    out = open(outdir / f"real_{shard}.jsonl", "w", encoding="utf-8")
    for i, p in enumerate(pairs):
        if i % nsh != shard:
            continue
        t0 = time.time()
        row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
               "classes": p["classes"], "expected": p["expected_verdict"],
               "expected_changed_objects": p.get("expected_changed_objects"),
               "label_confidence": p["label_confidence"], "human": p["human_expected_ru"]}
        try:
            exA, exB = side(p["side_a"]), side(p["side_b"])
            if not exA.segments or not exB.segments:
                raise RuntimeError("no vector geometry on one side")
            clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
            base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
            sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), (base[0], base[1]), (float(sd[0]), float(sd[1])),
                     (base[0] + float(sd[0]), base[1] + float(sd[1]))}
            dx, dy, score = M.register(exA.segments, exB.segments, seeds)
            LA, LB, meta = L.layers(exA, exB)
            raw = LDG.raw_ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
            ldg = LDG.build(exA, exB, off=(dx, dy), led=raw)
            low = LDG.build(exA, exB, off=(dx, dy), led=raw,
                            min_change_len_pt=2.0 * raw["S"])
            row.update({
                "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                "reg_offset": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
                "S": ldg["S"], "L_min_pt": ldg["L_min_pt"],
                "scalar": raw["scalar"]["ink_similarity"],
                "lost_share": raw["scalar"]["unmatched_share_a"],
                "n_records_raw": raw["n_records"],
                "n_changes": len(ldg["changes"]),
                "n_changes_low": len(low["changes"]),
                "types": [c["type"] for c in ldg["changes"]][:60],
                "shapes": [c.get("shape") for c in ldg["changes"]][:60],
                "changes": [{"type": c["type"], "shape": c.get("shape"),
                             "bbox": [round(v, 2) for v in f["_bbox"]],
                             "len": round(f["_len"], 2),
                             "obj_b": (c["object_before"] or {}).get("object_id"),
                             "obj_a": (c["object_after"] or {}).get("object_id"),
                             "cls_b": (c["object_before"] or {}).get("cls"),
                             "cls_a": (c["object_after"] or {}).get("cls"),
                             "label_b": (c["object_before"] or {}).get("label"),
                             "label_a": (c["object_after"] or {}).get("label"),
                             "welded": c.get("welded"),
                             "contact": next((e for e in c["evidence"]
                                              if e["kind"] == "contact"), None),
                             "ev": [e["kind"] for e in c["evidence"]],
                             "att": next((e for e in c["evidence"]
                                          if e["kind"] == "attachment"), None)}
                            for c, f in zip(ldg["changes"], ldg["_full"])][:60],
                "phrases": [{"id": q["id"], "text": q["text"], "n": q["n"],
                             "ink_pt": q["ink_pt"]} for q in LDG.phrases(ldg)],
                "phrases_low": [{"id": q["id"], "text": q["text"], "n": q["n"]}
                                for q in LDG.phrases(low)],
                "validate_violations": LDG.validate(ldg),
                "ledger_json": {"changes": ldg["changes"][:12]},
                "t_sec": round(time.time() - t0, 2)})
        except Exception as e:
            row["error"] = repr(e)
            row["tb"] = traceback.format_exc()[-300:]
        out.write(json.dumps(row, ensure_ascii=False) + "\n")
        out.flush()
        print("done", p["pair_id"], row.get("n_changes"), round(time.time() - t0, 1), flush=True)
    out.close()


if __name__ == "__main__":
    main()
