# -*- coding: utf-8 -*-
"""Two arms the first grid did not have: re-packaging noise and the object-to-object
falsification comparator."""
from __future__ import annotations
import glob
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import loc_agg as A  # noqa: E402


def rows(pat):
    out = []
    for f in sorted(glob.glob(str(ART / "loc_runs" / pat))):
        for line in open(f, encoding="utf-8"):
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def main():
    ALL = [r for r in rows("repack_*.jsonl") if "score" in r]
    out = {}
    for mode in sorted({r["noise"] for r in ALL}):
        out[f"packaging_noise:{mode}"] = arm([r for r in ALL if r["noise"] == mode])
    out.update(naive_arm())
    json.dump(out, open(ART / "loc_arms.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


def arm(R):
    neg = [r for r in R if r["inst"] == "NEG"]
    a = {
        "n_rows": len(R), "n_carriers": len({r["block_id"] for r in R}),
        "n_disciplines": len({r["discipline"] for r in R}),
        "negatives": {
            "n": len(neg),
            "median_records": A.med([r["score"]["n_records"] for r in neg]),
            "max_records": max([r["score"]["n_records"] for r in neg] or [0]),
            "share_zero_records": A.rate([r["score"]["n_records"] == 0 for r in neg]),
            "median_ink_similarity": A.med([r["scalar"]["ink_similarity"] for r in neg]),
            "median_seg_ratio": A.med([r["n_seg_b"] / max(r["n_seg"], 1) for r in neg]),
            "median_obj_delta": A.med([r["n_obj_b"] - r["n_obj_a"] for r in neg]),
            "share_counts_say_changed": A.rate([r["verdict_counts"] != "NO_GRAPHIC_CHANGE"
                                                for r in neg]),
            "share_scalar_says_changed": A.rate([r["verdict_scalar_999"] != "NO_GRAPHIC_CHANGE"
                                                 for r in neg]),
        },
        "positives": {},
    }
    for inst in sorted({r["inst"] for r in R if r["inst"] != "NEG"}):
        sel = [r for r in R if r["inst"] == inst]
        a["positives"][inst] = {
            "n": len(sel),
            "L2": A.rate([r["score"]["L2_localised"] for r in sel]),
            "L4": A.rate([r["score"]["L4_right_object"] for r in sel]),
            "median_false_records": A.med([r["score"]["n_false_records"] for r in sel]),
        }

    return a


def naive_arm():
    N = [r for r in rows("naive_*.jsonl") if "ink_records" in r]
    out = {"object_to_object_falsification": {"n_rows": len(N),
                                              "n_carriers": len({r["block_id"] for r in N})}}
    for inst in sorted({r["inst"] for r in N}):
        for noise in sorted({r["noise"] for r in N}):
            sel = [r for r in N if r["inst"] == inst and r["noise"] == noise]
            if not sel:
                continue
            out["object_to_object_falsification"][f"{inst}|{noise}"] = {
                "n": len(sel),
                "ink_median_records": A.med([r["ink_records"] for r in sel]),
                "obj_median_records": A.med([r["obj_records"] for r in sel]),
                "obj_max_records": max(r["obj_records"] for r in sel),
                "ink_L2": A.rate([r["ink_L2"] for r in sel]),
                "obj_L2": A.rate([r["obj_L2"] for r in sel]),
                "obj_median_churn_share": A.med(
                    [(r["obj_removed"] + r["obj_added"]) /
                     max(r["obj_removed"] + r["obj_added"] + 2 * r["obj_matched"], 1)
                     for r in sel]),
                "median_t_ink": A.med([r["t_ink"] for r in sel]),
                "median_t_obj": A.med([r["t_obj"] for r in sel]),
            }
            if inst == "NEG":
                out["object_to_object_falsification"][f"{inst}|{noise}"]["share_obj_reports_change"] = \
                    A.rate([r["obj_records"] > 0 for r in sel])
                out["object_to_object_falsification"][f"{inst}|{noise}"]["share_ink_reports_change"] = \
                    A.rate([r["ink_records"] > 0 for r in sel])
    return out


if __name__ == "__main__":
    main()
