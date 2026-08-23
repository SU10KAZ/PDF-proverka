# -*- coding: utf-8 -*-
"""`hyb` — итоговые таблицы: три руки x четыре метрики, локальный recall, разбор."""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import hyb_common as H          # noqa: E402

GC, NC = "GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE"
PAD = 1.0


def ov(a, b, pad=PAD):
    return not (a[2] + pad < b[0] or b[2] + pad < a[0] or a[3] + pad < b[1] or b[3] + pad < a[1])


def stats(rows, get):
    n = len(rows)
    if not n:
        return None
    acc = sum(1 for r in rows if get(r)["verdict"] == r["truth"]) / n
    neg = [r for r in rows if r["truth"] == NC]
    pos = [r for r in rows if r["truth"] == GC]
    fp = sum(1 for r in neg if get(r)["verdict"] == GC)
    tp = sum(1 for r in pos if get(r)["verdict"] == GC)
    tok = sum(get(r)["tokens"] for r in rows)
    return {"n": n, "accuracy": round(acc, 3),
            "false_graphic_changes": fp,
            "fp_rate": round(fp / len(neg), 3) if neg else None,
            "recall": round(tp / len(pos), 3) if pos else None,
            "tokens_total": round(tok, 1), "tokens_per_case": round(tok / n, 1)}


def main():
    R = json.load(open(H.ART / "hyb_results.json", encoding="utf-8"))["rows"]
    arb = {r["case_id"]: r for r in H.load("hyb_arbiter.json")["rows"]}
    direct = {r["case_id"]: r for r in H.load("hyb_armA_direct.json")["rows"]}
    gate = {g["case_id"]: g for g in H.load("hyb_gate.json")["cases"]}
    cf = {c["cand_id"]: c for c in H.load("hyb_cf_cases.json")["cases"]}

    for r in R:
        r["structural"] = arb[r["case_id"]]["structural"]
        d = direct.get(r["case_id"])
        if d and "error" not in d:
            r["armA_direct"] = {"verdict": d["verdict"], "tokens": r["armA"]["tokens"],
                                "max_len": d["max_interior_len"], "records": d["records"]}
        else:
            r["armA_direct"] = dict(r["armA"])

    arms = {"armA": lambda r: r["armA"], "armB": lambda r: r["armB"],
            "armC": lambda r: r["armC"], "armA_direct": lambda r: r["armA_direct"]}

    out = {"n_cases": len(R),
           "truth": {"GC": sum(1 for r in R if r["truth"] == GC),
                     "NO": sum(1 for r in R if r["truth"] == NC)},
           "overall": {k: stats(R, f) for k, f in arms.items()},
           "REAL": {k: stats([r for r in R if r["source"] == "REAL"], f) for k, f in arms.items()},
           "CF": {k: stats([r for r in R if r["source"] == "CF"], f) for k, f in arms.items()}}

    # ---- две пары бенчмарка, чью разметку опровергли прошлые зонды (loc L7, vis V-6)
    disputed = {"EOM-7fef43a3", "AR-441907a2"}
    sub = [r for r in R if r["case_id"] not in disputed]
    out["overall_without_disputed_labels"] = {k: stats(sub, f) for k, f in arms.items()}

    # ---- локальные изменения: класс C + D7 (контрфакты) и small_local_change (реальные)
    loc_rows = [r for r in R
                if (r["source"] == "CF" and (r["cf_class"] == "C" or r["cf_id"] == "D7_dim_geometry"))
                or (r["classes"] and "small_local_change" in r["classes"])]
    det = []
    for r in loc_rows:
        chg = r["change_bbox_pt"]
        namedA = namedC = None
        if chg:
            recsA = (direct.get(r["case_id"], {}) or {}).get("records") or []
            namedA = any(ov(x["bbox_pt"], chg) for x in recsA
                         if not x["at_boundary"] and x["change_len"] >= H.T_RECORD_PT)
            namedC = any(ov(x["bbox"], chg) for x in r["armC"]["named"])
        det.append({"case_id": r["case_id"], "cf_id": r["cf_id"], "truth": r["truth"],
                    "structural": r["structural"],
                    "A": r["armA"]["verdict"] == GC, "A_direct": r["armA_direct"]["verdict"] == GC,
                    "B": r["armB"]["verdict"] == GC, "C": r["armC"]["verdict"] == GC,
                    "A_right_object": namedA, "C_right_object": namedC,
                    "B_text": r["armB"]["what"]})
    out["local_change"] = {
        "n": len(det),
        "recall": {a: round(sum(1 for d in det if d[a]) / len(det), 3)
                   for a in ("A", "A_direct", "B", "C")},
        "right_object": {"A_direct": round(sum(1 for d in det if d["A_right_object"]) / len(det), 3),
                         "C": round(sum(1 for d in det if d["C_right_object"]) / len(det), 3)},
        "rows": det}

    # ---- гейт: сколько записей, во что обошёлся
    recs = [i for g in gate.values() for i in g["records"]]
    out["gate"] = {
        "n_interior_records": len(recs),
        "prefilter_SAME": sum(1 for i in recs if i["decision"] == "prefilter_SAME"),
        "vector_answers": sum(1 for i in recs if i["decision"] == "vector_answers"),
        "vision_windows": sum(1 for i in recs if i["decision"] == "vision_window"),
        "whole_block_calls": sum(1 for g in gate.values() if g["vision_calls"]),
        "cases_with_any_vision": sum(1 for r in R if r["armC"]["vision_calls"]),
        "vision_tokens_total": round(sum(r["armC"]["vision_tokens"] for r in R), 1),
    }

    # ---- окупаемость руки C против руки A
    flips = [{"case_id": r["case_id"], "A": r["armA"]["verdict"], "C": r["armC"]["verdict"],
              "truth": r["truth"], "vision_tokens": r["armC"]["vision_tokens"],
              "gain": (r["armC"]["verdict"] == r["truth"]) - (r["armA"]["verdict"] == r["truth"])}
             for r in R if r["armA"]["verdict"] != r["armC"]["verdict"]]
    gained = sum(1 for f in flips if f["gain"] > 0)
    lost = sum(1 for f in flips if f["gain"] < 0)
    vis_tok = sum(r["armC"]["vision_tokens"] for r in R)
    out["C_vs_A"] = {"flips": flips, "cases_fixed": gained, "cases_broken": lost,
                     "net_cases": gained - lost,
                     "vision_tokens_spent": round(vis_tok, 1),
                     "tokens_per_net_case": round(vis_tok / (gained - lost), 1)
                     if gained - lost else None,
                     "delta_accuracy": round((out['overall']['armC']['accuracy']
                                              - out['overall']['armA']['accuracy']), 3)}
    H.dump(out, "hyb_summary.json")
    print(json.dumps({k: out[k] for k in
                      ("truth", "overall", "REAL", "CF", "overall_without_disputed_labels",
                       "gate", "C_vs_A")}, ensure_ascii=False, indent=1)[:4000])
    print("LOCAL:", json.dumps(out["local_change"]["recall"], ensure_ascii=False),
          json.dumps(out["local_change"]["right_object"], ensure_ascii=False),
          "n=", out["local_change"]["n"])


if __name__ == "__main__":
    main()
