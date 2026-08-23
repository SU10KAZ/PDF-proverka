# -*- coding: utf-8 -*-
"""`hyb` — сведение трёх рук: точность, ЛОЖНЫЕ ГРАФИЧЕСКИЕ ИЗМЕНЕНИЯ, recall локального
изменения (и правильный ли объект назван), токены.

Рука A: реестр объектов (loc_common.ledger) + правило отчёта loc L5/L6
        (внутренняя запись длиной >= 60 pt -> GRAPHIC_CHANGE).
Рука B: Vision на весь блок, одна пара изображений, общий вопрос (контроль).
Рука C: A + гейт vis S5 (бесплатный пиксельный предфильтр -> длинная запись отвечает
        вектором -> короткая выжившая идёт в точечный Vision).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H          # noqa: E402

GC, NC = "GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE"


def est_tokens(text: str) -> float:
    """Грубая оценка токенов текстового payload: ASCII ~4 симв/токен, кириллица ~1.5."""
    a = sum(1 for c in text if ord(c) < 128)
    return round(a / 4.0 + (len(text) - a) / 1.5, 1)


def payload(records):
    """То, что детерминированный слой отдаёт дальше по конвейеру."""
    out = [{"type": r["type"], "bbox_pt": r["bbox_pt"], "change_len": r["change_len"],
            "objects": [o.get("label") or o.get("cls") for o in (r.get("objects_a") or [])[:3]]}
           for r in records]
    return json.dumps(out, ensure_ascii=False)


def main():
    vmap = {v["case_id"]: v for v in H.load("hyb_map.json")["views"]}
    bans = {a["view_id"]: a for a in H.load("hyb_armB.json")["answers"]}
    view_by_id = {v["view_id"]: v for v in H.load("hyb_map.json")["views"]}
    cans = {a["win_id"]: a for a in H.load("hyb_armC_vision.json")["answers"]}
    wmap = {w["win_id"]: w for w in H.load("hyb_win_map.json")["windows"]}
    gate = {g["case_id"]: g for g in H.load("hyb_gate.json")["cases"]}
    cfman = {c["cand_id"]: c for c in H.load("hyb_cf_cases.json")["cases"]}
    loc = {r["pair_id"]: r for r in H.load("loc_real_pairs.json")["pairs"]}
    cfA = {r["case_id"]: r for r in (json.loads(l) for l in
                                     open(H.ART / "hyb_armA_cf.jsonl", encoding="utf-8"))}
    mine = {p["pair_id"]: p for p in H.load("mine_pairs.json")["pairs"]}

    # window answer per (case_id, record ix)
    win_ans = {}
    for wid, w in wmap.items():
        win_ans[(w["case_id"], w["ix"])] = cans[wid]["verdict"]

    rows = []
    for cid, v in vmap.items():
        src = v["source"]
        truth = v["truth"]
        g = gate.get(cid, {})
        # ---------------- arm A
        if src == "REAL":
            r = loc.get(cid, {})
            err = r.get("error")
            recs = [x for x in (r.get("records_top") or []) if not x["at_boundary"]]
            all_int = r.get("rec_len_interior") or []
        else:
            r = cfA.get(cid, {})
            err = r.get("error")
            recs = [x for x in (r.get("records") or []) if not x["at_boundary"]]
            all_int = [x["change_len"] for x in (r.get("records") or []) if not x["at_boundary"]]
        a_fires = (not err) and any(L >= H.T_RECORD_PT for L in all_int)
        a_verdict = GC if a_fires else NC
        a_tokens = est_tokens(payload(recs[:12]))
        # ---------------- arm B
        b = bans[v["view_id"]]
        b_verdict = b["verdict"]
        b_tokens = v["tokens_pair_1500px"]
        # ---------------- arm C
        c_fires = False
        c_vis_tokens = 0.0
        c_calls = 0
        c_named = []
        for item in g.get("records", []):
            if item["decision"] == "prefilter_SAME":
                continue
            if item["decision"] == "vector_answers":
                c_fires = True
                c_named.append({"src": "vector", "bbox": item["bbox_pt"],
                                "len": item["change_len"]})
                continue
            ans = win_ans.get((cid, item["ix"]))
            c_calls += 1
            w = next((w for w in wmap.values()
                      if w["case_id"] == cid and w["ix"] == item["ix"]), None)
            if w and w.get("tokens"):
                c_vis_tokens += w["tokens"]
            if ans == "DIFFERENT":
                c_fires = True
                c_named.append({"src": "vision", "bbox": item["bbox_pt"],
                                "len": item["change_len"]})
        vc = bool(g.get("vision_calls"))
        if vc:                                   # V-C: тот же вход, что у руки B
            c_calls += 1
            c_vis_tokens += b_tokens
            if b_verdict == GC:
                c_fires = True
        c_verdict = GC if c_fires else NC
        c_tokens = round(a_tokens + c_vis_tokens, 1)
        # ---------------- ground truth extras
        man = cfman.get(cid, {}).get("manifest") if src == "CF" else None
        chg = cfman.get(cid, {}).get("change_bbox_pt") if src == "CF" else None
        rows.append({
            "case_id": cid, "source": src, "truth": truth,
            "view_id": v["view_id"], "swapped": v["swapped"],
            "cf_id": (v["meta"].get("cf_id") if src == "CF" else None),
            "cf_class": (v["meta"].get("cf_class") if src == "CF" else None),
            "classes": (v["meta"].get("classes") if src == "REAL" else None),
            "label_confidence": (v["meta"].get("label_confidence") if src == "REAL" else None),
            "discipline": v["meta"].get("discipline"),
            "armA": {"verdict": a_verdict, "error": err, "n_interior": len(all_int),
                     "max_len": round(max(all_int), 2) if all_int else 0.0,
                     "tokens": a_tokens, "sim": (r.get("scalar") or {}).get("ink_similarity")},
            "armB": {"verdict": b_verdict, "conf": b["conf"], "what": b["what_ru"],
                     "tokens": b_tokens},
            "armC": {"verdict": c_verdict, "vision_calls": c_calls,
                     "vision_tokens": round(c_vis_tokens, 1), "tokens": c_tokens,
                     "named": c_named, "gate_records": len(g.get("records", [])),
                     "prefiltered": sum(1 for i in g.get("records", [])
                                        if i["decision"] == "prefilter_SAME")},
            "change_bbox_pt": chg,
            "expected_changed_objects": (mine.get(cid, {}).get("expected_changed_objects")
                                         if src == "REAL" else None),
        })

    def stats(rows, arm):
        n = len(rows)
        acc = sum(1 for r in rows if r[arm]["verdict"] == r["truth"]) / n
        fp = sum(1 for r in rows if r["truth"] == NC and r[arm]["verdict"] == GC)
        fn = sum(1 for r in rows if r["truth"] == GC and r[arm]["verdict"] == NC)
        neg = sum(1 for r in rows if r["truth"] == NC)
        pos = n - neg
        tok = sum(r[arm]["tokens"] for r in rows)
        return {"n": n, "acc": round(acc, 3), "false_graphic_changes": fp,
                "fp_rate_on_negatives": round(fp / neg, 3) if neg else None,
                "missed": fn, "recall": round((pos - fn) / pos, 3) if pos else None,
                "tokens_total": round(tok, 1), "tokens_per_case": round(tok / n, 1)}

    out = {"n_cases": len(rows),
           "truth_balance": {"GRAPHIC_CHANGE": sum(1 for r in rows if r["truth"] == GC),
                             "NO_GRAPHIC_CHANGE": sum(1 for r in rows if r["truth"] == NC)},
           "overall": {a: stats(rows, a) for a in ("armA", "armB", "armC")},
           "by_source": {s: {a: stats([r for r in rows if r["source"] == s], a)
                             for a in ("armA", "armB", "armC")}
                         for s in ("REAL", "CF")},
           "rows": rows}
    H.dump(out, "hyb_results.json")
    for k, v in out["overall"].items():
        print(k, v)
    for s in ("REAL", "CF"):
        print("--", s)
        for k, v in out["by_source"][s].items():
            print("  ", k, v)


if __name__ == "__main__":
    main()
