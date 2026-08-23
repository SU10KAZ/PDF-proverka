# -*- coding: utf-8 -*-
"""`hyb` ARM C — vector object diff + pointed Vision через гейт vis_FINDINGS (S5).

Шаг 0  предфильтр: окно каждой ВНУТРЕННЕЙ записи реестра рендерится с обеих сторон,
       structural < 0.001  ->  SAME, Vision не вызывается (0 токенов).
Шаг 1  пережившая запись: change_len >= 60 pt -> GRAPHIC_CHANGE публикует ВЕКТОР;
       change_len <  60 pt -> V-A: вызов Vision на окно записи.
Шаг 2  V-C: блок без пригодного вектора -> Vision на весь блок ("тот же ли чертёж").
       V-D: выравнивание отказало   -> Vision на весь блок.

Окна пишутся под непрозрачными id в перемешанном порядке (hyb_win_map.json), чтобы
ответы Vision руки C выносились так же вслепую, как ответы руки B.

    python3 probes/hyb_gate.py
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H          # noqa: E402
import vis_common as V          # noqa: E402

SEED = 5150077
PAD_FRAC = 0.18
PAD_MIN = 6.0
MIN_SIDE = 26.0
TOP_RECORDS = 12


def pad_window(bbox):
    x0, y0, x1, y1 = bbox
    long_side = max(x1 - x0, y1 - y0)
    pad = max(PAD_MIN, PAD_FRAC * long_side)
    w = V.pad_rect([x0, y0, x1, y1], pad, MIN_SIDE)
    return V.square_rect(w)


def side_for_render(sd):
    p = Path(sd["pdf"])
    pdf = str(p if p.is_absolute() else (H.ROOT / p))
    return {"pdf": pdf, "page_index": sd["page_index"], "page_px": sd["page_px"]}


def real_rows():
    pairs = {p["pair_id"]: p for p in H.load("mine_pairs.json")["pairs"]}
    loc = {r["pair_id"]: r for r in H.load("loc_real_pairs.json")["pairs"]}
    out = []
    for pid, p in pairs.items():
        if p["expected_verdict"] not in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE"):
            continue
        r = loc.get(pid, {})
        recs = [rec for rec in (r.get("records_top") or []) if not rec["at_boundary"]]
        out.append({
            "case_id": pid, "source": "REAL", "truth": p["expected_verdict"],
            "left": {"pdf": p["side_a"]["pdf"], "page_index": p["side_a"]["page_index"],
                     "coords_px": p["side_a"]["coords_px"], "page_px": p["side_a"]["page_px"]},
            "right": {"pdf": p["side_b"]["pdf"], "page_index": p["side_b"]["page_index"],
                      "coords_px": p["side_b"]["coords_px"], "page_px": p["side_b"]["page_px"]},
            "records": recs[:TOP_RECORDS],
            "armA_error": r.get("error"),
            "reg_score": r.get("reg_score"),
            "n_records_interior": r.get("n_records_interior"),
            "clip_a": r.get("clip_a"), "clip_b": r.get("clip_b"),
        })
    return out


def cf_rows():
    out = []
    for r in (json.loads(l) for l in open(H.ART / "hyb_armA_cf.jsonl", encoding="utf-8")):
        recs = [rec for rec in (r.get("records") or []) if not rec["at_boundary"]]
        cases = {c["cand_id"]: c for c in H.load("hyb_cf_cases.json")["cases"]}
        c = cases[r["case_id"]]
        out.append({
            "case_id": r["case_id"], "source": "CF", "truth": r["truth"],
            "left": c["left"], "right": c["right"],
            "records": recs[:TOP_RECORDS],
            "armA_error": r.get("error"),
            "reg_score": r.get("reg_score"),
            "n_records_interior": r.get("n_records_interior"),
        })
    return out


def main():
    rows = real_rows() + cf_rows()
    H.WIN_DIR.mkdir(parents=True, exist_ok=True)
    tasks = []            # windows that actually need a Vision answer
    plan = []
    for row in rows:
        gate = {"case_id": row["case_id"], "source": row["source"], "truth": row["truth"],
                "records": [], "vision_calls": [], "vector_says": None,
                "reason": None}
        if row["armA_error"]:
            gate["reason"] = "V-C: вектор-ответа нет (no vector geometry on one side)"
            gate["vision_calls"].append({"kind": "whole_block"})
            plan.append(gate)
            continue
        for i, rec in enumerate(row["records"]):
            win = pad_window(rec["bbox_pt"])
            base = f"{row['case_id']}__r{i}"
            try:
                szL = V.render_region(side_for_render(row["left"]), win,
                                      H.WIN_DIR / f"_tmp_{base}_L.png", H.WINDOW_TARGET_PX)
                szR = V.render_region(side_for_render(row["right"]), win,
                                      H.WIN_DIR / f"_tmp_{base}_R.png", H.WINDOW_TARGET_PX)
                st = H.structural_diff(H.WIN_DIR / f"_tmp_{base}_L.png",
                                       H.WIN_DIR / f"_tmp_{base}_R.png")
            except Exception as e:                        # noqa: BLE001
                szL = szR = None
                st = 1.0
                print("WIN FAIL", base, repr(e))
            item = {"ix": i, "change_len": rec["change_len"], "type": rec["type"],
                    "bbox_pt": rec["bbox_pt"], "window_pt": [round(v, 2) for v in win],
                    "structural": round(st, 6), "px": [szL, szR],
                    "labels": [o.get("label") for o in (rec.get("objects_a") or [])][:3]}
            if st < H.PREFILTER_EPS:
                item["decision"] = "prefilter_SAME"
            elif rec["change_len"] >= H.T_RECORD_PT:
                item["decision"] = "vector_answers"
            else:
                item["decision"] = "vision_window"
                tasks.append({"base": base, "case_id": row["case_id"], "ix": i,
                              "tokens": round(H.image_tokens(*szL) + H.image_tokens(*szR), 1)
                                        if szL and szR else None})
            gate["records"].append(item)
        plan.append(gate)
    # anonymise + shuffle the windows that need an answer
    rng = random.Random(SEED)
    rng.shuffle(tasks)
    mapping = []
    for n, t in enumerate(tasks, 1):
        wid = f"w{n:02d}"
        for tag in ("L", "R"):
            src = H.WIN_DIR / f"_tmp_{t['base']}_{tag}.png"
            dst = H.WIN_DIR / f"{wid}_{1 if tag == 'L' else 2}.png"
            dst.write_bytes(src.read_bytes())
        mapping.append({"win_id": wid, **t})
    for p in H.WIN_DIR.glob("_tmp_*.png"):
        p.unlink()
    H.dump({"seed": SEED, "n_windows": len(mapping), "windows": mapping}, "hyb_win_map.json")
    H.dump({"eps": H.PREFILTER_EPS, "T_pt": H.T_RECORD_PT, "n_cases": len(plan),
            "cases": plan}, "hyb_gate.json")
    n_pre = sum(1 for g in plan for r in g["records"] if r["decision"] == "prefilter_SAME")
    n_vec = sum(1 for g in plan for r in g["records"] if r["decision"] == "vector_answers")
    print("records:", n_pre + n_vec + len(mapping), "prefilter_SAME:", n_pre,
          "vector_answers:", n_vec, "vision_windows:", len(mapping),
          "whole_block_calls:", sum(1 for g in plan if g["vision_calls"]))


if __name__ == "__main__":
    main()
