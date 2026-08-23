# -*- coding: utf-8 -*-
"""F3 [REAL] — how many FALSE "the count changed" statements does the family layer make?

This is the direct analogue of the v0.2 measurement that killed `repeated_elements`:
169 fabricated "count changed" statements on 7 quiet pairs (v0.2 H9), 6 of 8 unchanged
controls firing, tolerant two-pass firing on 5 of 8 (v0.2 P18/P19).

Populations (both REAL, both published separately):
  L  hand-labelled  — the 14 pairs of `mine_pairs.json` whose eye verdict is
                      NO_GRAPHIC_CHANGE (unchanged / different packaging / crop
                      boundary / block moved / text only / table only);
  Q  raster-quiet   — pairs of `mine_align2.jsonl` with NO large residual component
                      inside the block at equal physical scale (n_components_big == 0)
                      and different PDF files.  Weaker ground truth, more pairs,
                      more disciplines.

Variants measured for every pair (each is a separate architectural choice):
  mode   twopass | greedy      (v0.2 P19: greedy is order dependent)
  S      shared  | own         (v0.3 G2b: per-block S is a proven defect)
  scope  interior | all        (mine M5: the crop border invents "added objects")
Usage: fam_f3_false.py [out.json]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import fam_common as C
import fam_family as FAM

MAX_SEG = 60000
PAD_FRAC = 0.02
FLOORS = [0.0, 0.5, 1.0, 2.0, 4.0, 8.0]


def _side_from_align(r, tag):
    return {"pdf": r[f"pdf_{tag}"], "page_index": r[f"page_index_{tag}"],
            "coords_px": r[f"coords_{tag}"], "page_px": r[f"page_px_{tag}"],
            "block_id": r[f"block_{tag}"], "version": r[f"ver_{tag}"]}


def load_population():
    pairs = []
    seen = set()
    lab = json.load(open(G.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    for p in lab:
        if p["expected_verdict"] != "NO_GRAPHIC_CHANGE":
            continue
        key = (p["side_a"]["block_id"], p["side_b"]["block_id"])
        seen.add(key)
        pairs.append({"pair_id": p["pair_id"], "pop": "L", "discipline": p["discipline"],
                      "classes": p["classes"], "doc_id": p["doc_id"],
                      "side_a": p["side_a"], "side_b": p["side_b"],
                      "label_confidence": p["label_confidence"]})
    for line in open(G.ART / "mine_align2.jsonl", encoding="utf-8"):
        r = json.loads(line)
        al = r.get("align") or {}
        if al.get("n_components_big", 1) != 0 or r.get("same_pdf"):
            continue
        key = (r["block_a"], r["block_b"])
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"pair_id": f"Q-{r['discipline']}-{r['block_a'][:8]}", "pop": "Q",
                      "discipline": r["discipline"], "classes": [r.get("cat_a") or "default"],
                      "doc_id": r["doc_id"],
                      "side_a": _side_from_align(r, "a"), "side_b": _side_from_align(r, "b"),
                      "diff_frac": al.get("diff_frac_block_al"),
                      "label_confidence": "raster"})
    return pairs


def _inside(bbox, reg, pad):
    return (bbox[0] >= reg[0] + pad and bbox[1] >= reg[1] + pad and
            bbox[2] <= reg[2] - pad and bbox[3] <= reg[3] - pad)


def one_pair(p):
    t0 = time.time()
    row = {k: p[k] for k in ("pair_id", "pop", "discipline", "classes", "doc_id",
                             "label_confidence")}
    exA = G.F.extract_block(str(G.ROOT / p["side_a"]["pdf"]), p["side_a"]["page_index"],
                            p["side_a"]["coords_px"], *p["side_a"]["page_px"])
    exB = G.F.extract_block(str(G.ROOT / p["side_b"]["pdf"]), p["side_b"]["page_index"],
                            p["side_b"]["coords_px"], *p["side_b"]["page_px"])
    if not exA.segments or not exB.segments:
        return dict(row, error="no vector geometry")
    if max(len(exA.segments), len(exB.segments)) > MAX_SEG:
        return dict(row, error=f"too big {len(exA.segments)}/{len(exB.segments)}")
    clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
    base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
    seed = (p.get("screen") or [0.0, 0.0])
    seeds = {(0.0, 0.0), base, (base[0], base[1])}
    dx, dy, score = M.register(exA.segments, exB.segments, seeds)
    row.update({"n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                "reg_offset": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4)})
    # common interior region, in side-A page points
    regB = (clipB[0] + dx, clipB[1] + dy, clipB[2] + dx, clipB[3] + dy)
    reg = (max(clipA[0], regB[0]), max(clipA[1], regB[1]),
           min(clipA[2], regB[2]), min(clipA[3], regB[3]))
    pad = max(2.0, PAD_FRAC * min(reg[2] - reg[0], reg[3] - reg[1]))

    layers = {}
    for S_mode in ("own", "shared"):
        if S_mode == "own":
            LA = G.layer_of(exA.segments, exA.texts)
            LB = G.layer_of(exB.segments, exB.texts)
        else:
            S = max(layers["own"][0].S, layers["own"][1].S)
            LA = G.layer_of(exA.segments, exA.texts, S_override=S)
            LB = G.layer_of(exB.segments, exB.texts, S_override=S)
            row["S_shared"] = round(S, 3)
        layers[S_mode] = (LA, LB)
    row["S_a"], row["S_b"] = round(layers["own"][0].S, 3), round(layers["own"][1].S, 3)
    row["scale_src_a"] = layers["own"][0].scale_source
    row["scale_src_b"] = layers["own"][1].scale_source

    res = {}
    for S_mode, (LA, LB) in layers.items():
        oa_all, ob_all = LA.objects, LB.objects
        oa_in = [o for o in oa_all if _inside(o["bbox"], reg, pad)]
        ob_in = [o for o in ob_all
                 if _inside([o["bbox"][0] + dx, o["bbox"][1] + dy,
                             o["bbox"][2] + dx, o["bbox"][3] + dy], reg, pad)]
        for scope, (oa, ob) in (("all", (oa_all, ob_all)), ("interior", (oa_in, ob_in))):
            for mode in ("twopass", "greedy", "greedy_input"):
                FP = FAM.build_families_pair(oa, ob, mode=mode)
                rows2 = FAM.family_deltas(FP, min_family=2)
                rows3 = FAM.family_deltas(FP, min_family=3)
                res[f"{S_mode}|{scope}|{mode}"] = {
                    "n_obj_a": len(oa), "n_obj_b": len(ob),
                    "n_families": len(FP.families),
                    "n_repeated": sum(1 for f in FP.families if len(f["members"]) >= 2),
                    "false_rows_min2": len(rows2),
                    "false_rows_min3": len(rows3),
                    "false_delta_sum": sum(abs(r["delta"]) for r in rows2),
                    "false_delta_max": max([abs(r["delta"]) for r in rows2], default=0),
                    "top": [{"n_a": r["n_a"], "n_b": r["n_b"], "cls": r["cls"],
                             "diag": r["diag_med"], "n_seg_med": r["n_seg_med"]}
                            for r in rows2[:6]],
                }
    # ---- noise-floor sweep, best configuration only ------------------------
    LA, LB = layers["shared"]
    oa_in = [o for o in LA.objects if _inside(o["bbox"], reg, pad)]
    ob_in = [o for o in LB.objects
             if _inside([o["bbox"][0] + dx, o["bbox"][1] + dy,
                         o["bbox"][2] + dx, o["bbox"][3] + dy], reg, pad)]
    sweep = {}
    FPr = FAM.build_families_pair(oa_in, ob_in)
    row["robust"] = {str(lf): {"rows": len(FAM.family_deltas_robust(FPr, 2, link_frac=lf)),
                               "rows_min3": len(FAM.family_deltas_robust(FPr, 3, link_frac=lf))}
                     for lf in (2.0, 4.0, 8.0)}
    row["robust"]["plain"] = {"rows": len(FAM.family_deltas(FPr, 2)),
                              "rows_min3": len(FAM.family_deltas(FPr, 3))}
    for floor in FLOORS:
        FP = FAM.build_families_pair(oa_in, ob_in, min_diag_pt=floor)
        r2 = FAM.family_deltas(FP, min_family=2)
        sweep[str(floor)] = {
            "false_rows_min2": len(r2),
            "false_rows_min3": len(FAM.family_deltas(FP, min_family=3)),
            "false_delta_sum": sum(abs(r["delta"]) for r in r2),
            "n_repeated": sum(1 for f in FP.families if len(f["members"]) >= 2),
            "n_below_floor": FP.stats["n_below_floor"],
            "n_obj": FP.stats["n_obj"],
        }
    row["floor_sweep"] = sweep
    row["variants"] = res
    row["t_sec"] = round(time.time() - t0, 1)
    return row


def _work(p):
    try:
        return one_pair(p)
    except Exception:
        return {"pair_id": p["pair_id"], "pop": p["pop"],
                "error": traceback.format_exc().splitlines()[-1]}


def main():
    import multiprocessing as mp
    out_path = sys.argv[1] if len(sys.argv) > 1 else str(G.ART / "fam_f3_false.json")
    pairs = load_population()
    print("population", len(pairs), flush=True)
    out = []
    with mp.Pool(4, maxtasksperchild=4) as pool:
        for r in pool.imap_unordered(_work, pairs, chunksize=1):
            out.append(r)
            v = (r.get("variants") or {}).get("shared|interior|twopass", {})
            print(len(out), r["pair_id"], r.get("pop"), r.get("error", ""),
                  "false", v.get("false_rows_min2"), "/", v.get("n_repeated"),
                  flush=True)
    json.dump({"populations": {"L": "eye-labelled NO_GRAPHIC_CHANGE (mine_pairs.json)",
                               "Q": "raster-quiet: no big residual component inside the block"},
               "pad_frac": PAD_FRAC, "pairs": out},
              open(out_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
