# -*- coding: utf-8 -*-
"""L2 [REAL] — the same three-mode matching ablation on the 33 benchmark pairs.

There is no hand-drawn object correspondence on real pairs, so the REFERENCE here is
the INK correspondence (grp_match: whole-segment endpoint match at 0.8 pt, then
nearest-parallel fallback, both sides at equal physical scale in PDF points),
restricted to the strict 1:1 class (best_share >= 0.95 AND partner_purity >= 0.95).

HONEST CAVEAT, stated in the report: that reference is itself geometric.  It is
independent of the object DESCRIPTOR and of the assignment algorithm (it works on raw
segments), but it is not independent of geometry as such.  It therefore CANNOT be used
to argue "geometry is enough" on its own; it can be used to measure how much a label
CHANGES the answer, which is the question of this probe.

Usage: lbl_l2_real.py [workers]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G
import grp_match as M

MAX_SEG = 60000
MAX_OBJ = 4000
THR = 0.95
MODES = ("geom", "geom_pos", "geom_pos_label")


def one(p):
    row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
           "classes": p["classes"], "expected": p["expected_verdict"]}
    try:
        sa, sb = p["side_a"], p["side_b"]
        exA = G.F.extract_block(str(L.ROOT / sa["pdf"]), sa["page_index"], sa["coords_px"],
                                sa["page_px"][0], sa["page_px"][1])
        exB = G.F.extract_block(str(L.ROOT / sb["pdf"]), sb["page_index"], sb["coords_px"],
                                sb["page_px"][0], sb["page_px"][1])
        if not exA.segments or not exB.segments:
            row["skip"] = "no vector geometry"; return row
        if max(len(exA.segments), len(exB.segments)) > MAX_SEG:
            row["skip"] = f"too heavy ({len(exA.segments)}/{len(exB.segments)})"; return row
        S = max(exA.S, exB.S)
        LA = G.layer_of(exA.segments, exA.texts, S_override=S)
        LB = G.layer_of(exB.segments, exB.texts, S_override=S)
        if max(len(LA.objects), len(LB.objects)) > MAX_OBJ:
            row["skip"] = f"too many objects ({len(LA.objects)}/{len(LB.objects)})"; return row
        cA, cB = exA.frame["clip_display"], exB.frame["clip_display"]
        base = (cA[0] - cB[0], cA[1] - cB[1])
        seed = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
        seeds = {(0.0, 0.0), base, (float(seed[0]), float(seed[1])),
                 (base[0] + float(seed[0]), base[1] + float(seed[1]))}
        dx, dy, sc = M.register(exA.segments, exB.segments, seeds)
        rows = M.churn_rows(LA, exA.segments, LB, exB.segments, (dx, dy))
        # reference: strict 1:1 ink correspondence
        ref = {}
        for r in rows:
            if r["n_partners"] and r["best_share"] >= THR and r["partner_purity"] >= THR:
                ref[r["o"]] = r["partner"]
        # objects whose ink has NO partner at all -> genuinely absent on side B
        ref_absent = {r["o"] for r in rows if r["n_partners"] == 0}
        la = L.object_labels(LA, exA.texts)
        lb = L.object_labels(LB, exB.texts)
        row.update({"n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects),
                    "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                    "S": round(S, 3), "reg_offset": [round(dx, 3), round(dy, 3)],
                    "reg_score": round(sc, 4),
                    "n_ref_1to1": len(ref), "n_ref_absent": len(ref_absent),
                    "ref_cover": round(len(ref) / max(len(LA.objects), 1), 4),
                    "unique_label_share_a": round(
                        sum(1 for m, u in la if m and u) / max(len(LA.objects), 1), 4),
                    "labelled_share_a": round(
                        sum(1 for m, _ in la if m) / max(len(LA.objects), 1), 4)})
        for cond, off in (("registered", (dx, dy)), ("raw", (0.0, 0.0))):
            for mode in MODES:
                m = L.match_objects(LA, LB, mode, S, labels_a=la, labels_b=lb, off=off)
                pairs = {ia: ib for ia, ib, _ in m["pairs"]}
                corr = sum(1 for ia, ib in ref.items() if pairs.get(ia) == ib)
                wrong = sum(1 for ia, ib in ref.items() if ia in pairs and pairs[ia] != ib)
                miss = sum(1 for ia in ref if ia not in pairs)
                # false REMOVED = ink says a partner exists, matcher says removed
                fr = miss
                # false pairing of an object whose ink has no partner at all
                fa = sum(1 for ia in ref_absent if ia in pairs)
                row[f"{cond}/{mode}"] = {
                    "n_matched": len(pairs),
                    "acc_on_ref": round(corr / max(len(ref), 1), 5),
                    "wrong_on_ref": wrong, "missed_on_ref": miss,
                    "false_removed_share": round(fr / max(len(ref), 1), 5),
                    "paired_though_ink_absent": fa,
                    "paired_though_ink_absent_share": round(fa / max(len(ref_absent), 1), 5)
                    if ref_absent else None,
                }
    except Exception as exc:
        import traceback
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["tb"] = traceback.format_exc()[-600:]
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return row


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    pairs = json.load(open(L.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    res = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futs = [exe.submit(one, p) for p in pairs]
        for i, f in enumerate(as_completed(futs)):
            r = f.result(); res.append(r)
            print(f"  {i+1}/{len(pairs)} {r['pair_id']} "
                  f"{r.get('skip') or r.get('error') or r.get('registered/geom_pos',{}).get('acc_on_ref')}",
                  flush=True)
    json.dump({"threshold_1to1": THR, "modes": MODES, "pairs": res},
              open(L.ART / "lbl_l2_real.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("done")


if __name__ == "__main__":
    main()
