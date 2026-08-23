# -*- coding: utf-8 -*-
"""L5 — STABILITY OF THE OBJECT<->LABEL BINDING.

Arm REAL  [REAL]: 33 benchmark pairs.  Reference correspondence = ink correspondence
    (grp_match: both endpoints, 0.8 pt, at equal physical scale) restricted to the
    strict 1:1 class.  Question: does the SAME designation attach on both sides?
    NOTE the proxy: the reference itself is geometric, so this measures the label,
    not the geometry.

Arm CF   [CF]: the same question where the correspondence is exact (provenance),
    under a moved caption (D2), coordinate rounding (A6) and crop jitter (B3).

Arm RADIUS: how much the attached designation depends on the binding radius k*S.

Usage: lbl_l5_binding.py real|cf|radius [workers]
"""
from __future__ import annotations
import json, math, sys, time
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


def _mark_sets(layer, texts, ks=(1.0, 1.6, 2.5)):
    return {k: L.object_labels(layer, texts, k=k) for k in ks}


def real_pair(p):
    row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
           "classes": p["classes"], "expected": p["expected_verdict"]}
    try:
        sa, sb = p["side_a"], p["side_b"]
        exA = G.F.extract_block(str(L.ROOT / sa["pdf"]), sa["page_index"], sa["coords_px"],
                                sa["page_px"][0], sa["page_px"][1])
        exB = G.F.extract_block(str(L.ROOT / sb["pdf"]), sb["page_index"], sb["coords_px"],
                                sb["page_px"][0], sb["page_px"][1])
        if not exA.segments or not exB.segments:
            row["skip"] = "no vector geometry"
            return row
        if max(len(exA.segments), len(exB.segments)) > MAX_SEG:
            row["skip"] = f"too heavy ({len(exA.segments)}/{len(exB.segments)})"
            return row
        S = max(exA.S, exB.S)
        LA = G.layer_of(exA.segments, exA.texts, S_override=S)
        LB = G.layer_of(exB.segments, exB.texts, S_override=S)
        if max(len(LA.objects), len(LB.objects)) > MAX_OBJ:
            row["skip"] = f"too many objects ({len(LA.objects)}/{len(LB.objects)})"
            return row
        cA, cB = exA.frame["clip_display"], exB.frame["clip_display"]
        base = (cA[0] - cB[0], cA[1] - cB[1])
        seed = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
        seeds = {(0.0, 0.0), base, (float(seed[0]), float(seed[1])),
                 (base[0] + float(seed[0]), base[1] + float(seed[1]))}
        dx, dy, sc = M.register(exA.segments, exB.segments, seeds)
        rows = M.churn_rows(LA, exA.segments, LB, exB.segments, (dx, dy))
        la = L.object_labels(LA, exA.texts)
        lb = L.object_labels(LB, exB.texts)
        marks_b_all = {m for m, _ in lb if m}
        marks_a_all = {m for m, _ in la if m}
        st = {"both_same": 0, "both_diff": 0, "a_only": 0, "b_only": 0, "neither": 0}
        diff_examples = []
        rebind_to_other_a_mark = 0
        n11 = 0
        for r in rows:
            if r["n_partners"] == 0 or r["best_share"] < THR or r["partner_purity"] < THR:
                continue
            n11 += 1
            ia, ib = r["o"], r["partner"]
            ma, mb = la[ia][0], lb[ib][0]
            if ma and mb:
                if ma == mb:
                    st["both_same"] += 1
                else:
                    st["both_diff"] += 1
                    if mb in marks_a_all:
                        rebind_to_other_a_mark += 1
                    if len(diff_examples) < 8:
                        diff_examples.append([ma, mb])
            elif ma:
                st["a_only"] += 1
            elif mb:
                st["b_only"] += 1
            else:
                st["neither"] += 1
        both = st["both_same"] + st["both_diff"]
        row.update({
            "n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects),
            "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
            "S": round(S, 3), "reg_offset": [round(dx, 3), round(dy, 3)],
            "reg_score": round(sc, 4), "n_1to1": n11,
            "binding": st, "n_both_labelled": both,
            "same_label_share": round(st["both_same"] / both, 5) if both else None,
            "one_sided_share": round((st["a_only"] + st["b_only"]) / max(n11, 1), 5),
            "no_label_share": round(st["neither"] / max(n11, 1), 5),
            "rebind_to_another_a_mark": rebind_to_other_a_mark,
            "diff_examples": diff_examples,
            "text_lines": [len(exA.texts), len(exB.texts)],
            "obj_per_text": round(len(LA.objects) / max(len(exA.texts), 1), 2),
        })
    except Exception as exc:
        import traceback
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["tb"] = traceback.format_exc()[-600:]
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return row


CF_PLAN = [("D2_text_move", {}), ("A6_round_0.25", {}), ("B3_crop_jitter", {"frac": 0.02}),
           ("C3_move_object", {"bucket": "small", "frac": 0.01})]


def cf_carrier(rec):
    import v03_counterfactual as C
    out = {"carrier": {k: rec[k] for k in ("doc_id", "version", "block_id",
                                           "discipline", "cls", "bucket", "n_seg")},
           "rows": []}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = G.extract(pb)
        if not ex.segments or len(ex.segments) > MAX_SEG:
            out["error"] = f"segments={len(ex.segments)}"
            return out
        LA = G.layer_of(ex.segments, ex.texts)
        if not LA.objects or len(LA.objects) > MAX_OBJ:
            out["error"] = f"objects={len(LA.objects)}"
            return out
        for cf_id, kw in CF_PLAN:
            tag = cf_id + ("@" + "@".join(str(v) for v in kw.values()) if kw else "")
            row = {"cf": tag}
            try:
                ex2, man = C.apply(ex, LA, cf_id, **kw)
            except C.CFNotApplicable as e:
                row["skip"] = str(e); out["rows"].append(row); continue
            LA2 = G.layer_of(ex.segments, ex.texts, S_override=LA.S)
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            gt_ab, _ = L.gt_from_provenance(LA2, ex.segments, LB, ex2.segments)
            la = L.object_labels(LA2, ex.texts)
            lb = L.object_labels(LB, ex2.texts)
            st = {"both_same": 0, "both_diff": 0, "a_only": 0, "b_only": 0, "neither": 0}
            for ia, ib in gt_ab.items():
                if ib is None:
                    continue
                ma, mb = la[ia][0], lb[ib][0]
                if ma and mb:
                    st["both_same" if ma == mb else "both_diff"] += 1
                elif ma:
                    st["a_only"] += 1
                elif mb:
                    st["b_only"] += 1
                else:
                    st["neither"] += 1
            both = st["both_same"] + st["both_diff"]
            n = sum(st.values())
            row.update({"n_gt_pairs": n, "binding": st,
                        "same_label_share": round(st["both_same"] / both, 5) if both else None,
                        "lost_label_share": round(st["a_only"] / max(n, 1), 5)})
            out["rows"].append(row)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return out


def radius_carrier(rec):
    out = {"carrier": {k: rec[k] for k in ("doc_id", "version", "block_id",
                                           "discipline", "cls", "n_seg")}}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = G.extract(pb)
        if not ex.segments or len(ex.segments) > MAX_SEG:
            out["error"] = "heavy/empty"; return out
        Lay = G.layer_of(ex.segments, ex.texts)
        if not Lay.objects or len(Lay.objects) > MAX_OBJ:
            out["error"] = "objects"; return out
        ms = _mark_sets(Lay, ex.texts)
        n = len(Lay.objects)
        m10, m16, m25 = ms[1.0], ms[1.6], ms[2.5]
        out.update({
            "n_obj": n,
            "labelled_1.0": sum(1 for x in m10 if x[0]),
            "labelled_1.6": sum(1 for x in m16 if x[0]),
            "labelled_2.5": sum(1 for x in m25 if x[0]),
            "same_mark_1.0_vs_1.6": sum(1 for a, b in zip(m10, m16) if a[0] and b[0] and a[0] == b[0]),
            "diff_mark_1.0_vs_1.6": sum(1 for a, b in zip(m10, m16) if a[0] and b[0] and a[0] != b[0]),
            "same_mark_1.6_vs_2.5": sum(1 for a, b in zip(m16, m25) if a[0] and b[0] and a[0] == b[0]),
            "diff_mark_1.6_vs_2.5": sum(1 for a, b in zip(m16, m25) if a[0] and b[0] and a[0] != b[0]),
        })
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return out


def main():
    arm = sys.argv[1] if len(sys.argv) > 1 else "real"
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    if arm == "real":
        pairs = json.load(open(L.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
        res = []
        with ProcessPoolExecutor(max_workers=workers) as exe:
            futs = [exe.submit(real_pair, p) for p in pairs]
            for i, f in enumerate(as_completed(futs)):
                r = f.result(); res.append(r)
                print(f"  {i+1}/{len(pairs)} {r['pair_id']} "
                      f"{r.get('skip') or r.get('error') or r.get('same_label_share')}", flush=True)
        json.dump({"threshold_1to1": THR, "pairs": res},
                  open(L.ART / "lbl_l5_real.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
    else:
        import cf_build_set as CB
        carriers = CB.pick_carriers()
        fn = cf_carrier if arm == "cf" else radius_carrier
        res = []
        with ProcessPoolExecutor(max_workers=workers) as exe:
            futs = [exe.submit(fn, c) for c in carriers]
            for i, f in enumerate(as_completed(futs)):
                res.append(f.result())
                print(f"  {i+1}/{len(carriers)}", flush=True)
        json.dump({"arm": arm, "carriers": res},
                  open(L.ART / f"lbl_l5_{arm}.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
    print("done", arm)


if __name__ == "__main__":
    main()
