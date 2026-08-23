# -*- coding: utf-8 -*-
"""L4 [CF] — FALSE DEPENDENCE ON TEXT (§7).

D3_label_rename changes every occurrence of a designation (QF1 -> QF2) and does NOT
touch a single segment (cf: CF13, geometry byte-identical in 342/342).  A comparator
of GRAPHIC objects is therefore REQUIRED to answer NO_GRAPHIC_CHANGE.

Measured for three comparator designs on the same blocks:
    label_anchor    — the label only lowers the matching cost (honest)
    label_evidence  — a label mismatch also raises it (the trap)
    label_verdict   — a changed label is itself a ledger entry (the pure §7 failure)

Controls: D1_text_edit, D2_text_move, D4_table_values, D8_font_swap (also NO change)
and C3_move_object@small (a real change: the comparator must NOT stay silent).

Usage: lbl_l4_d3.py [workers]
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G

MAX_OBJ = 4000
MAX_SEG = 60000
PLAN = [("D3_label_rename", {}), ("D1_text_edit", {}), ("D2_text_move", {}),
        ("D4_table_values", {}), ("D8_font_swap", {}),
        ("C3_move_object", {"bucket": "small", "frac": 0.01})]


def run_carrier(rec):
    import v03_counterfactual as C
    out = {"carrier": {k: rec[k] for k in ("doc_id", "version", "block_id",
                                           "discipline", "cls", "bucket", "n_seg")},
           "rows": [], "error": None}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            out["error"] = "block not found"
            return out
        ex = G.extract(pb)
        if not ex.segments or len(ex.segments) > MAX_SEG:
            out["error"] = f"segments={len(ex.segments)}"
            return out
        LA = G.layer_of(ex.segments, ex.texts)
        if not LA.objects or len(LA.objects) > MAX_OBJ:
            out["error"] = f"objects={len(LA.objects)}"
            return out
        lab_a = L.object_labels(LA, ex.texts)
        out["side_a"] = {"n_obj": len(LA.objects), "n_text": len(ex.texts),
                         "S": round(LA.S, 3),
                         "n_labelled": sum(1 for m, _ in lab_a if m),
                         "n_unique_label": sum(1 for m, u in lab_a if m and u)}
        for cf_id, kw in PLAN:
            tag = cf_id + ("@" + "@".join(str(v) for v in kw.values()) if kw else "")
            row = {"cf": tag}
            try:
                ex2, man = C.apply(ex, LA, cf_id, **kw)
            except C.CFNotApplicable as e:
                row["skip"] = str(e)
                out["rows"].append(row)
                continue
            LB = G.layer_of(ex2.segments, ex2.texts, S_override=LA.S)
            LA2 = G.layer_of(ex.segments, ex.texts, S_override=LA.S)
            lab_a2 = L.object_labels(LA2, ex.texts)
            lab_b = L.object_labels(LB, ex2.texts)
            gt_ab, gt_ba = L.gt_from_provenance(LA2, ex.segments, LB, ex2.segments)
            row.update({"expected": man.get("expected_verdict"),
                        "n_obj_a": len(LA2.objects), "n_obj_b": len(LB.objects),
                        "n_touched_texts": len(man.get("touched_texts") or []),
                        "geometry_identical": len(ex.segments) == len(ex2.segments)})
            for use in ("anchor", "evidence"):
                m = L.match_objects(LA2, LB, "geom_pos_label", LA.S, labels_a=lab_a2,
                                    labels_b=lab_b, label_use=use)
                sc = L.score(m["pairs"], gt_ab, gt_ba, m["na"], m["nb"])
                lc = L.label_change_census(LA2, LB, m["pairs"], lab_a2, lab_b)
                v_plain = L.verdict(LA2, LB, m["pairs"], LA.S)
                v_lbl = L.verdict(LA2, LB, m["pairs"], LA.S, labels_a=lab_a2,
                                  labels_b=lab_b, label_is_evidence=True)
                row[f"label_{use}"] = {
                    "precision": sc["precision"], "recall": sc["recall"],
                    "n_matched": sc["n_matched"], "n_correct": sc["n_correct"],
                    "mispairs": sc["n_matched"] - sc["n_correct"],
                    "false_removed": sc["false_removed"], "false_added": sc["false_added"],
                    "label_census": lc,
                    "verdict_plain": v_plain["verdict"],
                    "entries_plain": v_plain["counts"],
                    "n_entries_plain": v_plain["n_entries"],
                    "verdict_label_as_evidence": v_lbl["verdict"],
                    "n_entries_label_as_evidence": v_lbl["n_entries"],
                    "n_renamed_entries": v_lbl["counts"]["RENAMED_OBJECT"],
                }
            # mode (b) for reference: no labels at all
            mb_ = L.match_objects(LA2, LB, "geom_pos", LA.S)
            scb = L.score(mb_["pairs"], gt_ab, gt_ba, mb_["na"], mb_["nb"])
            vb = L.verdict(LA2, LB, mb_["pairs"], LA.S)
            row["geom_pos"] = {"precision": scb["precision"], "recall": scb["recall"],
                               "verdict_plain": vb["verdict"],
                               "n_entries_plain": vb["n_entries"],
                               "entries_plain": vb["counts"]}
            out["rows"].append(row)
    except Exception as exc:
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["tb"] = traceback.format_exc()[-800:]
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return out


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    import cf_build_set as CB
    carriers = CB.pick_carriers()
    print(len(carriers), "carriers", flush=True)
    res = []
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futs = [exe.submit(run_carrier, c) for c in carriers]
        for i, f in enumerate(as_completed(futs)):
            r = f.result()
            res.append(r)
            print(f"  {i+1}/{len(carriers)} {r['carrier']['block_id'][:12]} "
                  f"{r.get('error') or len(r['rows'])}", flush=True)
    json.dump({"plan": [p[0] for p in PLAN], "carriers": res},
              open(L.ART / "lbl_l4_d3.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("done")


if __name__ == "__main__":
    main()
