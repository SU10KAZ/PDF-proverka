# -*- coding: utf-8 -*-
"""Class D re-check with correct text metrics (multiset of strings + moved bboxes).

The first pass compared SORTED text lists, which counts a re-ordered line as changed;
this pass counts a line as changed only if the multiset of strings changed.
"""
from __future__ import annotations
import json
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import cf_build_set as B          # noqa: E402

D_IDS = ["D1_text_edit", "D2_text_move", "D3_label_rename", "D4_table_values",
         "D5_table_row_text", "D6_dim_value_only", "D7_dim_geometry", "D8_font_swap"]


def one(rec):
    import grp_common as G
    import v03_objects as O
    import v03_counterfactual as C
    import cf_check as K
    out = {"carrier": rec["block_id"], "discipline": rec["discipline"], "rows": []}
    try:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = G.extract(pb)
        if not ex.segments:
            return out
        ol = O.build_objects(ex)
    except Exception as e:                                   # noqa: BLE001
        out["error"] = str(e)
        return out
    for cf in D_IDS:
        try:
            ex2, man = C.apply(ex, ol, cf)
        except C.CFNotApplicable:
            continue
        except Exception as e:                               # noqa: BLE001
            out["rows"].append({"cf_id": cf, "error": f"{type(e).__name__}: {e}"})
            continue
        gm = K.geometry_match(ex, ex2, tol=1e-6)
        cb, ca = Counter(t["text"] for t in ex.texts), Counter(t["text"] for t in ex2.texts)
        changed = sum((cb - ca).values())
        moved = 0
        if len(ex.texts) == len(ex2.texts):
            for a, b in zip(ex.texts, ex2.texts):
                if [round(v, 4) for v in a["bbox"]] != [round(v, 4) for v in b["bbox"]]:
                    moved += 1
        fonts_changed = sum(1 for a, b in zip(ex.texts, ex2.texts)
                            if a.get("font") != b.get("font")) if len(ex.texts) == len(ex2.texts) else None
        out["rows"].append({
            "cf_id": cf, "n_text": len(ex.texts), "n_text_after": len(ex2.texts),
            "geometry_identical": gm["identical"], "geom_frac_of_a": round(gm["frac_of_a"], 6),
            "n_seg_before": len(ex.segments), "n_seg_after": len(ex2.segments),
            "strings_changed": changed, "bboxes_moved": moved, "fonts_changed": fonts_changed,
            "declared_touched_texts": len(man.get("touched_texts") or []),
        })
    return out


def main():
    carriers = B.pick_carriers()
    res = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=10) as pool:
        for f in as_completed([pool.submit(one, r) for r in carriers]):
            res.append(f.result())
    agg = {}
    for cf in D_IDS:
        rows = [r for x in res for r in x["rows"] if r["cf_id"] == cf and "error" not in r]
        if not rows:
            continue
        agg[cf] = {
            "n": len(rows),
            "geometry_identical": sum(1 for r in rows if r["geometry_identical"]),
            "seg_count_equal": sum(1 for r in rows if r["n_seg_before"] == r["n_seg_after"]),
            "strings_changed_median": sorted(r["strings_changed"] for r in rows)[len(rows) // 2],
            "strings_changed_max": max(r["strings_changed"] for r in rows),
            "bboxes_moved_median": sorted(r["bboxes_moved"] for r in rows)[len(rows) // 2],
            "fonts_changed_median": sorted((r["fonts_changed"] or 0) for r in rows)[len(rows) // 2],
            "declared_touched_median": sorted(r["declared_touched_texts"] for r in rows)[len(rows) // 2],
            "n_text_median": sorted(r["n_text"] for r in rows)[len(rows) // 2],
        }
    json.dump({"per_cf": agg, "carriers": len(res), "sec": round(time.time() - t0, 1),
               "raw": res}, open(ART / "cf_text_checks.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(agg, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
