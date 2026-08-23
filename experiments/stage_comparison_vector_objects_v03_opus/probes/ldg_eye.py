# -*- coding: utf-8 -*-
"""Pictures for the wording check: is the thing the detector calls an OPENING / a BRANCH
actually an opening in a wall / a branch of a network, or a table ruling?

    python probes/ldg_eye.py OPENING_REMOVED 8
Renders 'before' and 'after' of the change region (zoomed, plus a wide context view).
"""
from __future__ import annotations
import json
import sys
import glob
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402

CF_OF = {"OPENING_REMOVED": "C10_remove_opening", "BRANCH_ADDED": "C9_add_branch",
         "OBJECT_ADDED": "C2_add_object@small", "OBJECT_REMOVED": "C1_remove_object@small"}


def main():
    pid = sys.argv[1]
    want = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    inst = CF_OF[pid]
    rows = []
    for f in sorted(glob.glob(str(ART / "ldg_runs" / "cf_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if r.get("inst") == inst and r.get("noise") == "none" and \
                    any(q["id"] == pid for q in r.get("phrases", [])):
                rows.append(r)
    rows.sort(key=lambda r: r["block_id"])
    outdir = ART / "ldg_eye"
    outdir.mkdir(exist_ok=True)
    index = []
    for r in rows[:want]:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        ex = G.extract(pb)
        ol = O.build_objects(ex)
        kw = {"bucket": "small"} if "@small" in inst else {}
        ex2, man = C.apply(ex, ol, inst.split("@")[0], **kw)
        bb = man["change_bbox_pt"]
        for pad_name, pad in (("zoom", 12.0), ("wide", 90.0)):
            fr = (bb[0] - pad, bb[1] - pad, bb[2] + pad, bb[3] + pad)
            C.render_extract(ex, frame=fr, target_px=520,
                             out_png=str(outdir / f"{pid}_{r['block_id']}_{pad_name}_A.png"))
            C.render_extract(ex2, frame=fr, target_px=520,
                             out_png=str(outdir / f"{pid}_{r['block_id']}_{pad_name}_B.png"))
        index.append({"phrase": pid, "block_id": r["block_id"], "doc_id": r["doc_id"],
                      "discipline": r["discipline"], "cls": r["cls"],
                      "n_seg": r["n_seg"], "bbox": bb,
                      "params": man.get("params"),
                      "objects": [o.get("object_id") for o in man.get("touched_objects", [])],
                      "obj_cls": [o.get("cls") for o in man.get("touched_objects", [])]})
        print("rendered", r["block_id"], r["discipline"], man.get("params"), flush=True)
    json.dump(index, open(ART / f"ldg_eye_{pid}.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
