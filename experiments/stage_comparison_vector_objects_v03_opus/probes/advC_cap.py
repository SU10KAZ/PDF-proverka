# -*- coding: utf-8 -*-
"""advC: what a size cap actually costs — objects kept vs ink kept on heavy blocks."""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O
import fam_family as FAM

ids = json.load(open(HERE.parent / "artifacts" / "advC_heavy_targets.json"))
out = []
for t in ids:
    pb = GC.prepared_block(t["doc_id"], t["version"], t["block_id"])
    if pb is None:
        continue
    ex = GC.extract(pb)
    layer = O.build_objects(ex)
    fam = FAM.build_families(layer)
    ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
    pay = C.describe(pb, ex, layer, fam, t["cls"], ptx)
    objs = pay["objects"]
    S = pay["scale"]["S"] or 1.0
    tot_ink = sum(o["ink_pt"] for o in objs) or 1.0
    row = {"block_id": t["block_id"], "n_seg": pay["quality"]["n_seg"], "S": S,
           "n_obj": len(objs), "bytes_full": C.nbytes(pay), "ink_total": round(tot_ink, 1),
           "variants": {}}
    def variant(name, keep, drop_desc=False, drop_fam=False, drop_label=False):
        sub = []
        for o in keep:
            r = dict(o)
            if drop_desc: r.pop("desc", None)
            if drop_fam: r.pop("fam", None)
            if drop_label: r.pop("label", None)
            sub.append(r)
        p2 = {k: v for k, v in pay.items() if k not in ("objects", "families")}
        p2["objects"] = sub
        if not drop_fam:
            p2["families"] = pay.get("families")
        row["variants"][name] = {
            "n_obj": len(sub), "bytes": C.nbytes(p2), "tokens": C.tokens(p2),
            "ink_kept": round(sum(o["ink_pt"] for o in keep) / tot_ink, 4)}
    variant("full", objs)
    variant("no_desc", objs, drop_desc=True)
    variant("no_desc_no_fam", objs, drop_desc=True, drop_fam=True)
    for thr, nm in ((3.0, "ink>=3pt"), (2.0 * S, "ink>=2S"), (60.0, "ink>=60pt")):
        variant(nm, [o for o in objs if o["ink_pt"] >= thr])
    variant("ink>=2S+no_desc", [o for o in objs if o["ink_pt"] >= 2.0 * S], drop_desc=True,
            drop_fam=True)
    out.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
(C.ART / "advC_cap.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                     encoding="utf-8")
