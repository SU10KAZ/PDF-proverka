# -*- coding: utf-8 -*-
"""How small may the shape descriptor be?  Quantise it and measure what the family
layer loses (ARI against full precision) and what the payload gains (bytes)."""
from __future__ import annotations
import copy, json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O
import fam_family as FAM

ART = C.ART
EX = ART / "ctr_examples"


def quantised(layer, nd):
    lay = copy.deepcopy(layer)
    for o in lay.objects:
        o["desc"] = dict(o["desc"])
        if nd is None:                       # 8-bit integer grid
            o["desc"]["vec"] = [round(v * 255) / 255.0 for v in o["desc"]["vec"]]
        else:
            o["desc"]["vec"] = [round(v, nd) for v in o["desc"]["vec"]]
    return lay


def main():
    sizes = json.load(open(ART / "ctr_payload_sizes.json", encoding="utf-8"))["cases"]
    idx = {r["block_id"]: r for r in GC.block_records()}
    out = []
    for row in sizes:
        name, bid = row["case"], row["block_id"]
        pay = json.load(open(EX / f"{name}.json", encoding="utf-8"))
        rec = idx.get(bid) or {"doc_id": pay["_source"]["doc_id"], "version": pay["_source"]["version"]}
        pb = GC.prepared_block(rec["doc_id"], rec["version"], bid)
        ex = GC.extract(pb)
        layer = O.build_objects(ex)
        base = FAM.build_families(layer)
        r = {"case": name, "n_obj": len(layer.objects),
             "rep_families_full": sum(1 for f in base.families if len(f["members"]) >= 2),
             "variants": {}}
        for tag, nd in (("round4", 4), ("round3", 3), ("round2", 2), ("uint8", None)):
            lq = quantised(layer, nd)
            fq = FAM.build_families(lq)
            payq = C.describe(pb, ex, lq, fq, rec.get("cls", "?"),
                              pay["quality"]["page_text_lines"])
            r["variants"][tag] = {
                "ari_vs_full": round(FAM.ari(base.obj_family, fq.obj_family), 4),
                "rep_families": sum(1 for f in fq.families if len(f["members"]) >= 2),
                "bytes": C.nbytes(payq), "tokens": C.tokens(payq)}
        # desc only for objects above the publication floor
        floor_pay = C.describe(pb, ex, layer, base, rec.get("cls", "?"),
                               pay["quality"]["page_text_lines"])
        kept = 0
        for o in floor_pay["objects"]:
            if o["ink_pt"] < 20.0:
                o.pop("desc", None)
            else:
                kept += 1
        r["desc_only_above_20pt"] = {"objects_with_desc": kept,
                                     "bytes": C.nbytes(floor_pay),
                                     "tokens": C.tokens(floor_pay)}
        out.append(r)
        print(json.dumps(r, ensure_ascii=False), flush=True)
    (ART / "ctr_desc_quantisation.json").write_text(
        json.dumps({"cases": out}, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
