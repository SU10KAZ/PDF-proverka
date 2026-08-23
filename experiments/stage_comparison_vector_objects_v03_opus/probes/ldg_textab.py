# -*- coding: utf-8 -*-
"""§7 ablation: strip the text layer from BOTH sides and see what the ledger loses.

A record that needed a string to exist would disappear here; a record that rests on ink
must survive.  The text layer also feeds the characteristic scale S, so what changes and
what does not is exactly the question.

    python probes/ldg_textab.py <shard> <nshards>
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
import ldg_ledger as LDG        # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402

INST = [("C1_remove_object@small", "C1_remove_object", {"bucket": "small"}),
        ("C9_add_branch", "C9_add_branch", {}),
        ("NEG", None, {})]


def run(exA, exB):
    LA, LB, meta = L.layers(exA, exB)
    ldg = LDG.build(exA, exB, LA=LA, LB=LB, meta=meta)
    return ldg


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    carriers = [c for c in pick_carriers() if c["n_seg"] <= 12000]
    out = open(ART / "ldg_runs" / f"textab_{shard}.jsonl", "w", encoding="utf-8")
    for ci, r in enumerate(carriers):
        if ci % nsh != shard:
            continue
        try:
            ex = G.extract(G.prepared_block(r["doc_id"], r["version"], r["block_id"]))
            ol = O.build_objects(ex)
        except Exception as e:
            print("FAIL", r["block_id"], repr(e), flush=True)
            continue
        if not ex.segments or not ex.texts:
            continue
        ex_nt = C._clone(ex, texts=[], prov={"ldg": "texts_stripped"})
        for inst, cf, kw in INST:
            try:
                ex2 = ex if cf is None else C.apply(ex, ol, cf, **kw)[0]
                exB = L.noisy(ex2, "round025", seed=20260823)
                exB_nt = C._clone(exB, texts=[], prov={"ldg": "texts_stripped"})
                a = run(ex, exB)
                b = run(ex_nt, exB_nt)
            except C.CFNotApplicable as e:
                out.write(json.dumps({"block_id": r["block_id"], "inst": inst,
                                      "skip": str(e)}, ensure_ascii=False) + "\n")
                continue
            except Exception as e:
                out.write(json.dumps({"block_id": r["block_id"], "inst": inst,
                                      "error": repr(e)}, ensure_ascii=False) + "\n")
                continue
            bb_a = [[round(v, 1) for v in c["evidence"][-1]["bbox_pt"]] for c in a["changes"]]
            bb_b = [[round(v, 1) for v in c["evidence"][-1]["bbox_pt"]] for c in b["changes"]]
            matched = sum(1 for x in bb_a if any(L._ov(x, y) >= 0.3 for y in bb_b))
            out.write(json.dumps({
                "block_id": r["block_id"], "discipline": r["discipline"],
                "n_seg": len(ex.segments), "n_text": len(ex.texts), "inst": inst,
                "S_with_text": a["S"], "S_no_text": b["S"],
                "n_changes_with_text": len(a["changes"]),
                "n_changes_no_text": len(b["changes"]),
                "records_surviving": matched,
                "phrases_with_text": [q["id"] for q in LDG.phrases(a)],
                "phrases_no_text": [q["id"] for q in LDG.phrases(b)],
                "labels_used_with_text": sum(
                    1 for c in a["changes"]
                    if (c["object_before"] or {}).get("label") or
                       (c["object_after"] or {}).get("label")),
            }, ensure_ascii=False) + "\n")
        out.flush()
        print("done", r["block_id"], flush=True)
    out.close()


if __name__ == "__main__":
    main()
