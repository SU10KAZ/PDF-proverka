# -*- coding: utf-8 -*-
"""scope · SC3b — the 966 "orphan" blocks: a real one-sided object, or just no block there?

A block of side A with no counterpart block on side B is an ORPHAN only in block space.
In page space its rectangle exists on both sheets.  Read the SAME rectangle from both
pages and let the ledger speak.  usage: scope_sc3_orphans.py <shard> <nshards>
"""
from __future__ import annotations
import json, random, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F        # noqa
import scope_sc2_run as R         # noqa

SEED = 20260823
N = 30


def main():
    sh, ns = int(sys.argv[1]), int(sys.argv[2])
    comps = [json.loads(l) for l in open(ART / "scope_components.jsonl", encoding="utf-8")]
    orph = [c for c in comps if "orphan" in c["kind"] and c.get("page_rect_equal") and not c["rot_mismatch"]]
    rnd = random.Random(SEED)
    pick = rnd.sample(orph, min(N, len(orph)))
    out = []
    (ART / "scope_orphan_parts").mkdir(exist_ok=True)
    for i, c in enumerate(pick):
        if i % ns != sh:
            continue
        side = "a" if c["kind"].endswith("orphan_a") else "b"
        b = c[f"blocks_{side}"][0]
        pdf_a = str(ROOT / c["pdf_a"]); pdf_b = str(ROOT / c["pdf_b"])
        pi_a = b["page_index"] if side == "a" else c["page_b"] - 1
        pi_b = c["page_b"] - 1 if side == "a" else b["page_index"]
        pi_a = c["page_a"] - 1
        pi_b = c["page_b"] - 1
        ppx_a = c["blocks_a"][0]["page_px"] if c["blocks_a"] else b["page_px"]
        ppx_b = c["blocks_b"][0]["page_px"] if c["blocks_b"] else b["page_px"]
        try:
            pra = F.open_doc(pdf_a)[pi_a].rect
            prb = F.open_doc(pdf_b)[pi_b].rect
            rect_pt = R.clip_pt(pdf_a if side == "a" else pdf_b,
                                pi_a if side == "a" else pi_b, b["coords_px"], b["page_px"])
            ra = R.px_rect_from_pt(rect_pt, pra, ppx_a)
            rb = R.px_rect_from_pt(rect_pt, prb, ppx_b)
            row = {"doc_id": c["doc_id"], "disc": c["discipline"], "kind": c["kind"],
                   "page_a": c["page_a"], "page_b": c["page_b"], "block": b["id"], "side": side,
                   "rect_pt": [round(v, 1) for v in rect_pt]}
            row["page_frame"] = R.run_arm(pdf_a, pi_a, ra, ppx_a, pdf_b, pi_b, rb, ppx_b, roi=rect_pt)
        except Exception as e:
            row = {"doc_id": c["doc_id"], "block": b["id"], "error": repr(e)}
        out.append(row)
        print(row.get("doc_id"), row.get("block"), row.get("page_frame", {}).get("n_records_big",
              row.get("page_frame", {}).get("error", row.get("error"))),
              row.get("page_frame", {}).get("similarity"), flush=True)
        F.clear_caches()
    json.dump(out, open(ART / "scope_orphan_parts" / f"{sh}.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
