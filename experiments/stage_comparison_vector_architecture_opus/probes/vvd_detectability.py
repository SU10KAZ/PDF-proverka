#!/usr/bin/env python3
"""VVD — deterministic ceiling on what a picture could possibly falsify.

For every mutation case: which claims moved, whether those claims are checkable from a
raster at all, and how big the numeric move was.  No model calls.

Claim checkability from a raster (argued in vvd_FINDINGS.md):
  picture_checkable   C3 C4 C10 C11 C14 (+ C1/C5/C6 only when the block is sparse enough to count)
  plausibility_only   C7 C8(junction half) C9
  not_checkable       C12 C13 (they describe the extractor, not the drawing)
  asymmetric          C2 (a raster always renders glyphs, so "N are garbled" can only ever be rejected)
"""
from __future__ import annotations

import json
import re

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

CHECKABLE = {"C3", "C4", "C10", "C11", "C14"}
COUNTABLE_IF_SPARSE = {"C1", "C5", "C6"}
PLAUSIBILITY = {"C7", "C9"}
MIXED = {"C8"}          # closed outlines checkable, junction count not
ASYMMETRIC = {"C2"}
NOT_CHECKABLE = {"C12", "C13"}

NUM = re.compile(r"-?\d+(?:\.\d+)?")


def numbers(text: str) -> list[float]:
    return [float(x) for x in NUM.findall(text or "")]


def main() -> None:
    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    blocks = manifest["blocks"]
    rows = []
    for case in manifest["cases"]:
        if case["family"] == "control":
            continue
        prof = blocks[case["block"]]["profile"]
        sparse = prof["texts"] <= 60 and prof["segments"] <= 2000
        changed = case["changed_claims"]
        buckets = {"checkable": [], "countable_if_sparse": [], "plausibility": [],
                   "mixed": [], "asymmetric": [], "not_checkable": []}
        for cid in changed:
            if cid in CHECKABLE:
                buckets["checkable"].append(cid)
            elif cid in COUNTABLE_IF_SPARSE:
                buckets["countable_if_sparse"].append(cid)
            elif cid in PLAUSIBILITY:
                buckets["plausibility"].append(cid)
            elif cid in MIXED:
                buckets["mixed"].append(cid)
            elif cid in ASYMMETRIC:
                buckets["asymmetric"].append(cid)
            elif cid in NOT_CHECKABLE:
                buckets["not_checkable"].append(cid)
        deltas = []
        for d in case.get("claim_delta") or []:
            b, a = numbers(d.get("before", "")), numbers(d.get("after", ""))
            rel = None
            if b and a and len(b) == len(a):
                pairs = [(x, y) for x, y in zip(b, a) if x != y]
                if pairs:
                    x, y = pairs[0]
                    rel = round(abs(y - x) / abs(x), 4) if x else None
            deltas.append({"claim_id": d["claim_id"], "before": d.get("before"),
                           "after": d.get("after"), "first_relative_move": rel})
        hard = (not buckets["checkable"]) and not (sparse and buckets["countable_if_sparse"])
        rows.append({
            "case_id": case["case_id"], "block": case["block"], "mutation": case["mutation"],
            "block_sparse_enough_to_count": sparse,
            "changed_claims": changed, "buckets": buckets,
            "n_picture_checkable": len(buckets["checkable"]) +
                                   (len(buckets["countable_if_sparse"]) if sparse else 0),
            "ceiling": "hard_or_impossible" if hard else "checkable",
            "claim_deltas": deltas,
        })
    dest = vv.ARTIFACTS / "vvd_detectability.json"
    dest.write_text(json.dumps({"rows": rows}, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")
    import collections
    c = collections.Counter((r["mutation"], r["ceiling"]) for r in rows)
    for k in sorted(c):
        print(k, c[k])
    print()
    for r in rows:
        print(f"{r['case_id']} {r['mutation']:16s} {r['block']:28s} sparse={r['block_sparse_enough_to_count']!s:5s} "
              f"checkable={r['n_picture_checkable']} {r['ceiling']:18s} "
              f"moves={[ (d['claim_id'], d['first_relative_move']) for d in r['claim_deltas'] ]}")
    print("written:", dest)


if __name__ == "__main__":
    main()
