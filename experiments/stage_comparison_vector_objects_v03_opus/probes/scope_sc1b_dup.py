# -*- coding: utf-8 -*-
"""scope · SC1b — the OTHER kind of ambiguity: identical drawings repeated on many sheets.

Uses the geometry hash the cns probe already computed (identical isotropically normalised
segment set).  Two questions, both about whether a positional 1:1 map is enough:

  (a) how often does one block of side A have SEVERAL geometrically identical candidates
      on side B (content-level 1->N);
  (b) how often does a block keep its geometry but MOVE to another place on the sheet
      (position-level match fails while content match succeeds).

Writes artifacts/scope_duplicates.json
"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

def main():
    # geometry hash per (doc_id, version, block_id)
    H = {}
    per_ver = {}
    for line in open(ART / "cns_block_classes.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if not r.get("geom_sha"):
            continue
        H[(r["doc_id"], r["version"], r["block_id"])] = (r["geom_sha"], r["page_number"], r["cls"], r["n_seg"])
        per_ver.setdefault((r["doc_id"], r["version"]), {}).setdefault(r["geom_sha"], []).append(
            (r["block_id"], r["page_number"], r["n_seg"]))

    comps = [json.loads(l) for l in open(ART / "scope_components.jsonl", encoding="utf-8")]
    # rebuild the per-version-pair block sets from the components
    S = {"blocks_a_hashed": 0, "amb_doc": 0, "amb_page": 0, "unique_in_b": 0, "absent_in_b": 0,
         "moved_same_page": 0, "moved_other_page": 0, "positional_ok": 0}
    hist = {}
    moved_examples = []
    seen_a = set()
    for c in comps:
        key_a = (c["doc_id"], c["ver_a"])
        key_b = (c["doc_id"], c["ver_b"])
        Bv = per_ver.get(key_b, {})
        for b in c["blocks_a"]:
            k = (c["doc_id"], c["ver_a"], b["id"])
            if k in seen_a:
                continue
            seen_a.add(k)
            h = H.get(k)
            if not h:
                continue
            S["blocks_a_hashed"] += 1
            cands = Bv.get(h[0], [])
            n = len(cands)
            hist[n] = hist.get(n, 0) + 1
            if n == 0:
                S["absent_in_b"] += 1
                continue
            if n == 1:
                S["unique_in_b"] += 1
            else:
                S["amb_doc"] += 1
                if sum(1 for x in cands if x[1] == c["page_b"]) > 1:
                    S["amb_page"] += 1
            # did the geometry stay in the same component (i.e. positional match works)?
            ids_b = {x["id"] for x in c["blocks_b"]}
            if any(x[0] in ids_b for x in cands):
                S["positional_ok"] += 1
            else:
                same_page = [x for x in cands if x[1] == c["page_b"]]
                if same_page:
                    S["moved_same_page"] += 1
                    if len(moved_examples) < 40:
                        moved_examples.append({"doc": c["doc_id"], "ver": [c["ver_a"], c["ver_b"]],
                                               "page_a": c["page_a"], "page_b": c["page_b"],
                                               "block_a": b["id"], "n_seg": h[3],
                                               "same_geometry_at": [x[0] for x in same_page]})
                else:
                    S["moved_other_page"] += 1
    out = {"schema_version": "scope_duplicates/1",
           "note": "geometry hash from cns (identical normalised segment set, blocks >=50 seg)",
           "summary": S,
           "candidates_per_block_hist": {str(k): v for k, v in sorted(hist.items())},
           "shares": {
               "ambiguous_within_document": round(S["amb_doc"] / max(1, S["blocks_a_hashed"]), 4),
               "ambiguous_within_the_matched_sheet": round(S["amb_page"] / max(1, S["blocks_a_hashed"]), 4),
               "geometry_moved_off_its_positional_partner": round(
                   (S["moved_same_page"] + S["moved_other_page"]) / max(1, S["blocks_a_hashed"]), 4),
           },
           "moved_examples": moved_examples}
    json.dump(out, open(ART / "scope_duplicates.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({"summary": S, "shares": out["shares"]}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
