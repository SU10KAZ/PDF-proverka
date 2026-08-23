# -*- coding: utf-8 -*-
"""scope · SC3 precondition — is the PAGE a common coordinate system at all?

For every matched sheet of the census: same page rectangle?  same /Rotate?  If the two
versions print the sheet at a different physical size, "page coordinates" need a scale
factor and stop being free.  Writes artifacts/scope_page_frames.json
"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F  # noqa


def main():
    comps = [json.loads(l) for l in open(ART / "scope_components.jsonl", encoding="utf-8")]
    pages = {}
    for c in comps:
        pages[(c["pdf_a"], c["pdf_b"], c["page_a"], c["page_b"])] = c
    S = {"pages": 0, "same_rect": 0, "same_rect_1pct": 0, "same_rot": 0, "err": 0}
    diffs = []
    for (pa, pb, na, nb), c in pages.items():
        try:
            ra = F.open_doc(str(ROOT / pa))[c["blocks_a"][0]["page_index"] if c["blocks_a"] else na - 1].rect
            rb = F.open_doc(str(ROOT / pb))[c["blocks_b"][0]["page_index"] if c["blocks_b"] else nb - 1].rect
        except Exception:
            S["err"] += 1
            continue
        S["pages"] += 1
        dw = abs(ra.width - rb.width) / max(ra.width, rb.width)
        dh = abs(ra.height - rb.height) / max(ra.height, rb.height)
        if ra.width == rb.width and ra.height == rb.height:
            S["same_rect"] += 1
        if dw <= 0.01 and dh <= 0.01:
            S["same_rect_1pct"] += 1
        else:
            diffs.append({"doc": c["doc_id"], "page_a": na, "page_b": nb,
                          "rect_a": [round(ra.width, 1), round(ra.height, 1)],
                          "rect_b": [round(rb.width, 1), round(rb.height, 1)]})
        if not c["rot_mismatch"]:
            S["same_rot"] += 1
        if S["pages"] % 400 == 0:
            F.clear_caches()
    S["share_same_rect"] = round(S["same_rect"] / max(1, S["pages"]), 4)
    S["share_same_rect_within_1pct"] = round(S["same_rect_1pct"] / max(1, S["pages"]), 4)
    S["share_same_rotation"] = round(S["same_rot"] / max(1, S["pages"]), 4)
    json.dump({"schema_version": "scope_page_frames/1", "summary": S,
               "examples_of_different_page_size": diffs[:30]},
              open(ART / "scope_page_frames.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(S, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
