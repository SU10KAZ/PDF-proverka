# -*- coding: utf-8 -*-
"""VERIFY F5: corpus-wide census of block classes on which the foundation module
cannot return anything meaningful (or returns something silently wrong)."""
import json, math
from collections import Counter
from pathlib import Path
ART = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_objects_v03_opus/artifacts")

rows = [json.loads(l) for l in open(ART/"vfy_corpus.jsonl", encoding="utf-8")]
c = Counter(); docs = Counter()
tot = 0
for r in rows:
    for p in r["pages"]:
        n = p["n_img"]
        tot += n
        if not r["pdf_exists"]:
            c["no_document_pdf"] += n; docs["no_document_pdf"] and None
            continue
        if p["rect"] is None:
            c["page_number_out_of_pdf_range"] += n
            continue
        if not p["w"] or not p["h"]:
            c["page_px_size_missing"] += n
            continue
        rect = p["rect"]
        sx = rect[2]/p["w"]; sy = rect[3]/p["h"]
        asp_pdf = rect[2]/rect[3] if rect[3] else 0
        asp_px = p["w"]/p["h"]
        if asp_px and abs(asp_pdf-asp_px)/asp_px > 0.01:
            c["page_aspect_mismatch"] += n
        for co, sh in zip(p["coords"], p["shape"]):
            if co is None:
                c["coords_unparsable"] += 1
                continue
            if any(math.isnan(v) or math.isinf(v) for v in co):
                c["coords_nan_inf"] += 1
                continue
            if co[2] <= co[0] or co[3] <= co[1]:
                c["coords_reversed_or_zero"] += 1
            w = (max(co[0],co[2])-min(co[0],co[2]))*sx
            h = (max(co[1],co[3])-min(co[1],co[3]))*sy
            if max(w, h) < 1:
                c["clip_long_side_lt_1pt_raises"] += 1
            if w <= 0 or h <= 0:
                c["clip_zero_area"] += 1
            if co[0] < -0.5 or co[1] < -0.5 or co[2] > p["w"]+0.5 or co[3] > p["h"]+0.5:
                c["coords_outside_page_px"] += 1
            if sh == "polygon":
                c["shape_polygon"] += 1
c["TOTAL_image_blocks"] = tot
print(json.dumps(dict(c), ensure_ascii=False, indent=1))
json.dump({"census": dict(c), "total": tot}, open(ART/"vfy_f5_census.json","w"), ensure_ascii=False, indent=1)
