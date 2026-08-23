# -*- coding: utf-8 -*-
"""CNS-2 — build contact sheets of REAL prepared blocks for human (model) eye labelling.

Renders through v03_foundation.render_block (== production crop_from_pdf region).
"""
from __future__ import annotations
import json, os, random, sys
from collections import defaultdict
from pathlib import Path
from PIL import Image, ImageDraw

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
OUT = ART / "cns_renders"
CELL = 700


def load_frame():
    exists = {}
    rows = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            p = b["pdf"]
            if p not in exists:
                exists[p] = os.path.exists(p)
            if exists[p]:
                rows.append(b)
    return rows


def sample_random(rows, n, seed):
    rng = random.Random(seed)
    by = defaultdict(list)
    for b in rows:
        by[b["discipline"]].append(b)
    tot = len(rows)
    out = []
    for d, v in sorted(by.items()):
        k = round(n * len(v) / tot)
        if k:
            out.extend(rng.sample(v, min(k, len(v))))
    rng.shuffle(out)
    return out[:n]


def build(blocks, tag, cols=2, rows_per=2):
    per = cols * rows_per
    manifest = []
    sheet = None
    dr = None
    for i, b in enumerate(blocks):
        idx = i % per
        if idx == 0:
            sheet = Image.new("RGB", (cols * CELL, rows_per * CELL), "white")
            dr = ImageDraw.Draw(sheet)
        label = f"{tag}{i:02d}"
        try:
            pix = F.render_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 target_px=CELL - 30)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            img.thumbnail((CELL - 12, CELL - 34))
        except Exception as exc:
            img = Image.new("RGB", (200, 60), "white")
            ImageDraw.Draw(img).text((4, 20), f"ERR {type(exc).__name__}", fill="red")
        cx = (idx % cols) * CELL
        cy = (idx // cols) * CELL
        sheet.paste(img, (cx + 6, cy + 28))
        dr.rectangle([cx + 1, cy + 1, cx + CELL - 2, cy + CELL - 2], outline=(200, 0, 0), width=2)
        dr.text((cx + 8, cy + 8), f"{label}  {b['discipline']} {b['doc_id'][:26]} p{b['page_number']}",
                fill=(180, 0, 0))
        manifest.append({"label": label, "block_id": b["block_id"], "doc_id": b["doc_id"],
                         "version": b["version"], "discipline": b["discipline"],
                         "page_number": b["page_number"], "category_code": b["category_code"],
                         "sheet": f"{tag}_sheet{i//per:02d}.png"})
        if idx == per - 1 or i == len(blocks) - 1:
            sheet.save(OUT / f"{tag}_sheet{i//per:02d}.png")
        F.clear_caches()
    (OUT / f"{tag}_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    print("sheets:", len(blocks), "->", (len(blocks) + per - 1) // per)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    mode = sys.argv[1]
    if mode == "random":
        n = int(sys.argv[2]); seed = int(sys.argv[3])
        tag = sys.argv[4] if len(sys.argv) > 4 else "R"
        rows = load_frame()
        blocks = sample_random(rows, n, seed)
        json.dump([b for b in blocks], open(OUT / f"{tag}_blocks.json", "w"), ensure_ascii=False)
        build(blocks, tag)
    elif mode == "list":
        blocks = json.load(open(sys.argv[2], encoding="utf-8"))
        build(blocks, sys.argv[3])
