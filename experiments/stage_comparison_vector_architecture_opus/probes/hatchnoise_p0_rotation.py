"""P0 (side finding) — page rotation breaks the block window.

page.get_drawings() and page.get_text() return coordinates in the UNROTATED page space,
while page.rect is the ROTATED rect.  Track A's extractor builds block_rect from page.rect,
so on a rotated page the clip window lands somewhere else entirely.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p0_rotation
"""
from __future__ import annotations

import collections
import json
import time
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C

OUT = C.ART / "hatchnoise_p0_rotation.json"


def main() -> None:
    pairs = json.loads((C.ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text(encoding="utf-8"))
    per_pair = []
    for pair in pairs["pairs"]:
        row = {"pair_id": pair["pair_id"], "discipline": pair["discipline"]}
        for side in ("left", "right"):
            document = fitz.open(C.ROOT / pair[side]["pdf"])
            page = document[pair[side]["page_index"]]
            bbox = pair[side]["bbox_norm"]
            rect_rot = fitz.Rect(
                bbox[0] * page.rect.width, bbox[1] * page.rect.height,
                bbox[2] * page.rect.width, bbox[3] * page.rect.height,
            )
            rect_unrot = rect_rot * page.derotation_matrix
            drawings = page.get_drawings()
            hit_naive = sum(1 for d in drawings if d.get("rect") is not None and fitz.Rect(d["rect"]).intersects(rect_rot))
            hit_fixed = sum(1 for d in drawings if d.get("rect") is not None and fitz.Rect(d["rect"]).intersects(rect_unrot))
            row[side] = {
                "rotation": page.rotation,
                "page_rect": [page.rect.width, page.rect.height],
                "drawings_page": len(drawings),
                "drawings_in_block_trackA_window": hit_naive,
                "drawings_in_block_derotated_window": hit_fixed,
                "ratio": round(hit_naive / max(hit_fixed, 1), 4),
            }
            document.close()
        per_pair.append(row)

    # corpus census of page rotation
    counter: collections.Counter[int] = collections.Counter()
    docs_with_rotation = 0
    docs = 0
    t0 = time.time()
    for path in sorted(C.ROOT.glob("projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.pdf")):
        try:
            document = fitz.open(path)
        except Exception:
            continue
        docs += 1
        rotations = [document[i].rotation for i in range(len(document))]
        counter.update(rotations)
        if any(r % 360 for r in rotations):
            docs_with_rotation += 1
        document.close()
        if time.time() - t0 > 300:
            break

    payload = {
        "probe": "hatchnoise_p0_rotation",
        "explanation": (
            "page.get_drawings()/get_text() coordinates are in the unrotated page box; page.rect is rotated. "
            "Building block_rect from page.rect (extractor.extract_block) therefore clips the wrong window "
            "on any page with rotation != 0."
        ),
        "benchmark_pairs": per_pair,
        "corpus_documents_scanned": docs,
        "corpus_documents_with_any_rotated_page": docs_with_rotation,
        "corpus_page_rotation_histogram": dict(sorted(counter.items())),
        "corpus_pages_total": sum(counter.values()),
        "corpus_pages_rotated": sum(v for k, v in counter.items() if k % 360),
        "elapsed_s": round(time.time() - t0, 1),
    }
    C.write_json(OUT, payload)
    for row in per_pair:
        print(f"{row['pair_id']:24s} rot L={row['left']['rotation']:3d} R={row['right']['rotation']:3d} "
              f"| trackA window {row['left']['drawings_in_block_trackA_window']:7d} vs derotated "
              f"{row['left']['drawings_in_block_derotated_window']:7d}")
    print(json.dumps({k: v for k, v in payload.items() if k not in {"benchmark_pairs"}}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
