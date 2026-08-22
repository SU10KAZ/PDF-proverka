"""Render raster crops of the descriptor-collision representatives found by
falsify_symbol_collisions.py so a human can confirm that two components sharing
one generic-topology descriptor really are different objects.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_render_collisions \
      --json falsify_sym_ss_sot_p7.json --top 8 --outdir falsify_crops/ss_sot_p7
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"


def crop(page, bbox, out: Path, pad: float = 6.0, target_px: int = 260) -> None:
    rect = fitz.Rect(bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad)
    zoom = target_px / max(rect.width, rect.height, 1e-6)
    zoom = min(zoom, 40)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect)
    out.parent.mkdir(parents=True, exist_ok=True)
    pix.save(out)


def redraw(reps, out: Path, cell: int = 200, gap: int = 16) -> None:
    """Draw ONLY the component segments the descriptor actually saw, side by side.

    This removes page context so a human sees exactly the geometry that produced
    the identical generic-topology descriptor.
    """
    n = len(reps)
    doc = fitz.open()
    page = doc.new_page(width=n * (cell + gap) + gap, height=cell + 2 * gap)
    shape = page.new_shape()
    for i, rep in enumerate(reps):
        ox = gap + i * (cell + gap)
        for seg in rep.get("segments_norm", []):
            x0, y0, x1, y1 = seg
            shape.draw_line(
                fitz.Point(ox + x0 * cell, gap + y0 * cell),
                fitz.Point(ox + x1 * cell, gap + y1 * cell),
            )
        shape.draw_rect(fitz.Rect(ox - 3, gap - 3, ox + cell + 3, gap + cell + 3))
    shape.finish(width=0.7, color=(0, 0, 0))
    shape.commit()
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
    out.parent.mkdir(parents=True, exist_ok=True)
    pix.save(out)
    doc.close()


def strip(page, reps, out: Path, pad: float = 6.0, cell: int = 220) -> None:
    """Render several component crops side by side into one PNG."""
    tiles = []
    for rep in reps:
        b = rep["bbox"]
        rect = fitz.Rect(b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad)
        zoom = min(cell / max(rect.width, rect.height, 1e-6), 40)
        tiles.append(page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect))
    if not tiles:
        return
    gap = 12
    width = sum(t.width for t in tiles) + gap * (len(tiles) + 1)
    height = max(t.height for t in tiles) + gap * 2
    canvas = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, width, height), False)
    canvas.clear_with(255)
    x = gap
    for t in tiles:
        t.set_origin(x, gap)
        canvas.copy(t, t.irect)
        x += t.width + gap
    out.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True)
    ap.add_argument("--top", type=int, default=8)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--mirror-top", type=int, default=4)
    args = ap.parse_args()

    data = json.loads((ART / args.json).read_text(encoding="utf-8"))
    doc = fitz.open(ROOT / data["pdf"])
    page = doc[data["page_index"]]
    outdir = ART / args.outdir
    index = []
    for n, coll in enumerate(data["collisions"][: args.top], 1):
        reps = coll["representatives"]
        name = f"collision_{n:02d}.png"
        strip(page, reps, outdir / name, pad=2.0)
        redraw(reps, outdir / f"collision_{n:02d}_isolated.png")
        index.append(
            {
                "png": str((outdir / name).relative_to(ART)),
                "l3_key": coll["l3_key"],
                "n_instances": coll["n_instances"],
                "n_distinct_shapes": coll["n_distinct_shapes"],
                "representatives": [
                    {
                        "bbox": r["bbox"],
                        "n_segments": r["n_segments"],
                        "n_nodes": r["n_nodes"],
                        "endpoints": r["endpoints"],
                        "branch_points": r["branch_points"],
                        "cycles": r["cycles"],
                        "degree_histogram": r["degree_histogram"],
                        "kinds": r["kinds"],
                        "near_text": r["near_text"],
                    }
                    for r in reps
                ],
            }
        )
    for n, pair in enumerate(data.get("mirror_pairs", [])[: args.mirror_top], 1):
        name = f"mirror_{n:02d}.png"
        strip(page, [pair["a"], pair["b"]], outdir / name, pad=2.0)
        redraw([pair["a"], pair["b"]], outdir / f"mirror_{n:02d}_isolated.png")
        index.append(
            {
                "png": str((outdir / name).relative_to(ART)),
                "kind": "mirror_pair",
                "a": {k: pair["a"][k] for k in ("bbox", "n_segments", "endpoints", "branch_points", "cycles", "near_text")},
                "b": {k: pair["b"][k] for k in ("bbox", "n_segments", "endpoints", "branch_points", "cycles", "near_text")},
                "same_l3_key": pair["a"]["l3_key"] == pair["b"]["l3_key"],
            }
        )
    for n, pair in enumerate(data.get("rotation_twins", [])[: args.mirror_top], 1):
        name = f"rotation_{n:02d}.png"
        strip(page, [pair["a"], pair["b"]], outdir / name, pad=2.0)
        redraw([pair["a"], pair["b"]], outdir / f"rotation_{n:02d}_isolated.png")
        index.append(
            {
                "png": str((outdir / name).relative_to(ART)),
                "kind": "rotation_twin",
                "degrees": pair["degrees"],
                "direct_shape_similarity": pair["direct_shape_similarity"],
                "rotated_shape_similarity": pair["rotated_shape_similarity"],
                "same_l3_key": pair["same_l3_key"],
                "a": {k: pair["a"][k] for k in ("bbox", "n_segments", "endpoints", "branch_points", "cycles", "near_text")},
                "b": {k: pair["b"][k] for k in ("bbox", "n_segments", "endpoints", "branch_points", "cycles", "near_text")},
            }
        )
    (outdir / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")
    print("wrote", outdir)


if __name__ == "__main__":
    main()
