"""VVG — the deterministic frame-mismatch detector (finding O13) as a gate signal.

`extract_block` builds the block rect from `page.rect` (display frame) and clips
`page.get_drawings()` / `page.get_text()`, which PyMuPDF returns in the mediabox frame.
On a page with /Rotate != 0 the description therefore describes a different region than
`page.get_pixmap(clip=rect)` renders.  Detecting that costs one attribute read.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_frame_signal
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
TRACK_A_DESC = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "descriptions"


def overlap(desc: dict) -> dict:
    pdf = desc["source"]["pdf"]
    page_index = desc["page_index"]
    bbox = desc["bbox_norm_on_page"]
    doc = fitz.open(pdf)
    page = doc[page_index]
    rot = page.rotation
    rect = fitz.Rect(bbox[0] * page.rect.width, bbox[1] * page.rect.height,
                     bbox[2] * page.rect.width, bbox[3] * page.rect.height)
    # what the description reads: rect interpreted in the MEDIABOX frame
    data_rect = rect
    # what the crop shows: rect in the DISPLAY frame, mapped back to the mediabox
    shown = rect * page.rotation_matrix if rot else rect
    inter = fitz.Rect(data_rect) & fitz.Rect(shown)
    a_data = data_rect.get_area()
    share = round(inter.get_area() / a_data, 4) if a_data else 0.0
    doc.close()
    return {"page_rotation": rot, "frame_overlap_share": share,
            "frame_mismatch": bool(rot != 0)}


def main() -> None:
    out = {}
    for pair_dir in sorted(TRACK_A_DESC.iterdir()):
        if not pair_dir.is_dir():
            continue
        for side in ("left", "right"):
            p = pair_dir / side / "vector_block.json"
            if not p.exists():
                continue
            d = json.loads(p.read_text(encoding="utf-8"))
            out[f"{pair_dir.name}:{side}"] = {"set": "track_a", **overlap(d)}
    index = json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]
    for block in index:
        d = json.loads((ROOT / block["description"]).read_text(encoding="utf-8"))
        out[block["id"]] = {"set": "fresh", **overlap(d)}
    n_mis = sum(1 for v in out.values() if v["frame_mismatch"])
    summary = {
        "blocks": len(out),
        "frame_mismatch_blocks": n_mis,
        "frame_mismatch_share": round(n_mis / len(out), 4),
        "track_a_mismatch": sum(1 for v in out.values() if v["set"] == "track_a" and v["frame_mismatch"]),
        "fresh_mismatch": sum(1 for v in out.values() if v["set"] == "fresh" and v["frame_mismatch"]),
    }
    (ART / "vvg_frame_signal.json").write_text(
        json.dumps({"summary": summary, "blocks": out}, ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    for k, v in out.items():
        if v["frame_mismatch"]:
            print(f"  {k:<40} rot={v['page_rotation']:>3} overlap={v['frame_overlap_share']}")


if __name__ == "__main__":
    main()
