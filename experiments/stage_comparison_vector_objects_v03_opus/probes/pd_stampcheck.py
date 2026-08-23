# -*- coding: utf-8 -*-
"""pd_stampcheck — визуальная проверка поля stamp_data.stage: рендерим сам штамп."""
from __future__ import annotations
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa

OUTDIR = Path(__file__).resolve().parents[1] / "artifacts" / "pd_crops" / "stamps"


def stamp_pages(rj, want_stage=None, limit=3):
    data = json.load(open(rj, encoding="utf-8"))
    picked = []
    for pg in data["pages"]:
        for b in pg.get("blocks") or []:
            if b.get("block_type") != "image":
                continue
            sd = b.get("stamp_data") or b.get("ocr_json")
            if isinstance(sd, str):
                try:
                    sd = json.loads(sd)
                except Exception:
                    sd = None
            if not isinstance(sd, dict):
                continue
            st = (sd.get("stage") or "").strip()
            if want_stage and st != want_stage:
                continue
            picked.append((pg, b, sd))
            break
        if len(picked) >= limit:
            break
    return picked


def main(rj, tag, want_stage=None, limit=3):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    blocks = {b.block_id: b for b in F.iter_prepared_blocks(rj)}
    out = []
    for pg, b, sd in stamp_pages(rj, want_stage, limit):
        pb = blocks.get(str(b.get("id")))
        if pb is None:
            continue
        png = OUTDIR / f"{tag}_p{pg['page_number']}_{pb.block_id[:8]}.png"
        try:
            F.render_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h,
                           target_px=1600, out_png=png)
        except Exception as e:
            print("ERR", e); continue
        out.append({"png": str(png), "page": pg["page_number"], "stamp": sd})
        print(png, json.dumps(sd, ensure_ascii=False))
    return out


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None,
         int(sys.argv[4]) if len(sys.argv) > 4 else 3)
