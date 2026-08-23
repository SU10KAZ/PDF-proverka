# -*- coding: utf-8 -*-
"""pd_stage_audit — случайная выборка штампов по заявленной стадии, рендер для визуальной сверки."""
from __future__ import annotations
import json, random, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa

BASE = Path(__file__).resolve().parents[1]
CENSUS = json.load(open(BASE / "artifacts" / "pd_stage_census.json", encoding="utf-8"))
OUT = BASE / "artifacts" / "pd_crops" / "stage_audit"


def pool(stage, seed=7, n=8, exclude_docs=()):
    rows = []
    for d in CENSUS["documents"]:
        if not d.get("pdf_exists"):
            continue
        if d["doc_id"] in exclude_docs:
            continue
        if stage not in (d.get("stage_hist") or {}):
            continue
        pages = [s["page_number"] for s in d["sheets"] if s.get("stage") == stage]
        for p in pages:
            rows.append((d["result_json"], d["doc_id"], p))
    random.Random(seed).shuffle(rows)
    return rows[:n]


def render(rows, tag):
    OUT.mkdir(parents=True, exist_ok=True)
    res = []
    for rj, doc_id, page in rows:
        data = json.load(open(ROOT / rj, encoding="utf-8"))
        blocks = {b.block_id: b for b in F.iter_prepared_blocks(ROOT / rj)}
        for pg in data["pages"]:
            if pg.get("page_number") != page:
                continue
            for b in pg.get("blocks") or []:
                if b.get("block_type") != "image":
                    continue
                sd = b.get("stamp_data") or b.get("ocr_json")
                if isinstance(sd, str):
                    try:
                        sd = json.loads(sd)
                    except Exception:
                        sd = None
                if not isinstance(sd, dict) or not sd.get("stage"):
                    continue
                pb = blocks.get(str(b.get("id")))
                if pb is None:
                    continue
                png = OUT / f"{tag}_{doc_id[:18].replace('/','_')}_p{page}.png"
                try:
                    F.render_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h,
                                   target_px=1500, out_png=png)
                except Exception as e:
                    print("ERR", e)
                    break
                res.append({"png": str(png.relative_to(BASE)), "doc": doc_id, "page": page,
                            "claimed_stage": sd.get("stage"), "sheet_name": sd.get("sheet_name"),
                            "document_code": sd.get("document_code")})
                print(png.name, "|", sd.get("stage"), "|", sd.get("document_code"))
                break
            break
    return res


if __name__ == "__main__":
    stage = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 7
    render(pool(stage, seed, n), f"claim{stage}")
