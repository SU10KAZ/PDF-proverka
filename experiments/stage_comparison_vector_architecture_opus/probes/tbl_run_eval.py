"""tbl_run_eval — reconstruct tables for the evaluated blocks, diff the two Track A pairs,
and render crops for ground-truth reading.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_run_eval
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_diff as D  # noqa: E402

OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
CROPS = OUT / "tbl_crops"

ALIA = "projects_v2/objects/214_Alia_ASTERUS/disciplines"

# fresh table-bearing blocks (not part of Track A's benchmark)
FRESH = [
    ("fresh_sot_k7_cable_journal", f"{ALIA}/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf", 17),
    ("fresh_sot_k7_specification", f"{ALIA}/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf", 18),
    ("fresh_aps_k3_specification", f"{ALIA}/SS/documents/13АВ-РД-АПЗ.АПС-К3 V1/versions/v001/02_work/document.pdf", 33),
    ("fresh_askuvt_cable_journal", f"{ALIA}/SS/documents/13АВ-РД-АСКУВТ/versions/v001/02_work/document.pdf", 34),
    ("fresh_kk_pa_specification", f"{ALIA}/SS/documents/13АВ-РД-КК-ПА/versions/v001/02_work/document.pdf", 21),
]


def render(page: fitz.Page, rect, path: Path, zoom: float = 3.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    r = fitz.Rect(*rect)
    r.x0 -= 4; r.y0 -= 4; r.x1 += 4; r.y1 += 4
    zoom = min(zoom, 5200.0 / max(r.width, r.height, 1.0))
    page.get_pixmap(clip=r, matrix=fitz.Matrix(zoom, zoom)).save(str(path))


def main() -> None:
    CROPS.mkdir(parents=True, exist_ok=True)
    result: dict = {"blocks": {}, "diffs": {}, "timing": {}}

    pairs = json.loads((ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]
    wanted = {"ss_table_graphic", "eom_singleline_changed"}

    for pair in pairs:
        if pair["pair_id"] not in wanted:
            continue
        sides = {}
        for side in ("left", "right"):
            info = pair[side]
            doc = fitz.open(str(ROOT / info["pdf"]))
            page = doc[info["page_index"]]
            w, h = page.rect.width, page.rect.height
            b = info["bbox_norm"]
            region = (b[0] * w, b[1] * h, b[2] * w, b[3] * h)
            t0 = time.time()
            drawings = page.get_drawings()
            t_dr = time.time() - t0

            t0 = time.time()
            clipped = T.reconstruct(page, drawings=drawings, region=region, clip_to_region=True)
            t_clip = time.time() - t0
            t0 = time.time()
            framed = T.reconstruct(page, drawings=drawings, region=region, clip_to_region=False)
            t_frame = time.time() - t0

            key = f"{pair['pair_id']}_{side}"
            for tag, tables in (("clip", clipped), ("frame", framed)):
                for i, t in enumerate(tables):
                    render(page, t["bbox"], CROPS / f"{key}_{tag}_{i}.png")
            render(page, region, CROPS / f"{key}_block.png")

            sides[side] = {
                "pdf": info["pdf"], "page_index": info["page_index"],
                "page_rotation": page.rotation,
                "region_pt": [round(v, 2) for v in region],
                "clip_mode_tables": [T.table_to_dict(t) | {"open_sides": t["open_sides"]} for t in clipped],
                "frame_mode_tables": [T.table_to_dict(t) | {"open_sides": t["open_sides"]} for t in framed],
                "clip_mode_rows": [T.table_rows(t) for t in clipped],
                "frame_mode_rows": [T.table_rows(t) for t in framed],
                "timing_s": {"get_drawings": round(t_dr, 2), "clip": round(t_clip, 2), "frame": round(t_frame, 2)},
                "_clip_objs": clipped, "_frame_objs": framed,
            }
            doc.close()

        diffs = {}
        for mode in ("clip", "frame"):
            lt = sides["left"][f"_{mode}_objs"]
            rt = sides["right"][f"_{mode}_objs"]
            if not lt or not rt:
                diffs[mode] = {"verdict": "NO_TABLE_DETECTED",
                               "left_tables": len(lt), "right_tables": len(rt)}
                continue
            L, R = lt[0], rt[0]
            open_any = {k: (L["open_sides"][k] or R["open_sides"][k]) for k in L["open_sides"]}
            d = D.diff_tables(L, R)
            d["open_sides_either"] = open_any
            if any(open_any.values()):
                d["gated_verdict"] = "TABLE_CLIPPED_NOT_COMPARABLE"
                d["gated_sentences"] = [
                    "Таблица обрезана рамкой блока (" +
                    ", ".join(k for k, v in open_any.items() if v) +
                    "); построчное сравнение не проводится."]
            else:
                d["gated_verdict"] = d["verdict"]
                d["gated_sentences"] = d["sentences"]
            diffs[mode] = d
        for side in sides:
            sides[side].pop("_clip_objs"); sides[side].pop("_frame_objs")
        result["blocks"][pair["pair_id"]] = sides
        result["diffs"][pair["pair_id"]] = diffs

    for name, pdf, page_index in FRESH:
        doc = fitz.open(str(ROOT / pdf))
        page = doc[page_index]
        t0 = time.time()
        drawings = page.get_drawings()
        t_dr = time.time() - t0
        t0 = time.time()
        tables = T.reconstruct(page, drawings=drawings)
        t_tab = time.time() - t0
        for i, t in enumerate(tables):
            render(page, t["bbox"], CROPS / f"{name}_{i}.png")
        result["blocks"][name] = {
            "pdf": pdf, "page_index": page_index, "page_rotation": page.rotation,
            "tables": [T.table_to_dict(t) | {"open_sides": t["open_sides"]} for t in tables],
            "rows": [T.table_rows(t) for t in tables],
            "timing_s": {"get_drawings": round(t_dr, 2), "reconstruct": round(t_tab, 2)},
        }
        doc.close()

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "tbl_tables.json").write_text(json.dumps(result, ensure_ascii=False, indent=1))

    print("=== diffs ===")
    for pid, modes in result["diffs"].items():
        for mode, d in modes.items():
            print(f"\n--- {pid} [{mode}] verdict={d.get('verdict')} gated={d.get('gated_verdict')}")
            print("    shapes", d.get("left_shape"), "->", d.get("right_shape"),
                  "open", d.get("open_sides_either"))
            for s in (d.get("gated_sentences") or [])[:12]:
                print("    *", s)
    print("\n=== fresh blocks ===")
    for name, _, _ in FRESH:
        b = result["blocks"][name]
        for t in b["tables"]:
            print(f"{name}: {t['rows']}x{t['cols']} cells={len(t['cells'])} "
                  f"filled={sum(1 for c in t['cells'] if c['text'])} open={t['open_sides']}")


if __name__ == "__main__":
    main()
