#!/usr/bin/env python3
"""Controls: dilution, crop boundary drift, CAD repacking.

* DILUTION (§18) — the same real change is compared inside a tight crop, a
  medium crop and the whole prepared block.  If the change disappears as the
  amount of unchanged graphics around it grows, MODE 1 is unusable.
* CROP BOUNDARY (§9) — identical content, only the crop rectangle drifts
  (shift and shrink, 0.25 %…10 %).  Every published region here is a false
  positive by construction.
* REPACKING (§19) — real pairs where the visible drawing is the same but the
  PDF primitives are packed differently.

Outputs: dilution_results.json, crop_boundary_results.json, repack_results.json
"""
from __future__ import annotations

import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES, Block, block_from_record  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.m1.diff import local_diff  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.run_benchmark import blocks_of  # noqa: E402


def overlaps(a, b, pad=4.0):
    return not (a[2] + pad < b[0] or a[0] - pad > b[2] or a[3] + pad < b[1] or a[1] - pad > b[3])


def sub_block(block: Block, rect, name) -> Block:
    return Block(block.pdf, block.page_index, name, [round(v, 3) for v in rect], label=block.label)


def dilution():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    picks = [pid for pid, g in gt.items()
             if g["human_label"] == "LOCAL_CHANGE" and g["eye_verified"] and g["scale"] in ("local", "many_local")
             and g["gt_regions"]]
    rows = []
    for pid in picks:
        g = gt[pid]
        p = bench[pid]
        a, b = blocks_of(p)
        graphic_regions = [r for r in g["gt_regions"] if (r.get("text_share") or 0.0) < 0.3]
        if not graphic_regions:
            print("skip (no graphic-only GT region):", pid, flush=True)
            continue
        target = max(graphic_regions, key=lambda r: r["cells"])["bbox_pt"]
        tw, th = target[2] - target[0], target[3] - target[1]
        cx, cy = (target[0] + target[2]) / 2, (target[1] + target[3]) / 2
        ladders = []
        for name, factor in (("tight", 1.3), ("medium", 4.0), ("wide", 12.0)):
            hw, hh = tw * factor / 2, th * factor / 2
            hw = max(hw, 25.0); hh = max(hh, 25.0)
            rect_r = [cx - hw, cy - hh, cx + hw, cy + hh]
            # keep inside the prepared blocks
            rect_r = [max(rect_r[0], b.bbox_vis[0]), max(rect_r[1], b.bbox_vis[1]),
                      min(rect_r[2], b.bbox_vis[2]), min(rect_r[3], b.bbox_vis[3])]
            dx = a.bbox_vis[0] - b.bbox_vis[0]
            dy = a.bbox_vis[1] - b.bbox_vis[1]
            rect_l = [rect_r[0] + dx, rect_r[1] + dy, rect_r[2] + dx, rect_r[3] + dy]
            try:
                t0 = time.time()
                out = local_diff(sub_block(a, rect_l, "dil_l"), sub_block(b, rect_r, "dil_r"))
                hit = any(overlaps(target, r["bbox"]) for r in out["change_regions"])
                ladders.append({
                    "crop": name, "factor": factor,
                    "area_ratio": round(((rect_r[2] - rect_r[0]) * (rect_r[3] - rect_r[1])) /
                                        max(1e-6, tw * th), 2),
                    "route": out["route"], "verdict": out["verdict"],
                    "regions": len(out["change_regions"]),
                    "target_found": bool(hit),
                    "changed_ink_fraction": out["diff"]["changed_ink_fraction"],
                    "left_ink_pt": out["registration"]["left_ink_pt"],
                    "latency_s": round(time.time() - t0, 2),
                })
            except Exception as e:  # noqa: BLE001
                ladders.append({"crop": name, "error": str(e)[:120]})
        # and the full prepared block, as prepared
        full = json.loads((ART / "diff_runs" / f"{pid}.json").read_text(encoding="utf-8"))
        hit_full = any(overlaps(target, r["bbox"]) for r in full["change_regions"])
        ladders.append({
            "crop": "prepared_block", "factor": None,
            "area_ratio": round(((b.bbox_vis[2] - b.bbox_vis[0]) * (b.bbox_vis[3] - b.bbox_vis[1])) /
                                max(1e-6, tw * th), 2),
            "route": full["route"], "verdict": full["verdict"],
            "regions": len(full["change_regions"]),
            "target_found": bool(hit_full),
            "changed_ink_fraction": full["diff"]["changed_ink_fraction"],
            "left_ink_pt": full["registration"]["left_ink_pt"],
            "latency_s": full["latency_s"],
        })
        rows.append({"pair_id": pid, "target_bbox": target, "ladder": ladders})
        print(pid, [(l["crop"], l.get("target_found"), l.get("regions")) for l in ladders], flush=True)
    (ART / "dilution_results.json").write_text(json.dumps(
        {"probe": "controls.dilution", "research_only": True, "pairs": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")


def crop_boundary():
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    # identical content: compare a block against ITSELF with a drifting crop
    picks = [pid for pid, g in gt.items() if g["human_label"] in ("NO_CHANGE", "CROP_DIFFERENCE")][:12]
    rows = []
    for pid in picks:
        p = bench[pid]
        _, b = blocks_of(p)
        w, h = b.width, b.height
        for kind in ("shift", "shrink"):
            for frac in (0.0025, 0.01, 0.02, 0.05, 0.10):
                if kind == "shift":
                    rect = [b.bbox_vis[0] + w * frac, b.bbox_vis[1] + h * frac,
                            b.bbox_vis[2] + w * frac, b.bbox_vis[3] + h * frac]
                else:
                    rect = [b.bbox_vis[0] + w * frac, b.bbox_vis[1] + h * frac,
                            b.bbox_vis[2] - w * frac, b.bbox_vis[3] - h * frac]
                try:
                    out = local_diff(b, sub_block(b, rect, "drift"))
                    rows.append({
                        "pair_id": pid, "kind": kind, "fraction": frac,
                        "route": out["route"], "verdict": out["verdict"],
                        "published": len(out["change_regions"]),
                        "suppressed_crop_artifact": sum(1 for r in out["suppressed_regions"]
                                                        if r.get("suppressed_by") in ("CROP_ARTIFACT", "OUTSIDE_COMMON_AREA")),
                        "sym_cov": out["registration"]["coverage"]["sym_cov"],
                        "transform": out["registration"]["transform"],
                        "published_ink_pt": [r["ink_pt"] for r in out["change_regions"][:5]],
                    })
                except Exception as e:  # noqa: BLE001
                    rows.append({"pair_id": pid, "kind": kind, "fraction": frac, "error": str(e)[:120]})
        print("drift", pid, sum(1 for r in rows if r["pair_id"] == pid and r.get("published")), flush=True)
    bad = [r for r in rows if r.get("published")]
    (ART / "crop_boundary_results.json").write_text(json.dumps(
        {"probe": "controls.crop_boundary", "research_only": True,
         "runs": len(rows), "runs_with_published_regions": len(bad), "rows": rows},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print("crop boundary: runs", len(rows), "with published regions", len(bad))


def repacking():
    sig = json.loads((ART / "pair_signals.json").read_text(encoding="utf-8"))["pairs"]
    ps = [p for p in sig if "error" not in p and p.get("raster")]
    cands = [p for p in ps
             if p["raster"]["changed_fraction"] < 2e-3
             and max(p["segments"]) > 300
             and min(p["segments"]) > 0
             and abs(p["segments"][0] - p["segments"][1]) / max(1, max(p["segments"])) > 0.15
             and max(p["texts"]) > 0 and min(p["texts"]) > 0]
    seen = set()
    rows = []
    for p in cands:
        key = (p["doc"], p["sheet_no"])
        if key in seen:
            continue
        seen.add(key)
        la = PAGES.page(p["pdf_left"], p["page_index_left"])["rect"]
        rb = PAGES.page(p["pdf_right"], p["page_index_right"])["rect"]
        a = block_from_record(p["pdf_left"], {"coords_norm": p["bbox_left"], "page_index": p["page_index_left"],
                                              "id": p["block_left"]}, la)
        b = block_from_record(p["pdf_right"], {"coords_norm": p["bbox_right"], "page_index": p["page_index_right"],
                                               "id": p["block_right"]}, rb)
        try:
            out = local_diff(a, b)
        except Exception as e:  # noqa: BLE001
            rows.append({"doc": p["doc"], "sheet": p["sheet_no"], "error": str(e)[:120]})
            continue
        rows.append({
            "doc": p["doc"], "discipline": p["discipline"], "sheet": p["sheet_no"],
            "segments": p["segments"], "ink_pt": p["ink_pt"],
            "segment_ratio": round(abs(p["segments"][0] - p["segments"][1]) / max(1, max(p["segments"])), 3),
            "raster_changed_fraction": p["raster"]["changed_fraction"],
            "route": out["route"], "verdict": out["verdict"],
            "published": len(out["change_regions"]),
            "published_ink_pt": [r["ink_pt"] for r in out["change_regions"][:5]],
            "sym_cov": out["registration"]["coverage"]["sym_cov"],
        })
        print("repack", rows[-1]["discipline"], rows[-1]["sheet"], rows[-1]["segments"],
              rows[-1]["route"], rows[-1]["published"], flush=True)
        if len(rows) >= 12:
            break
    (ART / "repack_results.json").write_text(json.dumps(
        {"probe": "controls.repacking", "research_only": True, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "dilution"):
        dilution()
    if which in ("all", "crop"):
        crop_boundary()
    if which in ("all", "repack"):
        repacking()
