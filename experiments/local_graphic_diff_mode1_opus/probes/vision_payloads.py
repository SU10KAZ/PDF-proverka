#!/usr/bin/env python3
"""Targeted vision (§13): build the payload AFTER the local diff, never before.

For every case the payload is: a small LEFT crop, a small RIGHT crop, the bbox
of the change region and one short question.  Two full blocks are never sent.

The image-token estimate uses the formula fitted in the previous audit
(`image_tokens ≈ min(1.2014·⌈w/32⌉·⌈h/32⌉ + 48.67, 3051)`); it is a provider-
unconfirmed estimate and is labelled as such.
"""
from __future__ import annotations

import json
import math
import pathlib
import sys

import cv2
import fitz
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.run_benchmark import blocks_of  # noqa: E402

CELL = 0.35          # pt per pixel in the vision crop (~72 dpi on the sheet)
MAX_SIDE = 520       # px


def image_tokens(w, h):
    return min(3051.0, 1.2014 * math.ceil(w / 32) * math.ceil(h / 32) + 48.67)


def crop(block, rect, cell=CELL):
    page = PAGES.page(block.pdf, block.page_index)["page"]
    pm = page.get_pixmap(clip=fitz.Rect(*rect), matrix=fitz.Matrix(1 / cell, 1 / cell),
                         colorspace=fitz.csGRAY, alpha=False)
    img = np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()
    if max(img.shape) > MAX_SIDE:
        s = MAX_SIDE / max(img.shape)
        img = cv2.resize(img, None, fx=s, fy=s, interpolation=cv2.INTER_AREA)
    return img


def main():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    out_dir = ART / "vision_crops"
    out_dir.mkdir(parents=True, exist_ok=True)
    cases = []
    for f in sorted((ART / "diff_runs").glob("*.json")):
        r = json.loads(f.read_text(encoding="utf-8"))
        pid = r["pair_id"]
        route = r["route"]
        regions = r["change_regions"]
        if route == "MODE_2_REQUIRED":
            continue                                  # MODE 2's business
        if route == "VISION_REQUIRED":
            picks = regions[:1] if regions else []
            question = ("Это реальное изменение графики или различие экспорта/кадрирования "
                        "(растровая подложка, текст в кривых)?")
            if not picks:                              # no region: ask about the whole block
                picks = [{"bbox": bench[pid]["_whole"] if False else None}]
        else:
            picks = [x for x in regions if x["ink_pt"] <= 60.0][:2]
            question = ("Это реальное изменение графического элемента или различие "
                        "кадрирования/экспорта?")
        a, b = blocks_of(bench[pid])
        t = r["registration"]["transform"]
        for i, reg in enumerate(picks):
            bb = reg.get("bbox")
            if bb is None:
                bb = [b.bbox_vis[0], b.bbox_vis[1],
                      b.bbox_vis[0] + min(220.0, b.width), b.bbox_vis[1] + min(220.0, b.height)]
            pad = max(10.0, 0.6 * max(bb[2] - bb[0], bb[3] - bb[1]))
            rr = [bb[0] - pad, bb[1] - pad, bb[2] + pad, bb[3] + pad]
            lr = [(rr[0] - t["tx"]) / t["scale"], (rr[1] - t["ty"]) / t["scale"],
                  (rr[2] - t["tx"]) / t["scale"], (rr[3] - t["ty"]) / t["scale"]]
            try:
                L, R = crop(a, lr), crop(b, rr)
            except Exception as e:  # noqa: BLE001
                continue
            hh = min(L.shape[0], R.shape[0]); ww = min(L.shape[1], R.shape[1])
            L, R = L[:hh, :ww], R[:hh, :ww]
            pair_img = np.hstack([L, np.full((hh, 6), 128, np.uint8), R])
            name = f"{pid}_v{i}.png"
            cv2.imwrite(str(out_dir / name), pair_img)
            cases.append({
                "case_id": f"{pid}_v{i}",
                "pair_id": pid, "route": route,
                "gt_label": gt[pid]["human_label"],
                "region_bbox": [round(v, 2) for v in bb],
                "region_ink_pt": reg.get("ink_pt"),
                "region_type": reg.get("change_type"),
                "question": question,
                "image": str(out_dir / name),
                "crop_px": [int(ww), int(hh)],
                "image_tokens_estimate": round(2 * image_tokens(ww, hh), 1),
                "verdict": None, "verdict_by": None, "verdict_note": None,
            })
    (ART / "vision_results.json").write_text(json.dumps(
        {"probe": "vision_payloads", "research_only": True,
         "token_formula": "min(3051, 1.2014*ceil(w/32)*ceil(h/32)+48.67) per image; provider-unconfirmed",
         "cases": cases}, ensure_ascii=False, indent=1), encoding="utf-8")
    print("cases", len(cases))
    for c in cases:
        print(f"  {c['case_id']:32s} {c['route']:18s} ink={c['region_ink_pt']} px={c['crop_px']} "
              f"tok={c['image_tokens_estimate']}")


if __name__ == "__main__":
    main()
