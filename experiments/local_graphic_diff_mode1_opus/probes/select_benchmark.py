#!/usr/bin/env python3
"""Select the MODE 1 benchmark from the mined candidates.

Stratified over the raster-change signal, disciplines and the special cases the
brief asks for (rotated page, dense drawing, different PDF packing, text-only,
table-only, unchanged, strongly rebuilt).  The raster signal is used only to
*stratify* — the label of every pair comes from a human looking at the renders.

Output: artifacts/benchmark_pairs.json
"""
from __future__ import annotations

import json
import pathlib
import re
import sys
from collections import Counter

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

TABLE_RE = re.compile(r"(специфик|ведомост|таблиц|журнал|экспликац|перечен)", re.I)


def text_change_share(p) -> float | None:
    """How much of the raster change sits inside text boxes (both sides)."""
    from experiments.local_graphic_diff_mode1_opus.probes.gt_tool import block_of, GT_CELL
    from experiments.local_graphic_diff_mode1_opus.m1.core import render_gray, text_spans
    import cv2

    a = block_of(p["pdf_left"], p["page_index_left"], p["bbox_left"])
    b = block_of(p["pdf_right"], p["page_index_right"], p["bbox_right"])
    cell = 1.0
    ga, gb = render_gray(a, cell), render_gray(b, cell)
    h = min(ga.shape[0], gb.shape[0]); w = min(ga.shape[1], gb.shape[1])
    A = (ga[:h, :w] < 200).astype(np.uint8); B = (gb[:h, :w] < 200).astype(np.uint8)
    try:
        win = cv2.createHanningWindow((w, h), cv2.CV_32F)
        (dx, dy), _ = cv2.phaseCorrelate(A.astype(np.float32) * win, B.astype(np.float32) * win)
        A = cv2.warpAffine(A, np.float32([[1, 0, dx], [0, 1, dy]]), (w, h), flags=cv2.INTER_NEAREST)
    except Exception:
        pass
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    ch = ((A & ~cv2.dilate(B, ker)) | (B & ~cv2.dilate(A, ker))).astype(np.uint8)
    if ch.sum() == 0:
        return None
    tm = np.zeros((h, w), np.uint8)
    for blk, boxes in ((a, text_spans(a)), (b, text_spans(b))):
        x0, y0 = blk.bbox_vis[0], blk.bbox_vis[1]
        for s in boxes:
            bb = s["bbox"]
            X0 = max(0, int((bb[0] - 1.5 - x0) / cell)); Y0 = max(0, int((bb[1] - 1.5 - y0) / cell))
            X1 = min(w, int((bb[2] + 1.5 - x0) / cell) + 1); Y1 = min(h, int((bb[3] + 1.5 - y0) / cell) + 1)
            if X1 > X0 and Y1 > Y0:
                tm[Y0:Y1, X0:X1] = 1
    return round(float((ch & tm).sum()) / float(ch.sum()), 4)


def main() -> None:
    sig = json.loads((ART / "pair_signals.json").read_text(encoding="utf-8"))["pairs"]
    ps = [p for p in sig if "error" not in p and p.get("raster")]
    for p in ps:
        p["cf"] = p["raster"]["changed_fraction"]
        p["max_ink"] = max(p["ink_pt"])
        p["seg_ratio"] = abs(p["segments"][0] - p["segments"][1]) / max(1, max(p["segments"]))
        p["is_table_label"] = bool(TABLE_RE.search((p["label_left"] or "") + " " + (p["label_right"] or "")))
        p["rotated"] = bool(p["page_rotation"][0] or p["page_rotation"][1])

    picked: dict[str, dict] = {}
    def take(bucket, items, n, key=None):
        cnt = 0
        seen_docs = Counter(v["doc"] for v in picked.values())
        for p in sorted(items, key=key or (lambda x: -x["max_ink"])):
            pid = f"{p['doc']}|{p['version_left']}->{p['version_right']}|{p['block_left']}"
            if pid in picked:
                continue
            if seen_docs[p["doc"]] >= 4:
                continue
            q = dict(p); q["bucket"] = bucket
            picked[pid] = q
            seen_docs[p["doc"]] += 1
            cnt += 1
            if cnt >= n:
                break

    # graphics only: a block with almost no vector ink is a different question
    graphic = [p for p in ps if p["max_ink"] > 500]

    take("unchanged", [p for p in graphic if p["cf"] == 0.0], 6)
    take("tiny_change", [p for p in graphic if 0 < p["cf"] <= 1e-3], 8)
    take("small_change", [p for p in graphic if 1e-3 < p["cf"] <= 6e-3], 8)
    take("medium_change", [p for p in graphic if 6e-3 < p["cf"] <= 0.02], 6)
    take("dense_small_change", [p for p in graphic if p["max_ink"] > 100_000 and p["cf"] <= 0.02], 4)
    take("repack", [p for p in graphic if p["cf"] < 2e-3 and p["seg_ratio"] > 0.10 and max(p["segments"]) > 200], 4,
         key=lambda x: -x["seg_ratio"])
    take("rotated_page", [p for p in graphic if p["rotated"]], 3)
    take("table_like", [p for p in graphic if p["is_table_label"] and p["cf"] <= 0.05], 4)
    take("strong_redesign", [p for p in graphic if p["cf"] > 0.25], 5)
    take("large_change", [p for p in graphic if 0.08 < p["cf"] <= 0.25], 4)

    rows = []
    for pid, p in picked.items():
        rows.append({
            "pair_id": f"{p['discipline'].lower()}_{p['bucket']}_{len(rows)+1:02d}",
            "bucket": p["bucket"],
            "origin": "real_revision_pair",
            "stage_claim": "revision of the same document (RD); NOT a documented P->RD pair",
            "doc": p["doc"], "discipline": p["discipline"],
            "version_left": p["version_left"], "version_right": p["version_right"],
            "pdf_left": p["pdf_left"], "pdf_right": p["pdf_right"],
            "page_index_left": p["page_index_left"], "page_index_right": p["page_index_right"],
            "sheet_no": p["sheet_no"], "sheet_name": p["sheet_name"],
            "block_left": p["block_left"], "block_right": p["block_right"],
            "bbox_left": p["bbox_left"], "bbox_right": p["bbox_right"],
            "bbox_iou": p["bbox_iou"],
            "label": p["label_right"] or p["label_left"],
            "signals": {"raster_changed_fraction": p["cf"], "ink_pt": p["ink_pt"],
                        "segments": p["segments"], "texts": p["texts"],
                        "page_rotation": p["page_rotation"], "page_images": p["page_images"],
                        "seg_ratio": round(p["seg_ratio"], 4), "width_pt": p["width_pt"],
                        "height_pt": p["height_pt"]},
        })
    print("computing text-change share...", flush=True)
    for r in rows:
        p = next(x for x in ps if x["block_left"] == r["block_left"] and x["doc"] == r["doc"])
        try:
            r["signals"]["text_change_share"] = text_change_share(p)
        except Exception as e:  # noqa: BLE001
            r["signals"]["text_change_share"] = None
            r["signals"]["text_change_error"] = str(e)[:120]
    (ART / "benchmark_pairs.json").write_text(json.dumps(
        {"probe": "select_benchmark", "research_only": True,
         "note": "все пары — реальные ревизии одного документа; ни одна не выдаётся за П↔РД",
         "n": len(rows), "pairs": rows}, ensure_ascii=False, indent=1), encoding="utf-8")
    print("selected", len(rows))
    print(Counter(r["bucket"] for r in rows).most_common())
    print(Counter(r["discipline"] for r in rows).most_common())


if __name__ == "__main__":
    main()
