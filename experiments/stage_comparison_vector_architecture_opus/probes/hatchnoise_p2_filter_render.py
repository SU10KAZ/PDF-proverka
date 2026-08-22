"""P2 — apply the discipline-free filter and RENDER what it keeps vs what it throws away.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2_filter_render [block ...]

Writes artifacts/hatchnoise/<block>/*.png and artifacts/hatchnoise_p2_filter.json
"""
from __future__ import annotations

import collections
import json
import re
import sys
import time
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F

OUT_JSON = C.ART / "hatchnoise_p2_filter.json"
HATCH_RE = re.compile(r"(PATT|HATCH|штрих|IZOLAT|ИЗОЛЯ|заливк)", re.IGNORECASE)
FURNITURE_RE = re.compile(r"(мебел|FURN|растен|озелен)", re.IGNORECASE)
UNDERLAY_RE = re.compile(r"(XREF|подоснов|underlay)", re.IGNORECASE)


def gt_class(layer: str) -> str:
    if not layer:
        return "unlabelled"
    if HATCH_RE.search(layer):
        return "hatch"
    if FURNITURE_RE.search(layer):
        return "furniture"
    if UNDERLAY_RE.search(layer):
        return "underlay"
    return "foreground"


def run(block: str) -> dict:
    spec = C.BLOCKS[block]
    pdf, page_index, bbox = spec["left"]
    t0 = time.time()
    payload = C.load_primitives(pdf, page_index, bbox)
    table = C.segment_table(payload)
    rows = table["rows"]
    flags, records, prim_flags = F.classify(rows)
    aspect = (bbox[3] - bbox[1]) * payload["page_size"][1] / ((bbox[2] - bbox[0]) * payload["page_size"][0])

    keep = [row for row, flag in zip(rows, flags) if not flag]
    drop = [row for row, flag in zip(rows, flags) if flag]

    out_dir = C.OUT / block
    C.render_crop(pdf, page_index, bbox, out_dir / "00_pdf_crop.png")
    C.render_segments(rows, out_dir / "01_all_segments.png", aspect=aspect,
                      title=f"{block}: ALL {len(rows)} segments")
    C.render_segments(keep, out_dir / "02_keep.png", aspect=aspect, color=(0, 0, 0),
                      title=f"{block}: KEPT {len(keep)} ({len(keep)/max(len(rows),1):.1%})")
    C.render_segments(drop, out_dir / "03_drop.png", aspect=aspect, color=(0.85, 0, 0),
                      title=f"{block}: DROPPED {len(drop)} ({len(drop)/max(len(rows),1):.1%})")
    per_rule = {}
    for rule in F.RULES:
        only = [row for row, flag in zip(rows, flags) if rule in flag]
        per_rule[rule] = len(only)
        if only:
            C.render_segments(only, out_dir / f"04_drop_{rule}.png", aspect=aspect, color=(0.85, 0, 0),
                              title=f"{block}: rule {rule} dropped {len(only)}")

    result = {
        "block": block,
        "discipline": spec["discipline"],
        "pdf": pdf,
        "page_index": page_index,
        "bbox_norm": bbox,
        "segments": len(rows),
        "primitives": table["n_primitives"],
        "texts": table["n_texts"],
        "dropped": len(drop),
        "dropped_frac": round(len(drop) / max(len(rows), 1), 4),
        "per_rule_dropped_segments": per_rule,
        "primitives_dropped": sum(1 for f in prim_flags if f),
        "primitives_dropped_frac": round(sum(1 for f in prim_flags if f) / max(len(prim_flags), 1), 4),
        "elapsed_s": round(time.time() - t0, 1),
        "renders": sorted(str(p.relative_to(C.ART.parent)) for p in out_dir.glob("*.png")),
    }

    # ground truth scoring where CAD layers exist
    classes = [gt_class(row["layer"]) for row in rows]
    counts = collections.Counter(classes)
    if counts.get("hatch", 0) + counts.get("furniture", 0) > 0 and counts.get("unlabelled", 0) < len(rows) * 0.5:
        positives = {"hatch", "furniture", "underlay"}
        tp = sum(1 for cls, flag in zip(classes, flags) if flag and cls in positives)
        fp = sum(1 for cls, flag in zip(classes, flags) if flag and cls not in positives)
        fn = sum(1 for cls, flag in zip(classes, flags) if not flag and cls in positives)
        tn = sum(1 for cls, flag in zip(classes, flags) if not flag and cls not in positives)
        result["ground_truth"] = {
            "class_counts": dict(counts),
            "true_positive": tp, "false_positive": fp, "false_negative": fn, "true_negative": tn,
            "precision": round(tp / max(tp + fp, 1), 4),
            "recall": round(tp / max(tp + fn, 1), 4),
            "foreground_eaten_frac": round(fp / max(fp + tn, 1), 4),
        }
        gt_pos = [row for row, cls in zip(rows, classes) if cls in positives]
        gt_neg = [row for row, cls in zip(rows, classes) if cls not in positives]
        C.render_segments(gt_pos, out_dir / "05_gt_background.png", aspect=aspect, color=(0.85, 0, 0),
                          title=f"{block}: CAD-layer background {len(gt_pos)}")
        C.render_segments(gt_neg, out_dir / "06_gt_foreground.png", aspect=aspect, color=(0, 0, 0),
                          title=f"{block}: CAD-layer foreground {len(gt_neg)}")
        eaten = [row for row, cls, flag in zip(rows, classes, flags) if flag and cls not in positives]
        if eaten:
            C.render_segments(eaten, out_dir / "07_false_positives.png", aspect=aspect, color=(0, 0.5, 0.9),
                              title=f"{block}: FOREGROUND eaten by filter {len(eaten)}")
    return result


def main() -> None:
    blocks = sys.argv[1:] or list(C.BLOCKS)
    results = []
    for block in blocks:
        print("...", block, flush=True)
        results.append(run(block))
        print(json.dumps(results[-1], ensure_ascii=False)[:400], flush=True)
    existing = {}
    if OUT_JSON.exists():
        existing = json.loads(OUT_JSON.read_text(encoding="utf-8")).get("blocks", {})
    existing.update({r["block"]: r for r in results})
    C.write_json(OUT_JSON, {
        "probe": "hatchnoise_p2_filter_render",
        "filter_defaults": F.DEFAULTS,
        "blocks": existing,
    })


if __name__ == "__main__":
    main()
