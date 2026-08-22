"""P1 — separating power of each candidate hatch/background feature, one at a time,
scored against CAD-layer ground truth on PDFs that expose layer names.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p1_features
"""
from __future__ import annotations

import collections
import json
import re
from pathlib import Path

import numpy as np

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C

OUT = C.ART / "hatchnoise_p1_features.json"

HATCH_RE = re.compile(r"(PATT|HATCH|штрих|IZOLAT|ИЗОЛЯ|заливк)", re.IGNORECASE)
FURNITURE_RE = re.compile(r"(мебел|FURN|растен|озелен)", re.IGNORECASE)
UNDERLAY_RE = re.compile(r"(XREF|подоснов|underlay)", re.IGNORECASE)

# Ground-truth blocks: identical list to hatchnoise_p4_transfer so both probes describe
# the same sheets (and share the extraction cache).
from experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p4_transfer import GT_BLOCKS

# feature name -> (extractor, direction) ; direction +1 means "large value => background"
FEATURES = {
    "seg_length_norm_inv": (lambda r: -r["len"], +1),
    "motif_repetition": (lambda r: r["motif_n"], +1),
    "cell_segment_density": (lambda r: r["cell_density"], +1),
    "cell_tiny_frac": (lambda r: r["cell_tiny_frac"], +1),
    "cell_dominant_angle_share": (lambda r: r["cell_dominant_share"], +1),
    "cell_orientation_entropy_inv": (lambda r: -r["cell_orientation_entropy"], +1),
    "stroke_width_inv": (lambda r: -r["width"], +1),
    "is_filled_path": (lambda r: 1.0 if r["filled"] else 0.0, +1),
    "is_colored": (lambda r: 1.0 if r["colored"] else 0.0, +1),
    "stroke_luminance": (lambda r: r["stroke_lum"], +1),
    "enclosed_in_closed_contour": (lambda r: 1.0 if r["enclosed"] else 0.0, +1),
    "cell_text_spans_inv": (lambda r: -float(r["cell_text_spans"]), +1),
    "parent_path_segment_count_inv": (lambda r: -float(r["prim_segs"]), +1),
    "parent_path_length_inv": (lambda r: -r["prim_len"], +1),
    "angle_is_45deg": (lambda r: 1.0 if 40.0 <= r["ang"] <= 50.0 or 130.0 <= r["ang"] <= 140.0 else 0.0, +1),
}


def auc(score: np.ndarray, label: np.ndarray) -> float:
    """Mann-Whitney AUC: P(score of positive > score of negative), ties = 0.5."""
    order = np.argsort(score, kind="mergesort")
    ranks = np.empty(len(score), dtype=float)
    sorted_scores = score[order]
    i = 0
    rank = 1.0
    while i < len(sorted_scores):
        j = i
        while j + 1 < len(sorted_scores) and sorted_scores[j + 1] == sorted_scores[i]:
            j += 1
        average = (rank + rank + (j - i)) / 2.0
        ranks[order[i : j + 1]] = average
        rank += j - i + 1
        i = j + 1
    positives = label.sum()
    negatives = len(label) - positives
    if positives == 0 or negatives == 0:
        return float("nan")
    return float((ranks[label == 1].sum() - positives * (positives + 1) / 2.0) / (positives * negatives))


def drop_at_precision(score: np.ndarray, label: np.ndarray, keep_fg: float = 0.99) -> dict[str, float]:
    """Highest fraction of background droppable while retaining `keep_fg` of foreground."""
    fg = score[label == 0]
    if len(fg) == 0:
        return {"threshold": float("nan"), "bg_dropped": float("nan")}
    threshold = float(np.quantile(fg, keep_fg))
    dropped = float((score[label == 1] > threshold).mean())
    kept_fg = float((score[label == 0] <= threshold).mean())
    return {"threshold": round(threshold, 5), "bg_dropped_frac": round(dropped, 4), "fg_kept_frac": round(kept_fg, 4)}


def label_map(layers: collections.Counter) -> dict[str, str]:
    mapping = {}
    for name in layers:
        if not name:
            mapping[name] = "unlabelled"
        elif HATCH_RE.search(name):
            mapping[name] = "hatch"
        elif FURNITURE_RE.search(name):
            mapping[name] = "furniture"
        elif UNDERLAY_RE.search(name):
            mapping[name] = "underlay"
        else:
            mapping[name] = "foreground"
    return mapping


def run_block(name: str, spec) -> dict:
    pdf, page_index, bbox = spec
    if page_index is None:
        import fitz

        document = fitz.open(C.ROOT / pdf)
        best = max(range(len(document)), key=lambda i: len(document[i].get_drawings()))
        page_index = best
        document.close()
    payload = C.load_primitives(pdf, page_index, bbox)
    table = C.segment_table(payload)
    rows = table["rows"]
    layers = collections.Counter(r["layer"] for r in rows)
    mapping = label_map(layers)
    classes = np.array([mapping[r["layer"]] for r in rows])
    class_counts = dict(collections.Counter(classes.tolist()))

    result = {
        "block": name,
        "pdf": pdf,
        "page_index": page_index,
        "bbox_norm": bbox,
        "segments": len(rows),
        "primitives": table["n_primitives"],
        "texts": table["n_texts"],
        "class_counts": class_counts,
        "layer_to_class": {k: v for k, v in sorted(mapping.items()) if k},
        "targets": {},
    }
    for target_name, positive in (("hatch_only", {"hatch"}), ("hatch_furniture_underlay", {"hatch", "furniture", "underlay"})):
        label = np.array([1 if c in positive else 0 for c in classes], dtype=int)
        if label.sum() == 0 or label.sum() == len(label):
            result["targets"][target_name] = {"skipped": "degenerate label"}
            continue
        per_feature = {}
        for feature_name, (getter, _) in FEATURES.items():
            score = np.array([float(getter(r)) for r in rows])
            per_feature[feature_name] = {
                "auc": round(auc(score, label), 4),
                **drop_at_precision(score, label, 0.99),
                "at_fg_keep_0.95": drop_at_precision(score, label, 0.95),
            }
        result["targets"][target_name] = {
            "positive_segments": int(label.sum()),
            "positive_frac": round(float(label.mean()), 4),
            "features": dict(sorted(per_feature.items(), key=lambda kv: -kv[1]["auc"])),
        }
    return result


def main() -> None:
    results = []
    for name, spec in GT_BLOCKS.items():
        print("...", name, flush=True)
        results.append(run_block(name, spec))
    summary = {}
    for feature in FEATURES:
        aucs, drops = [], []
        for block in results:
            data = block["targets"].get("hatch_only", {})
            stats = data.get("features", {}).get(feature)
            if stats:
                aucs.append(stats["auc"])
                drops.append(stats["bg_dropped_frac"])
        if not aucs:
            continue
        summary[feature] = {
            "blocks": len(aucs),
            "auc_min": min(aucs),
            "auc_max": max(aucs),
            "auc_mean": round(sum(aucs) / len(aucs), 4),
            "auc_values": aucs,
            "sign_flips": sum(1 for a in aucs if a < 0.5),
            "drop_at_fg99_values": drops,
            "drop_at_fg99_median": round(sorted(drops)[len(drops) // 2], 4),
        }
    payload = {
        "probe": "hatchnoise_p1_features",
        "cross_block_summary": dict(sorted(summary.items(), key=lambda kv: -kv[1]["auc_mean"])),
        "method": (
            "Ground truth = CAD layer name exposed by PyMuPDF page.get_drawings()['layer']. "
            "Layer names are mapped to classes by regex on the NAME only (auditable in layer_to_class). "
            "AUC is Mann-Whitney over per-segment feature scores; drop_at_precision reports how much "
            "background a single threshold removes while keeping 99% (and 95%) of foreground segments."
        ),
        "features_tested": list(FEATURES),
        "blocks": results,
    }
    C.write_json(OUT, payload)
    for block in results:
        print("\n==", block["block"], block["segments"], "segments", block["class_counts"])
        for target, data in block["targets"].items():
            if "features" not in data:
                continue
            print(f"  target={target} positives={data['positive_segments']} ({data['positive_frac']:.2%})")
            for feature, stats in list(data["features"].items())[:8]:
                print(f"    {feature:34s} AUC={stats['auc']:.3f}  drop@fg99={stats['bg_dropped_frac']:.3f}  drop@fg95={stats['at_fg_keep_0.95']['bg_dropped_frac']:.3f}")


if __name__ == "__main__":
    main()
