#!/usr/bin/env python3
"""relgraph_crop -- Track-B probe 2: CROP INVARIANCE.

Two experiments per block:

(A) FRAME-ONLY jitter. Take the real extraction, keep every PDF coordinate
    byte-identical, and only re-normalize against a jittered block rect.
    Content is provably unchanged, so any similarity drop is pure
    normalization-frame sensitivity.

(B) REAL re-extraction with a jittered bbox_norm (content near edges may
    genuinely enter/leave).

Compared metrics: v0.1 directional segment coverage (comparator) vs relation
multiset jaccard at 3 granularities vs entity inventory vs text multiset.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_crop.py
"""
from __future__ import annotations

import collections
import copy
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from experiments.stage_comparison_vector_blocks import comparator as C  # noqa: E402
from experiments.stage_comparison_vector_blocks import extractor as E  # noqa: E402
from relgraph_granularity import project  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

BLOCKS = ["ss_scheme_text_changed", "eom_singleline_changed", "ss_plan_dense"]
SIGNS = (+1.0, -1.0, -1.0, +1.0)   # x0 right, y0 up, x1 left, y1 down: translate + scale
VARIANTS = [("jitter_0.5%", 0.005), ("jitter_2%", 0.02), ("jitter_5%", 0.05)]


def jitter_rect(rect, frac, signs=SIGNS):
    w, h = rect[2] - rect[0], rect[3] - rect[1]
    return [rect[0] + signs[0] * frac * w, rect[1] + signs[1] * frac * h,
            rect[2] + signs[2] * frac * w, rect[3] + signs[3] * frac * h]


def crop_edge_rect(rect, frac=0.10):
    w = rect[2] - rect[0]
    return [rect[0], rect[1], rect[2] - frac * w, rect[3]]


def renormalize(desc, new_rect):
    """Rebuild `normalized` layers of a description against a different rect.
    PDF-space coordinates are untouched -> content is provably identical."""
    d = copy.deepcopy(desc)
    x0, y0, x1, y1 = new_rect
    w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
    for prim in d["geometry"]["primitives"]:
        segs = []
        for s, e in prim["raw"]["segments"]:
            segs.append([[(s[0] - x0) / w, (s[1] - y0) / h],
                         [(e[0] - x0) / w, (e[1] - y0) / h]])
        prim["normalized"]["segments"] = segs
        xs = [p[0] for seg in segs for p in seg]
        ys = [p[1] for seg in segs for p in seg]
        prim["normalized"]["bbox"] = [min(xs), min(ys), max(xs), max(ys)]
    for t in d["texts"]:
        b = t["bbox"]
        t["bbox_norm"] = [(b[0] - x0) / w, (b[1] - y0) / h, (b[2] - x0) / w, (b[3] - y0) / h]
        t["x_norm"] = (t["bbox_norm"][0] + t["bbox_norm"][2]) / 2
        t["y_norm"] = (t["bbox_norm"][1] + t["bbox_norm"][3]) / 2
    d["bbox"] = list(new_rect)
    return d


def coverage(left_desc, right_desc):
    runs = C._segment_coverage_runs(left_desc["geometry"]["primitives"],
                                    right_desc["geometry"]["primitives"], C.TOLERANCES)
    return {f"tol_{r['tolerance']}": r["similarity"] for r in runs}


def text_multiset(desc):
    return collections.Counter(t["text"] for t in desc["texts"])


def measure(base_desc, var_desc):
    gb = R.build_relation_graph(base_desc)
    gv = R.build_relation_graph(var_desc)
    out = {
        "segment_coverage": coverage(base_desc, var_desc),
        "rel_jaccard_G3": round(R.weighted_jaccard(gb["relations"], gv["relations"]), 6),
        "rel_jaccard_G1": round(R.weighted_jaccard(project(gb["relations"], 1),
                                                   project(gv["relations"], 1)), 6),
        "rel_jaccard_G0": round(R.weighted_jaccard(project(gb["relations"], 0),
                                                   project(gv["relations"], 0)), 6),
        "entity_jaccard": round(R.weighted_jaccard(gb["entities"], gv["entities"]), 6),
        "text_jaccard": round(R.weighted_jaccard(text_multiset(base_desc),
                                                 text_multiset(var_desc)), 6),
        "n_segments_base": gb["stats"]["n_segments"],
        "n_segments_var": gv["stats"]["n_segments"],
        "n_clusters_base": gb["stats"]["n_clusters"],
        "n_clusters_var": gv["stats"]["n_clusters"],
    }
    return out


def main() -> None:
    pairs = {p["pair_id"]: p for p in json.loads((A / "block_pairs.json").read_text())["pairs"]}
    results = []
    for pid in BLOCKS:
        side = pairs[pid]["left"]
        base = json.loads((A / "descriptions" / pid / "left" / "vector_block.json").read_text())
        rect = base["bbox"]
        print(f"\n=== {pid}  rect={[round(v,2) for v in rect]}")

        # (A) frame-only: content byte-identical, normalization frame moved
        for name, frac in VARIANTS + [("crop_edge_10%", None)]:
            new_rect = crop_edge_rect(rect) if frac is None else jitter_rect(rect, frac)
            var = renormalize(base, new_rect)
            m = measure(base, var)
            m.update({"block": pid, "experiment": "A_frame_only", "variant": name,
                      "rect": [round(v, 3) for v in new_rect]})
            results.append(m)
            print(f"  A {name:14s} cov@0.005={m['segment_coverage']['tol_0.005']:.4f} "
                  f"cov@0.01={m['segment_coverage']['tol_0.01']:.4f} "
                  f"relG3={m['rel_jaccard_G3']:.4f} relG1={m['rel_jaccard_G1']:.4f} "
                  f"relG0={m['rel_jaccard_G0']:.4f} ent={m['entity_jaccard']:.4f} "
                  f"txt={m['text_jaccard']:.4f}")

        # (B) real re-extraction with jittered bbox_norm
        bn = side["bbox_norm"]
        for name, frac in VARIANTS + [("crop_edge_10%", None)]:
            if frac is None:
                nb = crop_edge_rect(bn)
            else:
                nb = jitter_rect(bn, frac)
            nb = [max(0.0, min(1.0, v)) for v in nb]
            t0 = time.time()
            var = E.extract_block(side["pdf"], page_index=side["page_index"], bbox_norm=nb,
                                  block_id=f"{pid}_{name}")
            m = measure(base, var)
            m.update({"block": pid, "experiment": "B_reextract", "variant": name,
                      "bbox_norm": [round(v, 5) for v in nb],
                      "extract_seconds": round(time.time() - t0, 1)})
            results.append(m)
            print(f"  B {name:14s} cov@0.005={m['segment_coverage']['tol_0.005']:.4f} "
                  f"cov@0.01={m['segment_coverage']['tol_0.01']:.4f} "
                  f"relG3={m['rel_jaccard_G3']:.4f} relG1={m['rel_jaccard_G1']:.4f} "
                  f"relG0={m['rel_jaccard_G0']:.4f} ent={m['entity_jaccard']:.4f} "
                  f"txt={m['text_jaccard']:.4f} segs {m['n_segments_base']}->{m['n_segments_var']}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_crop_invariance.json").write_text(
        json.dumps({"research_only": True, "signs": SIGNS, "results": results},
                   ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_crop_invariance.json")


if __name__ == "__main__":
    main()
