#!/usr/bin/env python3
"""relgraph_rotfix -- Track-B probe: re-extract the rotated-page blocks in the
CORRECT coordinate space and see whether the benchmark verdicts survive.

extract_block multiplies bbox_norm by page.rect (rotated) but PyMuPDF returns
both drawings and text in the unrotated cropbox space. This probe keeps
extractor.py untouched and instead
  1. maps the named page-space rect into content space via derotation_matrix,
  2. feeds extract_block a bbox_norm that reproduces that content rect,
  3. re-orients the resulting description back into page space so it is
     directly comparable with an unrotated block.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotfix.py
"""
from __future__ import annotations

import collections
import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fitz  # noqa: E402
import relgraph_core as R  # noqa: E402
from experiments.stage_comparison_vector_blocks import comparator as C  # noqa: E402
from experiments.stage_comparison_vector_blocks import extractor as E  # noqa: E402
from relgraph_crop import coverage, text_multiset  # noqa: E402
from relgraph_granularity import project  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def extract_rotation_correct(pdf, page_index, bbox_norm, block_id):
    doc = fitz.open(ROOT / pdf)
    page = doc[page_index]
    pr = page.rect
    named = fitz.Rect(bbox_norm[0] * pr.width, bbox_norm[1] * pr.height,
                      bbox_norm[2] * pr.width, bbox_norm[3] * pr.height)
    content = fitz.Rect(named) * page.derotation_matrix
    content.normalize()
    rot = page.rotation
    rm = page.rotation_matrix
    doc.close()
    bn = [content.x0 / pr.width, content.y0 / pr.height,
          content.x1 / pr.width, content.y1 / pr.height]
    desc = E.extract_block(ROOT / pdf, page_index=page_index, bbox_norm=bn, block_id=block_id)
    if rot == 0:
        return desc, named, content
    # re-orient into page space and renormalize against the named rect
    d = copy.deepcopy(desc)
    nx0, ny0 = named.x0, named.y0
    nw, nh = max(named.width, 1e-9), max(named.height, 1e-9)

    def to_norm(x, y):
        p = fitz.Point(x, y) * rm
        return [(p.x - nx0) / nw, (p.y - ny0) / nh]

    for prim in d["geometry"]["primitives"]:
        segs = [[to_norm(s[0], s[1]), to_norm(e[0], e[1])]
                for s, e in prim["raw"]["segments"]]
        prim["normalized"]["segments"] = segs
        xs = [p[0] for sg in segs for p in sg]
        ys = [p[1] for sg in segs for p in sg]
        prim["normalized"]["bbox"] = [min(xs), min(ys), max(xs), max(ys)]
    for t in d["texts"]:
        b = t["bbox"]
        a1, a2 = to_norm(b[0], b[1]), to_norm(b[2], b[3])
        t["bbox_norm"] = [min(a1[0], a2[0]), min(a1[1], a2[1]),
                          max(a1[0], a2[0]), max(a1[1], a2[1])]
        t["x_norm"] = (t["bbox_norm"][0] + t["bbox_norm"][2]) / 2
        t["y_norm"] = (t["bbox_norm"][1] + t["bbox_norm"][3]) / 2
    d["bbox"] = [named.x0, named.y0, named.x1, named.y1]
    return d, named, content


def score(l, r):
    gl, gr = R.build_relation_graph(l), R.build_relation_graph(r)
    return {
        "segment_coverage": coverage(l, r),
        "rel_G3": round(R.weighted_jaccard(gl["relations"], gr["relations"]), 6),
        "rel_G1": round(R.weighted_jaccard(project(gl["relations"], 1),
                                           project(gr["relations"], 1)), 6),
        "rel_G0": round(R.weighted_jaccard(project(gl["relations"], 0),
                                           project(gr["relations"], 0)), 6),
        "entity": round(R.weighted_jaccard(gl["entities"], gr["entities"]), 6),
        "text": round(R.weighted_jaccard(text_multiset(l), text_multiset(r)), 6),
        "n_texts": [len(l["texts"]), len(r["texts"])],
        "n_segments": [gl["stats"]["n_segments"], gr["stats"]["n_segments"]],
        "n_clusters": [gl["stats"]["n_clusters"], gr["stats"]["n_clusters"]],
        "_graphs": (gl, gr),
    }


def main() -> None:
    pairs = {p["pair_id"]: p for p in json.loads((A / "block_pairs.json").read_text())["pairs"]}
    report = {"research_only": True, "pairs": []}
    for pid in ("eom_singleline_changed", "vk_nodes", "vk_plan", "vk_node_plan"):
        p = pairs[pid]
        sides = {}
        for sn in ("left", "right"):
            s = p[sn]
            d, named, content = extract_rotation_correct(
                s["pdf"], int(s["page_index"]), s["bbox_norm"], f"{pid}_{sn}_rotfix")
            sides[sn] = d
            print(f"{pid:24s} {sn:5s} named={[round(v,1) for v in named]} "
                  f"content={[round(v,1) for v in (content.x0,content.y0,content.x1,content.y1)]} "
                  f"texts={len(d['texts'])} prims={len(d['geometry']['primitives'])}")
        old_l = json.loads((A / "descriptions" / pid / "left" / "vector_block.json").read_text())
        old_r = json.loads((A / "descriptions" / pid / "right" / "vector_block.json").read_text())
        before = score(old_l, old_r)
        after = score(sides["left"], sides["right"])
        entry = {"pair_id": pid, "human_expected": p["human_expected"],
                 "before_rotation_fix": {k: v for k, v in before.items() if k != "_graphs"},
                 "after_rotation_fix": {k: v for k, v in after.items() if k != "_graphs"}}
        report["pairs"].append(entry)
        print(f"  before: cov@0.01={before['segment_coverage']['tol_0.01']:.4f} "
              f"relG1={before['rel_G1']:.4f} text={before['text']:.4f} texts={before['n_texts']}")
        print(f"  after : cov@0.01={after['segment_coverage']['tol_0.01']:.4f} "
              f"relG1={after['rel_G1']:.4f} text={after['text']:.4f} texts={after['n_texts']}")
        if pid == "eom_singleline_changed":
            gl, gr = after["_graphs"]
            tl = collections.Counter(t["text"].strip() for t in sides["left"]["texts"])
            tr = collections.Counter(t["text"].strip() for t in sides["right"]["texts"])
            import re
            pat = re.compile(r"^(QD|QF|Wh|ЩМкв)\s?\d*$", re.I)
            entry["device_texts_after_fix"] = {
                "left": sorted(t for t in tl if pat.match(t)),
                "right": sorted(t for t in tr if pat.match(t))}
            entry["groups_left_after_fix"] = [{"cls": g["cls"], "count": g["count"]}
                                              for g in gl["groups"] if g["count"] >= 2][:20]
            entry["groups_right_after_fix"] = [{"cls": g["cls"], "count": g["count"]}
                                               for g in gr["groups"] if g["count"] >= 2][:20]
            print("  device-like texts LEFT :", entry["device_texts_after_fix"]["left"])
            print("  device-like texts RIGHT:", entry["device_texts_after_fix"]["right"])
            print("  repeated groups LEFT :", entry["groups_left_after_fix"][:8])
            print("  repeated groups RIGHT:", entry["groups_right_after_fix"][:8])

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_rotfix.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_rotfix.json")


if __name__ == "__main__":
    main()
