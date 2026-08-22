#!/usr/bin/env python3
"""relgraph_eom -- Track-B probe 4: EXPRESSIVENESS.

What does the relation-level diff of the eom_singleline pair actually say, and
is «Добавлены два ответвления» derivable from it?

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_eom.py
"""
from __future__ import annotations

import collections
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from relgraph_granularity import project, coarse  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
PID = "eom_singleline_changed"


def horizontal_feeders(g, min_w=0.25, max_h=0.02):
    out = []
    for c in g["clusters"]:
        b = c["bbox"]
        if (b[2] - b[0]) >= min_w and (b[3] - b[1]) <= max_h:
            out.append({"id": c["id"], "w": round(b[2] - b[0], 3),
                        "y": round((b[1] + b[3]) / 2, 4), "n_seg": c["n_segments"],
                        "cls": c["cls"]})
    out.sort(key=lambda d: d["y"])
    return out


def label_rows(g):
    """Text spans grouped into horizontal bands (rough 'row' structure)."""
    rows = collections.defaultdict(list)
    for t in g["texts"]:
        rows[round(t["center"][1] / 0.02)].append(t["text"])
    return {k: v for k, v in sorted(rows.items())}


def main() -> None:
    graphs = {}
    for side in ("left", "right"):
        desc = json.loads((A / "descriptions" / PID / side / "vector_block.json").read_text())
        graphs[side] = R.build_relation_graph(desc)
    gl, gr = graphs["left"], graphs["right"]

    report = {"research_only": True, "pair": PID,
              "stats": {"left": gl["stats"], "right": gr["stats"]}}

    # 1. repeated-symbol groups (the object-count channel)
    def gtable(g):
        return [{"cls": x["cls"], "count": x["count"], "rows": x["rows"], "cols": x["cols"],
                 "fp": x["fp"]} for x in g["groups"] if x["count"] >= 2]
    report["groups_left"] = gtable(gl)
    report["groups_right"] = gtable(gr)

    print("=== repeated-shape groups (count >= 2)")
    print(f"{'LEFT':>44s}   |   RIGHT")
    for i in range(max(len(report["groups_left"]), len(report["groups_right"]))[:] if False else
                   max(len(report["groups_left"]), len(report["groups_right"]))):
        l = report["groups_left"][i] if i < len(report["groups_left"]) else None
        r = report["groups_right"][i] if i < len(report["groups_right"]) else None
        ls = f"{l['cls']:>28s} x{l['count']:<3d}" if l else " " * 33
        rs = f"{r['cls']:>28s} x{r['count']:<3d}" if r else ""
        print(f"  {ls}   |   {rs}")

    # 2. entity inventory delta (coarse class)
    el, er = collections.Counter(), collections.Counter()
    for k, v in gl["entities"].items():
        el[coarse(k, 1)] += v
    for k, v in gr["entities"].items():
        er[coarse(k, 1)] += v
    report["entity_delta_G1"] = {k: [el.get(k, 0), er.get(k, 0)]
                                 for k in sorted(set(el) | set(er))}
    print("\n=== entity inventory (coarse class) left -> right")
    for k in sorted(set(el) | set(er), key=lambda k: -(er.get(k, 0) + el.get(k, 0))):
        print(f"  {k:22s} {el.get(k,0):5d} -> {er.get(k,0):5d}")

    # 3. relation token delta at G1
    pl, pr = project(gl["relations"], 1), project(gr["relations"], 1)
    delta = []
    for k in set(pl) | set(pr):
        a, b = pl.get(k, 0), pr.get(k, 0)
        if a != b:
            delta.append((b - a, k, a, b))
    delta.sort(key=lambda x: -abs(x[0]))
    report["relation_delta_G1"] = [{"rel": list(k), "left": a, "right": b, "delta": d}
                                   for d, k, a, b in delta[:40]]
    print("\n=== top relation-token changes (G1)")
    for d, k, a, b in delta[:20]:
        print(f"  {d:+5d}  {str(k):55s} {a} -> {b}")

    # 4. horizontal feeders (candidate 'ответвления')
    fl, fr = horizontal_feeders(gl), horizontal_feeders(gr)
    report["horizontal_feeders_left"] = fl
    report["horizontal_feeders_right"] = fr
    print(f"\n=== horizontal long clusters (w>=0.25, h<=0.02): left={len(fl)} right={len(fr)}")
    for f in fl:
        print(f"  L y={f['y']:.3f} w={f['w']:.3f} nseg={f['n_seg']}")
    for f in fr:
        print(f"  R y={f['y']:.3f} w={f['w']:.3f} nseg={f['n_seg']}")

    # 5. text evidence (for contrast: what plain text already gives)
    pat = re.compile(r"^(QD|QF|Wh|ЩМкв)\s?\d*$", re.I)
    tl = [t["text"] for t in gl["texts"] if pat.match(t["text"].strip())]
    tr = [t["text"] for t in gr["texts"] if pat.match(t["text"].strip())]
    report["device_like_texts"] = {"left": sorted(tl), "right": sorted(tr)}
    print(f"\n=== device-like text tokens  left={sorted(tl)}\n                             right={sorted(tr)}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_eom_expressiveness.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_eom_expressiveness.json")


if __name__ == "__main__":
    main()
