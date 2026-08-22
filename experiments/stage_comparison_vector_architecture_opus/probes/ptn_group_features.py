"""Per-motif-group features used to test hatch-vs-symbol discrimination.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_group_features <pair> <side> [SIG] [top_n]
Writes artifacts/ptn/features_<pair>_<side>_<SIG>.json
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes.ptn_run_signatures import lattice_score  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn"


def straightness(motif, segments) -> float:
    """Max distance of the motif's points from their best-fit line / motif diagonal."""
    pts = []
    for i in motif["seg_indexes"]:
        pts.append(segments[i]["p0"])
        pts.append(segments[i]["p1"])
    n = len(pts)
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    sxx = sum((p[0] - mx) ** 2 for p in pts)
    syy = sum((p[1] - my) ** 2 for p in pts)
    sxy = sum((p[0] - mx) * (p[1] - my) for p in pts)
    theta = 0.5 * math.atan2(2 * sxy, sxx - syy)
    nx, ny = -math.sin(theta), math.cos(theta)
    dev = max(abs((p[0] - mx) * nx + (p[1] - my) * ny) for p in pts)
    return round(dev / max(motif["diag"], 1e-9), 4)


def closure(motif) -> tuple[float, int]:
    degree = Counter()
    for a, b in motif["nodes"]:
        degree[a] += 1
        degree[b] += 1
    open_ends = sum(1 for v in degree.values() if v == 1)
    junctions = sum(1 for v in degree.values() if v >= 3)
    return (round(1.0 - open_ends / max(len(degree), 1), 3), junctions)


def text_inside(motif, texts) -> bool:
    x0, y0, x1, y1 = motif["bbox"]
    for t in texts:
        b = t["bbox"]
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        if x0 <= cx <= x1 and y0 <= cy <= y1:
            return True
    return False


def analyse(pair: str, side: str, signame: str = "S1", top_n: int = 40) -> dict:
    desc = M.load_description(TRACK_A / pair / side / "vector_block.json")
    bundle = M.build_motifs(desc, unit=os.environ.get("PTN_UNIT", "cc_split"))
    M.enrich_bundle(bundle, desc)
    texts = desc["texts"]
    motifs, segments = bundle["motifs"], bundle["segments"]
    sigs = [M.signatures_for(m, bundle, texts) for m in motifs]
    groups: dict[str, list[int]] = defaultdict(list)
    for i, s in enumerate(sigs):
        groups[s[signame]].append(i)
    ranked = sorted(groups.items(), key=lambda kv: -len(kv[1]))[:top_n]

    # how many distinct S1 signatures live inside each S5c class (rotation/mirror variants)
    s5c_to_s1: dict[str, set[str]] = defaultdict(set)
    for s in sigs:
        s5c_to_s1[s["S5c"]].add(s["S1"])

    rows = []
    for rank, (key, members) in enumerate(ranked):
        rep = motifs[members[0]]
        cl, junc = closure(rep)
        rows.append(
            {
                "rank": rank,
                "sig": key,
                "count": len(members),
                "nseg": rep["nseg"],
                "diag_pt": round(sum(motifs[i]["diag"] for i in members) / len(members), 2),
                "straightness": straightness(rep, segments),
                "closure": cl,
                "junctions": junc,
                "labeled_frac": round(sum(1 for i in members if M.text_context(motifs[i], texts)[0]) / len(members), 3),
                "text_inside_frac": round(sum(1 for i in members if text_inside(motifs[i], texts)) / len(members), 3),
                "ext_touch_mean": round(
                    sum(M.relation_context(motifs[i], bundle)[0] for i in members) / len(members), 2
                ),
                "lattice": lattice_score([motifs[i]["center"] for i in members]),
                "orient_variants_in_S5c": len(s5c_to_s1[sigs[members[0]]["S5c"]]),
            }
        )
    return {
        "pair": pair,
        "side": side,
        "signature": signame,
        "motifs_total": len(motifs),
        "groups_total": len(groups),
        "rows": rows,
    }


def main() -> None:
    pair, side = sys.argv[1], sys.argv[2]
    signame = sys.argv[3] if len(sys.argv) > 3 else "S1"
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 40
    res = analyse(pair, side, signame, top_n)
    path = OUT / f"features_{pair}_{side}_{signame}.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(res, handle, ensure_ascii=False, indent=1)
    hdr = ["rank", "count", "nseg", "diag_pt", "straightness", "closure", "junctions",
           "labeled_frac", "text_inside_frac", "ext_touch_mean", "lattice", "orient_variants_in_S5c"]
    print("\t".join(hdr))
    for row in res["rows"]:
        print("\t".join(str(row[h]) for h in hdr))
    print(path)


if __name__ == "__main__":
    main()
