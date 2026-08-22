"""Run S0..S5c motif signatures over the 20 Track A block descriptions.

Repro:  python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_run_signatures
Writes: artifacts/ptn/signatures_<pair>_<side>.json  and  artifacts/ptn_signature_summary.json
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn"
OUT.mkdir(parents=True, exist_ok=True)

UNIT = os.environ.get("PTN_UNIT", "cc_split")
Q = float(os.environ.get("PTN_Q", "0.05"))


def lattice_score(centers: list[tuple[float, float]]) -> float:
    """Fraction of instances whose nearest-neighbour vector matches the median NN vector."""
    if len(centers) < 4:
        return 0.0
    vectors = []
    for i, c in enumerate(centers):
        best, bd = None, float("inf")
        for j, d in enumerate(centers):
            if i == j:
                continue
            dist = math.hypot(d[0] - c[0], d[1] - c[1])
            if dist < bd:
                bd, best = dist, (d[0] - c[0], d[1] - c[1])
        if best is not None:
            vectors.append((bd, math.degrees(math.atan2(best[1], best[0])) % 180))
    dists = sorted(v[0] for v in vectors)
    med_d = dists[len(dists) // 2]
    angs = sorted(v[1] for v in vectors)
    med_a = angs[len(angs) // 2]
    ok = sum(
        1
        for d, a in vectors
        if abs(d - med_d) <= 0.08 * max(med_d, 1e-9) and min(abs(a - med_a), 180 - abs(a - med_a)) <= 6
    )
    return round(ok / len(vectors), 3)


def analyse(desc_path: Path) -> dict:
    desc = M.load_description(desc_path)
    t0 = time.time()
    bundle = M.build_motifs(desc, unit=UNIT)
    M.enrich_bundle(bundle, desc)
    texts = desc["texts"]
    motifs = bundle["motifs"]
    sigs = [M.signatures_for(m, bundle, texts, Q) for m in motifs]
    elapsed = time.time() - t0

    per_sig = {}
    for name in M.SIGNATURES:
        groups: dict[str, list[int]] = defaultdict(list)
        for i, s in enumerate(sigs):
            groups[s[name]].append(i)
        rows = []
        for key, members in groups.items():
            centers = [motifs[i]["center"] for i in members]
            labeled = sum(1 for i in members if M.text_context(motifs[i], texts)[0])
            ext = [M.relation_context(motifs[i], bundle)[0] for i in members] if name == "S4" else None
            rows.append(
                {
                    "sig": key,
                    "count": len(members),
                    "nseg": motifs[members[0]]["nseg"],
                    "mean_diag_pt": round(sum(motifs[i]["diag"] for i in members) / len(members), 2),
                    "labeled_frac": round(labeled / len(members), 3),
                    "lattice": lattice_score(centers),
                    "members": members[:400],
                }
            )
        rows.sort(key=lambda r: -r["count"])
        per_sig[name] = {
            "groups_total": len(rows),
            "groups_repeated": sum(1 for r in rows if r["count"] >= 2),
            "instances_in_repeated": sum(r["count"] for r in rows if r["count"] >= 2),
            "top": rows[:25],
            "all_counts": {r["sig"]: r["count"] for r in rows},
        }

    # S0 baseline: current repeated_elements, verbatim
    s0 = desc["repeated_elements"]
    return {
        "unit": UNIT,
        "q": Q,
        "elapsed_s": round(elapsed, 2),
        "segments_total": len(bundle["segments"]),
        "network_segments": len(bundle["network"]),
        "oversized_components": bundle["oversized_components"],
        "motifs": len(motifs),
        "block_diag_pt": round(bundle["block_diag"], 2),
        "long_threshold_pt": round(bundle["long_threshold"], 2),
        "S0": {
            "groups_repeated": len(s0),
            "instances_in_repeated": sum(x["count"] for x in s0),
            "top": [
                {"sig": x["pattern_id"], "count": x["count"], "nseg": x["segment_count"], "ptype": x["primitive_type"]}
                for x in s0[:25]
            ],
            "all_counts": {x["pattern_id"]: x["count"] for x in s0},
        },
        "signatures": per_sig,
        "motif_geom": [
            {"i": i, "bbox": [round(v, 2) for v in m["bbox"]], "nseg": m["nseg"]} for i, m in enumerate(motifs)
        ],
    }


def main() -> None:
    only = sys.argv[1:] or None
    summary = {}
    for pair_dir in sorted(TRACK_A.iterdir()):
        if not pair_dir.is_dir():
            continue
        if only and pair_dir.name not in only:
            continue
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            t0 = time.time()
            res = analyse(path)
            key = f"{pair_dir.name}/{side}"
            with open(OUT / f"signatures_{pair_dir.name}_{side}.json", "w", encoding="utf-8") as handle:
                json.dump(res, handle, ensure_ascii=False)
            summary[key] = {
                k: res[k]
                for k in ("unit", "segments_total", "network_segments", "oversized_components", "motifs", "elapsed_s")
            }
            summary[key]["S0_groups"] = res["S0"]["groups_repeated"]
            summary[key]["S0_instances"] = res["S0"]["instances_in_repeated"]
            for name in M.SIGNATURES:
                summary[key][f"{name}_groups"] = res["signatures"][name]["groups_repeated"]
                summary[key][f"{name}_instances"] = res["signatures"][name]["instances_in_repeated"]
                summary[key][f"{name}_top"] = [r["count"] for r in res["signatures"][name]["top"][:5]]
            print(key, "motifs", res["motifs"], "S1g", summary[key]["S1_groups"], "S5g", summary[key]["S5_groups"],
                  f"{time.time() - t0:.1f}s", flush=True)
    with open(OUT.parent / "ptn_signature_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
