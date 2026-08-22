#!/usr/bin/env python3
"""signoise probe 4 — can structural_signature drive candidate search?

Uses artifacts/signoise_03_block_features.json (written by probe 3).

A) exact hash behaviour of structural_signature.level_3_structural_topology over the 20 blocks:
   distinct values, recall on the 10 true П↔РД counterparts, cross-pair collisions.
B) a cheap tolerant coarse descriptor of my own design (angle histogram + 4x4 spatial occupancy +
   log counts + topology ratios + text-category shares), z-scored, nearest-neighbour retrieval:
   rank of the true counterpart for each of the 20 query blocks.
C) a quantised bucket-hash built from the same descriptor, evaluated as an exact candidate filter.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_04_signature
"""
from __future__ import annotations

import collections
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "artifacts"
FEATURES = OUT / "signoise_03_block_features.json"

ANGLE_BINS = ("h_0", "d_45", "v_90", "d_135", "other")


def vector(feature: dict[str, Any], groups: tuple[str, ...]) -> list[float]:
    coarse = feature["coarse"]
    out: list[float] = []
    if "angles" in groups:
        out += [coarse["angle_shares"][name] for name in ANGLE_BINS]
    if "grid" in groups:
        out += list(coarse["grid_shares"])
    if "counts" in groups:
        out += [coarse["log_segments"], coarse["log_components"], coarse["log_texts"]]
    if "topology" in groups:
        out += [coarse["endpoint_ratio"], coarse["branch_ratio"],
                coarse["t_junction_ratio"], coarse["closed_ratio"]]
    if "textcat" in groups:
        out += [coarse["category_shares"][name] for name in ("label", "numeric", "engineering_value")]
    if "shape" in groups:
        out += [coarse["length_mean_norm"], coarse["length_p90_norm"], coarse["aspect_ratio"]]
    return out


def zscore(rows: list[list[float]]) -> list[list[float]]:
    n, d = len(rows), len(rows[0])
    out = [[0.0] * d for _ in range(n)]
    for j in range(d):
        column = [rows[i][j] for i in range(n)]
        mean = sum(column) / n
        sd = math.sqrt(sum((v - mean) ** 2 for v in column) / n) or 1.0
        for i in range(n):
            out[i][j] = (column[i] - mean) / sd
    return out


def retrieval(names: list[str], rows: list[list[float]]) -> dict[str, Any]:
    z = zscore(rows)
    counterpart = {n: n.split("/")[0] + "/" + ("right" if n.endswith("left") else "left") for n in names}
    ranks, top1 = {}, 0
    for i, name in enumerate(names):
        distances = sorted(
            ((math.dist(z[i], z[j]), names[j]) for j in range(len(names)) if j != i),
            key=lambda row: row[0],
        )
        order = [row[1] for row in distances]
        rank = order.index(counterpart[name]) + 1
        ranks[name] = {
            "true_counterpart": counterpart[name],
            "rank": rank,
            "nearest": order[0],
            "nearest_distance": round(distances[0][0], 4),
            "counterpart_distance": round(distances[rank - 1][0], 4),
        }
        top1 += rank == 1
    return {
        "top1_accuracy": round(top1 / len(names), 4),
        "top1_hits": top1,
        "queries": len(names),
        "mean_rank": round(sum(r["rank"] for r in ranks.values()) / len(names), 3),
        "median_rank": sorted(r["rank"] for r in ranks.values())[len(names) // 2],
        "worst_rank": max(r["rank"] for r in ranks.values()),
        "per_query": ranks,
    }


def bucket_key(feature: dict[str, Any]) -> str:
    coarse = feature["coarse"]
    dominant = max(ANGLE_BINS, key=lambda name: coarse["angle_shares"][name])
    payload = [
        round(coarse["log_segments"] * 2) / 2,
        round(coarse["log_texts"] * 2) / 2,
        round(coarse["log_components"] * 2) / 2,
        dominant,
        round(coarse["category_shares"]["label"] * 4) / 4,
        round(min(coarse["aspect_ratio"], 8.0) * 2) / 2,
    ]
    return hashlib.sha256(json.dumps(payload).encode()).hexdigest()[:16]


def collision_report(names: list[str], keys: dict[str, str], label: str) -> dict[str, Any]:
    groups = collections.defaultdict(list)
    for name in names:
        groups[keys[name]].append(name)
    true_pair_hits, cross_pair_collisions = 0, []
    for a, b in itertools.combinations(names, 2):
        if keys[a] != keys[b]:
            continue
        if a.split("/")[0] == b.split("/")[0]:
            true_pair_hits += 1
        else:
            cross_pair_collisions.append([a, b])
    return {
        "key": label,
        "distinct_values": len(groups),
        "blocks": len(names),
        "true_counterpart_pairs_recovered": true_pair_hits,
        "true_counterpart_pairs_total": len(names) // 2,
        "recall": round(true_pair_hits / (len(names) // 2), 4),
        "cross_pair_collisions": len(cross_pair_collisions),
        "cross_pair_collision_examples": cross_pair_collisions[:20],
        "largest_bucket": max(len(v) for v in groups.values()),
    }


def main() -> None:
    features = json.loads(FEATURES.read_text(encoding="utf-8"))
    names = list(features)

    signature_reports = {
        level: collision_report(names, {n: features[n]["signatures"][level] for n in names},
                                f"structural_signature.{level}")
        for level in ("l1", "l2", "l3")
    }
    l3_payload_reports = collision_report(
        names,
        {n: hashlib.sha256(json.dumps(features[n]["l3_payload"].get("primitive_types", {}),
                                      sort_keys=True).encode()).hexdigest()[:16] for n in names},
        "level_3_payload.primitive_types only",
    )
    degree_only = collision_report(
        names,
        {n: hashlib.sha256(json.dumps(features[n]["l3_payload"].get("degree_histogram", {}),
                                      sort_keys=True).encode()).hexdigest()[:16] for n in names},
        "level_3_payload.degree_histogram only",
    )
    mine = collision_report(names, {n: bucket_key(features[n]) for n in names},
                            "signoise coarse bucket hash")

    groupsets = {
        "full": ("angles", "grid", "counts", "topology", "textcat", "shape"),
        "angles_only": ("angles",),
        "grid_only": ("grid",),
        "counts_only": ("counts",),
        "topology_only": ("topology",),
        "no_grid": ("angles", "counts", "topology", "textcat", "shape"),
        "no_counts": ("angles", "grid", "topology", "textcat", "shape"),
    }
    retrievals = {
        label: retrieval(names, [vector(features[n], groups) for n in names])
        for label, groups in groupsets.items()
    }

    payload = {
        "probe": "signoise_04_signature",
        "research_only": True,
        "blocks": names,
        "exact_signature_as_candidate_key": signature_reports,
        "partial_signature_keys": {"primitive_types_only": l3_payload_reports,
                                   "degree_histogram_only": degree_only},
        "signoise_coarse_bucket_hash": mine,
        "nearest_neighbour_retrieval": retrievals,
    }
    (OUT / "signoise_04_signature.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 4 — is structural_signature usable for candidate search?",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_04_signature`",
        "(needs `signoise_03_block_features.json` from probe 3)",
        "",
        "## A. Exact hash keys over the 20 blocks (10 true П↔РД counterpart pairs)",
        "",
        "| key | distinct values / 20 | true pairs recovered | cross-pair collisions | largest bucket |",
        "|---|---:|---:|---:|---:|",
    ]
    for report in list(signature_reports.values()) + [l3_payload_reports, degree_only, mine]:
        lines.append(
            f"| `{report['key']}` | {report['distinct_values']} | "
            f"{report['true_counterpart_pairs_recovered']}/{report['true_counterpart_pairs_total']} "
            f"(recall {report['recall']:.2f}) | {report['cross_pair_collisions']} | {report['largest_bucket']} |"
        )
    lines += [
        "",
        "## B. Tolerant nearest-neighbour retrieval with a coarse descriptor (signoise design)",
        "",
        "34 dims: 5-bin angle histogram + 4x4 spatial occupancy of segment midpoints + log(segments/"
        "components/texts) + endpoint/branch/T-junction/closed ratios + text-category shares + "
        "mean & p90 segment length + aspect ratio. Z-scored across the 20 blocks, Euclidean distance.",
        "",
        "| descriptor | top-1 accuracy | mean rank of true counterpart | median | worst |",
        "|---|---:|---:|---:|---:|",
    ]
    for label, report in retrievals.items():
        lines.append(
            f"| `{label}` | {report['top1_hits']}/{report['queries']} = {report['top1_accuracy']:.2f} | "
            f"{report['mean_rank']} | {report['median_rank']} | {report['worst_rank']} |"
        )
    lines += ["", "### full descriptor, per query", "",
              "| query block | true counterpart | rank | nearest neighbour found |",
              "|---|---|---:|---|"]
    for name, row in retrievals["full"]["per_query"].items():
        lines.append(f"| {name} | {row['true_counterpart']} | {row['rank']} | {row['nearest']} |")
    (OUT / "signoise_04_signature.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_04_signature.json")


if __name__ == "__main__":
    main()
