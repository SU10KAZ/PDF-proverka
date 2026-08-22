#!/usr/bin/env python3
"""TCF probe 6 — is the degree histogram a usable signature?

Uses the histograms recomputed by probe 1 (all 20 blocks x 5 tolerances) and asks:
  * do any two different blocks collide exactly?
  * L1 distance between L1-normalized histograms: is a block closer to its own pair
    partner than to every other block (nearest-neighbour retrieval)?
  * is a block closer to its own pair partner than to ITSELF recomputed at a
    neighbouring tolerance (identity vs perturbation)?
  * does the histogram add anything over the single number `node_count`?

Run from repo root (probe 1 must have run first):
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p6_degree
"""
from __future__ import annotations

import itertools
import json
import pathlib

P1 = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p1_tolerance.json")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p6_degree.json")
MAX_DEGREE = 10


def vector(histogram: dict[str, int]) -> list[float]:
    bins = [0.0] * (MAX_DEGREE + 1)
    for key, value in histogram.items():
        bins[min(int(key), MAX_DEGREE)] += value
    total = sum(bins) or 1.0
    return [v / total for v in bins]


def l1(a, b) -> float:
    return round(sum(abs(x - y) for x, y in zip(a, b)), 6)


def main() -> None:
    data = json.loads(P1.read_text(encoding="utf-8"))
    raw = data["raw"]
    base = {}
    for pair, sides in raw.items():
        for side, tolerances in sides.items():
            base[f"{pair}/{side}"] = vector(tolerances["0.0025"]["degree_histogram"])
    names = sorted(base)
    exact = {}
    for pair_names in itertools.combinations(names, 2):
        h1 = raw[pair_names[0].split("/")[0]][pair_names[0].split("/")[1]]["0.0025"]["degree_histogram"]
        h2 = raw[pair_names[1].split("/")[0]][pair_names[1].split("/")[1]]["0.0025"]["degree_histogram"]
        if h1 == h2:
            exact[" == ".join(pair_names)] = h1
    matrix = {a: {b: l1(base[a], base[b]) for b in names if b != a} for a in names}
    rows = []
    nn_correct = 0
    identity_beats_perturbation = 0
    for name in names:
        pair, side = name.split("/")
        partner = f"{pair}/{'right' if side == 'left' else 'left'}"
        nearest = min(matrix[name], key=matrix[name].get)
        d_partner = matrix[name].get(partner)
        d_other = min(v for k, v in matrix[name].items() if k != partner)
        self_005 = l1(base[name], vector(raw[pair][side]["0.005"]["degree_histogram"]))
        self_001 = l1(base[name], vector(raw[pair][side]["0.001"]["degree_histogram"]))
        ok = nearest == partner
        nn_correct += int(ok)
        beats = d_partner is not None and d_partner < min(self_005, self_001)
        identity_beats_perturbation += int(beats)
        rows.append(
            {
                "block": name,
                "nearest_neighbour": nearest,
                "nn_is_own_partner": ok,
                "d_to_partner": d_partner,
                "d_to_nearest_other_block": round(d_other, 6),
                "d_to_self_at_tolerance_0.005": self_005,
                "d_to_self_at_tolerance_0.001": self_001,
                "partner_closer_than_own_tolerance_jitter": beats,
                "node_count": raw[pair][side]["0.0025"]["node_count"],
            }
        )
    payload = {
        "probe": "tcf_p6_degree",
        "blocks": len(names),
        "exact_histogram_collisions": exact,
        "nearest_neighbour_is_pair_partner": f"{nn_correct}/{len(names)}",
        "partner_closer_than_tolerance_jitter": f"{identity_beats_perturbation}/{len(names)}",
        "rows": rows,
        "l1_matrix": matrix,
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print("exact collisions:", len(exact))
    print("NN == own pair partner:", payload["nearest_neighbour_is_pair_partner"])
    print("partner closer than own tolerance jitter:", payload["partner_closer_than_tolerance_jitter"])
    print("\nblock\tnn\tnn_ok\td_partner\td_other\td_self@0.005\td_self@0.001")
    for r in rows:
        print(f"{r['block']}\t{r['nearest_neighbour']}\t{r['nn_is_own_partner']}\t{r['d_to_partner']}\t"
              f"{r['d_to_nearest_other_block']}\t{r['d_to_self_at_tolerance_0.005']}\t{r['d_to_self_at_tolerance_0.001']}")


if __name__ == "__main__":
    main()
