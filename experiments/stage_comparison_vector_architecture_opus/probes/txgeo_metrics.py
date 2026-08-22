#!/usr/bin/env python3
"""Aggregate hit rate / uniqueness / stability / dimension-scale-consistency.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_metrics
"""
from __future__ import annotations

import collections
import json
import math
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REL = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_relations"
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

TYPES = [
    "dimension_interval",
    "dimension_line_only",
    "leader",
    "symbol_cluster",
    "enclosure_tight",
    "contour_caption",
    "repeated_label",
    "grid_cell",
    "between_extension_lines",
    "band_association",
    "along_line",
    "enclosure_loose",
    "text_alignment",
    "nearest_geometry",
]

_NUM = re.compile(r"^[^\d]*?(\d{2,5})(?:[.,]\d+)?[^\d]*$")


def load(mode: str):
    out = {}
    for path in sorted((REL / mode).glob("*/*.json")):
        out[(path.parent.name, path.stem)] = json.loads(path.read_text(encoding="utf-8"))
    return out


def per_block_table(data):
    rows = []
    for (pair, side), res in sorted(data.items()):
        units = res["units"]
        n = len(units)
        row = {"pair": pair, "side": side, "units": n, "segments": res["counts"]["segments"]}
        for t in TYPES:
            hits = [u for u in units if t in u["relations"] and u["relations"][t].get("hit")]
            uniq = [u for u in hits if u["relations"][t].get("unique")]
            row[f"hit_{t}"] = len(hits)
            row[f"uniq_{t}"] = len(uniq)
        row["primary"] = dict(collections.Counter(u["primary"] for u in units))
        row["bound_any"] = sum(1 for u in units if u["primary"] != "unbound")
        rows.append(row)
    return rows


def dimension_scale_check(data, which="dimension_interval"):
    """A dimension relation can be self-verified: text value / measured segment length
    must land on ONE drawing scale for the whole block."""
    out = []
    for (pair, side), res in sorted(data.items()):
        ratios = []
        for u in res["units"]:
            rel = u["relations"].get(which)
            if not rel or not rel.get("hit"):
                continue
            m = _NUM.match(u["text"].strip())
            if not m:
                continue
            value = float(m.group(1))
            length = rel.get("measured_len_pt") or 0.0
            if length < 1e-6 or value <= 0:
                continue
            ratios.append(value / length)
        if len(ratios) < 3:
            out.append({"pair": pair, "side": side, "relation": which, "dimension_with_number": len(ratios), "verdict": "too few"})
            continue
        logs = [math.log10(r) for r in ratios]
        med = statistics.median(logs)
        consistent = sum(1 for l in logs if abs(l - med) < math.log10(1.05))
        out.append({
            "pair": pair, "side": side, "relation": which,
            "dimension_with_number": len(ratios),
            "median_mm_per_pt": round(10 ** med, 3),
            "within_5pct": consistent,
            "share_within_5pct": round(consistent / len(ratios), 3),
        })
    return out


def stability(data):
    """Same text on both sides of a pair — does it get the same primary relation?"""
    pairs = collections.defaultdict(dict)
    for (pair, side), res in data.items():
        pairs[pair][side] = res
    rows = []
    for pair, sides in sorted(pairs.items()):
        if set(sides) != {"left", "right"}:
            continue
        left, right = sides["left"], sides["right"]

        def keyed(res):
            d = collections.defaultdict(list)
            for u in res["units"]:
                d[u["text"].strip()].append(u)
            return d

        L, R = keyed(left), keyed(right)
        matched = same = 0
        v01_same = v01_matched = 0
        per_type = collections.Counter()
        per_type_same = collections.Counter()
        for text, lus in L.items():
            rus = R.get(text)
            if not rus or len(lus) != len(rus):
                continue
            lus = sorted(lus, key=lambda u: (u["bbox"][1], u["bbox"][0]))
            rus = sorted(rus, key=lambda u: (u["bbox"][1], u["bbox"][0]))
            for lu, ru in zip(lus, rus):
                matched += 1
                per_type[lu["primary"]] += 1
                if lu["primary"] == ru["primary"]:
                    same += 1
                    per_type_same[lu["primary"]] += 1
                lc = lu["relations"].get("nearest_geometry", {})
                rc = ru["relations"].get("nearest_geometry", {})
                if lc.get("hit") and rc.get("hit"):
                    v01_matched += 1
                    if lc.get("v01_confidence") == rc.get("v01_confidence"):
                        v01_same += 1
        rows.append({
            "pair": pair,
            "matched_units": matched,
            "same_primary": same,
            "stability": round(same / matched, 3) if matched else None,
            "per_type": {k: [per_type_same[k], v] for k, v in per_type.items()},
            "v01_confidence_stability": round(v01_same / v01_matched, 3) if v01_matched else None,
        })
    return rows


def main() -> None:
    report = {}
    for mode in ("span", "line"):
        data = load(mode)
        report[mode] = {
            "per_block": per_block_table(data),
            "dimension_scale_check": dimension_scale_check(data, "dimension_interval"),
            "dimension_scale_check_line_only": dimension_scale_check(data, "dimension_line_only"),
            "stability": stability(data),
        }
        totals = collections.Counter()
        uniq_totals = collections.Counter()
        units = 0
        prim = collections.Counter()
        for row in report[mode]["per_block"]:
            units += row["units"]
            for t in TYPES:
                totals[t] += row[f"hit_{t}"]
                uniq_totals[t] += row[f"uniq_{t}"]
            prim.update(row["primary"])
        report[mode]["totals"] = {
            "units": units,
            "hit_rate": {t: round(totals[t] / units, 4) for t in TYPES},
            "hits": dict(totals),
            "unique_hits": dict(uniq_totals),
            "uniqueness_given_hit": {t: (round(uniq_totals[t] / totals[t], 4) if totals[t] else None) for t in TYPES},
            "primary_distribution": dict(prim),
            "unbound_share": round(prim.get("unbound", 0) / units, 4),
        }
    (ART / "txgeo_metrics.json").write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")

    for mode in ("span", "line"):
        t = report[mode]["totals"]
        print(f"===== unit_mode={mode}  units={t['units']}")
        print(f"{'relation':26s} {'hits':>7s} {'hit_rate':>9s} {'unique':>7s} {'uniq|hit':>9s} {'primary':>8s}")
        for name in TYPES:
            print(f"{name:26s} {t['hits'][name]:7d} {t['hit_rate'][name]:9.3f} "
                  f"{t['unique_hits'][name]:7d} {str(t['uniqueness_given_hit'][name]):>9s} "
                  f"{t['primary_distribution'].get(name, 0):8d}")
        print(f"{'unbound':26s} {'':7s} {'':9s} {'':7s} {'':9s} {t['primary_distribution'].get('unbound', 0):8d}")
        print()


if __name__ == "__main__":
    main()
