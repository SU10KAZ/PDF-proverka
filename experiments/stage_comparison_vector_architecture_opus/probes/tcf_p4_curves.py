#!/usr/bin/env python3
"""TCF probe 4 — curve flattening, circle resampling and anisotropic normalization.

Measures
  A. the chord error of `extractor.CURVE_STEPS = 6` on the real cubic Bezier items
     of a few benchmark pages, in normalized block units, against the topology
     tolerance 0.0025;
  B. the census of circles/ellipses in the 20 stored descriptions and the radial
     inset of the fixed 24-gon resampling, r*(1-cos(pi/24)), against the tolerance
     (this is what decides whether a tangent line still connects to a circle);
  C. what the comparator's segment coverage reports when the same circle is
     flattened with two different phases / step counts (synthetic, clearly marked);
  D. the anisotropy of the block normalization: the topology tolerance 0.0025 is a
     circle in normalized space but an ellipse in page points.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p4_curves
"""
from __future__ import annotations

import json
import math
import pathlib
import statistics
import time

import fitz

from experiments.stage_comparison_vector_blocks import extractor as ex
from experiments.stage_comparison_vector_blocks.comparator import _segment_coverage_runs

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p4_curves.json")
CURVE_PAGES = ("ss_simple_node", "ss_scheme_text_changed", "eom_singleline_changed", "vk_plan")
TOL = 0.0025


def cubic_point(p, t):
    u = 1 - t
    return (
        u ** 3 * p[0][0] + 3 * u * u * t * p[1][0] + 3 * u * t * t * p[2][0] + t ** 3 * p[3][0],
        u ** 3 * p[0][1] + 3 * u * u * t * p[1][1] + 3 * u * t * t * p[2][1] + t ** 3 * p[3][1],
    )


def polyline_distance(point, polyline):
    return min(
        ex._point_segment_distance(point, [polyline[i], polyline[i + 1]])
        for i in range(len(polyline) - 1)
    )


def part_a() -> dict:
    rows = {}
    for pair in CURVE_PAGES:
        d = json.loads((ROOT / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
        bbox = d["bbox"]
        bw, bh = bbox[2] - bbox[0], bbox[3] - bbox[1]
        t0 = time.time()
        doc = fitz.open(d["source"]["pdf"])
        page = doc[d["page_index"]]
        drawings = page.get_drawings()
        errors_pt, errors_norm = [], []
        cubics = 0
        for drawing in drawings:
            rect = drawing.get("rect")
            if rect is None or not ex._rect_intersects([rect.x0, rect.y0, rect.x1, rect.y1], bbox):
                continue
            for item in drawing.get("items") or []:
                if item[0] != "c":
                    continue
                cubics += 1
                control = [ex._point(item[i]) for i in range(1, 5)]
                flat = ex._sample_cubic(item, ex.CURVE_STEPS)
                worst_pt = 0.0
                worst_norm = 0.0
                for k in range(1, 200):
                    p = cubic_point(control, k / 200)
                    worst_pt = max(worst_pt, polyline_distance(p, flat))
                    pn = ((p[0] - bbox[0]) / bw, (p[1] - bbox[1]) / bh)
                    fn = [((q[0] - bbox[0]) / bw, (q[1] - bbox[1]) / bh) for q in flat]
                    worst_norm = max(worst_norm, polyline_distance(pn, fn))
                errors_pt.append(worst_pt)
                errors_norm.append(worst_norm)
                if cubics >= 4000:
                    break
            if cubics >= 4000:
                break
        doc.close()
        rows[pair] = {
            "cubic_items_measured": cubics,
            "seconds": round(time.time() - t0, 1),
            "max_error_pt": round(max(errors_pt), 5) if errors_pt else None,
            "median_error_pt": round(statistics.median(errors_pt), 5) if errors_pt else None,
            "max_error_norm": round(max(errors_norm), 6) if errors_norm else None,
            "median_error_norm": round(statistics.median(errors_norm), 6) if errors_norm else None,
            "share_error_above_tolerance": round(
                sum(1 for e in errors_norm if e > TOL) / len(errors_norm), 4
            ) if errors_norm else None,
        }
        print("A", pair, rows[pair], flush=True)
    return rows


def part_b() -> dict:
    inset_factor = 1 - math.cos(math.pi / 24)
    rows = {}
    for pair_dir in sorted(ROOT.iterdir()):
        d = json.loads((pair_dir / "left" / "vector_block.json").read_text(encoding="utf-8"))
        radii = []
        for p in d["geometry"]["primitives"]:
            if p["type"] in {"circle", "ellipse"}:
                b = p["normalized"]["bbox"]
                radii.append(max(b[2] - b[0], b[3] - b[1]) / 2)
        rows[pair_dir.name] = {
            "circles": len(radii),
            "median_radius_norm": round(statistics.median(radii), 5) if radii else None,
            "max_radius_norm": round(max(radii), 5) if radii else None,
            "circles_whose_24gon_inset_exceeds_tolerance": sum(
                1 for r in radii if r * inset_factor > TOL
            ),
            "radius_needed_to_exceed_tolerance": round(TOL / inset_factor, 4),
        }
    rows["_inset_factor"] = round(inset_factor, 6)
    return rows


def _circle_primitive(cx, cy, r, n, phase, pid):
    points = [
        (cx + r * math.cos(2 * math.pi * i / n + phase), cy + r * math.sin(2 * math.pi * i / n + phase))
        for i in range(n)
    ]
    segs = [[list(points[i]), list(points[(i + 1) % n])] for i in range(n)]
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return {
        "id": pid,
        "type": "circle",
        "normalized": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "raw": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "length_norm": round(sum(ex._distance(*s) for s in segs), 5),
        "angle_degrees": 0.0,
        "segment_count": n,
        "closed": True,
        "style": {},
    }


def part_c() -> dict:
    out = {}
    for radius in (0.01, 0.03, 0.08):
        base = [_circle_primitive(0.5, 0.5, radius, 24, 0.0, "primitive-1")]
        cases = {
            "same_encoding": [_circle_primitive(0.5, 0.5, radius, 24, 0.0, "primitive-1")],
            "phase_shift_half_step": [
                _circle_primitive(0.5, 0.5, radius, 24, math.pi / 24, "primitive-1")
            ],
            "steps_16_vs_24": [_circle_primitive(0.5, 0.5, radius, 16, 0.0, "primitive-1")],
            "steps_32_vs_24": [_circle_primitive(0.5, 0.5, radius, 32, 0.0, "primitive-1")],
        }
        out[f"radius_{radius}"] = {
            name: _segment_coverage_runs(base, other, (0.001, 0.0025, 0.005, 0.01))
            for name, other in cases.items()
        }
    compact = {}
    for radius, cases in out.items():
        compact[radius] = {
            name: {str(run["tolerance"]): run["similarity"] for run in runs}
            for name, runs in cases.items()
        }
    return compact


def part_d() -> dict:
    rows = {}
    for pair_dir in sorted(ROOT.iterdir()):
        for side in ("left", "right"):
            d = json.loads((pair_dir / side / "vector_block.json").read_text(encoding="utf-8"))
            b = d["bbox"]
            w, h = b[2] - b[0], b[3] - b[1]
            rows[f"{pair_dir.name}/{side}"] = {
                "block_width_pt": round(w, 1),
                "block_height_pt": round(h, 1),
                "aspect": round(w / h, 3),
                "tolerance_pt_x": round(TOL * w, 3),
                "tolerance_pt_y": round(TOL * h, 3),
            }
    return rows


def main() -> None:
    payload = {
        "probe": "tcf_p4_curves",
        "curve_steps": ex.CURVE_STEPS,
        "tolerance": TOL,
        "A_cubic_flattening": part_a(),
        "B_circle_census": part_b(),
        "C_synthetic_reflattening_coverage": part_c(),
        "D_normalization_anisotropy": part_d(),
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print("\nB circle census:")
    for k, v in payload["B_circle_census"].items():
        print(" ", k, v)
    print("\nC synthetic re-flattening (segment coverage similarity):")
    for radius, cases in payload["C_synthetic_reflattening_coverage"].items():
        print(" ", radius, json.dumps(cases, ensure_ascii=False))
    print("\nD anisotropy (worst 6):")
    worst = sorted(payload["D_normalization_anisotropy"].items(),
                   key=lambda kv: abs(math.log(kv[1]["aspect"])), reverse=True)[:6]
    for k, v in worst:
        print(" ", k, v)


if __name__ == "__main__":
    main()
