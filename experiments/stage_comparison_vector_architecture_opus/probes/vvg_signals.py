"""VVG — cheap deterministic gate signals for a VectorBlockDescription.

ARM 3, Track B (Opus). Research only.

Every signal here is computed from the description alone (plus, for the pair-level
signals, the paired description).  Zero model calls, no raster, no OCR.

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_signals \
        --track-a --out artifacts/vvg_signals_tracka.json
"""
from __future__ import annotations

import argparse
import gzip
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Sequence

import numpy as np

ROOT = Path("/home/coder/projects/PDF-proverka")
EXP = ROOT / "experiments" / "stage_comparison_vector_architecture_opus"
ART = EXP / "artifacts"
TRACK_A_DESC = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "descriptions"

CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
MICRO_LEN = 0.001          # O12 definition: shorter than 0.1 % of the block
UNBOUND_DIST = 0.012       # extractor._anchors "high" threshold
SEG_SAMPLE_CAP = 20000     # keep the cost bounded on dense blocks


# --------------------------------------------------------------------- helpers

def load_description(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if path.suffix == ".gz":
        return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
    return json.loads(path.read_text(encoding="utf-8"))


def _segments(description: dict[str, Any]) -> np.ndarray:
    """(N, 4) array of normalized segments [x0, y0, x1, y1]."""
    out: list[tuple[float, float, float, float]] = []
    for prim in description.get("geometry", {}).get("primitives", []):
        for seg in prim.get("normalized", {}).get("segments", []):
            out.append((seg[0][0], seg[0][1], seg[1][0], seg[1][1]))
    if not out:
        return np.zeros((0, 4), dtype=float)
    return np.asarray(out, dtype=float)


def _segments_with_owner(description: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    out: list[tuple[float, float, float, float]] = []
    owner: list[int] = []
    for idx, prim in enumerate(description.get("geometry", {}).get("primitives", [])):
        for seg in prim.get("normalized", {}).get("segments", []):
            out.append((seg[0][0], seg[0][1], seg[1][0], seg[1][1]))
            owner.append(idx)
    if not out:
        return np.zeros((0, 4), dtype=float), np.zeros((0,), dtype=int)
    return np.asarray(out, dtype=float), np.asarray(owner, dtype=int)


class _UF:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, a: int) -> int:
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _component_count(seg: np.ndarray, tol: float) -> int:
    """Connected components of the segment graph, endpoints snapped to a grid of size ``tol``.

    Cheap approximation of extractor._topology: two segments are joined when they
    have endpoints in the same or an adjacent grid cell.  What matters for the gate
    is not the absolute value but how the value moves when ``tol`` moves.
    """
    n = seg.shape[0]
    if n == 0:
        return 0
    tol = max(tol, 1e-6)
    pts = np.vstack([seg[:, 0:2], seg[:, 2:4]])
    owners = np.concatenate([np.arange(n), np.arange(n)])
    cells = np.floor(pts / tol).astype(np.int64)
    uf = _UF(n)
    # bucket by cell; join everything in a cell, then join to the 3x3 neighbourhood
    buckets: dict[tuple[int, int], int] = {}
    for i in range(cells.shape[0]):
        key = (int(cells[i, 0]), int(cells[i, 1]))
        first = buckets.get(key)
        if first is None:
            buckets[key] = int(owners[i])
        else:
            uf.union(first, int(owners[i]))
    for (cx, cy), rep in buckets.items():
        for dx in (0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy <= 0:
                    continue
                other = buckets.get((cx + dx, cy + dy))
                if other is not None:
                    uf.union(rep, other)
    return len({uf.find(i) for i in range(n)})


def _point_segment_distance(px: np.ndarray, py: np.ndarray, seg: np.ndarray) -> np.ndarray:
    """(P, S) distances from points to segments, all normalized coordinates."""
    x0, y0, x1, y1 = seg[:, 0], seg[:, 1], seg[:, 2], seg[:, 3]
    dx, dy = x1 - x0, y1 - y0
    denom = dx * dx + dy * dy
    denom = np.where(denom == 0, 1e-12, denom)
    t = ((px[:, None] - x0[None, :]) * dx[None, :] + (py[:, None] - y0[None, :]) * dy[None, :]) / denom[None, :]
    t = np.clip(t, 0.0, 1.0)
    cx = x0[None, :] + t * dx[None, :]
    cy = y0[None, :] + t * dy[None, :]
    return np.hypot(px[:, None] - cx, py[:, None] - cy)


# --------------------------------------------------------------------- signals

def compute_signals(description: dict[str, Any]) -> dict[str, Any]:
    geom = description.get("geometry", {})
    extraction = geom.get("extraction", {})
    summary = description.get("primitive_summary", {})
    topo = description.get("topology", {})
    texts = description.get("texts", [])

    seg, owner = _segments_with_owner(description)
    n_seg = int(seg.shape[0])

    sig: dict[str, Any] = {}

    # --- G1 cap signals (O11) ------------------------------------------------
    uncapped = int(extraction.get("primitives_uncapped") or 0)
    kept = int(summary.get("primitive_count") or len(geom.get("primitives", [])))
    sig["cap_storage"] = bool(extraction.get("storage_capped"))
    sig["retained_primitive_fraction"] = round(kept / uncapped, 6) if uncapped else 1.0
    sig["cap_topology"] = bool(topo.get("segments_capped"))
    st, su = int(topo.get("segments_total") or 0), int(topo.get("segments_used") or 0)
    sig["retained_topology_fraction"] = round(su / st, 6) if st else 1.0
    sig["components_truncated"] = bool(topo.get("components_truncated"))
    sig["any_cap"] = bool(sig["cap_storage"] or sig["cap_topology"] or sig["components_truncated"])
    # worst retained fraction across the two caps — the "how much geometry survived" number
    sig["retained_fraction_min"] = round(min(sig["retained_primitive_fraction"],
                                             sig["retained_topology_fraction"]), 6)

    # --- G2 per-span readable-text ratio (O8a) -------------------------------
    n_text = len(texts)
    garbled = sum(1 for t in texts if CONTROL_RE.search(t.get("text", "")))
    sig["text_items"] = n_text
    sig["readable_text_ratio"] = round(1.0 - garbled / n_text, 6) if n_text else 1.0
    sig["garbled_text_ratio"] = round(garbled / n_text, 6) if n_text else 0.0
    # the block-level gate Track A actually uses, for comparison
    control_chars = sum(len(CONTROL_RE.findall(t.get("text", ""))) for t in texts)
    sig["block_level_undecodable_flag"] = bool(control_chars >= 5)

    # --- G3 text vs geometry density ----------------------------------------
    sig["segments"] = n_seg
    sig["primitives"] = kept
    sig["text_per_segment"] = round(n_text / n_seg, 6) if n_seg else 0.0
    sig["segments_per_text"] = round(n_seg / n_text, 4) if n_text else float(n_seg)
    area = 0.0
    for t in texts:
        b = t.get("bbox_norm") or [0, 0, 0, 0]
        area += max(0.0, b[2] - b[0]) * max(0.0, b[3] - b[1])
    sig["text_area_share"] = round(min(area, 1.0), 6)
    sig["log10_segments"] = round(math.log10(n_seg), 4) if n_seg else 0.0
    sig["text_density"] = n_text  # block is the unit square, so count == per-unit-area

    # --- G4 micro-segment fraction (O12) -------------------------------------
    if n_seg:
        lens = np.hypot(seg[:, 2] - seg[:, 0], seg[:, 3] - seg[:, 1])
        sig["micro_segment_fraction"] = round(float((lens < MICRO_LEN).mean()), 6)
        sig["median_segment_length"] = round(float(np.median(lens)), 8)
    else:
        sig["micro_segment_fraction"] = 0.0
        sig["median_segment_length"] = 0.0

    # --- G5 object-grouping instability --------------------------------------
    tol = float(topo.get("tolerance_norm") or 0.0025)
    if n_seg:
        sample = seg if n_seg <= SEG_SAMPLE_CAP else seg[:SEG_SAMPLE_CAP]
        c_lo = _component_count(sample, tol * 0.8)
        c_mid = _component_count(sample, tol)
        c_hi = _component_count(sample, tol * 1.25)
        base = max(c_mid, 1)
        sig["group_count_at_tol"] = c_mid
        sig["group_instability"] = round(abs(c_hi - c_lo) / base, 6)
        sig["group_instability_up"] = round(abs(c_mid - c_hi) / base, 6)
        sig["group_instability_down"] = round(abs(c_lo - c_mid) / base, 6)
    else:
        sig["group_count_at_tol"] = 0
        sig["group_instability"] = 0.0
        sig["group_instability_up"] = 0.0
        sig["group_instability_down"] = 0.0

    # --- G6 unbound / ambiguous text ----------------------------------------
    if n_text and n_seg:
        px = np.asarray([t.get("x_norm", 0.0) for t in texts], dtype=float)
        py = np.asarray([t.get("y_norm", 0.0) for t in texts], dtype=float)
        sample = seg if n_seg <= SEG_SAMPLE_CAP else seg[:SEG_SAMPLE_CAP]
        sample_owner = owner if n_seg <= SEG_SAMPLE_CAP else owner[:SEG_SAMPLE_CAP]
        d1 = np.full(px.shape[0], np.inf)
        competitors = np.zeros(px.shape[0], dtype=int)
        step = max(1, int(4_000_000 // max(sample.shape[0], 1)))
        for start in range(0, px.shape[0], step):
            stop = min(start + step, px.shape[0])
            dist = _point_segment_distance(px[start:stop], py[start:stop], sample)
            near = dist.min(axis=1)
            d1[start:stop] = near
            # how many DISTINCT primitives lie within 1.5x the nearest distance
            thr = np.maximum(near * 1.5, 1e-9)
            for row in range(dist.shape[0]):
                mask = dist[row] <= thr[row]
                competitors[start + row] = len(np.unique(sample_owner[mask]))
        sig["unbound_text_ratio"] = round(float((d1 > UNBOUND_DIST).mean()), 6)
        sig["ambiguous_text_ratio"] = round(float((competitors >= 2).mean()), 6)
        sig["median_text_anchor_distance"] = round(float(np.median(d1)), 6)
    else:
        sig["unbound_text_ratio"] = 0.0 if not n_text else 1.0
        sig["ambiguous_text_ratio"] = 0.0
        sig["median_text_anchor_distance"] = 0.0
    anchors = description.get("anchors", [])
    if anchors:
        sig["anchor_conf_high_share"] = round(
            sum(1 for a in anchors if a.get("confidence") == "high") / len(anchors), 6)
        sig["anchor_conf_none_share"] = round(
            sum(1 for a in anchors if a.get("confidence") == "none") / len(anchors), 6)
    else:
        sig["anchor_conf_high_share"] = 0.0
        sig["anchor_conf_none_share"] = 0.0

    # --- G7 boundary contact (crop-cut risk) ---------------------------------
    eps = 0.004
    sides = set()
    for prim in geom.get("primitives", []):
        b = prim.get("normalized", {}).get("bbox")
        if not b:
            continue
        if b[0] <= eps:
            sides.add("left")
        if b[1] <= eps:
            sides.add("top")
        if b[2] >= 1 - eps:
            sides.add("right")
        if b[3] >= 1 - eps:
            sides.add("bottom")
    for t in texts:
        b = t.get("bbox_norm")
        if not b:
            continue
        if b[0] <= eps:
            sides.add("left")
        if b[1] <= eps:
            sides.add("top")
        if b[2] >= 1 - eps:
            sides.add("right")
        if b[3] >= 1 - eps:
            sides.add("bottom")
    sig["boundary_edges_touched"] = len(sides)

    # --- G8 repeat-family shape (the C5/C6 risk driver) ----------------------
    reps = description.get("repeated_elements", [])
    sig["repeat_families"] = len(reps)
    total_rep = sum(int(r.get("count", 0)) for r in reps)
    sig["repeat_top_count"] = int(reps[0].get("count", 0)) if reps else 0
    sig["repeat_top_share"] = round(sig["repeat_top_count"] / total_rep, 6) if total_rep else 0.0
    sig["repeat_circle24_share"] = round(
        sum(1 for r in reps if int(r.get("segment_count", 0)) == 24) / len(reps), 6) if reps else 0.0
    sig["repeat_families_le1"] = bool(len(reps) <= 1)

    # --- G9 self-declared quality -------------------------------------------
    sig["vector_quality"] = description.get("vector_quality")
    sig["quality_not_good"] = bool(description.get("vector_quality") != "GOOD")
    sig["hatch_saturated"] = bool(len(description.get("hatch_like_structures", [])) >= 30)

    return sig


# ---------------------------------------------------------------- pair signals

def pair_signals(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """O10 anisotropy and an O9-style four-parameter alignment residual."""
    out: dict[str, Any] = {}
    lb, rb = left.get("bbox"), right.get("bbox")
    if lb and rb:
        lw, lh = lb[2] - lb[0], lb[3] - lb[1]
        rw, rh = rb[2] - rb[0], rb[3] - rb[1]
        if lh and rh and rw:
            out["anisotropy"] = round((lw / lh) / (rw / rh), 6)
            out["anisotropy_excess"] = round(abs(math.log(out["anisotropy"])), 6)
    ls, rs = _segments(left), _segments(right)
    if ls.shape[0] and rs.shape[0]:
        lm = np.column_stack([(ls[:, 0] + ls[:, 2]) / 2, (ls[:, 1] + ls[:, 3]) / 2])
        rm = np.column_stack([(rs[:, 0] + rs[:, 2]) / 2, (rs[:, 1] + rs[:, 3]) / 2])
        # four-parameter fit from robust moments (translation + per-axis scale)
        res = {}
        for axis, name in ((0, "x"), (1, "y")):
            l_lo, l_hi = np.percentile(lm[:, axis], [5, 95])
            r_lo, r_hi = np.percentile(rm[:, axis], [5, 95])
            scale = (r_hi - r_lo) / (l_hi - l_lo) if (l_hi - l_lo) else 1.0
            shift = r_lo - scale * l_lo
            res[f"scale_{name}"] = round(float(scale), 6)
            res[f"shift_{name}"] = round(float(shift), 6)
        out.update(res)
        out["align_shift_max"] = round(max(abs(res["shift_x"]), abs(res["shift_y"])), 6)
        out["align_scale_dev_max"] = round(max(abs(res["scale_x"] - 1), abs(res["scale_y"] - 1)), 6)
        # a single "how far apart are the two normalized frames" number, in block units
        out["align_residual"] = round(out["align_shift_max"] + 0.5 * out["align_scale_dev_max"], 6)
    return out


# ------------------------------------------------------------------------ main

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--descriptions", nargs="*", default=[])
    ap.add_argument("--track-a", action="store_true")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    rows: list[dict[str, Any]] = []
    paths: list[tuple[str, Path]] = []
    if args.track_a:
        for pair_dir in sorted(TRACK_A_DESC.iterdir()):
            if not pair_dir.is_dir():
                continue
            for side in ("left", "right"):
                p = pair_dir / side / "vector_block.json"
                if p.exists():
                    paths.append((f"{pair_dir.name}:{side}", p))
    for raw in args.descriptions:
        paths.append((Path(raw).stem, Path(raw)))

    descs: dict[str, dict[str, Any]] = {}
    for name, path in paths:
        d = load_description(path)
        descs[name] = d
        row = {"id": name, "path": str(path)}
        row.update(compute_signals(d))
        rows.append(row)
        print(f"ok {name}: segments={row['segments']} text={row['text_items']} "
              f"readable={row['readable_text_ratio']} instab={row['group_instability']}",
              file=sys.stderr)

    pairs = {}
    if args.track_a:
        for pair_dir in sorted(TRACK_A_DESC.iterdir()):
            l, r = f"{pair_dir.name}:left", f"{pair_dir.name}:right"
            if l in descs and r in descs:
                pairs[pair_dir.name] = pair_signals(descs[l], descs[r])

    out = Path(args.out)
    if not out.is_absolute():
        out = EXP / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"rows": rows, "pairs": pairs}, ensure_ascii=False, indent=1) + "\n",
                   encoding="utf-8")
    print(f"wrote {out} ({len(rows)} rows, {len(pairs)} pairs)", file=sys.stderr)


if __name__ == "__main__":
    main()
