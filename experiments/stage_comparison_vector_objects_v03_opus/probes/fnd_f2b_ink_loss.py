# -*- coding: utf-8 -*-
"""F2b — is dropping the "invisible" paths lossless?

The contrast test in fnd_ink.json showed invisible-rule segments sit on flat paper
30-50x more often than ordinary segments, but 62-79 % of them still land on ink.  That
is not yet a proof, because a phantom edge can COINCIDE with a real, separately
stroked edge.  This probe splits every dropped segment into three buckets:

  phantom    — the crop is flat paper there (no visible edge at all)
  redundant  — a KEPT (visible) segment is nearly coincident with it
  at_risk    — neither: something visible is there and nothing kept explains it

Only `at_risk` is information loss.  That number decides whether the rule is safe.
"""
from __future__ import annotations

import collections, json, math, os, sys, time
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
DPI = 200
FLAT = 20
COINC_PT = 0.5          # coincidence tolerance in PDF points
ANG_TOL = 3.0
N_BLOCKS = 120


def feats(segs):
    out = np.empty((len(segs), 5), dtype=np.float64)
    for i, s in enumerate(segs):
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        out[i] = ((x0 + x1) / 2, (y0 + y1) / 2, math.hypot(x1 - x0, y1 - y0),
                  math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0, 0)
    return out


def coincident(dropped, kept, tol=COINC_PT):
    """A dropped segment is redundant when a kept segment overlaps it geometrically."""
    if not kept:
        return [False] * len(dropped)
    K = feats(kept)
    cell = max(tol * 4, 1e-6)
    grid = collections.defaultdict(list)
    for j in range(len(K)):
        grid[(int(K[j, 0] // cell), int(K[j, 1] // cell))].append(j)
    D = feats(dropped)
    out = []
    for i in range(len(D)):
        gx, gy = int(D[i, 0] // cell), int(D[i, 1] // cell)
        found = False
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for j in grid.get((gx + dx, gy + dy), ()):
                    da = abs(D[i, 3] - K[j, 3])
                    if min(da, 180 - da) > ANG_TOL:
                        continue
                    # perpendicular distance of the dropped midpoint to the kept line
                    th = math.radians(K[j, 3])
                    ux, uy = math.cos(th), math.sin(th)
                    vx, vy = D[i, 0] - K[j, 0], D[i, 1] - K[j, 1]
                    perp = abs(-uy * vx + ux * vy)
                    along = abs(ux * vx + uy * vy)
                    if perp <= tol and along <= K[j, 2] / 2 + D[i, 2] / 2 + tol:
                        found = True
                        break
                if found:
                    break
            if found:
                break
        out.append(found)
    return out


def contrast_at(A, segs, clip, rs, samples=3):
    H, W = A.shape
    vals = []
    for s in segs:
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        best = 0
        for k in range(samples):
            t = (k + 0.5) / samples
            px = int((x0 + (x1 - x0) * t - clip[0]) * rs)
            py = int((y0 + (y1 - y0) * t - clip[1]) * rs)
            a, b = max(0, py - 2), min(H, py + 3)
            c, d = max(0, px - 2), min(W, px + 3)
            if a >= b or c >= d:
                continue
            win = A[a:b, c:d]
            best = max(best, int(win.max() - win.min()))
        vals.append(best)
    return vals


def main():
    src = json.loads((ART / "fnd_ink.json").read_text(encoding="utf-8"))
    want = {r["block_id"] for r in src["rows"] if r.get("seg_invisible", 0) > 0}
    blocks = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["block_id"] in want:
                blocks.append(b)
                if len(blocks) >= N_BLOCKS:
                    break
    print("blocks with invisible paint:", len(blocks))
    tot = collections.Counter()
    by_rule = collections.defaultdict(collections.Counter)
    rows = []
    t0 = time.time()
    for i, b in enumerate(blocks):
        try:
            fr = F.block_frame(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
            ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 frame=fr, keep_dropped_segments=True)
            dropped = ex.quality.pop("dropped_segments", [])
            if not dropped:
                continue
            if len(dropped) > 20000 or len(ex.segments) > 200000:
                rows.append({"block_id": b["block_id"], "skipped": "too big",
                             "n_dropped": len(dropped), "n_kept": len(ex.segments)})
                continue
            rs = max(0.5, min(8.0, DPI / 72.0))
            pix = F.render_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 dpi=DPI, frame=fr)
            A = np.asarray(Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                           .convert("L")).astype(np.int16)
            clip = ex.frame["clip_display"]
            cvals = contrast_at(A, dropped, clip, rs)
            red = coincident(dropped, ex.segments)
            r = {"block_id": b["block_id"], "discipline": b["discipline"],
                 "n_kept": len(ex.segments), "n_dropped": len(dropped)}
            c = collections.Counter()
            for s, cv, rd in zip(dropped, cvals, red):
                bucket = "phantom" if cv < FLAT else ("redundant" if rd else "at_risk")
                c[bucket] += 1
                tot[bucket] += 1
                by_rule[s["ink_rule"]][bucket] += 1
            r.update({k: c[k] for k in ("phantom", "redundant", "at_risk")})
            r["at_risk_share"] = c["at_risk"] / max(1, len(dropped))
            r["at_risk_share_of_kept"] = c["at_risk"] / max(1, len(ex.segments))
            rows.append(r)
        except Exception as exc:
            rows.append({"block_id": b["block_id"], "error": f"{type(exc).__name__}: {exc}"})
        F.clear_caches()
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    n = sum(tot.values())
    ok = [r for r in rows if "at_risk" in r]
    summary = {
        "n_blocks": len(ok), "n_dropped_segments": n,
        "phantom": tot["phantom"], "redundant": tot["redundant"], "at_risk": tot["at_risk"],
        "share_phantom": tot["phantom"] / max(1, n),
        "share_redundant": tot["redundant"] / max(1, n),
        "share_at_risk": tot["at_risk"] / max(1, n),
        "share_lossless": (tot["phantom"] + tot["redundant"]) / max(1, n),
        "median_block_at_risk_share": float(np.median([r["at_risk_share"] for r in ok])) if ok else None,
        "p90_block_at_risk_share": float(np.percentile([r["at_risk_share"] for r in ok], 90)) if ok else None,
        "median_at_risk_relative_to_kept": float(np.median([r["at_risk_share_of_kept"] for r in ok])) if ok else None,
        "p90_at_risk_relative_to_kept": float(np.percentile([r["at_risk_share_of_kept"] for r in ok], 90)) if ok else None,
        "by_rule": {k: dict(v) for k, v in by_rule.items()},
        "params": {"dpi": DPI, "flat_threshold": FLAT, "coincidence_pt": COINC_PT,
                   "angle_tol_deg": ANG_TOL},
    }
    (ART / "fnd_ink_loss.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
