# -*- coding: utf-8 -*-
"""F2 — invisible paint census + empirical proof that each rule is provable.

Census over >=200 real prepared graphic blocks: how many paths paint nothing, what
share of the segments they produce, per discipline, and how many blocks are >20 %
invisible.

Proof: an "invisible" path is claimed to leave NO visible edge in the rendered crop.
That is measurable — render the crop, walk along the segments the path would emit and
measure the local raster contrast (max-min of a 5x5 window).  A real stroke sits on a
high-contrast ridge; a phantom edge sits in flat paper.  We compare the distributions.
"""
from __future__ import annotations

import json, os, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
SEED = 20260823
N_CENSUS = 260
N_PROOF = 60
PROOF_DPI = 200
CONTRAST_FLAT = 20      # 5x5 window range below this = flat paper, no visible edge


def sample(n, seed=SEED, per_doc=2, min_side=60):
    rng = random.Random(seed)
    rows = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["rotation_source"] == "missing":
                continue
            if b["coords_px"][2] - b["coords_px"][0] < min_side:
                continue
            if b["coords_px"][3] - b["coords_px"][1] < min_side:
                continue
            rows.append(b)
    rng.shuffle(rows)
    seen = Counter()
    out = []
    for b in rows:
        k = (b["doc_id"], b["version"])
        if seen[k] >= per_doc:
            continue
        seen[k] += 1
        out.append(b)
        if len(out) >= n:
            break
    return out


def contrast_profile(pix, segs, clip, rs, max_seg=400, samples=3):
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("L")
    A = np.asarray(img).astype(np.int16)
    H, W = A.shape
    vals = []
    step = max(1, len(segs) // max_seg)
    for s in segs[::step]:
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        for k in range(samples):
            t = (k + 0.5) / samples
            px = int((x0 + (x1 - x0) * t - clip[0]) * rs)
            py = int((y0 + (y1 - y0) * t - clip[1]) * rs)
            a, b = max(0, py - 2), min(H, py + 3)
            c, d = max(0, px - 2), min(W, px + 3)
            if a >= b or c >= d:
                continue
            win = A[a:b, c:d]
            vals.append(int(win.max() - win.min()))
    return vals


def main():
    blocks = sample(N_CENSUS)
    print("census blocks:", len(blocks))
    rows = []
    t0 = time.time()
    tot = Counter()
    by_disc = defaultdict(Counter)
    for i, b in enumerate(blocks):
        pdf, pi, cpx, (pw, ph) = b["pdf"], b["page_index"], b["coords_px"], b["page_px"]
        r = {"block_id": b["block_id"], "discipline": b["discipline"], "doc_id": b["doc_id"],
             "version": b["version"], "rotation": b["rotation"]}
        try:
            ex = F.extract_block(pdf, pi, cpx, pw, ph, drop_invisible=True,
                                 keep_dropped_segments=True)
            dropped = ex.quality.pop("dropped_segments", [])
            by_rule_seg = Counter(s["ink_rule"] for s in dropped)
            r.update({
                "paths_total": ex.paths_total,
                "paths_invisible": ex.paths_invisible,
                "paths_outside_clip": ex.paths_outside_clip,
                "seg_raw": ex.segments_raw_count,
                "seg_inked": ex.inked_segments_count,
                "seg_invisible": ex.invisible_dropped,
                "invisible_share": (ex.invisible_dropped / ex.segments_raw_count) if ex.segments_raw_count else 0.0,
                "by_rule_paths": dict(ex.invisible_by_rule),
                "by_rule_segments": dict(by_rule_seg),
            })
            tot["paths_total"] += ex.paths_total
            tot["paths_invisible"] += ex.paths_invisible
            tot["paths_outside_clip"] += ex.paths_outside_clip
            tot["seg_raw"] += ex.segments_raw_count
            tot["seg_invisible"] += ex.invisible_dropped
            tot["seg_inked"] += ex.inked_segments_count
            for k, v in by_rule_seg.items():
                tot["rule_seg_" + str(k)] += v
            for k, v in ex.invisible_by_rule.items():
                tot["rule_path_" + str(k)] += v
            d = by_disc[b["discipline"]]
            d["seg_raw"] += ex.segments_raw_count
            d["seg_invisible"] += ex.invisible_dropped
            d["blocks"] += 1
        except Exception as exc:
            r["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(r)
        F.clear_caches()
        if (i + 1) % 40 == 0:
            print(f"  census {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    ok = [r for r in rows if "error" not in r and r.get("seg_raw", 0) > 0]
    shares = sorted(r["invisible_share"] for r in ok)
    census = {
        "n_blocks_sampled": len(rows),
        "n_blocks_with_geometry": len(ok),
        "n_blocks_error": sum(1 for r in rows if "error" in r),
        "paths_total": tot["paths_total"],
        "paths_invisible": tot["paths_invisible"],
        "paths_outside_clip": tot["paths_outside_clip"],
        "share_paths_invisible": tot["paths_invisible"] / max(1, tot["paths_total"]),
        "segments_raw": tot["seg_raw"],
        "segments_invisible": tot["seg_invisible"],
        "share_segments_invisible": tot["seg_invisible"] / max(1, tot["seg_raw"]),
        "median_block_invisible_share": float(np.median(shares)) if shares else None,
        "mean_block_invisible_share": float(np.mean(shares)) if shares else None,
        "n_blocks_invisible_gt_20pct": sum(1 for s in shares if s > 0.20),
        "share_blocks_invisible_gt_20pct": (sum(1 for s in shares if s > 0.20) / len(shares)) if shares else None,
        "n_blocks_invisible_gt_50pct": sum(1 for s in shares if s > 0.50),
        "by_rule_segments": {k[len("rule_seg_"):]: v for k, v in tot.items() if k.startswith("rule_seg_")},
        "by_rule_paths": {k[len("rule_path_"):]: v for k, v in tot.items() if k.startswith("rule_path_")},
        "by_discipline": {k: {"blocks": v["blocks"], "segments_raw": v["seg_raw"],
                              "segments_invisible": v["seg_invisible"],
                              "share": v["seg_invisible"] / max(1, v["seg_raw"])}
                          for k, v in sorted(by_disc.items())},
    }

    # ---------------- proof pass ------------------------------------------------
    proof_blocks = [b for b in blocks if True][:N_PROOF]
    vis_vals: list[int] = []
    inv_vals: dict[str, list[int]] = defaultdict(list)
    per_block = []
    for i, b in enumerate(proof_blocks):
        pdf, pi, cpx, (pw, ph) = b["pdf"], b["page_index"], b["coords_px"], b["page_px"]
        try:
            fr = F.block_frame(pdf, pi, cpx, pw, ph)
            ex = F.extract_block(pdf, pi, cpx, pw, ph, drop_invisible=True,
                                 keep_dropped_segments=True, frame=fr)
            dropped = ex.quality.pop("dropped_segments", [])
            if not dropped or not ex.segments:
                continue
            rs = max(0.5, min(8.0, PROOF_DPI / 72.0))
            pix = F.render_block(pdf, pi, cpx, pw, ph, dpi=PROOF_DPI, frame=fr)
            clip = ex.frame["clip_display"]
            v = contrast_profile(pix, ex.segments, clip, rs)
            vis_vals.extend(v)
            byrule = defaultdict(list)
            for s in dropped:
                byrule[s["ink_rule"]].append(s)
            blk = {"block_id": b["block_id"], "n_visible_samples": len(v),
                   "median_visible_contrast": float(np.median(v)) if v else None}
            for rule, segs in byrule.items():
                iv = contrast_profile(pix, segs, clip, rs)
                inv_vals[rule].extend(iv)
                blk[f"median_contrast_{rule}"] = float(np.median(iv)) if iv else None
            per_block.append(blk)
        except Exception as exc:
            per_block.append({"block_id": b["block_id"], "error": f"{type(exc).__name__}: {exc}"})
        F.clear_caches()
        if (i + 1) % 20 == 0:
            print(f"  proof {i+1}/{len(proof_blocks)} {time.time()-t0:.0f}s", flush=True)

    def stat(v):
        if not v:
            return None
        a = np.array(v)
        return {"n": int(a.size), "median": float(np.median(a)), "mean": float(a.mean()),
                "p90": float(np.percentile(a, 90)),
                "share_flat_lt20": float((a < CONTRAST_FLAT).mean())}

    proof = {
        "dpi": PROOF_DPI,
        "window": "5x5 max-min of grayscale",
        "flat_threshold": CONTRAST_FLAT,
        "visible_segments": stat(vis_vals),
        "invisible_segments_by_rule": {k: stat(v) for k, v in inv_vals.items()},
        "n_blocks_used": len([p for p in per_block if "error" not in p]),
        "per_block": per_block[:80],
    }

    (ART / "fnd_ink.json").write_text(json.dumps({
        "census": census, "proof": proof, "rules": F.INK_RULES, "seed": SEED,
        "rows": rows,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"census": {k: v for k, v in census.items() if k != "by_discipline"},
                      "by_discipline": census["by_discipline"],
                      "proof": {k: v for k, v in proof.items() if k != "per_block"}},
                     ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
