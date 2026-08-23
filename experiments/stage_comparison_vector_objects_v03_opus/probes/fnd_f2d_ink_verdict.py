# -*- coding: utf-8 -*-
"""F2d — final ink verdict: does the filter delete a VISIBLE LINE that nothing replaces?

Per dropped segment, two independent measurements on the 200-dpi crop:
  ridge      the outline is a dark ridge (on-line pixel darker than both sides by >=30
             on the majority of samples) -> a visible line exists exactly there
  redundant  a KEPT segment is geometrically coincident with it (<=0.5 pt, <=3 deg)

A white path cannot draw a dark ridge, so `ridge` means some OTHER path drew that line.
The only genuinely lossy bucket is `ridge & not redundant`.
"""
from __future__ import annotations

import collections, json, math, os, sys, time
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402
from experiments.stage_comparison_vector_objects_v03_opus.probes.fnd_f2b_ink_loss import coincident  # noqa: E402
from experiments.stage_comparison_vector_objects_v03_opus.probes.fnd_f2c_ink_attrib import ridge_share, RS, RIDGE_SHARE, DPI  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
N_BLOCKS = 120


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
    tot = collections.Counter()
    rows = []
    t0 = time.time()
    for i, b in enumerate(blocks):
        try:
            fr = F.block_frame(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
            ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 frame=fr, keep_dropped_segments=True)
            dropped = ex.quality.pop("dropped_segments", [])
            if not dropped or len(dropped) > 20000:
                continue
            pix = F.render_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 dpi=DPI, frame=fr)
            A = np.asarray(Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                           .convert("L")).astype(np.int16)
            clip = ex.frame["clip_display"]
            red = coincident(dropped, ex.segments)
            c = collections.Counter()
            for s, rd in zip(dropped, red):
                is_ridge = ridge_share(A, s, clip) >= RIDGE_SHARE
                k = ("ridge" if is_ridge else "no_ridge") + ("_redundant" if rd else "_alone")
                c[k] += 1
                tot[k] += 1
            r = {"block_id": b["block_id"], "discipline": b["discipline"],
                 "n_dropped": len(dropped), "n_kept": len(ex.segments), **dict(c)}
            r["lossy_share_of_dropped"] = c["ridge_alone"] / max(1, len(dropped))
            r["lossy_share_of_kept"] = c["ridge_alone"] / max(1, len(ex.segments))
            rows.append(r)
        except Exception as exc:
            rows.append({"block_id": b["block_id"], "error": f"{type(exc).__name__}: {exc}"})
        F.clear_caches()
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    n = sum(tot.values())
    ok = [r for r in rows if "lossy_share_of_dropped" in r]
    summary = {
        "n_blocks": len(ok), "n_dropped_segments": n,
        "buckets": dict(tot),
        "share_ridge_alone_LOSSY": tot["ridge_alone"] / max(1, n),
        "share_ridge_redundant": tot["ridge_redundant"] / max(1, n),
        "share_no_ridge_alone": tot["no_ridge_alone"] / max(1, n),
        "share_no_ridge_redundant": tot["no_ridge_redundant"] / max(1, n),
        "median_block_lossy_share_of_dropped": float(np.median([r["lossy_share_of_dropped"] for r in ok])),
        "p90_block_lossy_share_of_dropped": float(np.percentile([r["lossy_share_of_dropped"] for r in ok], 90)),
        "median_block_lossy_share_of_kept": float(np.median([r["lossy_share_of_kept"] for r in ok])),
        "p90_block_lossy_share_of_kept": float(np.percentile([r["lossy_share_of_kept"] for r in ok], 90)),
        "n_blocks_lossy_gt_2pct_of_kept": sum(1 for r in ok if r["lossy_share_of_kept"] > 0.02),
    }
    (ART / "fnd_ink_verdict.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
