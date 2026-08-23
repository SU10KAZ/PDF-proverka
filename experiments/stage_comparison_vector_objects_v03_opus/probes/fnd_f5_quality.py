# -*- coding: utf-8 -*-
"""F5 — how good is the source under the prepared blocks?

Independent random sample (own seed) of real prepared graphic blocks.  Measures what
fraction of the corpus is even usable for a vector object layer: raster-only blocks,
text converted to curves, broken text encodings, raster pasted over vector, and the
distribution of segment counts per block.
"""
from __future__ import annotations

import json, os, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
SEED = 771113
N = 320


def sample(n, seed=SEED, per_doc=2):
    rng = random.Random(seed)
    rows = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["rotation_source"] == "missing":
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


def main():
    blocks = sample(N)
    print("sample:", len(blocks), Counter(b["discipline"] for b in blocks))
    rows = []
    t0 = time.time()
    for i, b in enumerate(blocks):
        r = {"block_id": b["block_id"], "discipline": b["discipline"], "doc_id": b["doc_id"],
             "version": b["version"], "rotation": b["rotation"], "shape_type": b["shape_type"],
             "coords_px": b["coords_px"], "ocr_len": b["ocr_len"]}
        try:
            ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
            r.update({
                "n_seg": ex.inked_segments_count,
                "n_seg_raw": ex.segments_raw_count,
                "n_text": len(ex.texts),
                "n_images": len(ex.images),
                "n_curves": ex.curves_flattened_count,
                "border_share": ex.clipped_at_border_flags["share"],
                "S": ex.char_scale["S"],
                "s_text": ex.char_scale["s_text"],
                "area_pt2": (ex.frame["clip_display"][2] - ex.frame["clip_display"][0]) *
                            (ex.frame["clip_display"][3] - ex.frame["clip_display"][1]),
                **{f"q_{k}": v for k, v in ex.quality.items()},
            })
            if ex.images:
                r["max_img_dpi"] = max(max(im["dpi"]) for im in ex.images)
        except Exception as exc:
            r["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(r)
        F.clear_caches()
        if (i + 1) % 40 == 0:
            print(f"  {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    ok = [r for r in rows if "error" not in r]
    n = len(ok)
    segs = np.array([r["n_seg"] for r in ok])

    def share(pred):
        return sum(1 for r in ok if pred(r)) / max(1, n)

    by_disc = defaultdict(list)
    for r in ok:
        by_disc[r["discipline"]].append(r)

    summary = {
        "n_sampled": len(rows), "n_ok": n, "n_error": len(rows) - n, "seed": SEED,
        "raster_only": share(lambda r: r["q_raster_only"]),
        "no_vector_at_all": share(lambda r: r["n_seg"] == 0),
        "empty_block": share(lambda r: r["q_empty"]),
        "text_in_curves": share(lambda r: r["q_text_in_curves"]),
        "no_text_layer": share(lambda r: r["n_text"] == 0),
        "broken_text": share(lambda r: r["q_broken_text"]),
        "raster_over_vector": share(lambda r: r["q_raster_over_vector"]),
        "has_any_raster": share(lambda r: r["n_images"] > 0),
        "clamped_to_page": share(lambda r: r["q_clamped_to_page"]),
        "segments_per_block": {
            "median": float(np.median(segs)), "mean": float(segs.mean()),
            "p10": float(np.percentile(segs, 10)), "p25": float(np.percentile(segs, 25)),
            "p75": float(np.percentile(segs, 75)), "p90": float(np.percentile(segs, 90)),
            "p99": float(np.percentile(segs, 99)), "max": int(segs.max()),
            "share_lt_10": float((segs < 10).mean()),
            "share_lt_50": float((segs < 50).mean()),
            "share_ge_1000": float((segs >= 1000).mean()),
            "share_ge_10000": float((segs >= 10000).mean()),
        },
        "border_cut_share": {
            "median": float(np.median([r["border_share"] for r in ok])),
            "share_blocks_gt_5pct": share(lambda r: r["border_share"] > 0.05),
        },
        "usable_for_object_layer": share(
            lambda r: r["n_seg"] >= 20 and not r["q_raster_only"] and not r["q_broken_text"]),
        "by_discipline": {
            d: {"n": len(v),
                "median_segments": float(np.median([x["n_seg"] for x in v])),
                "raster_only": sum(1 for x in v if x["q_raster_only"]) / len(v),
                "no_text": sum(1 for x in v if x["n_text"] == 0) / len(v),
                "broken_text": sum(1 for x in v if x["q_broken_text"]) / len(v),
                "usable": sum(1 for x in v if x["n_seg"] >= 20 and not x["q_raster_only"]
                              and not x["q_broken_text"]) / len(v)}
            for d, v in sorted(by_disc.items())},
        "elapsed_s": round(time.time() - t0, 1),
    }
    (ART / "fnd_source_quality.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
