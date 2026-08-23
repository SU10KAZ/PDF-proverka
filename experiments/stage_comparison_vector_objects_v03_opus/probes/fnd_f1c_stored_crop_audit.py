# -*- coding: utf-8 -*-
"""F1c — does the STORED crop PNG show the region coords_px describes?

Our render is byte-identical to production's crop_from_pdf (F1a).  But most stored
crops in blocks_stage02_100/ were fetched from the crop service (source="cloud"), not
rendered locally.  If the service used a different bbox convention the human reviewed a
different picture than a probe reads.  Measured as 1-px-dilated ink IoU on 200 blocks
that have a stored PNG.
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
SEED = 4242
N = 200
GOOD = 0.60      # dilated ink IoU above this = the stored crop shows our region


def dil(M):
    D = M.copy()
    for ax in (0, 1):
        for s in (-1, 1):
            D |= np.roll(M, s, axis=ax)
    return D


def main():
    rng = random.Random(SEED)
    # index of stored crops per version dir
    idxcache: dict[str, dict] = {}
    cands = []
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            vd = str(Path(b["result_json"]).parents[1])
            if vd not in idxcache:
                p = Path(vd) / "03_analysis/latest/blocks_stage02_100/index.json"
                m = {}
                if p.exists():
                    try:
                        jd = json.loads(p.read_text(encoding="utf-8"))
                        for e in jd.get("blocks") or []:
                            f = p.parent / str(e.get("file") or "")
                            if f.exists():
                                m[str(e.get("block_id"))] = (str(f), e.get("source"))
                    except Exception:
                        pass
                idxcache[vd] = m
            hit = idxcache[vd].get(b["block_id"])
            if hit:
                b["png"], b["png_source"] = hit
                cands.append(b)
    print("blocks with a stored PNG:", len(cands))
    rng.shuffle(cands)
    per_doc = Counter()
    sel = []
    for b in cands:
        k = (b["doc_id"], b["version"])
        if per_doc[k] >= 3:
            continue
        per_doc[k] += 1
        sel.append(b)
        if len(sel) >= N:
            break

    rows = []
    t0 = time.time()
    for i, b in enumerate(sel):
        r = {"block_id": b["block_id"], "rotation": b["rotation"], "discipline": b["discipline"],
             "png_source": b["png_source"], "legacy_id": not b["block_id"].startswith("blk_"),
             "page_index_conflict": b["page_index_conflict"]}
        try:
            pix = F.render_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"],
                                 dpi=100, min_long_side=800)
            mine = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            st = Image.open(b["png"])
            r["size_stored"] = list(st.size)
            r["size_mine"] = [pix.width, pix.height]
            r["size_rel_diff"] = max(abs(st.size[0] - pix.width) / max(1, pix.width),
                                     abs(st.size[1] - pix.height) / max(1, pix.height))
            if st.size != mine.size:
                mine = mine.resize(st.size, Image.LANCZOS)
            A = np.asarray(st.convert("L")) < 250
            B = np.asarray(mine.convert("L")) < 250
            r["ink_iou_dilated"] = float((dil(A) & dil(B)).sum() / max(1, (dil(A) | dil(B)).sum()))
            r["ink_stored"] = float(A.mean())
            r["ink_mine"] = float(B.mean())
        except Exception as exc:
            r["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(r)
        F.clear_caches()
        if (i + 1) % 40 == 0:
            print(f"  {i+1}/{len(sel)} {time.time()-t0:.0f}s", flush=True)

    ok = [r for r in rows if "ink_iou_dilated" in r]
    v = np.array([r["ink_iou_dilated"] for r in ok])
    byrot = defaultdict(list)
    for r in ok:
        byrot[r["rotation"]].append(r["ink_iou_dilated"])
    summary = {
        "n_blocks_with_stored_png_in_corpus": len(cands),
        "n_sampled": len(rows), "n_ok": len(ok), "seed": SEED,
        "png_source_hist": dict(Counter(r["png_source"] for r in rows)),
        "median_ink_iou_dilated": float(np.median(v)),
        "share_iou_ge_0.60": float((v >= GOOD).mean()),
        "share_iou_lt_0.30": float((v < 0.30).mean()),
        "share_size_rel_diff_gt_1pct": float(np.mean([r["size_rel_diff"] > 0.01 for r in ok])),
        "by_rotation": {str(k): {"n": len(x), "median_iou": float(np.median(x)),
                                 "share_ge_0.60": float(np.mean(np.array(x) >= GOOD))}
                        for k, x in sorted(byrot.items())},
        "mismatch_by_legacy_id": {
            "legacy": float(np.mean([r["ink_iou_dilated"] < GOOD for r in ok if r["legacy_id"]]))
            if any(r["legacy_id"] for r in ok) else None,
            "blk_prefixed": float(np.mean([r["ink_iou_dilated"] < GOOD for r in ok if not r["legacy_id"]]))
            if any(not r["legacy_id"] for r in ok) else None,
        },
        "note": "1-px dilation removes hairline phase noise; IoU below 0.30 means a different region",
    }
    (ART / "fnd_stored_crop_audit.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
