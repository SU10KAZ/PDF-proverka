# -*- coding: utf-8 -*-
"""F1 — coordinate system and /Rotate.

(a) render_block() must be pixel-identical to the production crop path.
(b) geometry read through clip_page must live inside the region the picture shows,
    while the naive path (production clip fed straight to get_drawings) must fail
    on /Rotate != 0.

Writes artifacts/fnd_rotation.json and overlays into artifacts/fnd_overlays/.
"""
from __future__ import annotations

import hashlib, json, os, random, re, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402
from backend.app.pipeline.stages.crop_blocks.blocks import crop_from_pdf  # noqa: E402  (read-only use)

ROOT = Path("experiments/stage_comparison_vector_objects_v03_opus")
ART = ROOT / "artifacts"
OVL = ART / "fnd_overlays"
TMP = Path("/tmp/claude-1001/-home-coder-projects-PDF-proverka/3b5be6df-cbfd-4d4c-a127-963748205759/scratchpad/fnd_render")
DPI = 100
MIN_LONG = 800
SEED = 20260823

WORD = re.compile(r"[0-9a-zA-Zа-яА-ЯёЁ]{2,}")
TAG = re.compile(r"<[^>]+>")


def toks(s: str) -> set[str]:
    s = TAG.sub(" ", s or "")
    return {w.lower() for w in WORD.findall(s)}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def sha(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def sample_blocks(n_rot0=30, n_rot90=20, n_rot270=20):
    rng = random.Random(SEED)
    buckets = defaultdict(list)
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["rotation_source"] == "missing":
                continue
            w, h = b["coords_px"][2] - b["coords_px"][0], b["coords_px"][3] - b["coords_px"][1]
            if w < 60 or h < 60:
                continue
            buckets[b["rotation"]].append(b)
    out = []
    for rot, n in ((0, n_rot0), (90, n_rot90), (270, n_rot270)):
        pool = buckets.get(rot, [])
        # spread over documents: at most 3 blocks per document
        rng.shuffle(pool)
        per_doc = Counter()
        picked = []
        for b in pool:
            k = (b["doc_id"], b["version"])
            if per_doc[k] >= 3:
                continue
            per_doc[k] += 1
            picked.append(b)
            if len(picked) >= n:
                break
        out.extend(picked)
    return out


def ink_mask(pix) -> np.ndarray:
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("L")
    return np.asarray(img) < 250


def seg_on_ink(segments, clip, rs, mask, samples=5, radius=1) -> float:
    if not segments:
        return float("nan")
    H, W = mask.shape
    hit = 0
    for s in segments:
        (x0, y0), (x1, y1) = s["p0"], s["p1"]
        ok = False
        for k in range(samples):
            t = (k + 0.5) / samples
            px = int((x0 + (x1 - x0) * t - clip[0]) * rs)
            py = int((y0 + (y1 - y0) * t - clip[1]) * rs)
            a, b = max(0, py - radius), min(H, py + radius + 1)
            c, d = max(0, px - radius), min(W, px + radius + 1)
            if a < b and c < d and mask[a:b, c:d].any():
                ok = True
                break
        hit += ok
    return hit / len(segments)


def overlay(pix, segments, clip, rs, out_png, color=(255, 0, 0)):
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("RGB")
    dr = ImageDraw.Draw(img)
    for s in segments:
        dr.line([((s["p0"][0] - clip[0]) * rs, (s["p0"][1] - clip[1]) * rs),
                 ((s["p1"][0] - clip[0]) * rs, (s["p1"][1] - clip[1]) * rs)],
                fill=color, width=1)
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    img.save(out_png)


def find_prod_png(b) -> Path | None:
    ver_dir = Path(b["result_json"]).parents[1]
    idx = ver_dir / "03_analysis/latest/blocks_stage02_100/index.json"
    if not idx.exists():
        return None
    try:
        d = json.loads(idx.read_text(encoding="utf-8"))
    except Exception:
        return None
    for e in d.get("blocks") or []:
        if e.get("block_id") == b["block_id"]:
            p = idx.parent / str(e.get("file") or "")
            return p if p.exists() else None
    return None


def cmp_png(a: Path, bimg: Image.Image):
    """Compare an existing production PNG with our pixmap, after size alignment."""
    ia = Image.open(a).convert("L")
    ib = bimg.convert("L")
    if ia.size != ib.size:
        ib = ib.resize(ia.size, Image.LANCZOS)
    A = np.asarray(ia).astype(np.int16)
    B = np.asarray(ib).astype(np.int16)
    diff = np.abs(A - B)
    return {
        "size_a": list(ia.size), "size_b": list(bimg.size),
        "mean_abs_diff": float(diff.mean()),
        "share_pixels_within_8": float((diff <= 8).mean()),
        "share_pixels_exact": float((diff == 0).mean()),
        "ink_iou": float(((A < 250) & (B < 250)).sum() /
                         max(1, ((A < 250) | (B < 250)).sum())),
    }


def main():
    OVL.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)
    blocks = sample_blocks()
    print("sample:", Counter(b["rotation"] for b in blocks))
    rows = []
    n_ovl = Counter()
    t0 = time.time()
    for i, b in enumerate(blocks):
        pdf, pi = b["pdf"], b["page_index"]
        cpx, (pw, ph) = b["coords_px"], b["page_px"]
        row = {"block_id": b["block_id"], "rotation": b["rotation"],
               "rotation_source": b["rotation_source"], "discipline": b["discipline"],
               "doc_id": b["doc_id"], "version": b["version"], "page_index": pi,
               "coords_px": cpx, "page_px": [pw, ph], "shape_type": b["shape_type"]}
        try:
            fr = F.block_frame(pdf, pi, cpx, pw, ph)
            row["clip_display"] = [fr.clip_display.x0, fr.clip_display.y0,
                                   fr.clip_display.x1, fr.clip_display.y1]
            row["clip_page"] = [fr.clip_page.x0, fr.clip_page.y0, fr.clip_page.x1, fr.clip_page.y1]

            # --- (a) pixel identity vs production ---------------------------------
            mine = TMP / f"mine_{b['block_id'][:24]}_{i}.png"
            prod = TMP / f"prod_{b['block_id'][:24]}_{i}.png"
            pix = F.render_block(pdf, pi, cpx, pw, ph, dpi=DPI, min_long_side=MIN_LONG,
                                 out_png=mine, frame=fr)
            crop_from_pdf(Path(pdf), pi + 1, list(cpx), pw, ph, prod,
                          dpi=DPI, min_long_side=MIN_LONG)
            row["render_size"] = [pix.width, pix.height]
            row["sha_mine"] = sha(mine)
            row["sha_prod"] = sha(prod)
            row["sha_equal"] = row["sha_mine"] == row["sha_prod"]
            pi_img = Image.open(prod).convert("L")
            mi_img = Image.open(mine).convert("L")
            A = np.asarray(pi_img).astype(np.int16)
            B = np.asarray(mi_img).astype(np.int16)
            row["pixel_equal_share_vs_prod"] = float((A == B).mean()) if A.shape == B.shape else 0.0

            existing = find_prod_png(b)
            if existing:
                row["stage02_png"] = str(existing)
                row["vs_stage02_png"] = cmp_png(existing, Image.frombytes(
                    "RGB", (pix.width, pix.height), pix.samples))

            # --- (b) geometry inside the region ------------------------------------
            rs = max(0.5, min(8.0, max(DPI / 72.0, MIN_LONG / max(fr.w, fr.h))))
            mask = ink_mask(pix)
            clip = row["clip_display"]

            ok = F.extract_block(pdf, pi, cpx, pw, ph, frame=fr)
            bad = F.extract_block(pdf, pi, cpx, pw, ph, frame=fr, naive_rotation=True)

            row["n_seg_correct"] = len(ok.segments)
            row["n_seg_naive"] = len(bad.segments)
            row["seg_on_ink_correct"] = seg_on_ink(ok.segments, clip, rs, mask)
            row["seg_on_ink_naive"] = seg_on_ink(bad.segments, clip, rs, mask)
            row["ink_share_of_crop"] = float(mask.mean())

            # segments inside clip (by construction 1.0 for correct; measured anyway)
            def inside(segs):
                if not segs:
                    return float("nan")
                c = 0
                for s in segs:
                    mx = (s["p0"][0] + s["p1"][0]) / 2
                    my = (s["p0"][1] + s["p1"][1]) / 2
                    if clip[0] - 1e-6 <= mx <= clip[2] + 1e-6 and clip[1] - 1e-6 <= my <= clip[3] + 1e-6:
                        c += 1
                return c / len(segs)
            row["seg_inside_clip_correct"] = inside(ok.segments)
            row["seg_inside_clip_naive"] = inside(bad.segments)

            ocr = toks(b_ocr(b))
            row["ocr_tokens"] = len(ocr)
            row["text_correct"] = len(ok.texts)
            row["text_naive"] = len(bad.texts)
            row["jaccard_correct"] = jaccard(toks(" ".join(t["text"] for t in ok.texts)), ocr)
            row["jaccard_naive"] = jaccard(toks(" ".join(t["text"] for t in bad.texts)), ocr)

            if n_ovl[b["rotation"]] < 3 and len(ok.segments) > 30:
                tag = f"rot{b['rotation']}_{n_ovl[b['rotation']]}_{b['block_id'][:12]}"
                overlay(pix, ok.segments, clip, rs, OVL / f"{tag}_correct.png", (220, 0, 0))
                overlay(pix, bad.segments, clip, rs, OVL / f"{tag}_naive.png", (0, 90, 220))
                row["overlay"] = tag
                n_ovl[b["rotation"]] += 1
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        rows.append(row)
        F.clear_caches()
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(blocks)} {time.time()-t0:.0f}s", flush=True)

    ok_rows = [r for r in rows if "error" not in r]
    summary = {"n_sampled": len(rows), "n_ok": len(ok_rows), "n_error": len(rows) - len(ok_rows),
               "dpi": DPI, "min_long_side": MIN_LONG, "seed": SEED,
               "by_rotation": {}}
    summary["render_sha_equal_all"] = sum(1 for r in ok_rows if r.get("sha_equal"))
    summary["render_pixel_equal_share_min"] = min((r.get("pixel_equal_share_vs_prod", 0) for r in ok_rows), default=None)
    st = [r["vs_stage02_png"] for r in ok_rows if r.get("vs_stage02_png")]
    if st:
        summary["vs_stage02_png"] = {
            "n": len(st),
            "median_share_within_8": float(np.median([s["share_pixels_within_8"] for s in st])),
            "median_ink_iou": float(np.median([s["ink_iou"] for s in st])),
            "n_share_within_8_ge_0995": sum(1 for s in st if s["share_pixels_within_8"] >= 0.995),
            "threshold_note": "совпадением считаем share_pixels_within_8 >= 0.995 после приведения "
                              "к одному размеру (разный dpi/ресемплинг даёт ±8 уровней серого на "
                              "антиалиасинге штрихов)",
        }
    for rot in (0, 90, 270):
        sub = [r for r in ok_rows if r["rotation"] == rot]
        if not sub:
            continue
        def med(key):
            v = [r[key] for r in sub if isinstance(r.get(key), float) and r[key] == r[key]]
            return float(np.median(v)) if v else None
        with_seg = [r for r in sub if r.get("n_seg_correct", 0) >= 10]
        summary["by_rotation"][str(rot)] = {
            "n": len(sub),
            "n_with_geometry": len(with_seg),
            "median_seg_on_ink_correct": float(np.median([r["seg_on_ink_correct"] for r in with_seg])) if with_seg else None,
            "median_seg_on_ink_naive": float(np.median([r["seg_on_ink_naive"] for r in with_seg if r["seg_on_ink_naive"] == r["seg_on_ink_naive"]])) if with_seg else None,
            "mean_seg_on_ink_correct": float(np.mean([r["seg_on_ink_correct"] for r in with_seg])) if with_seg else None,
            "mean_seg_on_ink_naive": float(np.nanmean([r["seg_on_ink_naive"] for r in with_seg])) if with_seg else None,
            "median_jaccard_correct": med("jaccard_correct"),
            "median_jaccard_naive": med("jaccard_naive"),
            "mean_jaccard_correct": float(np.mean([r["jaccard_correct"] for r in sub])),
            "mean_jaccard_naive": float(np.mean([r["jaccard_naive"] for r in sub])),
            "n_naive_zero_segments": sum(1 for r in sub if r.get("n_seg_naive") == 0),
            "n_correct_zero_segments": sum(1 for r in sub if r.get("n_seg_correct") == 0),
            "median_seg_inside_clip_correct": med("seg_inside_clip_correct"),
            "median_seg_inside_clip_naive": med("seg_inside_clip_naive"),
            "n_sha_equal": sum(1 for r in sub if r.get("sha_equal")),
        }
    (ART / "fnd_rotation.json").write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1,
                   default=lambda o: None), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


def b_ocr(b) -> str:
    """ocr_text is not carried in the jsonl index; re-read it lazily from result.json."""
    key = (b["result_json"], b["block_id"])
    if key in _OCR:
        return _OCR[key]
    blocks = F.iter_prepared_blocks(b["result_json"], graphic_only=True,
                                    resolve_rotation_from_pdf=False)
    for x in blocks:
        _OCR[(b["result_json"], x.block_id)] = x.ocr_text
    return _OCR.get(key, "")


_OCR: dict = {}

if __name__ == "__main__":
    main()
