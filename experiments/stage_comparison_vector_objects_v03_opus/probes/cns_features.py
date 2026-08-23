# -*- coding: utf-8 -*-
"""CNS-1 — deterministic feature census of every prepared graphic block.

Reads blocks ONLY through probes/v03_foundation.py.  One row per real
block_type=="image" block from a real result.json.  No synthetic bboxes.

Output: artifacts/cns_features.jsonl (one JSON object per block).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")

MAX_SEG_FOR_HEAVY = 60000        # subsample cap for O(n log n) feature maths
DESIG_RE = re.compile(r"^[A-ZА-ЯЁ]{0,4}[-–—.]?\d{1,4}([.\-/]\d{1,4})?[A-ZА-ЯЁa-zа-яё]?$")
TOKEN_RE = re.compile(r"[^\s,;()]+")


def _sha(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "replace")).hexdigest()[:20]


def _q(a, qs):
    if len(a) == 0:
        return [0.0] * len(qs)
    return [float(v) for v in np.percentile(a, qs)]


def features(b: dict) -> dict:
    r = {
        "block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
        "discipline": b["discipline"], "obj_id": b["obj_id"],
        "page_number": b["page_number"], "page_index": b["page_index"],
        "rotation": b["rotation"], "shape_type": b["shape_type"],
        "category_code": b["category_code"], "ocr_len": b["ocr_len"],
        "pdf": b["pdf"],
    }
    ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
    cd = ex.frame["clip_display"]
    W = float(cd[2] - cd[0]); H = float(cd[3] - cd[1])
    area = max(W * H, 1e-9)
    pr = ex.frame["page_rect"]
    page_area = max((pr[2] - pr[0]) * (pr[3] - pr[1]), 1e-9)
    r.update({
        "W_pt": round(W, 2), "H_pt": round(H, 2), "area_pt2": round(area, 1),
        "page_area_frac": round(area / page_area, 5),
        "aspect": round(W / H, 4) if H > 0 else 0.0,
        "n_seg": ex.inked_segments_count, "n_seg_raw": ex.segments_raw_count,
        "n_paths": ex.paths_total, "n_curves": ex.curves_flattened_count,
        "n_text": len(ex.texts), "n_images": len(ex.images),
        "invisible_share": ex.quality["invisible_share_segments"],
        "raster_coverage": ex.quality["raster_coverage"],
        "raster_only": bool(ex.quality["raster_only"]),
        "has_vector": bool(ex.quality["has_vector"]),
        "empty": bool(ex.quality["empty"]),
        "text_in_curves_fnd": bool(ex.quality["text_in_curves"]),
        "broken_text": bool(ex.quality["broken_text"]),
        "garbled_ratio": ex.quality["garbled_ratio"],
        "border_share": ex.clipped_at_border_flags["share"],
        "S": round(ex.char_scale["S"], 3), "s_text": round(ex.char_scale["s_text"], 3),
    })
    r["raster_coverage_sum"] = round(min(1.0, sum(im["coverage_of_block"] for im in ex.images)), 4)
    r["max_img_dpi"] = round(max((max(im["dpi"]) for im in ex.images), default=0.0), 1)

    segs = ex.segments
    n = len(segs)
    if n:
        if n > MAX_SEG_FOR_HEAVY:
            step = n / MAX_SEG_FOR_HEAVY
            idx = (np.arange(MAX_SEG_FOR_HEAVY) * step).astype(int)
            sub = [segs[i] for i in idx]
            r["subsampled"] = True
        else:
            sub = segs
            r["subsampled"] = False
        P = np.array([[s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1]] for s in sub], dtype=np.float64)
        L = np.hypot(P[:, 2] - P[:, 0], P[:, 3] - P[:, 1])
        dx = np.abs(P[:, 2] - P[:, 0]); dy = np.abs(P[:, 3] - P[:, 1])
        tot = float(L.sum())
        r["total_len"] = round(tot, 1)
        r["len_med"], r["len_p90"], r["len_p99"], r["len_max"] = [round(v, 3) for v in _q(L, [50, 90, 99, 100])]
        r["len_p10"] = round(_q(L, [10])[0], 4)
        r["ink_density"] = round(tot / area, 5)          # pt of line per pt^2
        hor = dy <= 0.35
        ver = dx <= 0.35
        r["share_hor"] = round(float(hor.mean()), 4)
        r["share_ver"] = round(float(ver.mean()), 4)
        r["share_axis"] = round(float((hor | ver).mean()), 4)
        r["len_share_axis"] = round(float(L[hor | ver].sum() / max(tot, 1e-9)), 4)
        ops = Counter(s["op"] for s in sub)
        r["op_share_re"] = round((ops["re"] + ops["qu"]) / len(sub), 4)
        r["op_share_c"] = round(ops["c"] / len(sub), 4)
        r["op_share_l"] = round(ops["l"] / len(sub), 4)
        r["n_paths_in_block"] = len({s["path"] for s in sub})
        r["seg_per_path"] = round(len(sub) / max(1, r["n_paths_in_block"]), 3)
        widths = np.array([float(s["w"] or 0.0) for s in sub])
        r["n_stroke_widths"] = int(len(np.unique(np.round(widths, 2))))
        r["n_colors"] = len({s["color"] for s in sub})
        r["share_filled"] = round(float(np.mean([1.0 if s["fill"] else 0.0 for s in sub])), 4)

        # ---- ruling lines / grid ------------------------------------------------
        def rulings(mask, coord_a, coord_b, span, other_span):
            """Cluster axis-parallel segments by their constant coordinate and
            measure combined covered length per cluster."""
            if not mask.any():
                return 0, 0, 0.0, []
            c = coord_a[mask]
            lo = np.minimum(coord_b[mask, 0], coord_b[mask, 1])
            hi = np.maximum(coord_b[mask, 0], coord_b[mask, 1])
            order = np.argsort(c)
            c = c[order]; lo = lo[order]; hi = hi[order]
            groups = []
            cur = [0]
            for i in range(1, len(c)):
                if c[i] - c[cur[0]] <= 1.0:
                    cur.append(i)
                else:
                    groups.append(cur); cur = [i]
            groups.append(cur)
            long_half = 0; long_full = 0; covered = []
            for g in groups:
                iv = sorted(zip(lo[g], hi[g]))
                merged = 0.0; s0, e0 = iv[0]
                for s, e in iv[1:]:
                    if s <= e0 + 0.5:
                        e0 = max(e0, e)
                    else:
                        merged += e0 - s0; s0, e0 = s, e
                merged += e0 - s0
                if merged >= 0.5 * span:
                    long_half += 1
                    covered.append(float(np.mean(c[g])))
                if merged >= 0.9 * span:
                    long_full += 1
            return long_half, long_full, 0.0, covered

        cy = (P[:, 1] + P[:, 3]) / 2.0
        cx = (P[:, 0] + P[:, 2]) / 2.0
        h_half, h_full, _, h_pos = rulings(hor, cy, P[:, [0, 2]], W, H)
        v_half, v_full, _, v_pos = rulings(ver, cx, P[:, [1, 3]], H, W)
        r["n_rule_h"] = h_half; r["n_rule_h_full"] = h_full
        r["n_rule_v"] = v_half; r["n_rule_v_full"] = v_full
        r["grid_cells"] = max(0, h_half - 1) * max(0, v_half - 1)
        # vertical extent spanned by the horizontal rulings and vice versa
        r["rule_h_span"] = round((max(h_pos) - min(h_pos)) / H, 4) if len(h_pos) >= 2 else 0.0
        r["rule_v_span"] = round((max(v_pos) - min(v_pos)) / W, 4) if len(v_pos) >= 2 else 0.0
        # regularity of the row pitch
        if len(h_pos) >= 3:
            d = np.diff(sorted(h_pos))
            r["row_pitch_cv"] = round(float(np.std(d) / max(np.mean(d), 1e-9)), 4)
            r["row_pitch_med"] = round(float(np.median(d)), 3)
        else:
            r["row_pitch_cv"] = -1.0; r["row_pitch_med"] = 0.0
        # share of ink length lying ON a ruling line
        on_rule = 0.0
        if h_pos:
            hp = np.array(h_pos)
            m = hor.copy()
            if m.any():
                d = np.abs(cy[m][:, None] - hp[None, :]).min(axis=1)
                on_rule += float(L[m][d <= 1.0].sum())
        if v_pos:
            vp = np.array(v_pos)
            m = ver.copy()
            if m.any():
                d = np.abs(cx[m][:, None] - vp[None, :]).min(axis=1)
                on_rule += float(L[m][d <= 1.0].sum())
        r["len_share_on_rulings"] = round(on_rule / max(tot, 1e-9), 4)

        # ---- fingerprints -------------------------------------------------------
        s = max(W, H)
        Q = np.round(np.stack([(P[:, 0] - cd[0]) / s, (P[:, 1] - cd[1]) / s,
                               (P[:, 2] - cd[0]) / s, (P[:, 3] - cd[1]) / s], axis=1), 3)
        flip = (Q[:, 0] > Q[:, 2]) | ((Q[:, 0] == Q[:, 2]) & (Q[:, 1] > Q[:, 3]))
        Q[flip] = Q[flip][:, [2, 3, 0, 1]]
        keys = np.sort(np.array([f"{a:.3f},{b_:.3f},{c_:.3f},{d_:.3f}" for a, b_, c_, d_ in Q]))
        r["geom_sha"] = _sha("|".join(keys.tolist()))
        top = np.argsort(-L)[:64]
        r["geom_sha_top"] = _sha("|".join(sorted(f"{Q[i,0]:.3f},{Q[i,1]:.3f},{Q[i,2]:.3f},{Q[i,3]:.3f}" for i in top)))
    else:
        r["subsampled"] = False
        for k in ("total_len", "len_med", "len_p90", "len_p99", "len_max", "len_p10", "ink_density",
                  "share_hor", "share_ver", "share_axis", "len_share_axis", "op_share_re",
                  "op_share_c", "op_share_l", "seg_per_path", "share_filled",
                  "len_share_on_rulings", "rule_h_span", "rule_v_span", "row_pitch_med"):
            r[k] = 0.0
        for k in ("n_rule_h", "n_rule_h_full", "n_rule_v", "n_rule_v_full", "grid_cells",
                  "n_paths_in_block", "n_stroke_widths", "n_colors"):
            r[k] = 0
        r["row_pitch_cv"] = -1.0
        r["geom_sha"] = ""; r["geom_sha_top"] = ""

    # ---- text ------------------------------------------------------------------
    texts = ex.texts
    if texts:
        T = np.array([t["bbox"] for t in texts], dtype=np.float64)
        tarea = float(np.sum((T[:, 2] - T[:, 0]) * (T[:, 3] - T[:, 1])))
        chars = sum(len(t["text"]) for t in texts)
        r["n_chars"] = chars
        r["text_area_share"] = round(tarea / area, 5)
        r["text_lines_per_1e4pt2"] = round(len(texts) / area * 1e4, 4)
        r["chars_per_1e4pt2"] = round(chars / area * 1e4, 3)
        sizes = np.array([t["size"] for t in texts])
        r["text_size_med"] = round(float(np.median(sizes)), 3)
        r["text_size_cv"] = round(float(np.std(sizes) / max(np.mean(sizes), 1e-9)), 4)
        dirs = Counter((round(t["dir"][0]), round(t["dir"][1])) for t in texts)
        r["text_dir_main_share"] = round(dirs.most_common(1)[0][1] / len(texts), 4)
        r["n_text_dirs"] = len(dirs)
        # left-edge column clustering (legend / list signature)
        xs = np.sort(T[:, 0])
        cl = 1; big = 1; cur = 1
        for i in range(1, len(xs)):
            if xs[i] - xs[i - 1] <= max(2.0, 0.01 * W):
                cur += 1
            else:
                cl += 1; big = max(big, cur); cur = 1
        big = max(big, cur)
        r["text_left_col_share"] = round(big / len(texts), 4)
        r["n_text_left_cols"] = cl
        # row clustering (baseline rows)
        ys = np.sort((T[:, 1] + T[:, 3]) / 2)
        rows = 1
        for i in range(1, len(ys)):
            if ys[i] - ys[i - 1] > max(1.0, 0.004 * H):
                rows += 1
        r["n_text_rows"] = rows
        if rows >= 3:
            yc = [ys[0]]
            for v in ys[1:]:
                if v - yc[-1] > max(1.0, 0.004 * H):
                    yc.append(v)
            d = np.diff(yc)
            r["text_row_pitch_cv"] = round(float(np.std(d) / max(np.mean(d), 1e-9)), 4)
        else:
            r["text_row_pitch_cv"] = -1.0
        toks = []
        for t in texts:
            toks.extend(TOKEN_RE.findall(t["text"]))
        desig = [t for t in toks if DESIG_RE.match(t)]
        r["n_tokens"] = len(toks); r["n_uniq_tokens"] = len(set(toks))
        r["n_desig"] = len(desig); r["n_uniq_desig"] = len(set(desig))
        r["digit_token_share"] = round(sum(1 for t in toks if any(ch.isdigit() for ch in t)) / max(1, len(toks)), 4)
        r["text_sha"] = _sha("|".join(sorted(set(t["text"] for t in texts))))
        # designation adjacent to real geometry
        if n:
            SC = max(ex.char_scale["S"], 1.0)
            mid = np.stack([(P[:, 0] + P[:, 2]) / 2, (P[:, 1] + P[:, 3]) / 2], axis=1)
            if len(mid) > 20000:
                mid = mid[np.linspace(0, len(mid) - 1, 20000).astype(int)]
            cell = max(2.0 * SC, 4.0)
            grid = defaultdict(int)
            for gx, gy in np.floor(mid / cell).astype(int):
                grid[(int(gx), int(gy))] += 1
            near = 0
            uniq_near = set()
            for t in texts:
                tk = TOKEN_RE.findall(t["text"])
                if not any(DESIG_RE.match(x) for x in tk):
                    continue
                gx = int(math.floor(t["cx"] / cell)); gy = int(math.floor(t["cy"] / cell))
                hit = any(grid.get((gx + i, gy + j), 0) for i in (-1, 0, 1) for j in (-1, 0, 1))
                if hit:
                    near += 1
                    uniq_near.update(x for x in tk if DESIG_RE.match(x))
            r["n_desig_near_geom"] = near
            r["n_uniq_desig_near_geom"] = len(uniq_near)
        else:
            r["n_desig_near_geom"] = 0; r["n_uniq_desig_near_geom"] = 0
    else:
        for k in ("n_chars", "n_tokens", "n_uniq_tokens", "n_desig", "n_uniq_desig",
                  "n_text_rows", "n_text_left_cols", "n_text_dirs", "n_desig_near_geom",
                  "n_uniq_desig_near_geom"):
            r[k] = 0
        for k in ("text_area_share", "text_lines_per_1e4pt2", "chars_per_1e4pt2",
                  "text_size_med", "text_size_cv", "text_dir_main_share",
                  "text_left_col_share", "digit_token_share"):
            r[k] = 0.0
        r["text_row_pitch_cv"] = -1.0
        r["text_sha"] = ""
    return r


def worker(task):
    pdf, blocks = task
    out = []
    t0 = time.time()
    for b in blocks:
        try:
            out.append(features(b))
        except Exception as exc:
            out.append({"block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
                        "discipline": b["discipline"], "pdf": b["pdf"],
                        "error": f"{type(exc).__name__}: {exc}"})
        F._DRAW_CACHE.clear()
    F.clear_caches()
    return pdf, out, round(time.time() - t0, 1)


def main():
    import multiprocessing as mp
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nproc = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    outname = sys.argv[3] if len(sys.argv) > 3 else "cns_features.jsonl"

    by_pdf = defaultdict(list)
    exists: dict[str, bool] = {}
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            p = b["pdf"]
            if p not in exists:
                exists[p] = os.path.exists(p)
            if not exists[p]:
                continue
            by_pdf[p].append(b)
    tasks = sorted(by_pdf.items(), key=lambda kv: -len(kv[1]))
    if limit:
        import random
        rng = random.Random(20260823)
        allb = [b for _, v in tasks for b in v]
        rng.shuffle(allb)
        allb = allb[:limit]
        by = defaultdict(list)
        for b in allb:
            by[b["pdf"]].append(b)
        tasks = sorted(by.items(), key=lambda kv: -len(kv[1]))
    total = sum(len(v) for _, v in tasks)
    print(f"docs={len(tasks)} blocks={total}", flush=True)

    done = 0
    t0 = time.time()
    with open(ART / outname, "w", encoding="utf-8") as out:
        with mp.get_context("fork").Pool(nproc, maxtasksperchild=4) as pool:
            for pdf, rows, dt in pool.imap_unordered(worker, tasks):
                for r in rows:
                    out.write(json.dumps(r, ensure_ascii=False) + "\n")
                out.flush()
                done += len(rows)
                el = time.time() - t0
                print(f"{done}/{total} {el:.0f}s eta={el/max(done,1)*(total-done):.0f}s  {dt}s {Path(pdf).parent.parent.parent.name}", flush=True)
    print("DONE", time.time() - t0)


if __name__ == "__main__":
    main()
