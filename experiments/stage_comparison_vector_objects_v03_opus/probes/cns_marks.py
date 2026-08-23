# -*- coding: utf-8 -*-
"""CNS-7 — how many prepared blocks carry NO object designation next to geometry?

`cns_features.jsonl` counts designations with a loose regex that also matches a
bare dimension number ("2080").  This probe re-reads a random sample of real
blocks and separates three strengths of anchor:

  loose  : any token matching the designation regex (incl. pure numbers)
  mark   : token containing BOTH a letter and a digit  (К1, ВР-1, Ст-3, П14.5-1)
  marked : such a token whose text box sits within 2*S of inked geometry
"""
from __future__ import annotations
import json, math, os, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path
import numpy as np

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F
from experiments.stage_comparison_vector_objects_v03_opus.probes.cns_features import TOKEN_RE, DESIG_RE

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")

LETTERS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЭЮЯabcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщэюя")


def is_mark(t: str) -> bool:
    if len(t) < 2 or len(t) > 16:
        return False
    has_l = any(ch in LETTERS for ch in t)
    has_d = any(ch.isdigit() for ch in t)
    return has_l and has_d


def one(b):
    ex = F.extract_block(b["pdf"], b["page_index"], b["coords_px"], *b["page_px"])
    segs = ex.segments
    S = max(ex.char_scale["S"], 1.0)
    cell = max(2.0 * S, 4.0)
    grid = defaultdict(int)
    if segs:
        P = np.array([[(s["p0"][0] + s["p1"][0]) / 2, (s["p0"][1] + s["p1"][1]) / 2] for s in segs])
        if len(P) > 20000:
            P = P[np.linspace(0, len(P) - 1, 20000).astype(int)]
        for gx, gy in np.floor(P / cell).astype(int):
            grid[(int(gx), int(gy))] += 1
    loose, marks, marks_near = set(), set(), set()
    for t in ex.texts:
        toks = TOKEN_RE.findall(t["text"])
        near = any(grid.get((int(math.floor(t["cx"] / cell)) + i,
                             int(math.floor(t["cy"] / cell)) + j), 0)
                   for i in (-1, 0, 1) for j in (-1, 0, 1))
        for tk in toks:
            if DESIG_RE.match(tk):
                loose.add(tk)
            if is_mark(tk):
                marks.add(tk)
                if near:
                    marks_near.add(tk)
    return {"block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
            "discipline": b["discipline"], "n_seg": ex.inked_segments_count,
            "n_text": len(ex.texts), "n_loose": len(loose), "n_marks": len(marks),
            "n_marks_near_geom": len(marks_near)}


def worker(task):
    pdf, blocks = task
    out = []
    for b in blocks:
        try:
            out.append(one(b))
        except Exception as exc:
            out.append({"block_id": b["block_id"], "error": f"{type(exc).__name__}: {exc}"})
        F._DRAW_CACHE.clear()
    F.clear_caches()
    return out


def main():
    import multiprocessing as mp
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    nproc = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    rows = []
    exists = {}
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["pdf"] not in exists:
                exists[b["pdf"]] = os.path.exists(b["pdf"])
            if exists[b["pdf"]]:
                rows.append(b)
    rng = random.Random(24680)
    sample = rng.sample(rows, min(N, len(rows)))
    by = defaultdict(list)
    for b in sample:
        by[b["pdf"]].append(b)
    tasks = sorted(by.items(), key=lambda kv: -len(kv[1]))
    out = []
    t0 = time.time()
    with mp.get_context("fork").Pool(nproc, maxtasksperchild=4) as pool:
        for res in pool.imap_unordered(worker, tasks):
            out.extend(res)
            if len(out) % 500 < len(res):
                print(f"  {len(out)}/{len(sample)} {time.time()-t0:.0f}s", flush=True)
    ok = [r for r in out if "error" not in r]
    cls = {}
    try:
        for line in open(ART / "cns_block_classes.jsonl", encoding="utf-8"):
            c = json.loads(line)
            cls[(c["doc_id"], c["version"], c["block_id"])] = c["cls"]
    except FileNotFoundError:
        pass
    for r in ok:
        r["cls"] = cls.get((r["doc_id"], r["version"], r["block_id"]))
    n = len(ok)
    def share(f, sub=None):
        s = sub if sub is not None else ok
        return round(sum(1 for r in s if f(r)) / max(1, len(s)), 5)
    res = {
        "n_sampled": len(sample), "n_ok": n, "n_error": len(out) - n, "seed": 24680,
        "share_no_text_at_all": share(lambda r: r["n_text"] == 0),
        "share_no_loose_designation": share(lambda r: r["n_loose"] == 0),
        "share_no_mark_letter_and_digit": share(lambda r: r["n_marks"] == 0),
        "share_no_mark_near_geometry": share(lambda r: r["n_marks_near_geom"] == 0),
        "marks_per_block": {q: float(np.percentile([r["n_marks"] for r in ok], q)) for q in (10, 25, 50, 75, 90)},
        "marks_near_per_block": {q: float(np.percentile([r["n_marks_near_geom"] for r in ok], q)) for q in (10, 25, 50, 75, 90)},
        "by_class": {},
        "by_discipline": {},
    }
    for c in sorted({r.get("cls") for r in ok if r.get("cls")}):
        sub = [r for r in ok if r.get("cls") == c]
        res["by_class"][c] = {"n": len(sub),
                              "no_text": share(lambda r: r["n_text"] == 0, sub),
                              "no_mark": share(lambda r: r["n_marks"] == 0, sub),
                              "no_mark_near_geom": share(lambda r: r["n_marks_near_geom"] == 0, sub)}
    for d in sorted({r["discipline"] for r in ok}):
        sub = [r for r in ok if r["discipline"] == d]
        res["by_discipline"][d] = {"n": len(sub), "no_mark_near_geom": share(lambda r: r["n_marks_near_geom"] == 0, sub)}
    (ART / "cns_marks.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k not in ("by_class", "by_discipline")}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
