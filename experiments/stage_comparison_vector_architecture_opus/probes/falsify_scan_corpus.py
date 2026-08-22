"""falsify_ probe, step 1: cheap corpus-wide hunt for falsification candidates.

For every document with >=2 versions, for every page index present in both
versions, computes:
  * text token multiset  (packaging independent, meaning bearing)
  * PDF path packaging counts (paths, l/c/re/qu operator counts)  -- packaging
  * a quantized, direction-independent SEGMENT SET in normalized page space
    (packaging independent, geometry bearing)

Then reports, per page pair:
  text_same        : text multiset identical
  geom_jaccard     : |A&B| / |A|B| over the quantized segment set

Shortlists:
  A-candidate  text identical, geom_jaccard high but < 1   -> localized graphic-only change
  B-candidate  text identical, geom_jaccard LOW            -> possible re-export / repackaging
  OUTLINE      text count collapses to ~0 while geometry explodes -> text->outlines

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_scan_corpus
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "falsify_corpus_scan.json"
QUANT = 2000  # normalized page grid
MAX_ITEMS_PER_PAGE = 400_000
TIME_BUDGET_S = float(os.environ.get("FALSIFY_SCAN_BUDGET", "1800"))


def _q(x: float, y: float, w: float, h: float) -> tuple[int, int]:
    return (int(round(x / w * QUANT)), int(round(y / h * QUANT)))


def _pt(v):
    """cdrawings returns plain tuples; get_drawings returns Point/Rect objects."""
    if hasattr(v, "x"):
        return (float(v.x), float(v.y))
    return (float(v[0]), float(v[1]))


def _rect4(v):
    if hasattr(v, "x0"):
        return (float(v.x0), float(v.y0), float(v.x1), float(v.y1))
    return (float(v[0]), float(v[1]), float(v[2]), float(v[3]))


def _quad_pts(v):
    if hasattr(v, "ul"):
        return [_pt(v.ul), _pt(v.ur), _pt(v.lr), _pt(v.ll)]
    if len(v) == 4 and not isinstance(v[0], (int, float)):
        return [_pt(p) for p in v]
    return [
        (float(v[0]), float(v[1])),
        (float(v[2]), float(v[3])),
        (float(v[4]), float(v[5])),
        (float(v[6]), float(v[7])),
    ]


def page_fingerprint(page):
    w, h = page.rect.width, page.rect.height
    if w <= 0 or h <= 0:
        return {}
    words = page.get_text("words")
    toks = sorted(x[4] for x in words)
    text_hash = hashlib.sha256(" ".join(toks).encode("utf-8")).hexdigest()[:16]
    try:
        drawings = page.get_cdrawings()
    except Exception as exc:  # pragma: no cover
        return {"error": str(exc)}
    ops = {"l": 0, "c": 0, "re": 0, "qu": 0, "other": 0}
    segs = set()
    n_items = 0
    capped = False
    for path in drawings:
        for item in path.get("items", ()):
            n_items += 1
            if n_items > MAX_ITEMS_PER_PAGE:
                capped = True
                break
            kind = item[0]
            edges = []
            try:
                if kind == "l":
                    ops["l"] += 1
                    edges = [(_pt(item[1]), _pt(item[2]))]
                elif kind == "c":
                    ops["c"] += 1
                    edges = [(_pt(item[1]), _pt(item[4]))]
                elif kind == "re":
                    ops["re"] += 1
                    x0, y0, x1, y1 = _rect4(item[1])
                    cs = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
                    edges = [(cs[i], cs[(i + 1) % 4]) for i in range(4)]
                elif kind == "qu":
                    ops["qu"] += 1
                    cs = _quad_pts(item[1])
                    edges = [(cs[i], cs[(i + 1) % 4]) for i in range(4)]
                else:
                    ops["other"] += 1
            except Exception:
                continue
            for a, b in edges:
                qa = _q(a[0], a[1], w, h)
                qb = _q(b[0], b[1], w, h)
                if qa == qb:
                    continue
                segs.add((qa[0], qa[1], qb[0], qb[1]) if qa < qb else (qb[0], qb[1], qa[0], qa[1]))
        if capped:
            break
    return {
        "w": round(w, 1),
        "h": round(h, 1),
        "n_words": len(words),
        "text_hash": text_hash,
        "paths": len(drawings),
        "items": n_items,
        "ops": ops,
        "segs": segs,
        "n_segs": len(segs),
        "capped": capped,
    }


def main() -> None:
    version_dirs = sorted(ROOT.glob("projects_v2/objects/*/disciplines/*/documents/*/versions"))
    started = time.time()
    results = []
    scanned_docs = 0
    for vdir in version_dirs:
        if time.time() - started > TIME_BUDGET_S:
            print("TIME BUDGET REACHED, stopping scan", file=sys.stderr)
            break
        vs = sorted(d for d in os.listdir(vdir) if d.startswith("v") and d[1:].isdigit())
        pdfs = [(v, vdir / v / "02_work" / "document.pdf") for v in vs]
        pdfs = [(v, p) for v, p in pdfs if p.exists()]
        if len(pdfs) < 2:
            continue
        scanned_docs += 1
        doc_rel = str(vdir.relative_to(ROOT / "projects_v2/objects")).replace("/versions", "")
        for (va, pa), (vb, pb) in zip(pdfs, pdfs[1:]):
            try:
                if pa.stat().st_size == pb.stat().st_size and (
                    hashlib.sha256(pa.read_bytes()).hexdigest()
                    == hashlib.sha256(pb.read_bytes()).hexdigest()
                ):
                    results.append({"doc": doc_rel, "pair": va + "->" + vb, "identical_pdf": True})
                    continue
                da, db = fitz.open(pa), fitz.open(pb)
            except Exception as exc:
                results.append({"doc": doc_rel, "pair": va + "->" + vb, "error": repr(exc)})
                continue
            n = min(len(da), len(db))
            pages = []
            for i in range(n):
                if time.time() - started > TIME_BUDGET_S:
                    break
                try:
                    fa = page_fingerprint(da[i])
                    fb = page_fingerprint(db[i])
                except Exception as exc:
                    continue
                if not fa or not fb or "error" in fa or "error" in fb:
                    continue
                sa, sb = fa.pop("segs"), fb.pop("segs")
                inter = len(sa & sb)
                union = len(sa | sb) or 1
                pages.append(
                    {
                        "page_index": i,
                        "size_same": (fa["w"], fa["h"]) == (fb["w"], fb["h"]),
                        "size": [[fa["w"], fa["h"]], [fb["w"], fb["h"]]],
                        "text_same": fa["text_hash"] == fb["text_hash"],
                        "n_words": [fa["n_words"], fb["n_words"]],
                        "paths": [fa["paths"], fb["paths"]],
                        "items": [fa["items"], fb["items"]],
                        "ops": [fa["ops"], fb["ops"]],
                        "n_segs": [fa["n_segs"], fb["n_segs"]],
                        "geom_jaccard": round(inter / union, 5),
                        "geom_left_cov": round(inter / (len(sa) or 1), 5),
                        "geom_right_cov": round(inter / (len(sb) or 1), 5),
                        "capped": fa["capped"] or fb["capped"],
                    }
                )
            results.append(
                {
                    "doc": doc_rel,
                    "pair": va + "->" + vb,
                    "n_pages": [len(da), len(db)],
                    "pages": pages,
                }
            )
            da.close()
            db.close()
        print("[%7.1fs] %s" % (time.time() - started, doc_rel), file=sys.stderr)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps(
            {
                "quant_grid": QUANT,
                "scanned_docs": scanned_docs,
                "elapsed_s": round(time.time() - started, 1),
                "results": results,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print("wrote", OUT)


if __name__ == "__main__":
    main()
