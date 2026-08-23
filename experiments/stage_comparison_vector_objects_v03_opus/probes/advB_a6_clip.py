# -*- coding: utf-8 -*-
"""advB — the foundation ignores CLIP PATHS.

`v03_foundation.extract_block` reads geometry with `page.get_drawings()` (no
`extended=True`) and therefore never sees the clipping stack.  Anything a PDF hides
behind a clip path is still counted as inked geometry.  This probe measures how much
of the corpus that costs, using `get_drawings(extended=True)`, which reports the
`clip` / `group` items and lets the active clip be reconstructed.
"""
from __future__ import annotations
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fitz
import grp_common as G
import v03_foundation as F

SEED = 20260823


def page_clip_report(pdf, page_index, clip_rect=None):
    """Reconstruct the clipping stack from `level` and measure geometry hidden by it."""
    doc = F.open_doc(pdf)
    page = doc[page_index]
    ext = page.get_drawings(extended=True)
    lvl: dict[int, fitz.Rect] = {}
    n_paths = n_hidden = n_partial = 0
    a_tot = a_hidden = 0.0
    n_paths_in_block = n_hidden_in_block = 0
    for d in ext:
        L = int(d.get("level") or 0)
        for k in [k for k in lvl if k >= L]:
            del lvl[k]
        t = d.get("type")
        if t in ("clip", "group"):
            sc = d.get("scissor")
            if sc is not None:
                lvl[L] = fitz.Rect(sc)
            continue
        r = fitz.Rect(d.get("rect"))
        n_paths += 1
        # a hairline path has zero area: measure with a 0.5 pt fattening so that
        # horizontal and vertical strokes are not silently ignored
        rr = fitz.Rect(r.x0 - 0.25, r.y0 - 0.25, r.x1 + 0.25, r.y1 + 0.25)
        area = max(rr.get_area(), 1e-9)
        a_tot += area
        in_block = clip_rect is not None and F._rect_overlaps(r, clip_rect)
        if in_block:
            n_paths_in_block += 1
        if lvl:
            cur = None
            for k in sorted(lvl):
                cur = fitz.Rect(lvl[k]) if cur is None else (cur & lvl[k])
            inter = rr & cur
            ia = max(inter.get_area(), 0.0) if not inter.is_empty else 0.0
            if ia <= 1e-9:
                n_hidden += 1
                a_hidden += area
                if in_block:
                    n_hidden_in_block += 1
            elif ia < area * 0.999:
                n_partial += 1
                a_hidden += (area - ia)
    return {"n_paths": n_paths, "n_clip_items": sum(1 for d in ext if d.get("type") in ("clip", "group")),
            "n_paths_fully_hidden": n_hidden, "n_paths_partly_hidden": n_partial,
            "area_hidden_share": round(a_hidden / max(a_tot, 1e-9), 6),
            "n_paths_in_block": n_paths_in_block, "n_hidden_in_block": n_hidden_in_block}


def main():
    docs = json.load(open(G.ART / "fnd_corpus_index.json", encoding="utf-8"))["documents"]
    docs = [d for d in docs if d.get("pdf_exists")]
    rng = random.Random(SEED)
    rng.shuffle(docs)
    out = []
    for d in docs[:60]:
        rj = d["result_json"]
        full = rj if Path(rj).is_absolute() else str(G.ROOT / rj)
        try:
            blocks = list(F.iter_prepared_blocks(full))
        except Exception as e:
            continue
        if not blocks:
            continue
        pb = rng.choice(blocks)
        try:
            fr = F.block_frame(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h)
            r = page_clip_report(pb.pdf_path, pb.page_index, fitz.Rect(fr.clip_page))
        except Exception as e:
            r = {"error": repr(e)}
        r.update({"doc_id": d["doc_id"], "version": d["version"], "page_index": pb.page_index})
        out.append(r)
        print(json.dumps(r, ensure_ascii=False), flush=True)
    ok = [r for r in out if "error" not in r]
    summ = {"n_pages": len(ok),
            "pages_with_clip_items": sum(1 for r in ok if r["n_clip_items"] > 0),
            "pages_with_hidden_paths": sum(1 for r in ok if r["n_paths_fully_hidden"] > 0),
            "pages_with_partly_hidden": sum(1 for r in ok if r["n_paths_partly_hidden"] > 0),
            "max_hidden_area_share": max([r["area_hidden_share"] for r in ok], default=0.0),
            "rows": out}
    json.dump(summ, open(G.ART / "advB_clip.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in summ.items() if k != "rows"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
