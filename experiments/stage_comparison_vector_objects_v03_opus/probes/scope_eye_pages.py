# -*- coding: utf-8 -*-
"""scope · eyes — render the two SHEETS of a hard pair with every prepared block outlined.

The block crop alone cannot tell "A is nested in B" from "the sheet was redrawn": the
answer is on the page.  One PNG per pair: page A | page B, every prepared graphic block
framed (green), the block of the pair filled red/blue.

usage: scope_eye_pages.py <pair_id> [pair_id ...]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F  # noqa
import fitz  # noqa
from PIL import Image, ImageDraw  # noqa

OUT = ART / "scope_crops"
MAX = 1400


def rows():
    out = {}
    for f in ("mine_extract.jsonl", "mine_extract2.jsonl"):
        p = ART / f
        if p.exists():
            for line in open(p, encoding="utf-8"):
                r = json.loads(line)
                out[r["pair_id"]] = r
    return out


def page_png(pdf, page_index, maxpx=MAX):
    doc = F.open_doc(str(ROOT / pdf) if not Path(pdf).is_absolute() else pdf)
    page = doc[page_index]
    r = page.rect
    s = maxpx / max(r.width, r.height)
    pix = page.get_pixmap(matrix=fitz.Matrix(s, s), alpha=False)
    im = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    return im, s, r


def blocks_of(pdf, page_number):
    rj = Path(str(ROOT / pdf)).parent / "result.json"
    bs = F.iter_prepared_blocks(str(rj))
    return [b for b in bs if b.page_number == page_number]


def draw(im, s, prect, blks, hi_id, color):
    d = ImageDraw.Draw(im, "RGBA")
    for b in blks:
        fr = F.block_frame(b.pdf_path, b.page_index, b.coords_px, b.page_px_w, b.page_px_h)
        c = fr.clip_display
        box = [c.x0 * s, c.y0 * s, c.x1 * s, c.y1 * s]
        if b.block_id == hi_id:
            d.rectangle(box, outline=color, width=5, fill=color + (40,))
        else:
            d.rectangle(box, outline=(0, 150, 0), width=2)
    return im


def main():
    R = rows()
    OUT.mkdir(exist_ok=True)
    for pid in sys.argv[1:]:
        r = R[pid]
        ims = []
        for side, col in (("a", (220, 0, 0)), ("b", (0, 60, 220))):
            im, s, prect = page_png(r[f"pdf_{side}"], r[f"page_index_{side}"])
            bl = blocks_of(r[f"pdf_{side}"], r[f"page_{side}"])
            draw(im, s, prect, bl, r[f"block_{side}"], col)
            ims.append((im, len(bl)))
        W = sum(i.width for i, _ in ims) + 16
        H = max(i.height for i, _ in ims) + 24
        cv = Image.new("RGB", (W, H), (235, 235, 235))
        x = 0
        for im, n in ims:
            cv.paste(im, (x, 24))
            x += im.width + 16
        d = ImageDraw.Draw(cv)
        d.text((4, 6), f"{pid}  A={r['ver_a']} p{r['page_a']} ({ims[0][1]} blocks) | "
                       f"B={r['ver_b']} p{r['page_b']} ({ims[1][1]} blocks)", fill=(0, 0, 0))
        cv.save(OUT / f"{pid}_pages.png")
        print(pid, "->", OUT / f"{pid}_pages.png", ims[0][1], ims[1][1], flush=True)
        F.clear_caches()


if __name__ == "__main__":
    main()
