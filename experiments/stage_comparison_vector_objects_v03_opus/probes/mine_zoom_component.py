"""mine · step 6b — zoom on one residual component of a pair, both sides + overlay.

usage: mine_zoom_component.py <input.jsonl> <pair_id> [component_index] [pad_pt]
Component boxes come from align2.top_components (px at align2 scale, block-A origin).
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import mine_align2 as AL    # noqa
import fitz, numpy as np
from PIL import Image, ImageDraw

ART = Path(__file__).resolve().parents[1] / "artifacts"
INK = 200


def zoom(r, ci=0, pad_pt=40, px_per_pt=6.0, out_dir=None, interior=True):
    a2 = r["align2"]
    comps = r.get("interior_components") if interior and r.get("interior_components") else a2["top_components"]
    c = comps[ci]
    s = a2["scale_px_per_pt"]
    x0, y0, x1, y1 = [v / s for v in c["bbox_px"]]          # block-A points, block origin
    fa = F.block_frame(r["pdf_a"], r["page_index_a"], r["coords_a"], *r["page_px_a"])
    fb = F.block_frame(r["pdf_b"], r["page_index_b"], r["coords_b"], *r["page_px_b"])
    dy, dx = a2["shift_pt"]
    def sub(fr, pdf, page_index, ox, oy):
        cl = fitz.Rect(fr.clip_display.x0 + x0 - pad_pt + ox, fr.clip_display.y0 + y0 - pad_pt + oy,
                       fr.clip_display.x0 + x1 + pad_pt + ox, fr.clip_display.y0 + y1 + pad_pt + oy)
        cl = cl & fr.page_rect
        page = F.open_doc(pdf)[page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(px_per_pt, px_per_pt), clip=cl, alpha=False)
        a = np.frombuffer(pix.samples, np.uint8).reshape(pix.height, pix.width, pix.n)
        if pix.n >= 3:
            f = a[:, :, :3].astype(np.uint32)
            g = ((f[:, :, 0] * 299 + f[:, :, 1] * 587 + f[:, :, 2] * 114) // 1000).astype(np.uint8)
        else:
            g = a[:, :, 0]
        return g
    ga = sub(fa, r["pdf_a"], r["page_index_a"], 0, 0)
    gb = sub(fb, r["pdf_b"], r["page_index_b"], -dx, -dy)
    h = max(ga.shape[0], gb.shape[0]); w = max(ga.shape[1], gb.shape[1])
    A = np.zeros((h, w), bool); B = np.zeros((h, w), bool)
    A[:ga.shape[0], :ga.shape[1]] = ga < INK
    B[:gb.shape[0], :gb.shape[1]] = gb < INK
    ov = np.full((h, w, 3), 255, np.uint8)
    ov[A & B] = (30, 30, 30); ov[A & ~B] = (220, 0, 0); ov[B & ~A] = (0, 60, 220)
    panels = []
    for arr in (A, B):
        im = np.full((h, w), 255, np.uint8); im[arr] = 0
        panels.append(Image.fromarray(im).convert("RGB"))
    panels.append(Image.fromarray(ov))
    canvas = Image.new("RGB", (w * 3 + 20, h + 20), (240, 240, 240))
    for i, im in enumerate(panels):
        canvas.paste(im, (i * (w + 10), 20))
    d = ImageDraw.Draw(canvas)
    d.text((4, 5), f"{r['pair_id']} comp#{ci} px={c['px']} a={c['a_px']} b={c['b_px']}", fill=(0, 0, 0))
    out_dir = out_dir or (ART / "mine_crops_zoom")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"{r['pair_id']}_c{ci}.png"
    canvas.save(p)
    return str(p)


if __name__ == "__main__":
    src, pid = sys.argv[1], sys.argv[2]
    ci = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    pad = float(sys.argv[4]) if len(sys.argv) > 4 else 40
    rows = {json.loads(l)["pair_id"]: json.loads(l) for l in open(ART / src, encoding="utf-8")}
    print(zoom(rows[pid], ci, pad))
