# -*- coding: utf-8 -*-
"""pd_montage — рендер пары блоков рядом (левый = П, правый = РД) для глазной проверки."""
from __future__ import annotations
import sys
from pathlib import Path
from PIL import Image, ImageDraw
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_cropfetch as C


def montage(url_a, url_b, out_png, label_a="П", label_b="РД", side=620):
    tmp = Path(out_png).parent / "_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    pa = C.render_crop(url_a, tmp / "a.png", side)
    pb = C.render_crop(url_b, tmp / "b.png", side)
    if not pa or not pb:
        return None
    ia, ib = Image.open(pa).convert("RGB"), Image.open(pb).convert("RGB")
    for im in (ia, ib):
        im.thumbnail((side, side), Image.LANCZOS)
    h = max(ia.height, ib.height) + 18
    out = Image.new("RGB", (ia.width + ib.width + 12, h), "white")
    out.paste(ia, (0, 18)); out.paste(ib, (ia.width + 12, 18))
    d = ImageDraw.Draw(out)
    d.text((4, 3), label_a, fill="black")
    d.text((ia.width + 16, 3), label_b, fill="black")
    d.line([(ia.width + 6, 0), (ia.width + 6, h)], fill="red", width=2)
    out.save(out_png)
    return out_png


def contact_sheet(urls, labels, out_png, cols=4, cell=430):
    """Сетка кропов с подписями — чтобы глазами сопоставить наборы узлов."""
    from PIL import Image, ImageDraw
    tmp = Path(out_png).parent / "_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    imgs = []
    for i, u in enumerate(urls):
        p = C.render_crop(u, tmp / f"c{i}.png", cell)
        if not p:
            imgs.append(None); continue
        im = Image.open(p).convert("RGB")
        im.thumbnail((cell, cell), Image.LANCZOS)
        imgs.append(im)
    rows = (len(imgs) + cols - 1) // cols
    W, H = cols * (cell + 6), rows * (cell + 20)
    out = Image.new("RGB", (W, H), "white")
    d = ImageDraw.Draw(out)
    for i, im in enumerate(imgs):
        r, c = divmod(i, cols)
        x, y = c * (cell + 6), r * (cell + 20)
        d.text((x + 2, y + 2), labels[i][:46], fill="red")
        if im is not None:
            out.paste(im, (x, y + 16))
        d.rectangle([x, y, x + cell + 4, y + cell + 18], outline="gray")
    out.save(out_png)
    return out_png
