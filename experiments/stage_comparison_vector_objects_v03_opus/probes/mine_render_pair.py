"""mine · step 6 — render a candidate pair for HUMAN inspection.

One PNG per pair: [A | B | registered overlay], overlay paints ink only in A red,
only in B blue, common ink black; the biggest residual components are boxed.
Rendering and geometry go through v03_foundation only.

usage: mine_render_pair.py <input.jsonl> [pair_id ...]
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import mine_align2 as AL    # noqa
import fitz, numpy as np
from PIL import Image, ImageDraw

ART = Path(__file__).resolve().parents[1] / "artifacts"
OUT = ART / "mine_crops"
INK_T = 200
MAXPX = 1000


def render_pair(r, out_dir=OUT, maxpx=MAXPX):
    out_dir.mkdir(parents=True, exist_ok=True)
    wa, ha = r["wh_pt_a"]; wb, hb = r["wh_pt_b"]
    s = maxpx / max(wa, ha, wb, hb)
    Ai, _ = AL.ink_at_scale(r["pdf_a"], r["page_index_a"], r["coords_a"], *r["page_px_a"], s=s)
    Bi, _ = AL.ink_at_scale(r["pdf_b"], r["page_index_b"], r["coords_b"], *r["page_px_b"], s=s)
    H = max(Ai.shape[0], Bi.shape[0]); W = max(Ai.shape[1], Bi.shape[1])
    m = int(0.06 * max(H, W)) + 4
    H += 2 * m; W += 2 * m
    A = np.zeros((H, W), bool); B = np.zeros((H, W), bool)
    A[m:m + Ai.shape[0], m:m + Ai.shape[1]] = Ai
    B[m:m + Bi.shape[0], m:m + Bi.shape[1]] = Bi
    dy, dx = AL.best_shift(A, B, maxshift=max(24, m + 16))
    Bs = np.zeros_like(B)
    ys0, ys1 = max(0, dy), min(H, H + dy); xs0, xs1 = max(0, dx), min(W, W + dx)
    Bs[ys0:ys1, xs0:xs1] = B[ys0 - dy:ys1 - dy, xs0 - dx:xs1 - dx]
    Ad, Bd = AL.dil(A), AL.dil(Bs)
    oa, ob = A & ~Bd, Bs & ~Ad
    ov = np.full((H, W, 3), 255, np.uint8)
    ov[A & Bs] = (30, 30, 30)
    ov[oa] = (220, 0, 0)
    ov[ob] = (0, 60, 220)
    panels = []
    for arr in (A, Bs):
        im = np.full((H, W), 255, np.uint8); im[arr] = 0
        panels.append(Image.fromarray(im).convert("RGB"))
    panels.append(Image.fromarray(ov))
    d3 = ImageDraw.Draw(panels[2])
    for c in (r.get("align2") or {}).get("top_components", [])[:6]:
        x0, y0, x1, y1 = c["bbox_px"]
        d3.rectangle([x0 + m - 3, y0 + m - 3, x1 + m + 3, y1 + m + 3], outline=(0, 160, 0))
    gap = 10
    canvas = Image.new("RGB", (W * 3 + gap * 2, H + 22), (240, 240, 240))
    for i, im in enumerate(panels):
        canvas.paste(im, (i * (W + gap), 22))
    d = ImageDraw.Draw(canvas)
    a2 = r.get("align2") or {}
    d.text((4, 6), f"{r['pair_id']} A={r['ver_a']} p{r['page_a']}", fill=(0, 0, 0))
    d.text((W + gap + 4, 6), f"B={r['ver_b']} p{r['page_b']} (registered)", fill=(0, 0, 0))
    d.text((2 * (W + gap) + 4, 6),
           f"red=only A, blue=only B  diff={a2.get('diff_frac_block')} comps={a2.get('n_components_big')}",
           fill=(0, 0, 0))
    p = out_dir / f"{r['pair_id']}.png"
    canvas.save(p)
    return str(p)


def main(argv):
    rows = [json.loads(l) for l in open(ART / argv[0], encoding="utf-8")]
    if len(argv) > 1:
        want = set(argv[1:])
        rows = [r for r in rows if r["pair_id"] in want]
    for r in rows:
        try:
            print(render_pair(r))
        except Exception as e:
            print("ERR", r.get("pair_id"), str(e)[:120])


if __name__ == "__main__":
    main(sys.argv[1:])
