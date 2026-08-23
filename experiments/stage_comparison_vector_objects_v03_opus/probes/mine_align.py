"""mine · step 3 — register the two sides and localise the difference.

The coarse screen compares two blocks in one anisotropic 512-grid.  Where the two
prepared bboxes differ (median IoU(coords_norm) = 0.909) the whole content shifts,
so raw overlap under-reports agreement: on pairs whose bbox is the same to 2 %
(n=68) median iou_dil is 0.829, over the whole set only 0.239.

Here the two ink maps are registered by FFT cross-correlation (integer shift,
searched to +-64 px of the 512-grid), and only then differenced.  Residual ink is
grouped into connected components -> candidate changed OBJECTS.

Runs one version pair per process (argv: pair index) -> artifacts/mine_align_parts/.
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import fitz, numpy as np
from scipy import ndimage

ART = Path(__file__).resolve().parents[1] / "artifacts"
G = 512
INK_T = 200
MAXSHIFT = 64


def gray(pix):
    a = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix.n >= 3:
        f = a[:, :, :3].astype(np.uint32)
        return (((f[:, :, 0] * 299 + f[:, :, 1] * 587 + f[:, :, 2] * 114) // 1000)).astype(np.uint8)
    return a[:, :, 0]


def render_common(pdf, page_index, coords_px, ppw, pph, g=G):
    fr = F.block_frame(pdf, page_index, coords_px, ppw, pph)
    clip = fitz.Rect(fr.clip_display)
    if clip.width < 0.5 or clip.height < 0.5:
        raise ValueError("degenerate clip")
    page = F.open_doc(pdf)[page_index]
    a = gray(page.get_pixmap(matrix=fitz.Matrix(g / clip.width, g / clip.height), clip=clip, alpha=False))
    if a.shape != (g, g):
        b = np.full((g, g), 255, np.uint8)
        h = min(g, a.shape[0]); w = min(g, a.shape[1])
        b[:h, :w] = a[:h, :w]
        a = b
    return a, fr


def dil(m, k=1):
    o = m.copy()
    for _ in range(k):
        p = o.copy()
        o[1:, :] |= p[:-1, :]; o[:-1, :] |= p[1:, :]
        o[:, 1:] |= p[:, :-1]; o[:, :-1] |= p[:, 1:]
    return o


def best_shift(A, B, maxshift=MAXSHIFT):
    """integer (dy, dx) that maximises overlap of B shifted onto A."""
    fa = np.fft.rfft2(A.astype(np.float32))
    fb = np.fft.rfft2(B.astype(np.float32))
    cc = np.fft.irfft2(fa * np.conj(fb), s=A.shape)
    m = np.full(cc.shape, -1.0, np.float32)
    s = maxshift
    m[:s + 1, :s + 1] = cc[:s + 1, :s + 1]
    m[:s + 1, -s:] = cc[:s + 1, -s:]
    m[-s:, :s + 1] = cc[-s:, :s + 1]
    m[-s:, -s:] = cc[-s:, -s:]
    iy, ix = np.unravel_index(int(np.argmax(m)), m.shape)
    dy = iy if iy <= s else iy - A.shape[0]
    dx = ix if ix <= s else ix - A.shape[1]
    return int(dy), int(dx)


def shift_img(B, dy, dx):
    out = np.zeros_like(B)
    h, w = B.shape
    ys0, ys1 = max(0, dy), min(h, h + dy)
    xs0, xs1 = max(0, dx), min(w, w + dx)
    out[ys0:ys1, xs0:xs1] = B[ys0 - dy:ys1 - dy, xs0 - dx:xs1 - dx]
    return out


def analyse(A, B):
    dy, dx = best_shift(A, B)
    Bs = shift_img(B, dy, dx)
    # valid region (where the shift did not pull in empty margin)
    valid = np.ones_like(A)
    if dy > 0: valid[:dy, :] = 0
    elif dy < 0: valid[dy:, :] = 0
    if dx > 0: valid[:, :dx] = 0
    elif dx < 0: valid[:, dx:] = 0
    Av, Bv = A & valid, Bs & valid
    Ad, Bd = dil(Av), dil(Bv)
    oa, ob = Av & ~Bd, Bv & ~Ad
    both = oa | ob
    lab, n = ndimage.label(dil(both, 2), structure=np.ones((3, 3), int))
    comps = []
    if n:
        objs = ndimage.find_objects(lab)
        sizes = ndimage.sum(both, lab, index=range(1, n + 1))
        order = np.argsort(sizes)[::-1]
        for k in order[:8]:
            sl = objs[k]
            comps.append({
                "px": int(sizes[k]),
                "bbox_grid": [int(sl[1].start), int(sl[0].start), int(sl[1].stop), int(sl[0].stop)],
                "a_px": int((oa[sl] & (lab[sl] == k + 1)).sum()),
                "b_px": int((ob[sl] & (lab[sl] == k + 1)).sum()),
            })
        sizes_sorted = np.sort(sizes)[::-1]
    else:
        sizes_sorted = np.array([])
    tot = int(valid.sum())
    return {
        "shift_dy": dy, "shift_dx": dx,
        "shift_frac": round(float(np.hypot(dy, dx)) / G, 4),
        "valid_frac": round(tot / (G * G), 4),
        "ink_a_v": int(Av.sum()), "ink_b_v": int(Bv.sum()),
        "iou_dil_al": round(int((Ad & Bd).sum()) / max(1, int((Ad | Bd).sum())), 4),
        "only_a_al": int(oa.sum()), "only_b_al": int(ob.sum()),
        "only_a_frac_ink_al": round(float(oa.sum()) / max(1, int(Av.sum())), 4),
        "only_b_frac_ink_al": round(float(ob.sum()) / max(1, int(Bv.sum())), 4),
        "diff_frac_block_al": round(float(both.sum()) / max(1, tot), 6),
        "n_components": int(n),
        "n_components_big": int((sizes_sorted >= 40).sum()) if n else 0,
        "top_components": comps,
        "top1_frac_of_diff": round(float(sizes_sorted[0] / max(1, both.sum())), 4) if n else 0.0,
    }


def main(argv):
    i = int(argv[0])
    rows = [json.loads(l) for l in open(ART / "mine_screen.jsonl", encoding="utf-8")]
    keys = sorted({(r["doc_id"], r["ver_a"], r["ver_b"]) for r in rows})
    if i >= len(keys):
        return
    k = keys[i]
    sub = [r for r in rows if (r["doc_id"], r["ver_a"], r["ver_b"]) == k]
    (ART / "mine_align_parts").mkdir(exist_ok=True)
    t0 = time.time()
    with open(ART / "mine_align_parts" / f"{i:04d}.jsonl", "w", encoding="utf-8") as out:
        for r in sub:
            try:
                ga, _ = render_common(r["pdf_a"], r["page_index_a"], r["coords_a"], *r["page_px_a"])
                gb, _ = render_common(r["pdf_b"], r["page_index_b"], r["coords_b"], *r["page_px_b"])
            except Exception as e:
                continue
            A = ga < INK_T; B = gb < INK_T
            rec = dict(r)
            rec["align"] = analyse(A, B)
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[{i}] {k[0]} {len(sub)} {time.time()-t0:.1f}s", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
