"""mine · step 3b — registration at EQUAL PHYSICAL SCALE.

Step 3 rasterised each side into its own 512-grid fitted to its own bbox.  Because
the prepared bboxes of the two versions differ (median IoU(coords_norm)=0.909), that
fit rescales the two drawings differently and no translation can bring them together
(median diff_frac_block 0.073 even after FFT registration).

Here both sides are rendered at the SAME px/pt scale, padded to a common canvas and
registered by FFT cross-correlation.  Only then is the residual measured.  A pair is
reported with both numbers so the effect of the frame choice can be seen.

argv: [pair_group_index]  -> artifacts/mine_align2_parts/NNNN.jsonl
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import fitz, numpy as np
from scipy import ndimage

ART = Path(__file__).resolve().parents[1] / "artifacts"
INK_T = 200
MAXPX = 1200          # longest side of the rendered block
PAD = 0.06            # extra margin of the canvas, share of the long side


def gray(pix):
    a = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix.n >= 3:
        f = a[:, :, :3].astype(np.uint32)
        return (((f[:, :, 0] * 299 + f[:, :, 1] * 587 + f[:, :, 2] * 114) // 1000)).astype(np.uint8)
    return a[:, :, 0]


def ink_at_scale(pdf, page_index, coords_px, ppw, pph, s):
    fr = F.block_frame(pdf, page_index, coords_px, ppw, pph)
    clip = fitz.Rect(fr.clip_display)
    page = F.open_doc(pdf)[page_index]
    pix = page.get_pixmap(matrix=fitz.Matrix(s, s), clip=clip, alpha=False)
    return gray(pix) < INK_T, clip


def dil(m, k=1):
    o = m.copy()
    for _ in range(k):
        p = o.copy()
        o[1:, :] |= p[:-1, :]; o[:-1, :] |= p[1:, :]
        o[:, 1:] |= p[:, :-1]; o[:, :-1] |= p[:, 1:]
    return o


def best_shift(A, B, maxshift):
    fa = np.fft.rfft2(A.astype(np.float32)); fb = np.fft.rfft2(B.astype(np.float32))
    cc = np.fft.irfft2(fa * np.conj(fb), s=A.shape)
    m = np.full(cc.shape, -1.0, np.float32)
    s = min(maxshift, min(A.shape) // 2 - 1)
    m[:s + 1, :s + 1] = cc[:s + 1, :s + 1]; m[:s + 1, -s:] = cc[:s + 1, -s:]
    m[-s:, :s + 1] = cc[-s:, :s + 1]; m[-s:, -s:] = cc[-s:, -s:]
    iy, ix = np.unravel_index(int(np.argmax(m)), m.shape)
    dy = iy if iy <= s else iy - A.shape[0]
    dx = ix if ix <= s else ix - A.shape[1]
    return int(dy), int(dx)


def pair_signals(r):
    wa, ha = r["wh_pt_a"]; wb, hb = r["wh_pt_b"]
    s = MAXPX / max(wa, ha, wb, hb)
    Ai, ca = ink_at_scale(r["pdf_a"], r["page_index_a"], r["coords_a"], *r["page_px_a"], s=s)
    Bi, cb = ink_at_scale(r["pdf_b"], r["page_index_b"], r["coords_b"], *r["page_px_b"], s=s)
    H = max(Ai.shape[0], Bi.shape[0]); W = max(Ai.shape[1], Bi.shape[1])
    m = int(PAD * max(H, W)) + 4
    H += 2 * m; W += 2 * m
    A = np.zeros((H, W), bool); B = np.zeros((H, W), bool)
    A[m:m + Ai.shape[0], m:m + Ai.shape[1]] = Ai
    B[m:m + Bi.shape[0], m:m + Bi.shape[1]] = Bi
    dy, dx = best_shift(A, B, maxshift=max(24, m + 16))
    Bs = np.zeros_like(B)
    ys0, ys1 = max(0, dy), min(H, H + dy); xs0, xs1 = max(0, dx), min(W, W + dx)
    Bs[ys0:ys1, xs0:xs1] = B[ys0 - dy:ys1 - dy, xs0 - dx:xs1 - dx]
    Ad, Bd = dil(A), dil(Bs)
    oa, ob = A & ~Bd, Bs & ~Ad
    both = oa | ob
    lab, n = ndimage.label(dil(both, 2), structure=np.ones((3, 3), int))
    comps = []
    if n:
        sizes = ndimage.sum(both, lab, index=range(1, n + 1))
        objs = ndimage.find_objects(lab)
        order = np.argsort(sizes)[::-1]
        for k in order[:10]:
            sl = objs[k]
            sel = (lab[sl] == k + 1)
            comps.append({"px": int(sizes[k]),
                          "bbox_px": [int(sl[1].start) - m, int(sl[0].start) - m,
                                      int(sl[1].stop) - m, int(sl[0].stop) - m],
                          "a_px": int((oa[sl] & sel).sum()), "b_px": int((ob[sl] & sel).sum())})
        srt = np.sort(sizes)[::-1]
    else:
        srt = np.array([0.0])
    area_block = float(Ai.shape[0] * Ai.shape[1])
    return {
        "scale_px_per_pt": round(s, 4),
        "canvas": [H, W], "pad": m,
        "shift_px": [dy, dx], "shift_pt": [round(dy / s, 2), round(dx / s, 2)],
        "shift_frac_block": round(float(np.hypot(dy, dx)) / max(Ai.shape), 4),
        "ink_a": int(A.sum()), "ink_b": int(B.sum()),
        "iou_dil": round(int((Ad & Bd).sum()) / max(1, int((Ad | Bd).sum())), 4),
        "only_a": int(oa.sum()), "only_b": int(ob.sum()),
        "only_a_frac_ink": round(float(oa.sum()) / max(1, int(A.sum())), 4),
        "only_b_frac_ink": round(float(ob.sum()) / max(1, int(Bs.sum())), 4),
        "diff_frac_block": round(float(both.sum()) / max(1.0, area_block), 6),
        "n_components": int(n),
        "n_components_big": int((srt >= 40).sum()),
        "top1_px": int(srt[0]) if n else 0,
        "top1_frac_of_diff": round(float(srt[0] / max(1, both.sum())), 4) if n else 0.0,
        "top_components": comps,
    }


def main(argv):
    i = int(argv[0])
    rows = [json.loads(l) for l in open(ART / "mine_align.jsonl", encoding="utf-8")]
    keys = sorted({(r["doc_id"], r["ver_a"], r["ver_b"]) for r in rows})
    if i >= len(keys):
        return
    k = keys[i]
    sub = [r for r in rows if (r["doc_id"], r["ver_a"], r["ver_b"]) == k]
    (ART / "mine_align2_parts").mkdir(exist_ok=True)
    t0 = time.time()
    with open(ART / "mine_align2_parts" / f"{i:04d}.jsonl", "w", encoding="utf-8") as out:
        for r in sub:
            rec = dict(r)
            try:
                rec["align2"] = pair_signals(r)
            except Exception as e:
                rec["align2_error"] = str(e)[:160]
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[{i}] {k[0]} {len(sub)} {time.time()-t0:.1f}s", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
