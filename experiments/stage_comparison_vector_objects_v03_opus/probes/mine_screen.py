"""mine · step 2 (combined) — match prepared graphic blocks across two document
versions and compute the screening signals in ONE render pass.

Page map : R3 (text-Jaccard arbiter, artifacts/mine_pagematch.json).  Measured
           accuracy of the cheap rules against it: stamp sheet key 0.912 (n=604),
           page_number 0.490 (n=2323)  -> artifacts/mine_pagematch.json.
Block map: greedy IoU on coords_norm, threshold 0.05.
Raster   : both sides rasterised into the SAME 512x512 grid with an anisotropic
           matrix over the block clip, ink = gray<200, compared with 1 px dilation.

Budgets: at most MAX_PER_PAIR block pairs per version pair (seeded sample) and
BUDGET_S seconds per version pair; whatever is skipped is recorded.

Writes artifacts/mine_screen.jsonl + artifacts/mine_screen_summary.json
"""
import json, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import fitz, numpy as np

ART = Path(__file__).resolve().parents[1] / "artifacts"
G = 512
CELL = 32
INK_T = 200
MAX_PER_PAIR = 70
BUDGET_S = 120.0
SEED = 20260823


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
    m = fitz.Matrix(g / clip.width, g / clip.height)
    a = gray(page.get_pixmap(matrix=m, clip=clip, alpha=False))
    if a.shape != (g, g):
        b = np.full((g, g), 255, np.uint8)
        h = min(g, a.shape[0]); w = min(g, a.shape[1])
        b[:h, :w] = a[:h, :w]
        a = b
    return a, fr


def dil(m):
    o = m.copy()
    o[1:, :] |= m[:-1, :]; o[:-1, :] |= m[1:, :]
    o[:, 1:] |= m[:, :-1]; o[:, :-1] |= m[:, 1:]
    return o


def cells(m, c=CELL):
    n = G // c
    return m.reshape(n, c, n, c).sum(axis=(1, 3))


def iou_box(a, b):
    ax0, ay0, ax1, ay1 = a; bx0, by0, bx1, by1 = b
    ix = max(0.0, min(ax1, bx1) - max(ax0, bx0)); iy = max(0.0, min(ay1, by1) - max(ay0, by0))
    inter = ix * iy
    ua = (ax1 - ax0) * (ay1 - ay0) + (bx1 - bx0) * (by1 - by0) - inter
    return inter / ua if ua > 0 else 0.0


def norm_of(b):
    if b.coords_norm:
        return b.coords_norm
    return (b.coords_px[0] / b.page_px_w, b.coords_px[1] / b.page_px_h,
            b.coords_px[2] / b.page_px_w, b.coords_px[3] / b.page_px_h)


def main(argv):
    """argv: [pair_index]  -- process exactly one version pair into a part file."""
    t_all = time.time()
    only_i = int(argv[0]) if argv else None
    pm = json.load(open(ART / "mine_pagematch.json", encoding="utf-8"))
    idx = json.load(open(ART / "mine_pair_index.json", encoding="utf-8"))
    P_by = {(p["doc_id"], p["ver_a"], p["ver_b"]): p for p in idx["pairs"] if not p["same_pdf"]}
    if only_i is None:
        out = open(ART / "mine_screen.jsonl", "w", encoding="utf-8")
    else:
        (ART / "mine_screen_parts").mkdir(exist_ok=True)
        out = open(ART / "mine_screen_parts" / f"{only_i:04d}.jsonl", "w", encoding="utf-8")
    S = {"version_pairs": 0, "pages": 0, "cand_pairs": 0, "written": 0,
         "skipped_budget": 0, "skipped_cap": 0, "render_errors": 0,
         "unmatched_a": 0, "unmatched_b": 0}
    rnd = random.Random(SEED)
    usable = [r for r in pm["rows"] if "r3" in r]
    if only_i is not None:
        usable = usable[only_i:only_i + 1]
    for row in usable:
        key = next((k for k in P_by if k[0] == row["doc"] and f"{k[1]}->{k[2]}" == row["ver"]), None)
        if key is None:
            continue
        Pp = P_by[key]
        t0 = time.time()
        try:
            BA = F.iter_prepared_blocks(Pp["result_a"])
            BB = F.iter_prepared_blocks(Pp["result_b"])
        except Exception:
            continue
        byA, byB = {}, {}
        for b in BA:
            byA.setdefault(b.page_number, []).append(b)
        for b in BB:
            byB.setdefault(b.page_number, []).append(b)
        r1 = {int(k): v for k, v in Pp["page_map_r1"].items()}
        todo = []
        for pa_s, pb in row["r3"].items():
            pa = int(pa_s)
            la, lb = byA.get(pa, []), byB.get(pb, [])
            if not la and not lb:
                continue
            S["pages"] += 1
            cand = []
            for i, a in enumerate(la):
                for j, b in enumerate(lb):
                    cand.append((iou_box(norm_of(a), norm_of(b)), i, j))
            cand.sort(reverse=True)
            usedA, usedB = set(), set()
            for v, i, j in cand:
                if v <= 0.05 or i in usedA or j in usedB:
                    continue
                usedA.add(i); usedB.add(j)
                todo.append((v, pa, pb, la[i], lb[j], r1.get(pa) == pb))
            S["unmatched_a"] += len(la) - len(usedA)
            S["unmatched_b"] += len(lb) - len(usedB)
        S["cand_pairs"] += len(todo)
        if len(todo) > MAX_PER_PAIR:
            S["skipped_cap"] += len(todo) - MAX_PER_PAIR
            todo = rnd.sample(todo, MAX_PER_PAIR)
        done = 0
        for v, pa, pb, a, b, sheet_ok in todo:
            if time.time() - t0 > BUDGET_S:
                S["skipped_budget"] += len(todo) - done
                break
            done += 1
            try:
                ga, fa = render_common(a.pdf_path, a.page_index, a.coords_px, a.page_px_w, a.page_px_h)
                gb, fb = render_common(b.pdf_path, b.page_index, b.coords_px, b.page_px_w, b.page_px_h)
            except Exception:
                S["render_errors"] += 1
                continue
            A = ga < INK_T; B = gb < INK_T
            Ad, Bd = dil(A), dil(B)
            oa = A & ~Bd; ob = B & ~Ad
            ca, cb = cells(oa), cells(ob)
            tot = G * G
            rec = {
                "doc_id": Pp["doc_id"], "discipline": Pp["discipline"], "obj_id": Pp["obj_id"],
                "ver_a": Pp["ver_a"], "ver_b": Pp["ver_b"],
                "pdf_a": Pp["pdf_a"], "pdf_b": Pp["pdf_b"],
                "sha_a": Pp["sha_a"], "sha_b": Pp["sha_b"], "same_pdf": Pp["sha_a"] == Pp["sha_b"],
                "page_a": a.page_number, "page_b": b.page_number,
                "page_index_a": a.page_index, "page_index_b": b.page_index,
                "sheet_confirmed": bool(sheet_ok),
                "block_a": a.block_id, "block_b": b.block_id,
                "coords_a": list(a.coords_px), "coords_b": list(b.coords_px),
                "page_px_a": [a.page_px_w, a.page_px_h], "page_px_b": [b.page_px_w, b.page_px_h],
                "norm_a": [round(x, 5) for x in norm_of(a)], "norm_b": [round(x, 5) for x in norm_of(b)],
                "rot_a": a.rotation, "rot_b": b.rotation,
                "cat_a": a.category_code, "cat_b": b.category_code,
                "shape_a": a.shape_type, "shape_b": b.shape_type,
                "ocr_len_a": len(a.ocr_text), "ocr_len_b": len(b.ocr_text),
                "iou_norm": round(v, 4),
                "clip_pt_a": [round(x, 2) for x in (fa.clip_display.x0, fa.clip_display.y0,
                                                    fa.clip_display.x1, fa.clip_display.y1)],
                "clip_pt_b": [round(x, 2) for x in (fb.clip_display.x0, fb.clip_display.y0,
                                                    fb.clip_display.x1, fb.clip_display.y1)],
                "wh_pt_a": [round(fa.clip_display.width, 2), round(fa.clip_display.height, 2)],
                "wh_pt_b": [round(fb.clip_display.width, 2), round(fb.clip_display.height, 2)],
                "ink_a": int(A.sum()), "ink_b": int(B.sum()),
                "ink_density_a": round(float(A.mean()), 5), "ink_density_b": round(float(B.mean()), 5),
                "iou_raw": round(int((A & B).sum()) / max(1, int((A | B).sum())), 4),
                "iou_dil": round(int((Ad & Bd).sum()) / max(1, int((Ad | Bd).sum())), 4),
                "only_a_px": int(oa.sum()), "only_b_px": int(ob.sum()),
                "only_a_frac_block": round(float(oa.sum()) / tot, 6),
                "only_b_frac_block": round(float(ob.sum()) / tot, 6),
                "only_a_frac_ink": round(float(oa.sum()) / max(1, int(A.sum())), 4),
                "only_b_frac_ink": round(float(ob.sum()) / max(1, int(B.sum())), 4),
                "diff_cells_a": int((ca > 8).sum()), "diff_cells_b": int((cb > 8).sum()),
                "max_cell_a": int(ca.max()), "max_cell_b": int(cb.max()),
                "identical_px": bool((ga == gb).all()),
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            S["written"] += 1
        out.flush()
        F.clear_caches()
        S["version_pairs"] += 1
        print(f"[{S['version_pairs']}] {Pp['discipline']}/{Pp['doc_id']} {Pp['ver_a']}->{Pp['ver_b']} "
              f"cand={len(todo)} done={done} tot={S['written']} {time.time()-t0:.1f}s", flush=True)
    out.close()
    S["elapsed_s"] = round(time.time() - t_all, 1)
    if only_i is not None:
        (ART / "mine_screen_parts" / f"{only_i:04d}.summary.json").write_text(
            json.dumps(S, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(S, ensure_ascii=False))
        return
    (ART / "mine_screen_summary.json").write_text(json.dumps(
        {"schema_version": "mine_screen/1", "research_only": True,
         "params": {"grid": G, "ink_threshold": INK_T, "max_per_version_pair": MAX_PER_PAIR,
                    "budget_s_per_version_pair": BUDGET_S, "seed": SEED,
                    "block_match": "greedy IoU(coords_norm) > 0.05",
                    "page_match": "R3 text-Jaccard arbiter"},
         "summary": S}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(S, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1:])
