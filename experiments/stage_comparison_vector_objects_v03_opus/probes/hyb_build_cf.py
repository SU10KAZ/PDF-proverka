# -*- coding: utf-8 -*-
"""`hyb` — materialise counterfactual cases as REAL PDF pairs of a whole prepared block.

Every case ends up as two one-page PDFs plus the coords of the prepared block, so all
three arms consume exactly the same input:

    arm A / arm C : F.extract_block(left_pdf ...) vs F.extract_block(right_pdf ...)
    arm B         : F.render_block(left_pdf ...)  vs F.render_block(right_pdf ...)

Three materialisation modes:

  patch  — a white rectangle over the changed area on BOTH sides and a redraw of the
           geometry/text that touches it (left redraws the original, right the
           counterfactual).  Any artefact of the redraw appears on both sides, so it
           cannot leak the answer.  This is the vis_cf_build recipe, extended to several
           patches at once and to text (which vis rejected, because vis had no need for
           class D).
  page   — the counterfactual IS a page rewrite (A7 re-export, D9 text->curves,
           B5 /Rotate): both sides are honest PDFs produced by a real tool.
  frame  — nothing is rewritten at all, only the block's own coords_px move (B3 crop
           jitter): left and right are the same file.

Usage:  python3 probes/hyb_build_cf.py [n_carriers]
"""
from __future__ import annotations

import json
import random
import sys
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import fitz                        # noqa: E402
import hyb_common as H             # noqa: E402
import v03_foundation as F         # noqa: E402
import v03_objects as O            # noqa: E402
import v03_counterfactual as CF    # noqa: E402
import grp_common as G             # noqa: E402

WHITE_PAD = 1.6
SEED = 20260823


# ---------------------------------------------------------------- changed geometry

def _key(s):
    a = tuple(round(v, 2) for v in s["p0"])
    b = tuple(round(v, 2) for v in s["p1"])
    return (a, b) if a <= b else (b, a)


def changed_segments(ex, ex2):
    ka, kb = {}, {}
    for s in ex.segments:
        ka[_key(s)] = ka.get(_key(s), 0) + 1
    for s in ex2.segments:
        kb[_key(s)] = kb.get(_key(s), 0) + 1
    out = []
    for k, n in ka.items():
        if kb.get(k, 0) != n:
            out.append(k)
    for k, n in kb.items():
        if ka.get(k, 0) != n:
            out.append(k)
    return out


def cluster_rects(keys, r=14.0, cap=6):
    """Group changed segment endpoints into a few rectangles (single-link, grid)."""
    if not keys:
        return []
    boxes = [[min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])]
             for a, b in keys]
    parent = list(range(len(boxes)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        a, b = find(i), find(j)
        if a != b:
            parent[b] = a

    grid = {}
    for i, b in enumerate(boxes):
        for gx in range(int(b[0] // r), int(b[2] // r) + 1):
            for gy in range(int(b[1] // r), int(b[3] // r) + 1):
                grid.setdefault((gx, gy), []).append(i)
    for cell, ix in grid.items():
        for j in ix[1:]:
            union(ix[0], j)
        gx, gy = cell
        for d in ((1, 0), (0, 1), (1, 1), (1, -1)):
            nb = grid.get((gx + d[0], gy + d[1]))
            if nb:
                union(ix[0], nb[0])
    groups = {}
    for i in range(len(boxes)):
        groups.setdefault(find(i), []).append(i)
    rects = []
    for ix in groups.values():
        xs0 = min(boxes[i][0] for i in ix); ys0 = min(boxes[i][1] for i in ix)
        xs1 = max(boxes[i][2] for i in ix); ys1 = max(boxes[i][3] for i in ix)
        rects.append([xs0, ys0, xs1, ys1])
    rects.sort(key=lambda b: -(b[2] - b[0]) * (b[3] - b[1]))
    return rects[:cap]


# ---------------------------------------------------------------- patch materialiser

def _white(c):
    if c is None:
        return True
    try:
        return all(float(v) >= 0.985 for v in c)
    except Exception:
        return True


def patch_ok(ex, ex2, patches, allow_text=False):
    """A white-out cannot restore a raster or a filled path; text only if we redraw it."""
    for e in (ex, ex2):
        for im in e.images:
            for p in patches:
                if H.isect(im.get("bbox") or [0, 0, 0, 0], p):
                    return "raster_in_patch"
        for s in e.segments:
            if _white(s.get("fill")):
                continue
            for p in patches:
                if H.isect(H.seg_bbox(s), p):
                    return "fill_in_patch"
        if not allow_text:
            for t in e.texts:
                for p in patches:
                    if H.isect(t["bbox"], p):
                        return "text_in_patch"
    return None


def patched_pdf(pb, segments, texts, patches, out_pdf: Path, draw_text=True):
    src = F.open_doc(pb.pdf_path)
    nd = fitz.open()
    nd.insert_pdf(src, from_page=pb.page_index, to_page=pb.page_index)
    page = nd[0]
    if page.rotation % 360 != 0:
        raise RuntimeError("carrier page is rotated; patch carriers are /Rotate 0 only")
    wrs = []
    for p in patches:
        wr = H.rect(p) + fitz.Rect(-WHITE_PAD, -WHITE_PAD, WHITE_PAD, WHITE_PAD)
        page.draw_rect(wr, color=None, fill=(1, 1, 1), width=0)
        wrs.append([wr.x0, wr.y0, wr.x1, wr.y1])
    groups, n = {}, 0
    for s in segments:
        if not any(H.isect(H.seg_bbox(s), w) for w in wrs):
            continue
        col = tuple(s.get("color") or (0.0, 0.0, 0.0))
        wid = round(max(float(s.get("w") or 0.0), 0.1), 3)
        groups.setdefault((col, wid), []).append(s)
    for (col, wid), items in sorted(groups.items(), key=lambda kv: repr(kv[0])):
        sh = page.new_shape()
        for s in items:
            sh.draw_line(fitz.Point(*s["p0"]), fitz.Point(*s["p1"]))
            n += 1
        sh.finish(color=col, width=wid, closePath=False)
        sh.commit()
    nt = 0
    if draw_text and texts:
        font = fitz.Font(fontfile=CF.DEJAVU)
        tw = fitz.TextWriter(page.rect)
        for t in texts:
            if not any(H.isect(t["bbox"], w) for w in wrs):
                continue
            bb = t["bbox"]
            size = float(t.get("_draw_size") or 0.0) or max(float(t.get("size") or 0.0), 1.0)
            try:
                tw.append(fitz.Point(bb[0], bb[3] - 0.18 * size), t["text"], font=font,
                          fontsize=size)
                nt += 1
            except Exception:
                pass
        if nt:
            tw.write_text(page, color=(0, 0, 0))
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    nd.save(str(out_pdf), garbage=3, deflate=True)
    nd.close()
    return n, nt


# ---------------------------------------------------------------- case construction

def side(pdf, page_index, coords_px, page_px):
    return {"pdf": str(pdf), "page_index": int(page_index),
            "coords_px": [float(v) for v in coords_px],
            "page_px": [float(page_px[0]), float(page_px[1])]}


def fidelity(pb, left_pdf, window):
    """left (patched original) against the untouched page over the changed window."""
    import numpy as np
    tmp = H.CF_DIR / "_fid"
    tmp.mkdir(parents=True, exist_ok=True)
    a = side(left_pdf, 0, pb.coords_px, (pb.page_px_w, pb.page_px_h))
    b = side(pb.pdf_path, pb.page_index, pb.coords_px, (pb.page_px_w, pb.page_px_h))
    import vis_common as V
    V.render_region({"pdf": a["pdf"], "page_index": 0, "page_px": a["page_px"]},
                    window, tmp / "a.png", 500)
    V.render_region({"pdf": b["pdf"], "page_index": pb.page_index, "page_px": b["page_px"]},
                    window, tmp / "b.png", 500)
    return round(H.structural_diff(tmp / "a.png", tmp / "b.png"), 5)


def _fit_text_sizes(ex, ex2):
    """Give both sides ONE font size per text slot, fitted to the ORIGINAL box.

    The redraw uses DejaVu (the carrier's CAD font is not embeddable here); without a
    fit the redrawn string is wider or narrower than the ink it replaces.  The size is
    computed from the ORIGINAL string and copied to the counterfactual side, so the two
    sides differ only in the characters, never in the type size.
    """
    font = fitz.Font(fontfile=CF.DEJAVU)
    sizes = {}
    for i, t in enumerate(ex.texts):
        base = max(float(t.get("size") or 0.0), 1.0)
        w = (t["bbox"][2] - t["bbox"][0])
        try:
            need = font.text_length(t["text"], base)
        except Exception:
            need = 0.0
        s = base * (w / need) if need > 0.5 and w > 0.5 else base
        sizes[i] = max(1.0, min(base * 1.6, s))
        t["_draw_size"] = sizes[i]
    for i, t in enumerate(ex2.texts):
        t["_draw_size"] = sizes.get(i, max(float(t.get("size") or 0.0), 1.0))


def build_patch_case(pb, ex, L, cf_id, cid, *, params=None, obj_idx=None,
                     allow_text=False, max_patch=520.0):
    params = params or {}
    ex2, man = CF.apply(ex, L, cf_id, **params)
    if allow_text:
        _fit_text_sizes(ex, ex2)
    if obj_idx is not None:
        patches = [list(L.objects[obj_idx]["bbox"])]
    else:
        keys = changed_segments(ex, ex2)
        patches = cluster_rects(keys)
        if allow_text:
            for t in man.get("touched_texts") or []:
                patches.append(list(t["bbox_pt"]))
        if not patches:
            return {"cand_id": cid, "reject": "no_change_materialised"}
    patches = [list(H.rect(p)) for p in patches]
    for p in patches:
        if max(p[2] - p[0], p[3] - p[1]) > max_patch:
            return {"cand_id": cid, "reject": f"patch_too_big_{round(max(p[2]-p[0], p[3]-p[1]))}"}
    bad = patch_ok(ex, ex2, patches, allow_text=allow_text)
    if bad:
        return {"cand_id": cid, "reject": bad}
    lp, rp = H.CF_DIR / f"{cid}_L.pdf", H.CF_DIR / f"{cid}_R.pdf"
    nl = patched_pdf(pb, ex.segments, ex.texts, patches, lp, draw_text=allow_text)
    nr = patched_pdf(pb, ex2.segments, ex2.texts, patches, rp, draw_text=allow_text)
    xs0 = min(p[0] for p in patches); ys0 = min(p[1] for p in patches)
    xs1 = max(p[2] for p in patches); ys1 = max(p[3] for p in patches)
    fid = fidelity(pb, lp, [xs0 - 6, ys0 - 6, xs1 + 6, ys1 + 6])
    return {"cand_id": cid, "mode": "patch", "cf_id": cf_id, "cf_class": man["cf_class"],
            "expected_verdict": man["expected_verdict"],
            "left": side(lp, 0, pb.coords_px, (pb.page_px_w, pb.page_px_h)),
            "right": side(rp, 0, pb.coords_px, (pb.page_px_w, pb.page_px_h)),
            "patches_pt": [[round(v, 2) for v in p] for p in patches],
            "change_bbox_pt": [round(v, 2) for v in (xs0, ys0, xs1, ys1)],
            "redraw_fidelity_diff": fid,
            "n_redrawn": [nl, nr],
            "manifest": {k: man.get(k) for k in
                         ("cf_class", "cf_id", "seed", "params", "touched_objects",
                          "touched_texts", "changed_primitives", "delta", "expected_ledger",
                          "invariants")}}


def build_page_case(pb, ex, L, cf_id, cid, params=None):
    ex2, man = CF.apply(ex, L, cf_id, **(params or {}))
    info = man["page_rewrite"]
    lp = H.CF_DIR / f"{cid}_L.pdf"
    rp = H.CF_DIR / f"{cid}_R.pdf"
    H.CF_DIR.mkdir(parents=True, exist_ok=True)
    Path(lp).write_bytes(Path(info["src_pdf"]).read_bytes())
    Path(rp).write_bytes(Path(info["out_pdf"]).read_bytes())
    right_px = man["params"].get("coords_px_after") or pb.coords_px
    right_page = man["params"].get("page_px_after") or [pb.page_px_w, pb.page_px_h]
    return {"cand_id": cid, "mode": "page", "cf_id": man["cf_id"], "cf_class": man["cf_class"],
            "expected_verdict": man["expected_verdict"],
            "left": side(lp, 0, pb.coords_px, (pb.page_px_w, pb.page_px_h)),
            "right": side(rp, 0, right_px, right_page),
            "patches_pt": [], "change_bbox_pt": None, "redraw_fidelity_diff": None,
            "manifest": {k: man.get(k) for k in
                         ("cf_class", "cf_id", "seed", "params", "touched_objects",
                          "changed_primitives", "delta", "expected_ledger", "invariants")}}


def build_frame_case(pb, ex, cid, frac):
    ex2, man = CF.apply(ex, None, "B3_crop_jitter", frac=frac)
    new_px = man["params"]["coords_px_after"] if "coords_px_after" in man.get("params", {}) else None
    if new_px is None:
        new_px = ex2.provenance["coords_px"]
    one = H.CF_DIR / f"{cid}_L.pdf"
    H.CF_DIR.mkdir(parents=True, exist_ok=True)
    CF._single_page_pdf(pb.pdf_path, pb.page_index, one)
    return {"cand_id": cid, "mode": "frame", "cf_id": f"B3_crop_jitter_{frac}",
            "cf_class": "B", "expected_verdict": man["expected_verdict"],
            "left": side(one, 0, pb.coords_px, (pb.page_px_w, pb.page_px_h)),
            "right": side(one, 0, new_px, (pb.page_px_w, pb.page_px_h)),
            "patches_pt": [], "change_bbox_pt": None, "redraw_fidelity_diff": None,
            "manifest": {k: man.get(k) for k in
                         ("cf_class", "cf_id", "seed", "params", "changed_primitives",
                          "delta", "expected_ledger", "invariants")}}


def pick_object(L, *, kinds=("symbol", "linear"), dmin=10.0, dmax=110.0,
                nmin=6, nmax=500, k=6, rng=None):
    cand = [i for i, o in enumerate(L.objects)
            if o["cls"] in kinds and dmin <= o["diag"] <= dmax and nmin <= o["n_seg"] <= nmax]
    (rng or random.Random(7)).shuffle(cand)
    return cand[:k]


PLAN = [
    # (cf_id, want, kwargs)   -- quotas are PER counterfactual id, not per class
    ("C1_remove_object", 2, {"params": {"bucket": "small"}}),
    ("C2_add_object", 2, {"params": {"bucket": "small"}}),
    ("C3_move_object", 2, {"params": {"bucket": "small", "frac": 0.02}}),
    ("C6_reshape_object", 2, {"params": {"bucket": "small"}}),
    ("C9_add_branch", 2, {}),
    ("D7_dim_geometry", 2, {"allow_text": True}),
    ("D1_text_edit", 2, {"allow_text": True}),
    ("D2_text_move", 1, {"allow_text": True}),
    ("D3_label_rename", 2, {"allow_text": True}),
    ("D4_table_values", 1, {"allow_text": True}),
    ("D5_table_row_text", 1, {"allow_text": True}),
    ("D6_dim_value_only", 1, {"allow_text": True}),
    ("A6_round_0.5", 2, {"obj": True}),
    ("A1_path_split", 1, {"obj": True}),
    ("A2_path_merge", 1, {"obj": True}),
    ("D9_text_to_curves", 1, {"page": True}),
    ("A7_reexport_cairo", 1, {"page": True}),
    ("B3_crop_jitter", 1, {"frame": True, "frac": 0.02}),
]

GROUP = {"C1": "C", "C2": "C", "C3": "C", "C6": "C", "C9": "C"}


def main():
    n_lim = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    per_carrier_cap = 3
    rows = json.load(open(H.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    rows = [r for r in rows if r["bucket"] in ("sparse", "medium", "dense")
            and r["cls"] in ("drawing", "vector_raster_mix", "legend_notes", "table", "stamp")
            and 150 <= r["n_seg"] <= 9000]
    random.Random(SEED).shuffle(rows)
    want = {cf: n for cf, n, _ in PLAN}
    got = {cf: 0 for cf in want}
    cases, attempts = [], []
    seen_disc = {}
    for r in rows[:n_lim]:
        if all(got[k] >= want[k] for k in want):
            break
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None or pb.rotation % 360 != 0:
            continue
        try:
            ex = G.extract(pb)
            if len(ex.segments) < 150:
                continue
            L = O.build_objects(ex)
        except Exception as e:                                   # noqa: BLE001
            attempts.append({"block": r["block_id"], "reject": f"carrier:{e!r}"})
            continue
        rng = random.Random(hash(pb.block_id) & 0xFFFF)
        per_carrier = 0
        for cf_id, _n, kw in PLAN:
            if got[cf_id] >= want[cf_id] or per_carrier >= per_carrier_cap:
                continue
            cid = f"{pb.discipline}_{pb.block_id[:8]}_{cf_id}"
            try:
                if kw.get("page"):
                    res = build_page_case(pb, ex, L, cf_id, cid)
                elif kw.get("frame"):
                    res = build_frame_case(pb, ex, cid, kw["frac"])
                elif kw.get("obj"):
                    ok = None
                    for oi in pick_object(L, rng=rng):
                        res = build_patch_case(pb, ex, L, cf_id, f"{cid}_{oi}", obj_idx=oi)
                        attempts.append({"cid": res["cand_id"], "reject": res.get("reject")})
                        if not res.get("reject"):
                            ok = res
                            break
                    res = ok or {"cand_id": cid, "reject": "no_object_variant"}
                else:
                    res = build_patch_case(pb, ex, L, cf_id, cid,
                                           params=kw.get("params"),
                                           allow_text=kw.get("allow_text", False))
                    attempts.append({"cid": cid, "reject": res.get("reject")})
            except CF.CFNotApplicable as e:
                attempts.append({"cid": cid, "reject": f"not_applicable:{e}"})
                continue
            except Exception as e:                               # noqa: BLE001
                attempts.append({"cid": cid, "reject": f"error:{e!r}"})
                continue
            if res.get("reject"):
                continue
            res["carrier"] = r
            res["group"] = res["cf_class"]
            cases.append(res)
            got[cf_id] += 1
            per_carrier += 1
            seen_disc[pb.discipline] = seen_disc.get(pb.discipline, 0) + 1
            print("OK", res["cand_id"], res.get("redraw_fidelity_diff"), flush=True)
    H.dump({"seed": SEED, "want": want, "got": got, "n_cases": len(cases),
            "disciplines": seen_disc, "n_attempts": len(attempts),
            "attempts": attempts, "cases": cases}, "hyb_cf_cases.json")
    print("built:", got, "cases:", len(cases), "disciplines:", seen_disc)


if __name__ == "__main__":
    main()
