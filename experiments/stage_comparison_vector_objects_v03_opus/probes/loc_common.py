# -*- coding: utf-8 -*-
"""`loc` probe — OBJECT-LEVEL COMPARATOR (ledger) + the two baselines it must beat.

Design decisions, each forced by a measured result of an earlier probe of this track:

* everything in PDF POINTS (v0.2: tolerances in block fractions are meaningless);
* the characteristic scale S is SHARED between the two sides (grp G2b: own-S per side
  produced 1 635 objects against 200 on identical geometry);
* the ledger is built from UNMATCHED INK, not from matching object to object
  (grp G2-3: object boundaries survive <0.51 of the ink above 15 000 segments per
  block, so an object-to-object ledger would report its own churn on ~28.6 % of the
  corpus).  Objects are used to NAME the change, not to find it;
* every record carries an explicit `at_boundary` flag (mine M5: 36.5 % of real pairs
  with a residual have all of it on the crop border).

Three comparators are measured side by side so the claim "object level sees what a
scalar cannot" is a number and not an opinion:

    scalar_verdict()  — one global similarity score (the v0.1 baseline)
    counts_verdict()  — primitive/object counters (the "did we just build a counter?" test)
    ledger()          — the object-level ledger
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
sys.path.insert(0, str(HERE))

import v03_objects as O          # noqa: E402
import grp_match as M            # noqa: E402

DEF = {
    "tol_pt": 0.8,          # ink correspondence tolerance, PDF points
    "cluster_r_S": 1.5,     # change-region clustering radius, units of S
    "cluster_r_min": 3.0,   # ... but never below this many points
    "min_change_len_pt": 0.0,   # ledger threshold (the ROC knob), points of ink
    "border_pad_frac": 0.02,
    "border_pad_min": 2.0,
    "move_match_thr": 0.70,  # share of lost ink explained by translated new ink
    "pure_thr": 0.05,       # region is pure REMOVED/ADDED if the other side <5 %
    "obj_share_thr": 0.20,  # object is named in a record if >=20 % of its ink is in it
    "keep_seg_ix": False,   # `ldg`: keep the segment indices of each record (evidence)
}


# ---------------------------------------------------------------- object layers

def shared_scale(exA, exB, params=None):
    p = dict(O.DEFAULTS)
    p.update(params or {})
    Sa, srca, _ = O.characteristic_scale(exA.segments, exA.texts, p)
    Sb, srcb, _ = O.characteristic_scale(exB.segments, exB.texts, p)
    return max(Sa, Sb), (Sa, srca), (Sb, srcb)


def layers(exA, exB, *, shared=True, params=None):
    prm = dict(params or {})
    S, a, b = shared_scale(exA, exB, prm)
    if shared:
        prm["S_override"] = S
    LA = O.build_objects(exA, **prm)
    LB = O.build_objects(exB, **prm)
    return LA, LB, {"S_shared": S, "S_a": a[0], "S_b": b[0],
                    "src_a": a[1], "src_b": b[1]}


# ---------------------------------------------------------------- ink correspondence

def _seg_ang(s):
    return math.degrees(math.atan2(s["p1"][1] - s["p0"][1],
                                   s["p1"][0] - s["p0"][0])) % 180.0


def unmatched_mask(segs_q, segs_ref, off, tol):
    """For every segment of `segs_q`: is its ink present in `segs_ref` (shifted by off)?

    Returns (flags, matched_len, total_len).  Stage 1 = both endpoints (exact
    decomposition), stage 2 = three sample points against the nearest near-parallel
    reference segment (different decomposition).  A segment is 'matched' when all
    three sample points find a partner; partial hits count proportionally.
    """
    if not segs_ref:
        return [0.0] * len(segs_q), 0.0, sum(s["len"] for s in segs_q)
    shifted = [{**s, "p0": (s["p0"][0] + off[0], s["p0"][1] + off[1]),
                "p1": (s["p1"][0] + off[0], s["p1"][1] + off[1])} for s in segs_ref]
    eidx = M.build_endpoint_index(shifted)
    pidx = M.build_index(segs_ref, off)
    flags = []
    mlen = tlen = 0.0
    for s in segs_q:
        L = s["len"]
        tlen += L
        if M.query_endpoints(eidx, s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], tol) is not None:
            flags.append(1.0)
            mlen += L
            continue
        ang = _seg_ang(s)
        hit = 0
        for k in range(3):
            t = (k + 0.5) / 3
            x = s["p0"][0] + t * (s["p1"][0] - s["p0"][0])
            y = s["p0"][1] + t * (s["p1"][1] - s["p0"][1])
            if M.query(pidx, x, y, ang, tol) is not None:
                hit += 1
        f = hit / 3.0
        flags.append(f)
        mlen += L * f
    return flags, mlen, tlen


# ---------------------------------------------------------------- change regions

def _cluster(items, r):
    """Union-find over a uniform grid; `items` = (x0,y0,x1,y1,payload)."""
    n = len(items)
    parent = list(range(n))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    cell = max(r, 1e-6)
    grid: dict[tuple[int, int], list[int]] = {}
    for i, (x0, y0, x1, y1, _p) in enumerate(items):
        L = math.hypot(x1 - x0, y1 - y0)
        k = min(int(L / cell) + 1, 4000)
        seen = set()
        for j in range(k + 1):
            t = j / k
            gx = int(math.floor((x0 + t * (x1 - x0)) / cell))
            gy = int(math.floor((y0 + t * (y1 - y0)) / cell))
            if (gx, gy) in seen:
                continue
            seen.add((gx, gy))
            grid.setdefault((gx, gy), []).append(i)
    for (gx, gy), members in grid.items():
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                other = grid.get((gx + dx, gy + dy))
                if not other:
                    continue
                union(members[0], other[0])
        for m in members[1:]:
            union(members[0], m)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    return list(groups.values())


def _bbox(pts):
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return [min(xs), min(ys), max(xs), max(ys)]


def _translation_fit(lost_segs, new_segs, tol):
    """Is `new` the translated copy of `lost`?  Returns (share, dx, dy)."""
    if not lost_segs or not new_segs:
        return 0.0, 0.0, 0.0
    cl = _centroid(lost_segs)
    cn = _centroid(new_segs)
    dx, dy = cn[0] - cl[0], cn[1] - cl[1]
    flags, mlen, tlen = unmatched_mask(lost_segs, new_segs, (-dx, -dy), tol)
    return (mlen / max(tlen, 1e-9)), dx, dy


def _centroid(segs):
    sx = sy = w = 0.0
    for s in segs:
        L = max(s["len"], 1e-9)
        sx += (s["p0"][0] + s["p1"][0]) / 2 * L
        sy += (s["p0"][1] + s["p1"][1]) / 2 * L
        w += L
    return sx / w, sy / w


def ledger(exA, exB, *, off=(0.0, 0.0), LA=None, LB=None, params=None, meta=None):
    """Object-level ledger between two prepared-block extracts, in PDF points.

    `off` maps B into A (A ~ B + off), the convention grp_g2_churn.py measured.
    """
    p = dict(DEF)
    p.update(params or {})
    if LA is None or LB is None:
        LA, LB, meta = layers(exA, exB)
    S = meta["S_shared"] if meta else max(LA.S, LB.S)
    tol = p["tol_pt"]

    fa, mla, tla = unmatched_mask(exA.segments, exB.segments, off, tol)
    fb, mlb, tlb = unmatched_mask(exB.segments, exA.segments, (-off[0], -off[1]), tol)

    lost = [(k, exA.segments[k], 1.0 - fa[k]) for k in range(len(exA.segments)) if fa[k] < 0.999]
    new = [(k, exB.segments[k], 1.0 - fb[k]) for k in range(len(exB.segments)) if fb[k] < 0.999]

    r = max(p["cluster_r_S"] * S, p["cluster_r_min"])
    items = []
    for (k, s, u) in lost:
        items.append((s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], ("A", k, u)))
    for (k, s, u) in new:
        items.append((s["p0"][0] + off[0], s["p0"][1] + off[1],
                      s["p1"][0] + off[0], s["p1"][1] + off[1], ("B", k, u)))
    groups = _cluster(items, r) if items else []

    fr = exA.frame["clip_display"]
    padx = max(p["border_pad_min"], p["border_pad_frac"] * (fr[2] - fr[0]))
    pady = max(p["border_pad_min"], p["border_pad_frac"] * (fr[3] - fr[1]))

    regions = []
    for g in groups:
        A_ix, B_ix = [], []
        lenA = lenB = 0.0
        pts = []
        for i in g:
            x0, y0, x1, y1, (side, k, u) = items[i]
            pts += [(x0, y0), (x1, y1)]
            if side == "A":
                A_ix.append(k)
                lenA += exA.segments[k]["len"] * u
            else:
                B_ix.append(k)
                lenB += exB.segments[k]["len"] * u
        bb = _bbox(pts)
        tot = lenA + lenB
        rec = {"bbox_pt": [round(v, 3) for v in bb],
               "len_lost": round(lenA, 3), "len_new": round(lenB, 3),
               "n_seg_lost": len(A_ix), "n_seg_new": len(B_ix),
               "change_len": round(tot, 3),
               "_A_ix": A_ix, "_B_ix": B_ix}
        rec["at_boundary"] = bool(bb[0] <= fr[0] + padx or bb[1] <= fr[1] + pady or
                                  bb[2] >= fr[2] - padx or bb[3] >= fr[3] - pady)
        if lenB <= p["pure_thr"] * max(lenA, 1e-9):
            rec["type"] = "REMOVED_OBJECT"
        elif lenA <= p["pure_thr"] * max(lenB, 1e-9):
            rec["type"] = "ADDED_OBJECT"
        else:
            share, dx, dy = _translation_fit([exA.segments[k] for k in A_ix],
                                             [{**exB.segments[k],
                                               "p0": (exB.segments[k]["p0"][0] + off[0],
                                                      exB.segments[k]["p0"][1] + off[1]),
                                               "p1": (exB.segments[k]["p1"][0] + off[0],
                                                      exB.segments[k]["p1"][1] + off[1])}
                                              for k in B_ix], tol)
            if share >= p["move_match_thr"] and math.hypot(dx, dy) > tol:
                rec["type"] = "MOVED_OBJECT"
                rec["dx_pt"], rec["dy_pt"] = round(dx, 3), round(dy, 3)
                rec["move_share"] = round(share, 3)
            else:
                rec["type"] = "CHANGED_OBJECT"
                rec["move_share"] = round(share, 3)
        regions.append(rec)

    # ---- pair a pure REMOVED with a pure ADDED that is its translated copy -------
    rem = [i for i, x in enumerate(regions) if x["type"] == "REMOVED_OBJECT"]
    add = [i for i, x in enumerate(regions) if x["type"] == "ADDED_OBJECT"]
    used = set()
    for i in rem:
        if i in used:
            continue
        best = None
        for j in add:
            if j in used:
                continue
            la, lb = regions[i]["len_lost"], regions[j]["len_new"]
            if la <= 0 or lb <= 0 or not (0.75 <= la / lb <= 1.33):
                continue
            share, dx, dy = _translation_fit(
                [exA.segments[k] for k in regions[i]["_A_ix"]],
                [{**exB.segments[k],
                  "p0": (exB.segments[k]["p0"][0] + off[0], exB.segments[k]["p0"][1] + off[1]),
                  "p1": (exB.segments[k]["p1"][0] + off[0], exB.segments[k]["p1"][1] + off[1])}
                 for k in regions[j]["_B_ix"]], tol)
            if share >= p["move_match_thr"] and (best is None or share > best[0]):
                best = (share, j, dx, dy)
        if best:
            share, j, dx, dy = best
            used.add(i)
            used.add(j)
            bb = _bbox([(regions[i]["bbox_pt"][0], regions[i]["bbox_pt"][1]),
                        (regions[i]["bbox_pt"][2], regions[i]["bbox_pt"][3]),
                        (regions[j]["bbox_pt"][0], regions[j]["bbox_pt"][1]),
                        (regions[j]["bbox_pt"][2], regions[j]["bbox_pt"][3])])
            merged = {"type": "MOVED_OBJECT",
                      "bbox_pt": [round(v, 3) for v in bb],
                      "bbox_from_pt": regions[i]["bbox_pt"], "bbox_to_pt": regions[j]["bbox_pt"],
                      "len_lost": regions[i]["len_lost"], "len_new": regions[j]["len_new"],
                      "n_seg_lost": regions[i]["n_seg_lost"], "n_seg_new": regions[j]["n_seg_new"],
                      "change_len": round(regions[i]["change_len"] + regions[j]["change_len"], 3),
                      "dx_pt": round(dx, 3), "dy_pt": round(dy, 3), "move_share": round(share, 3),
                      "at_boundary": regions[i]["at_boundary"] or regions[j]["at_boundary"],
                      "_A_ix": regions[i]["_A_ix"], "_B_ix": regions[j]["_B_ix"]}
            regions.append(merged)
    regions = [x for k, x in enumerate(regions) if k not in used]

    # ---- name the objects --------------------------------------------------------
    for rec in regions:
        rec["objects_a"] = _name_objects(LA, rec["_A_ix"], exA.segments, p["obj_share_thr"])
        rec["objects_b"] = _name_objects(LB, rec["_B_ix"], exB.segments, p["obj_share_thr"])
        # probe `ldg` needs the segment indices to attach GEOMETRIC evidence to a record
        # (which existing ink the new ink touches, and at what angle).  Off by default,
        # so every number measured before this option existed is unchanged.
        if p.get("keep_seg_ix"):
            rec["seg_ix_a"] = list(rec["_A_ix"])
            rec["seg_ix_b"] = list(rec["_B_ix"])
        rec.pop("_A_ix", None)
        rec.pop("_B_ix", None)

    thr = p["min_change_len_pt"]
    kept = [x for x in regions if x["change_len"] >= thr]
    kept.sort(key=lambda x: -x["change_len"])

    sim = (mla + mlb) / max(tla + tlb, 1e-9)
    return {
        "records": kept,
        "n_records": len(kept),
        "n_records_interior": sum(1 for x in kept if not x["at_boundary"]),
        "changed_len_total": round(sum(x["change_len"] for x in kept), 3),
        "scalar": {
            "ink_similarity": round(sim, 6),
            "unmatched_share_a": round(1 - mla / max(tla, 1e-9), 6),
            "unmatched_share_b": round(1 - mlb / max(tlb, 1e-9), 6),
            "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
            "ink_len_a": round(tla, 2), "ink_len_b": round(tlb, 2),
        },
        "counts": {
            "n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects),
            "d_obj": len(LB.objects) - len(LA.objects),
            "d_seg": len(exB.segments) - len(exA.segments),
            "cls_a": LA.counts(), "cls_b": LB.counts(),
        },
        "S": round(S, 4),
        "meta": meta or {},
    }


def _name_objects(L, seg_ix, segs, share_thr):
    if not seg_ix:
        return []
    acc: dict[int, float] = {}
    for k in seg_ix:
        oi = L.seg2obj[k] if k < len(L.seg2obj) else -1
        if oi >= 0:
            acc[oi] = acc.get(oi, 0.0) + segs[k]["len"]
    out = []
    for oi, ln in sorted(acc.items(), key=lambda kv: -kv[1]):
        o = L.objects[oi]
        sh = ln / max(o["seg_len"], 1e-9)
        out.append({"object_id": o["object_id"], "cls": o["cls"],
                    "share_of_object": round(min(sh, 1.0), 3),
                    "bbox_pt": o["bbox"], "label": o.get("label")})
        if len(out) >= 8:
            break
    return [x for x in out if x["share_of_object"] >= share_thr] or out[:1]


# ---------------------------------------------------------------- baselines

def scalar_verdict(led, thr=0.999):
    """v0.1 baseline: ONE global number -> NEAR_IDENTICAL / CHANGED."""
    return "NO_GRAPHIC_CHANGE" if led["scalar"]["ink_similarity"] >= thr else "GRAPHIC_CHANGE"


def counts_verdict(led):
    """The 'did we just build a counter?' baseline: primitive and object counters."""
    c = led["counts"]
    return "NO_GRAPHIC_CHANGE" if (c["d_obj"] == 0 and c["d_seg"] == 0) else "GRAPHIC_CHANGE"


# ---------------------------------------------------------------- scoring vs manifest

_TYPE_OK = {
    "REMOVED_OBJECT": {"REMOVED_OBJECT", "CHANGED_OBJECT"},
    "ADDED_OBJECT": {"ADDED_OBJECT", "CHANGED_OBJECT"},
    "MOVED_OBJECT": {"MOVED_OBJECT", "CHANGED_OBJECT"},
    "RESHAPED_OBJECT": {"CHANGED_OBJECT", "MOVED_OBJECT", "ADDED_OBJECT", "REMOVED_OBJECT"},
    "DIMENSION_CHAIN_CHANGED": {"ADDED_OBJECT", "CHANGED_OBJECT", "REMOVED_OBJECT"},
    "SPLIT_OBJECT": {"REMOVED_OBJECT", "CHANGED_OBJECT"},
    "MERGED_OBJECTS": {"ADDED_OBJECT", "CHANGED_OBJECT"},
    # DEFECT-2 (see loc_FINDINGS): these two expected types were missing, so C9/C10
    # scored L3 = L4 = 0 by omission of a dictionary key, not by any property of the
    # comparator.  Adding a branch ADDS ink; closing an opening ADDS ink as well.
    "ADDED_BRANCH": {"ADDED_OBJECT", "CHANGED_OBJECT"},
    "REMOVED_OPENING": {"ADDED_OBJECT", "CHANGED_OBJECT"},
}


_OV_PAD = 1.0   # points


def _ov_raw(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0]))
    iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    aa = max(a[2] - a[0], 1e-9) * max(a[3] - a[1], 1e-9)
    ab = max(b[2] - b[0], 1e-9) * max(b[3] - b[1], 1e-9)
    return inter / max(min(aa, ab), 1e-9)


def _ov(a, b):
    """Bbox overlap that survives DEGENERATE boxes.

    DEFECT-1 (see loc_FINDINGS).  A change that is one straight line — an added
    branch, a closed opening, a split of a polyline — has a bbox of zero height or
    zero width, and the plain area ratio is then 0 even when the record bbox is
    IDENTICAL to the ground-truth bbox.  This is the same defect class that cost the
    foundation 45 % of its geometry (fnd_GATEFIX): a rectangle predicate that is false
    on a rectangle with no area.  Both boxes are therefore measured a second time
    inflated by `_OV_PAD` points and the larger of the two values is taken, which
    keeps the measure monotone with respect to the unpadded one.
    """
    v = _ov_raw(a, b)
    pa = [a[0] - _OV_PAD, a[1] - _OV_PAD, a[2] + _OV_PAD, a[3] + _OV_PAD]
    pb = [b[0] - _OV_PAD, b[1] - _OV_PAD, b[2] + _OV_PAD, b[3] + _OV_PAD]
    return max(v, _ov_raw(pa, pb))


def score_against_manifest(led, man, *, pad_pt=2.0):
    """Three levels of 'the ledger says the right thing about the right object'."""
    exp_bb = man.get("change_bbox_pt")
    exp_types = [e.get("type") for e in man.get("expected_ledger", [])]
    # only ids the manifest actually asserts as SURVIVING identity: an ADDED object is a
    # new object on side B and its `copy_of` id belongs to side A, so it is not required.
    exp_oids = {e.get("object_id") for e in man.get("expected_ledger", []) if e.get("object_id")}
    exp_oids.discard(None)
    if exp_bb:
        exp_bb = [exp_bb[0] - pad_pt, exp_bb[1] - pad_pt, exp_bb[2] + pad_pt, exp_bb[3] + pad_pt]
    recs = led["records"]
    hit_loc = hit_type = hit_obj = None
    for r in recs:
        if not exp_bb:
            continue
        if _ov(r["bbox_pt"], exp_bb) < 0.30:
            continue
        hit_loc = hit_loc or r
        ok_types = set()
        for t in exp_types:
            ok_types |= _TYPE_OK.get(t, {t})
        if r["type"] in ok_types:
            hit_type = hit_type or r
            ids = {o["object_id"] for o in r["objects_a"]} | {o["object_id"] for o in r["objects_b"]}
            if not exp_oids or ids & exp_oids:
                hit_obj = hit_obj or r
    n_false = sum(1 for r in recs if not exp_bb or _ov(r["bbox_pt"], exp_bb) < 0.30)
    n_false_int = sum(1 for r in recs
                      if (not exp_bb or _ov(r["bbox_pt"], exp_bb) < 0.30) and not r["at_boundary"])
    rec_rows = [[round(r["change_len"], 3),
                 1 if (exp_bb and _ov(r["bbox_pt"], exp_bb) >= 0.30) else 0,
                 1 if r["at_boundary"] else 0] for r in recs[:400]]
    # full geometry of the top records, so that any later change of the scoring rule
    # can be replayed offline instead of re-running the comparator
    rec_boxes = [[r["type"], [round(v, 2) for v in r["bbox_pt"]], round(r["change_len"], 2),
                  1 if r["at_boundary"] else 0] for r in recs[:40]]
    return {
        "recs": rec_rows,
        "rec_boxes": rec_boxes,
        "exp_bbox_padded": [round(v, 2) for v in exp_bb] if exp_bb else None,
        "L1_any": bool(recs),
        "L2_localised": hit_loc is not None,
        "L3_right_type": hit_type is not None,
        "L4_right_object": hit_obj is not None,
        "n_records": len(recs),
        "n_false_records": n_false,
        "n_false_records_interior": n_false_int,
        "hit_change_len": hit_loc["change_len"] if hit_loc else 0.0,
        "hit_type": hit_loc["type"] if hit_loc else None,
        "top_false_len": max([r["change_len"] for r in recs
                              if not exp_bb or _ov(r["bbox_pt"], exp_bb) < 0.30] or [0.0]),
    }


# ---------------------------------------------------------------- export noise
# Two prepared blocks of two revisions are never byte-identical exports.  A
# counterfactual applied to a segment list IS byte-identical everywhere it did not
# touch, which would make detection trivially perfect.  So the B side is additionally
# passed through one class-A rewrite, and the SAME rewrite is applied to the negative
# (no-change) control, so positives and negatives differ only by the counterfactual.

import random as _random                      # noqa: E402
import grp_common as _G                       # noqa: E402
import v03_counterfactual as _C               # noqa: E402


def _jitter_nodes(segs, eps, seed):
    """Per-NODE uniform jitter: coordinates that coincided keep coinciding.
    That is how a re-export perturbs geometry; per-endpoint noise would tear every
    polyline apart and is not a fair model."""
    rng = _random.Random(seed)
    shift: dict[tuple, tuple] = {}

    def q(p):
        k = (round(p[0], 4), round(p[1], 4))
        if k not in shift:
            shift[k] = (rng.uniform(-eps, eps), rng.uniform(-eps, eps))
        d = shift[k]
        return (p[0] + d[0], p[1] + d[1])

    out = []
    for i, s in enumerate(segs):
        t = dict(s)
        t["p0"], t["p1"] = q(s["p0"]), q(s["p1"])
        t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
        t["i"] = i
        out.append(t)
    return out


NOISE_MODES = ("none", "repack", "resample", "chords5", "round025", "jitter03", "jitter10")


def noisy(ex, mode, seed=0):
    """Return a copy of `ex` whose geometry went through one export-noise rewrite."""
    if mode == "none":
        return ex
    if mode == "repack":
        # A1: only the path ids change.  The extract is a list of SEGMENTS, so this
        # rewrite is a no-op for the ledger by construction - measured, not assumed.
        segs = _G.rw_path_split([dict(s) for s in ex.segments], _random.Random(seed))
    elif mode == "resample":
        # A3: every curve is written with a different chord density - the real
        # "same picture, other packaging" case (mine M11: x2.21 segments at 0.08 %
        # visible difference)
        segs = _G.REWRITES["A3_curve_resample_down"]([dict(s) for s in ex.segments],
                                                     _random.Random(seed))
    elif mode == "chords5":
        segs = _G.REWRITES["A4b_circle_to_chords5"]([dict(s) for s in ex.segments],
                                                    _random.Random(seed))
    elif mode == "round025":
        segs = _G.REWRITES["A6_round_0.25"]([dict(s) for s in ex.segments], _random.Random(seed))
    elif mode.startswith("jitter"):
        segs = _jitter_nodes(ex.segments, int(mode[6:]) / 10.0, seed)
    else:
        raise ValueError(mode)
    for k, s in enumerate(segs):
        s["i"] = k
        s.pop("src", None)
    return _C._clone(ex, segments=segs, prov={"loc_noise": mode})


# ---------------------------------------------------------------- falsification arm
# The alternative design the track was warned against: build the ledger by matching
# OBJECT to OBJECT instead of ink to ink.  grp G2-3 measured that object boundaries
# survive <0.51 of the ink above 15 000 segments per block, so this variant should
# report its own churn as change.  Measuring it is how the main design is falsifiable.

def object_ledger(LA, LB, off=(0.0, 0.0), *, S=None, max_r_S=3.0, desc_thr=0.30,
                  len_ratio=0.30):
    S = S or max(LA.S, LB.S)
    R = max_r_S * S
    cand = []
    for i, a in enumerate(LA.objects):
        for j, b in enumerate(LB.objects):
            d = math.hypot(a["cx"] - (b["cx"] + off[0]), a["cy"] - (b["cy"] + off[1]))
            if d > R:
                continue
            if abs(a["seg_len"] - b["seg_len"]) > len_ratio * max(a["seg_len"], b["seg_len"]):
                continue
            dd = O.descriptor_distance(a["desc"], b["desc"])
            if dd > desc_thr:
                continue
            cand.append((d + dd, i, j))
    cand.sort()
    ua, ub = set(), set()
    moved = 0
    for _, i, j in cand:
        if i in ua or j in ub:
            continue
        ua.add(i)
        ub.add(j)
        a, b = LA.objects[i], LB.objects[j]
        if math.hypot(a["cx"] - (b["cx"] + off[0]), a["cy"] - (b["cy"] + off[1])) > 1.0:
            moved += 1
    rem = [i for i in range(len(LA.objects)) if i not in ua]
    add = [j for j in range(len(LB.objects)) if j not in ub]
    recs = ([{"type": "REMOVED_OBJECT", "bbox_pt": LA.objects[i]["bbox"],
              "change_len": LA.objects[i]["seg_len"], "at_boundary": False,
              "objects_a": [{"object_id": LA.objects[i]["object_id"]}], "objects_b": []}
             for i in rem] +
            [{"type": "ADDED_OBJECT",
              "bbox_pt": [LB.objects[j]["bbox"][0] + off[0], LB.objects[j]["bbox"][1] + off[1],
                          LB.objects[j]["bbox"][2] + off[0], LB.objects[j]["bbox"][3] + off[1]],
              "change_len": LB.objects[j]["seg_len"], "at_boundary": False,
              "objects_a": [], "objects_b": [{"object_id": LB.objects[j]["object_id"]}]}
             for j in add])
    recs.sort(key=lambda r: -r["change_len"])
    return {"records": recs, "n_records": len(recs), "n_removed": len(rem),
            "n_added": len(add), "n_matched": len(ua), "n_moved": moved,
            "n_records_interior": len(recs),
            "changed_len_total": round(sum(r["change_len"] for r in recs), 3),
            "scalar": {"ink_similarity": 0.0}, "counts": {}}
