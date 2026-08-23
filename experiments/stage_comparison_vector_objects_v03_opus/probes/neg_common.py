# -*- coding: utf-8 -*-
"""Shared machinery for probe `neg` (negative controls: text / tables / dimensions).

Nothing here reads a PDF on its own: every block comes through
`v03_foundation.extract_block` (via `grp_common`), every object layer through
`v03_objects.build_objects`, every counterfactual through `v03_counterfactual.apply`.

The one thing this file adds is the GRAPHIC VERDICT: a comparator that turns two
object layers into a ledger of graphic changes.  A negative control passes only when
that ledger is empty.
"""
from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

EXP = Path(__file__).resolve().parents[1]
ART = EXP / "artifacts"
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(EXP / "probes"))

import v03_foundation as F        # noqa: E402
import v03_objects as O           # noqa: E402
import v03_counterfactual as CF   # noqa: E402
import grp_common as G            # noqa: E402
import grp_match as M             # noqa: E402


# ------------------------------------------------------------------ carriers

def carriers():
    d = json.load(open(ART / "cf_manifest.json", encoding="utf-8"))
    return d["carriers"]


_EX_CACHE: dict[str, object] = {}


def carrier_extract(c):
    key = f"{c['doc_id']}|{c['version']}|{c['block_id']}"
    if key not in _EX_CACHE:
        pb = G.prepared_block(c["doc_id"], c["version"], c["block_id"])
        if pb is None:
            raise RuntimeError(f"no prepared block {key}")
        _EX_CACHE[key] = G.extract(pb)
    return _EX_CACHE[key]


def carrier_key(c):
    return f"{c['discipline']}-{c['doc_id']}-{c['version']}-{c['block_id']}"


# ------------------------------------------------------------------ comparator

DEFAULT_CMP = {
    "tol": 0.8,          # ink correspondence tolerance, PDF points
    "u_share": 0.35,     # an object counts as changed when this share of its ink is unpartnered
    "L_min_S": 2.0,      # ... AND that unpartnered ink is at least L_min_S * S points
    "L_min_abs": 3.0,    # ... and at least this many points, whatever S is
    "border_margin_S": 1.0,   # object closer than this to the crop edge is a border object
    "shared_scale": True,     # G2-2b: both sides grouped with S = max(S_a, S_b)
}


def _frame(ex):
    return [float(v) for v in ex.frame["clip_display"]]


def _touches_border(bbox, fr, margin):
    return (bbox[0] - fr[0] <= margin or bbox[1] - fr[1] <= margin
            or fr[2] - bbox[2] <= margin or fr[3] - bbox[3] <= margin)


def layers(ex_a, ex_b, shared_scale=True, **params):
    la = O.build_objects(ex_a, **params)
    lb = O.build_objects(ex_b, **params)
    if shared_scale and abs(la.S - lb.S) > 1e-9:
        S = max(la.S, lb.S)
        la = O.build_objects(ex_a, S_override=S, **params)
        lb = O.build_objects(ex_b, S_override=S, **params)
    return la, lb


def offset(ex_a, ex_b, seeds=None, tol=0.8):
    """Translation between the two sides, measured.  (0,0) is tried first and kept
    when it already explains the ink — that is the case for every segment-level
    counterfactual, and skipping the search there saves the run."""
    sa, sb = ex_a.segments, ex_b.segments
    if not sa or not sb:
        return (0.0, 0.0), 0.0, "empty"
    eidx = M.build_endpoint_index(sb)
    sub = sa if len(sa) <= 800 else sa[::max(1, len(sa) // 800)]
    z = M.share_endpoints(sub, eidx, (0.0, 0.0), 0.1)
    if z >= 0.99:
        return (0.0, 0.0), z, "zero"
    fa, fb = _frame(ex_a), _frame(ex_b)
    seeds = seeds or [(0.0, 0.0), (fb[0] - fa[0], fb[1] - fa[1])]
    dx, dy, final = M.register(sa, sb, seeds, tol=tol)
    return (dx, dy), final, "search"


def ledger(ex_a, ex_b, la, lb, off, cfg):
    """Ledger of GRAPHIC changes: ink of an object that has no partner on the other side.

    Boundary churn (an object split or merged between the two sides) is deliberately
    NOT a ledger entry — v0.2 OBJ-9 showed that is the dominant noise source and it is
    not evidence of a project change.  Only unpartnered INK is.
    """
    S = max(la.S, lb.S)
    Lmin = max(cfg["L_min_S"] * S, cfg["L_min_abs"])
    margin = cfg["border_margin_S"] * S
    fa, fb = _frame(ex_a), _frame(ex_b)
    rows_ab = M.churn_rows(la, ex_a.segments, lb, ex_b.segments, off, tol=cfg["tol"])
    rows_ba = M.churn_rows(lb, ex_b.segments, la, ex_a.segments,
                           (-off[0], -off[1]), tol=cfg["tol"])
    entries, border_entries = [], []
    for side, rows, lay, fr in (("A", rows_ab, la, fa), ("B", rows_ba, lb, fb)):
        for r in rows:
            u = r["unmatched_share"] * r["len"]
            if u < Lmin or r["unmatched_share"] < cfg["u_share"]:
                continue
            o = lay.objects[r["o"]]
            e = {"side": side,
                 "type": "REMOVED_OR_CHANGED" if side == "A" else "ADDED_OR_CHANGED",
                 "object_id": o["object_id"], "cls": o["cls"],
                 "bbox_pt": o["bbox"], "n_seg": o["n_seg"],
                 "unmatched_len_pt": round(u, 3),
                 "unmatched_share": round(r["unmatched_share"], 4),
                 "label": o.get("label")}
            if _touches_border(o["bbox"], fr, margin):
                e["at_crop_border"] = True
                border_entries.append(e)
            else:
                entries.append(e)
    return {
        "verdict": "GRAPHIC_CHANGE" if entries else "NO_GRAPHIC_CHANGE",
        "n_entries": len(entries),
        "n_border_entries": len(border_entries),
        "entries": entries,
        "border_entries": border_entries,
        "unmatched_len_pt": round(sum(e["unmatched_len_pt"] for e in entries), 3),
        "S": round(S, 4), "L_min_pt": round(Lmin, 3),
        "rows_ab": rows_ab, "rows_ba": rows_ba,
    }


def compare(ex_a, ex_b, cfg=None, layer_params=None, want_rows=False):
    cfg = {**DEFAULT_CMP, **(cfg or {})}
    lp = dict(layer_params or {})
    la, lb = layers(ex_a, ex_b, shared_scale=cfg["shared_scale"], **lp)
    off, share, how = offset(ex_a, ex_b, tol=cfg["tol"])
    led = ledger(ex_a, ex_b, la, lb, off, cfg)
    ch_ab = M.classify(led["rows_ab"])
    out = {
        "verdict": led["verdict"], "n_entries": led["n_entries"],
        "n_border_entries": led["n_border_entries"],
        "entries": led["entries"], "border_entries": led["border_entries"],
        "unmatched_len_pt": led["unmatched_len_pt"],
        "S_a": round(la.S, 4), "S_b": round(lb.S, 4), "S_used": led["S"],
        "scale_src_a": la.scale_source, "scale_src_b": lb.scale_source,
        "n_obj_a": len(la.objects), "n_obj_b": len(lb.objects),
        "n_seg_a": len(ex_a.segments), "n_seg_b": len(ex_b.segments),
        "off": [round(off[0], 3), round(off[1], 3)], "off_how": how,
        "off_share": round(share, 4),
        "one_to_one": round(ch_ab["one_to_one"], 5),
        "lost_share": round(ch_ab["lost"], 5),
        "L_min_pt": led["L_min_pt"],
    }
    if want_rows:
        out["_rows_ab"] = led["rows_ab"]
        out["_rows_ba"] = led["rows_ba"]
        out["_layers"] = (la, lb)
    return out


def ledger_at(ex_a, ex_b, la, lb, off, L_min_abs, u_share=0.35, L_min_S=0.0, tol=0.8,
              rows=None):
    """Re-score an already computed pair at another sensitivity threshold."""
    cfg = {**DEFAULT_CMP, "L_min_abs": L_min_abs, "L_min_S": L_min_S, "u_share": u_share,
           "tol": tol}
    if rows is None:
        return ledger(ex_a, ex_b, la, lb, off, cfg)
    S = max(la.S, lb.S)
    Lmin = max(L_min_S * S, L_min_abs)
    margin = cfg["border_margin_S"] * S
    fa, fb = _frame(ex_a), _frame(ex_b)
    n, nb = 0, 0
    for side, rr, lay, fr in (("A", rows[0], la, fa), ("B", rows[1], lb, fb)):
        for r in rr:
            u = r["unmatched_share"] * r["len"]
            if u < Lmin or r["unmatched_share"] < u_share:
                continue
            o = lay.objects[r["o"]]
            if _touches_border(o["bbox"], fr, margin):
                nb += 1
            else:
                n += 1
    return {"n_entries": n, "n_border_entries": nb,
            "verdict": "GRAPHIC_CHANGE" if n else "NO_GRAPHIC_CHANGE"}


def pct(vals, q):
    return G.pct(vals, q)


def med(vals):
    v = sorted(vals)
    if not v:
        return None
    n = len(v)
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def dump(name, obj):
    p = ART / name
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, default=str)
    print(f"[neg] wrote {p} ({p.stat().st_size} bytes)")


# ------------------------------------------------------------------ ledger flavours
# Four candidate ledger designs.  A negative control must produce ZERO records in the
# flavour a production comparator would use; measuring all four is what makes the
# recommendation ("key the ledger to unpartnered INK") a measurement and not taste.

def ledger_variants(ex_a, ex_b, la, lb, off, cfg, rows_ab, rows_ba, churn_thr=0.95):
    S = max(la.S, lb.S)
    Lmin = max(cfg["L_min_S"] * S, cfg["L_min_abs"])
    margin = cfg["border_margin_S"] * S
    fa, fb = _frame(ex_a), _frame(ex_b)

    n_ink = n_ink_border = 0
    for side, rr, lay, fr in (("A", rows_ab, la, fa), ("B", rows_ba, lb, fb)):
        for r in rr:
            u = r["unmatched_share"] * r["len"]
            if u < Lmin or r["unmatched_share"] < cfg["u_share"]:
                continue
            if _touches_border(lay.objects[r["o"]]["bbox"], fr, margin):
                n_ink_border += 1
            else:
                n_ink += 1

    n_count = abs(len(la.objects) - len(lb.objects))

    ids_a: dict[str, int] = {}
    ids_b: dict[str, int] = {}
    for o in la.objects:
        ids_a[o["object_id"]] = ids_a.get(o["object_id"], 0) + 1
    for o in lb.objects:
        ids_b[o["object_id"]] = ids_b.get(o["object_id"], 0) + 1
    n_id = sum(max(0, v - ids_b.get(k, 0)) for k, v in ids_a.items()) \
        + sum(max(0, v - ids_a.get(k, 0)) for k, v in ids_b.items())

    n_churn = 0
    for r in rows_ab:
        if r["n_partners"] == 0:
            n_churn += 1
        elif r["best_share"] < churn_thr or r["partner_purity"] < churn_thr:
            n_churn += 1

    # labels: an object whose text anchor changed (this must never be a graphic record)
    n_label = 0
    if la.objects and lb.objects:
        lab_a = {}
        for r in rows_ab:
            o = la.objects[r["o"]]
            if r["n_partners"] and r.get("best_share", 0) >= churn_thr:
                p = lb.objects[r["partner"]]
                if (o.get("label") or None) != (p.get("label") or None):
                    n_label += 1
    return {"ink": n_ink, "ink_border": n_ink_border, "count": n_count,
            "object_id": n_id, "churn": n_churn, "label": n_label}


def full_compare(ex_a, ex_b, cfg=None, layer_params=None, shared_scale=None):
    cfg = {**DEFAULT_CMP, **(cfg or {})}
    if shared_scale is not None:
        cfg["shared_scale"] = shared_scale
    lp = dict(layer_params or {})
    la, lb = layers(ex_a, ex_b, shared_scale=cfg["shared_scale"], **lp)
    off, share, how = offset(ex_a, ex_b, tol=cfg["tol"])
    rows_ab = M.churn_rows(la, ex_a.segments, lb, ex_b.segments, off, tol=cfg["tol"])
    rows_ba = M.churn_rows(lb, ex_b.segments, la, ex_a.segments,
                           (-off[0], -off[1]), tol=cfg["tol"])
    lv = ledger_variants(ex_a, ex_b, la, lb, off, cfg, rows_ab, rows_ba)
    ch = M.classify(rows_ab)
    return {
        "ledger": lv,
        "verdict": "GRAPHIC_CHANGE" if lv["ink"] else "NO_GRAPHIC_CHANGE",
        "S_a": round(la.S, 4), "S_b": round(lb.S, 4),
        "scale_src_a": la.scale_source, "scale_src_b": lb.scale_source,
        "n_obj_a": len(la.objects), "n_obj_b": len(lb.objects),
        "n_seg_a": len(ex_a.segments), "n_seg_b": len(ex_b.segments),
        "n_text_a": len(ex_a.texts), "n_text_b": len(ex_b.texts),
        "off": [round(off[0], 3), round(off[1], 3)], "off_how": how,
        "off_share": round(share, 4),
        "one_to_one": round(ch["one_to_one"], 5), "lost_share": round(ch["lost"], 5),
        "L_min_pt": round(max(cfg["L_min_S"] * max(la.S, lb.S), cfg["L_min_abs"]), 3),
        "_la": la, "_lb": lb, "_off": off, "_rows": (rows_ab, rows_ba), "_cfg": cfg,
    }


def geometry_identical(ex_a, ex_b) -> bool:
    if len(ex_a.segments) != len(ex_b.segments):
        return False
    ka = sorted((round(s["p0"][0], 6), round(s["p0"][1], 6),
                 round(s["p1"][0], 6), round(s["p1"][1], 6)) for s in ex_a.segments)
    kb = sorted((round(s["p0"][0], 6), round(s["p0"][1], 6),
                 round(s["p1"][0], 6), round(s["p1"][1], 6)) for s in ex_b.segments)
    return ka == kb


# ------------------------------------------------------------------ fast exact pre-pass
# `grp_match.churn_rows` searches a spatial grid for every segment; on a 59 575-segment
# block that costs 30 s per comparison.  Every text-only control leaves the ink
# byte-identical, so an exact coordinate hash resolves those segments in O(1) and the
# grid is used only for what the hash misses.  Semantics are unchanged: an exact
# coordinate match is a both-endpoint match at tolerance 0.

def _skey(x0, y0, x1, y1, q=10000.0):
    a = (int(round(x0 * q)), int(round(y0 * q)))
    b = (int(round(x1 * q)), int(round(y1 * q)))
    return (a, b) if a <= b else (b, a)


def rows_fast(layer_a, segs_a, layer_b, segs_b, off, tol=0.8):
    hb: dict = {}
    for s in segs_b:
        hb.setdefault(_skey(s["p0"][0] + off[0], s["p0"][1] + off[1],
                            s["p1"][0] + off[0], s["p1"][1] + off[1]), s["i"])
    eidx = pidx = None
    seg2obj_b = layer_b.seg2obj
    rows = []
    n_hash = n_grid = n_none = 0
    for oi, o in enumerate(layer_a.objects):
        acc: dict[int, float] = {}
        tot = unmatched = 0.0
        for gi in o["segments"]:
            s = segs_a[gi]
            L = s["len"]
            tot += L
            bi = hb.get(_skey(s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1]))
            if bi is not None:
                n_hash += 1
                ob = seg2obj_b[bi]
                if ob >= 0:
                    acc[ob] = acc.get(ob, 0.0) + L
                    continue
            if eidx is None:
                eidx = M.build_endpoint_index(
                    [{**t, "p0": (t["p0"][0] + off[0], t["p0"][1] + off[1]),
                      "p1": (t["p1"][0] + off[0], t["p1"][1] + off[1])} for t in segs_b])
                pidx = M.build_index(segs_b, off)
            bi = M.query_endpoints(eidx, s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], tol)
            if bi is not None:
                n_grid += 1
                ob = seg2obj_b[bi]
                if ob >= 0:
                    acc[ob] = acc.get(ob, 0.0) + L
                    continue
            ang = math.degrees(math.atan2(s["p1"][1] - s["p0"][1],
                                          s["p1"][0] - s["p0"][0])) % 180.0
            votes: dict[int, float] = {}
            for k in range(3):
                t = (k + 0.5) / 3
                x = s["p0"][0] + t * (s["p1"][0] - s["p0"][0])
                y = s["p0"][1] + t * (s["p1"][1] - s["p0"][1])
                bj = M.query(pidx, x, y, ang, tol)
                if bj is None:
                    continue
                ob = seg2obj_b[bj]
                if ob >= 0:
                    votes[ob] = votes.get(ob, 0.0) + L / 3
            if votes:
                n_grid += 1
                for ob, w in votes.items():
                    acc[ob] = acc.get(ob, 0.0) + w
            else:
                n_none += 1
                unmatched += L
        if tot <= 0:
            continue
        matched = sum(acc.values())
        row = {"o": oi, "cls": o["cls"], "len": tot, "matched": matched,
               "unmatched_share": unmatched / tot}
        if not acc:
            row.update({"n_partners": 0, "best_share": 0.0, "partner_purity": 0.0})
        else:
            bj, bl = max(acc.items(), key=lambda kv: kv[1])
            row.update({"n_partners": len(acc), "partner": bj,
                        "best_share": bl / max(matched, 1e-9),
                        "partner_purity": bl / max(layer_b.objects[bj]["seg_len"], 1e-9)})
        rows.append(row)
    if rows:
        rows[0]["_hash"] = n_hash
        rows[0]["_grid"] = n_grid
        rows[0]["_none"] = n_none
    return rows


def full_compare2(ex_a, ex_b, cfg=None, layer_params=None, shared_scale=None,
                  la=None, lb=None):
    """Same contract as full_compare, with the exact-hash pre-pass."""
    cfg = {**DEFAULT_CMP, **(cfg or {})}
    if shared_scale is not None:
        cfg["shared_scale"] = shared_scale
    lp = dict(layer_params or {})
    if la is None or lb is None:
        la, lb = layers(ex_a, ex_b, shared_scale=cfg["shared_scale"], **lp)
    off, share, how = offset(ex_a, ex_b, tol=cfg["tol"])
    rows_ab = rows_fast(la, ex_a.segments, lb, ex_b.segments, off, tol=cfg["tol"])
    rows_ba = rows_fast(lb, ex_b.segments, la, ex_a.segments,
                        (-off[0], -off[1]), tol=cfg["tol"])
    lv = ledger_variants(ex_a, ex_b, la, lb, off, cfg, rows_ab, rows_ba)
    ch = M.classify(rows_ab)
    return {
        "ledger": lv,
        "verdict": "GRAPHIC_CHANGE" if lv["ink"] else "NO_GRAPHIC_CHANGE",
        "S_a": round(la.S, 4), "S_b": round(lb.S, 4),
        "scale_src_a": la.scale_source, "scale_src_b": lb.scale_source,
        "n_obj_a": len(la.objects), "n_obj_b": len(lb.objects),
        "n_seg_a": len(ex_a.segments), "n_seg_b": len(ex_b.segments),
        "n_text_a": len(ex_a.texts), "n_text_b": len(ex_b.texts),
        "off": [round(off[0], 3), round(off[1], 3)], "off_how": how,
        "off_share": round(share, 4),
        "one_to_one": round(ch["one_to_one"], 5), "lost_share": round(ch["lost"], 5),
        "L_min_pt": round(max(cfg["L_min_S"] * max(la.S, lb.S), cfg["L_min_abs"]), 3),
        "_la": la, "_lb": lb, "_off": off, "_rows": (rows_ab, rows_ba), "_cfg": cfg,
    }


def ink_entry_list(ex_a, ex_b, la, lb, off, cfg, rows_ab, rows_ba):
    """The ink-keyed ledger as a LIST, each record carrying its object index so a
    downstream filter (e.g. 'this object is a letter contour') can be applied."""
    S = max(la.S, lb.S)
    Lmin = max(cfg["L_min_S"] * S, cfg["L_min_abs"])
    margin = cfg["border_margin_S"] * S
    fa, fb = _frame(ex_a), _frame(ex_b)
    out = []
    for side, rr, lay, fr in (("A", rows_ab, la, fa), ("B", rows_ba, lb, fb)):
        for r in rr:
            u = r["unmatched_share"] * r["len"]
            if u < Lmin or r["unmatched_share"] < cfg["u_share"]:
                continue
            o = lay.objects[r["o"]]
            out.append({"side": side, "oi": r["o"], "cls": o["cls"],
                        "object_id": o["object_id"], "bbox": o["bbox"],
                        "n_seg": o["n_seg"], "unmatched_len_pt": round(u, 3),
                        "border": _touches_border(o["bbox"], fr, margin)})
    return out
