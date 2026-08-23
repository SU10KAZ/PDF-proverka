# -*- coding: utf-8 -*-
"""Table negative controls and the row add/remove boundary case (N2).

The cf engine's D4/D5 edit the TEXT of cells.  What it does not have is the case the
task asks about explicitly: a table ROW added or removed.  In real CAD a row insert
does not just add one rule — it pushes everything below it down and stretches the
vertical rules.  Both variants are built here so the difference can be measured.
"""
from __future__ import annotations
import math, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_counterfactual as CF   # noqa: E402


def _frame(ex):
    return [float(v) for v in ex.frame["clip_display"]]


def rulings(ex, min_frac=0.45, tol=0.6):
    """Horizontal rules of a table: long, flat segments, merged by y."""
    fr = _frame(ex)
    w = max(fr[2] - fr[0], 1e-6)
    cand = []
    for s in ex.segments:
        dx = s["p1"][0] - s["p0"][0]
        dy = s["p1"][1] - s["p0"][1]
        L = s["len"]
        if L < min_frac * w or abs(dy) > 0.02 * max(abs(dx), 1e-9):
            continue
        cand.append((min(s["p0"][1], s["p1"][1]), min(s["p0"][0], s["p1"][0]),
                     max(s["p0"][0], s["p1"][0]), L))
    cand.sort()
    out = []
    for y, x0, x1, L in cand:
        if out and abs(out[-1]["y"] - y) <= tol:
            out[-1]["x0"] = min(out[-1]["x0"], x0)
            out[-1]["x1"] = max(out[-1]["x1"], x1)
            out[-1]["n"] += 1
            continue
        out.append({"y": y, "x0": x0, "x1": x1, "n": 1})
    return out


def _shift_below(ex, y_cut, dh):
    segs = []
    for s in ex.segments:
        t = dict(s)
        t["src"] = [s["i"]]
        p0 = list(s["p0"]); p1 = list(s["p1"])
        if p0[1] > y_cut + 1e-9:
            p0[1] += dh
        if p1[1] > y_cut + 1e-9:
            p1[1] += dh
        t["p0"] = (p0[0], p0[1]); t["p1"] = (p1[0], p1[1])
        t["len"] = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
        segs.append(t)
    texts = []
    for tx in ex.texts:
        u = dict(tx)
        if u["cy"] > y_cut + 1e-9:
            bb = u["bbox"]
            u["bbox"] = [bb[0], bb[1] + dh, bb[2], bb[3] + dh]
            u["cy"] = u["cy"] + dh
        texts.append(u)
    return segs, texts


def _pick_gap(ex, key, cf_id, min_rows=4):
    rs = rulings(ex)
    if len(rs) < min_rows:
        raise CF.CFNotApplicable(f"only {len(rs)} horizontal rules (need >= {min_rows})")
    S = float(ex.char_scale.get("S") or 1.0)
    gaps = [(rs[i + 1]["y"] - rs[i]["y"], i) for i in range(len(rs) - 1)]
    gaps = [(g, i) for g, i in gaps if g >= max(1.5 * S, 4.0)]
    if not gaps:
        raise CF.CFNotApplicable("no row gap wider than max(1.5*S, 4 pt)")
    gaps.sort()
    g, i = gaps[len(gaps) // 2]           # median gap: a typical row
    return rs, i, g, S


def row_insert(ex, key, shift=True):
    cf_id = "NR2_row_insert_shift" if shift else "NR1_row_insert_nogap"
    rs, i, g, S = _pick_gap(ex, key, cf_id)
    y_top, y_bot = rs[i]["y"], rs[i + 1]["y"]
    x0 = min(rs[i]["x0"], rs[i + 1]["x0"])
    x1 = max(rs[i]["x1"], rs[i + 1]["x1"])
    style = (ex.segments[0].get("w"), ex.segments[0].get("color"), ex.segments[0].get("fill"))
    for s in ex.segments:
        if abs(s["len"] - (rs[i]["x1"] - rs[i]["x0"])) < 1.0:
            style = (s.get("w"), s.get("color"), s.get("fill"))
            break
    if shift:
        segs, texts = _shift_below(ex, y_bot - 1e-6, g)
        y_new = y_bot
    else:
        segs = [dict(s) | {"src": [s["i"]]} for s in ex.segments]
        texts = [dict(t) for t in ex.texts]
        y_new = (y_top + y_bot) / 2.0
    line = CF._mk_seg((x0, y_new), (x1, y_new), style, src=[], tag="NR_row_rule")
    segs.append(line)
    CF._renumber(segs)
    # a row is not just a rule: carry the cell texts of the source row down with it
    added_txt = 0
    for t in ex.texts:
        if y_top < t["cy"] < y_bot:
            u = dict(t)
            bb = u["bbox"]
            u["bbox"] = [bb[0], bb[1] + g, bb[2], bb[3] + g]
            u["cy"] = u["cy"] + g
            u["text"] = "НОВАЯ " + str(u["text"])
            texts.append(u)
            added_txt += 1
    ex2 = CF._clone(ex, segments=segs, texts=texts, prov={"cf": cf_id})
    man = {"cf_class": "N", "cf_id": cf_id,
           "params": {"row_gap_pt": round(g, 3), "n_rules": len(rs), "shift": shift,
                      "rule_len_pt": round(x1 - x0, 3), "texts_added": added_txt},
           "change_bbox_pt": [round(v, 3) for v in
                              ([x0, y_top, x1, _frame(ex)[3]] if shift else [x0, y_new, x1, y_new])],
           "expected_verdict": "GRAPHIC_CHANGE",
           "n_seg_before": len(ex.segments), "n_seg_after": len(segs)}
    return ex2, man


def row_delete(ex, key, shift=True):
    cf_id = "NR3_row_delete_shift" if shift else "NR4_row_delete_nogap"
    rs, i, g, S = _pick_gap(ex, key, cf_id, min_rows=5)
    if i == 0:
        i = 1
    y_del = rs[i]["y"]
    keep = []
    removed = 0
    for s in ex.segments:
        dy = abs(s["p1"][1] - s["p0"][1])
        yy = (s["p0"][1] + s["p1"][1]) / 2
        if dy < 0.5 and abs(yy - y_del) <= 0.6 and s["len"] >= 0.4 * (rs[i]["x1"] - rs[i]["x0"]):
            removed += 1
            continue
        keep.append(dict(s) | {"src": [s["i"]]})
    texts = [dict(t) for t in ex.texts if not (rs[i - 1]["y"] < t["cy"] < y_del)]
    ex2 = CF._clone(ex, segments=CF._renumber(keep), texts=texts, prov={"cf": cf_id})
    if shift:
        dh = -(y_del - rs[i - 1]["y"])
        segs, texts2 = _shift_below(ex2, y_del - 1e-6, dh)
        ex2 = CF._clone(ex, segments=CF._renumber(segs), texts=texts2, prov={"cf": cf_id})
    man = {"cf_class": "N", "cf_id": cf_id,
           "params": {"rules_removed": removed, "n_rules": len(rs), "shift": shift,
                      "row_h_pt": round(y_del - rs[i - 1]["y"], 3)},
           "change_bbox_pt": [round(rs[i]["x0"], 3), round(rs[i - 1]["y"], 3),
                              round(rs[i]["x1"], 3), round(_frame(ex)[3], 3)],
           "expected_verdict": "GRAPHIC_CHANGE",
           "n_seg_before": len(ex.segments), "n_seg_after": len(ex2.segments)}
    return ex2, man


VARIANTS = [("NR1_row_insert_nogap", {"shift": False}),
            ("NR2_row_insert_shift", {"shift": True}),
            ("NR3_row_delete_shift", {"shift": True})]


def apply(ex, cf_id, key, **p):
    if cf_id.startswith("NR1") or cf_id.startswith("NR2"):
        return row_insert(ex, key, shift=p.get("shift", True))
    if cf_id.startswith("NR3") or cf_id.startswith("NR4"):
        return row_delete(ex, key, shift=p.get("shift", True))
    raise ValueError(cf_id)
