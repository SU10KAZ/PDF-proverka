# -*- coding: utf-8 -*-
"""`ldg` — the minimal GraphicChangeLedger (BRIEF §23) and the expert phrases (§24).

Nothing here finds a change: the change is found by `loc_common.ledger()`, which keys
the record on UNMATCHED INK (loc L18: an object-to-object ledger emits a median of 62
false records on a rewrite that changed nothing).  This module only

  * projects a record into the small on-the-wire contract,
  * attaches GEOMETRIC evidence to it (which existing ink the new ink touches, at what
    angle, how far it travelled),
  * refines the record type where the evidence earns a narrower name,
  * turns records into the Russian phrases of §24 — and every phrase is a claim whose
    precision is measured, not asserted.

BRIEF §7: a text change is NOT evidence of a graphic change.  No branch of this module
reads a string; `label` travels through the record as an ADDRESS only and is never a
reason for a record to exist.  `validate()` proves that on data.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import loc_common as L        # noqa: E402
import grp_match as M         # noqa: E402

# evidence kinds that are allowed to justify a record; `label` is deliberately absent
GEOMETRIC_EVIDENCE = {"ink_lost", "ink_new", "translation", "attachment",
                      "contact", "frame"}
TEXT_EVIDENCE = {"label"}

# the record types of the contract.  A type is in the contract only if the track's data
# produced it (probe report ldg_FINDINGS, table L1).
TYPES = ("REMOVED_OBJECT", "ADDED_OBJECT", "MOVED_OBJECT", "CHANGED_OBJECT")

DEFAULTS = {
    "attach_r_S": 0.6,       # an end 'touches' ink within max(attach_r_pt, attach_r_S*S)
    "attach_r_pt": 1.0,
    "straight_ratio": 0.85,  # ink length / bbox diagonal of the run
    "collinear_cos": 0.985,  # |cos| between the new run and the ink it lands on
    "branch_min_deg": 20.0,
    "same_shape_thr": 0.70,  # two added regions are one 'family' if each is a translated
                             # copy of the other to this share of its ink
    "weld_share_thr": 0.50,  # ink is a CONNECTOR (branch / bridge / closed gap), not an
                             # object, when it is one straight run welded into surviving
                             # ink by at least this share of its endpoints.  Measured:
                             # C8/C9/C10 always >=0.5, 74 % of true added objects = 0.0
}


# --------------------------------------------------------------------- geometry help

def _dir(s):
    d = max(s["len"], 1e-9)
    return ((s["p1"][0] - s["p0"][0]) / d, (s["p1"][1] - s["p0"][1]) / d)


def _shift(s, off):
    return {**s, "p0": (s["p0"][0] + off[0], s["p0"][1] + off[1]),
            "p1": (s["p1"][0] + off[0], s["p1"][1] + off[1])}


def _run_geometry(segs):
    """Is this pile of segments one straight run?  Returns (is_run, end0, end1, u)."""
    if not segs:
        return False, None, None, None
    tot = sum(s["len"] for s in segs)
    pts = []
    for s in segs:
        pts += [s["p0"], s["p1"]]
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    diag = math.hypot(max(xs) - min(xs), max(ys) - min(ys))
    if diag <= 1e-9:
        return False, None, None, None
    # principal direction = direction of the longest segment
    lg = max(segs, key=lambda s: s["len"])
    u = _dir(lg)
    # every segment must be near-parallel to it
    for s in segs:
        v = _dir(s)
        if abs(u[0] * v[0] + u[1] * v[1]) < 0.98 and s["len"] > 0.15 * tot:
            return False, None, None, None
    if tot / diag < DEFAULTS["straight_ratio"]:
        return False, None, None, None
    ts = [(p[0] * u[0] + p[1] * u[1], p) for p in pts]
    ts.sort()
    return True, ts[0][1], ts[-1][1], u


def _index_of(segs, off=(0.0, 0.0)):
    return M.build_index(segs, off)


def _nearest(index, px, py, r):
    """Nearest segment to a point within r, ANY direction.  Returns (dist, data row)."""
    grid, data, cell = index
    gx, gy = int(math.floor(px / cell)), int(math.floor(py / cell))
    rad = int(math.ceil(r / cell))
    best = None
    for dx in range(-rad, rad + 1):
        for dy in range(-rad, rad + 1):
            for k in grid.get((gx + dx, gy + dy), ()):  # noqa: E501
                x0, y0, x1, y1, ang, i = data[k]
                d2 = M._pt_seg_dist2(px, py, x0, y0, x1, y1)
                if best is None or d2 < best[0]:
                    best = (d2, data[k])
    if best is None or best[0] > r * r:
        return None
    return math.sqrt(best[0]), best[1]


def _contact(segs, host_index, S, p):
    """How much of this ink is WELDED into ink that was already there?

    Free-standing new ink (a symbol dropped into an empty spot) touches nothing; ink that
    was welded in at both ends (a branch, a bridge, a closed opening) is not a new
    object, whatever its length.  Measured over segment endpoints, in PDF points.
    """
    if not segs or host_index is None:
        return None
    r = max(p["attach_r_pt"], p["attach_r_S"] * S)
    pts = []
    for s in segs:
        pts += [s["p0"], s["p1"]]
    # only the endpoints of the run as a whole matter; interior endpoints of a polyline
    # touch each other, not the host - but the host index holds ONLY unchanged ink, so
    # every hit below is a hit on pre-existing geometry
    touch = sum(1 for q in pts if _nearest(host_index, q[0], q[1], r) is not None)
    return {"n_endpoints": len(pts), "n_touching": touch,
            "share_touching": round(touch / max(len(pts), 1), 4),
            "radius_pt": round(r, 3)}


def _attachment(run_segs, host_index, S, p):
    """Where does a straight run of NEW ink land on the ink that was already there?"""
    ok, e0, e1, u = _run_geometry(run_segs)
    if not ok:
        return None
    r = max(p["attach_r_pt"], p["attach_r_S"] * S)
    ends = []
    for e in (e0, e1):
        hit = _nearest(host_index, e[0], e[1], r)
        if hit is None:
            ends.append(None)
            continue
        d, row = hit
        ha = math.radians(row[4])
        hv = (math.cos(ha), math.sin(ha))
        cos = abs(u[0] * hv[0] + u[1] * hv[1])
        ends.append({"dist_pt": round(d, 3), "host_seg": row[5],
                     "cos": round(cos, 4),
                     "angle_deg": round(math.degrees(math.acos(min(1.0, cos))), 1)})
    n_att = sum(1 for e in ends if e)
    length = math.hypot(e1[0] - e0[0], e1[1] - e0[1])
    return {"attached_ends": n_att, "ends": ends, "run_len_pt": round(length, 3),
            "run_over_S": round(length / max(S, 1e-9), 2)}


# --------------------------------------------------------------------- the ledger

def raw_ledger(exA, exB, *, off=(0.0, 0.0), LA=None, LB=None, meta=None):
    """The ink-keyed ledger of `loc`, with the segment indices kept as evidence."""
    return L.ledger(exA, exB, off=off, LA=LA, LB=LB, meta=meta,
                    params={"keep_seg_ix": True})


def build(exA, exB, *, off=(0.0, 0.0), LA=None, LB=None, meta=None,
          params=None, min_change_len_pt=None, drop_boundary=True, led=None):
    """Minimal GraphicChangeLedger for one pair of prepared graphic blocks."""
    p = dict(DEFAULTS)
    p.update(params or {})
    if led is None:
        led = raw_ledger(exA, exB, off=off, LA=LA, LB=LB, meta=meta)
    S = led["S"]
    L_min = min_change_len_pt if min_change_len_pt is not None else max(2.0 * S, 3.0)

    recs = [r for r in led["records"] if r["change_len"] >= L_min]
    if drop_boundary:
        recs = [r for r in recs if not r["at_boundary"]]

    # ink that did NOT change is the host: an attachment must land on ink that exists on
    # both sides, not on the new ink itself
    changed_a = set()
    changed_b = set()
    for r in led["records"]:
        changed_a.update(r.get("seg_ix_a") or [])
        changed_b.update(r.get("seg_ix_b") or [])
    host = [exA.segments[k] for k in range(len(exA.segments)) if k not in changed_a]
    host_index = _index_of(host) if host else None

    changes = []
    for r in recs:
        segs_a = [exA.segments[k] for k in (r.get("seg_ix_a") or [])]
        segs_b = [_shift(exB.segments[k], off) for k in (r.get("seg_ix_b") or [])]
        ev = []
        if r["len_lost"] > 0:
            ev.append({"kind": "ink_lost", "len_pt": round(r["len_lost"], 2),
                       "n_seg": r["n_seg_lost"]})
        if r["len_new"] > 0:
            ev.append({"kind": "ink_new", "len_pt": round(r["len_new"], 2),
                       "n_seg": r["n_seg_new"]})
        if r["type"] == "MOVED_OBJECT" and "dx_pt" in r:
            ev.append({"kind": "translation", "dx_pt": r["dx_pt"], "dy_pt": r["dy_pt"],
                       "share": r.get("move_share"),
                       "dist_pt": round(math.hypot(r["dx_pt"], r["dy_pt"]), 3)})
        att = None
        if host_index is not None and segs_b and not segs_a:
            att = _attachment(segs_b, host_index, S, p)
        elif host_index is not None and segs_a and not segs_b:
            att = _attachment(segs_a, host_index, S, p)
        if att:
            ev.append({"kind": "attachment", **att})
        con = _contact(segs_b or segs_a, host_index, S, p)
        if con:
            ev.append({"kind": "contact", **con})
        ev.append({"kind": "frame", "at_boundary": bool(r["at_boundary"]),
                   "bbox_pt": r["bbox_pt"]})

        rec_type = r["type"]
        # a single straight run welded into ink that stayed is a CONNECTOR, not an object
        welded = bool(att and att["attached_ends"] >= 1 and con and
                      con["share_touching"] >= p["weld_share_thr"])
        shape = None
        if att and att["attached_ends"] == 2 and all(
                e and e["cos"] >= p["collinear_cos"] for e in att["ends"]):
            shape = "GAP_CLOSED" if r["len_new"] > 0 else "GAP_OPENED"
        elif att and att["attached_ends"] == 1:
            e = [x for x in att["ends"] if x][0]
            if e["angle_deg"] >= p["branch_min_deg"]:
                shape = "BRANCH_ADDED" if r["len_new"] > 0 else "BRANCH_REMOVED"

        obj_before = (r["objects_a"] or [None])[0]
        obj_after = (r["objects_b"] or [None])[0]
        ch = {"type": rec_type,
              "welded": welded,
              "object_before": _obj(obj_before),
              "object_after": _obj(obj_after),
              "evidence": ev}
        if shape:
            ch["shape"] = shape
        changes.append({**ch, "_len": r["change_len"], "_bbox": r["bbox_pt"],
                        "_segs_b": segs_b, "_segs_a": segs_a})

    # two ADDED records that are translated copies of each other = one family
    _mark_same_shape(changes, p)

    out = {"changes": [{k: v for k, v in c.items() if not k.startswith("_")}
                       for c in changes],
           "_full": changes,
           "S": S, "L_min_pt": round(L_min, 3),
           "n_records_raw": led["n_records"],
           "scalar": led["scalar"], "counts": led["counts"]}
    return out


def _obj(o):
    if not o:
        return None
    return {"object_id": o["object_id"], "cls": o["cls"],
            "bbox_pt": [round(v, 2) for v in o["bbox_pt"]] if o.get("bbox_pt") else None,
            "label": o.get("label"), "share": o.get("share_of_object")}


def _mark_same_shape(changes, p):
    add = [c for c in changes if c["type"] == "ADDED_OBJECT" and c["_segs_b"]]
    n = len(add)
    if n < 2:
        return
    groups = list(range(n))

    def find(a):
        while groups[a] != a:
            groups[a] = groups[groups[a]]
            a = groups[a]
        return a

    for i in range(n):
        for j in range(i + 1, n):
            if find(i) == find(j):
                continue
            s1, _, _ = L._translation_fit(add[i]["_segs_b"], add[j]["_segs_b"], 0.8)
            s2, _, _ = L._translation_fit(add[j]["_segs_b"], add[i]["_segs_b"], 0.8)
            if min(s1, s2) >= p["same_shape_thr"]:
                groups[find(j)] = find(i)
    for i, c in enumerate(add):
        c["_family"] = find(i)
        c["family"] = find(i)


# --------------------------------------------------------------------- validation §7

def validate(ldg):
    """Every record must rest on GEOMETRIC evidence.  Returns a list of violations."""
    bad = []
    for i, c in enumerate(ldg["changes"]):
        kinds = {e["kind"] for e in c["evidence"]}
        ink = [e for e in c["evidence"] if e["kind"] in ("ink_lost", "ink_new")]
        if not (kinds & GEOMETRIC_EVIDENCE):
            bad.append({"i": i, "why": "no geometric evidence"})
        elif not ink or max(e["len_pt"] for e in ink) <= 0:
            bad.append({"i": i, "why": "no ink evidence"})
        if kinds & TEXT_EVIDENCE:
            bad.append({"i": i, "why": "text used as evidence"})
    return bad


# --------------------------------------------------------------------- §24 phrases

def phrases(ldg):
    """Russian phrases for the expert, each with the records that hold it up."""
    full = ldg["_full"]
    out = []
    # only shapes that a counterfactual of this track can make TRUE are allowed to speak:
    # GAP_OPENED and BRANCH_REMOVED fired 12 times and were wrong 12 times (no
    # counterfactual removes a branch or opens a gap), so they carry no phrase and fall
    # through to the weak one.  "There is nothing to check it on" -> it does not speak.
    gap_closed = [c for c in full if c.get("shape") == "GAP_CLOSED"]
    branch_add = [c for c in full if c.get("shape") == "BRANCH_ADDED"]
    gap_opened, branch_rem = [], []
    # ink that was welded into geometry that stayed is NOT a new/removed OBJECT; it is a
    # change of configuration.  The guard is measured (ldg_FINDINGS, L3): without it the
    # sentence "a graphic object was added" is false in 64 % of its uses.
    removed = [c for c in full if c["type"] == "REMOVED_OBJECT" and not c["welded"]
               and c.get("shape") not in ("GAP_OPENED", "BRANCH_REMOVED")]
    added = [c for c in full if c["type"] == "ADDED_OBJECT" and not c["welded"]]
    changed = ([c for c in full if c["type"] in ("CHANGED_OBJECT", "MOVED_OBJECT")] +
               [c for c in full if c["type"] in ("ADDED_OBJECT", "REMOVED_OBJECT")
                and (c["welded"] or c.get("shape") in ("GAP_OPENED", "BRANCH_REMOVED"))
                and c.get("shape") not in ("GAP_CLOSED", "BRANCH_ADDED")])

    if gap_closed:
        out.append(_ph("OPENING_REMOVED", "Удалён проём." if len(gap_closed) == 1
                       else f"Удалено проёмов: {len(gap_closed)}.", gap_closed,
                       safe="Разрыв в линии закрыт: линия стала непрерывной "
                            f"({len(gap_closed)} шт.)."))
    if gap_opened:
        out.append(_ph("OPENING_ADDED", "Добавлен проём." if len(gap_opened) == 1
                       else f"Добавлено проёмов: {len(gap_opened)}.", gap_opened))
    if branch_add:
        n = len(branch_add)
        word = {1: "одно ответвление", 2: "два ответвления"}.get(n, f"{n} ответвлений")
        out.append(_ph("BRANCH_ADDED", f"Добавлено {word}.", branch_add,
                       safe=f"К существующей линии пристроен новый отрезок ({n} шт.)."))
    if branch_rem:
        n = len(branch_rem)
        word = {1: "одно ответвление", 2: "два ответвления"}.get(n, f"{n} ответвлений")
        out.append(_ph("BRANCH_REMOVED", f"Удалено {word}.", branch_rem))
    fam = {}
    for c in added:
        if "_family" in c:
            fam.setdefault(c["_family"], []).append(c)
    for g in fam.values():
        if len(g) >= 2:
            out.append(_ph("ADDED_SAME_KIND",
                           f"Добавлено {len(g)} однотипных графических объекта."
                           if len(g) < 5 else
                           f"Добавлено {len(g)} однотипных графических объектов.", g))
    plain_add = [c for c in added if not c.get("shape")
                 and len(fam.get(c.get("_family", -1), [])) < 2]
    if plain_add:
        out.append(_ph("OBJECT_ADDED",
                       "Добавлен графический объект." if len(plain_add) == 1
                       else f"Добавлено графических объектов: {len(plain_add)}.", plain_add))
    plain_rem = [c for c in removed if not c.get("shape")]
    if plain_rem:
        out.append(_ph("OBJECT_REMOVED",
                       "Удалён графический объект." if len(plain_rem) == 1
                       else f"Удалено графических объектов: {len(plain_rem)}.", plain_rem))
    if changed:
        out.append(_ph("CONFIG_CHANGED",
                       "Изменена конфигурация элемента." if len(changed) == 1
                       else f"Изменена конфигурация элементов: {len(changed)}.", changed))
    return out


def _ph(pid, text, recs, safe=None):
    return {"id": pid, "text": text, "safe_text": safe or text, "n": len(recs),
            "bboxes": [r["_bbox"] for r in recs[:8]],
            "ink_pt": round(sum(r["_len"] for r in recs), 2),
            "objects": [(r["object_before"] or r["object_after"] or {}).get("object_id")
                        for r in recs[:8]]}
