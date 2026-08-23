# -*- coding: utf-8 -*-
"""Shared helpers for the `rel` probe (relations between graphical objects, VECTOR 0.3).

Reuses grp_common for corpus access and the class-A rewrites, and grp_match for the
ink correspondence between two real sides.  Adds:
  * object correspondence via SEGMENT PROVENANCE (exact, for counterfactuals),
  * object correspondence via ink overlap (for real pairs),
  * relation survival accounting that separates "the relation died" from
    "one of its endpoints died".
"""
from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import grp_match as M           # noqa: E402
import rel_relations as R       # noqa: E402
import v03_foundation as F      # noqa: E402
import v03_objects as O         # noqa: E402

ART = G.ART
ROOT = G.ROOT


# ------------------------------------------------------------------ layer + relations

def layer_and_rels(ex, *, S_override=None, obj_params=None, rel_params=None):
    op = dict(obj_params or {})
    if S_override:
        op["S_override"] = S_override
    layer = O.build_objects(ex, **op)
    rels = R.build_relations(layer, ex, **(rel_params or {}))
    return layer, rels


# ------------------------------------------------------------------ correspondence

def match_by_provenance(layer_a, segs_a, layer_b, segs_b):
    """Exact object correspondence when side B was produced from side A by a rewrite
    that stamped ``src`` (the originating segment ids) on every segment.

    Returns (a2b, b2a, overlap) where overlap[(ia,ib)] = shared ink length.
    """
    src_of = []
    for s in segs_b:
        src_of.append(s.get("src") or [s["i"]])
    len_a = {s["i"]: s["len"] for s in segs_a}
    ia_of_seg = {}
    for ia, o in enumerate(layer_a.objects):
        for g in o["segments"]:
            ia_of_seg[segs_a[g]["i"]] = ia
    ov: dict[tuple[int, int], float] = {}
    for ib, o in enumerate(layer_b.objects):
        for g in o["segments"]:
            for si in src_of[g]:
                ia = ia_of_seg.get(si)
                if ia is None:
                    continue
                w = len_a.get(si, 0.0) / max(len(src_of[g]), 1)
                ov[(ia, ib)] = ov.get((ia, ib), 0.0) + w
    return _greedy(ov, len(layer_a.objects), len(layer_b.objects))


def match_by_ink(layer_a, segs_a, layer_b, segs_b, off, tol=0.8):
    """Object correspondence for two REAL sides: segment-level nearest ink under a
    measured translation, aggregated to objects.  Same machinery grp used for churn."""
    idx = M.build_index(segs_b, off=(0.0, 0.0))
    obj_of_b = [-1] * len(segs_b)
    for ib, o in enumerate(layer_b.objects):
        for g in o["segments"]:
            obj_of_b[g] = ib
    ov: dict[tuple[int, int], float] = {}
    for ia, o in enumerate(layer_a.objects):
        for g in o["segments"]:
            s = segs_a[g]
            x0, y0 = s["p0"][0] + off[0], s["p0"][1] + off[1]
            x1, y1 = s["p1"][0] + off[0], s["p1"][1] + off[1]
            ang = math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0
            hit = None
            for t in (0.5,):
                px, py = x0 + t * (x1 - x0), y0 + t * (y1 - y0)
                hit = M.query(idx, px, py, ang, tol)
                if hit is not None:
                    break
            if hit is None:
                continue
            ib = obj_of_b[hit]
            if ib < 0:
                continue
            ov[(ia, ib)] = ov.get((ia, ib), 0.0) + s["len"]
    return _greedy(ov, len(layer_a.objects), len(layer_b.objects))


def _greedy(ov, na, nb):
    a2b = [-1] * na
    b2a = [-1] * nb
    for (ia, ib), w in sorted(ov.items(), key=lambda kv: -kv[1]):
        if a2b[ia] == -1 and b2a[ib] == -1:
            a2b[ia], b2a[ib] = ib, ia
    return a2b, b2a, ov


# ------------------------------------------------------------------ relation survival

def survival(rels_a, rels_b, a2b, layer_a, layer_b, texts_a=None, texts_b=None):
    """For every relation on A: did it survive on B?

    Two denominators, always reported together (gate-fix lesson: precision alone lies):
      * raw   — over ALL relations of the type on A;
      * cond  — over relations whose BOTH endpoints found a partner on B.
    """
    keyed_b: set = set()
    for r in rels_b:
        keyed_b.add(_key_b(r))
    out: dict[str, dict] = {}
    for r in rels_a:
        t = r["type"]
        d = out.setdefault(t, {"n": 0, "endpoints_ok": 0, "survived": 0,
                               "survived_of_endpoints_ok": 0})
        d["n"] += 1
        a = a2b[r["a"]] if r["a"] is not None and r["a"] < len(a2b) else -1
        if t == "LABEL_ANCHOR":
            ok = a >= 0
            k = ("LABEL_ANCHOR", a, (r.get("text") or "").strip())
        else:
            b = a2b[r["b"]] if r["b"] is not None and r["b"] < len(a2b) else -1
            ok = a >= 0 and b >= 0
            if r.get("sym"):
                k = (t, min(a, b), max(a, b))
            else:
                k = (t, a, b)
        if ok:
            d["endpoints_ok"] += 1
        alive = ok and k in keyed_b
        if alive:
            d["survived"] += 1
            d["survived_of_endpoints_ok"] += 1
    return out


def _key_b(r):
    t = r["type"]
    if t == "LABEL_ANCHOR":
        return (t, r["a"], (r.get("text") or "").strip())
    a, b = r["a"], r["b"]
    if r.get("sym"):
        a, b = min(a, b), max(a, b)
    return (t, a, b)


# ------------------------------------------------------------------ misc

def pct(vals, q):
    return G.pct(vals, q)


def dump(name, obj):
    p = ART / name
    json.dump(obj, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return str(p)
