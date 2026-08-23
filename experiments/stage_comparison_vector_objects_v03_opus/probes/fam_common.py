# -*- coding: utf-8 -*-
"""Shared helpers for the `fam` probe (family identity, VECTOR 0.3)."""
from __future__ import annotations
import math, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G          # noqa: E402
import fam_family as FAM        # noqa: E402

ART = G.ART


def ink_family_labels(layer, segs, famlayer, n_base: int, offset: int = 0):
    """Family label of every ORIGINAL segment id, via the `src` manifest.

    A rewritten segment carries `src` = the original ids it came from; where one
    original id is covered by several rewritten segments we take the family that
    owns the largest share of its ink.  Segments with no owner get -1.
    """
    acc: dict[int, dict[int, float]] = {}
    for oi, o in enumerate(layer.objects):
        f = famlayer.obj_family[oi + offset]
        for gi in o["segments"]:
            s = segs[gi]
            src = s.get("src") or [gi]
            w = s["len"] / max(len(src), 1)
            for si in src:
                d = acc.setdefault(si, {})
                d[f] = d.get(f, 0.0) + w
    out = [-1] * n_base
    for si, d in acc.items():
        if 0 <= si < n_base:
            out[si] = max(d.items(), key=lambda kv: (kv[1], -kv[0]))[0]
    return out


def counts_by_family(famlayer, side):
    return [famlayer.cardinality(i, side) for i in range(len(famlayer.families))]


def pct(vals, q):
    return G.pct(vals, q)


def summarize(vals):
    if not vals:
        return None
    return {"n": len(vals), "median": round(statistics.median(vals), 5),
            "mean": round(statistics.mean(vals), 5),
            "p10": round(G.pct(vals, 0.10), 5), "p90": round(G.pct(vals, 0.90), 5),
            "min": round(min(vals), 5), "max": round(max(vals), 5)}


def border_flags(layer, clip, pad_frac=0.02):
    w, h = clip[2] - clip[0], clip[3] - clip[1]
    px, py = max(2.0, pad_frac * w), max(2.0, pad_frac * h)
    for o in layer.objects:
        b = o["bbox"]
        o["border"] = (b[0] <= clip[0] + px or b[1] <= clip[1] + py or
                       b[2] >= clip[2] - px or b[3] >= clip[3] - py)


# ---------------------------------------------------------------------------
# STRICT class-A rewrites.
#
# DEFECT FOUND IN A NEIGHBOUR PROBE'S MODULE (reported, not silently patched):
# `grp_common._closed_circles(min_pts=5)` accepts ANY closed chain of >= 4
# segments whose points fit a circle -- and the four corners of a rectangle fit
# a circle exactly.  So `grp_common.REWRITES["A4*"]` turns rectangles into
# circles: the rewrite is NOT ink preserving and every downstream "class A must
# not change anything" measurement built on it is contaminated.  The `cf` probe
# found the same defect independently and built a strict detector inside
# `v03_counterfactual`; we reuse THAT one rather than writing a third.
# `fam_contamination.json` measures how much of the corpus is affected.
# ---------------------------------------------------------------------------
def _strict_rewrites():
    import v03_counterfactual as CF
    out = dict(G.REWRITES)
    for k, mk in CF._STRICT_A4.items():
        fn = mk()

        def wrap(f=fn):
            def g(segs, rng):
                try:
                    return f(segs, rng)
                except CF.CFNotApplicable:
                    return G.REWRITES["A0_identity"](segs, rng)
            return g
        out[k] = wrap()
    return out


REWRITES = _strict_rewrites()
