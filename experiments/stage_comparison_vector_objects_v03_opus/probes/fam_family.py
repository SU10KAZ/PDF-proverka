# -*- coding: utf-8 -*-
"""fam — FAMILY layer on top of the v0.3 object layer.

A *family* is a maximal set of graphical objects of one block (or of a pair of
blocks) that a human would call "the same element, drawn N times": 12 sockets,
14 dampers, 5 axis bubbles.  The whole point of the layer is the sentence
"12 -> 14" (BRIEF section 14), and the whole risk is that the sentence is false.

Design constraints, all of them taken from measured failures of v0.1 / v0.2:

* **no exact hash.**  v0.2 P9: a 0.24 pt (2.8 %) CAD export rounding split one
  physically identical symbol into three motifs; OBJ-11: an exact motif hash
  survives 4-8 classes out of ~50 between exports.  So the signature is a
  tolerant neighbourhood in a continuous descriptor, never a key.
* **deterministic two-pass, never greedy.**  v0.2 P19: greedy leader clustering
  produced 50 changed clusters on a byte-identical PDF, the two-pass produced 0.
  Pass 1 seeds (in an order derived from CONTENT, not from input order), pass 2
  re-assigns every object to the nearest seed centroid and merges centroids.
  Pass 2 does not depend on the order of objects at all.
* **size is a gate, not a coordinate.**  Two circles of different radius are not
  the same family; the 25-number descriptor is scale free, so absolute size (PDF
  points, per fnd/mine M4 - never block fractions) is a separate hard gate.
* **families are built JOINTLY for a compared pair.**  Clustering each side alone
  and matching families afterwards re-introduces the identity problem the
  descriptor cannot solve (grp G7: top-1 by descriptor 0.700).  Pooling both
  sides into one clustering makes "12 -> 14" a count inside ONE family.

Public API
----------
    build_families(layer, **params)                  -> FamilyLayer  (one block)
    build_families_pair(layer_a, layer_b, **params)  -> FamilyLayer  (pooled)
    family_deltas(famlayer)                          -> list of "N -> M" rows
    ari(labels_a, labels_b)                          -> adjusted Rand index
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

FAM_DEFAULTS: dict[str, Any] = {
    # tolerant neighbourhood in the 25-number shape descriptor (L1, range ~2)
    "eps_desc": 0.25,
    # absolute size gate: |log(diag_a / diag_b)| <= size_tol  (0.12 ~ +/-12 %)
    "size_tol": 0.12,
    # objects smaller than this many PDF points never form families (noise floor)
    "min_diag_pt": 0.5,
    # class grouping: "coarse" (compact / linear / area), "exact", "none"
    "class_split": "coarse",
    # pass 2: assignment radius, in units of eps_desc
    "assign_slack": 1.0,
    # pass 2 repetitions (2 = seed, reassign, recentre, reassign)
    "passes": 2,
    # merge two centroids closer than merge_frac * eps_desc
    "merge_frac": 1.0,
    # a family is "repeated" from this cardinality up
    "min_family": 2,
    # clustering mode: "twopass" (default) or "greedy" (the v0.2 failure, kept
    # so the difference can be measured rather than asserted)
    "mode": "twopass",
}

_CLASS_COARSE = {"symbol": "compact", "stray": "compact", "composite": "compact",
                 "linear": "linear", "area": "area"}


def _class_key(cls: str, mode: str) -> str:
    if mode == "none":
        return ""
    if mode == "exact":
        return cls
    return _CLASS_COARSE.get(cls, cls)


def _l1(a: Sequence[float], b: Sequence[float]) -> float:
    return sum(abs(x - y) for x, y in zip(a, b))


def _obj_vec(o: dict) -> list[float]:
    d = o.get("desc")
    if isinstance(d, dict):
        return d["vec"]
    return d


def _obj_diag(o: dict) -> float:
    d = o.get("desc")
    if isinstance(d, dict):
        return float(d["diag"])
    return float(o.get("diag", 0.0))


@dataclass
class FamilyLayer:
    families: list[dict]
    obj_family: list[int]          # object index -> family index
    params: dict
    stats: dict
    sides: Optional[list[int]] = None   # per-object side id (0/1) when pooled
    objects: list[dict] = field(default_factory=list)

    def cardinality(self, fi: int, side: Optional[int] = None) -> int:
        f = self.families[fi]
        if side is None:
            return len(f["members"])
        return sum(1 for m in f["members"] if self.sides[m] == side)

    def to_json(self, top: int = 0) -> dict:
        fams = self.families if not top else sorted(
            self.families, key=lambda f: -len(f["members"]))[:top]
        return {"params": self.params, "stats": self.stats,
                "families": [{k: v for k, v in f.items() if k not in ("vec", "members")}
                             | {"n": len(f["members"])} for f in fams]}


# ---------------------------------------------------------------- clustering

def _bucket(logd: float, size_tol: float) -> int:
    return int(math.floor(logd / max(size_tol, 1e-6)))


def _canonical_key(o: dict, vec: Sequence[float]) -> tuple:
    """Order derived from CONTENT only.  Two exports of the same drawing produce
    (almost) the same order, which is what makes pass 1 reproducible; pass 2 then
    removes the residual order dependence entirely."""
    return (-round(o.get("seg_len", 0.0), 3), round(_obj_diag(o), 3),
            tuple(round(v, 3) for v in vec))


def _cluster(items, p):
    """items: list of (idx, class_key, vec, logdiag).  Returns list of member-lists."""
    eps = p["eps_desc"]
    stol = p["size_tol"]
    order = (list(range(len(items))) if p["mode"] == "greedy_input"
             else sorted(range(len(items)), key=lambda k: items[k][4]))

    # ---- pass 1: leader seeding in canonical (content) order -----------------
    seeds: list[dict] = []
    index: dict[tuple, list[int]] = {}
    for k in order:
        _, ck, vec, logd, _key = items[k]
        b = _bucket(logd, stol)
        best = None
        for bb in (b - 1, b, b + 1):
            for si in index.get((ck, bb), ()):  # candidate seeds only
                s = seeds[si]
                if abs(logd - s["logd"]) > stol:
                    continue
                d = _l1(vec, s["vec"])
                if d <= eps and (best is None or d < best[1]):
                    best = (si, d)
        if best is None:
            seeds.append({"vec": list(vec), "logd": logd, "ck": ck, "members": [k]})
            index.setdefault((ck, b), []).append(len(seeds) - 1)
        else:
            seeds[best[0]]["members"].append(k)

    if p["mode"] in ("greedy", "greedy_input"):
        return [s["members"] for s in seeds]

    # ---- pass 2: recentre, reassign (order independent), merge ---------------
    for _ in range(max(1, int(p["passes"]))):
        _recentre(seeds, items)
        seeds = _merge_centroids(seeds, p)
        seeds = _reassign(seeds, items, p)
    _recentre(seeds, items)
    return [s["members"] for s in seeds if s["members"]]


def _recentre(seeds, items):
    for s in seeds:
        if not s["members"]:
            continue
        vecs = [items[k][2] for k in s["members"]]
        n = len(vecs)
        s["vec"] = [sum(v[j] for v in vecs) / n for j in range(len(vecs[0]))]
        s["logd"] = statistics.median([items[k][3] for k in s["members"]])


def _merge_centroids(seeds, p):
    eps = p["eps_desc"] * p["merge_frac"]
    stol = p["size_tol"]
    live = [s for s in seeds if s["members"]]
    # deterministic: biggest first, then canonical centroid
    order = sorted(range(len(live)),
                   key=lambda i: (-len(live[i]["members"]), live[i]["ck"],
                                  round(live[i]["logd"], 4),
                                  tuple(round(v, 4) for v in live[i]["vec"])))
    kept: list[dict] = []
    kidx: dict[tuple, list[int]] = {}
    for i in order:
        s = live[i]
        b = _bucket(s["logd"], stol)
        tgt = None
        for bb in (b - 1, b, b + 1):
            for ki in kidx.get((s["ck"], bb), ()):
                t = kept[ki]
                if abs(s["logd"] - t["logd"]) > stol:
                    continue
                d = _l1(s["vec"], t["vec"])
                if d <= eps and (tgt is None or d < tgt[1]):
                    tgt = (ki, d)
        if tgt is None:
            kept.append({"vec": list(s["vec"]), "logd": s["logd"], "ck": s["ck"],
                         "members": list(s["members"])})
            kidx.setdefault((s["ck"], b), []).append(len(kept) - 1)
        else:
            kept[tgt[0]]["members"].extend(s["members"])
    return kept


def _reassign(seeds, items, p):
    """Every object goes to the nearest centroid within the assignment radius.
    Depends only on the centroids, therefore not on the order of the objects."""
    r = p["eps_desc"] * p["assign_slack"]
    stol = p["size_tol"]
    idx: dict[tuple, list[int]] = {}
    for si, s in enumerate(seeds):
        idx.setdefault((s["ck"], _bucket(s["logd"], stol)), []).append(si)
    new = [{"vec": list(s["vec"]), "logd": s["logd"], "ck": s["ck"], "members": []}
           for s in seeds]
    orphan: list[int] = []
    for k, (_, ck, vec, logd, _key) in enumerate(items):
        b = _bucket(logd, stol)
        best = None
        for bb in (b - 1, b, b + 1):
            for si in idx.get((ck, bb), ()):
                s = seeds[si]
                if abs(logd - s["logd"]) > stol:
                    continue
                d = _l1(vec, s["vec"])
                if d <= r and (best is None or d < best[1] or
                               (d == best[1] and si < best[0])):
                    best = (si, d)
        if best is None:
            orphan.append(k)
        else:
            new[best[0]]["members"].append(k)
    new = [s for s in new if s["members"]]
    # orphans: seed fresh clusters among themselves, in canonical order
    if orphan:
        sub = [items[k] for k in orphan]
        pp = dict(p)
        pp["mode"] = "greedy"
        for members in _cluster(sub, pp):
            ks = [orphan[m] for m in members]
            vecs = [items[k][2] for k in ks]
            n = len(vecs)
            new.append({"vec": [sum(v[j] for v in vecs) / n for j in range(len(vecs[0]))],
                        "logd": statistics.median([items[k][3] for k in ks]),
                        "ck": items[ks[0]][1], "members": ks})
    return new


# ---------------------------------------------------------------- public

def _items_from(objects, p, side_of=None):
    """Eligible objects only.  An object smaller than `min_diag_pt` PDF points is
    below the noise floor of the corpus and is NOT allowed into any family: measured
    in F3, a family of 82 members with a median diagonal of 0.17 pt produced the
    single largest batch of false "the count changed" rows in the whole probe.
    Excluded objects keep family -1 and are invisible to `family_deltas`."""
    floor = float(p.get("min_diag_pt") or 0.0)
    out = []
    for i, o in enumerate(objects):
        diag = max(_obj_diag(o), 1e-6)
        if diag < floor:
            continue
        vec = _obj_vec(o)
        ck = _class_key(o["cls"], p["class_split"])
        out.append((i, ck, vec, math.log(diag), None))
    # canonical key last (needs vec)
    return [(t[0], t[1], t[2], t[3], _canonical_key(objects[t[0]], t[2])) for t in out]


def _finalise(objects, groups, p, sides=None):
    families = []
    obj_family = [-1] * len(objects)
    for members in groups:
        members = sorted(members)
        vecs = [_obj_vec(objects[m]) for m in members]
        n = len(vecs)
        cen = [sum(v[j] for v in vecs) / n for j in range(len(vecs[0]))]
        diags = [_obj_diag(objects[m]) for m in members]
        bb = [min(objects[m]["bbox"][0] for m in members),
              min(objects[m]["bbox"][1] for m in members),
              max(objects[m]["bbox"][2] for m in members),
              max(objects[m]["bbox"][3] for m in members)]
        radii = [_l1(_obj_vec(objects[m]), cen) for m in members]
        fam = {
            "members": members,
            "cls": objects[members[0]]["cls"],
            "class_key": _class_key(objects[members[0]]["cls"], p["class_split"]),
            "diag_med": round(statistics.median(diags), 3),
            "diag_min": round(min(diags), 3), "diag_max": round(max(diags), 3),
            "n_seg_med": statistics.median([objects[m]["n_seg"] for m in members]),
            "seg_len_sum": round(sum(objects[m].get("seg_len", 0.0) for m in members), 3),
            "radius_max": round(max(radii), 5),
            "bbox": [round(v, 2) for v in bb],
            "vec": [round(v, 5) for v in cen],
        }
        if sides is not None:
            fam["n_a"] = sum(1 for m in members if sides[m] == 0)
            fam["n_b"] = sum(1 for m in members if sides[m] == 1)
        families.append(fam)
        for m in members:
            obj_family[m] = len(families) - 1
    # deterministic family order
    order = sorted(range(len(families)),
                   key=lambda i: (-len(families[i]["members"]),
                                  families[i]["class_key"],
                                  families[i]["diag_med"],
                                  tuple(families[i]["vec"])))
    families = [families[i] for i in order]
    remap = {old: new for new, old in enumerate(order)}
    obj_family = [remap[f] if f >= 0 else -1 for f in obj_family]
    return families, obj_family


def build_families(layer, **params) -> FamilyLayer:
    p = dict(FAM_DEFAULTS)
    p.update(params or {})
    objects = layer.objects if hasattr(layer, "objects") else layer
    items = _items_from(objects, p)
    groups = [[items[k][0] for k in g] for g in (_cluster(items, p) if items else [])]
    families, obj_family = _finalise(objects, groups, p) if items else ([], [-1] * len(objects))
    stats = _stats(objects, families, p)
    return FamilyLayer(families, obj_family, p, stats, None, objects)


def build_families_pair(layer_a, layer_b, **params) -> FamilyLayer:
    p = dict(FAM_DEFAULTS)
    p.update(params or {})
    oa = layer_a.objects if hasattr(layer_a, "objects") else layer_a
    ob = layer_b.objects if hasattr(layer_b, "objects") else layer_b
    objects = list(oa) + list(ob)
    sides = [0] * len(oa) + [1] * len(ob)
    items = _items_from(objects, p)
    groups = [[items[k][0] for k in g] for g in (_cluster(items, p) if items else [])]
    families, obj_family = _finalise(objects, groups, p, sides) if items else ([], [-1] * len(objects))
    stats = _stats(objects, families, p)
    stats["n_obj_a"], stats["n_obj_b"] = len(oa), len(ob)
    return FamilyLayer(families, obj_family, p, stats, sides, objects)


def _stats(objects, families, p):
    mf = p["min_family"]
    n_excluded = sum(1 for o in objects
                     if _obj_diag(o) < float(p.get("min_diag_pt") or 0.0))
    rep = [f for f in families if len(f["members"]) >= mf]
    n_obj = len(objects)
    tot_len = sum(o.get("seg_len", 0.0) for o in objects) or 1.0
    in_rep = sum(len(f["members"]) for f in rep)
    len_rep = sum(f["seg_len_sum"] for f in rep)
    return {
        "n_obj": n_obj, "n_families": len(families),
        "n_repeated_families": len(rep),
        "objects_in_repeated": in_rep,
        "share_objects_in_repeated": round(in_rep / max(n_obj, 1), 5),
        "share_ink_in_repeated": round(len_rep / tot_len, 5),
        "largest_family": max((len(f["members"]) for f in families), default=0),
        "singletons": sum(1 for f in families if len(f["members"]) == 1),
        "n_below_floor": n_excluded,
    }


def family_deltas(fl: FamilyLayer, min_family: int = 2) -> list[dict]:
    """The "N -> M" rows the layer would publish for a compared pair."""
    rows = []
    for fi, f in enumerate(fl.families):
        na, nb = f.get("n_a", 0), f.get("n_b", 0)
        if max(na, nb) < min_family:
            continue
        if na == nb:
            continue
        rows.append({"family": fi, "n_a": na, "n_b": nb, "delta": nb - na,
                     "cls": f["cls"], "diag_med": f["diag_med"],
                     "bbox": f["bbox"], "n_seg_med": f["n_seg_med"]})
    rows.sort(key=lambda r: (-abs(r["delta"]), r["family"]))
    return rows


# ---------------------------------------------------------------- ARI

def ari(labels_a: Sequence[int], labels_b: Sequence[int]) -> float:
    """Adjusted Rand Index between two labellings of the SAME items."""
    n = len(labels_a)
    if n != len(labels_b) or n < 2:
        return float("nan")
    from collections import Counter
    cont = Counter(zip(labels_a, labels_b))
    ra = Counter(labels_a)
    rb = Counter(labels_b)
    c2 = lambda k: k * (k - 1) / 2.0
    sij = sum(c2(v) for v in cont.values())
    sa = sum(c2(v) for v in ra.values())
    sb = sum(c2(v) for v in rb.values())
    tot = c2(n)
    exp = sa * sb / tot if tot else 0.0
    mx = (sa + sb) / 2.0
    return (sij - exp) / (mx - exp) if abs(mx - exp) > 1e-12 else 1.0


# ------------------------------------------------------- robust ("super-family") deltas
#
# Measured failure mode (F2, A6 rounding).  A 0.01-0.25 pt coordinate rounding does not
# move an object out of its family -- it splits the family in two:
#     family X  n_a=90  n_b=70   (diag 56.691, class stray)
#     family Y  n_a=0   n_b=21   (diag 56.690, class stray)
# The ink is unchanged; the two rows are our own clustering boundary, and they cancel.
# So a cardinality row is published only after the deltas of MUTUALLY NEIGHBOURING
# families are summed: families are linked when they share a class band, a size band and
# lie within `link_frac * eps_desc` of each other in the descriptor, and the connected
# component ("super-family") is the unit that is allowed to say "N -> M".
# This is not a background filter and not a tuned threshold: it is the statement that the
# layer may not publish a difference that its own clustering boundary can explain.

def super_families(fl: FamilyLayer, link_frac: float = 2.0, max_pair: int = 400000):
    fams = fl.families
    n = len(fams)
    parent = list(range(n))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    stol = fl.params["size_tol"]
    eps = fl.params["eps_desc"] * link_frac
    idx: dict[tuple, list[int]] = {}
    logd = [math.log(max(f["diag_med"], 1e-6)) for f in fams]
    for i, f in enumerate(fams):
        idx.setdefault((f["class_key"], _bucket(logd[i], stol)), []).append(i)
    pairs = 0
    for i, f in enumerate(fams):
        b = _bucket(logd[i], stol)
        for bb in (b - 1, b, b + 1):
            for j in idx.get((f["class_key"], bb), ()):
                if j <= i:
                    continue
                if abs(logd[i] - logd[j]) > stol:
                    continue
                pairs += 1
                if pairs > max_pair:
                    break
                if _l1(f["vec"], fams[j]["vec"]) <= eps:
                    union(i, j)
    comps: dict[int, list[int]] = {}
    for i in range(n):
        comps.setdefault(find(i), []).append(i)
    return list(comps.values()), pairs


def family_deltas_robust(fl: FamilyLayer, min_family: int = 2, link_frac: float = 2.0):
    comps, _ = super_families(fl, link_frac)
    rows = []
    for ci, members in enumerate(comps):
        na = sum(fl.families[m].get("n_a", 0) for m in members)
        nb = sum(fl.families[m].get("n_b", 0) for m in members)
        if max(na, nb) < min_family or na == nb:
            continue
        bb = [min(fl.families[m]["bbox"][0] for m in members),
              min(fl.families[m]["bbox"][1] for m in members),
              max(fl.families[m]["bbox"][2] for m in members),
              max(fl.families[m]["bbox"][3] for m in members)]
        rows.append({"super": ci, "families": sorted(members), "n_fam": len(members),
                     "n_a": na, "n_b": nb, "delta": nb - na,
                     "cls": fl.families[members[0]]["cls"],
                     "diag_med": fl.families[members[0]]["diag_med"],
                     "n_seg_med": fl.families[members[0]]["n_seg_med"],
                     "bbox": [round(v, 2) for v in bb]})
    rows.sort(key=lambda r: (-abs(r["delta"]), r["super"]))
    return rows


def super_family_of(fl: FamilyLayer, obj_index: int, link_frac: float = 2.0):
    """(n_a, n_b, n_families) of the super-family that holds this object, or None."""
    fi = fl.obj_family[obj_index]
    if fi < 0:
        return None
    comps, _ = super_families(fl, link_frac)
    for members in comps:
        if fi in members:
            na = sum(fl.families[m].get("n_a", 0) for m in members)
            nb = sum(fl.families[m].get("n_b", 0) for m in members)
            return na, nb, len(members), members
    return None
