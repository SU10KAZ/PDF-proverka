# -*- coding: utf-8 -*-
"""Shared helpers for the `grp` probe (object layer, VECTOR 0.3).

Blocks are read ONLY through v03_foundation.  This file adds:
 * a corpus sampler over cns_block_classes.jsonl (the census the `cns` probe produced),
 * the class-A counterfactual rewrites (representation repacking) applied to the
   SEGMENT LIST of a real block, each keeping a provenance id per segment,
 * ink-overlap churn measurement between two object partitions.
"""
from __future__ import annotations

import json
import math
import os
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EXP = Path(__file__).resolve().parents[1]
ART = EXP / "artifacts"
sys.path.insert(0, str(EXP / "probes"))

import v03_foundation as F           # noqa: E402
import v03_objects as O              # noqa: E402

PROJ = ROOT / "projects_v2" / "objects"


# ------------------------------------------------------------------ corpus access

def block_records(path=None):
    path = path or (ART / "cns_block_classes.jsonl")
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


_RESULT_INDEX: dict[str, str] | None = None


def result_json_for(doc_id: str, version: str) -> str | None:
    """Locate 02_work/result.json for a (doc_id, version) pair.  Cached scan."""
    global _RESULT_INDEX
    if _RESULT_INDEX is None:
        idx: dict[str, str] = {}
        corpus = ART / "fnd_corpus_index.json"
        data = json.load(open(corpus, encoding="utf-8"))
        for e in data["documents"]:
            if not e.get("pdf_exists"):
                continue
            idx[f"{e['doc_id']}|{e['version']}"] = e["result_json"]
        _RESULT_INDEX = idx
    return _RESULT_INDEX.get(f"{doc_id}|{version}")


_BLOCK_CACHE: dict[str, dict] = {}


def prepared_block(doc_id: str, version: str, block_id: str):
    key = f"{doc_id}|{version}"
    if key not in _BLOCK_CACHE:
        rj = result_json_for(doc_id, version)
        if rj is None:
            return None
        full = rj if os.path.isabs(rj) else str(ROOT / rj)
        _BLOCK_CACHE[key] = {b.block_id: b for b in F.iter_prepared_blocks(full)}
    return _BLOCK_CACHE[key].get(block_id)


def extract(pb, **kw):
    return F.extract_block(pb.pdf_path, pb.page_index, pb.coords_px,
                           pb.page_px_w, pb.page_px_h, **kw)


# ------------------------------------------------------------------ class A rewrites
# Every rewrite takes the inked segment list of a REAL block and returns a new segment
# list where the drawn ink is identical and only the packaging differs.  Each output
# segment carries `src` = the id(s) of the original segment(s) it came from, which is
# what makes boundary churn measurable exactly instead of approximately.

def _seg(p0, p1, src, path, op, style, i):
    return {"i": i, "p0": (p0[0], p0[1]), "p1": (p1[0], p1[1]),
            "len": math.hypot(p1[0] - p0[0], p1[1] - p0[1]),
            "path": path, "op": op, "closed": False,
            "w": style[0], "color": style[1], "fill": style[2],
            "ink_rule": None, "border": False, "src": src}


def _base(segs):
    out = []
    for k, s in enumerate(segs):
        t = dict(s)
        t["i"] = k
        t["src"] = [s["i"]]
        out.append(t)
    return out


def rw_identity(segs, rng):
    return _base(segs)


def rw_path_split(segs, rng):
    out = []
    for k, s in enumerate(segs):
        t = dict(s)
        t["i"] = k
        t["path"] = k                      # every segment becomes its own path
        t["src"] = [s["i"]]
        out.append(t)
    return out


def rw_path_merge(segs, rng):
    """Put every chain of touching segments into ONE path id (opposite of A1)."""
    tol = 1e-3
    out = _base(segs)
    pid = 0
    prev = None
    for t in out:
        if prev is not None and math.hypot(prev["p1"][0] - t["p0"][0],
                                           prev["p1"][1] - t["p0"][1]) > tol:
            pid += 1
        t["path"] = pid
        prev = t
    return out


def _resample_polyline(pts, m):
    """Resample a polyline to m chords of equal arclength."""
    seglen = [math.hypot(pts[k + 1][0] - pts[k][0], pts[k + 1][1] - pts[k][1])
              for k in range(len(pts) - 1)]
    total = sum(seglen)
    if total <= 0:
        return pts
    out = [pts[0]]
    target = total / m
    acc = 0.0
    k = 0
    pos = pts[0]
    for step in range(1, m):
        want = target * step
        while k < len(seglen) and acc + seglen[k] < want:
            acc += seglen[k]
            k += 1
        if k >= len(seglen):
            break
        t = (want - acc) / max(seglen[k], 1e-12)
        out.append((pts[k][0] + t * (pts[k + 1][0] - pts[k][0]),
                    pts[k][1] + t * (pts[k + 1][1] - pts[k][1])))
    out.append(pts[-1])
    return out


def _curve_chains(segs):
    """Contiguous runs of curve-derived segments inside one path."""
    chains = []
    cur = []
    for k, s in enumerate(segs):
        if s.get("op") != "c":
            if len(cur) > 1:
                chains.append(cur)
            cur = []
            continue
        if cur and segs[cur[-1]]["path"] == s["path"] and \
                math.hypot(segs[cur[-1]]["p1"][0] - s["p0"][0],
                           segs[cur[-1]]["p1"][1] - s["p0"][1]) <= 1e-3:
            cur.append(k)
        else:
            if len(cur) > 1:
                chains.append(cur)
            cur = [k]
    if len(cur) > 1:
        chains.append(cur)
    return chains


def _resample_curves(segs, rng, m_new):
    """Rewrite every curve chain with a different chord density (A3 / A4)."""
    chains = _curve_chains(segs)
    in_chain = {}
    for ci, ch in enumerate(chains):
        for k in ch:
            in_chain[k] = ci
    out = []
    done = set()
    for k, s in enumerate(segs):
        ci = in_chain.get(k)
        if ci is None:
            t = dict(s); t["src"] = [s["i"]]; out.append(t); continue
        if ci in done:
            continue
        done.add(ci)
        ch = chains[ci]
        pts = [segs[ch[0]]["p0"]] + [segs[g]["p1"] for g in ch]
        src = [segs[g]["i"] for g in ch]
        m = m_new(len(ch))
        rp = _resample_polyline([tuple(q) for q in pts], m)
        style = (segs[ch[0]]["w"], segs[ch[0]]["color"], segs[ch[0]]["fill"])
        for j in range(len(rp) - 1):
            out.append(_seg(rp[j], rp[j + 1], src, segs[ch[0]]["path"], "c", style, 0))
    for k, t in enumerate(out):
        t["i"] = k
    return out


def rw_curve_resample_down(segs, rng):
    return _resample_curves(segs, rng, lambda n: max(2, n // 4))


def rw_curve_resample_up(segs, rng):
    return _resample_curves(segs, rng, lambda n: n * 3)


def rw_circle_to_bezier(segs, rng):
    """A4: re-emit every closed curve chain as 4 cubic Beziers flattened at 6 steps,
    i.e. exactly what a different exporter produces for the same circle."""
    chains = _curve_chains(segs)
    closed = []
    for ch in chains:
        p0 = segs[ch[0]]["p0"]
        p1 = segs[ch[-1]]["p1"]
        if math.hypot(p0[0] - p1[0], p0[1] - p1[1]) <= 1e-2:
            closed.append(ch)
    in_chain = {}
    for ci, ch in enumerate(closed):
        for k in ch:
            in_chain[k] = ci
    out = []
    done = set()
    for k, s in enumerate(segs):
        ci = in_chain.get(k)
        if ci is None:
            t = dict(s); t["src"] = [s["i"]]; out.append(t); continue
        if ci in done:
            continue
        done.add(ci)
        ch = closed[ci]
        pts = [segs[ch[0]]["p0"]] + [segs[g]["p1"] for g in ch]
        xs = [q[0] for q in pts]; ys = [q[1] for q in pts]
        cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
        r = sum(math.hypot(q[0] - cx, q[1] - cy) for q in pts) / len(pts)
        src = [segs[g]["i"] for g in ch]
        style = (segs[ch[0]]["w"], segs[ch[0]]["color"], segs[ch[0]]["fill"])
        kap = 0.5522847498307936
        quarters = []
        for qi in range(4):
            a0 = math.pi / 2 * qi
            a1 = a0 + math.pi / 2
            p0 = (cx + r * math.cos(a0), cy + r * math.sin(a0))
            p3 = (cx + r * math.cos(a1), cy + r * math.sin(a1))
            t0 = (-math.sin(a0), math.cos(a0))
            t1 = (-math.sin(a1), math.cos(a1))
            p1 = (p0[0] + kap * r * t0[0], p0[1] + kap * r * t0[1])
            p2 = (p3[0] - kap * r * t1[0], p3[1] - kap * r * t1[1])
            quarters.append((p0, p1, p2, p3))
        for (q0, q1, q2, q3) in quarters:
            steps = 6
            prev = q0
            for j in range(1, steps + 1):
                t = j / steps
                u = 1 - t
                pt = (u**3 * q0[0] + 3 * u * u * t * q1[0] + 3 * u * t * t * q2[0] + t**3 * q3[0],
                      u**3 * q0[1] + 3 * u * u * t * q1[1] + 3 * u * t * t * q2[1] + t**3 * q3[1])
                out.append(_seg(prev, pt, src, segs[ch[0]]["path"], "c", style, 0))
                prev = pt
    for k, t in enumerate(out):
        t["i"] = k
    return out


def rw_order_shuffle(segs, rng):
    out = _base(segs)
    rng.shuffle(out)
    for k, t in enumerate(out):
        t["i"] = k
    return out


def _rw_round(q):
    def f(segs, rng):
        out = []
        for k, s in enumerate(segs):
            t = dict(s)
            t["p0"] = (round(s["p0"][0] / q) * q, round(s["p0"][1] / q) * q)
            t["p1"] = (round(s["p1"][0] / q) * q, round(s["p1"][1] / q) * q)
            t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
            t["i"] = k
            t["src"] = [s["i"]]
            if t["len"] > 1e-9:
                out.append(t)
        for k, t in enumerate(out):
            t["i"] = k
        return out
    return f


def rw_lineweight(segs, rng):
    out = _base(segs)
    for t in out:
        w = t.get("w") or 0.0
        t["w"] = round(w * 1.5 + 0.05, 3)
        c = t.get("color")
        if c:
            t["color"] = tuple(round(min(1.0, v * 0.85 + 0.05), 3) for v in c)
    return out


# --- circle re-encoding (the v0.2 R12 case), independent of the `op` tag ----------

def _geo_chains(segs, tol=1e-3):
    """Contiguous point-runs by geometry alone (used by the circle re-encoders)."""
    chains = []
    cur = [0] if segs else []
    for k in range(1, len(segs)):
        a, b = segs[k - 1], segs[k]
        if math.hypot(a["p1"][0] - b["p0"][0], a["p1"][1] - b["p0"][1]) <= tol:
            cur.append(k)
        else:
            chains.append(cur)
            cur = [k]
    if cur:
        chains.append(cur)
    return chains


def _closed_circles(segs, resid_rel=0.03, min_pts=5):
    """Closed chains whose points lie on a circle.  Ground truth for A4/A4b."""
    out = []
    for ch in _geo_chains(segs):
        if len(ch) + 1 < min_pts:
            continue
        p0 = segs[ch[0]]["p0"]
        p1 = segs[ch[-1]]["p1"]
        if math.hypot(p0[0] - p1[0], p0[1] - p1[1]) > 1e-2:
            continue
        pts = [tuple(segs[ch[0]]["p0"])] + [tuple(segs[g]["p1"]) for g in ch]
        fit = O._fit_circle(pts)
        if fit is None:
            continue
        cx, cy, r, resid = fit
        if r <= 1e-6 or resid / r > resid_rel:
            continue
        out.append((ch, cx, cy, r))
    return out


def _recode_circles(segs, encoder):
    circles = _closed_circles(segs)
    in_ch = {}
    for ci, (ch, *_r) in enumerate(circles):
        for k in ch:
            in_ch[k] = ci
    out, done, touched = [], set(), 0
    for k, s in enumerate(segs):
        ci = in_ch.get(k)
        if ci is None:
            t = dict(s); t["src"] = [s["i"]]; out.append(t); continue
        if ci in done:
            continue
        done.add(ci)
        ch, cx, cy, r = circles[ci]
        touched += len(ch)
        src = [segs[g]["i"] for g in ch]
        style = (segs[ch[0]]["w"], segs[ch[0]]["color"], segs[ch[0]]["fill"])
        a0 = math.atan2(segs[ch[0]]["p0"][1] - cy, segs[ch[0]]["p0"][0] - cx)
        pts = encoder(cx, cy, r, a0)
        for j in range(len(pts) - 1):
            out.append(_seg(pts[j], pts[j + 1], src, segs[ch[0]]["path"], "c", style, 0))
    for k, t in enumerate(out):
        t["i"] = k
    return out, touched, len(circles)


def _enc_bezier(cx, cy, r, a0):
    kap = 0.5522847498307936
    pts = []
    for qi in range(4):
        b0 = a0 + math.pi / 2 * qi
        b1 = b0 + math.pi / 2
        q0 = (cx + r * math.cos(b0), cy + r * math.sin(b0))
        q3 = (cx + r * math.cos(b1), cy + r * math.sin(b1))
        t0 = (-math.sin(b0), math.cos(b0))
        t1 = (-math.sin(b1), math.cos(b1))
        q1 = (q0[0] + kap * r * t0[0], q0[1] + kap * r * t0[1])
        q2 = (q3[0] - kap * r * t1[0], q3[1] - kap * r * t1[1])
        prev = q0
        if qi == 0:
            pts.append(q0)
        for j in range(1, 7):
            t = j / 6; u = 1 - t
            pt = (u**3 * q0[0] + 3 * u * u * t * q1[0] + 3 * u * t * t * q2[0] + t**3 * q3[0],
                  u**3 * q0[1] + 3 * u * u * t * q1[1] + 3 * u * t * t * q2[1] + t**3 * q3[1])
            pts.append(pt)
    return pts


def _enc_chords(m):
    def f(cx, cy, r, a0):
        return [(cx + r * math.cos(a0 + 2 * math.pi * k / m),
                 cy + r * math.sin(a0 + 2 * math.pi * k / m)) for k in range(m + 1)]
    return f


def rw_circle_bezier(segs, rng):
    return _recode_circles(segs, _enc_bezier)[0]


def rw_circle_chords5(segs, rng):
    return _recode_circles(segs, _enc_chords(5))[0]


def rw_circle_chords24(segs, rng):
    return _recode_circles(segs, _enc_chords(24))[0]


def rewrite_bite(name, segs_in, segs_out):
    """How much the rewrite actually touched.  A no-op rewrite must not be counted
    as evidence of stability (brief, execution rule 5)."""
    if name == "A0_identity":
        return 0
    if name.startswith("A1") or name.startswith("A2"):
        return len(segs_in)                       # repackaging touches every path id
    if name.startswith("A5"):
        return len(segs_in)
    if name.startswith("A8"):
        return len(segs_in)
    if name.startswith("A6"):
        moved = 0
        for a, b in zip(segs_in, segs_out):
            if a["p0"] != b["p0"] or a["p1"] != b["p1"]:
                moved += 1
        return moved + max(0, len(segs_in) - len(segs_out))
    # curve / circle recoders: count source segments that were replaced
    touched = set()
    for b in segs_out:
        src = b.get("src") or []
        if len(src) > 1 or (len(src) == 1 and b.get("op") == "c" and len(segs_out) != len(segs_in)):
            touched.update(src)
    return len(touched)


REWRITES = {
    "A0_identity": rw_identity,
    "A1_path_split": rw_path_split,
    "A2_path_merge": rw_path_merge,
    "A3_curve_resample_down": rw_curve_resample_down,
    "A3_curve_resample_up": rw_curve_resample_up,
    "A4_circle_to_bezier": rw_circle_bezier,
    "A4b_circle_to_chords5": rw_circle_chords5,
    "A4c_circle_to_chords24": rw_circle_chords24,
    "A5_order_shuffle": rw_order_shuffle,
    "A6_round_0.01": _rw_round(0.01),
    "A6_round_0.1": _rw_round(0.1),
    "A6_round_0.25": _rw_round(0.25),
    "A6_round_0.5": _rw_round(0.5),
    "A8_lineweight": rw_lineweight,
}


class _FakeExtract:
    def __init__(self, segments, texts):
        self.segments = segments
        self.texts = texts


def layer_of(segments, texts, **params):
    return O.build_objects(_FakeExtract(segments, texts), **params)


# ------------------------------------------------------------------ churn (exact)

def churn_exact(layer_a, segs_a, layer_b, segs_b):
    """Boundary churn between two object partitions of the SAME ink.

    ``segs_b`` carries ``src`` = list of original segment ids.  For every object on
    the left we ask: how many right-hand objects overlap its ink, and what share of
    its ink goes into the largest of them.  This is the metric the brief demands —
    a distribution, not "share matched".
    """
    # right side: original segment id -> (object index, weight share of its ink)
    owner: dict[int, list[tuple[int, float]]] = {}
    for oi, o in enumerate(layer_b.objects):
        for gi in o["segments"]:
            s = segs_b[gi]
            src = s.get("src") or [gi]
            w = s["len"] / max(len(src), 1)
            for si in src:
                owner.setdefault(si, []).append((oi, w))
    rows = []
    for oi, o in enumerate(layer_a.objects):
        tot = 0.0
        acc: dict[int, float] = {}
        for gi in o["segments"]:
            L = segs_a[gi]["len"]
            tot += L
            for (oj, w) in owner.get(gi, ()):  # weights are proportional
                acc[oj] = acc.get(oj, 0.0) + L
        if tot <= 0:
            continue
        if not acc:
            rows.append({"o": oi, "cls": o["cls"], "n_seg": o["n_seg"], "len": tot,
                         "n_partners": 0, "best_share": 0.0, "partner_purity": 0.0})
            continue
        best_oj, best = max(acc.items(), key=lambda kv: kv[1])
        # purity of the winning partner: how much of ITS ink comes from this object
        pj = layer_b.objects[best_oj]
        rows.append({"o": oi, "cls": o["cls"], "n_seg": o["n_seg"], "len": tot,
                     "n_partners": len(acc), "best_share": best / tot,
                     "partner_purity": best / max(pj["seg_len"], 1e-9),
                     "partner": best_oj})
    return rows


def classify_churn(rows, thr=0.98):
    """1:1 / 1:N (split) / N:1 (merge) / mixed, by ink length share."""
    out = {"one_to_one": 0.0, "split": 0.0, "merge": 0.0, "mixed": 0.0, "lost": 0.0}
    tot = sum(r["len"] for r in rows) or 1.0
    for r in rows:
        if r["n_partners"] == 0:
            k = "lost"
        elif r["best_share"] >= thr and r["partner_purity"] >= thr:
            k = "one_to_one"
        elif r["best_share"] < thr and r["partner_purity"] >= thr:
            k = "split"
        elif r["best_share"] >= thr and r["partner_purity"] < thr:
            k = "merge"
        else:
            k = "mixed"
        out[k] += r["len"]
    return {k: v / tot for k, v in out.items()}


def pct(vals, q):
    if not vals:
        return None
    v = sorted(vals)
    k = min(len(v) - 1, max(0, int(round(q * (len(v) - 1)))))
    return v[k]
