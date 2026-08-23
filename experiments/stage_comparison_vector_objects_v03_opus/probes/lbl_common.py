# -*- coding: utf-8 -*-
"""Shared helpers for the `lbl` probe (VECTOR 0.3, Opus): how much do TEXT LABELS
help object correspondence, and what is achievable without them.

Blocks are read ONLY through v03_foundation (via grp_common); objects ONLY through
v03_objects; counterfactuals ONLY through v03_counterfactual.  Nothing here opens a
PDF or re-implements extraction.

Three things live here:
  1. label census  — what text, if any, sits next to an object, at k*S
  2. object matcher — ONE algorithm, three information modes (geom / +pos / +label)
  3. ground truth   — exact for counterfactuals (segment `src` provenance),
                      ink-derived reference for real pairs (documented as a proxy)
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402

ART = G.ART
ROOT = G.ROOT

# ---------------------------------------------------------------- designations

TOKEN_RE = re.compile(r"[^\s,;()]+")
LETTERS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЭЮЯ"
              "abcdefghijklmnopqrstuvwxyzабвгдеёжзийклмнопрстуфхцчшщэюя")
# same "mark" definition the `cns` probe used for CNS-7 (letter AND digit)
def is_mark(t: str) -> bool:
    if not (2 <= len(t) <= 16):
        return False
    return any(ch in LETTERS for ch in t) and any(ch.isdigit() for ch in t)


def marks_of(text: str) -> list[str]:
    return [t for t in TOKEN_RE.findall(text or "") if is_mark(t)]


def norm_mark(t: str) -> str:
    return t.upper().replace("–", "-").replace("—", "-").strip(".,:;")


# ---------------------------------------------------------------- label census

K_LADDER = (0.5, 1.0, 1.6, 2.5)


def _gap(bb, tb) -> float:
    gx = max(0.0, max(bb[0] - tb[2], tb[0] - bb[2]))
    gy = max(0.0, max(bb[1] - tb[3], tb[1] - bb[3]))
    return math.hypot(gx, gy)


def block_mark_index(texts) -> dict[str, int]:
    """How many TEXT LINES of this block carry each normalised mark token."""
    cnt: dict[str, int] = {}
    for t in texts:
        for m in {norm_mark(x) for x in marks_of(t["text"])}:
            cnt[m] = cnt.get(m, 0) + 1
    return cnt


def label_census(layer, texts, ks=K_LADDER) -> list[dict]:
    """Per-object label record.  Pure measurement: never changes the object layer."""
    S = max(float(layer.S), 1e-6)
    cnt = block_mark_index(texts)
    tb = [t["bbox"] for t in texts]
    tmark = [[norm_mark(x) for x in marks_of(t["text"])] for t in texts]
    out = []
    for o in layer.objects:
        bb = o["bbox"]
        gaps = [(_gap(bb, tb[i]), i) for i in range(len(texts))]
        gaps.sort()
        rec = {"cls": o["cls"], "n_seg": o["n_seg"], "diag": o["diag"]}
        for k in ks:
            r = k * S
            near = [(g, i) for g, i in gaps if g <= r]
            key = f"k{k}"
            if not near:
                rec[key] = {"state": "no_text"}
                continue
            # nearest text that actually carries a mark token
            mk = next(((g, i) for g, i in near if tmark[i]), None)
            if mk is None:
                rec[key] = {"state": "text_no_mark", "n_text": len(near),
                            "nearest": texts[near[0][1]]["text"][:40]}
                continue
            g, i = mk
            m = tmark[i][0]
            rec[key] = {"state": "unique_mark" if cnt.get(m, 0) == 1 else "repeated_mark",
                        "mark": m, "n_lines_with_mark": cnt.get(m, 0),
                        "gap_over_S": round(g / S, 3), "n_text": len(near)}
        out.append(rec)
    return out


def object_labels(layer, texts, k=1.6):
    """(mark, unique_in_block) per object, at radius k*S.  Used by the matcher."""
    S = max(float(layer.S), 1e-6)
    cnt = block_mark_index(texts)
    r = k * S
    res = []
    for o in layer.objects:
        bb = o["bbox"]
        best = None
        for t in texts:
            mm = marks_of(t["text"])
            if not mm:
                continue
            g = _gap(bb, t["bbox"])
            if g <= r and (best is None or g < best[0]):
                best = (g, norm_mark(mm[0]))
        if best is None:
            res.append((None, False))
        else:
            res.append((best[1], cnt.get(best[1], 0) == 1))
    return res


# ---------------------------------------------------------------- matcher

CLS_ORDER = ("symbol", "linear", "area", "composite", "stray")

DEFAULT_MATCH = {
    "R_pos_S": 3.0,        # positional gate, units of S (v0.2/G7 used 3*S)
    "tau_desc": 0.60,      # descriptor L1 gate for geometry-only
    "tau_desc_loose": 1.20,
    "w_size": 0.50,
    "w_pos": 1.00,
    "tau_cost": 0.90,      # assignment gate on total cost
    "label_bonus": 0.60,
    "label_penalty": 0.60,
    "cls_gate": True,
}


def _mats(layer):
    n = len(layer.objects)
    D = np.zeros((n, O.DESC_LEN), dtype=np.float32)
    C = np.zeros((n, 2), dtype=np.float32)
    dg = np.zeros(n, dtype=np.float32)
    cl = np.zeros(n, dtype=np.int8)
    for i, o in enumerate(layer.objects):
        D[i] = o["desc"]["vec"]
        C[i] = (o["cx"], o["cy"])
        dg[i] = max(o["diag"], 1e-3)
        cl[i] = CLS_ORDER.index(o["cls"]) if o["cls"] in CLS_ORDER else 9
    return D, C, dg, cl


def match_objects(LA, LB, mode, S, *, labels_a=None, labels_b=None,
                  off=(0.0, 0.0), label_use="anchor", p=None, chunk=96):
    """One assignment algorithm, three information modes.

    mode: "geom" | "geom_pos" | "geom_pos_label"
    off:  translation applied to B centres before comparing positions (registration).
    label_use: "anchor"   — a matching unique mark only LOWERS cost (never raises it)
               "evidence" — a mark mismatch also RAISES cost (the §7 trap, measured)

    Assignment: global greedy over candidate pairs sorted by cost, one-to-one,
    deterministic.  Returns (pairs, top1, cost_of_pair).
    """
    p = dict(DEFAULT_MATCH, **(p or {}))
    na, nb = len(LA.objects), len(LB.objects)
    if na == 0 or nb == 0:
        return {"pairs": [], "top1": {}, "na": na, "nb": nb, "mode": mode}
    DA, CA, GA, KA = _mats(LA)
    DB, CB, GB, KB = _mats(LB)
    CB = CB + np.asarray(off, dtype=np.float32)
    S = max(float(S), 1e-6)
    use_pos = mode in ("geom_pos", "geom_pos_label")
    use_lbl = mode == "geom_pos_label"
    if use_lbl:
        ma = [(labels_a[i][0] if labels_a[i][1] else None) for i in range(na)]
        mb = [(labels_b[i][0] if labels_b[i][1] else None) for i in range(nb)]
        mb_ix: dict[str, list[int]] = {}
        for j, m in enumerate(mb):
            if m:
                mb_ix.setdefault(m, []).append(j)

    cand = []          # (cost, ia, ib)
    top1: dict[int, tuple[int, float]] = {}
    logGB = np.log(GB)
    for a0 in range(0, na, chunk):
        a1 = min(na, a0 + chunk)
        dd = np.abs(DA[a0:a1, None, :] - DB[None, :, :]).sum(axis=2)
        ds = np.abs(np.log(GA[a0:a1, None]) - logGB[None, :])
        cost = dd + p["w_size"] * np.minimum(ds, 2.0)
        ok = np.ones_like(cost, dtype=bool)
        if p["cls_gate"]:
            ok &= (KA[a0:a1, None] == KB[None, :])
        if use_pos:
            dx = CA[a0:a1, 0][:, None] - CB[None, :, 0]
            dy = CA[a0:a1, 1][:, None] - CB[None, :, 1]
            dp = np.sqrt(dx * dx + dy * dy) / S
            ok &= (dp <= p["R_pos_S"]) & (dd <= p["tau_desc_loose"])
            cost = cost + p["w_pos"] * np.minimum(dp / p["R_pos_S"], 1.0)
        else:
            ok &= (dd <= p["tau_desc"])
        if use_lbl:
            for ii in range(a0, a1):
                m = ma[ii]
                row = ii - a0
                if m and m in mb_ix:
                    # a matching UNIQUE designation lifts the positional gate: that is
                    # the whole point of an anchor - it survives a frame we cannot register
                    for j in mb_ix[m]:
                        cost[row, j] -= p["label_bonus"]
                        if (not p["cls_gate"]) or KA[ii] == KB[j]:
                            ok[row, j] = True
                if label_use == "evidence":
                    for j in range(nb):
                        if m and mb[j] and mb[j] != m:
                            cost[row, j] += p["label_penalty"]
        big = cost.copy()
        big[~ok] = np.inf
        for row in range(a1 - a0):
            j = int(np.argmin(big[row]))
            v = float(big[row, j])
            if math.isfinite(v):
                top1[a0 + row] = (j, v)
        rr, cc = np.nonzero(ok & (cost <= p["tau_cost"]))
        for r_, c_ in zip(rr.tolist(), cc.tolist()):
            cand.append((float(cost[r_, c_]), a0 + r_, c_))

    cand.sort(key=lambda t: (t[0], t[1], t[2]))
    used_a, used_b = set(), set()
    pairs = []
    for c, ia, ib in cand:
        if ia in used_a or ib in used_b:
            continue
        used_a.add(ia); used_b.add(ib)
        pairs.append((ia, ib, c))
    return {"pairs": pairs, "top1": top1, "na": na, "nb": nb, "mode": mode,
            "n_cand": len(cand)}


# ---------------------------------------------------------------- ground truth

NEW_TAGS = {"C2_added", "C8_link", "C9_branch", "C10_closed_opening", "D7_tick"}


def _endpoint_key(s, q=1e-3):
    a = (round(s["p0"][0] / q), round(s["p0"][1] / q))
    b = (round(s["p1"][0] / q), round(s["p1"][1] / q))
    return (a, b) if a <= b else (b, a)


def gt_from_provenance(LA, segs_a, LB, segs_b):
    """EXACT object correspondence for a counterfactual side B built from side A.

    Every side-B segment carries `src` = indices of the side-A segments it came from
    (v03_counterfactual), except the ones explicitly tagged as newly created.
    Returns (gt_ab, gt_ba, w) where gt_ab[ia] = ib or None.
    """
    seg2obj_a = LA.seg2obj
    seg2obj_b = LB.seg2obj
    W: dict[tuple[int, int], float] = {}
    born: dict[int, float] = {}
    # Some counterfactuals do not touch the segment list at all (class D) or rebuild it
    # by re-reading the PDF (B3 crop jitter): there is no `src` to follow.  Then the
    # ancestor is found by EXACT endpoint identity, which is honest for exactly those
    # rewrites (they are defined not to move geometry).
    has_src = any(s.get("src") for s in segs_b)
    epk: dict = {}
    if not has_src:
        for k, s in enumerate(segs_a):
            epk.setdefault(_endpoint_key(s), []).append(k)
    for gi, s in enumerate(segs_b):
        ob = seg2obj_b[gi]
        if ob < 0:
            continue
        L = s["len"]
        if s.get("cf_tag") in NEW_TAGS:
            born[ob] = born.get(ob, 0.0) + L
            continue
        src = s.get("src")
        if not src:
            if has_src:
                born[ob] = born.get(ob, 0.0) + L
                continue
            cand = epk.get(_endpoint_key(s))
            if not cand:
                born[ob] = born.get(ob, 0.0) + L
                continue
            src = cand
        w = L / len(src)
        for k in src:
            if 0 <= k < len(seg2obj_a):
                oa = seg2obj_a[k]
                if oa >= 0:
                    W[(oa, ob)] = W.get((oa, ob), 0.0) + w
    best_ab: dict[int, tuple[int, float]] = {}
    best_ba: dict[int, tuple[int, float]] = {}
    for (oa, ob), w in W.items():
        if oa not in best_ab or w > best_ab[oa][1]:
            best_ab[oa] = (ob, w)
        if ob not in best_ba or w > best_ba[ob][1]:
            best_ba[ob] = (oa, w)
    gt_ab = {ia: (best_ab[ia][0] if ia in best_ab else None) for ia in range(len(LA.objects))}
    gt_ba = {ib: (best_ba[ib][0] if ib in best_ba else None) for ib in range(len(LB.objects))}
    # an object made only of newly born ink has no ancestor even if some stray src leaked
    for ib, o in enumerate(LB.objects):
        if born.get(ib, 0.0) >= 0.9 * o["seg_len"]:
            gt_ba[ib] = None
    return gt_ab, gt_ba


def score(pairs, gt_ab, gt_ba, na, nb):
    """precision / recall of the correspondence + false ADDED / REMOVED."""
    matched_a = {ia: ib for ia, ib, _ in pairs}
    matched_b = {ib: ia for ia, ib, _ in pairs}
    gt_pairs = {ia: ib for ia, ib in gt_ab.items() if ib is not None}
    correct = sum(1 for ia, ib in matched_a.items() if gt_ab.get(ia) == ib)
    prec = correct / max(len(matched_a), 1)
    rec = correct / max(len(gt_pairs), 1)
    false_removed = sum(1 for ia in range(na) if ia not in matched_a and gt_ab.get(ia) is not None)
    false_added = sum(1 for ib in range(nb) if ib not in matched_b and gt_ba.get(ib) is not None)
    true_removed = sum(1 for ia in range(na) if gt_ab.get(ia) is None)
    true_added = sum(1 for ib in range(nb) if gt_ba.get(ib) is None)
    caught_removed = sum(1 for ia in range(na) if gt_ab.get(ia) is None and ia not in matched_a)
    caught_added = sum(1 for ib in range(nb) if gt_ba.get(ib) is None and ib not in matched_b)
    return {"na": na, "nb": nb, "n_matched": len(matched_a), "n_correct": correct,
            "precision": round(prec, 5), "recall": round(rec, 5),
            "n_gt_pairs": len(gt_pairs),
            "false_removed": false_removed, "false_added": false_added,
            "false_removed_share": round(false_removed / max(na, 1), 5),
            "false_added_share": round(false_added / max(nb, 1), 5),
            "true_removed": true_removed, "true_added": true_added,
            "caught_removed": caught_removed, "caught_added": caught_added}


def top1_acc(top1, gt_ab):
    gt = {ia: ib for ia, ib in gt_ab.items() if ib is not None}
    if not gt:
        return None, 0
    ok = sum(1 for ia, ib in gt.items() if top1.get(ia, (None,))[0] == ib)
    return ok / len(gt), len(gt)


# ---------------------------------------------------------------- misc

def pct(v, q):
    return float(np.percentile(v, q)) if len(v) else None


def summarise(vals):
    if not vals:
        return None
    a = np.asarray(vals, dtype=float)
    return {"n": int(a.size), "median": round(float(np.median(a)), 5),
            "mean": round(float(a.mean()), 5),
            "p10": round(float(np.percentile(a, 10)), 5),
            "p90": round(float(np.percentile(a, 90)), 5),
            "min": round(float(a.min()), 5), "max": round(float(a.max()), 5)}


# ---------------------------------------------------------------- verdict / ledger

def verdict(LA, LB, pairs, S, *, off=(0.0, 0.0), move_tol_pt=0.5, shape_tol=0.25,
            labels_a=None, labels_b=None, label_is_evidence=False):
    """Turn a correspondence into a ledger.  Deterministic, no discipline vocabulary.

    ``label_is_evidence=True`` adds the §7 trap on purpose: a renamed label alone
    becomes a ledger entry.  The honest mode is False (a label is an anchor only).
    """
    S = max(float(S), 1e-6)
    ma = {ia: ib for ia, ib, _ in pairs}
    mb = {ib: ia for ia, ib, _ in pairs}
    ent = {"REMOVED_OBJECT": [], "ADDED_OBJECT": [], "MOVED_OBJECT": [],
           "RESHAPED_OBJECT": [], "RENAMED_OBJECT": []}
    ink_removed = ink_added = ink_moved = 0.0
    for ia, o in enumerate(LA.objects):
        if ia not in ma:
            ent["REMOVED_OBJECT"].append(ia)
            ink_removed += o["seg_len"]
    for ib, o in enumerate(LB.objects):
        if ib not in mb:
            ent["ADDED_OBJECT"].append(ib)
            ink_added += o["seg_len"]
    for ia, ib, _c in pairs:
        a, b = LA.objects[ia], LB.objects[ib]
        d = math.hypot(a["cx"] - (b["cx"] + off[0]), a["cy"] - (b["cy"] + off[1]))
        if d > move_tol_pt:
            ent["MOVED_OBJECT"].append((ia, ib, round(d, 3)))
            ink_moved += a["seg_len"]
        dd = O.descriptor_distance(a["desc"], b["desc"])
        if dd > shape_tol:
            ent["RESHAPED_OBJECT"].append((ia, ib, round(dd, 4)))
        if label_is_evidence and labels_a is not None and labels_b is not None:
            la, lb = labels_a[ia][0], labels_b[ib][0]
            if la and lb and la != lb:
                ent["RENAMED_OBJECT"].append((ia, ib, la, lb))
    n = sum(len(v) for v in ent.values())
    tot = sum(o["seg_len"] for o in LA.objects) or 1.0
    return {"verdict": "GRAPHIC_CHANGE" if n else "NO_GRAPHIC_CHANGE",
            "n_entries": n,
            "counts": {k: len(v) for k, v in ent.items()},
            "ink_share_removed": round(ink_removed / tot, 6),
            "ink_share_added": round(ink_added / tot, 6),
            "ink_share_moved": round(ink_moved / tot, 6),
            "entries": ent}


def label_change_census(LA, LB, pairs, labels_a, labels_b):
    """How many correspondences carry a CHANGED label string (the size of the flood
    a label-as-evidence comparator would emit)."""
    same = diff = only_a = only_b = none = 0
    for ia, ib, _c in pairs:
        la, lb = labels_a[ia][0], labels_b[ib][0]
        if la and lb:
            same += (la == lb); diff += (la != lb)
        elif la:
            only_a += 1
        elif lb:
            only_b += 1
        else:
            none += 1
    n = len(pairs) or 1
    return {"n_pairs": len(pairs), "same": same, "changed": diff,
            "only_a": only_a, "only_b": only_b, "no_label": none,
            "changed_share_of_pairs": round(diff / n, 5),
            "changed_share_of_labelled": round(diff / max(same + diff, 1), 5)}
