# -*- coding: utf-8 -*-
"""scope · SC1 — census of the BLOCK-TO-BLOCK relation between two document versions.

For every matched sheet (page map R3, the arbiter mine_pagematch.json measured at 0.912)
build the FULL bipartite overlap graph between the prepared graphic blocks of side A and
side B, take its connected components and classify each one:

    1:1   one block on each side
    1:N   one block of A meets several blocks of B          (fragmentation)
    N:1   several blocks of A meet one block of B           (merge)
    N:M   tangle
    1:0 / 0:1   a block with no counterpart at all          (orphan)

Geometry is compared in page-normalised coordinates.  result.json px space maps LINEARLY
onto the page display rect (see v03_foundation.block_frame), so px-normalised == display-
normalised; sheets whose /Rotate differs between versions are counted separately because
their display frames are not the same frame.

Writes artifacts/scope_relation_census.json (+ scope_components.jsonl).
"""
from __future__ import annotations
import json, sys, itertools
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
ROOT = EXP.parents[1]
sys.path.insert(0, str(HERE))
import v03_foundation as F  # noqa

EDGE_IOU = 0.05          # the threshold mine_screen used for its greedy 1:1 matcher
EDGE_SETS = {"iou005": 0.05, "iou010": 0.10, "iou030": 0.30}


def norm_of(b):
    if b.coords_norm:
        return tuple(b.coords_norm)
    return (b.coords_px[0] / b.page_px_w, b.coords_px[1] / b.page_px_h,
            b.coords_px[2] / b.page_px_w, b.coords_px[3] / b.page_px_h)


def rect(n):
    x0, y0, x1, y1 = n
    return (min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))


def area(r):
    return max(0.0, r[2] - r[0]) * max(0.0, r[3] - r[1])


def inter(a, b):
    return area((max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])))


def iou(a, b):
    i = inter(a, b)
    u = area(a) + area(b) - i
    return i / u if u > 0 else 0.0


def contain(a, b):
    """share of the SMALLER rect covered by the other"""
    i = inter(a, b)
    m = min(area(a), area(b))
    return i / m if m > 0 else 0.0


def components(nA, nB, edges):
    par = list(range(nA + nB))

    def find(x):
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x

    for i, j in edges:
        a, b = find(i), find(nA + j)
        if a != b:
            par[a] = b
    groups = {}
    for k in range(nA + nB):
        groups.setdefault(find(k), []).append(k)
    out = []
    for g in groups.values():
        A = [k for k in g if k < nA]
        B = [k - nA for k in g if k >= nA]
        out.append((A, B))
    return out


def main():
    pm = json.load(open(ART / "mine_pagematch.json", encoding="utf-8"))
    idx = json.load(open(ART / "mine_pair_index.json", encoding="utf-8"))
    P_by = {(p["doc_id"], p["ver_a"], p["ver_b"]): p for p in idx["pairs"] if not p["same_pdf"]}

    S = {k: 0 for k in ["version_pairs", "pages", "blocks_a", "blocks_b",
                        "rot_mismatch_pages", "rot_mismatch_blocks"]}
    kinds = {}
    per_disc = {}
    sens = {k: {} for k in EDGE_SETS}
    comp_rows = []
    nested_rows = []

    for row in pm["rows"]:
        if "r3" not in row:
            continue
        key = next((k for k in P_by if k[0] == row["doc"] and f"{k[1]}->{k[2]}" == row["ver"]), None)
        if key is None:
            continue
        Pp = P_by[key]
        try:
            BA = F.iter_prepared_blocks(Pp["result_a"])
            BB = F.iter_prepared_blocks(Pp["result_b"])
        except Exception as e:
            print("skip", Pp["doc_id"], repr(e))
            continue
        byA, byB = {}, {}
        for b in BA:
            byA.setdefault(b.page_number, []).append(b)
        for b in BB:
            byB.setdefault(b.page_number, []).append(b)
        S["version_pairs"] += 1
        disc = Pp["discipline"]
        D = per_disc.setdefault(disc, {"pages": 0, "comp": 0, "non11": 0, "blocks": 0, "blocks_non11": 0})

        for pa_s, pb in row["r3"].items():
            pa = int(pa_s)
            la, lb = byA.get(pa, []), byB.get(pb, [])
            if not la and not lb:
                continue
            S["pages"] += 1
            D["pages"] += 1
            S["blocks_a"] += len(la)
            S["blocks_b"] += len(lb)
            rot_a = la[0].rotation if la else None
            rot_b = lb[0].rotation if lb else None
            rot_mismatch = (rot_a is not None and rot_b is not None and rot_a != rot_b)
            if rot_mismatch:
                S["rot_mismatch_pages"] += 1
                S["rot_mismatch_blocks"] += len(la) + len(lb)
            RA = [rect(norm_of(b)) for b in la]
            RB = [rect(norm_of(b)) for b in lb]

            for name, thr in EDGE_SETS.items():
                e = [(i, j) for i in range(len(RA)) for j in range(len(RB)) if iou(RA[i], RB[j]) > thr]
                for A, B in components(len(RA), len(RB), e):
                    k = f"{'N' if len(A)>1 else len(A)}:{'N' if len(B)>1 else len(B)}"
                    sens[name][k] = sens[name].get(k, 0) + 1

            edges = [(i, j) for i in range(len(RA)) for j in range(len(RB)) if iou(RA[i], RB[j]) > EDGE_IOU]
            for A, B in components(len(RA), len(RB), edges):
                na, nb = len(A), len(B)
                if na == 1 and nb == 1:
                    ra, rb = RA[A[0]], RB[B[0]]
                    ij = iou(ra, rb)
                    cn = contain(ra, rb)
                    ar = area(ra) / area(rb) if area(rb) > 0 else 0.0
                    if cn >= 0.90 and (ar >= 1.5 or ar <= 1 / 1.5):
                        kind = "1:1_nested"
                    elif ij >= 0.80:
                        kind = "1:1_aligned"
                    else:
                        kind = "1:1_partial"
                elif na == 1 and nb > 1:
                    kind = "1:N"
                elif na > 1 and nb == 1:
                    kind = "N:1"
                elif na > 1 and nb > 1:
                    kind = "N:M"
                elif nb == 0:
                    kind = "1:0_orphan_a"
                else:
                    kind = "0:1_orphan_b"
                kinds[kind] = kinds.get(kind, 0) + 1
                D["comp"] += 1
                D["blocks"] += na + nb
                if not kind.startswith("1:1"):
                    D["non11"] += 1
                    D["blocks_non11"] += na + nb
                rec = {"doc_id": Pp["doc_id"], "discipline": disc,
                       "ver_a": Pp["ver_a"], "ver_b": Pp["ver_b"],
                       "pdf_a": Pp["pdf_a"], "pdf_b": Pp["pdf_b"],
                       "page_a": pa, "page_b": pb, "kind": kind,
                       "rot_a": rot_a, "rot_b": rot_b, "rot_mismatch": rot_mismatch,
                       "n_a": na, "n_b": nb,
                       "blocks_a": [{"id": la[i].block_id, "coords_px": list(la[i].coords_px),
                                     "page_px": [la[i].page_px_w, la[i].page_px_h],
                                     "page_index": la[i].page_index,
                                     "cat": la[i].category_code, "norm": [round(v, 5) for v in RA[i]]}
                                    for i in A],
                       "blocks_b": [{"id": lb[j].block_id, "coords_px": list(lb[j].coords_px),
                                     "page_px": [lb[j].page_px_w, lb[j].page_px_h],
                                     "page_index": lb[j].page_index,
                                     "cat": lb[j].category_code, "norm": [round(v, 5) for v in RB[j]]}
                                    for j in B]}
                if na and nb:
                    ua = (min(RA[i][0] for i in A), min(RA[i][1] for i in A),
                          max(RA[i][2] for i in A), max(RA[i][3] for i in A))
                    ub = (min(RB[j][0] for j in B), min(RB[j][1] for j in B),
                          max(RB[j][2] for j in B), max(RB[j][3] for j in B))
                    rec["union_iou"] = round(iou(ua, ub), 4)
                    rec["union_contain"] = round(contain(ua, ub), 4)
                    rec["union_area_ratio"] = round(area(ua) / area(ub), 4) if area(ub) else None
                    rec["sum_area_a"] = round(sum(area(RA[i]) for i in A), 6)
                    rec["sum_area_b"] = round(sum(area(RB[j]) for j in B), 6)
                comp_rows.append(rec)
                if kind in ("1:N", "N:1", "N:M", "1:1_nested", "1:1_partial"):
                    nested_rows.append(rec)
        F.clear_caches()
        print(f"[{S['version_pairs']}] {disc}/{Pp['doc_id']} pages={D['pages']} comps={D['comp']}", flush=True)

    with open(ART / "scope_components.jsonl", "w", encoding="utf-8") as fh:
        for r in comp_rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    tot_comp = sum(kinds.values())
    matched = {k: v for k, v in kinds.items() if not k.endswith("orphan_a") and not k.endswith("orphan_b")}
    tot_matched = sum(matched.values())
    out = {
        "schema_version": "scope_relation_census/1",
        "research_only": True,
        "params": {"edge": f"IoU(page-normalised bbox) > {EDGE_IOU}",
                   "page_map": "R3 text-Jaccard arbiter (mine_pagematch.json)",
                   "nested_rule": "containment>=0.90 and area ratio outside [1/1.5, 1.5]",
                   "aligned_rule": "IoU>=0.80"},
        "summary": S,
        "kinds": kinds,
        "shares_of_all_components": {k: round(v / tot_comp, 4) for k, v in kinds.items()},
        "shares_of_matched_components": {k: round(v / tot_matched, 4) for k, v in matched.items()},
        "sensitivity_edge_threshold": sens,
        "per_discipline": per_disc,
    }
    json.dump(out, open(ART / "scope_relation_census.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({"kinds": kinds, "summary": S}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
