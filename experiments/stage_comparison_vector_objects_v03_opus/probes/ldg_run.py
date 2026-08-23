# -*- coding: utf-8 -*-
"""[CF] driver for probe `ldg`: ledger contract + expert phrases against exact truth.

    python probes/ldg_run.py <shard> <nshards>

One row per (carrier, instance, export-noise).  Negatives carry the SAME export noise as
positives, so a positive and its control differ by exactly one thing (the loc protocol).
"""
from __future__ import annotations
import json
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
import ldg_ledger as LDG        # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402

MAX_SEG = 30000
NOISES = ("none", "round025")

# instance -> (cf_id, kwargs, ground truth phrases {phrase_id: count})
PLAN = [
    ("NEG", None, {}, {}),
    ("A1_path_split", "A1_path_split", {}, {}),
    ("D1_text_edit", "D1_text_edit", {}, {}),
    ("D3_label_rename", "D3_label_rename", {}, {}),
    ("C1_remove_object@small", "C1_remove_object", {"bucket": "small"}, {"OBJECT_REMOVED": 1}),
    ("C2_add_object@small", "C2_add_object", {"bucket": "small"}, {"OBJECT_ADDED": 1}),
    ("C2x2_same_object", "*C2x2", {}, {"ADDED_SAME_KIND": 2}),
    ("C3_move_object@small", "C3_move_object", {"bucket": "small", "frac": 0.01},
     {"CONFIG_CHANGED": 1}),
    ("C6_reshape_object@small", "C6_reshape_object", {"bucket": "small"},
     {"CONFIG_CHANGED": 1}),
    ("C7_split_object", "C7_split_object", {}, {"CONFIG_CHANGED": 1}),
    ("C8_merge_objects", "C8_merge_objects", {}, {"CONFIG_CHANGED": 1}),
    ("C9_add_branch", "C9_add_branch", {}, {"BRANCH_ADDED": 1}),
    ("C9x2_add_two_branches", "*C9x2", {}, {"BRANCH_ADDED": 2}),
    ("C10_remove_opening", "C10_remove_opening", {}, {"OPENING_REMOVED": 1}),
]


def _c2_same_object(ex, ol):
    """Two copies of the SAME motif, both produced by the module's own C2.

    C2 duplicates ONE object per call and the object is chosen by a seed derived from
    `key`, so the pair "two copies of one motif" is obtained by running C2 under many
    keys and composing the two runs that happened to duplicate the same object.  The
    geometry is the module's; only the composition is here.
    """
    runs = {}
    pick = None
    for k in range(60):
        try:
            ex_k, m = C.apply(ex, ol, "C2_add_object", key=f"ldg{k}", bucket="small")
        except C.CFNotApplicable:
            continue
        cid = m["expected_ledger"][0].get("copy_of")
        runs.setdefault(cid, []).append((ex_k, m))
        if len(runs[cid]) == 2:
            (e1, m1), (e2, m2) = runs[cid]
            b1, b2 = m1["change_bbox_pt"], m2["change_bbox_pt"]
            if not (b1[0] > b2[2] or b2[0] > b1[2] or b1[1] > b2[3] or b2[1] > b1[3]):
                runs[cid] = runs[cid][:1]     # the two copies collide, keep looking
                continue
            pick = (e1, m1, e2, m2)
            break
    if pick is None:
        raise C.CFNotApplicable("no two C2 runs duplicated the same object")
    e1, m1, e2, m2 = pick
    n0 = len(ex.segments)
    segs = [dict(s) for s in e1.segments] + [dict(s) for s in e2.segments[n0:]]
    C._renumber(segs)
    ex2 = C._clone(ex, segments=segs, prov={"cf": "C2x2_same_object"})
    b1, b2 = m1["change_bbox_pt"], m2["change_bbox_pt"]
    man = {"cf_class": "C", "cf_id": "C2x2_same_object",
           "compound_of": ["C2_add_object", "C2_add_object"],
           "copy_of": m1["expected_ledger"][0].get("copy_of"),
           "change_bbox_pt": [min(b1[0], b2[0]), min(b1[1], b2[1]),
                              max(b1[2], b2[2]), max(b1[3], b2[3])],
           "change_bboxes_pt": [b1, b2],
           "touched_objects": (m1.get("touched_objects") or []) + (m2.get("touched_objects") or []),
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": (m1.get("expected_ledger") or []) + (m2.get("expected_ledger") or [])}
    return ex2, man


def _compound(ex, ol, kind):
    """Two module-produced counterfactuals in a row (the module is still the only source
    of a counterfactual).  Manifest = union of the two."""
    if kind == "C2x2":
        return _c2_same_object(ex, ol)
    cf = "C9_add_branch"
    kw = {}
    ex1, m1 = C.apply(ex, ol, cf, key="ldgA", **kw)
    ol1 = O.build_objects(ex1)
    ex2, m2 = C.apply(ex1, ol1, cf, key="ldgB", **kw)
    b1, b2 = m1["change_bbox_pt"], m2["change_bbox_pt"]
    man = {"cf_class": "C", "cf_id": kind, "compound_of": [m1["cf_id"], m2["cf_id"]],
           "change_bbox_pt": [min(b1[0], b2[0]), min(b1[1], b2[1]),
                              max(b1[2], b2[2]), max(b1[3], b2[3])],
           "change_bboxes_pt": [b1, b2],
           "touched_objects": (m1.get("touched_objects") or []) + (m2.get("touched_objects") or []),
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": (m1.get("expected_ledger") or []) + (m2.get("expected_ledger") or [])}
    return ex2, man


def _hit(bbs, exp_list):
    """Does a phrase point at the true change?  (max overlap over its boxes x truth)"""
    best = 0.0
    for b in bbs:
        for e in exp_list:
            best = max(best, L._ov(b, e))
    return round(best, 3)


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    carriers = [c for c in pick_carriers() if c["n_seg"] <= MAX_SEG]
    outdir = ART / "ldg_runs"
    outdir.mkdir(exist_ok=True)
    out = open(outdir / f"cf_{shard}.jsonl", "w", encoding="utf-8")
    for ci, r in enumerate(carriers):
        if ci % nsh != shard:
            continue
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            ol = O.build_objects(ex)
        except Exception as e:
            print("CARRIER FAIL", r["block_id"], repr(e), flush=True)
            continue
        if not ex.segments:
            continue
        base = {"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
                "discipline": r["discipline"], "bucket": r["bucket"], "cls": r["cls"],
                "n_seg": len(ex.segments), "n_obj": len(ol.objects), "S": round(ol.S, 3),
                "n_text": len(ex.texts)}
        made = {}
        for inst, cf, kw, truth in PLAN:
            if cf is None:
                made[inst] = (ex, {"cf_id": None, "cf_class": None, "change_bbox_pt": None},
                              truth)
                continue
            try:
                if cf.startswith("*"):
                    ex2, man = _compound(ex, ol, cf[1:])
                else:
                    ex2, man = C.apply(ex, ol, cf, **kw)
                made[inst] = (ex2, man, truth)
            except C.CFNotApplicable as e:
                out.write(json.dumps({**base, "inst": inst, "skip": str(e)},
                                     ensure_ascii=False) + "\n")
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst, "error": repr(e)},
                                     ensure_ascii=False) + "\n")
        for noise in NOISES:
            for inst, (ex2, man, truth) in made.items():
                t0 = time.time()
                try:
                    exB = L.noisy(ex2, noise, seed=20260823)
                    LA, LB, meta = L.layers(ex, exB)
                    raw = LDG.raw_ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                    ldg = LDG.build(ex, exB, led=raw)              # production floor
                    phs = LDG.phrases(ldg)
                    bad = LDG.validate(ldg)
                    S_l = raw["S"]
                    low = LDG.build(ex, exB, led=raw, min_change_len_pt=2.0 * S_l)
                    phs_low = LDG.phrases(low)
                except Exception as e:
                    out.write(json.dumps({**base, "inst": inst, "noise": noise,
                                          "error": repr(e),
                                          "tb": traceback.format_exc()[-400:]},
                                         ensure_ascii=False) + "\n")
                    continue
                exp = man.get("change_bboxes_pt") or (
                    [man["change_bbox_pt"]] if man.get("change_bbox_pt") else [])
                ch = ldg["changes"]
                row = {**base, "inst": inst, "noise": noise,
                       "cf_id": man.get("cf_id"), "cf_class": man.get("cf_class"),
                       "truth": truth, "exp_bboxes": [[round(v, 2) for v in b] for b in exp],
                       "S_ledger": ldg["S"], "L_min_pt": ldg["L_min_pt"],
                       "n_records_raw": ldg["n_records_raw"], "n_changes": len(ch),
                       "types": [c["type"] for c in ch],
                       "shapes": [c.get("shape") for c in ch],
                       "changes": [{"type": c["type"], "shape": c.get("shape"),
                                    "bbox": [round(v, 2) for v in f["_bbox"]],
                                    "len": round(f["_len"], 2),
                                    "on_target": _hit([f["_bbox"]], exp) if exp else 0.0,
                                    "obj_b": (c["object_before"] or {}).get("object_id"),
                                    "obj_a": (c["object_after"] or {}).get("object_id"),
                                    "welded": c.get("welded"),
                                    "contact": next((e for e in c["evidence"]
                                                     if e["kind"] == "contact"), None),
                                    "welded": c.get("welded"),
                             "contact": next((e for e in c["evidence"]
                                              if e["kind"] == "contact"), None),
                             "ev": [e["kind"] for e in c["evidence"]],
                                    "att": next((e for e in c["evidence"]
                                                 if e["kind"] == "attachment"), None)}
                                   for c, f in zip(ch, ldg["_full"])][:60],
                       "phrases": [{"id": p["id"], "text": p["text"], "n": p["n"],
                                    "ink_pt": p["ink_pt"],
                                    "on_target": _hit(p["bboxes"], exp) if exp else 0.0}
                                   for p in phs],
                       "validate_violations": bad,
                       "n_changes_low": len(low["changes"]),
                       "L_min_low_pt": low["L_min_pt"],
                       "phrases_low": [{"id": p["id"], "text": p["text"], "n": p["n"],
                                        "on_target": _hit(p["bboxes"], exp) if exp else 0.0}
                                       for p in phs_low],
                       "shapes_low": [c.get("shape") for c in low["changes"]],
                       "scalar": ldg["scalar"]["ink_similarity"],
                       "t_sec": round(time.time() - t0, 2)}
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
            out.flush()
        print("done", r["block_id"], r["bucket"], len(ex.segments), flush=True)
    out.close()


if __name__ == "__main__":
    main()
