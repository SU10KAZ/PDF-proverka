# -*- coding: utf-8 -*-
"""R3 — CAN THE TYPE GIVE AN ADDRESS?  ("this object, the one connected to X")

A relation is not a score.  Its only job in a comparator is to let a human sentence
point at the object that changed.  So the measure is not "does the graph match" but
"for a change whose ground truth we know exactly, does type T yield an address that is
non-empty, resolvable and unambiguous".

Ground truth: the counterfactual manifest (C1 remove / C2 add / C3 move / C9 branch).
The changed object is known by construction, not by detection.

Four nested shares are reported per type, always together:

    any     — the changed object has at least one relation of type T
    stable  — ... whose anchor also exists on the other side (else the address cannot
              be resolved by the reader: "connected to a thing that is not there")
    unique  — ... whose anchor is uniquely nameable inside the block (a motif that
              occurs 40 times is not an address)
    usable  = stable AND unique on the SAME relation

Usage:  rel_r3_address.py [shard nshards]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
import v03_counterfactual as CF
import cf_build_set as CB

PLAN = ([("C1_remove_object", {"bucket": b}) for b in ("tiny", "small", "large")] +
        [("C2_add_object", {"bucket": b}) for b in ("tiny", "small", "large")] +
        [("C3_move_object", {"bucket": b, "frac": f})
         for b in ("tiny", "small", "large") for f in (0.005, 0.02)] +
        [("C9_add_branch", {})])
MAX_OBJ = 4000


def motif_multiplicity(layer, eps=0.05):
    """How many objects share this object's motif (class + quantised descriptor + size)."""
    import math
    codes = {}
    for o in layer.objects:
        k = (o["cls"], tuple(int(round(v / eps)) for v in o["desc"]["vec"]),
             int(round(math.log(max(o["diag"], 1e-6)) / math.log(1.5))))
        codes[k] = codes.get(k, 0) + 1
        o["_motif"] = k
    return [codes[o["_motif"]] for o in layer.objects]


def text_multiplicity(texts):
    c = {}
    for t in texts:
        s = (t.get("text") or "").strip()
        c[s] = c.get(s, 0) + 1
    return c


def address_stats(layer, rels, x_ix, partner, mult, tcount):
    """For the changed object x_ix: per relation type, the four nested shares."""
    out = {}
    inc = {}
    for r in rels:
        for u in (r.get("a"), r.get("b")):
            if u == x_ix:
                inc.setdefault(r["type"], []).append(r)
                break
    for t in R.REL_TYPES:
        rs = inc.get(t, [])
        any_ = len(rs) > 0
        stable = unique = usable = False
        example = None
        for r in rs:
            if t == "LABEL_ANCHOR":
                s = (r.get("text") or "").strip()
                st = bool(s)                       # a text carries itself to the other side
                un = tcount.get(s, 0) == 1
            elif t == "LEADER_TO":
                other = r["b"] if r["a"] == x_ix else r["a"]
                s = (r.get("text") or "").strip()
                st = (partner[other] >= 0) if other is not None else False
                un = (tcount.get(s, 0) == 1) if s else (mult[other] == 1)
            else:
                other = r["b"] if r["a"] == x_ix else r["a"]
                if other is None:
                    continue
                st = partner[other] >= 0
                un = mult[other] == 1
            stable |= st
            unique |= un
            if st and un and not usable:
                usable = True
                example = {"type": t, "anchor": (r.get("text") if t == "LABEL_ANCHOR"
                                                 else layer.objects[r["b"] if r["a"] == x_ix
                                                                    else r["a"]]["object_id"])}
        out[t] = {"any": any_, "stable": stable, "unique": unique, "usable": usable,
                  "n": len(rs), "example": example}
    return out


def find_added(layer, ex, seg_ix):
    """The object on the AFTER side that carries the added segments."""
    want = set(seg_ix)
    best, bn = -1, 0
    for oi, o in enumerate(layer.objects):
        k = sum(1 for g in o["segments"] if g in want)
        if k > bn:
            best, bn = oi, k
    return best


def one_carrier(rec):
    row = {"carrier": rec, "runs": [], "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = C.G.extract(pb)
        if not ex.segments:
            row["error"] = "no geometry"; return row
        L0 = C.O.build_objects(ex)
        if not (4 <= len(L0.objects) <= MAX_OBJ):
            row["error"] = f"n_obj {len(L0.objects)}"; return row
        rel0 = R.build_relations(L0, ex)
        mult0 = motif_multiplicity(L0)
        tc0 = text_multiplicity(ex.texts)
        id2ix = {o["object_id"]: i for i, o in enumerate(L0.objects)}
        row["n_obj"] = len(L0.objects); row["n_seg"] = len(ex.segments)
        for cf_id, kw in PLAN:
            tag = cf_id + "".join(f"@{v}" for v in kw.values())
            try:
                ex2, man = CF.apply(ex, L0, cf_id, **kw)
            except Exception as e:
                row["runs"].append({"tag": tag, "skipped": repr(e)[:100]}); continue
            L2 = C.O.build_objects(ex2, S_override=L0.S)
            rel2 = R.build_relations(L2, ex2)
            a2b, b2a, ov = C.match_by_provenance(L0, ex.segments, L2, ex2.segments)
            oid = man["touched_objects"][0]["object_id"] if man["touched_objects"] else None
            if cf_id in ("C1_remove_object", "C3_move_object"):
                x = id2ix.get(oid, -1)
                if x < 0:
                    row["runs"].append({"tag": tag, "skipped": "touched object not found"})
                    continue
                st = address_stats(L0, rel0, x, a2b, mult0, tc0)
                side = "A"
            else:                                     # C2 / C9: the new thing lives on B
                mult2 = motif_multiplicity(L2)
                tc2 = text_multiplicity(ex2.texts)
                x = find_added(L2, ex2, man["changed_primitives"].get("added_segment_ix", []))
                if x < 0:
                    row["runs"].append({"tag": tag, "skipped": "added object not found"})
                    continue
                st = address_stats(L2, rel2, x, b2a, mult2, tc2)
                side = "B"
            row["runs"].append({"tag": tag, "cf": cf_id, "side": side,
                                "bucket": kw.get("bucket"), "frac": kw.get("frac"),
                                "area_frac": (man["touched_objects"][0]["area_frac_of_block"]
                                              if man["touched_objects"] else None),
                                "x_cls": (L0 if side == "A" else L2).objects[x]["cls"],
                                "addr": st})
    except Exception:
        row["error"] = traceback.format_exc()[-400:]
    return row


def main(shard, n):
    carriers = [r for i, r in enumerate(CB.pick_carriers()) if i % n == shard]
    out = []
    for k, rec in enumerate(carriers):
        t0 = time.time()
        r = one_carrier(rec)
        print(f"[{shard}] {k+1}/{len(carriers)} {rec['block_id']} "
              f"{round(time.time()-t0,1)}s err={r['error'] is not None}", flush=True)
        out.append(r)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r3_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
