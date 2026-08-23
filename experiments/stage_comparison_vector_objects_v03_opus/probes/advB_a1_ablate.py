# -*- coding: utf-8 -*-
"""advB attack #1 — is the OBJECT LAYER load-bearing for the verdict at all?

Three comparators on the same real benchmark pairs, same registration, same
thresholds:

  OBJ   = the track's ledger (loc_common.ledger) — objects built, shared S, records
          keyed on unmatched ink, objects used to NAME records.
  SEG   = the SAME unmatched-ink clustering with the object layer REMOVED
          (no build_objects call at all; S taken straight from the raw medians).
  DUMB  = a single scalar: share of ink that found no partner.

  and three OBJECT-KEYED ledgers, which is what "compare by objects" would mean
  literally:  count / object_id multiset / boundary churn.

If OBJ == SEG on every pair, the object layer contributes nothing to the verdict.
"""
from __future__ import annotations
import json, math, statistics, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G      # noqa
import grp_match as M       # noqa
import loc_common as L      # noqa
import v03_objects as O     # noqa

TOL = 0.8


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


def raw_scale(exA, exB):
    """S with NO object layer involved — same formula, computed inline."""
    def one(ex):
        lens = sorted(s["len"] for s in ex.segments)
        sg = statistics.median(lens) if lens else 0.0
        sz = [t["size"] for t in ex.texts if t.get("size", 0) > 0]
        return statistics.median(sz) if len(sz) >= 5 else (sg or 1.0)
    return max(one(exA), one(exB))


def seg_only_records(exA, exB, off, S):
    """Ledger with the object layer deleted.  Same ink correspondence, same
    clustering radius, same boundary flag — objects simply never built."""
    fa, mla, tla = L.unmatched_mask(exA.segments, exB.segments, off, TOL)
    fb, mlb, tlb = L.unmatched_mask(exB.segments, exA.segments, (-off[0], -off[1]), TOL)
    items = []
    for k, f in enumerate(fa):
        if f < 0.999:
            s = exA.segments[k]
            items.append((s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], ("A", k, 1.0 - f)))
    for k, f in enumerate(fb):
        if f < 0.999:
            s = exB.segments[k]
            items.append((s["p0"][0] + off[0], s["p0"][1] + off[1],
                          s["p1"][0] + off[0], s["p1"][1] + off[1], ("B", k, 1.0 - f)))
    r = max(1.5 * S, 3.0)
    groups = L._cluster(items, r) if items else []
    fr = exA.frame["clip_display"]
    padx = max(2.0, 0.02 * (fr[2] - fr[0]))
    pady = max(2.0, 0.02 * (fr[3] - fr[1]))
    recs = []
    for g in groups:
        pts, lenA, lenB = [], 0.0, 0.0
        for i in g:
            x0, y0, x1, y1, (sd, k, u) = items[i]
            pts += [(x0, y0), (x1, y1)]
            if sd == "A":
                lenA += exA.segments[k]["len"] * u
            else:
                lenB += exB.segments[k]["len"] * u
        bb = L._bbox(pts)
        at_b = bool(bb[0] <= fr[0] + padx or bb[1] <= fr[1] + pady or
                    bb[2] >= fr[2] - padx or bb[3] >= fr[3] - pady)
        recs.append({"bbox_pt": [round(v, 3) for v in bb], "change_len": lenA + lenB,
                     "at_boundary": at_b})
    recs.sort(key=lambda x: -x["change_len"])
    sim = (mla + mlb) / max(tla + tlb, 1e-9)
    return recs, sim, (tla + tlb) - (mla + mlb)


def verdict_from(recs, thr, interior_only=True):
    xs = [r for r in recs if (not interior_only or not r["at_boundary"])]
    top = max([r["change_len"] for r in xs], default=0.0)
    return ("GRAPHIC_CHANGE" if top >= thr else "NO_GRAPHIC_CHANGE"), top


def main():
    want = set(sys.argv[1:])
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    import os
    outp = ART / "advB" / os.environ.get("ADVB_OUT", "a1_rows.jsonl")
    outp.parent.mkdir(exist_ok=True, parents=True)
    done = set()
    if outp.exists():
        for line in open(outp, encoding="utf-8"):
            done.add(json.loads(line).get("pair_id"))
    sink = open(outp, "a", encoding="utf-8")
    for p in pairs:
        pid = p["pair_id"]
        if want and pid not in want:
            continue
        if not want and pid in done:
            continue
        t0 = time.time()
        row = {"pair_id": pid, "discipline": p["discipline"], "classes": p["classes"],
               "expected": p["expected_verdict"], "conf": p["label_confidence"]}
        try:
            exA, exB = side(p["side_a"]), side(p["side_b"])
            if not exA.segments or not exB.segments:
                raise RuntimeError("no vector geometry on one side")
            clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
            base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
            sd = (p.get("screen_signals") or {}).get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), base, (float(sd[0]), float(sd[1])),
                     (base[0] + float(sd[0]), base[1] + float(sd[1]))}
            dx, dy, score = M.register(exA.segments, exB.segments, seeds)
            off = (dx, dy)
            S_raw = raw_scale(exA, exB)
            # --- SEG (no object layer at all)
            t1 = time.time()
            seg_recs, sim, unmatched_len = seg_only_records(exA, exB, off, S_raw)
            t_seg = time.time() - t1
            # --- OBJ (track's ledger)
            t1 = time.time()
            LA, LB, meta = L.layers(exA, exB)
            led = L.ledger(exA, exB, off=off, LA=LA, LB=LB, meta=meta)
            t_obj = time.time() - t1
            S_obj = meta["S_shared"]
            thr_obj = max(2 * S_obj, 3.0)
            thr_seg = max(2 * S_raw, 3.0)
            v_obj, top_obj = verdict_from(led["records"], thr_obj)
            v_seg, top_seg = verdict_from(seg_recs, thr_seg)
            # --- object-keyed comparators
            ca, cb = LA.counts(), LB.counts()
            n_obj_a, n_obj_b = len(LA.objects), len(LB.objects)
            ids_a = sorted(o["object_id"] for o in LA.objects)
            ids_b = sorted(o["object_id"] for o in LB.objects)
            from collections import Counter
            ma, mb = Counter(ids_a), Counter(ids_b)
            id_mismatch = sum((ma - mb).values()) + sum((mb - ma).values())
            rows = M.churn_rows(LA, exA.segments, LB, exB.segments, off, tol=TOL)
            cls = M.classify(rows)
            row.update({
                "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
                "off": [round(dx, 3), round(dy, 3)], "reg_score": round(score, 4),
                "S_raw": round(S_raw, 3), "S_obj": round(S_obj, 3),
                "sim_seg": round(sim, 6), "unmatched_len": round(unmatched_len, 2),
                "unmatched_share": round(1 - sim, 8),
                "n_rec_obj": led["n_records"], "n_rec_obj_int": led["n_records_interior"],
                "n_rec_seg": len(seg_recs),
                "n_rec_seg_int": sum(1 for r in seg_recs if not r["at_boundary"]),
                "top_obj": round(top_obj, 2), "top_seg": round(top_seg, 2),
                "v_obj": v_obj, "v_seg": v_seg,
                "n_obj_a": n_obj_a, "n_obj_b": n_obj_b, "d_obj": n_obj_b - n_obj_a,
                "counts_a": ca, "counts_b": cb,
                "objid_mismatch": id_mismatch,
                "objid_mismatch_share": round(id_mismatch / max(1, n_obj_a + n_obj_b), 4),
                "churn_1to1": round(cls.get("one_to_one", 0.0), 5),
                "t_seg": round(t_seg, 2), "t_obj": round(t_obj, 2),
                "sec": round(time.time() - t0, 1),
            })
            # bbox agreement of the top interior record between OBJ and SEG
            io = [r for r in led["records"] if not r["at_boundary"]]
            iso = [r for r in seg_recs if not r["at_boundary"]]
            if io and iso:
                a, b = io[0]["bbox_pt"], iso[0]["bbox_pt"]
                inter = max(0.0, min(a[2], b[2]) - max(a[0], b[0])) * \
                        max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
                ua = (a[2] - a[0]) * (a[3] - a[1]); ub = (b[2] - b[0]) * (b[3] - b[1])
                row["top_iou"] = round(inter / max(ua + ub - inter, 1e-9), 4)
        except Exception as e:
            row["error"] = repr(e)
        sink.write(json.dumps(row, ensure_ascii=False) + "\n"); sink.flush()
        print(f"{pid} {row.get('v_obj')} / {row.get('v_seg')} exp={row['expected']} "
              f"{row.get('sec')}s {row.get('error','')}", flush=True)


if __name__ == "__main__":
    main()
