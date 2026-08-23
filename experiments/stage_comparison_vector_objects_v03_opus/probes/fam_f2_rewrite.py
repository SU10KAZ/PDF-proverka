# -*- coding: utf-8 -*-
"""F2 [CF] — does the FAMILY partition survive a pure representation rewrite?

For each real prepared block: build the object layer and the family layer on the
original ink, apply rewrite A1..A6 to the SAME ink, rebuild both layers, and ask
two different questions:

  (1) PARTITION   — is the ink split into the same families?  Adjusted Rand Index
      of the family labelling induced on the original segments.  ARI, not "share
      of objects that kept their family", because the number of families changes.
  (2) CARDINALITY — pooling both sides into ONE clustering, how many families
      report a different member count?  This is the sentence "12 -> 14", and after
      a pure repackaging every such row is FALSE by construction.

Both directions of the GATEFIX lesson are covered: (1) is a completeness measure
(every segment of the block carries a label, unlabeled ink is counted), (2) is the
precision measure.
Usage: fam_f2_rewrite.py [n_blocks] [out.json]
"""
from __future__ import annotations
import json, random, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_common as C
import fam_family as FAM

REWRITES = ["A1_path_split", "A2_path_merge", "A3_curve_resample_down",
            "A3_curve_resample_up", "A4_circle_to_bezier", "A4b_circle_to_chords5",
            "A4c_circle_to_chords24", "A5_order_shuffle",
            "A6_round_0.01", "A6_round_0.1", "A6_round_0.25", "A6_round_0.5",
            "A0_identity"]
MAX_SEG = 60000
SEED = 20260823


def one_block(b, out_rows):
    pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
    if pb is None:
        return
    ex = G.extract(pb)
    segs0 = ex.segments
    if not segs0 or len(segs0) > MAX_SEG:
        return
    base = C.REWRITES["A0_identity"](segs0, random.Random(SEED))
    LA = G.layer_of(base, ex.texts)
    FA = FAM.build_families(LA)
    labA = C.ink_family_labels(LA, base, FA, len(segs0))
    row0 = {"block_id": b["block_id"], "doc_id": b["doc_id"], "version": b["version"],
            "discipline": b["discipline"], "cls": b["cls"], "bucket": b["bucket"],
            "n_seg": len(segs0), "n_obj": len(LA.objects),
            "n_fam": FA.stats["n_families"], "n_rep": FA.stats["n_repeated_families"],
            "S": round(LA.S, 3), "scale_src": LA.scale_source}
    for name in REWRITES:
        rng = random.Random(SEED)
        try:
            segsB = C.REWRITES[name](segs0, rng)
        except Exception as e:
            out_rows.append(dict(row0, rewrite=name, error=repr(e)))
            continue
        bite = G.rewrite_bite(name, segs0, segsB)
        LB = G.layer_of(segsB, ex.texts)
        FB = FAM.build_families(LB)
        labB = C.ink_family_labels(LB, segsB, FB, len(segs0))
        keep = [i for i in range(len(segs0)) if labA[i] >= 0 and labB[i] >= 0]
        a = FAM.ari([labA[i] for i in keep], [labB[i] for i in keep])
        # ---- pooled clustering: how many families claim a changed count?
        FP = FAM.build_families_pair(LA, LB)
        rows = FAM.family_deltas(FP)
        n_rep_pool = sum(1 for f in FP.families if len(f["members"]) >= 2)
        # ink-weighted share of the block that lies in a family with a false delta
        bad_len = 0.0
        for r in rows:
            bad_len += FP.families[r["family"]]["seg_len_sum"]
        tot_len = sum(o.get("seg_len", 0.0) for o in LA.objects) + \
                  sum(o.get("seg_len", 0.0) for o in LB.objects)
        out_rows.append(dict(row0, rewrite=name, bite=bite,
                             n_obj_b=len(LB.objects), n_fam_b=FB.stats["n_families"],
                             n_rep_b=FB.stats["n_repeated_families"],
                             S_b=round(LB.S, 3),
                             ari=round(a, 6) if a == a else None,
                             labeled_share=round(len(keep) / max(len(segs0), 1), 5),
                             same_family_share=round(
                                 sum(1 for i in keep if labA[i] == labB[i]) / max(len(keep), 1), 5)
                             if keep else None,
                             false_delta_rows=len(rows),
                             false_delta_max=max([abs(r["delta"]) for r in rows], default=0),
                             pooled_repeated_families=n_rep_pool,
                             false_delta_ink_share=round(bad_len / max(tot_len, 1e-9), 5),
                             top_false=[{"n_a": r["n_a"], "n_b": r["n_b"], "cls": r["cls"],
                                         "diag": r["diag_med"]} for r in rows[:5]]))


def _work(b):
    rows = []
    t0 = time.time()
    try:
        one_block(b, rows)
    except Exception:
        return b["block_id"], [], traceback.format_exc().splitlines()[-1]
    for r in rows:
        r["t_block_sec"] = round(time.time() - t0, 2)
    return b["block_id"], rows, None


def main():
    import multiprocessing as mp
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 270
    out_path = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f2_rewrite.jsonl")
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))["blocks"][:n]
    fh = open(out_path, "w", encoding="utf-8")
    done = 0
    with mp.Pool(8, maxtasksperchild=4) as pool:
        for bid, rows, err in pool.imap_unordered(_work, sample, chunksize=1):
            if err:
                print("ERR", bid, err, flush=True)
                continue
            if not rows:
                continue
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            fh.flush()
            done += 1
            a0 = [r for r in rows if r["rewrite"] == "A6_round_0.25"]
            print(done, bid[:12], rows[0]["discipline"], rows[0]["n_seg"], "obj",
                  rows[0]["n_obj"], "fam", rows[0]["n_fam"],
                  "| A6.25 ari", a0[0].get("ari") if a0 else None,
                  "false", a0[0].get("false_delta_rows") if a0 else None,
                  rows[0]["t_block_sec"], "s", flush=True)
    fh.close()


if __name__ == "__main__":
    main()
