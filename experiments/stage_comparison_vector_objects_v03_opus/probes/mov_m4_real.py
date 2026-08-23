# -*- coding: utf-8 -*-
"""M4 / M4b / M5 on REAL pairs [REAL].

    python probes/mov_m4_real.py bench   [--shard i --of k]
    python probes/mov_m4_real.py fallback
    python probes/mov_m4_real.py border  [--shard i --of k] [--n 120]

`bench`    — the 33 eye-confirmed pairs of `mine_pairs.json` (cross-revision only:
             the corpus has NO P->RD pairs, probe `pd`).
`fallback` — the 7 eye-confirmed R<->R pairs of `pd_block_pairs.json`
             (two corpora of the same project, same stage).
`border`   — a larger sample of ordinary cross-revision pairs, split into
             "all big residual components touch the crop border" (mine M5) and an
             interior-difference control, to measure the attribution of M4b.

For every pair three transforms are scored on the SAME evidence (unmatched ink share):
  * `anchor`   — the object-anchor estimate of mov_align (what the probe proposes),
  * `bbox_org` — align the two crop frames by their origin (s = 1),
  * `bbox_fit` — fit the crop frames to each other (isotropic s from the frame sizes).
This is the measurement behind "align on objects, not on the bbox".
"""
from __future__ import annotations
import argparse, hashlib, json, math, random, sys, time, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC          # noqa: E402
import mov_align as MA           # noqa: E402
import grp_common as G           # noqa: E402
import v03_foundation as F       # noqa: E402
import v03_objects as O          # noqa: E402

ART = MC.ART
ROOT = G.ROOT
OUT = ART / "mov_runs"
MIN_SHARE = 1e-4
SEG_CAP = 130000


def side_extract(side):
    rj = side["result_json"]
    rj = rj if Path(rj).is_absolute() else str(ROOT / rj)
    for pb in F.iter_prepared_blocks(rj):
        if pb.block_id == side["block_id"]:
            return F.extract_block(pb.pdf_path, pb.page_index, pb.coords_px,
                                   pb.page_px_w, pb.page_px_h)
    raise RuntimeError(f"block {side['block_id']} not found in {rj}")


def raw_extract(pdf, page_index, coords_px, page_px):
    p = pdf if Path(pdf).is_absolute() else str(ROOT / pdf)
    return F.extract_block(p, int(page_index), coords_px, page_px[0], page_px[1])


def bbox_transforms(fa, fb):
    """The two naive frame-based alignments, for comparison against the anchor estimate."""
    wa, ha = fa[2] - fa[0], fa[3] - fa[1]
    wb, hb = fb[2] - fb[0], fb[3] - fb[1]
    org = MA.Sim(1.0, 0, fb[0] - fa[0], fb[1] - fa[1])
    s = 0.5 * (wb / max(wa, 1e-9) + hb / max(ha, 1e-9))
    cax, cay = (fa[0] + fa[2]) / 2, (fa[1] + fa[3]) / 2
    cbx, cby = (fb[0] + fb[2]) / 2, (fb[1] + fb[3]) / 2
    fit = MA.Sim(s, 0, cbx - s * cax, cby - s * cay)
    return {"bbox_org": org, "bbox_fit": fit}


def ink_score(exA, exB, T, S, inter, mode="strict"):
    r = MA.ink_changes(exA.segments, exB.segments, T, S, inter, MA.DEFAULTS,
                       mode=mode, max_seg=SEG_CAP)
    if "skipped" in r:
        return {"skipped": r["skipped"]}
    return {k: r[k] for k in ("unmatched_ink_share_a", "unmatched_ink_share_b",
                              "moved_ink_share_a", "lost_ink_share_a", "new_ink_share_b",
                              "border_ink_share_a", "border_ink_share_b",
                              "n_border_clusters", "n_lost_clusters", "n_new_clusters",
                              "n_moved_clusters")}


def comparable_share(segs, T, inter):
    """Share of a side's ink length that lies inside the comparable region (the frame
    intersection).  Ink outside it can never be called added / removed / moved, so this
    number is the honest denominator of any verdict about the pair."""
    tot = ins = 0.0
    x0, y0, x1, y1 = inter
    for s in segs:
        L = s["len"]
        tot += L
        mx = (s["p0"][0] + s["p1"][0]) / 2
        my = (s["p0"][1] + s["p1"][1]) / 2
        if T is not None:
            mx, my = T((mx, my))
        if x0 <= mx <= x1 and y0 <= my <= y1:
            ins += L
    return round(ins / tot, 5) if tot > 0 else None


def findings(out, tot_a):
    led = out.get("ledger_unified", [])
    f = [l for l in led if l["type"] in ("MOVED_INK", "REMOVED_INK", "ADDED_INK")
         and l["ink_len"] / max(tot_a, 1e-9) > MIN_SHARE]
    b = [l for l in led if l["type"] == "BORDER_UNCERTAIN"]
    return f, b


def one_pair(meta, exA, exB, *, do_naive=True, modes=("strict",)):
    row = dict(meta)
    t0 = time.time()
    row["n_seg_a"], row["n_seg_b"] = len(exA.segments), len(exB.segments)
    row["n_txt_a"], row["n_txt_b"] = len(exA.texts), len(exB.texts)
    fa, fb = MC.frame_of(exA), MC.frame_of(exB)
    row["frame_a"] = [round(v, 2) for v in fa]
    row["frame_b"] = [round(v, 2) for v in fb]
    row["frame_wh_a"] = [round(fa[2] - fa[0], 2), round(fa[3] - fa[1], 2)]
    row["frame_wh_b"] = [round(fb[2] - fb[0], 2), round(fb[3] - fb[1], 2)]
    if not exA.segments or not exB.segments:
        row["status"] = "NO_VECTOR"
        row["verdict"] = "UNKNOWN"
        row["t_sec"] = round(time.time() - t0, 1)
        return row
    out, rep, LA, LB = MC.compare(exA, exB, modes=modes,
                                  max_seg_ink=SEG_CAP)
    row["status"] = out.get("status")
    row["reason"] = out.get("reason")
    row["verdict"] = out.get("verdict")
    row["n_obj_a"], row["n_obj_b"] = out.get("n_obj_a"), out.get("n_obj_b")
    row["S_a"], row["S_b"] = out.get("S_a"), out.get("S_b")
    row["estimate"] = {k: (out.get("estimate") or {}).get(k)
                       for k in ("inliers", "inlier_ratio", "n_candidates",
                                 "theta_free_deg", "s_free", "ambiguous",
                                 "second_consensus", "reason")}
    if out.get("status") == "ALIGNMENT_UNAVAILABLE":
        row["t_sec"] = round(time.time() - t0, 1)
        return row
    g = out.get("global_transform") or out.get("transform")
    row["transform"] = g
    row["transform_anchors_only"] = out.get("transform_anchors_only")
    row["t_norm_pt"] = round(math.hypot(g["tx"], g["ty"]), 4)
    row["s_dev"] = round(abs(g["s"] - 1.0), 6)
    row["theta"] = g["theta"]
    row["residual"] = out.get("residual")
    row["obj_counts"] = out.get("counts")
    row["frame_overlap_share"] = out.get("frame_overlap_share")
    row["ink"] = out.get("ink")
    tot_a = sum(s["len"] for s in exA.segments)
    row["ink_len_a"] = round(tot_a, 1)
    f, b = findings(out, tot_a)
    row["n_findings"] = len(f)
    row["n_border_entries"] = len(b)
    row["findings_top"] = sorted(f, key=lambda l: -l["ink_len"])[:10]
    row["border_top"] = sorted(b, key=lambda l: -l["ink_len"])[:5]
    row["finding_types"] = sorted({l["type"] for l in f})
    Tg = MA.Sim(g["s"], g["theta"], g["tx"], g["ty"])
    inter = out.get("frame_intersection")
    if inter:
        row["comparable_share_a"] = comparable_share(exA.segments, Tg, inter)
        row["comparable_share_b"] = comparable_share(exB.segments, None, inter)
    # ---- naive frame alignments on the same evidence
    if do_naive and max(len(exA.segments), len(exB.segments)) <= SEG_CAP:
        S = max(row["S_a"] or 1.0, row["S_b"] or 1.0)
        Tanc = MA.Sim(g["s"], g["theta"], g["tx"], g["ty"])
        row["naive"] = {}
        for name, T in bbox_transforms(fa, fb).items():
            inter = MA._frame_intersection(fa, fb, T)
            try:
                row["naive"][name] = ink_score(exA, exB, T, S, inter)
                row["naive"][name]["transform"] = T.as_dict()
                row["naive"][name]["dt_from_anchor_pt"] = round(
                    math.hypot(T.tx - Tanc.tx, T.ty - Tanc.ty), 3)
            except Exception as e:
                row["naive"][name] = {"error": repr(e)}
    row["t_sec"] = round(time.time() - t0, 1)
    return row


# ------------------------------------------------------------------ sources

def bench_rows(shard, of, only=None):
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    for i, p in enumerate(pairs):
        if only:
            if p["pair_id"] not in only:
                continue
        elif i % of != shard:
            continue
        meta = {"source": "benchmark", "pair_id": p["pair_id"], "discipline": p["discipline"],
                "doc_id": p["doc_id"], "classes": p["classes"],
                "expected": p["expected_verdict"],
                "expected_changed_objects": p.get("expected_changed_objects"),
                "label_confidence": p["label_confidence"],
                "human": p["human_expected_ru"],
                "ver_a": p["side_a"]["version"], "ver_b": p["side_b"]["version"]}
        try:
            exA = side_extract(p["side_a"]); exB = side_extract(p["side_b"])
        except Exception as e:
            yield dict(meta, error=repr(e))
            continue
        if max(len(exA.segments), len(exB.segments)) > SEG_CAP:
            yield dict(meta, status="SKIPPED_TOO_DENSE",
                       n_seg_a=len(exA.segments), n_seg_b=len(exB.segments))
            continue
        try:
            yield one_pair(meta, exA, exB)
        except Exception as e:
            yield dict(meta, error=repr(e), trace=traceback.format_exc()[-900:])


def _parse_pd(s):
    """'214/OV/13АВ-РД-ОВ1.1-К1 V1 v001' -> (doc_id, version)."""
    tail = s.split("/", 2)[2]
    parts = tail.rsplit(" ", 1)
    return parts[0], parts[1]


def _resolve_block(doc, ver, bid):
    """`pd_block_pairs.json` stores TRUNCATED / mistyped block ids (e.g. '9PHE-EHAF-UTC'
    where the corpus has '9PHE-EHAF-UTX', and several ids ending in an ellipsis).
    Resolve by unique prefix; refuse an ambiguous match rather than guess."""
    pb = G.prepared_block(doc, ver, bid)
    if pb is not None:
        return pb, "exact"
    rj = G.result_json_for(doc, ver)
    if rj is None:
        return None, "no result.json"
    pref = bid.replace("\u2026", "").rstrip("-")[:9]
    if len(pref) < 6:
        return None, "prefix too short"
    full = rj if Path(rj).is_absolute() else str(ROOT / rj)
    hit = [b for b in F.iter_prepared_blocks(full) if b.block_id.startswith(pref)]
    if len(hit) == 1:
        return hit[0], f"prefix:{pref}"
    return None, f"prefix:{pref} n={len(hit)}"


def fallback_rows(shard, of):
    data = json.load(open(ART / "pd_block_pairs.json", encoding="utf-8"))
    for i, p in enumerate(data["fallback_confirmed_pairs"]):
        if i % of != shard:
            continue
        da, va = _parse_pd(p["a"]["doc"])
        db, vb = _parse_pd(p["b"]["doc"])
        meta = {"source": "fallback_RR", "pair_id": f"RR{i:02d}", "axis": p["axis"],
                "doc_a": da, "ver_a": va, "doc_b": db, "ver_b": vb,
                "evidence": p.get("evidence"), "expected": None,
                "discipline": None, "classes": ["fallback_R_to_R"]}
        pa, ra = _resolve_block(da, va, p["a"]["block_id"])
        pb, rb = _resolve_block(db, vb, p["b"]["block_id"])
        meta["resolved"] = [ra, rb]
        if pa is None or pb is None:
            yield dict(meta, error=f"block not resolvable ({ra} / {rb})")
            continue
        meta["block_a"], meta["block_b"] = pa.block_id, pb.block_id
        meta["discipline"] = da.split("-")[0]
        try:
            exA, exB = G.extract(pa), G.extract(pb)
        except Exception as e:
            yield dict(meta, error=repr(e))
            continue
        if max(len(exA.segments), len(exB.segments)) > SEG_CAP:
            yield dict(meta, status="SKIPPED_TOO_DENSE",
                       n_seg_a=len(exA.segments), n_seg_b=len(exB.segments))
            continue
        try:
            yield one_pair(meta, exA, exB)
        except Exception as e:
            yield dict(meta, error=repr(e), trace=traceback.format_exc()[-900:])


def border_sample(n_border, n_ctrl, seed=20260823):
    """Split the 3 940 real pairs by mine's crop-border rule and sample both sides."""
    rows = []
    for line in open(ART / "mine_align2.jsonl", encoding="utf-8"):
        r = json.loads(line)
        a2 = r.get("align2")
        if not a2 or r.get("same_pdf"):
            continue
        sc = a2["scale_px_per_pt"]
        w = r["wh_pt_a"][0] * sc; h = r["wh_pt_a"][1] * sc
        mx = max(6.0, 0.02 * max(w, h))
        big = [c for c in a2["top_components"] if c["px"] >= 60]
        if not big:
            continue
        inter = []
        for c in big:
            x0, y0, x1, y1 = c["bbox_px"]
            if x0 < mx or y0 < mx or x1 > w - mx or y1 > h - mx:
                continue
            inter.append(c)
        pid = f"{r['discipline']}-{hashlib.sha1(('%s|%s|%s|%s|%s' % (r['doc_id'], r['ver_a'], r['ver_b'], r['block_a'], r['block_b'])).encode()).hexdigest()[:8]}"
        rows.append({"pair_id": pid, "r": r, "n_big": len(big), "n_int": len(inter),
                     "border_only": len(inter) == 0,
                     "int_px": sum(c["px"] for c in inter),
                     "big_px": sum(c["px"] for c in big)})
    rnd = random.Random(seed)
    bo = [x for x in rows if x["border_only"]]
    ct = [x for x in rows if not x["border_only"] and x["int_px"] >= 200]
    rnd.shuffle(bo); rnd.shuffle(ct)
    n_bo_all, n_ct_all = len(bo), len(ct)
    bo, ct = bo[:n_border], ct[:n_ctrl]
    n_bo_take, n_ct_take = len(bo), len(ct)
    # interleave the two groups so that a partial run is still a balanced sample
    mix = []
    while bo or ct:
        if bo:
            mix.append(bo.pop())
        if bo:
            mix.append(bo.pop())
        if ct:
            mix.append(ct.pop())
    return mix, {"n_pairs_with_big_components": len(rows),
                 "n_border_only": n_bo_all, "n_interior": n_ct_all,
                 "border_only_share": round(n_bo_all / max(1, len(rows)), 4),
                 "n_sampled_border": n_bo_take, "n_sampled_interior": n_ct_take}


def border_rows(shard, of, n):
    sample, stats = border_sample(n, n // 2)
    if shard == 0:
        json.dump(stats, open(ART / "mov_border_universe.json", "w"), indent=1)
    for i, x in enumerate(sample):
        if i % of != shard:
            continue
        r = x["r"]
        meta = {"source": "border_sample", "pair_id": x["pair_id"],
                "discipline": r["discipline"], "doc_id": r["doc_id"],
                "ver_a": r["ver_a"], "ver_b": r["ver_b"],
                "group": "border_only" if x["border_only"] else "interior",
                "n_big_components": x["n_big"], "n_interior_components": x["n_int"],
                "int_px": x["int_px"], "big_px": x["big_px"],
                "is_stamp": (r["cat_a"] == "stamp" or r["cat_b"] == "stamp"),
                "expected": None, "classes": []}
        try:
            exA = raw_extract(r["pdf_a"], r["page_index_a"], r["coords_a"], r["page_px_a"])
            exB = raw_extract(r["pdf_b"], r["page_index_b"], r["coords_b"], r["page_px_b"])
        except Exception as e:
            yield dict(meta, error=repr(e))
            continue
        if max(len(exA.segments), len(exB.segments)) > 25000:
            yield dict(meta, status="SKIPPED_TOO_DENSE",
                       n_seg_a=len(exA.segments), n_seg_b=len(exB.segments))
            continue
        try:
            yield one_pair(meta, exA, exB, do_naive=False)
        except Exception as e:
            yield dict(meta, error=repr(e), trace=traceback.format_exc()[-900:])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["bench", "fallback", "border"])
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    ap.add_argument("--n", type=int, default=120)
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    only = set(x for x in a.only.split(",") if x)
    gen = {"bench": lambda: bench_rows(a.shard, a.of, only),
           "fallback": lambda: fallback_rows(a.shard, a.of),
           "border": lambda: border_rows(a.shard, a.of, a.n)}[a.mode]()
    fh = open(OUT / (a.out or f"{a.mode}_{a.shard}.jsonl"), "w", encoding="utf-8")
    k = 0
    for row in gen:
        k += 1
        fh.write(json.dumps(row, ensure_ascii=False, default=float) + "\n")
        fh.flush()
        print(f"[{a.mode}.{a.shard}] {k} {row.get('pair_id')} {row.get('status')} "
              f"{row.get('verdict')} nf={row.get('n_findings')} "
              f"t={row.get('t_norm_pt')} {row.get('t_sec')}s {row.get('error','')}",
              flush=True)
    fh.close()


if __name__ == "__main__":
    main()
