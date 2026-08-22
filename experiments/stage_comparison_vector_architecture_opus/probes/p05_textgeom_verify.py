#!/usr/bin/env python3
"""Adversarial re-verification of the p05_textgeom (`txgeo`) headline claims.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.p05_textgeom_verify

Reads only: artifacts/txgeo_relations/line/*/left.json, artifacts/txgeo_dimension_check.json,
Track A descriptions (…/stage_comparison_vector_blocks/artifacts/descriptions) and this probe's
fresh descriptions.  Writes nothing.
"""
from __future__ import annotations

import collections
import json
import math
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
REL = ART / "txgeo_relations/line"
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
FRESH = ART / "txgeo_fresh_descriptions"

PURE = re.compile(r"^\d{2,5}$")
MM_PER_PT = 25.4 / 72.0
STANDARD = [1, 2, 5, 10, 20, 25, 50, 75, 100, 200, 250, 400, 500]
# the probe's own hardcoded per-block scales (probes/txgeo_confidence.py)
TRUSTED = {"ar_plan": 35.283, "ar_wall_sections": 17.644, "fresh_ar_lintels": 3.546,
           "fresh_kj_sections": 17.612, "ss_plan_dense": 35.269, "vk_nodes": 7.073}
# the nearest standard scale for each, used for a referent comparison that is NOT fitted per referent
TRUE_STD = {"ar_plan": 100, "ar_wall_sections": 50, "fresh_ar_lintels": 10,
            "fresh_kj_sections": 50, "ss_plan_dense": 100, "vk_nodes": 20}


def desc(block: str, side: str = "left") -> dict:
    p = TRACK_A / block / side / "vector_block.json"
    if not p.exists():
        p = FRESH / block / side / "vector_block.json"
    return json.loads(p.read_text(encoding="utf-8"))


def modal_scale(ratios, bin_factor: float = 1.02):
    """Verbatim copy of probes/txgeo_dimension_check.py::modal_scale."""
    if not ratios:
        return 0.0, 0
    logs = [math.log(r) / math.log(bin_factor) for r in ratios]
    counts = collections.Counter(int(round(l)) for l in logs)
    best_bin, _ = counts.most_common(1)[0]
    members = [r for r, l in zip(ratios, logs) if abs(l - best_bin) <= 1.0]
    return sum(members) / len(members), len(members)


def nearest_std(implied):
    if not implied:
        return None, None
    n = min(STANDARD, key=lambda s: abs(math.log(implied / s)))
    return n, abs(implied / n - 1.0) * 100


def build_samples():
    out = []
    for block, scale in TRUSTED.items():
        d = desc(block)
        anchors = {a["text_id"]: a for a in d["anchors"]}
        res = json.loads((REL / block / "left.json").read_text(encoding="utf-8"))
        for u in res["units"]:
            if not PURE.match(u["text"].strip()):
                continue
            rel = u["relations"].get("dimension_interval")
            if not rel or not rel.get("hit"):
                continue
            dn = [anchors[s]["distance_norm"] for s in u["span_ids"]
                  if s in anchors and anchors[s].get("distance_norm") is not None]
            cf = [anchors[s]["confidence"] for s in u["span_ids"] if s in anchors]
            out.append({
                "block": block,
                "correct": abs((float(u["text"]) / rel["measured_len_pt"]) / scale - 1.0) <= 0.02,
                "ratio": float(u["text"]) / rel["measured_len_pt"],
                "scale": scale,
                "ticks": rel.get("ticks_in_reach", 0),
                "cands": rel.get("candidates", 0),
                "centred": bool(rel.get("centred_on_interval")),
                "dn_v01": min(dn) if dn else None,
                "conf_v01": cf[0] if cf else None,
            })
    return out


def sec(t):
    print("\n" + "=" * 96 + f"\n{t}\n" + "=" * 96)


def main() -> None:
    S = build_samples()
    print(f"reproduced sample: n={len(S)} precision={sum(s['correct'] for s in S)/len(S):.3f}")

    sec("V1 — v0.1 anchors.confidence: the REAL field, whole corpus (claim 1)")
    print(f"{'block':24s} {'all texts':>9s} {'high':>6s} {'cand':>6s} {'none':>6s} | pure-int: {'high':>5s} {'cand':>5s}")
    blocks = [p.name for p in sorted(TRACK_A.iterdir())] + [p.name for p in sorted(FRESH.iterdir()) if p.is_dir()]
    for b in blocks:
        d = desc(b)
        texts = {t["id"]: t for t in d["texts"]}
        c, cpi = collections.Counter(), collections.Counter()
        for a in d["anchors"]:
            c[a["confidence"]] += 1
            t = texts.get(a["text_id"])
            if t and PURE.match((t.get("text") or "").strip()):
                cpi[a["confidence"]] += 1
        mark = "  <in TRUSTED-6>" if b in TRUSTED else ""
        print(f"{b:24s} {sum(c.values()):9d} {c['high']:6d} {c['candidate']:6d} {c['none']:6d} | "
              f"{cpi['high']:14d} {cpi['candidate']:5d}{mark}")
    print("\nv0.1 confidence on the SAME 374 units:", dict(collections.Counter(s["conf_v01"] for s in S)))

    sec("V2 — does v0.1's OWN distance_norm carry bits on the same 374? (claims 1 and 2)")
    have = [s for s in S if s["dn_v01"] is not None]
    ds = sorted(s["dn_v01"] for s in have)
    q = [ds[int(len(ds) * k / 5)] for k in range(1, 5)]
    buck = collections.defaultdict(lambda: [0, 0])
    for s in have:
        k = next((f"Q{i+1}(<={qq:.4f})" for i, qq in enumerate(q) if s["dn_v01"] <= qq), f"Q5(>{q[-1]:.4f})")
        buck[k][1] += 1
        buck[k][0] += s["correct"]
    for k in sorted(buck):
        print(f"   {k:22s} n={buck[k][1]:4d} prec={buck[k][0]/buck[k][1]:.3f}")
    srt = sorted(have, key=lambda s: -s["dn_v01"])
    for n, probe_p, label in ((175, 0.720, "gate ticks<=2 & centred"), (83, 0.867, "gate + single line")):
        base = sum(x["correct"] for x in srt[:n]) / n
        print(f"   matched coverage n={n}: probe {label} p={probe_p:.3f}  vs  v0.1 distance_norm top-{n} p={base:.3f}")

    sec("V3 — sample composition and per-block gate behaviour (claims 1-3)")
    print(f"{'block':22s} {'n':>4s} {'share':>6s} {'prec':>6s} | {'gate1 n':>7s} {'p':>6s} | {'gate2 n':>7s} {'p':>6s}")
    for b in TRUSTED:
        sub = [s for s in S if s["block"] == b]
        g1 = [s for s in sub if s["ticks"] <= 2 and s["centred"]]
        g2 = [s for s in g1 if s["cands"] == 1]
        f = lambda x: (f"{sum(t['correct'] for t in x)/len(x):.3f}" if x else "  -  ")
        print(f"{b:22s} {len(sub):4d} {len(sub)/len(S):6.3f} {f(sub):>6s} | {len(g1):7d} {f(g1):>6s} | {len(g2):7d} {f(g2):>6s}")
    ge3 = [s for s in S if s["ticks"] >= 3]
    print(f"\n   ticks>=3 aggregate: n={len(ge3)} correct={sum(s['correct'] for s in ge3)} "
          f"prec={sum(s['correct'] for s in ge3)/len(ge3):.4f}   (claim says 0.024; 0.024 is the 3-4 bucket alone)")
    print("\n   within-block ticks split (is `ticks` just a block-identity proxy?):")
    for b in TRUSTED:
        sub = [s for s in S if s["block"] == b]
        lo = [s for s in sub if s["ticks"] <= 2]
        hi = [s for s in sub if s["ticks"] >= 3]
        f = lambda x: (f"n={len(x):4d} p={sum(t['correct'] for t in x)/len(x):.3f}" if x else "n=   0        ")
        print(f"     {b:22s} ticks<=2 {f(lo)}   ticks>=3 {f(hi)}")

    sec("V4 — recall denominators (claim 3)")
    dim = json.loads((ART / "txgeo_dimension_check.json").read_text(encoding="utf-8"))
    pi6 = sum(r["pure_integer_texts"] for r in dim if r["block"] in TRUSTED)
    pi15 = sum(r["pure_integer_texts"] for r in dim)
    print(f"   pure-integer texts, 6 TRUSTED blocks: {pi6};  all 15 blocks: {pi15}")
    print(f"   dimension_interval hits (probe denominator): {len(S)}")
    print(f"   gate keeps 83 -> recall vs hits {83/len(S):.3f} ('78 % lost'); vs pure-int texts in the 6 blocks {83/pi6:.3f}; vs all 15 blocks {83/pi15:.3f}")
    print(f"   of the 83, 72 are correct -> verified-value yield {72/pi6:.3f} (6 blocks) / {72/pi15:.3f} (15 blocks)")

    sec("V5 — is the label just 'agrees with the ticks<=2 majority'? (claims 2-3)")
    for b in TRUSTED:
        sub = [s for s in S if s["block"] == b]
        a = [s["ratio"] for s in sub if s["ticks"] <= 2]
        z = [s["ratio"] for s in sub if s["ticks"] >= 3]
        ma, na = modal_scale(a)
        mz, nz = modal_scale(z)
        print(f"   {b:22s} ticks<=2 n={len(a):4d} mode={ma:8.3f} (1:{ma/MM_PER_PT:7.2f}, {na} members)"
              f"   ticks>=3 n={len(z):4d} mode={mz:8.3f} (1:{(mz/MM_PER_PT if mz else 0):7.2f}, {nz} members)")

    sec("V6 — REFERENT SHAPE re-done with ONE estimator for both referents (claim 4)")
    print("txgeo_referent_shape.txt has no generating script; its header says lineOnly is a MEDIAN")
    print("while the interval column equals modal_mm_per_pt from txgeo_dimension_check.json (a MODE).")
    print(f"\n{'block':22s} | {'lineOnly n':>10s} {'median':>8s} {'MODE':>8s} {'1:X':>8s} {'err%':>6s} | "
          f"{'interval n':>10s} {'MODE':>8s} {'1:X':>8s} {'err%':>6s}")
    lo_ok = lo_tot = iv_ok = iv_tot = 0
    for p in sorted(REL.glob("*/left.json")):
        b = p.parent.name
        res = json.loads(p.read_text(encoding="utf-8"))
        lo, iv = [], []
        for u in res["units"]:
            t = u["text"].strip()
            if not PURE.match(t):
                continue
            r1 = u["relations"].get("dimension_line_only")
            if r1 and r1.get("hit") and r1.get("measured_len_pt", 0) > 1e-6:
                lo.append(float(t) / r1["measured_len_pt"])
            r2 = u["relations"].get("dimension_interval")
            if r2 and r2.get("hit") and r2.get("measured_len_pt", 0) > 1e-6:
                iv.append(float(t) / r2["measured_len_pt"])
        m1, _ = modal_scale(lo)
        m2, _ = modal_scale(iv)
        n1, e1 = nearest_std(m1 / MM_PER_PT if m1 else 0)
        n2, e2 = nearest_std(m2 / MM_PER_PT if m2 else 0)
        if lo:
            lo_tot += 1
            lo_ok += int(e1 is not None and e1 <= 2)
        if iv:
            iv_tot += 1
            iv_ok += int(e2 is not None and e2 <= 2)
        med = statistics.median(lo) if lo else 0.0
        g = lambda x: (f"{x:6.2f}" if x is not None else "  None")
        print(f"{b:22s} | {len(lo):10d} {med:8.3f} {m1:8.3f} {(m1/MM_PER_PT if m1 else 0):8.2f} {g(e1)} | "
              f"{len(iv):10d} {m2:8.3f} {(m2/MM_PER_PT if m2 else 0):8.2f} {g(e2)}")
    print(f"\n   whole-segment referent lands within 2 % of a standard scale on {lo_ok}/{lo_tot} measurable blocks")
    print(f"   tick-interval referent lands within 2 % of a standard scale on {iv_ok}/{iv_tot} measurable blocks")

    print("\n   per-TEXT accuracy, same texts (having BOTH relations), against the block's TRUE standard scale:")
    print(f"   {'block':22s} {'n both':>7s} {'whole-segment':>14s} {'tick-interval':>14s}")
    tot_lo = tot_iv = tot_n = 0
    for b, std in TRUE_STD.items():
        res = json.loads((REL / b / "left.json").read_text(encoding="utf-8"))
        sc = std * MM_PER_PT
        n = lo = iv = 0
        for u in res["units"]:
            t = u["text"].strip()
            if not PURE.match(t):
                continue
            r1 = u["relations"].get("dimension_line_only")
            r2 = u["relations"].get("dimension_interval")
            if not (r1 and r1.get("hit") and r2 and r2.get("hit")):
                continue
            if r1.get("measured_len_pt", 0) <= 1e-6 or r2.get("measured_len_pt", 0) <= 1e-6:
                continue
            n += 1
            lo += int(abs((float(t) / r1["measured_len_pt"]) / sc - 1) <= 0.02)
            iv += int(abs((float(t) / r2["measured_len_pt"]) / sc - 1) <= 0.02)
        tot_lo += lo
        tot_iv += iv
        tot_n += n
        print(f"   {b:22s} {n:7d} {lo:7d} ({lo/n if n else 0:.3f}) {iv:7d} ({iv/n if n else 0:.3f})")
    print(f"   {'POOLED':22s} {tot_n:7d} {tot_lo:7d} ({tot_lo/tot_n:.3f}) {tot_iv:7d} ({tot_iv/tot_n:.3f})")

    print("\n   is the 'primitive' already the interval?  interval/segment length ratio:")
    for b in ("ar_plan", "ss_plan_dense"):
        res = json.loads((REL / b / "left.json").read_text(encoding="utf-8"))
        same = n = 0
        hist = collections.Counter()
        for u in res["units"]:
            t = u["text"].strip()
            if not PURE.match(t):
                continue
            r1 = u["relations"].get("dimension_line_only")
            r2 = u["relations"].get("dimension_interval")
            if not (r1 and r1.get("hit") and r2 and r2.get("hit")):
                continue
            n += 1
            same += int(abs(r1["measured_len_pt"] - r2["measured_len_pt"]) <= 0.05)
            hist[round(r2["measured_len_pt"] / r1["measured_len_pt"], 1)] += 1
        print(f"     {b:22s} n={n:4d}  interval == whole segment: {same} ({same/n:.3f})  hist={dict(sorted(hist.items()))}")

    print("\n   bin_factor sensitivity of the modal scale (T5's own falsification test):")
    for b in list(TRUSTED) + ["fresh_kj_plan_part", "fresh_ov_spec_table", "vk_plan", "vk_node_plan"]:
        res = json.loads((REL / b / "left.json").read_text(encoding="utf-8"))
        iv = []
        for u in res["units"]:
            t = u["text"].strip()
            if not PURE.match(t):
                continue
            r2 = u["relations"].get("dimension_interval")
            if r2 and r2.get("hit") and r2.get("measured_len_pt", 0) > 1e-6:
                iv.append(float(t) / r2["measured_len_pt"])
        cells = []
        for bf in (1.01, 1.02, 1.05, 1.10):
            m, _ = modal_scale(iv, bf)
            cells.append(f"bf={bf}: 1:{(m/MM_PER_PT if m else 0):7.2f}")
        print(f"     {b:22s} n={len(iv):4d}  " + "   ".join(cells))

    sec("V7 — grid_cell (claim 5)")
    print(f"{'block':22s} {'units':>6s} {'gridHit':>8s} {'share':>6s} | {'col>=3':>7s} {'row>=3':>7s} | "
          f"{'cell(l,r) shared by >=3':>24s}")
    for p in sorted(REL.glob("*/left.json")):
        b = p.parent.name
        us = json.loads(p.read_text(encoding="utf-8"))["units"]
        gh = [u for u in us if u["relations"].get("grid_cell", {}).get("hit")]
        col = sum(1 for u in us if u["relations"].get("text_alignment", {}).get("column_size", 0) >= 3)
        row = sum(1 for u in us if u["relations"].get("text_alignment", {}).get("row_size", 0) >= 3)
        xs = collections.Counter()
        for u in gh:
            ref = u["relations"]["grid_cell"]["referent"].split(":")[1].split(",")
            xs[(ref[0], ref[2])] += 1
        shared = sum(v for v in xs.values() if v >= 3)
        print(f"{b:22s} {len(us):6d} {len(gh):8d} {len(gh)/len(us):6.2f} | {col/len(us):7.2f} {row/len(us):7.2f} | "
              f"{(shared/len(gh) if gh else 0):24.2f}")


if __name__ == "__main__":
    main()
