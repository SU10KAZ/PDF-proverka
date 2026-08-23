# -*- coding: utf-8 -*-
"""N5 — render the FALSE GRAPHIC CHANGE matrix as the markdown the report carries."""
from __future__ import annotations
import glob, json, statistics, sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N   # noqa: E402

ART = N.ART


def load(name):
    p = ART / name
    return json.load(open(p, encoding="utf-8")) if p.exists() else None


def shards(pattern, key):
    out = []
    for f in sorted(glob.glob(str(ART / pattern))):
        out += json.load(open(f, encoding="utf-8")).get(key, [])
    return out


def row(label, v, ink, inkb=None, own=None):
    n = len(v)
    disc = len({x.get("discipline") for x in v})
    fp = sum(1 for x in ink if x > 0) / max(1, n)
    mean = statistics.fmean(ink) if ink else 0.0
    mb = statistics.fmean(inkb) if inkb else 0.0
    ow_fp = sum(1 for x in own if x > 0) / max(1, len(own)) if own else 0.0
    ow_m = statistics.fmean(own) if own else 0.0
    return (f"| `{label}` | {n} | {disc} | **{fp:.3f}** | **{mean:.3f}** | {max(ink) if ink else 0} "
            f"| {mb:.2f} | {ow_fp:.3f} | {ow_m:.2f} |")


def main():
    out = []
    out.append("| класс негативного контроля | n | дисц. | доля ложных срабатываний "
               "| средн. ложных записей на блок | макс | средн. граничных "
               "| `count` при своём `S`: доля | средн. |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    n1 = load("neg_n1_text.json")
    if n1:
        g = defaultdict(list)
        for r in n1["rows"]:
            g[r["variant"]].append(r)
        for k, v in sorted(g.items()):
            out.append(row("TEXT_ONLY / " + k, v,
                           [x["shared"]["ledger"]["ink"] for x in v],
                           [x["shared"]["ledger"]["ink_border"] for x in v],
                           [x["own_scale"]["ledger"]["count"] for x in v]))
    n2 = load("neg_n2_table.json")
    if n2:
        g = defaultdict(list)
        for r in n2["d45"]:
            g[r["cf_id"]].append(r)
        for k, v in sorted(g.items()):
            out.append(row("TABLE_ONLY / " + k, v,
                           [x["shared"]["ledger"]["ink"] for x in v],
                           [x["shared"]["ledger"]["ink_border"] for x in v],
                           [x["own_scale"]["ledger"]["count"] for x in v]))
    n4 = load("neg_n4_dims.json")
    if n4:
        v = [r for r in n4["rows"] if r["cf_id"] == "D6_dim_value_only"]
        if v:
            out.append(row("DIM / D6_dim_value_only", v,
                           [r["res"]["ledger"]["ink"] for r in v]))
    cf = shards("neg_runs/neg_n3_curves_*of6.json", "cf")
    e = [r for r in cf if "curve_text_edit" in r]
    if e:
        out.append(row("CURVES / NC1_text_edit [CF]", e,
                       [r["curve_text_edit"]["n_entries_raw"] for r in e]))
    real = shards("neg_runs/neg_n3_curves_*of6.json", "real")
    er = [r for r in real if "curve_text_edit" in r]
    if er:
        out.append(row("CURVES / NC1_text_edit [REAL]", er,
                       [r["curve_text_edit"]["n_entries_raw"] for r in er]))

    # ---- real silent pairs -----------------------------------------------------
    rp = load("neg_real_pairs.json")
    out2 = []
    if rp:
        CORRECTED = {"EOM-7fef43a3"}
        sil = [p for p in rp["pairs"]
               if p["expected_verdict"] == "NO_GRAPHIC_CHANGE" or p["pair_id"] in CORRECTED]
        buckets = defaultdict(list)
        for p in sil:
            for c in p["classes"]:
                buckets[c].append(p)
            buckets["ВСЕ ТИХИЕ ПАРЫ"].append(p)
        out2.append("| класс «тихой» реальной пары | n | доля ложных | средн. записей "
                    "| средн. граничных | `count` | `object_id` | `churn` |")
        out2.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for k in sorted(buckets, key=lambda s: (s != "ВСЕ ТИХИЕ ПАРЫ", s)):
            v = buckets[k]
            inner = [p["n_entries_inner"] for p in v]
            out2.append(
                f"| `{k}` | {len(v)} | **{sum(1 for x in inner if x > 0) / len(v):.3f}** "
                f"| **{statistics.fmean(inner):.3f}** "
                f"| {statistics.fmean([p['n_entries_border'] for p in v]):.2f} "
                f"| {statistics.fmean([p['res']['ledger']['count'] for p in v]):.2f} "
                f"| {statistics.fmean([p['res']['ledger']['object_id'] for p in v]):.2f} "
                f"| {statistics.fmean([p['res']['ledger']['churn'] for p in v]):.2f} |")
    txt = "\n".join(out) + "\n\n" + "\n".join(out2) + "\n"
    (ART / "neg_n5_matrix.md").write_text(txt, encoding="utf-8")
    print(txt)


if __name__ == "__main__":
    main()
