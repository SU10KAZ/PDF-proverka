# -*- coding: utf-8 -*-
"""Aggregate the L1 label census: object-weighted and BLOCK-weighted, by discipline,
by block class, by object class."""
from __future__ import annotations
import json, sys, statistics
from collections import defaultdict
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L

STATES = ("unique_mark", "repeated_mark", "text_no_mark", "no_text")


def shares(counts):
    n = sum(counts.get(s, 0) for s in STATES)
    return {s: (counts.get(s, 0) / n if n else 0.0) for s in STATES}, n


def obj_weighted(rows, k="k1.6"):
    tot = defaultdict(int)
    for r in rows:
        for s in STATES:
            tot[s] += r["by_k"][k].get(s, 0)
    sh, n = shares(tot)
    return {**{s: round(v, 4) for s, v in sh.items()}, "n_obj": n, "n_blocks": len(rows)}


def block_weighted(rows, k="k1.6"):
    per = defaultdict(list)
    for r in rows:
        sh, n = shares(r["by_k"][k])
        if n == 0:
            continue
        for s in STATES:
            per[s].append(sh[s])
    return {s: {"median": round(statistics.median(v), 4),
                "mean": round(statistics.fmean(v), 4)} for s, v in per.items()} | \
           {"n_blocks": len(per["no_text"])}


def main():
    d = json.load(open(L.ART / "lbl_census.json", encoding="utf-8"))
    rows = [r for r in d["rows"] if "by_k" in r]
    skipped = [{"key": r["key"], "why": r.get("skip") or r.get("error")}
               for r in d["rows"] if "by_k" not in r]
    out = {"n_blocks_measured": len(rows), "n_blocks_skipped": len(skipped),
           "skipped": skipped, "k_ladder": d["k_ladder"], "populations": {}}
    for pop in ("bench", "corpus"):
        rs = [r for r in rows if r["pop"] == pop]
        e = {"n_blocks": len(rs), "n_obj": sum(r["n_obj"] for r in rs),
             "by_k_object_weighted": {f"k{k}": obj_weighted(rs, f"k{k}") for k in d["k_ladder"]},
             "by_k_block_weighted": {f"k{k}": block_weighted(rs, f"k{k}") for k in d["k_ladder"]}}
        # discipline / block class breakdown at the working radius
        for field in ("discipline", "cls", "bucket"):
            grp = defaultdict(list)
            for r in rs:
                if r.get(field):
                    grp[r[field]].append(r)
            if grp:
                e[f"by_{field}"] = {g: {"object_weighted": obj_weighted(v),
                                        "block_weighted_median_unique":
                                            block_weighted(v)["unique_mark"]["median"],
                                        "n_blocks": len(v)}
                                    for g, v in sorted(grp.items())}
        # object class breakdown
        oc = defaultdict(lambda: defaultdict(int))
        for r in rs:
            for c, st in r.get("by_obj_cls", {}).items():
                for s, v in st.items():
                    oc[c][s] += v
        e["by_object_class"] = {}
        for c, st in sorted(oc.items()):
            sh, n = shares(st)
            e["by_object_class"][c] = {**{s: round(v, 4) for s, v in sh.items()}, "n_obj": n}
        # blocks with NO usable anchor at all
        e["blocks_with_zero_unique_mark_objects"] = round(
            sum(1 for r in rs if r["by_k"]["k1.6"].get("unique_mark", 0) == 0) / max(len(rs), 1), 4)
        e["blocks_with_zero_text"] = round(
            sum(1 for r in rs if r["n_text"] == 0) / max(len(rs), 1), 4)
        e["blocks_under_10pct_unique"] = round(
            sum(1 for r in rs if shares(r["by_k"]["k1.6"])[0]["unique_mark"] < 0.10) /
            max(len(rs), 1), 4)
        out["populations"][pop] = e
    json.dump(out, open(L.ART / "lbl_census_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    for pop in ("bench", "corpus"):
        e = out["populations"][pop]
        print("==", pop, e["n_blocks"], "blocks", e["n_obj"], "objects")
        print(" obj-weighted k1.6:", e["by_k_object_weighted"]["k1.6"])
        print(" block-weighted k1.6 medians:",
              {s: e["by_k_block_weighted"]["k1.6"][s]["median"] for s in STATES})
        print(" zero-unique-mark blocks:", e["blocks_with_zero_unique_mark_objects"],
              " zero-text blocks:", e["blocks_with_zero_text"],
              " <10% unique:", e["blocks_under_10pct_unique"])
        print(" by object class:", json.dumps(e["by_object_class"], ensure_ascii=False))
        if "by_cls" in e:
            print(" by block class:", json.dumps(
                {k: (v["object_weighted"]["unique_mark"], v["n_blocks"])
                 for k, v in e["by_cls"].items()}, ensure_ascii=False))
        if "by_discipline" in e:
            print(" by discipline unique_mark:", json.dumps(
                {k: (v["object_weighted"]["unique_mark"], v["n_blocks"])
                 for k, v in e["by_discipline"].items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
