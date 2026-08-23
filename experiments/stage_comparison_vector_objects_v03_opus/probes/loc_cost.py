# -*- coding: utf-8 -*-
"""Cost of one comparison: measured, fitted, and extrapolated to the corpus tail."""
from __future__ import annotations
import glob
import json
import math
import statistics
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def main():
    R = []
    for f in glob.glob(str(ART / "loc_runs" / "sens_*.jsonl")) + \
            glob.glob(str(ART / "loc_runs" / "rescore*.jsonl")):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if "score" in r and r["n_seg"] > 0:
                R.append(r)
    bins = [(0, 500), (500, 2000), (2000, 5000), (5000, 15000), (15000, 50000),
            (50000, 10 ** 9)]
    table = []
    for lo, hi in bins:
        s = [r["t_sec"] for r in R if lo <= r["n_seg"] < hi]
        if not s:
            continue
        table.append({"n_seg": f"{lo}-{hi if hi < 10**8 else 'inf'}", "n_rows": len(s),
                      "median_s": round(statistics.median(s), 3),
                      "p90_s": round(sorted(s)[int(0.9 * (len(s) - 1))], 2),
                      "max_s": round(max(s), 1)})
    per = {}
    for k, v in {}.items():
        per[k] = v
    xs = [math.log(r["n_seg"]) for r in R]
    ys = [math.log(max(r["t_sec"], 1e-3)) for r in R]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    a = math.exp(my - b * mx)
    out = {
        "note": "one ledger = two object layers + two-way ink matching + clustering; "
                "wall time on a contended 16-core box, single process per comparison",
        "n_rows": n, "by_block_size": table,
        "fit": {"model": "t_sec = A * n_seg^B", "A": float(f"{a:.4g}"), "B": round(b, 3)},
        "extrapolation": {
            "n_seg_10k": round(a * 10000 ** b, 2),
            "n_seg_50k": round(a * 50000 ** b, 2),
            "n_seg_200k": round(a * 200000 ** b, 1),
            "corpus_max_817732_seg": round(a * 817732 ** b, 1),
        },
        "corpus_context": "fnd/GATEFIX: median block 1 858 segments, 28.6 % of blocks "
                          ">= 10 000, max 817 732",
    }
    json.dump(out, open(ART / "loc_cost.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
