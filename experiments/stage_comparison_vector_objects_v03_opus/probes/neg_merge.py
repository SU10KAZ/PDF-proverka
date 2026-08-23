# -*- coding: utf-8 -*-
"""Merge the sharded runs of probe `neg` into the single artefacts the report cites."""
from __future__ import annotations
import glob, json, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N   # noqa: E402

JOBS = [
    ("neg_n3_curves.json", "neg_runs/neg_n3_curves_*of*.json", ("cf", "real", "skips")),
    ("neg_n3b_glyph.json", "neg_runs/neg_n3b_*of*.json", ("cf", "real", "skips")),
    ("neg_n6_power.json", "neg_runs/neg_n6_*of*.json", ("rows", "skips")),
    ("neg_n4b_dimsweep.json", "neg_runs/neg_n4b_*of*_cap*.json", ("rows", "skips")),
    ("neg_n3c_oracle.json", "neg_runs/neg_n3c_*of*.json", ("rows", "skips")),
]


def main():
    for out, pattern, keys in JOBS:
        files = sorted(glob.glob(str(N.ART / pattern)))
        if not files:
            print(f"[neg_merge] {out}: no shards")
            continue
        merged = {"schema": f"merged:{out}", "shards": [Path(f).name for f in files]}
        for k in keys:
            merged[k] = []
        head = None
        for f in files:
            d = json.load(open(f, encoding="utf-8"))
            head = head or d
            for k in keys:
                merged[k] += d.get(k, [])
        for k, v in (head or {}).items():
            if k not in merged and k not in ("shard", "sec"):
                merged[k] = v
        for k in keys:
            merged[f"n_{k}"] = len(merged[k])
        N.dump(out, merged)


if __name__ == "__main__":
    main()
