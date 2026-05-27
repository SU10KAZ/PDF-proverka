"""Dedup quality audit.

For each algorithm output, count:
  - Total findings.
  - Distinct class keys.
  - Cluster size distribution (1, 2, 3+).
  - Findings whose `is_canonical=False` survived (a bug, should be zero).

Useful for H6 ("class-level dedup alone drops FP by ≥ 80 over baseline
multi-agent if applied retroactively").
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from algorithm_research.runners.class_dedup import (  # noqa: E402
    derive_class_key, collapse_to_canonical,
)


def audit_file(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    findings = data.get("findings", [])
    keys = [derive_class_key(f).to_str() for f in findings]
    counter = Counter(keys)
    distinct_classes = len(counter)
    multi_clusters = {k: c for k, c in counter.items() if c > 1}
    n_dupes_present = sum(c - 1 for c in counter.values() if c > 1)
    # Simulate retroactive collapse.
    deduped, report = collapse_to_canonical(findings)
    return {
        "path": str(path),
        "total_in": len(findings),
        "distinct_classes": distinct_classes,
        "multi_clusters": len(multi_clusters),
        "n_dupes_present": n_dupes_present,
        "retroactive_collapsed_count": len(deduped),
        "retroactive_drops": report.same_class_drops,
        "sample_clusters": list(multi_clusters.items())[:5],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="JSON result files to audit")
    args = ap.parse_args()
    for p in args.paths:
        path = Path(p)
        if not path.exists():
            print(f"skip {p} (not found)", file=sys.stderr)
            continue
        result = audit_file(path)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
