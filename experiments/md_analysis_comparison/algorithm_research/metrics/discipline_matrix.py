"""Per-discipline algorithm winner matrix."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


RESEARCH_RESULTS = Path(__file__).resolve().parents[1] / "results"


def build():
    scores_path = RESEARCH_RESULTS / "_scores.json"
    if not scores_path.exists():
        sys.exit("Run metrics/score_algorithms.py first.")
    data = json.loads(scores_path.read_text(encoding="utf-8"))
    rows = data["rows"]
    # group by (discipline, algorithm__prompt)
    grid: dict[str, dict[str, list[dict]]] = {}
    for r in rows:
        disc = r["discipline"] or "?"
        key = f"{r['algorithm']}__{r['prompt_set']}"
        grid.setdefault(disc, {}).setdefault(key, []).append(r)
    out = {}
    for disc, by_alg in grid.items():
        out[disc] = {}
        for alg, rs in by_alg.items():
            out[disc][alg] = {
                "n": len(rs),
                "avg_strict": round(mean([r["scores"]["strict_production"] for r in rs]), 1),
                "avg_balanced": round(mean([r["scores"]["balanced_engineering"] for r in rs]), 1),
                "matched_gt": sum(r["matched_gt"] for r in rs),
                "missed_critical": sum(r["missed_critical"] for r in rs),
                "false_positives": sum(r["false_positives"] for r in rs),
                "beyond_gt": sum(r.get("beyond_gt", 0) for r in rs),
            }
    out_path = RESEARCH_RESULTS / "_discipline_matrix.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Discipline matrix saved -> {out_path}")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    args = ap.parse_args()
    build()


if __name__ == "__main__":
    main()
