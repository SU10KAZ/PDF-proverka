"""Cost model — wall-clock and call-count comparisons.

Builds a cost table over all (algorithm, prompt_set, case) combinations
present in `algorithm_research/results/`.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import mean


RESEARCH_RESULTS = Path(__file__).resolve().parents[1] / "results"


def collect() -> list[dict]:
    rows = []
    for alg_dir in sorted(RESEARCH_RESULTS.iterdir() if RESEARCH_RESULTS.exists() else []):
        if not alg_dir.is_dir() or "__" not in alg_dir.name:
            continue
        alg, prompt = alg_dir.name.split("__", 1)
        for case_file in sorted(alg_dir.glob("*.json")):
            data = json.loads(case_file.read_text(encoding="utf-8"))
            meta = data.get("meta") or {}
            rows.append({
                "algorithm": alg, "prompt_set": prompt,
                "case_id": case_file.stem,
                "duration_sec": float(data.get("duration_sec") or 0.0),
                "current_method_findings": meta.get("current_method_findings"),
                "completeness_findings": meta.get("completeness_findings"),
                "cross_discipline_findings": meta.get("cross_discipline_findings"),
                "pre_critic_count": meta.get("pre_critic_count"),
                "post_critic_findings": meta.get("post_critic_findings"),
                "n_findings": len(data.get("findings") or []),
                "router_triggered": (meta.get("router_decision") or {}).get("cross_discipline_triggered"),
            })
    return rows


def aggregate(rows: list[dict]) -> dict:
    by_alg: dict[str, list[dict]] = {}
    for r in rows:
        key = f"{r['algorithm']}__{r['prompt_set']}"
        by_alg.setdefault(key, []).append(r)
    out = {}
    for key, rs in by_alg.items():
        out[key] = {
            "cases": len(rs),
            "avg_duration_sec": round(mean([r["duration_sec"] for r in rs]) if rs else 0, 1),
            "total_duration_sec": round(sum(r["duration_sec"] for r in rs), 1),
            "router_triggered_count": sum(1 for r in rs if r["router_triggered"]),
        }
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    rows = collect()
    agg = aggregate(rows)
    payload = {"rows": rows, "aggregates": agg}
    out_path = Path(args.out) if args.out else RESEARCH_RESULTS / "_costs.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Cost rows: {len(rows)}; algorithms aggregated: {len(agg)}")
    for k, v in agg.items():
        print(f"  {k}: cases={v['cases']} avg={v['avg_duration_sec']}s total={v['total_duration_sec']}s router_triggered={v['router_triggered_count']}")


if __name__ == "__main__":
    main()
