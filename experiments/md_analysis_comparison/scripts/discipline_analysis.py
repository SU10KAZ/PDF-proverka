"""Discipline-level analysis: where does each method actually win/lose?

Reads comparison_outputs/per_case.json and produces a per-discipline
breakdown plus qualitative tags (completeness_gain, contradiction_gain,
noise_excess, duplicate_count, critic_pruning_rate).
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners.unified_output_schema import load_run_result  # noqa: E402


def _agent_breakdown(meta: dict) -> dict:
    out = {"agents": {}, "critic_summary": None, "reviewer_stats": None}
    for ar in meta.get("agents_run", []) or []:
        out["agents"][ar["agent"]] = {
            "findings": ar.get("findings", 0),
            "duration": ar.get("duration", 0),
            "error": ar.get("error"),
        }
    out["critic_summary"] = meta.get("critic_summary")
    out["reviewer_stats"] = meta.get("reviewer_stats")
    return out


def analyze() -> dict:
    per_case_path = cfg.COMPARISON_OUTPUTS_DIR / "per_case.json"
    if not per_case_path.exists():
        print("Run scripts/compare_results.py first", file=sys.stderr)
        sys.exit(1)
    per_case = json.loads(per_case_path.read_text(encoding="utf-8"))

    by_discipline: dict[str, dict[str, list]] = defaultdict(lambda: {"current_method": [], "multi_agent": []})
    for row in per_case:
        by_discipline[row["discipline"]][row["method"]].append(row)

    discipline_report: dict[str, dict] = {}
    for disc, by_method in by_discipline.items():
        cur_rows = by_method["current_method"]
        ma_rows = by_method["multi_agent"]
        if not cur_rows or not ma_rows:
            continue
        d = {
            "cases": len(cur_rows),
            "current": _agg(cur_rows),
            "multi_agent": _agg(ma_rows),
        }
        d["completeness_gain"] = d["multi_agent"]["total_findings"] - d["current"]["total_findings"]
        d["missed_critical_delta"] = d["multi_agent"]["missed_critical"] - d["current"]["missed_critical"]
        d["fp_excess"] = d["multi_agent"]["false_positives"] - d["current"]["false_positives"]
        d["winner"] = _decide_winner(d)
        discipline_report[disc] = d

    # Per-case qualitative tags
    case_tags: list[dict] = []
    for c_dir in sorted(cfg.DATASETS_DIR.iterdir()):
        if not c_dir.is_dir():
            continue
        cid = c_dir.name
        cur_p = cfg.RESULTS_DIR / cid / "current.json"
        ma_p = cfg.RESULTS_DIR / cid / "multi_agent.json"
        if not (cur_p.exists() and ma_p.exists()):
            continue
        cur_rr = load_run_result(cur_p)
        ma_rr = load_run_result(ma_p)
        case_tags.append({
            "case_id": cid,
            "discipline": cur_rr.discipline,
            "current_findings": len(cur_rr.findings),
            "multi_findings": len(ma_rr.findings),
            "current_duration_sec": cur_rr.duration_sec,
            "multi_duration_sec": ma_rr.duration_sec,
            "cost_ratio": round(ma_rr.duration_sec / max(cur_rr.duration_sec, 1), 2),
            "multi_agent_breakdown": _agent_breakdown(ma_rr.meta),
        })

    out = {
        "by_discipline": discipline_report,
        "by_case": case_tags,
        "overall": _overall_winner(discipline_report),
    }
    out_path = cfg.COMPARISON_OUTPUTS_DIR / "discipline_analysis.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def _agg(rows: list[dict]) -> dict:
    return {
        "total_findings": sum(r["total_findings"] for r in rows),
        "matched_gt": sum(r["matched_gt"] for r in rows),
        "missed_gt": sum(r["missed_gt"] for r in rows),
        "missed_critical": sum(r["missed_critical"] for r in rows),
        "false_positives": sum(r["false_positives"] for r in rows),
        "duplicates_internal": sum(r["duplicates_internal"] for r in rows),
        "cross_discipline_found": sum(r["cross_discipline_found"] for r in rows),
        "hidden_contradictions_found": sum(r["hidden_contradictions_found"] for r in rows),
        "avg_score": round(mean(r["score"] for r in rows), 2),
        "avg_noise_score": round(mean(r["noise_score"] for r in rows), 3),
        "avg_evidence_ratio": round(mean(r["has_evidence_quote_ratio"] for r in rows), 2),
    }


def _decide_winner(d: dict) -> dict:
    cur, ma = d["current"], d["multi_agent"]
    reasons = []
    if ma["missed_critical"] < cur["missed_critical"]:
        reasons.append(f"multi-agent caught {cur['missed_critical'] - ma['missed_critical']} more critical")
    elif ma["missed_critical"] > cur["missed_critical"]:
        reasons.append(f"current caught {ma['missed_critical'] - cur['missed_critical']} more critical")
    if ma["false_positives"] - cur["false_positives"] > 5 * d["cases"]:
        reasons.append(f"multi-agent FP excess +{ma['false_positives']-cur['false_positives']}")
    elif cur["false_positives"] - ma["false_positives"] > 5 * d["cases"]:
        reasons.append(f"current FP excess +{cur['false_positives']-ma['false_positives']}")
    if ma["hidden_contradictions_found"] > cur["hidden_contradictions_found"]:
        reasons.append(f"multi-agent found {ma['hidden_contradictions_found']-cur['hidden_contradictions_found']} more hidden contradictions")
    if ma["cross_discipline_found"] > cur["cross_discipline_found"]:
        reasons.append(f"multi-agent found {ma['cross_discipline_found']-cur['cross_discipline_found']} more cross-discipline")
    if ma["avg_score"] > cur["avg_score"] + 5:
        verdict = "multi_agent"
    elif cur["avg_score"] > ma["avg_score"] + 5:
        verdict = "current_method"
    else:
        verdict = "tie"
    return {"verdict": verdict, "reasons": reasons}


def _overall_winner(by_disc: dict) -> dict:
    counts = {"current_method": 0, "multi_agent": 0, "tie": 0}
    for d in by_disc.values():
        counts[d["winner"]["verdict"]] += 1
    return counts


def main():
    out = analyze()
    print("=== By discipline ===")
    for disc, d in out["by_discipline"].items():
        print(f"\n{disc} ({d['cases']} cases) → {d['winner']['verdict']}")
        print(f"  current: score={d['current']['avg_score']}, missed_crit={d['current']['missed_critical']}, FP={d['current']['false_positives']}")
        print(f"  multi:   score={d['multi_agent']['avg_score']}, missed_crit={d['multi_agent']['missed_critical']}, FP={d['multi_agent']['false_positives']}")
        print(f"  reasons: {d['winner']['reasons']}")
    print(f"\n=== Overall ===")
    print(json.dumps(out["overall"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
