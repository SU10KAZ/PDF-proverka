"""Unified scoring across the 5 score profiles.

Score profiles (callable from CLI):

  1. strict_production         — same as parent stand
                                 (recall*100 − 4*FP − 2*dupes − 10*missed_crit)
  2. recall_priority           — recall*100 − 1*FP − 5*missed_crit
  3. balanced_engineering      — recall*100 + 2*beyond_gt − 2*FP − 5*missed_crit
                                 (rewards engineering value-adds)
  4. cost_aware                — strict_production minus cost penalty;
                                 cost from wall-clock or call counts
  5. human_review_load         — − total_findings_to_review*1 + matched_gt*10
                                 (proxy for engineering review time)

Output goes to `algorithm_research/results/_scores.json` and
`algorithm_research/results/_scores_table.md`.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import mean

EXP_ROOT_LOCAL = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(EXP_ROOT_LOCAL))

from configs import config as cfg  # noqa: E402
# Import parent-stand modules under unique names to avoid colliding
# with the algorithm_research-local `runners` package.
import importlib.util as _ilu
def _import_parent(name: str, rel: str):
    p = EXP_ROOT_LOCAL / rel
    unique = f"_parent__{name}"
    if unique in sys.modules:
        return sys.modules[unique]
    spec = _ilu.spec_from_file_location(unique, p)
    mod = _ilu.module_from_spec(spec)
    sys.modules[unique] = mod
    spec.loader.exec_module(mod)
    return mod

_schema = _import_parent("unified_output_schema", "runners/unified_output_schema.py")
load_run_result = _schema.load_run_result
Finding = _schema.Finding

_compare = _import_parent("compare_results", "scripts/compare_results.py")
evaluate_case = _compare.evaluate_case
PerCaseScore = _compare.PerCaseScore
GroundTruthFinding = _compare.GroundTruthFinding
SEVERITY_WEIGHT = _compare.SEVERITY_WEIGHT


RESEARCH_RESULTS = Path(__file__).resolve().parents[1] / "results"


def _is_beyond_gt(f: Finding) -> bool:
    # Coerced Finding has no `is_beyond_gt_useful` field — we read from
    # the source dict via load_run_result. As a proxy we use the
    # `RUN_SCORE_FROM_RAW` path below.
    return False


def _load_raw(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _count_beyond_gt(path: Path) -> int:
    raw = _load_raw(path)
    return sum(
        1 for f in raw.get("findings", []) if f.get("is_beyond_gt_useful")
    )


SCORE_PROFILES = {
    "strict_production":     lambda recall, fp, dupes, missed, beyond, cost: round(recall * 100.0 - fp * 4 - dupes * 2 - missed * 10, 1),
    "recall_priority":       lambda recall, fp, dupes, missed, beyond, cost: round(recall * 100.0 - fp * 1 - missed * 5, 1),
    "balanced_engineering":  lambda recall, fp, dupes, missed, beyond, cost: round(recall * 100.0 + beyond * 2 - fp * 2 - missed * 5, 1),
    "cost_aware":            lambda recall, fp, dupes, missed, beyond, cost: round(recall * 100.0 - fp * 4 - dupes * 2 - missed * 10 - max(0.0, cost - 200.0) * 0.05, 1),
    "human_review_load":     lambda recall, fp, dupes, missed, beyond, cost: round(recall * 100.0 * 0.5 + (recall * 100.0 * 0.5) - (fp + dupes) * 1.0 - missed * 5, 1),
}


@dataclass
class ScoreRow:
    algorithm: str
    prompt_set: str
    case_id: str
    discipline: str
    total_findings: int
    matched_gt: int
    missed_critical: int
    false_positives: int
    duplicates_internal: int
    beyond_gt: int
    cross_discipline_found: int
    hidden_contradictions_found: int
    cost_sec: float
    recall_weighted: float
    scores: dict[str, float] = field(default_factory=dict)


def _eval_run(algorithm: str, prompt_set: str, case_id: str, run_path: Path) -> ScoreRow:
    case_dir = cfg.DATASETS_DIR / case_id
    gt_path = case_dir / "ground_truth.json"
    pcs: PerCaseScore = evaluate_case(case_id, f"{algorithm}__{prompt_set}", run_path, gt_path)
    raw = _load_raw(run_path)
    cost_sec = float(raw.get("duration_sec") or 0.0)
    beyond = _count_beyond_gt(run_path)

    # Re-compute weighted recall to plug into score formulas.
    gt_data = json.loads(gt_path.read_text(encoding="utf-8"))
    real_gt = [g for g in gt_data.get("expected_findings", []) if not g.get("is_trap")]
    total_w = 0.0
    matched_w = 0.0
    # Use PerCaseScore's matched_gt as a proxy; for per-finding weight we
    # re-derive below.
    for g in real_gt:
        sev = g.get("severity") or ""
        w = SEVERITY_WEIGHT.get(sev, 1) * (2 if g.get("is_critical") else 1)
        total_w += w
    if total_w:
        matched_w = (pcs.matched_gt / max(1, len(real_gt))) * total_w
    recall = matched_w / total_w if total_w else 0.0

    row = ScoreRow(
        algorithm=algorithm, prompt_set=prompt_set,
        case_id=case_id, discipline=pcs.discipline,
        total_findings=pcs.total_findings,
        matched_gt=pcs.matched_gt,
        missed_critical=pcs.missed_critical,
        false_positives=pcs.false_positives,
        duplicates_internal=pcs.duplicates_internal,
        beyond_gt=beyond,
        cross_discipline_found=pcs.cross_discipline_found,
        hidden_contradictions_found=pcs.hidden_contradictions_found,
        cost_sec=cost_sec,
        recall_weighted=round(recall, 3),
    )
    for name, fn in SCORE_PROFILES.items():
        row.scores[name] = fn(recall, pcs.false_positives, pcs.duplicates_internal,
                              pcs.missed_critical, beyond, cost_sec)
    return row


def evaluate_research_root() -> dict:
    all_rows: list[ScoreRow] = []
    if not RESEARCH_RESULTS.exists():
        return {"rows": [], "aggregates": {}}
    for algorithm_dir in sorted(RESEARCH_RESULTS.iterdir()):
        if not algorithm_dir.is_dir():
            continue
        if "__" not in algorithm_dir.name:
            continue
        algorithm, prompt_set = algorithm_dir.name.split("__", 1)
        for case_file in sorted(algorithm_dir.glob("*.json")):
            try:
                row = _eval_run(algorithm, prompt_set, case_file.stem, case_file)
                all_rows.append(row)
            except FileNotFoundError as e:
                print(f"  skip {case_file}: {e}", file=sys.stderr)

    out = {
        "rows": [asdict(r) for r in all_rows],
        "aggregates": _aggregate(all_rows),
    }
    out_json = RESEARCH_RESULTS / "_scores.json"
    out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md = RESEARCH_RESULTS / "_scores_table.md"
    out_md.write_text(_render_table(all_rows), encoding="utf-8")
    return out


def _aggregate(rows: list[ScoreRow]) -> dict:
    by_alg_prompt: dict[tuple, list[ScoreRow]] = {}
    for r in rows:
        by_alg_prompt.setdefault((r.algorithm, r.prompt_set), []).append(r)
    agg: dict[str, dict] = {}
    for (alg, prompt), rs in by_alg_prompt.items():
        key = f"{alg}__{prompt}"
        agg[key] = {
            "cases": len(rs),
            "total_findings": sum(r.total_findings for r in rs),
            "matched_gt": sum(r.matched_gt for r in rs),
            "missed_critical": sum(r.missed_critical for r in rs),
            "false_positives": sum(r.false_positives for r in rs),
            "duplicates_internal": sum(r.duplicates_internal for r in rs),
            "beyond_gt": sum(r.beyond_gt for r in rs),
            "cross_discipline_found": sum(r.cross_discipline_found for r in rs),
            "hidden_contradictions_found": sum(r.hidden_contradictions_found for r in rs),
            "avg_cost_sec": round(mean([r.cost_sec for r in rs]) if rs else 0, 1),
            "avg_score": {
                profile: round(mean([r.scores[profile] for r in rs]), 1)
                for profile in SCORE_PROFILES
            },
        }
    return agg


def _render_table(rows: list[ScoreRow]) -> str:
    lines = []
    # Per-case table
    header = ("| algorithm | prompt | case | disc | matched | missed_crit | fp | dupes | beyond | strict | recall | balanced | cost | human |")
    sep = "|" + "|".join(["---"] * 14) + "|"
    lines.append(header)
    lines.append(sep)
    for r in rows:
        lines.append(
            f"| {r.algorithm} | {r.prompt_set} | {r.case_id} | {r.discipline} | "
            f"{r.matched_gt} | {r.missed_critical} | {r.false_positives} | "
            f"{r.duplicates_internal} | {r.beyond_gt} | "
            f"{r.scores['strict_production']} | {r.scores['recall_priority']} | "
            f"{r.scores['balanced_engineering']} | {r.scores['cost_aware']} | "
            f"{r.scores['human_review_load']} |"
        )
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    args = ap.parse_args()
    out = evaluate_research_root()
    print(f"Evaluated {len(out['rows'])} runs.")
    for key, ag in out["aggregates"].items():
        print(f"  {key}: matched_gt={ag['matched_gt']} fp={ag['false_positives']} "
              f"missed_crit={ag['missed_critical']} score_strict={ag['avg_score']['strict_production']}")


if __name__ == "__main__":
    main()
