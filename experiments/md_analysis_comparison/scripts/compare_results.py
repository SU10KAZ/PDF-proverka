"""Compare current_method vs multi_agent on a dataset.

For each case, load both results and the ground truth, compute per-metric
scores, then aggregate into a comparison table. Output:

- comparison_outputs/per_case.json   — full detail
- comparison_outputs/summary.json    — aggregates
- comparison_outputs/table.md        — final markdown table

Matching of findings against ground truth is text-similarity based
(SequenceMatcher on `problem` + `evidence_quote`). Threshold tuneable.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners.unified_output_schema import load_run_result, Finding  # noqa: E402

SEVERITY_WEIGHT = {
    "КРИТИЧЕСКОЕ": 4,
    "ЭКОНОМИЧЕСКОЕ": 3,
    "ЭКСПЛУАТАЦИОННОЕ": 2,
    "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ": 2,
    "РЕКОМЕНДАТЕЛЬНОЕ": 1,
}

DEFAULT_MATCH_THRESHOLD = 0.45


@dataclass
class GroundTruthFinding:
    id: str
    severity: str
    description: str
    must_match_substring: str = ""
    is_critical: bool = False
    is_trap: bool = False
    cross_discipline: bool = False
    hidden_contradiction: bool = False


@dataclass
class MatchResult:
    gt_id: str
    matched_finding_id: str | None
    similarity: float
    severity_match: bool


@dataclass
class PerCaseScore:
    case_id: str
    discipline: str
    method: str
    total_findings: int
    matched_gt: int
    missed_gt: int
    missed_critical: int
    false_positives: int
    duplicates_internal: int
    avg_norm_confidence: float
    avg_finding_confidence: float
    has_evidence_quote_ratio: float
    severity_distribution: dict[str, int] = field(default_factory=dict)
    cross_discipline_found: int = 0
    hidden_contradictions_found: int = 0
    noise_score: float = 0.0
    score: float = 0.0


def _sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower(), autojunk=False).ratio()


def _match_finding_to_gt(f: Finding, gt: GroundTruthFinding) -> float:
    if gt.must_match_substring and gt.must_match_substring.lower() in (
        f.problem.lower() + " " + f.description.lower() + " " + f.evidence_quote.lower()
    ):
        return 1.0
    sims = [
        _sim(f.problem, gt.description),
        _sim(f.description, gt.description),
        _sim(f.evidence_quote, gt.description) * 0.7,
    ]
    return max(sims)


def _detect_internal_dupes(findings: list[Finding]) -> int:
    """Count obvious duplicates inside one method's output."""
    count = 0
    seen: list[str] = []
    for f in findings:
        sig = f.problem.lower()[:80]
        if any(_sim(sig, s) > 0.75 for s in seen):
            count += 1
        else:
            seen.append(sig)
    return count


def evaluate_case(
    case_id: str,
    method: str,
    result_path: Path,
    gt_path: Path,
    match_threshold: float = DEFAULT_MATCH_THRESHOLD,
) -> PerCaseScore:
    result = load_run_result(result_path)
    gt_data = json.loads(gt_path.read_text(encoding="utf-8"))
    gt_items = [
        GroundTruthFinding(
            id=g.get("id", f"GT-{i:02d}"),
            severity=g.get("severity", ""),
            description=g.get("description", ""),
            must_match_substring=g.get("must_match_substring", ""),
            is_critical=bool(g.get("is_critical", False) or g.get("severity") == "КРИТИЧЕСКОЕ"),
            is_trap=bool(g.get("is_trap", False)),
            cross_discipline=bool(g.get("cross_discipline", False)),
            hidden_contradiction=bool(g.get("hidden_contradiction", False)),
        )
        for i, g in enumerate(gt_data.get("expected_findings", []), start=1)
    ]
    traps = [g for g in gt_items if g.is_trap]
    real_gt = [g for g in gt_items if not g.is_trap]

    matches: dict[str, MatchResult] = {}
    matched_finding_ids: set[str] = set()
    for gt in real_gt:
        best: tuple[float, Finding | None] = (0.0, None)
        for f in result.findings:
            if f.id in matched_finding_ids:
                continue
            s = _match_finding_to_gt(f, gt)
            if s > best[0]:
                best = (s, f)
        sim, f = best
        if f is not None and sim >= match_threshold:
            matched_finding_ids.add(f.id)
            sev_match = (f.severity == gt.severity) if gt.severity else True
            matches[gt.id] = MatchResult(gt.id, f.id, sim, sev_match)
        else:
            matches[gt.id] = MatchResult(gt.id, None, sim, False)

    matched_gt = sum(1 for m in matches.values() if m.matched_finding_id)
    missed_gt = len(real_gt) - matched_gt
    missed_critical = sum(
        1 for gt in real_gt
        if gt.is_critical and not matches[gt.id].matched_finding_id
    )

    trap_triggered = 0
    for f in result.findings:
        for trap in traps:
            if _match_finding_to_gt(f, trap) >= match_threshold:
                trap_triggered += 1
                break
    unmatched_findings = [f for f in result.findings if f.id not in matched_finding_ids]
    false_positives = trap_triggered + max(0, len(unmatched_findings) - max(0, missed_gt) - trap_triggered)
    false_positives = min(false_positives, len(unmatched_findings))

    cross_discipline_found = sum(
        1 for gt in real_gt
        if gt.cross_discipline and matches[gt.id].matched_finding_id
    )
    hidden_contradictions_found = sum(
        1 for gt in real_gt
        if gt.hidden_contradiction and matches[gt.id].matched_finding_id
    )

    sev_dist: dict[str, int] = {}
    for f in result.findings:
        sev_dist[f.severity] = sev_dist.get(f.severity, 0) + 1

    dupes = _detect_internal_dupes(result.findings)
    has_ev = sum(1 for f in result.findings if f.evidence_quote.strip())
    has_ev_ratio = (has_ev / len(result.findings)) if result.findings else 0.0

    avg_norm_conf = mean([f.norm_confidence for f in result.findings if f.norm_confidence > 0]) if any(f.norm_confidence > 0 for f in result.findings) else 0.0
    avg_conf = mean([f.confidence for f in result.findings if f.confidence > 0]) if any(f.confidence > 0 for f in result.findings) else 0.0

    noise_score = false_positives / max(1, len(result.findings))

    # Combined score: weighted recall on real GT - penalties.
    recall_weighted = 0.0
    total_weight = 0.0
    for gt in real_gt:
        w = SEVERITY_WEIGHT.get(gt.severity, 1)
        if gt.is_critical:
            w *= 2
        total_weight += w
        if matches[gt.id].matched_finding_id:
            recall_weighted += w
    recall_score = (recall_weighted / total_weight) if total_weight else 0.0
    score = (recall_score * 100.0) - (false_positives * 4) - (dupes * 2) - (missed_critical * 10)
    score = round(score, 1)

    return PerCaseScore(
        case_id=case_id, discipline=result.discipline, method=method,
        total_findings=len(result.findings),
        matched_gt=matched_gt, missed_gt=missed_gt, missed_critical=missed_critical,
        false_positives=false_positives, duplicates_internal=dupes,
        avg_norm_confidence=round(avg_norm_conf, 2),
        avg_finding_confidence=round(avg_conf, 2),
        has_evidence_quote_ratio=round(has_ev_ratio, 2),
        severity_distribution=sev_dist,
        cross_discipline_found=cross_discipline_found,
        hidden_contradictions_found=hidden_contradictions_found,
        noise_score=round(noise_score, 2),
        score=score,
    )


def compare_dataset(dataset_dir: Path, results_dir: Path, output_dir: Path) -> dict:
    cases = sorted([p for p in dataset_dir.iterdir() if p.is_dir() and (p / "case.json").exists()])
    per_case: list[dict] = []
    table_rows: list[dict] = []

    for case in cases:
        case_id = case.name
        gt = case / "ground_truth.json"
        if not gt.exists():
            continue
        cur_path = results_dir / case_id / "current.json"
        ma_path = results_dir / case_id / "multi_agent.json"
        row: dict = {"case_id": case_id}
        info = json.loads((case / "case.json").read_text(encoding="utf-8"))
        row["discipline"] = info.get("discipline", "")

        cur_score = ma_score = None
        if cur_path.exists():
            cur_score = evaluate_case(case_id, "current_method", cur_path, gt)
            per_case.append({"method": "current_method", **asdict(cur_score)})
            row.update({
                "current_findings": cur_score.total_findings,
                "current_score": cur_score.score,
                "missed_critical_current": cur_score.missed_critical,
                "false_positive_current": cur_score.false_positives,
            })
        if ma_path.exists():
            ma_score = evaluate_case(case_id, "multi_agent", ma_path, gt)
            per_case.append({"method": "multi_agent", **asdict(ma_score)})
            row.update({
                "multi_agent_findings": ma_score.total_findings,
                "multi_agent_score": ma_score.score,
                "missed_critical_multi": ma_score.missed_critical,
                "false_positive_multi": ma_score.false_positives,
            })

        if cur_score and ma_score:
            if ma_score.score > cur_score.score + 2:
                winner = "multi_agent"
            elif cur_score.score > ma_score.score + 2:
                winner = "current_method"
            else:
                winner = "tie"
            row["winner"] = winner
            row["notes"] = _short_note(cur_score, ma_score)
        else:
            row["winner"] = "incomplete"
            row["notes"] = "missing one or both runs"
        table_rows.append(row)

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "per_case.json").write_text(
        json.dumps(per_case, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = _aggregate(per_case)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "table.md").write_text(_render_table(table_rows), encoding="utf-8")
    return {"per_case": per_case, "summary": summary, "table_rows": table_rows}


def _short_note(cur, ma) -> str:
    parts = []
    if ma.missed_critical < cur.missed_critical:
        parts.append(f"multi-agent caught {cur.missed_critical - ma.missed_critical} more critical")
    elif ma.missed_critical > cur.missed_critical:
        parts.append(f"current caught {ma.missed_critical - cur.missed_critical} more critical")
    if ma.false_positives < cur.false_positives:
        parts.append(f"less noise (FP {cur.false_positives}->{ma.false_positives})")
    elif ma.false_positives > cur.false_positives:
        parts.append(f"more noise (FP {cur.false_positives}->{ma.false_positives})")
    if ma.duplicates_internal > cur.duplicates_internal + 2:
        parts.append("multi-agent has more internal dupes")
    return "; ".join(parts) or "comparable"


def _aggregate(per_case: list[dict]) -> dict:
    by_method: dict[str, dict] = {}
    for row in per_case:
        m = row["method"]
        d = by_method.setdefault(m, {
            "cases": 0, "total_findings": 0, "matched_gt": 0, "missed_gt": 0,
            "missed_critical": 0, "false_positives": 0, "duplicates_internal": 0,
            "cross_discipline_found": 0, "hidden_contradictions_found": 0,
            "avg_score": 0.0, "scores": [],
        })
        d["cases"] += 1
        d["total_findings"] += row["total_findings"]
        d["matched_gt"] += row["matched_gt"]
        d["missed_gt"] += row["missed_gt"]
        d["missed_critical"] += row["missed_critical"]
        d["false_positives"] += row["false_positives"]
        d["duplicates_internal"] += row["duplicates_internal"]
        d["cross_discipline_found"] += row["cross_discipline_found"]
        d["hidden_contradictions_found"] += row["hidden_contradictions_found"]
        d["scores"].append(row["score"])
    for m, d in by_method.items():
        d["avg_score"] = round(mean(d["scores"]), 2) if d["scores"] else 0.0
        d.pop("scores", None)
    return by_method


def _render_table(rows: list[dict]) -> str:
    header = ("| case_id | discipline | current_score | multi_agent_score | "
              "missed_critical_current | missed_critical_multi | "
              "false_positive_current | false_positive_multi | winner | notes |")
    sep = "|" + "|".join(["---"] * 10) + "|"
    body = []
    for r in rows:
        body.append(
            f"| {r.get('case_id','')} | {r.get('discipline','')} | "
            f"{r.get('current_score','-')} | {r.get('multi_agent_score','-')} | "
            f"{r.get('missed_critical_current','-')} | {r.get('missed_critical_multi','-')} | "
            f"{r.get('false_positive_current','-')} | {r.get('false_positive_multi','-')} | "
            f"{r.get('winner','-')} | {r.get('notes','')} |"
        )
    return "\n".join([header, sep] + body) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default=str(cfg.DATASETS_DIR))
    ap.add_argument("--results", default=str(cfg.RESULTS_DIR))
    ap.add_argument("--out", default=str(cfg.COMPARISON_OUTPUTS_DIR))
    args = ap.parse_args()
    res = compare_dataset(Path(args.datasets), Path(args.results), Path(args.out))
    print(f"Compared {len(res['table_rows'])} cases. Outputs in {args.out}")
    print(f"Methods aggregate: {json.dumps(res['summary'], ensure_ascii=False)}")


if __name__ == "__main__":
    main()
