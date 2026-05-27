"""Score a benchmark run's results.json and produce leaderboard.csv + leaderboard.md.

Usage:
  python backend/scripts/score_vision_benchmark.py <run_dir> [<run_dir2> ...]
  python backend/scripts/score_vision_benchmark.py --latest

Metrics per model:
  has_diff_accuracy   — for cases with ground truth: did the model correctly say
                        "diff exists" or "no diff"?
  must_find_score     — fraction of must_find keywords matched in the summary/diffs
  false_positive_score — penalty for inventing diffs on identical/noisy cases
  json_validity       — fraction of cases with parseable diff JSON
  stability           — fraction of cases without transport/HTTP error
  latency_score       — normalized inverse of avg latency
  hallucination_penalty — extra penalty for keywords from must_not_find

Composite:
  quality_score =
      0.35 * must_find_score
    + 0.25 * has_diff_accuracy
    + 0.15 * false_positive_score
    + 0.10 * json_validity
    + 0.10 * stability
    + 0.05 * latency_score
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "comparison" / "model_benchmarks"
RUNS = BENCH / "runs"


def text_for_match(entry: dict[str, Any]) -> str:
    bits: list[str] = []
    diff = entry.get("diff_json")
    if isinstance(diff, dict):
        bits.append(diff.get("summary") or "")
        for d in diff.get("differences", []) or []:
            if isinstance(d, dict):
                bits.append(d.get("description") or "")
                bits.append(d.get("evidence") or "")
                bits.append(d.get("type") or "")
    bits.append(entry.get("raw_text_preview") or "")
    return " ".join(bits).lower()


def keyword_match_fraction(text: str, keywords: list[str]) -> float:
    if not keywords:
        return 1.0
    if not text:
        return 0.0
    hits = 0
    for kw in keywords:
        kw_l = kw.lower().strip()
        if not kw_l:
            hits += 1
            continue
        # tolerant match: handle synonyms by reducing to alnum chars
        target = re.sub(r"\s+", " ", kw_l)
        if target in text:
            hits += 1
            continue
        # split into words and require >= 60% to appear
        words = [w for w in re.split(r"[\s,.;:/]+", target) if len(w) > 2]
        if not words:
            continue
        word_hits = sum(1 for w in words if w in text)
        if word_hits / len(words) >= 0.6:
            hits += 1
    return hits / len(keywords)


def hallucination_fraction(text: str, must_not: list[str]) -> float:
    if not must_not:
        return 0.0
    if not text:
        return 0.0
    triggered = 0
    for kw in must_not:
        kw_l = kw.lower().strip()
        if kw_l and kw_l in text:
            triggered += 1
    return triggered / len(must_not)


def predicted_has_diff(entry: dict[str, Any]) -> bool | None:
    """Try to infer the model's verdict whether a significant diff exists."""
    diff = entry.get("diff_json")
    if isinstance(diff, dict) and "has_significant_difference" in diff:
        v = diff["has_significant_difference"]
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.lower() in {"true", "yes", "да"}
    if isinstance(diff, dict) and isinstance(diff.get("differences"), list):
        return len(diff["differences"]) > 0
    text = (entry.get("raw_text_preview") or "").lower()
    if not text:
        return None
    if any(p in text for p in ["no significant", "идентичн", "отличий не", "нет значимых", "no differences"]):
        return False
    if any(p in text for p in ["добавлен", "удал", "изменён", "изменено", "added", "removed", "changed"]):
        return True
    return None


def score_model(entries: list[dict[str, Any]], cases_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    n = len(entries)
    success = [e for e in entries if e.get("ok")]
    json_valid = [e for e in entries if e.get("json_valid")]
    durations = [e["duration_sec"] for e in entries if e.get("duration_sec") is not None]

    # per-case accuracy where ground truth exists
    gt_entries = []
    gt_predictions: list[tuple[bool, bool | None, float, float]] = []
    must_find_scores: list[float] = []
    fp_inflation: list[float] = []
    halluc: list[float] = []
    for e in entries:
        case = cases_by_id.get(e["case_id"]) or {}
        exp = case.get("expected") or {}
        if exp.get("mode") != "ground_truth":
            continue
        gt_entries.append(e)
        text = text_for_match(e)
        mf = keyword_match_fraction(text, exp.get("must_find") or [])
        if exp.get("has_significant_difference") is True:
            must_find_scores.append(mf)
        else:
            must_find_scores.append(1.0)  # nothing required
        hl = hallucination_fraction(text, exp.get("must_not_find") or [])
        halluc.append(hl)
        pred = predicted_has_diff(e)
        gold = exp.get("has_significant_difference")
        gt_predictions.append((bool(gold), pred, mf, hl))
        # False positive inflation: model said diff exists where it doesn't
        if exp.get("has_significant_difference") is False:
            if pred is True or hl > 0.2:
                fp_inflation.append(0.0)
            else:
                fp_inflation.append(1.0)

    correct_dir = 0
    decided = 0
    for gold, pred, mf, hl in gt_predictions:
        if pred is None:
            continue
        decided += 1
        if pred == gold:
            correct_dir += 1

    has_diff_accuracy = correct_dir / decided if decided else 0.0
    must_find_score = (sum(must_find_scores) / len(must_find_scores)) if must_find_scores else 0.0
    false_positive_score = (sum(fp_inflation) / len(fp_inflation)) if fp_inflation else 1.0
    json_validity = len(json_valid) / n if n else 0.0
    stability = len(success) / n if n else 0.0
    avg_latency = (sum(durations) / len(durations)) if durations else float("inf")
    hallucination_penalty = (sum(halluc) / len(halluc)) if halluc else 0.0

    return {
        "n_cases": n,
        "ok_cases": len(success),
        "json_valid_cases": len(json_valid),
        "gt_cases": len(gt_entries),
        "has_diff_accuracy": round(has_diff_accuracy, 3),
        "must_find_score": round(must_find_score, 3),
        "false_positive_score": round(false_positive_score, 3),
        "hallucination_penalty": round(hallucination_penalty, 3),
        "json_validity": round(json_validity, 3),
        "stability": round(stability, 3),
        "avg_latency_sec": round(avg_latency, 2) if avg_latency != float("inf") else None,
        "decided_cases": decided,
    }


def latency_score(avg_latency: float | None, all_latencies: list[float]) -> float:
    if avg_latency is None or not all_latencies:
        return 0.0
    lo, hi = min(all_latencies), max(all_latencies)
    if hi <= lo:
        return 1.0
    return round(1.0 - (avg_latency - lo) / (hi - lo), 3)


def quality_score(s: dict[str, Any], lat: float) -> float:
    return round(
        0.35 * s["must_find_score"]
        + 0.25 * s["has_diff_accuracy"]
        + 0.15 * s["false_positive_score"]
        + 0.10 * s["json_validity"]
        + 0.10 * s["stability"]
        + 0.05 * lat,
        3,
    )


def score_run(run_dir: Path) -> dict[str, Any]:
    results_path = run_dir / "results.json"
    blob = json.loads(results_path.read_text(encoding="utf-8"))
    cases_blob = json.loads((BENCH / "dataset" / "cases.json").read_text(encoding="utf-8"))
    cases_by_id = {c["id"]: c for c in cases_blob["cases"]}

    by_model: dict[str, list[dict[str, Any]]] = {}
    for e in blob["results"]:
        by_model.setdefault(e["model"], []).append(e)

    raw_scores: dict[str, dict[str, Any]] = {m: score_model(es, cases_by_id) for m, es in by_model.items()}
    all_latencies = [s["avg_latency_sec"] for s in raw_scores.values() if s.get("avg_latency_sec")]
    ranking = []
    for model, s in raw_scores.items():
        lat = latency_score(s.get("avg_latency_sec"), all_latencies)
        qs = quality_score(s, lat)
        ranking.append({
            "model": model,
            "quality_score": qs,
            "latency_score": lat,
            **s,
        })
    ranking.sort(key=lambda r: r["quality_score"], reverse=True)

    out = {
        "run": blob["run"],
        "scored_models": ranking,
        "cases_total": len(cases_blob["cases"]),
    }
    (run_dir / "scores.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = run_dir / "leaderboard.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "rank", "model_id", "provider", "quality_score", "has_diff_accuracy",
            "must_find_score", "false_positive_score", "hallucination_penalty",
            "json_validity", "stability", "avg_latency_sec", "ok_cases", "n_cases",
        ])
        for i, r in enumerate(ranking, 1):
            w.writerow([
                i, r["model"], "lmstudio_ngrok", r["quality_score"],
                r["has_diff_accuracy"], r["must_find_score"], r["false_positive_score"],
                r["hallucination_penalty"], r["json_validity"], r["stability"],
                r["avg_latency_sec"], r["ok_cases"], r["n_cases"],
            ])

    md_lines = [
        f"# Leaderboard — {blob['run']}",
        "",
        f"- cases: {len(cases_blob['cases'])} (GT cases: {ranking[0]['gt_cases'] if ranking else 0})",
        f"- scored models: {len(ranking)}",
        "",
        "| rank | model | quality | has_diff_acc | must_find | fp_score | halluc | json_ok | stability | avg_lat_s | ok |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(ranking, 1):
        md_lines.append(
            f"| {i} | `{r['model']}` | **{r['quality_score']}** | {r['has_diff_accuracy']} | "
            f"{r['must_find_score']} | {r['false_positive_score']} | {r['hallucination_penalty']} | "
            f"{r['json_validity']} | {r['stability']} | {r['avg_latency_sec']} | {r['ok_cases']}/{r['n_cases']} |"
        )
    md_lines.append("")
    md_lines.append("**Composite formula:** quality = 0.35·must_find + 0.25·has_diff_acc + 0.15·fp_score + 0.10·json + 0.10·stability + 0.05·latency.")
    (run_dir / "leaderboard.md").write_text("\n".join(md_lines), encoding="utf-8")

    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dirs", nargs="*")
    parser.add_argument("--latest", action="store_true", help="score the latest run dir")
    args = parser.parse_args()

    targets: list[Path] = []
    if args.latest:
        all_runs = sorted(RUNS.glob("run_*"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not all_runs:
            print("no runs found", file=sys.stderr)
            return 2
        targets.append(all_runs[0])
    for d in args.run_dirs:
        targets.append(Path(d))
    if not targets:
        print("nothing to score", file=sys.stderr)
        return 2

    for t in targets:
        out = score_run(t)
        print(f"scored {t}: top={out['scored_models'][0]['model'] if out['scored_models'] else '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
