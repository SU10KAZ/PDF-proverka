"""Aggregate multiple benchmark runs into a final summary with variance.

Usage:
  python backend/scripts/aggregate_vision_benchmark.py <run_dir_main> <run_dir_top3_a> <run_dir_top3_b> [...]
  python backend/scripts/aggregate_vision_benchmark.py --auto    # picks latest -main2 + all -top3* runs

Writes:
  comparison/model_benchmarks/final_report.md
  comparison/model_benchmarks/aggregate.json
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "comparison" / "model_benchmarks"
RUNS = BENCH / "runs"


def load_run(run_dir: Path) -> dict[str, Any]:
    scores_path = run_dir / "scores.json"
    if not scores_path.exists():
        # try to score on the fly
        from score_vision_benchmark import score_run as _score  # type: ignore
        _score(run_dir)
    return json.loads((run_dir / "scores.json").read_text(encoding="utf-8"))


def aggregate(main_run: Path, repeat_runs: list[Path]) -> dict[str, Any]:
    main = load_run(main_run)
    main_by_model: dict[str, dict[str, Any]] = {r["model"]: r for r in main["scored_models"]}

    repeats: dict[str, list[dict[str, Any]]] = {}
    for rd in repeat_runs:
        s = load_run(rd)
        for r in s["scored_models"]:
            repeats.setdefault(r["model"], []).append(r)

    aggregated: list[dict[str, Any]] = []
    for model_id, base in main_by_model.items():
        all_qs = [base["quality_score"]]
        all_mf = [base["must_find_score"]]
        all_dur = [base.get("avg_latency_sec") or 0]
        rep_entries = repeats.get(model_id, [])
        for r in rep_entries:
            all_qs.append(r["quality_score"])
            all_mf.append(r["must_find_score"])
            all_dur.append(r.get("avg_latency_sec") or 0)
        std_qs = statistics.pstdev(all_qs) if len(all_qs) > 1 else 0.0
        std_mf = statistics.pstdev(all_mf) if len(all_mf) > 1 else 0.0
        mean_qs = statistics.mean(all_qs)
        aggregated.append({
            "model": model_id,
            "main_quality": base["quality_score"],
            "mean_quality": round(mean_qs, 3),
            "std_quality": round(std_qs, 3),
            "std_must_find": round(std_mf, 3),
            "runs_count": len(all_qs),
            "main_metrics": base,
            "repeat_metrics": rep_entries,
            "mean_latency_sec": round(statistics.mean(all_dur), 2) if all_dur else None,
        })
    aggregated.sort(key=lambda a: (a["mean_quality"], -a["std_quality"]), reverse=True)
    return {
        "main_run": main["run"],
        "repeat_run_count": len(repeat_runs),
        "models": aggregated,
    }


def write_final_report(agg: dict[str, Any]) -> Path:
    out = BENCH / "final_report.md"
    discovered = json.loads((BENCH / "discovered_models.json").read_text(encoding="utf-8"))
    cases = json.loads((BENCH / "dataset" / "cases.json").read_text(encoding="utf-8"))

    ranked = agg["models"]
    primary = ranked[0] if ranked else None
    fallback = ranked[1] if len(ranked) > 1 else None

    lines: list[str] = []
    lines.append("# Vision-Model Benchmark — Final Report")
    lines.append("")
    lines.append(f"- Generated: 2026-05-25")
    lines.append(f"- Main run: `{agg['main_run']}`")
    lines.append(f"- Repeat (top-3) runs: {agg['repeat_run_count']}")
    lines.append(f"- Dataset cases: {cases['case_count']} ({', '.join(cases['categories'])})")
    lines.append("")
    lines.append("## 1. Endpoints discovered")
    for ep in discovered["endpoints"]:
        lines.append(f"- **{ep['id']}** — `{ep['base_url']}` ({ep['kind']}), status: {ep['status']}")
    lines.append("")
    lines.append("**Skipped as external paid APIs:**")
    for ex in discovered.get("external_endpoints_skipped", []):
        lines.append(f"- `{ex['name']}` (env `{ex['env_var']}`) → `{ex['reason']}`")
    lines.append("")
    if discovered.get("local_endpoints_probed_no_response"):
        lines.append("**Local endpoints probed but unreachable:**")
        for ep in discovered["local_endpoints_probed_no_response"]:
            lines.append(f"- `{ep['url']}` → `{ep['reason']}`")
        lines.append("")
    lines.append("## 2. Models discovered")
    total_listed = discovered["summary"]["total_models_listed"]
    vlm = discovered["summary"]["vlm_models_total"]
    sel = discovered["summary"]["vlm_selected_for_benchmark"]
    lines.append(f"Listed: {total_listed} · VLM: {vlm} · Selected for benchmark: {sel}")
    lines.append("")
    lines.append("| id | type | arch | quant | selected? | reason |")
    lines.append("|---|---|---|---|---|---|")
    for m in discovered["models"]:
        sel_str = "✓" if m.get("selected_for_benchmark") else f"✗ ({m.get('skip_reason','')})"
        lines.append(f"| `{m['id']}` | {m['type']} | {m.get('arch','-')} | {m.get('quantization','-')} | {sel_str} | {m.get('notes','')[:80]} |")
    lines.append("")
    lines.append("## 3. Leaderboard (main run + variance across reruns)")
    lines.append("")
    lines.append("| rank | model | main_q | mean_q | std_q | runs | latency_s |")
    lines.append("|---|---|---|---|---|---|---|")
    for i, r in enumerate(ranked, 1):
        lines.append(f"| {i} | `{r['model']}` | {r['main_quality']} | **{r['mean_quality']}** | {r['std_quality']} | {r['runs_count']} | {r['mean_latency_sec']} |")
    lines.append("")
    lines.append("## 4. Per-metric leaderboard (main run only)")
    lines.append("")
    lines.append("| model | quality | has_diff_acc | must_find | fp_score | halluc | json_ok | stability |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in ranked:
        m = r["main_metrics"]
        lines.append(f"| `{r['model']}` | {m['quality_score']} | {m['has_diff_accuracy']} | {m['must_find_score']} | {m['false_positive_score']} | {m['hallucination_penalty']} | {m['json_validity']} | {m['stability']} |")
    lines.append("")
    lines.append("## 5. Recommendation")
    lines.append("")
    if primary:
        lines.append(f"**Primary:** `{primary['model']}` — mean quality {primary['mean_quality']} ± {primary['std_quality']}")
        m = primary["main_metrics"]
        lines.append(f"- has_diff_accuracy: {m['has_diff_accuracy']}, must_find: {m['must_find_score']}, fp_score: {m['false_positive_score']}, json: {m['json_validity']}, stability: {m['stability']}, avg latency: {m['avg_latency_sec']}s")
    else:
        lines.append("**Primary:** _none — no suitable vision model found_")
    if fallback:
        lines.append(f"\n**Fallback:** `{fallback['model']}` — mean quality {fallback['mean_quality']} ± {fallback['std_quality']}")
    lines.append("")
    if primary:
        lines.append("### Suggested env (NOT applied automatically)")
        lines.append("```bash")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER=local_openai_compatible")
        lines.append(f"STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL={discovered['endpoints'][0]['base_url']}")
        lines.append(f"STAGE_COMPARISON_GRAPHIC_LLM_MODEL={primary['model']}")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE=0.0")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS=1800")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC=300")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE=1100")
        lines.append("STAGE_COMPARISON_GRAPHIC_LLM_AUTH=basic")
        lines.append("# (Basic Auth header uses NGROK_AUTH_USER/NGROK_AUTH_PASS already in .env)")
        lines.append("```")
    lines.append("")
    lines.append("## 6. Notes & limitations")
    lines.append("- OpenRouter / Gemini / api.openai.com / anthropic.com APIs were **not used** in this benchmark.")
    lines.append("- All requests went to the user's local LM Studio via ngrok (Basic Auth).")
    lines.append("- 14 of 22 cases have ground-truth labels (synthetic); 8 are real stage_1/stage_2 page pairs marked `manual_review`.")
    lines.append("- Models were JIT-loaded one at a time via `/api/v1/models/load`; only one VLM instance was active at any moment.")
    lines.append("- chandra-ocr-2 was reloaded as the default at the end of the benchmark.")
    lines.append("- Production graphic-diff provider was NOT modified.")
    lines.append("- The webapp, UI, batch jobs, findings, and reports were NOT modified.")
    lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dirs", nargs="*", help="main run dir followed by repeat run dirs")
    parser.add_argument("--auto", action="store_true", help="discover latest -main2 and any -top3* runs")
    args = parser.parse_args()

    main_run: Path | None = None
    repeat_runs: list[Path] = []
    if args.auto:
        all_runs = sorted(RUNS.glob("run_*"), key=lambda p: p.stat().st_mtime)
        for r in reversed(all_runs):
            if main_run is None and r.name.endswith("-main2"):
                main_run = r
                break
        repeat_runs = [r for r in all_runs if "-top3" in r.name]
    elif args.run_dirs:
        main_run = Path(args.run_dirs[0])
        repeat_runs = [Path(p) for p in args.run_dirs[1:]]

    if not main_run:
        print("no main run found", file=sys.stderr)
        return 2

    # ensure score files exist
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    agg = aggregate(main_run, repeat_runs)
    (BENCH / "aggregate.json").write_text(json.dumps(agg, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path = write_final_report(agg)
    print(f"wrote {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
