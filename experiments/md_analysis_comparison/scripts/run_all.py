"""Orchestrator: run both methods on a list of cases, then compare.

Usage:
  python scripts/run_all.py                  # all cases
  python scripts/run_all.py --only eom_01_cable_sizing ov_01_ventilation
  python scripts/run_all.py --methods current_method
  python scripts/run_all.py --parallel-cases 2
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners import current_method_runner, multi_agent_method_runner  # noqa: E402
from scripts.compare_results import compare_dataset  # noqa: E402


def _run_case(case_id: str, method: str) -> tuple[str, str, bool, str]:
    case_dir = cfg.DATASETS_DIR / case_id
    out_dir = cfg.RESULTS_DIR / case_id
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        if method == "current_method":
            r = current_method_runner.run(case_dir, out_dir / "current.json")
        elif method == "multi_agent":
            r = multi_agent_method_runner.run(case_dir, out_dir / "multi_agent.json")
        else:
            return case_id, method, False, f"unknown method: {method}"
        return case_id, method, True, f"{len(r.findings)} findings in {r.duration_sec:.1f}s"
    except Exception as exc:
        return case_id, method, False, f"exception: {exc!r}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", default=None, help="Specific case_ids to run")
    ap.add_argument("--methods", nargs="*", default=["current_method", "multi_agent"])
    ap.add_argument("--parallel-cases", type=int, default=1,
                    help="How many case×method tasks to run concurrently")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Skip case×method if its result file already exists")
    ap.add_argument("--no-compare", action="store_true")
    args = ap.parse_args()

    if args.only:
        cases = args.only
    else:
        cases = sorted(d.name for d in cfg.DATASETS_DIR.iterdir() if d.is_dir() and (d / "case.json").exists())

    tasks: list[tuple[str, str]] = []
    for c in cases:
        for m in args.methods:
            target = cfg.RESULTS_DIR / c / (f"{'current' if m == 'current_method' else 'multi_agent'}.json")
            if args.skip_existing and target.exists():
                print(f"[skip] {c}/{m} exists")
                continue
            tasks.append((c, m))

    print(f"Running {len(tasks)} tasks ({len(cases)} cases × {len(args.methods)} methods), "
          f"parallel={args.parallel_cases}")
    started = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.parallel_cases)) as ex:
        futures = [ex.submit(_run_case, c, m) for c, m in tasks]
        for fut in as_completed(futures):
            cid, m, ok, msg = fut.result()
            tag = "OK" if ok else "FAIL"
            print(f"[{tag}] {cid}/{m}: {msg}")
            results.append({"case": cid, "method": m, "ok": ok, "msg": msg})
    duration = time.time() - started
    print(f"\nTotal: {duration:.1f}s")

    summary_path = cfg.LOGS_DIR / "run_all_summary.json"
    summary_path.write_text(json.dumps({"duration_sec": duration, "results": results},
                                       ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Run summary: {summary_path}")

    if not args.no_compare:
        out = compare_dataset(cfg.DATASETS_DIR, cfg.RESULTS_DIR, cfg.COMPARISON_OUTPUTS_DIR)
        print(f"\nComparison done. Cases: {len(out['table_rows'])}")
        print(f"Methods aggregate: {json.dumps(out['summary'], ensure_ascii=False)}")
        print(f"Markdown table: {cfg.COMPARISON_OUTPUTS_DIR / 'table.md'}")


if __name__ == "__main__":
    main()
