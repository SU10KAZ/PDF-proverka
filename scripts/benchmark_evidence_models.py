#!/usr/bin/env python3
"""Benchmark local vision models on evidence golden set (graphic cases)."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from backend.app.pipeline.stages.findings_review.evidence_verifier.context_loader import (
    load_finding_context,
)
from backend.app.pipeline.stages.findings_review.evidence_verifier.golden_set import load_golden_set
from backend.app.pipeline.stages.findings_review.evidence_verifier.graphic_verifier import (
    verify_graphic_async,
)

BENCHMARK_DIR = ROOT / "benchmarks" / "evidence_verify"
DEFAULT_MODELS = [
    "qwen/qwen3.6-35b-a3b",
    "qwen/qwen3.6-27b",
    "google/gemma-4-12b",
    "google/gemma-4-26b-a4b",
    "google/gemma-4-31b",
]


def _expert_to_llm(expert: str) -> str:
    return "accept" if expert == "accepted" else "reject"


async def _run_model(model: str, cases: list[dict], limit: int) -> dict:
    results = []
    correct = 0
    false_reject = 0
    latencies = []
    graphic_cases = [
        c for c in cases
        if c.get("case_class") in ("graphic_confirmed", "graphic_rejected", "graphic_mixed")
    ][:limit]

    for case in graphic_cases:
        finding = {**case["finding"], "id": case["item_id"]}
        pid = case["source_project"]
        t0 = time.time()
        try:
            ctx = load_finding_context(pid, finding, section=case.get("section") or "")
            if not any(b.png_path for b in ctx.blocks):
                results.append({"item_id": case["item_id"], "status": "no_png"})
                continue
            decision = await verify_graphic_async(ctx, model=model)
            elapsed = time.time() - t0
            latencies.append(elapsed)
            expected = _expert_to_llm(case["expert_decision"])
            match = decision.llm_decision == expected or (
                expected == "accept" and decision.llm_decision == "borderline"
            )
            if match:
                correct += 1
            if expected == "accept" and decision.llm_decision == "reject":
                false_reject += 1
            results.append({
                "item_id": case["item_id"],
                "expected": expected,
                "got": decision.llm_decision,
                "confidence": decision.confidence,
                "latency_sec": round(elapsed, 2),
                "match": match,
            })
        except Exception as exc:
            results.append({"item_id": case["item_id"], "status": "error", "error": str(exc)})

    n = len([r for r in results if r.get("match") is not None])
    return {
        "model": model,
        "cases_run": len(results),
        "scored": n,
        "accuracy": round(correct / n, 3) if n else 0,
        "false_reject_rate": round(false_reject / n, 3) if n else 0,
        "avg_latency_sec": round(sum(latencies) / len(latencies), 2) if latencies else 0,
        "results": results,
    }


async def main_async(models: list[str], limit: int) -> int:
    golden = load_golden_set()
    if not golden:
        print("Run build_evidence_golden_set.py first", file=sys.stderr)
        return 1
    cases = golden.get("cases", [])
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now().isoformat(),
        "golden_total": len(cases),
        "models": [],
    }
    for model in models:
        print(f"Benchmarking {model}...", flush=True)
        report["models"].append(await _run_model(model, cases, limit))

    report["models"].sort(key=lambda x: (-x["accuracy"], x["false_reject_rate"]))
    out = BENCHMARK_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(out), "leaderboard": [
        {k: m[k] for k in ("model", "accuracy", "false_reject_rate", "avg_latency_sec")}
        for m in report["models"]
    ]}, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    return asyncio.run(main_async(args.models, args.limit))


if __name__ == "__main__":
    raise SystemExit(main())
