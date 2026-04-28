"""
Confirmatory fail-mode probe for baseline Pro (gemini-3.1-pro-preview).

Goal:
    Pin down the exact fail-mode of baseline Pro on the 2 problematic blocks
    from the recall-hybrid run (20260422_073520) — coverage 88.2% with
    6DRC-7KQL-9TJ and 4MQJ-6NXP-4YH missing under "high reasoning".

    For each block, we replay 5 calls per mode and capture EVERYTHING needed
    to classify the failure:
      - empty array
      - wrong block_id
      - multiple analyses
      - truncate / healing artifact
      - api error / silent-fail

Modes (all baseline-style: reasoning.effort = "high"):
    A: healing ON,  parallelism = 2
    B: healing OFF, parallelism = 2
    C: healing ON,  parallelism = 1   (only if A still produced any failure)

For every individual call we save:
    raw_text_primary / raw_text_retry
    finish_reason / response_id (primary + retry)
    output_tokens / reasoning_tokens
    healing_initial / healing_used_for_final
    analysis_count_primary / analysis_count_retry / analysis_count_final
    classification (success / failure_mode)

Single-block strict success rule (matches run_one_block update):
    success iff len(block_analyses) == 1 AND analysis.block_id == input bid
    (empty/missing block_id in single-block → inferred to input bid → success)

Empty-response retry rule:
    On empty array (or missing field), ALWAYS retry once with
    response_healing=False, even if initial healing was already off.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    build_page_contexts,
    build_system_prompt,
)
from run_pro_diagnostic_small_project import (  # noqa: E402
    BlockResult,
    MAX_OUTPUT_TOKENS,
    MODEL_PRO,
    PER_BLOCK_TIMEOUT_SEC,
    PROJECT_REL,
    run_one_block,
)
from webapp.services.openrouter_block_batch import (  # noqa: E402
    load_openrouter_block_batch_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("failmode_probe")


PROBE_BLOCK_IDS = ["6DRC-7KQL-9TJ", "4MQJ-6NXP-4YH"]
N_REPEATS_DEFAULT = 5
RAW_TEXT_TRUNCATE = 8000  # chars — keep raw responses inspectable but JSON small


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8")


def _br_to_call_record(
    r: BlockResult,
    *,
    mode: str,
    repeat_idx: int,
    healing_initial: bool,
    parallelism: int,
    reasoning_effort: str,
) -> dict:
    return {
        "mode": mode,
        "repeat_idx": repeat_idx,
        "block_id": r.block_id,
        "page": r.page,
        "reasoning_effort": reasoning_effort,
        "parallelism": parallelism,
        "healing_initial": healing_initial,
        "healing_effective_for_final": r.healing_effective,
        "is_error": r.is_error,
        "error_message": r.error_message,
        "success": r.success,
        "failure_mode": r.failure_mode,
        "analysis_count_primary": r.analysis_count,
        "analysis_count_retry": r.analysis_count_retry,
        "analysis_count_final": r.analysis_count_final,
        "multiple_analyses_in_single_block": r.multiple_analyses_in_single_block,
        "wrong_block_id_count": r.wrong_block_id_count,
        "returned_block_id_primary": r.returned_block_id_primary,
        "returned_block_id_retry": r.returned_block_id_retry,
        "empty_response_detected": r.empty_response_detected,
        "empty_response_after_retry": r.empty_response_after_retry,
        "retry_attempted": r.retry_attempted,
        "retry_recovered": r.retry_recovered,
        "primary_finish_reason": r.primary_finish_reason,
        "retry_finish_reason": r.retry_finish_reason,
        "primary_response_id": r.primary_response_id,
        "retry_response_id": r.retry_response_id,
        "duration_ms": r.duration_ms,
        "prompt_tokens": r.prompt_tokens,
        "output_tokens": r.output_tokens,
        "reasoning_tokens": r.reasoning_tokens,
        "cached_tokens": r.cached_tokens,
        "cost_usd": r.cost_usd,
        "cost_source": r.cost_source,
        "raw_text_primary": (r.raw_text_primary or "")[:RAW_TEXT_TRUNCATE],
        "raw_text_primary_truncated": len(r.raw_text_primary or "") > RAW_TEXT_TRUNCATE,
        "raw_text_primary_full_len": len(r.raw_text_primary or ""),
        "raw_text_retry": (r.raw_text_retry or "")[:RAW_TEXT_TRUNCATE],
        "raw_text_retry_truncated": len(r.raw_text_retry or "") > RAW_TEXT_TRUNCATE,
        "raw_text_retry_full_len": len(r.raw_text_retry or ""),
        "parsed_block": r.parsed_block,  # final parsed analysis (may be None)
    }


async def run_mode(
    *,
    mode_id: str,
    label: str,
    blocks: list[dict],
    repeats: int,
    healing: bool,
    parallelism: int,
    reasoning_effort: str,
    project_dir: Path,
    project_info: dict,
    schema: dict,
) -> list[dict]:
    """Execute `repeats` × `len(blocks)` calls under one mode, return call records."""
    system_prompt = build_system_prompt(project_info, len(blocks))
    page_contexts = build_page_contexts(project_dir)

    sem = asyncio.Semaphore(parallelism)

    # Build the flat task list: every (repeat_idx, block) pair is independent.
    pairs: list[tuple[int, dict]] = []
    for rep in range(1, repeats + 1):
        for b in blocks:
            pairs.append((rep, b))

    records: list[dict | None] = [None] * len(pairs)

    async def _one(idx: int, rep: int, b: dict) -> None:
        async with sem:
            t0 = time.monotonic()
            r = await run_one_block(
                block=b,
                project_dir=project_dir,
                project_info=project_info,
                system_prompt=system_prompt,
                page_contexts=page_contexts,
                model=MODEL_PRO,
                reasoning_effort=reasoning_effort,
                response_healing_initial=healing,
                schema=schema,
                timeout=PER_BLOCK_TIMEOUT_SEC,
                max_output_tokens=MAX_OUTPUT_TOKENS,
                capture_raw=True,
            )
            wall = time.monotonic() - t0
            tag = "SUCCESS" if r.success else f"FAIL({r.failure_mode or 'unknown'})"
            logger.info(
                "[mode=%s rep=%d/%d %s] %s dur=%.1fs primary_n=%d retry_n=%s "
                "fr_p=%s fr_r=%s out_tok=%d reason_tok=%d heal_init=%s heal_used=%s",
                mode_id, rep, repeats, b["block_id"], tag, wall,
                r.analysis_count,
                r.analysis_count_retry if r.retry_attempted else "n/a",
                r.primary_finish_reason or "?",
                r.retry_finish_reason or "n/a",
                r.output_tokens, r.reasoning_tokens,
                healing, r.healing_effective,
            )
            records[idx] = _br_to_call_record(
                r,
                mode=mode_id,
                repeat_idx=rep,
                healing_initial=healing,
                parallelism=parallelism,
                reasoning_effort=reasoning_effort,
            )

    start = time.monotonic()
    await asyncio.gather(*(_one(i, rep, b) for i, (rep, b) in enumerate(pairs)))
    elapsed = time.monotonic() - start
    logger.info("Mode %s (%s) done in %.1fs across %d calls.", mode_id, label, elapsed, len(pairs))

    return [r for r in records if r is not None]


def summarize_mode(mode_id: str, label: str, records: list[dict]) -> dict:
    n = len(records)
    by_block: dict[str, list[dict]] = {}
    for r in records:
        by_block.setdefault(r["block_id"], []).append(r)

    success = sum(1 for r in records if r["success"])
    empty = sum(1 for r in records if r["empty_response_detected"])
    empty_after_retry = sum(1 for r in records if r["empty_response_after_retry"])
    multi = sum(1 for r in records if r["multiple_analyses_in_single_block"])
    wrong_id = sum(1 for r in records if r["wrong_block_id_count"])
    api_err = sum(1 for r in records if r["is_error"])
    retry_recovered = sum(1 for r in records if r["retry_recovered"])

    failure_modes = Counter(
        (r["failure_mode"] or "success") for r in records
    )
    finish_reasons_primary = Counter(r["primary_finish_reason"] or "(none)" for r in records)
    finish_reasons_retry = Counter(
        r["retry_finish_reason"] or "(no_retry)"
        for r in records if r["retry_attempted"]
    )

    per_block_summary = {}
    for bid, rs in by_block.items():
        per_block_summary[bid] = {
            "n": len(rs),
            "success": sum(1 for r in rs if r["success"]),
            "empty_response": sum(1 for r in rs if r["empty_response_detected"]),
            "empty_after_retry": sum(1 for r in rs if r["empty_response_after_retry"]),
            "multiple_analyses": sum(1 for r in rs if r["multiple_analyses_in_single_block"]),
            "wrong_block_id": sum(1 for r in rs if r["wrong_block_id_count"]),
            "api_error": sum(1 for r in rs if r["is_error"]),
            "retry_recovered": sum(1 for r in rs if r["retry_recovered"]),
            "failure_modes": dict(Counter(r["failure_mode"] or "success" for r in rs)),
            "primary_finish_reasons": dict(
                Counter(r["primary_finish_reason"] or "(none)" for r in rs)
            ),
            "median_output_tokens": _median([r["output_tokens"] for r in rs]),
            "median_reasoning_tokens": _median([r["reasoning_tokens"] for r in rs]),
            "median_duration_s": _median([r["duration_ms"] / 1000.0 for r in rs]),
            "max_raw_text_full_len_primary": max(
                (r["raw_text_primary_full_len"] for r in rs), default=0
            ),
        }

    return {
        "mode_id": mode_id,
        "label": label,
        "n_calls": n,
        "success": success,
        "success_rate": round(success / n, 3) if n else 0.0,
        "empty_response_count": empty,
        "empty_response_after_retry_count": empty_after_retry,
        "multiple_analyses_count": multi,
        "wrong_block_id_count": wrong_id,
        "api_error_count": api_err,
        "retry_recovered_count": retry_recovered,
        "failure_mode_distribution": dict(failure_modes),
        "primary_finish_reason_distribution": dict(finish_reasons_primary),
        "retry_finish_reason_distribution": dict(finish_reasons_retry),
        "per_block": per_block_summary,
    }


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def build_failmode_report_md(summaries: list[dict]) -> str:
    lines: list[str] = ["# Pro baseline fail-mode probe — summary\n"]
    lines.append("Confirmatory probe of the 2 baseline-failing blocks "
                 "(`6DRC-7KQL-9TJ`, `4MQJ-6NXP-4YH`) under high reasoning.\n")

    # Headline table: mode-level
    lines.append("## Mode totals\n")
    headers = ["Mode", "Label", "Calls", "Success", "Success%",
               "EmptyResp", "EmptyAfterRetry", "MultiAnalyses",
               "WrongBlockId", "APIError", "RetryRecovered"]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for s in summaries:
        lines.append(
            "| {mode_id} | {label} | {n} | {ok} | {pct}% | {er} | {ear} | {ma} | {wb} | {ae} | {rr} |".format(
                mode_id=s["mode_id"], label=s["label"],
                n=s["n_calls"], ok=s["success"],
                pct=int(round(s["success_rate"] * 100)),
                er=s["empty_response_count"],
                ear=s["empty_response_after_retry_count"],
                ma=s["multiple_analyses_count"],
                wb=s["wrong_block_id_count"],
                ae=s["api_error_count"],
                rr=s["retry_recovered_count"],
            )
        )
    lines.append("")

    # Per-mode failure breakdown
    for s in summaries:
        lines.append(f"## {s['mode_id']} — failure mode distribution")
        lines.append("```")
        lines.append(json.dumps(s["failure_mode_distribution"], ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append(f"Primary finish_reason distribution:")
        lines.append("```")
        lines.append(json.dumps(s["primary_finish_reason_distribution"], ensure_ascii=False, indent=2))
        lines.append("```")
        if s["retry_finish_reason_distribution"]:
            lines.append(f"Retry finish_reason distribution:")
            lines.append("```")
            lines.append(json.dumps(s["retry_finish_reason_distribution"], ensure_ascii=False, indent=2))
            lines.append("```")
        lines.append(f"### Per-block ({s['mode_id']})")
        for bid, pb in s["per_block"].items():
            lines.append(
                f"- `{bid}` n={pb['n']} success={pb['success']}/{pb['n']} "
                f"empty={pb['empty_response']} empty_after_retry={pb['empty_after_retry']} "
                f"multi={pb['multiple_analyses']} wrong_id={pb['wrong_block_id']} "
                f"api_err={pb['api_error']} retry_recovered={pb['retry_recovered']} "
                f"median_dur={pb['median_duration_s']:.1f}s "
                f"median_out_tok={pb['median_output_tokens']:.0f} "
                f"median_reason_tok={pb['median_reasoning_tokens']:.0f} "
                f"max_raw_len={pb['max_raw_text_full_len_primary']}"
            )
            if pb["failure_modes"]:
                lines.append(
                    f"  - failure_modes: {json.dumps(pb['failure_modes'], ensure_ascii=False)}"
                )
        lines.append("")

    # Verdict
    lines.append("## Diagnosis (rule-based)\n")
    diag = diagnose(summaries)
    for line in diag:
        lines.append(f"- {line}")
    lines.append("")
    return "\n".join(lines)


def diagnose(summaries: list[dict]) -> list[str]:
    """Produce rule-based fail-mode diagnosis."""
    out: list[str] = []
    by_id = {s["mode_id"]: s for s in summaries}

    def dom(s: dict) -> str:
        if not s["failure_mode_distribution"]:
            return "—"
        # Drop "success" key when picking dominant FAILURE
        non_success = {k: v for k, v in s["failure_mode_distribution"].items() if k != "success"}
        if not non_success:
            return "all_success"
        return max(non_success.items(), key=lambda kv: kv[1])[0]

    a = by_id.get("A")
    b = by_id.get("B")
    if a:
        if a["empty_response_count"] > 0 and a["empty_response_after_retry_count"] > 0:
            out.append(
                f"Mode A reproduces baseline empty-response failure: "
                f"{a['empty_response_count']} primaries empty, "
                f"{a['empty_response_after_retry_count']} still empty after retry. "
                f"This is a HARD silent-fail (model returns valid JSON envelope but "
                f"`block_analyses == []`)."
            )
        elif a["empty_response_count"] > 0 and a["retry_recovered_count"] > 0:
            out.append(
                f"Mode A: {a['empty_response_count']} empties → "
                f"{a['retry_recovered_count']} recovered after retry-without-healing. "
                f"Suggests the empty-array is a TRANSIENT issue or healing-amplified."
            )
        elif a["empty_response_count"] == 0 and a["multiple_analyses_count"] == 0 and a["wrong_block_id_count"] == 0:
            if a["success"] == a["n_calls"]:
                out.append("Mode A: NO failures observed in this probe — baseline 2/17 missing not reproduced under N=5 (transient/rate-dependent?).")
            else:
                out.append(f"Mode A failures dominated by `{dom(a)}` (no empty/multi/wrong-id).")
        else:
            out.append(
                f"Mode A failure mix: empty={a['empty_response_count']}, "
                f"multi={a['multiple_analyses_count']}, wrong_id={a['wrong_block_id_count']}, "
                f"api_err={a['api_error_count']}; dominant non-success = `{dom(a)}`"
            )

    if a and b:
        delta_succ = b["success"] - a["success"]
        if delta_succ > 0:
            out.append(
                f"Mode B (healing OFF) success {b['success']}/{b['n_calls']} > A {a['success']}/{a['n_calls']} "
                f"(+{delta_succ}) → healing is implicated in the failure."
            )
        elif delta_succ < 0:
            out.append(
                f"Mode B (healing OFF) success {b['success']}/{b['n_calls']} < A {a['success']}/{a['n_calls']} "
                f"({delta_succ}) → healing was actually helping."
            )
        else:
            out.append(
                f"Mode B success ≈ Mode A → healing is NOT the differentiator."
            )

    c = by_id.get("C")
    if c:
        ref = a if a else b
        if ref:
            delta = c["success"] - ref["success"] * (c["n_calls"] / max(ref["n_calls"], 1))
            if delta > 0:
                out.append(
                    f"Mode C (parallelism=1) success {c['success']}/{c['n_calls']} better than reference → "
                    f"there's a concurrency/rate component to the failure."
                )
            else:
                out.append(
                    f"Mode C (parallelism=1) success {c['success']}/{c['n_calls']} no better than reference → "
                    f"failure is NOT concurrency-related."
                )
    return out


# ── Main ─────────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> int:
    project_dir = Path(args.project_dir).resolve()
    blocks_index = json.loads(
        (project_dir / "_output" / "blocks" / "index.json").read_text(encoding="utf-8")
    )
    by_id = {b["block_id"]: b for b in blocks_index["blocks"]}
    blocks = [by_id[b] for b in PROBE_BLOCK_IDS if b in by_id]
    if len(blocks) != len(PROBE_BLOCK_IDS):
        missing = set(PROBE_BLOCK_IDS) - {b["block_id"] for b in blocks}
        logger.error("Missing probe blocks in index: %s", missing)
        return 2

    project_info = json.loads((project_dir / "project_info.json").read_text(encoding="utf-8"))
    schema = load_openrouter_block_batch_schema()

    ts = _ts()
    out_dir = project_dir / "_experiments" / "pro_baseline_failmode_probe" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "timestamp": ts,
        "project_dir": str(project_dir),
        "model": MODEL_PRO,
        "probe_block_ids": PROBE_BLOCK_IDS,
        "n_repeats": args.repeats,
        "reasoning_effort": "high",
        "modes": [
            {"id": "A", "label": "high+heal_on,par=2", "healing": True, "parallelism": 2},
            {"id": "B", "label": "high+heal_off,par=2", "healing": False, "parallelism": 2},
            {"id": "C (conditional)", "label": "high+heal_on,par=1", "healing": True, "parallelism": 1,
             "trigger": "any failure in A"},
        ],
        "strict_success_rule": "len(block_analyses)==1 AND block_id matches input",
        "retry_rule": "on empty array → 1 retry with response_healing=False (always)",
        "openrouter_path": True,
        "strict_schema": True,
        "require_parameters": True,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "per_block_timeout_sec": PER_BLOCK_TIMEOUT_SEC,
        "raw_text_truncate": RAW_TEXT_TRUNCATE,
    }
    _save_json(out_dir / "manifest.json", manifest)

    summaries: list[dict] = []

    # ── Mode A ────────────────────────────────────────────────────────────
    logger.info("=== Mode A: high reasoning + healing ON, parallelism=2 ===")
    a_records = await run_mode(
        mode_id="A", label="high+heal_on,par=2",
        blocks=blocks, repeats=args.repeats,
        healing=True, parallelism=2, reasoning_effort="high",
        project_dir=project_dir, project_info=project_info, schema=schema,
    )
    _save_json(out_dir / "mode_A_calls.json", a_records)
    a_summary = summarize_mode("A", "high+heal_on,par=2", a_records)
    _save_json(out_dir / "mode_A_summary.json", a_summary)
    summaries.append(a_summary)

    # ── Mode B ────────────────────────────────────────────────────────────
    logger.info("=== Mode B: high reasoning + healing OFF, parallelism=2 ===")
    b_records = await run_mode(
        mode_id="B", label="high+heal_off,par=2",
        blocks=blocks, repeats=args.repeats,
        healing=False, parallelism=2, reasoning_effort="high",
        project_dir=project_dir, project_info=project_info, schema=schema,
    )
    _save_json(out_dir / "mode_B_calls.json", b_records)
    b_summary = summarize_mode("B", "high+heal_off,par=2", b_records)
    _save_json(out_dir / "mode_B_summary.json", b_summary)
    summaries.append(b_summary)

    # ── Mode C (conditional) ──────────────────────────────────────────────
    needs_c = (a_summary["success"] < a_summary["n_calls"])
    if needs_c:
        logger.info("=== Mode C: high reasoning + healing ON, parallelism=1 (triggered by A failures) ===")
        c_records = await run_mode(
            mode_id="C", label="high+heal_on,par=1",
            blocks=blocks, repeats=args.repeats,
            healing=True, parallelism=1, reasoning_effort="high",
            project_dir=project_dir, project_info=project_info, schema=schema,
        )
        _save_json(out_dir / "mode_C_calls.json", c_records)
        c_summary = summarize_mode("C", "high+heal_on,par=1", c_records)
        _save_json(out_dir / "mode_C_summary.json", c_summary)
        summaries.append(c_summary)
    else:
        logger.info("Skipping Mode C: Mode A had no failures.")

    # ── Final report ─────────────────────────────────────────────────────
    rep_md = build_failmode_report_md(summaries)
    (out_dir / "failmode_report.md").write_text(rep_md, encoding="utf-8")
    _save_json(out_dir / "all_summaries.json", summaries)

    logger.info("Done. Artifacts: %s", out_dir)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--project-dir",
        default=str(_ROOT / PROJECT_REL),
        help="Project dir (default: KJ small project).",
    )
    p.add_argument(
        "--repeats", type=int, default=N_REPEATS_DEFAULT,
        help=f"Repeats per (mode, block). Default {N_REPEATS_DEFAULT}.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
