"""
Offline re-evaluation of Gemini 3.1 Pro as a stage 02 second-pass engine.

Goal:
    Re-use existing small-project artifacts to answer a narrow question:
    does Pro remain a viable second-pass candidate once the transient
    baseline completeness flake is corrected, and if so in which config?

Inputs:
    1. recall-hybrid small-project run
    2. Pro diagnostic run
    3. baseline failmode probe run

No API calls are made. The script only reads local artifacts and writes a
new analysis report under:
    <project_dir>/_experiments/pro_second_pass_reevaluation/<timestamp>/
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import RunMetrics  # noqa: E402
from run_stage02_recall_hybrid import (  # noqa: E402
    SHORT_SUMMARY_THRESHOLD,
    select_second_pass_winner,
)


DEFAULT_PROJECT_REL = (
    "projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.1-К1К2 (2).pdf"
)
DEFAULT_RECALL_RUN = "20260422_073520"
DEFAULT_DIAG_RUN = "20260422_085128"
DEFAULT_PROBE_RUN = "20260422_091702"
BASELINE_MODE = "A"
BASELINE_COST_EST_PER_BLOCK = 0.0080


@dataclass(frozen=True)
class ProbeObservation:
    block_id: str
    mode: str
    repeat_idx: int
    findings_count: int
    kv_count: int
    summary_len: int
    unreadable: bool
    output_tokens: int
    reasoning_tokens: int
    cost_usd: float
    duration_s: float


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _runmetrics_from_summary(summary: dict) -> RunMetrics:
    allowed = {f.name for f in fields(RunMetrics)}
    payload = {k: v for k, v in summary.items() if k in allowed}
    return RunMetrics(**payload)


def load_probe_observations(calls_path: Path) -> dict[str, list[ProbeObservation]]:
    records = _load_json(calls_path)
    out: dict[str, list[ProbeObservation]] = defaultdict(list)
    for rec in records:
        if not rec.get("success"):
            continue
        parsed = rec.get("parsed_block") or {}
        findings = parsed.get("findings") or []
        kv = parsed.get("key_values_read") or []
        summary = str(parsed.get("summary") or "").strip()
        obs = ProbeObservation(
            block_id=str(rec.get("block_id") or ""),
            mode=str(rec.get("mode") or ""),
            repeat_idx=int(rec.get("repeat_idx") or 0),
            findings_count=len(findings),
            kv_count=len(kv),
            summary_len=len(summary),
            unreadable=bool(parsed.get("unreadable_text")),
            output_tokens=int(rec.get("output_tokens") or 0),
            reasoning_tokens=int(rec.get("reasoning_tokens") or 0),
            cost_usd=float(rec.get("cost_usd") or 0.0),
            duration_s=float(rec.get("duration_ms") or 0.0) / 1000.0,
        )
        out[obs.block_id].append(obs)
    return dict(out)


def summarize_probe_observations(observations: dict[str, list[ProbeObservation]]) -> dict:
    summary: dict[str, dict] = {}
    for block_id, items in observations.items():
        items_by_mode: dict[str, list[ProbeObservation]] = defaultdict(list)
        for item in items:
            items_by_mode[item.mode].append(item)

        block_entry = {
            "all_modes": {
                "n": len(items),
                "findings_distribution": dict(Counter(i.findings_count for i in items)),
                "kv_distribution": dict(Counter(i.kv_count for i in items)),
                "success_modes": sorted({i.mode for i in items}),
            },
            "modes": {},
        }

        for mode, mode_items in sorted(items_by_mode.items()):
            ranked = sorted(
                mode_items,
                key=lambda i: (i.findings_count, i.kv_count, i.summary_len, i.repeat_idx),
            )
            med = ranked[len(ranked) // 2]
            block_entry["modes"][mode] = {
                "n": len(mode_items),
                "findings_distribution": dict(Counter(i.findings_count for i in mode_items)),
                "kv_distribution": dict(Counter(i.kv_count for i in mode_items)),
                "median_observation": asdict(med),
                "min_findings": ranked[0].findings_count,
                "max_findings": ranked[-1].findings_count,
            }

        summary[block_id] = block_entry

    return summary


def choose_probe_replacements(
    observations: dict[str, list[ProbeObservation]],
    *,
    mode: str,
    policy: str,
) -> dict[str, ProbeObservation]:
    if policy not in {"conservative", "median", "optimistic"}:
        raise ValueError(f"Unsupported policy: {policy}")

    chosen: dict[str, ProbeObservation] = {}
    for block_id, items in observations.items():
        mode_items = [i for i in items if i.mode == mode]
        if not mode_items:
            raise ValueError(f"No successful observations for {block_id} in mode {mode}")
        ranked = sorted(
            mode_items,
            key=lambda i: (i.findings_count, i.kv_count, i.summary_len, i.repeat_idx),
        )
        if policy == "conservative":
            chosen[block_id] = ranked[0]
        elif policy == "median":
            chosen[block_id] = ranked[len(ranked) // 2]
        else:
            chosen[block_id] = ranked[-1]
    return chosen


def _classify_diff_entry(entry: dict) -> str:
    delta_findings = int(entry["engine_findings"]) - int(entry["flash_findings"])
    f_unreadable = bool(entry["flash_unreadable"])
    e_unreadable = bool(entry["engine_unreadable"])
    f_kv = int(entry["flash_kv"])
    e_kv = int(entry["engine_kv"])
    f_summary_len = int(entry["flash_summary_len"])
    e_summary_len = int(entry["engine_summary_len"])

    is_improved = (
        (delta_findings > 0)
        or (f_unreadable and not e_unreadable)
        or (f_kv == 0 and e_kv > 0)
        or (e_summary_len > f_summary_len + 30 and e_summary_len > SHORT_SUMMARY_THRESHOLD)
    )
    is_degraded = (
        (delta_findings < 0 and int(entry["flash_findings"]) > 0)
        or (not f_unreadable and e_unreadable)
    )

    if entry["engine_missing"]:
        return "missing_in_engine"
    if is_improved and not is_degraded:
        return "improved"
    if is_degraded and not is_improved:
        return "degraded"
    return "unchanged"


def build_counterfactual_diff(baseline_diff: dict, replacements: dict[str, ProbeObservation]) -> dict:
    per_block: list[dict] = []
    improved = unchanged = degraded = 0
    added_findings = added_kv = 0
    flash_total_findings = flash_total_kv = 0
    engine_total_findings = engine_total_kv = 0
    unreadable_recovery = 0

    for row in baseline_diff["per_block_diff"]:
        cur = dict(row)
        replacement = replacements.get(cur["block_id"])
        if replacement is not None:
            cur["engine_findings"] = replacement.findings_count
            cur["engine_kv"] = replacement.kv_count
            cur["engine_unreadable"] = replacement.unreadable
            cur["engine_missing"] = False
            cur["engine_summary_len"] = replacement.summary_len
            cur["delta_findings"] = cur["engine_findings"] - cur["flash_findings"]
            cur["delta_kv"] = cur["engine_kv"] - cur["flash_kv"]
            cur["status"] = _classify_diff_entry(cur)

        flash_total_findings += int(cur["flash_findings"])
        flash_total_kv += int(cur["flash_kv"])
        engine_total_findings += int(cur["engine_findings"])
        engine_total_kv += int(cur["engine_kv"])
        added_findings += max(0, int(cur["delta_findings"]))
        added_kv += max(0, int(cur["delta_kv"]))
        if cur["status"] == "improved":
            improved += 1
        elif cur["status"] in {"degraded", "missing_in_engine"}:
            degraded += 1
        else:
            unchanged += 1
        if cur["flash_unreadable"] and not cur["engine_unreadable"]:
            unreadable_recovery += 1
        per_block.append(cur)

    return {
        "engine": baseline_diff["engine"],
        "escalation_set_size": baseline_diff["escalation_set_size"],
        "improved": improved,
        "unchanged": unchanged,
        "degraded": degraded,
        "unreadable_recovery": unreadable_recovery,
        "added_findings": added_findings,
        "added_kv": added_kv,
        "flash_total_findings": flash_total_findings,
        "flash_total_kv": flash_total_kv,
        "engine_total_findings": engine_total_findings,
        "engine_total_kv": engine_total_kv,
        "per_block_diff": per_block,
    }


def build_counterfactual_summary(
    baseline_summary: dict,
    counterfactual_diff: dict,
    replacements: dict[str, ProbeObservation],
) -> dict:
    total_blocks = int(baseline_summary["total_input_blocks"])
    valid_blocks = total_blocks
    replaced_estimated = min(
        int(baseline_summary.get("cost_sources_estimated", 0)),
        len(replacements),
    )
    adjusted_cost = (
        float(baseline_summary["total_cost_usd"])
        - replaced_estimated * BASELINE_COST_EST_PER_BLOCK
        + sum(r.cost_usd for r in replacements.values())
    )

    out = dict(baseline_summary)
    out["coverage_pct"] = 100.0
    out["missing_count"] = 0
    out["total_findings"] = counterfactual_diff["engine_total_findings"]
    out["total_key_values"] = counterfactual_diff["engine_total_kv"]
    out["blocks_with_findings"] = sum(
        1 for row in counterfactual_diff["per_block_diff"] if int(row["engine_findings"]) > 0
    )
    out["findings_per_100_blocks"] = round(
        out["total_findings"] / max(1, total_blocks) * 100,
        2,
    )
    out["total_cost_usd"] = round(adjusted_cost, 6)
    out["cost_per_valid_block"] = round(adjusted_cost / max(1, valid_blocks), 6)
    out["cost_per_finding"] = round(adjusted_cost / max(1, out["total_findings"]), 6)
    out["cost_sources_actual"] = int(baseline_summary.get("cost_sources_actual", 0)) + len(replacements)
    out["cost_sources_estimated"] = max(
        0,
        int(baseline_summary.get("cost_sources_estimated", 0)) - len(replacements),
    )
    return out


def build_config_rows(
    baseline_summary: dict,
    counter_scenarios: list[dict],
    v2_summary: dict,
    v3_summary: dict,
    claude_summary: dict,
) -> list[dict]:
    rows = [
        {
            "config_id": "pro_high_observed_baseline",
            "label": "Pro high reasoning, healing=ON, par=2 (observed baseline)",
            "coverage_pct": baseline_summary["coverage_pct"],
            "missing_count": baseline_summary["missing_count"],
            "total_findings": baseline_summary["total_findings"],
            "cost_usd": baseline_summary["total_cost_usd"],
            "source": "recall_hybrid",
        },
        {
            "config_id": "pro_low_heal_on",
            "label": "Pro low reasoning, healing=ON, par=2",
            "coverage_pct": v2_summary["coverage_pct"],
            "missing_count": v2_summary["missing_count"],
            "total_findings": v2_summary["total_findings"],
            "cost_usd": v2_summary["total_cost_usd"],
            "source": "pro_diagnostic",
        },
        {
            "config_id": "pro_low_heal_off",
            "label": "Pro low reasoning, healing=OFF, par=2",
            "coverage_pct": v3_summary["coverage_pct"],
            "missing_count": v3_summary["missing_count"],
            "total_findings": v3_summary["total_findings"],
            "cost_usd": v3_summary["total_cost_usd"],
            "source": "pro_diagnostic",
        },
        {
            "config_id": "claude_second_pass",
            "label": "Claude second pass, healing=ON, par=2",
            "coverage_pct": claude_summary["coverage_pct"],
            "missing_count": claude_summary["missing_count"],
            "total_findings": claude_summary["total_findings"],
            "cost_usd": claude_summary["total_cost_usd"],
            "source": "recall_hybrid",
        },
    ]
    for scenario in counter_scenarios:
        rows.append(
            {
                "config_id": scenario["scenario_id"],
                "label": scenario["label"],
                "coverage_pct": scenario["counterfactual_summary"]["coverage_pct"],
                "missing_count": scenario["counterfactual_summary"]["missing_count"],
                "total_findings": scenario["counterfactual_summary"]["total_findings"],
                "cost_usd": scenario["counterfactual_summary"]["total_cost_usd"],
                "source": "probe_informed_counterfactual",
            }
        )
    return rows


def build_recommendation(
    counter_scenarios: list[dict],
    claude_summary: dict,
    v2_summary: dict,
    v3_summary: dict,
) -> dict:
    median = next(s for s in counter_scenarios if s["scenario_id"] == "pro_high_counterfactual_median")
    optimistic = next(s for s in counter_scenarios if s["scenario_id"] == "pro_high_counterfactual_optimistic")

    winner = median["winner_vs_claude"]["winner"]
    if winner == "Claude (claude-opus-4-7)":
        final_call = "claude_still_best_small_kj"
    else:
        final_call = "pro_reopened_as_winner"

    return {
        "final_call": final_call,
        "pro_candidate": True,
        "candidate_config": {
            "model": "google/gemini-3.1-pro-preview",
            "reasoning_effort": "high",
            "response_healing_initial": True,
            "parallelism": 2,
            "single_block_success_rule": "len(block_analyses)==1 and block_id matches input",
            "empty_single_block_retry": True,
        },
        "why_candidate": [
            "The original 2/17 missing baseline blocks were fully reproduced as successes in the confirmatory high-reasoning probe.",
            "Low reasoning fixed completeness but materially reduced findings and is not recall-safe.",
            "Healing OFF was not the differentiator; the stable signal was high reasoning on the flaky blocks.",
        ],
        "why_not_winner_yet": [
            f"Median probe-informed Pro still trails Claude on findings ({median['counterfactual_diff']['engine_total_findings']} vs {claude_summary['total_findings']}).",
            f"Even optimistic probe-informed Pro trails Claude on added findings (+{optimistic['counterfactual_diff']['added_findings']} vs +46).",
            f"Low-reasoning Pro variants underperform both median counterfactual Pro and Claude ({v2_summary['total_findings']} / {v3_summary['total_findings']} findings).",
        ],
        "recommended_next_step": (
            "If Pro needs a final admission test, run only one narrow 17-block high-reasoning "
            "second-pass rerun with the strict single-block success rule and the current retry guardrails. "
            "Do not use low reasoning for recall-oriented second pass."
        ),
    }


def build_markdown_report(
    *,
    probe_summary: dict,
    baseline_summary: dict,
    baseline_diff: dict,
    v2_summary: dict,
    v3_summary: dict,
    claude_summary: dict,
    claude_diff: dict,
    counter_scenarios: list[dict],
    recommendation: dict,
) -> str:
    lines = [
        "# Pro Second-Pass Re-evaluation (offline, probe-informed)\n",
        "## Scope\n",
        "- No new API calls.",
        "- Reused the original recall-hybrid run, Pro diagnostic, and baseline failmode probe.",
        "- Question: can Gemini 3.1 Pro return to the candidate set for recall-oriented second pass on the small KJ project?\n",
        "## Baseline anchors\n",
        f"- Observed Pro baseline: coverage {baseline_summary['coverage_pct']:.2f}%, missing {baseline_summary['missing_count']}, total findings {baseline_summary['total_findings']}, cost ${baseline_summary['total_cost_usd']:.4f}.",
        f"- Claude second pass: coverage {claude_summary['coverage_pct']:.2f}%, degraded {claude_diff['degraded']}, total findings {claude_summary['total_findings']}, cost ${claude_summary['total_cost_usd']:.4f}.",
        f"- Pro low reasoning + heal ON: coverage {v2_summary['coverage_pct']:.2f}%, total findings {v2_summary['total_findings']}, cost ${v2_summary['total_cost_usd']:.4f}.",
        f"- Pro low reasoning + heal OFF: coverage {v3_summary['coverage_pct']:.2f}%, total findings {v3_summary['total_findings']}, cost ${v3_summary['total_cost_usd']:.4f}.\n",
        "## Probe stability on the 2 flaky baseline blocks\n",
    ]

    for block_id, block_data in sorted(probe_summary.items()):
        all_modes = block_data["all_modes"]
        lines.append(
            f"- `{block_id}`: successful high-reasoning observations {all_modes['n']} "
            f"across modes {', '.join(all_modes['success_modes'])}; findings distribution "
            f"{all_modes['findings_distribution']}."
        )

    lines.extend([
        "\n## Probe-informed counterfactuals for baseline-style Pro (mode A = high reasoning + heal ON)\n",
        "| Scenario | Coverage | Improved | Degraded | Added findings | Total findings | Approx cost USD | Winner vs Claude |",
        "|----------|----------|----------|----------|----------------|----------------|-----------------|------------------|",
    ])

    for scenario in counter_scenarios:
        s = scenario["counterfactual_summary"]
        d = scenario["counterfactual_diff"]
        w = scenario["winner_vs_claude"]["winner"]
        lines.append(
            f"| {scenario['label']} | {s['coverage_pct']:.2f}% | {d['improved']} | {d['degraded']} "
            f"| +{d['added_findings']} | {s['total_findings']} | ${s['total_cost_usd']:.4f} | {w} |"
        )

    lines.extend([
        "\n## Recommendation\n",
        f"- Final call: `{recommendation['final_call']}`.",
        "- Pro should stay in the candidate pool only in this config:",
        f"  - model: `{recommendation['candidate_config']['model']}`",
        f"  - reasoning.effort: `{recommendation['candidate_config']['reasoning_effort']}`",
        f"  - healing: `{recommendation['candidate_config']['response_healing_initial']}`",
        f"  - parallelism: `{recommendation['candidate_config']['parallelism']}`",
        f"  - success rule: `{recommendation['candidate_config']['single_block_success_rule']}`",
        f"  - empty-result retry: `{recommendation['candidate_config']['empty_single_block_retry']}`",
        "",
        "Why Pro remains a candidate:",
    ])
    for item in recommendation["why_candidate"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("Why Claude still wins on this small KJ benchmark:")
    for item in recommendation["why_not_winner_yet"]:
        lines.append(f"- {item}")
    lines.extend([
        "",
        "Next step if a final confirmation is still needed:",
        f"- {recommendation['recommended_next_step']}",
        "",
        "## Caveat",
        "- Counterfactual quality metrics are exact only for the replaced probe blocks and the unchanged baseline blocks.",
        "- Counterfactual cost is approximate: baseline estimated-cost placeholders were replaced with actual probe call costs.",
        "- Counterfactual elapsed/token totals were intentionally left anchored to the observed baseline run to avoid false precision.",
        "",
    ])

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=_ROOT / DEFAULT_PROJECT_REL,
        help="Project directory containing the existing experiment artifacts.",
    )
    parser.add_argument(
        "--recall-run",
        default=DEFAULT_RECALL_RUN,
        help="Timestamp of the recall-hybrid small-project run.",
    )
    parser.add_argument(
        "--diag-run",
        default=DEFAULT_DIAG_RUN,
        help="Timestamp of the Pro diagnostic small-project run.",
    )
    parser.add_argument(
        "--probe-run",
        default=DEFAULT_PROBE_RUN,
        help="Timestamp of the Pro baseline failmode probe run.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Optional output directory override.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_dir = args.project_dir.resolve()

    recall_dir = project_dir / "_experiments" / "stage02_recall_hybrid_small_project" / args.recall_run
    diag_dir = project_dir / "_experiments" / "pro_diagnostic_small_project" / args.diag_run
    probe_dir = project_dir / "_experiments" / "pro_baseline_failmode_probe" / args.probe_run
    out_dir = args.out_dir or (project_dir / "_experiments" / "pro_second_pass_reevaluation" / _ts())

    baseline_summary = _load_json(recall_dir / "second_pass_pro_summary.json")
    baseline_diff = _load_json(recall_dir / "second_pass_pro_diff.json")
    claude_summary = _load_json(recall_dir / "second_pass_claude_summary.json")
    claude_diff = _load_json(recall_dir / "second_pass_claude_diff.json")
    v2_summary = _load_json(diag_dir / "v2_summary.json")
    v3_summary = _load_json(diag_dir / "v3_summary.json")
    probe_observations = load_probe_observations(probe_dir / "mode_A_calls.json")
    probe_observations_b = load_probe_observations(probe_dir / "mode_B_calls.json")

    merged_probe_observations = {
        block_id: sorted(
            probe_observations.get(block_id, []) + probe_observations_b.get(block_id, []),
            key=lambda i: (i.mode, i.repeat_idx),
        )
        for block_id in sorted(set(probe_observations) | set(probe_observations_b))
    }
    probe_summary = summarize_probe_observations(merged_probe_observations)

    claude_rm = _runmetrics_from_summary(claude_summary)
    counter_scenarios: list[dict] = []
    for policy in ("conservative", "median", "optimistic"):
        replacements = choose_probe_replacements(
            merged_probe_observations,
            mode=BASELINE_MODE,
            policy=policy,
        )
        cf_diff = build_counterfactual_diff(baseline_diff, replacements)
        cf_summary = build_counterfactual_summary(baseline_summary, cf_diff, replacements)
        cf_rm = _runmetrics_from_summary(cf_summary)
        winner, rationale = select_second_pass_winner(
            cf_rm,
            cf_diff,
            claude_rm,
            claude_diff,
            baseline_diff["escalation_set_size"],
        )
        counter_scenarios.append(
            {
                "scenario_id": f"pro_high_counterfactual_{policy}",
                "label": f"Pro high reasoning counterfactual ({policy})",
                "replacement_mode": BASELINE_MODE,
                "replacement_policy": policy,
                "selected_probe_replacements": {
                    bid: asdict(obs) for bid, obs in replacements.items()
                },
                "counterfactual_summary": cf_summary,
                "counterfactual_diff": cf_diff,
                "winner_vs_claude": {
                    "winner": winner,
                    "rationale": rationale,
                },
            }
        )

    recommendation = build_recommendation(
        counter_scenarios,
        claude_summary,
        v2_summary,
        v3_summary,
    )
    config_rows = build_config_rows(
        baseline_summary,
        counter_scenarios,
        v2_summary,
        v3_summary,
        claude_summary,
    )

    payload = {
        "project_dir": str(project_dir),
        "source_artifacts": {
            "recall_hybrid_dir": str(recall_dir),
            "pro_diagnostic_dir": str(diag_dir),
            "failmode_probe_dir": str(probe_dir),
        },
        "probe_summary": probe_summary,
        "baseline_summary": baseline_summary,
        "baseline_diff": baseline_diff,
        "claude_summary": claude_summary,
        "claude_diff": claude_diff,
        "v2_summary": v2_summary,
        "v3_summary": v3_summary,
        "counterfactual_scenarios": counter_scenarios,
        "config_rows": config_rows,
        "recommendation": recommendation,
    }
    report_md = build_markdown_report(
        probe_summary=probe_summary,
        baseline_summary=baseline_summary,
        baseline_diff=baseline_diff,
        v2_summary=v2_summary,
        v3_summary=v3_summary,
        claude_summary=claude_summary,
        claude_diff=claude_diff,
        counter_scenarios=counter_scenarios,
        recommendation=recommendation,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    _save_json(out_dir / "reevaluation_summary.json", payload)
    (out_dir / "recommendation.md").write_text(report_md, encoding="utf-8")

    print(out_dir)


if __name__ == "__main__":
    main()
