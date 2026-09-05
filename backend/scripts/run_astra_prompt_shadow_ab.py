#!/usr/bin/env python3
"""Run a paired Astra prompt A/B on blocks linked to expert decisions.

The model never receives the expert decisions. They are joined only after both
prompt variants finish and are used as known-positive / known-negative replay
labels. Unmatched new model findings remain explicitly unreviewed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2,
    SYSTEM_PROMPT_PROFILE_PRODUCTION,
    SYSTEM_PROMPT_PROFILES,
)
from backend.app.services.llm.codex_runner import find_codex_cli
from backend.scripts.run_stage02_codex_block_ab import (
    DISCIPLINE_ORDER,
    DOCUMENT_RETRIEVAL_PROFILE_NONE,
    DOCUMENT_RETRIEVAL_PROFILES,
    BlockCandidate,
    collect_candidates,
    greedy_match,
    run_one,
    utc_stamp,
    write_json,
)


OUT_ROOT = Path(tempfile.gettempdir()) / "astra_prompt_shadow"
KNOWN_DECISIONS = frozenset({"accepted", "rejected"})


@dataclass(frozen=True)
class ReviewedBlock:
    candidate: BlockCandidate
    references: tuple[dict[str, Any], ...]

    @property
    def has_accepted(self) -> bool:
        return any(item["decision"] == "accepted" for item in self.references)

    @property
    def label_class(self) -> str:
        return "has_accepted" if self.has_accepted else "rejected_only"


def _comparison_finding(finding: dict[str, Any]) -> dict[str, Any]:
    problem = str(finding.get("problem") or finding.get("finding") or "").strip()
    description = str(finding.get("description") or "").strip()
    combined = "\n".join(part for part in (problem, description) if part)
    return {
        "severity": str(finding.get("severity") or ""),
        "category": str(finding.get("category") or ""),
        "finding": combined,
        "norm_quote": finding.get("norm_quote"),
        "value_found": str(finding.get("value_found") or ""),
        "recommendation": str(
            finding.get("solution") or finding.get("recommendation") or ""
        ),
    }


def collect_reviewed_blocks(
    candidates: Iterable[BlockCandidate],
) -> list[ReviewedBlock]:
    """Join current block artifacts to same-version expert decisions.

    Only ``source_block_ids`` are accepted. ``related_block_ids`` can point to a
    useful cross-reference that does not itself contain enough evidence to
    reproduce the reviewed finding.
    """
    version_cache: dict[str, tuple[dict[str, dict[str, Any]], list[dict[str, Any]]] | None] = {}
    reviewed: list[ReviewedBlock] = []
    for candidate in candidates:
        version_key = str(candidate.version_dir)
        if version_key not in version_cache:
            review_path = candidate.version_dir / "04_review" / "expert_review.json"
            findings_path = candidate.latest_dir / "03_findings.json"
            if not review_path.is_file() or not findings_path.is_file():
                version_cache[version_key] = None
            else:
                try:
                    review_data = json.loads(review_path.read_text(encoding="utf-8"))
                    findings_data = json.loads(findings_path.read_text(encoding="utf-8"))
                    decisions = {
                        str(item.get("item_id")): item
                        for item in review_data.get("decisions", [])
                        if isinstance(item, dict)
                        and item.get("decision") in KNOWN_DECISIONS
                    }
                    findings = [
                        item
                        for item in findings_data.get("findings", [])
                        if isinstance(item, dict)
                    ]
                    version_cache[version_key] = decisions, findings
                except (OSError, json.JSONDecodeError, TypeError):
                    version_cache[version_key] = None
        joined = version_cache[version_key]
        if joined is None:
            continue
        decisions, findings = joined
        references: list[dict[str, Any]] = []
        for finding in findings:
            finding_id = str(finding.get("id") or "")
            decision = decisions.get(finding_id)
            if decision is None:
                continue
            source_ids = {str(item) for item in finding.get("source_block_ids") or []}
            if candidate.block_id not in source_ids:
                continue
            references.append(
                {
                    "reference_key": f"{version_key}::{finding_id}",
                    "finding_id": finding_id,
                    "decision": str(decision["decision"]),
                    "problem": str(
                        finding.get("problem") or finding.get("finding") or ""
                    ),
                    "description": str(finding.get("description") or ""),
                    "rejection_reason": str(
                        decision.get("rejection_reason")
                        or decision.get("reason")
                        or ""
                    ),
                    "comparison_finding": _comparison_finding(finding),
                }
            )
        if references:
            reviewed.append(ReviewedBlock(candidate, tuple(references)))
    return reviewed


def _reference_keys(item: ReviewedBlock, decision: str) -> set[str]:
    return {
        str(ref["reference_key"])
        for ref in item.references
        if ref["decision"] == decision
    }


def _select_diverse(
    pool: list[ReviewedBlock],
    *,
    limit: int,
    focus_decision: str,
) -> list[ReviewedBlock]:
    remaining = list(pool)
    selected: list[ReviewedBlock] = []
    used_references: set[str] = set()
    discipline_counts: Counter[str] = Counter()
    document_counts: Counter[tuple[str, str, str]] = Counter()
    order = {name: index for index, name in enumerate(DISCIPLINE_ORDER)}
    while remaining and len(selected) < limit:
        def rank(item: ReviewedBlock) -> tuple[Any, ...]:
            candidate = item.candidate
            document_key = (
                candidate.discipline,
                candidate.document,
                candidate.version,
            )
            new_refs = _reference_keys(item, focus_decision) - used_references
            return (
                discipline_counts[candidate.discipline],
                document_counts[document_key],
                0 if new_refs else 1,
                -len(new_refs),
                len(item.references),
                order.get(candidate.discipline, len(order)),
                candidate.document,
                candidate.version,
                candidate.page,
                candidate.block_id,
            )

        chosen = min(remaining, key=rank)
        remaining.remove(chosen)
        selected.append(chosen)
        discipline_counts[chosen.candidate.discipline] += 1
        document_counts[
            (
                chosen.candidate.discipline,
                chosen.candidate.document,
                chosen.candidate.version,
            )
        ] += 1
        used_references.update(_reference_keys(chosen, focus_decision))
    return selected


def select_reviewed_balanced(
    reviewed: list[ReviewedBlock],
    *,
    limit: int,
) -> list[ReviewedBlock]:
    if limit < 2:
        raise ValueError("limit must be at least 2")
    positive_pool = [item for item in reviewed if item.has_accepted]
    negative_pool = [item for item in reviewed if not item.has_accepted]
    positive_limit = min((limit + 1) // 2, len(positive_pool))
    negative_limit = min(limit - positive_limit, len(negative_pool))
    if positive_limit + negative_limit < limit:
        positive_limit = min(limit - negative_limit, len(positive_pool))
    selected_positive = _select_diverse(
        positive_pool,
        limit=positive_limit,
        focus_decision="accepted",
    )
    selected_negative = _select_diverse(
        negative_pool,
        limit=negative_limit,
        focus_decision="rejected",
    )
    selected: list[ReviewedBlock] = []
    for index in range(max(len(selected_positive), len(selected_negative))):
        if index < len(selected_positive):
            selected.append(selected_positive[index])
        if index < len(selected_negative):
            selected.append(selected_negative[index])
    return selected[:limit]


def _expert_eval(
    result: dict[str, Any],
    references: tuple[dict[str, Any], ...],
    *,
    threshold: float,
) -> dict[str, Any]:
    comparison_references = [item["comparison_finding"] for item in references]
    model_findings = [
        item for item in result.get("codex_findings") or [] if isinstance(item, dict)
    ]
    matches, missed, extra = greedy_match(
        comparison_references,
        model_findings,
        threshold=threshold,
    )
    matched_references: list[dict[str, Any]] = []
    for match in matches:
        reference = references[int(match["gpt_index"])]
        matched_references.append(
            {
                "reference_key": reference["reference_key"],
                "finding_id": reference["finding_id"],
                "decision": reference["decision"],
                "score": match["score"],
                "problem": reference["problem"],
                "model_finding": match["codex"],
            }
        )
    missed_references = [
        {
            "reference_key": references[int(item["gpt_index"])]["reference_key"],
            "finding_id": references[int(item["gpt_index"])]["finding_id"],
            "decision": references[int(item["gpt_index"])]["decision"],
            "problem": references[int(item["gpt_index"])]["problem"],
        }
        for item in missed
    ]
    return {
        "threshold": threshold,
        "accepted_references": sum(
            item["decision"] == "accepted" for item in references
        ),
        "rejected_references": sum(
            item["decision"] == "rejected" for item in references
        ),
        "matched_accepted": sum(
            item["decision"] == "accepted" for item in matched_references
        ),
        "matched_rejected": sum(
            item["decision"] == "rejected" for item in matched_references
        ),
        "matched_reference_keys": [
            item["reference_key"] for item in matched_references
        ],
        "matched_references": matched_references,
        "missed_references": missed_references,
        "unreviewed_model_findings": [item["codex"] for item in extra],
        "unreviewed_model_findings_count": len(extra),
    }


def _paired_diff(
    left_result: dict[str, Any],
    right_result: dict[str, Any],
    *,
    threshold: float,
) -> dict[str, Any]:
    left_findings = left_result.get("codex_findings") or []
    right_findings = right_result.get("codex_findings") or []
    matches, removed, added = greedy_match(
        left_findings,
        right_findings,
        threshold=threshold,
    )
    return {
        "threshold": threshold,
        "semantic_matches": len(matches),
        "removed_from_left": [item["gpt"] for item in removed],
        "added_by_right": [item["codex"] for item in added],
        "removed_count": len(removed),
        "added_count": len(added),
    }


def _percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def _variant_summary(records: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    results = [item[variant]["result"] for item in records]
    evals = [item[variant]["expert_eval"] for item in records]
    ok = [result for result in results if result.get("ok")]
    accepted = sum(item["accepted_references"] for item in evals)
    rejected = sum(item["rejected_references"] for item in evals)
    matched_accepted = sum(item["matched_accepted"] for item in evals)
    matched_rejected = sum(item["matched_rejected"] for item in evals)
    durations = [int(item.get("duration_ms") or 0) for item in ok]
    input_tokens = sum(int(item.get("input_tokens") or 0) for item in ok)
    cached_tokens = sum(int(item.get("cached_input_tokens") or 0) for item in ok)
    output_tokens = sum(int(item.get("output_tokens") or 0) for item in ok)
    reasoning_tokens = sum(int(item.get("reasoning_tokens") or 0) for item in ok)
    return {
        "blocks_ok": len(ok),
        "blocks_failed": len(results) - len(ok),
        "blocks_nonempty": sum(int(item.get("codex_findings_count") or 0) > 0 for item in ok),
        "model_findings": sum(int(item.get("codex_findings_count") or 0) for item in ok),
        "known_accepted_references": accepted,
        "known_accepted_matched": matched_accepted,
        "known_accepted_match_rate": round(matched_accepted / accepted, 4) if accepted else None,
        "known_rejected_references": rejected,
        "known_rejected_matched": matched_rejected,
        "known_rejected_resurrection_rate": round(matched_rejected / rejected, 4) if rejected else None,
        "unreviewed_model_findings": sum(
            item["unreviewed_model_findings_count"] for item in evals
        ),
        "input_tokens": input_tokens,
        "cached_input_tokens": cached_tokens,
        "uncached_input_tokens": input_tokens - cached_tokens,
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning_tokens,
        "total_tokens": input_tokens + output_tokens,
        "total_tokens_avg_per_ok_block": round(
            (input_tokens + output_tokens) / len(ok), 1
        ) if ok else None,
        "duration_ms_sum": sum(durations),
        "duration_ms_avg": round(sum(durations) / len(durations), 1) if durations else None,
        "duration_ms_p95": _percentile(durations, 0.95),
    }


def summarize(
    records: list[dict[str, Any]],
    *,
    wall_clock_ms: int,
    left_prompt_profile: str,
    right_prompt_profile: str,
    left_retrieval_profile: str,
    right_retrieval_profile: str,
) -> dict[str, Any]:
    left = _variant_summary(records, "left")
    right = _variant_summary(records, "right")
    return {
        "blocks": len(records),
        "wall_clock_ms": wall_clock_ms,
        "corpus": {
            "has_accepted_blocks": sum(item["label_class"] == "has_accepted" for item in records),
            "rejected_only_blocks": sum(item["label_class"] == "rejected_only" for item in records),
            "by_discipline": dict(sorted(Counter(item["discipline"] for item in records).items())),
        },
        "variants": {
            "left": {
                "prompt_profile": left_prompt_profile,
                "document_retrieval_profile": left_retrieval_profile,
            },
            "right": {
                "prompt_profile": right_prompt_profile,
                "document_retrieval_profile": right_retrieval_profile,
            },
        },
        "left": left,
        "right": right,
        "delta_right_minus_left": {
            key: (right[key] - left[key])
            for key in (
                "blocks_nonempty",
                "model_findings",
                "known_accepted_matched",
                "known_rejected_matched",
                "unreviewed_model_findings",
                "input_tokens",
                "cached_input_tokens",
                "uncached_input_tokens",
                "output_tokens",
                "reasoning_tokens",
                "total_tokens",
                "duration_ms_sum",
            )
        },
        "paired": {
            "blocks_with_right_additions": sum(item["diff"]["added_count"] > 0 for item in records),
            "blocks_with_right_removals": sum(item["diff"]["removed_count"] > 0 for item in records),
            "right_added_findings": sum(item["diff"]["added_count"] for item in records),
            "right_removed_findings": sum(item["diff"]["removed_count"] for item in records),
            "blocks_gaining_known_accepted": sum(
                item["right"]["expert_eval"]["matched_accepted"]
                > item["left"]["expert_eval"]["matched_accepted"]
                for item in records
            ),
            "blocks_losing_known_accepted": sum(
                item["right"]["expert_eval"]["matched_accepted"]
                < item["left"]["expert_eval"]["matched_accepted"]
                for item in records
            ),
            "blocks_adding_known_rejected": sum(
                item["right"]["expert_eval"]["matched_rejected"]
                > item["left"]["expert_eval"]["matched_rejected"]
                for item in records
            ),
        },
        "interpretation_limit": (
            "Known accepted/rejected rates cover previously reviewed findings only. "
            "Unmatched new findings are not automatically false positives and require review."
        ),
    }


def _compact_reference(reference: dict[str, Any]) -> dict[str, Any]:
    return {
        "reference_key": reference["reference_key"],
        "finding_id": reference["finding_id"],
        "decision": reference["decision"],
        "problem": reference["problem"],
        "description": reference["description"],
        "rejection_reason": reference["rejection_reason"][:3000],
    }


def write_report(run_dir: Path, summary: dict[str, Any], records: list[dict[str, Any]]) -> None:
    left = summary["left"]
    right = summary["right"]
    left_meta = summary["variants"]["left"]
    right_meta = summary["variants"]["right"]
    left_label = (
        f"{left_meta['prompt_profile']} + "
        f"{left_meta['document_retrieval_profile']}"
    )
    right_label = (
        f"{right_meta['prompt_profile']} + "
        f"{right_meta['document_retrieval_profile']}"
    )
    lines = [
        "# Astra prompt shadow A/B",
        "",
        f"- Blocks: {summary['blocks']} ({summary['corpus']['has_accepted_blocks']} with accepted; "
        f"{summary['corpus']['rejected_only_blocks']} rejected-only)",
        f"- Wall clock: {summary['wall_clock_ms'] / 1000:.1f} s",
        "",
        f"| Metric | left: {left_label} | right: {right_label} |",
        "| --- | ---: | ---: |",
        f"| Findings | {left['model_findings']} | {right['model_findings']} |",
        f"| Non-empty blocks | {left['blocks_nonempty']} | {right['blocks_nonempty']} |",
        f"| Known accepted matched | {left['known_accepted_matched']}/{left['known_accepted_references']} | "
        f"{right['known_accepted_matched']}/{right['known_accepted_references']} |",
        f"| Known rejected resurrected | {left['known_rejected_matched']}/{left['known_rejected_references']} | "
        f"{right['known_rejected_matched']}/{right['known_rejected_references']} |",
        f"| Unreviewed new candidates | {left['unreviewed_model_findings']} | {right['unreviewed_model_findings']} |",
        f"| Input tokens | {left['input_tokens']} | {right['input_tokens']} |",
        f"| Cached input tokens | {left['cached_input_tokens']} | {right['cached_input_tokens']} |",
        f"| Output tokens | {left['output_tokens']} | {right['output_tokens']} |",
        f"| Reasoning tokens | {left['reasoning_tokens']} | {right['reasoning_tokens']} |",
        f"| Total tokens | {left['total_tokens']} | {right['total_tokens']} |",
        f"| Avg duration/block | {left['duration_ms_avg']} ms | {right['duration_ms_avg']} ms |",
        "",
        "Known accepted/rejected metrics replay previously reviewed findings. Unmatched new "
        "candidates need manual review before they can be counted as correct or false.",
        "",
        "## Paired differences",
        "",
    ]
    for item in records:
        if not item["diff"]["added_count"] and not item["diff"]["removed_count"]:
            continue
        lines.extend(
            [
                f"### {item['discipline']} / {item['document']} / {item['block_id']}",
                "",
                f"- Expert class: {item['label_class']}",
                f"- left/right findings: {item['left']['result']['codex_findings_count']} / "
                f"{item['right']['result']['codex_findings_count']}",
            ]
        )
        for finding in item["diff"]["added_by_right"]:
            lines.append(f"- right added: {str(finding.get('finding') or '')[:500]}")
        for finding in item["diff"]["removed_from_left"]:
            lines.append(f"- right removed: {str(finding.get('finding') or '')[:500]}")
        lines.append("")
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


async def async_main(args: argparse.Namespace) -> int:
    candidates = collect_candidates()
    reviewed = collect_reviewed_blocks(candidates)
    selected = select_reviewed_balanced(reviewed, limit=args.limit)
    if len(selected) < args.limit:
        raise RuntimeError(
            f"Only {len(selected)} reviewed blocks available for requested limit {args.limit}"
        )
    run_dir = Path(args.run_dir) if args.run_dir else OUT_ROOT / utc_stamp()
    run_dir.mkdir(parents=True, exist_ok=True)
    selection = {
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "variants": {
            "left": {
                "prompt_profile": args.left_prompt_profile,
                "document_retrieval_profile": args.left_retrieval_profile,
            },
            "right": {
                "prompt_profile": args.right_prompt_profile,
                "document_retrieval_profile": args.right_retrieval_profile,
            },
        },
        "limit": args.limit,
        "available_candidates": len(candidates),
        "reviewed_candidates": len(reviewed),
        "match_threshold": args.match_threshold,
        "selected": [
            {
                "label": item.candidate.label,
                "label_class": item.label_class,
                "discipline": item.candidate.discipline,
                "document": item.candidate.document,
                "version": item.candidate.version,
                "block_id": item.candidate.block_id,
                "page": item.candidate.page,
                "references": [
                    _compact_reference(reference) for reference in item.references
                ],
            }
            for item in selected
        ],
    }
    write_json(run_dir / "selection.json", selection)
    print(
        f"[astra-prompt-shadow] run_dir={run_dir} selected={len(selected)} "
        f"reviewed_available={len(reviewed)}",
        flush=True,
    )
    if args.dry_run:
        print(json.dumps(selection, ensure_ascii=False, indent=2))
        return 0
    if not find_codex_cli():
        raise RuntimeError("Codex CLI not found")

    records: list[dict[str, Any]] = []
    started = time.monotonic()
    for index, item in enumerate(selected, start=1):
        candidate = item.candidate
        print(
            f"[astra-prompt-shadow] {index:02d}/{len(selected)} "
            f"{candidate.label} class={item.label_class}",
            flush=True,
        )
        common = {
            "candidate": candidate,
            "index": index,
            "model": args.model,
            "reasoning_effort": args.reasoning_effort,
            "timeout": args.timeout,
            "threshold": args.match_threshold,
            "profile": "baseline",
            "all_candidates": candidates,
            "style_examples_limit": 0,
            "resume": args.resume,
        }
        left_task = run_one(
            run_dir=run_dir / "left",
            system_prompt_profile=args.left_prompt_profile,
            document_retrieval_profile=args.left_retrieval_profile,
            **common,
        )
        right_task = run_one(
            run_dir=run_dir / "right",
            system_prompt_profile=args.right_prompt_profile,
            document_retrieval_profile=args.right_retrieval_profile,
            **common,
        )
        left_result, right_result = await asyncio.gather(left_task, right_task)
        left_eval = _expert_eval(
            left_result,
            item.references,
            threshold=args.match_threshold,
        )
        right_eval = _expert_eval(
            right_result,
            item.references,
            threshold=args.match_threshold,
        )
        record = {
            "label": candidate.label,
            "label_class": item.label_class,
            "discipline": candidate.discipline,
            "document": candidate.document,
            "version": candidate.version,
            "block_id": candidate.block_id,
            "page": candidate.page,
            "expert_references": [
                _compact_reference(reference) for reference in item.references
            ],
            "left": {"result": left_result, "expert_eval": left_eval},
            "right": {"result": right_result, "expert_eval": right_eval},
            "diff": _paired_diff(
                left_result,
                right_result,
                threshold=args.match_threshold,
            ),
        }
        records.append(record)
        write_json(run_dir / "results.partial.json", records)
        print(
            f"[astra-prompt-shadow]   left={left_result.get('codex_findings_count')} "
            f"right={right_result.get('codex_findings_count')} "
            f"accepted_hits={left_eval['matched_accepted']}/{right_eval['matched_accepted']} "
            f"rejected_hits={left_eval['matched_rejected']}/{right_eval['matched_rejected']}",
            flush=True,
        )

    wall_clock_ms = int((time.monotonic() - started) * 1000)
    summary = summarize(
        records,
        wall_clock_ms=wall_clock_ms,
        left_prompt_profile=args.left_prompt_profile,
        right_prompt_profile=args.right_prompt_profile,
        left_retrieval_profile=args.left_retrieval_profile,
        right_retrieval_profile=args.right_retrieval_profile,
    )
    write_json(run_dir / "results.json", records)
    write_json(run_dir / "summary.json", summary)
    write_report(run_dir, summary, records)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument(
        "--model",
        default=os.environ.get("AUDIT_CODEX_STAGE_MODEL", "codex/gpt-6-astra"),
    )
    parser.add_argument(
        "--reasoning-effort",
        choices=("low", "medium", "high", "xhigh", "max", "ultra"),
        default="low",
    )
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--match-threshold", type=float, default=0.30)
    parser.add_argument(
        "--left-prompt-profile",
        choices=tuple(sorted(SYSTEM_PROMPT_PROFILES)),
        default=SYSTEM_PROMPT_PROFILE_PRODUCTION,
    )
    parser.add_argument(
        "--right-prompt-profile",
        choices=tuple(sorted(SYSTEM_PROMPT_PROFILES)),
        default=SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2,
    )
    parser.add_argument(
        "--left-retrieval-profile",
        choices=tuple(sorted(DOCUMENT_RETRIEVAL_PROFILES)),
        default=DOCUMENT_RETRIEVAL_PROFILE_NONE,
    )
    parser.add_argument(
        "--right-retrieval-profile",
        choices=tuple(sorted(DOCUMENT_RETRIEVAL_PROFILES)),
        default=DOCUMENT_RETRIEVAL_PROFILE_NONE,
    )
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
