#!/usr/bin/env python3
"""Compare two classic audit findings JSON files.

The script is intentionally provider-agnostic: it does not call any external
service. It compares an existing baseline 03_findings.json with another
03_findings.json produced elsewhere and writes a compact JSON report.
"""
from __future__ import annotations

import argparse
import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")
NUMBER_RE = re.compile(r"\d+(?:[,.]\d+)?")
MEASURE_NUMBER_RE = re.compile(
    r"(\d+(?:[,.]\d+)?)(?=[^\n\r]{0,12}\b(?:мм|м|к[аa]|[аa]|ом|in|квт|kw|а[·* ]?ч|ah)\b)",
    re.IGNORECASE,
)
TECH_REF_RE = re.compile(
    r"\b(?:"
    r"qf\d+(?:\.\d+)*(?:[a-zа-я]+)?"
    r"|rs[- ]?485"
    r"|cat\s*5e"
    r"|ei(?:s)?"
    r"|rei"
    r"|tehstrong"
    r"|firewall"
    r"|l\d+x\d+x\d+"
    r"|лжд[- ]?\d+"
    r"|лдж[- ]?\d+"
    r"|бср\s*м\d+x\d+"
    r")\b",
    re.IGNORECASE,
)
NORM_REF_RE = re.compile(
    r"\b(?:сп|снип|пуэ|фз|гост(?:\s+р)?|iso|iec)\s*"
    r"(?:[a-zа-яё]*\s*)?"
    r"\d+(?:[.\-]\d+)*(?:[-–]\d+)?",
    re.IGNORECASE,
)
TECH_SHORT_TOKENS = {
    "сп",
    "км",
    "кж",
    "ар",
    "рд",
    "кз",
    "ei",
    "qf",
    "rs",
    "pe",
}


def _load_items(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("findings") or data.get("items") or data.get("remarks") or []
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]


def _item_text(item: dict[str, Any]) -> str:
    parts = [
        item.get("problem"),
        item.get("description"),
        item.get("finding"),
        item.get("norm"),
        item.get("solution"),
        item.get("risk"),
        item.get("category"),
    ]
    return " ".join(str(part) for part in parts if part)


def _tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for word in WORD_RE.findall(text or ""):
        normalized = word.lower()
        if len(normalized) > 2 or normalized in TECH_SHORT_TOKENS:
            tokens.add(normalized)
    return tokens


def _numbers(text: str) -> set[str]:
    values: set[str] = set()
    for raw in NUMBER_RE.findall(text or ""):
        normalized = raw.replace(",", ".")
        has_decimal = "." in normalized
        try:
            value = float(normalized)
        except ValueError:
            continue
        if not has_decimal and value < 10:
            continue
        values.add(f"{value:g}")
    return values


def _measure_numbers(text: str) -> set[str]:
    values: set[str] = set()
    for raw in MEASURE_NUMBER_RE.findall(text or ""):
        normalized = raw.replace(",", ".")
        try:
            value = float(normalized)
        except ValueError:
            continue
        values.add(f"{value:g}")
    return values


def _norm_refs(text: str) -> set[str]:
    refs: set[str] = set()
    for raw in NORM_REF_RE.findall(text or ""):
        normalized = re.sub(r"\s+", " ", raw.lower().replace("–", "-")).strip()
        normalized = normalized.replace(",", ".")
        refs.add(normalized)
    return refs


def _tech_refs(text: str) -> set[str]:
    refs: set[str] = set()
    for raw in TECH_REF_RE.findall(text or ""):
        normalized = raw.lower().replace(" ", "").replace("х", "x")
        refs.add(normalized)
    return refs


def _can_reuse_candidate_for_grouped_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """Allow one broad candidate finding to cover several baseline findings.

    This is intentionally narrow: it only applies when the baseline has concrete
    equipment/item identifiers and every such identifier is present in a broader
    candidate finding with multiple identifiers. It catches cases like one Codex
    finding grouping QF4.11/QF4.12/QF5.1.1 while Claude split them.
    """
    left_refs = _tech_refs(_item_text(left))
    right_refs = _tech_refs(_item_text(right))
    return bool(left_refs and len(right_refs) >= 2 and left_refs.issubset(right_refs))


def _similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_text = _item_text(left)
    right_text = _item_text(right)
    left_tokens = _tokens(left_text)
    right_tokens = _tokens(right_text)
    jaccard = len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))
    sequence = SequenceMatcher(None, left_text[:1200], right_text[:1200]).ratio()
    category_bonus = 0.08 if left.get("category") and left.get("category") == right.get("category") else 0.0
    left_numbers = _numbers(left_text)
    right_numbers = _numbers(right_text)
    shared_numbers = left_numbers & right_numbers
    left_measure_numbers = _measure_numbers(left_text)
    right_measure_numbers = _measure_numbers(right_text)
    shared_measure_numbers = left_measure_numbers & right_measure_numbers
    numeric_bonus = min(0.30, 0.02 * len(shared_numbers)) if len(shared_numbers) >= 2 else 0.0
    shared_tech_refs = _tech_refs(left_text) & _tech_refs(right_text)
    tech_bonus = min(0.20, 0.14 * len(shared_tech_refs))
    shared_norm_refs = _norm_refs(left_text) & _norm_refs(right_text)
    norm_bonus = 0.0
    has_unmatched_measurement = bool(
        len(left_measure_numbers) >= 2
        and not shared_measure_numbers
        and not shared_tech_refs
    )
    if (
        shared_norm_refs
        and not has_unmatched_measurement
        and (jaccard >= 0.10 or len(left_tokens & right_tokens) >= 12)
    ):
        norm_bonus = min(0.24, 0.12 * len(shared_norm_refs))
    numeric_miss_penalty = 0.0
    if len(left_numbers) >= 2 and not shared_numbers and not shared_tech_refs:
        numeric_miss_penalty = 0.12
    if has_unmatched_measurement:
        numeric_miss_penalty = max(numeric_miss_penalty, 0.18)
    return round(
        (0.65 * jaccard)
        + (0.35 * sequence)
        + category_bonus
        + numeric_bonus
        + tech_bonus
        + norm_bonus
        - numeric_miss_penalty,
        3,
    )


def _brief(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "severity": item.get("severity"),
        "category": item.get("category"),
        "problem": (item.get("problem") or item.get("finding") or item.get("description") or "")[:320],
    }


def compare(
    baseline_path: Path,
    candidate_path: Path,
    *,
    threshold: float,
) -> dict[str, Any]:
    baseline = _load_items(baseline_path)
    candidate = _load_items(candidate_path)

    matches: list[dict[str, Any]] = []
    used_candidate: set[int] = set()

    for baseline_idx, baseline_item in enumerate(baseline):
        best: tuple[float, int] | None = None
        for candidate_idx, candidate_item in enumerate(candidate):
            if candidate_idx in used_candidate and not _can_reuse_candidate_for_grouped_match(
                baseline_item,
                candidate_item,
            ):
                continue
            score = _similarity(baseline_item, candidate_item)
            if best is None or score > best[0]:
                best = (score, candidate_idx)
        if best and best[0] >= threshold:
            score, candidate_idx = best
            used_candidate.add(candidate_idx)
            matches.append(
                {
                    "baseline_idx": baseline_idx,
                    "candidate_idx": candidate_idx,
                    "similarity": score,
                    "candidate_reused": sum(1 for match in matches if match["candidate_idx"] == candidate_idx) > 0,
                    "baseline": _brief(baseline[baseline_idx]),
                    "candidate": _brief(candidate[candidate_idx]),
                }
            )

    unmatched_baseline = [
        _brief(item)
        for idx, item in enumerate(baseline)
        if all(match["baseline_idx"] != idx for match in matches)
    ]
    unmatched_candidate = [
        _brief(item)
        for idx, item in enumerate(candidate)
        if idx not in used_candidate
    ]

    severity_matches = sum(
        1
        for match in matches
        if baseline[match["baseline_idx"]].get("severity")
        == candidate[match["candidate_idx"]].get("severity")
    )
    category_matches = sum(
        1
        for match in matches
        if baseline[match["baseline_idx"]].get("category")
        == candidate[match["candidate_idx"]].get("category")
    )

    return {
        "status": "done",
        "baseline_path": str(baseline_path),
        "candidate_path": str(candidate_path),
        "threshold": threshold,
        "baseline_findings": len(baseline),
        "candidate_findings": len(candidate),
        "matched": len(matches),
        "unique_candidate_matches": len(used_candidate),
        "candidate_reused_matches": sum(1 for match in matches if match.get("candidate_reused")),
        "candidate_recall_vs_baseline": round(len(matches) / len(baseline), 3) if baseline else None,
        "candidate_precision_vs_baseline": round(len(used_candidate) / len(candidate), 3) if candidate else None,
        "severity_match_rate_on_matched": round(severity_matches / len(matches), 3) if matches else None,
        "category_match_rate_on_matched": round(category_matches / len(matches), 3) if matches else None,
        "matches": matches,
        "unmatched_baseline": unmatched_baseline,
        "unmatched_candidate": unmatched_candidate,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="Baseline 03_findings.json, for example Claude output")
    parser.add_argument("candidate", type=Path, help="Candidate 03_findings.json, for example Codex output")
    parser.add_argument("--out", type=Path, help="Where to write comparison_report.json")
    parser.add_argument("--threshold", type=float, default=0.38, help="Similarity threshold for a match")
    args = parser.parse_args()

    report = compare(args.baseline, args.candidate, threshold=args.threshold)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_keys = [
        "status",
        "baseline_findings",
        "candidate_findings",
        "matched",
        "candidate_recall_vs_baseline",
        "candidate_precision_vs_baseline",
        "severity_match_rate_on_matched",
        "category_match_rate_on_matched",
    ]
    print(json.dumps({key: report[key] for key in summary_keys}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
