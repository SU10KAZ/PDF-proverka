"""Fuzzy (similarity-based) deduplication — production-ready, stdlib-only.

Use when findings lack `problem_class` tags (baseline prompts) or you need a
soft second pass after `class_dedup.collapse_to_canonical`.

Algorithm:
  - Build a signature for each finding:
      lowercase, punctuation-stripped concatenation of
        (category | problem | affected_system | evidence_quote[:120])
  - For each new finding, compare its signature against every kept
    finding's signature using `difflib.SequenceMatcher.ratio()`.
  - If best similarity >= sim_threshold (default 0.7) and best-match exists,
    treat as a duplicate of that finding. Replace the kept one if the new
    finding has a better canonical_score; otherwise drop the new one.

Production safety:
  - Never silently drops a КРИТИЧЕСКОЕ finding. If a new КРИТИЧЕСКОЕ has a
    fuzzy match against a non-critical kept finding, the new one is added
    as a separate cluster (never collapsed). If a new КРИТИЧЕСКОЕ matches
    another КРИТИЧЕСКОЕ, both are kept (each КРИТИЧЕСКОЕ stays as its own
    canonical).
  - Output count never exceeds input count.
  - Threshold of 0.7 was validated on the 8-case algorithm_research dataset
    (see reports/phase0_phase1_validation_report.md §1.3). On A0 production
    outputs Phase 0 fuzzy_dedup is a no-op. On multi-source merged outputs
    fuzzy_dedup at 0.7 reduces FP by ~18%.

This module is intentionally independent from `class_dedup.py` (no
cross-imports). It has its own copy of the small normalisation helpers and
severity table so the two modules can be vendored independently.

CLI:
    python fuzzy_dedup.py <input.json> [--out <out.json>] [--threshold 0.7]

Python 3.11+.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Constants & normalisation — DUPLICATED from class_dedup.py on purpose.
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")

SEVERITY_WEIGHT: dict[str, int] = {
    "КРИТИЧЕСКОЕ": 5,
    "ЭКОНОМИЧЕСКОЕ": 4,
    "ЭКСПЛУАТАЦИОННОЕ": 3,
    "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ": 2,
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 2,
    "РЕКОМЕНДАТЕЛЬНОЕ": 1,
}

DEFAULT_SIM_THRESHOLD = 0.7


def _normalise(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _severity_weight(sev: Any) -> int:
    return SEVERITY_WEIGHT.get(str(sev or "").strip(), 0)


def _is_critical(f: dict) -> bool:
    return _severity_weight(f.get("severity")) >= SEVERITY_WEIGHT["КРИТИЧЕСКОЕ"]


def _canonical_score(f: dict) -> tuple[int, float, int, int, int]:
    """Same ordering as in class_dedup.py — see dedup_thresholds.md."""
    desc_len = len(f.get("description") or f.get("finding") or "")
    ev_len = len(f.get("evidence_quote") or f.get("md_excerpt") or "")
    norm_filled = 1 if (f.get("norm") or "").strip() else 0
    sev_weight = _severity_weight(f.get("severity"))
    try:
        conf = float(f.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    return (sev_weight, conf, norm_filled, desc_len, ev_len)


def _signature(f: dict) -> str:
    parts = [
        f.get("category") or "",
        f.get("problem") or f.get("finding") or f.get("description") or "",
        f.get("affected_system") or "",
        (f.get("evidence_quote") or f.get("md_excerpt") or "")[:120],
    ]
    return _normalise(" | ".join(parts))


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b, autojunk=False).ratio()


# ---------------------------------------------------------------------------
# Public types.
# ---------------------------------------------------------------------------

@dataclass
class DedupReport:
    total_in: int = 0
    total_out: int = 0
    clusters: int = 0
    same_class_drops: int = 0
    same_class_drops_by_key: dict[str, int] = field(default_factory=dict)
    critical_collapsed_count: int = 0
    sim_threshold: float = DEFAULT_SIM_THRESHOLD
    methods_seen: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Core dedup.
# ---------------------------------------------------------------------------

def fuzzy_dedup(
    findings: list[dict],
    sim_threshold: float = DEFAULT_SIM_THRESHOLD,
) -> tuple[list[dict], DedupReport]:
    """Similarity-based dedup. See module docstring for semantics.

    Returns (kept_findings, report). Order of kept_findings is preserved
    in original input order (modulo replacements when a later finding wins
    the canonical_score contest).
    """
    if not (0.0 <= sim_threshold <= 1.0):
        raise ValueError(f"sim_threshold must be in [0,1]; got {sim_threshold!r}")

    report = DedupReport(total_in=len(findings), sim_threshold=sim_threshold)
    kept: list[dict] = []
    kept_sigs: list[str] = []

    for f in findings:
        sig = _signature(f)
        best_idx = -1
        best_sim = 0.0
        for i, prev_sig in enumerate(kept_sigs):
            s = _similarity(sig, prev_sig)
            if s > best_sim:
                best_sim = s
                best_idx = i

        matched = best_sim >= sim_threshold and best_idx >= 0
        if matched:
            existing = kept[best_idx]
            new_is_crit = _is_critical(f)
            old_is_crit = _is_critical(existing)

            # Critical-protect: never collapse a КРИТИЧЕСКОЕ into anything,
            # and never collapse anything into a КРИТИЧЕСКОЕ if the new one
            # is also КРИТИЧЕСКОЕ (each КРИТ stays).
            if new_is_crit or old_is_crit:
                # Both kept as separate canonicals.
                report.critical_collapsed_count += 1
                kept.append(f)
                kept_sigs.append(sig)
                continue

            # Standard collapse: prefer the higher canonical_score.
            if _canonical_score(f) > _canonical_score(existing):
                kept[best_idx] = f
                kept_sigs[best_idx] = sig
            report.same_class_drops += 1
            key = sig[:60] or "<empty-sig>"
            report.same_class_drops_by_key[key] = (
                report.same_class_drops_by_key.get(key, 0) + 1
            )
            continue

        kept.append(f)
        kept_sigs.append(sig)

    report.clusters = len(kept)
    report.total_out = len(kept)

    # Defensive invariants.
    assert report.total_out <= report.total_in, (
        "fuzzy_dedup violated count invariant"
    )
    # КРИТИЧЕСКОЕ count must not decrease.
    crit_in = sum(1 for f in findings if _is_critical(f))
    crit_out = sum(1 for f in kept if _is_critical(f))
    assert crit_out >= crit_in, (
        f"fuzzy_dedup dropped a critical finding "
        f"(in={crit_in}, out={crit_out}); guard failed."
    )
    return kept, report


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------

def _load_findings(path: Path) -> tuple[Any, list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        if "findings" in data and isinstance(data["findings"], list):
            return data, data["findings"]
        if "text_findings" in data and isinstance(data["text_findings"], list):
            return data, data["text_findings"]
    if isinstance(data, list):
        return data, data
    raise ValueError(
        f"Cannot find findings list in {path}: expected dict with"
        " 'findings'/'text_findings' or top-level list."
    )


def _write_back(
    original: Any,
    new_findings: list[dict],
    report: DedupReport,
    dest: Path,
) -> None:
    if isinstance(original, dict):
        out = dict(original)
        if "findings" in original:
            out["findings"] = new_findings
        elif "text_findings" in original:
            out["text_findings"] = new_findings
        out["meta"] = {
            **(out.get("meta") or {}),
            "dedup_report": report.to_dict(),
        }
    else:
        out = new_findings
    dest.write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", help="Path to JSON file with findings.")
    ap.add_argument("--out", default=None, help="Output path (default: <input>.fuzzy.json)")
    ap.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_SIM_THRESHOLD,
        help=f"Similarity threshold in [0,1] (default {DEFAULT_SIM_THRESHOLD}).",
    )
    args = ap.parse_args(argv)

    src = Path(args.input)
    original, items = _load_findings(src)
    new_items, report = fuzzy_dedup(items, sim_threshold=args.threshold)

    dest = Path(args.out) if args.out else src.with_suffix(".fuzzy.json")
    _write_back(original, new_items, report, dest)

    print(
        f"fuzzy_dedup(threshold={args.threshold}): "
        f"{report.total_in} -> {report.total_out} "
        f"(drops={report.same_class_drops}, "
        f"crit_protected={report.critical_collapsed_count})"
    )
    print(f"Saved: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
