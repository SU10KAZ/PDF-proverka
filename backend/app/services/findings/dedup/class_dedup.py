"""Problem-class deduplication — production-ready, stdlib-only.

Two findings are class-duplicates if they share:
  (problem_class, normalised(affected_system), interface_type, discipline_pair)

When `problem_class` is absent (baseline prompts) we fall back to a
category + canonicalised problem-string key. There is no similarity threshold
here — class_dedup is exact-tuple match. For similarity-based collapse on
un-tagged findings see `fuzzy_dedup.py`.

Safety guarantees (production):
  - Never silently drops a КРИТИЧЕСКОЕ finding. If two КРИТИЧЕСКОЕ findings
    land in the same class cluster, BOTH are kept (the secondary one is
    re-promoted to its own cluster with a synthetic key suffix). The number
    of times this safeguard fires is recorded in
    DedupReport.critical_collapsed_count.
  - Output count never exceeds input count.
  - Fail-open posture: a caller that wraps these helpers in try/except can
    proceed with original findings on any error — same shape, no data loss.

Adapted from
  experiments/md_analysis_comparison/algorithm_research/runners/class_dedup.py
but standalone: no parent-stand import, plain dicts (not Finding dataclass),
pure stdlib.

CLI:
    python class_dedup.py <input.json> [--out <out.json>] [--mode collapse|mark]

Mode `collapse` keeps only canonicals (DEFAULT).
Mode `mark` keeps all findings but tags duplicates via `internal_duplicate_of`.

Python 3.11+.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Normalisation helpers (kept local; do NOT import from fuzzy_dedup.py).
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")


def _normalise(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _short_signature(text: str, n: int = 80) -> str:
    return _normalise(text)[:n]


# ---------------------------------------------------------------------------
# Public types.
# ---------------------------------------------------------------------------

# Severity → numeric weight. Higher = more important. Used both for canonical
# selection and for the КРИТИЧЕСКОЕ-protect guard.
SEVERITY_WEIGHT: dict[str, int] = {
    "КРИТИЧЕСКОЕ": 5,
    "ЭКОНОМИЧЕСКОЕ": 4,
    "ЭКСПЛУАТАЦИОННОЕ": 3,
    "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ": 2,
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 2,  # production format uses a space
    "РЕКОМЕНДАТЕЛЬНОЕ": 1,
}


def _severity_weight(sev: Any) -> int:
    return SEVERITY_WEIGHT.get(str(sev or "").strip(), 0)


def _is_critical(f: dict) -> bool:
    return _severity_weight(f.get("severity")) >= SEVERITY_WEIGHT["КРИТИЧЕСКОЕ"]


def _is_disputed(f: dict) -> bool:
    from backend.app.pipeline.stages.block_analysis.provenance import (
        is_disputed_comparison,
    )

    return is_disputed_comparison(f.get("detector_comparison"))


@dataclass
class ClassKey:
    problem_class: str
    affected_system: str
    interface_type: str | None
    discipline_pair: str | None  # alphabetised CSV

    def tuple(self) -> tuple[str, str, str, str]:
        return (
            self.problem_class,
            self.affected_system,
            self.interface_type or "",
            self.discipline_pair or "",
        )

    def to_str(self) -> str:
        return "|".join(self.tuple())


@dataclass
class ClusterEntry:
    canonical: dict
    duplicates: list[dict] = field(default_factory=list)
    class_key: ClassKey | None = None


@dataclass
class DedupReport:
    total_in: int = 0
    total_out: int = 0
    clusters: int = 0
    same_class_drops: int = 0
    same_class_drops_by_key: dict[str, int] = field(default_factory=dict)
    # Number of times the КРИТИЧЕСКОЕ-protect rule rescued a finding from a
    # collapse it would otherwise have suffered. Zero is the expected value
    # when problem_class semantics are correct.
    critical_collapsed_count: int = 0
    # Unresolved detector conflicts retained as separate candidates.
    disputed_protected_count: int = 0
    # Methods/agents represented in the input (for merge_across_methods).
    methods_seen: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Class-key derivation.
# ---------------------------------------------------------------------------

def derive_class_key(f: dict) -> ClassKey:
    """Build a class key from a plain dict.

    Falls back gracefully when v2 fields are absent (baseline prompts):
      - `problem_class` missing → category + 60-char signature of problem.
      - `affected_system` missing → 50-char signature of evidence_quote.
      - `interface_type` / `discipline_pair` missing → None.
    """
    raw = f if isinstance(f, dict) else dict(f)

    problem_class = raw.get("problem_class") or ""
    if not problem_class:
        problem_class = (raw.get("category") or "other") + ":" + _short_signature(
            raw.get("problem") or raw.get("description") or raw.get("finding") or "",
            60,
        )

    affected_system = _normalise(raw.get("affected_system") or "")
    if not affected_system:
        affected_system = _short_signature(raw.get("evidence_quote") or "", 50)

    interface_type = raw.get("interface_type") or None
    if interface_type:
        interface_type = _normalise(str(interface_type))

    dp = raw.get("discipline_pair") or raw.get("cross_discipline_with") or None
    if isinstance(dp, list):
        if dp:
            dp = ",".join(sorted(_normalise(str(x)) for x in dp if x))
        else:
            dp = None
    elif isinstance(dp, str):
        dp = _normalise(dp)

    return ClassKey(
        problem_class=str(problem_class),
        affected_system=str(affected_system),
        interface_type=interface_type,
        discipline_pair=dp,
    )


# ---------------------------------------------------------------------------
# Canonical scoring.
# ---------------------------------------------------------------------------

def _canonical_score(f: dict) -> tuple[int, float, int, int, int]:
    """Higher tuple = better canonical candidate.

    Ordering (must stay stable — documented in dedup_thresholds.md):
      1. severity weight (КРИТ=5, ЭКОН=4, ЭКСПЛ=3, ПРОВ=2, РЕКОМ=1)
      2. confidence (0.0–1.0)
      3. norm citation filled (1/0)
      4. description length
      5. evidence_quote length
    """
    desc_len = len(f.get("description") or f.get("finding") or "")
    ev_len = len(f.get("evidence_quote") or f.get("md_excerpt") or "")
    norm_filled = 1 if (f.get("norm") or "").strip() else 0
    sev_weight = _severity_weight(f.get("severity"))
    try:
        conf = float(f.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    return (sev_weight, conf, norm_filled, desc_len, ev_len)


# ---------------------------------------------------------------------------
# Critical-protect helper.
# ---------------------------------------------------------------------------

def _split_critical_protected(
    cluster_members: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Split cluster members into (critical_protected, mergeable).

    Rule: every КРИТИЧЕСКОЕ finding stays as its own canonical. Only
    non-critical members may be collapsed into a single canonical.

    Returns:
        critical_protected — list to keep as separate canonicals (1 each).
        mergeable          — list to collapse via standard canonical_score.
    """
    crit = [f for f in cluster_members if _is_critical(f)]
    non_crit = [f for f in cluster_members if not _is_critical(f)]
    return crit, non_crit


# ---------------------------------------------------------------------------
# Core clustering.
# ---------------------------------------------------------------------------

def cluster_findings(
    findings: list[dict],
) -> tuple[list[ClusterEntry], DedupReport]:
    """Group findings into clusters by class key.

    Each cluster has exactly one canonical and zero or more duplicates.
    When two КРИТИЧЕСКОЕ findings share a class key, they are split into
    two separate clusters (with disambiguated keys) — never collapsed.
    """
    report = DedupReport(total_in=len(findings))

    # First pass: bucket by raw class key.
    bucket: dict[str, list[dict]] = {}
    for f in findings:
        ks = derive_class_key(f).to_str()
        bucket.setdefault(ks, []).append(f)

    clusters: list[ClusterEntry] = []
    for ks, members in bucket.items():
        disputed = [f for f in members if _is_disputed(f)]
        mergeable_members = [f for f in members if not _is_disputed(f)]
        critical, non_critical = _split_critical_protected(mergeable_members)

        for disputed_f in disputed:
            clusters.append(
                ClusterEntry(
                    canonical=disputed_f,
                    class_key=derive_class_key(disputed_f),
                )
            )
            if len(members) > 1:
                report.disputed_protected_count += 1

        # Each КРИТИЧЕСКОЕ becomes its own cluster (disambiguated key suffix
        # only if there's more than one).
        for idx, crit_f in enumerate(critical):
            suffix = "" if idx == 0 else f"#crit{idx}"
            key_obj = derive_class_key(crit_f)
            if suffix:
                key_obj = ClassKey(
                    problem_class=key_obj.problem_class + suffix,
                    affected_system=key_obj.affected_system,
                    interface_type=key_obj.interface_type,
                    discipline_pair=key_obj.discipline_pair,
                )
                report.critical_collapsed_count += 1
            clusters.append(ClusterEntry(canonical=crit_f, class_key=key_obj))

        # Non-critical members collapse via canonical_score.
        if non_critical:
            canon = non_critical[0]
            duplicates: list[dict] = []
            for f in non_critical[1:]:
                if _canonical_score(f) > _canonical_score(canon):
                    duplicates.append(canon)
                    canon = f
                else:
                    duplicates.append(f)
            clusters.append(
                ClusterEntry(
                    canonical=canon,
                    duplicates=duplicates,
                    class_key=derive_class_key(canon),
                )
            )
            if duplicates:
                report.same_class_drops += len(duplicates)
                report.same_class_drops_by_key[ks] = (
                    report.same_class_drops_by_key.get(ks, 0) + len(duplicates)
                )

    report.clusters = len(clusters)
    report.total_out = len(clusters)
    return clusters, report


# ---------------------------------------------------------------------------
# Collapse / mark / merge.
# ---------------------------------------------------------------------------

def collapse_to_canonical(
    findings: list[dict],
) -> tuple[list[dict], DedupReport]:
    """Single-pass dedup: keep only canonical from each class cluster."""
    clusters, report = cluster_findings(findings)
    canonical_list: list[dict] = []
    for cluster in clusters:
        canon = dict(cluster.canonical)
        from backend.app.pipeline.stages.block_analysis.provenance import (
            aggregate_traceability,
        )
        canon.update(aggregate_traceability([canon, *cluster.duplicates]))
        source_agents: set[str] = {
            canon.get("source_agent") or canon.get("source", "") or ""
        }
        for d in cluster.duplicates:
            source_agents.add(d.get("source_agent") or d.get("source", "") or "")
        canon["source_agents"] = sorted([s for s in source_agents if s])
        canon["class_key"] = (
            cluster.class_key.to_str() if cluster.class_key else None
        )
        canon["duplicate_count_in_cluster"] = len(cluster.duplicates)
        canon["is_canonical"] = True
        canonical_list.append(canon)

    # Defensive invariant: output count must not exceed input count.
    assert len(canonical_list) <= len(findings), (
        "class_dedup violated count invariant"
    )
    return canonical_list, report


def mark_duplicates(
    findings: list[dict],
) -> tuple[list[dict], DedupReport]:
    """Mark duplicates without dropping them. Used as PRE-CRITIC input.

    Returns the same findings (in original order) annotated with:
      - `internal_duplicate_of` = id of the cluster canonical (or None)
      - `is_canonical` = True for canonicals, False for duplicates
      - `class_key` = the computed key string
    """
    clusters, report = cluster_findings(findings)
    id_to_canonical: dict[str, str] = {}
    canonical_ids: set[str] = set()
    for cluster in clusters:
        canon_id = (
            cluster.canonical.get("id")
            or cluster.canonical.get("temp_id")
            or ""
        )
        canonical_ids.add(canon_id)
        for dup in cluster.duplicates:
            dup_id = dup.get("id") or dup.get("temp_id") or ""
            id_to_canonical[dup_id] = canon_id

    annotated: list[dict] = []
    for f in findings:
        fid = f.get("id") or f.get("temp_id") or ""
        f2 = dict(f)
        if fid in id_to_canonical:
            f2["internal_duplicate_of"] = id_to_canonical[fid]
            f2["is_canonical"] = False
        else:
            f2["internal_duplicate_of"] = None
            f2["is_canonical"] = fid in canonical_ids
        f2["class_key"] = derive_class_key(f).to_str()
        annotated.append(f2)

    # Output count equals input count for mark mode.
    assert len(annotated) == len(findings), (
        "mark_duplicates violated count-equals-input invariant"
    )
    return annotated, report


def merge_across_methods(
    method_to_findings: dict[str, list[dict]],
    priority: list[str] | None = None,
) -> tuple[list[dict], DedupReport]:
    """Merge findings from multiple methods/agents with priority preference.

    `priority` is an ordered list of method names; when two findings clash on
    class key, the canonical comes from the earlier method in the priority
    list (tie-broken by canonical_score). Defaults to insertion order.

    Each КРИТИЧЕСКОЕ finding stays separate even if it shares a class key.
    """
    method_order = priority or list(method_to_findings.keys())
    pri_index = {m: i for i, m in enumerate(method_order)}

    ordered: list[dict] = []
    for name in method_order:
        for f in method_to_findings.get(name, []):
            ff = dict(f)
            ff["_method"] = name
            ordered.append(ff)

    report = DedupReport(total_in=len(ordered))
    report.methods_seen = list(method_order)

    # Bucket by class key first.
    bucket: dict[str, list[dict]] = {}
    for f in ordered:
        ks = derive_class_key(f).to_str()
        bucket.setdefault(ks, []).append(f)

    def _score(f: dict) -> tuple:
        return (
            -pri_index.get(f.get("_method", ""), 10_000),
        ) + _canonical_score(f)

    out: list[dict] = []
    for ks, members in bucket.items():
        disputed = [f for f in members if _is_disputed(f)]
        mergeable_members = [f for f in members if not _is_disputed(f)]
        critical, non_critical = _split_critical_protected(mergeable_members)
        for disputed_f in disputed:
            canon = dict(disputed_f)
            canon["source_agents"] = sorted({canon.get("_method") or ""} - {""})
            canon["class_key"] = derive_class_key(disputed_f).to_str()
            canon["duplicate_count_in_cluster"] = 0
            canon["is_canonical"] = True
            canon.pop("_method", None)
            out.append(canon)
            if len(members) > 1:
                report.disputed_protected_count += 1
        for idx, crit_f in enumerate(critical):
            canon = dict(crit_f)
            canon["source_agents"] = sorted({canon.get("_method") or ""} - {""})
            suffix = "" if idx == 0 else f"#crit{idx}"
            key_obj = derive_class_key(crit_f)
            if suffix:
                key_obj = ClassKey(
                    problem_class=key_obj.problem_class + suffix,
                    affected_system=key_obj.affected_system,
                    interface_type=key_obj.interface_type,
                    discipline_pair=key_obj.discipline_pair,
                )
                report.critical_collapsed_count += 1
            canon["class_key"] = key_obj.to_str()
            canon["duplicate_count_in_cluster"] = 0
            canon["is_canonical"] = True
            canon.pop("_method", None)
            out.append(canon)

        if non_critical:
            canon = non_critical[0]
            duplicates: list[dict] = []
            for f in non_critical[1:]:
                if _score(f) > _score(canon):
                    duplicates.append(canon)
                    canon = f
                else:
                    duplicates.append(f)
            merged = dict(canon)
            merged["source_agents"] = sorted({
                s for s in (
                    [canon.get("_method") or ""]
                    + [d.get("_method") or "" for d in duplicates]
                ) if s
            })
            merged["class_key"] = derive_class_key(canon).to_str()
            merged["duplicate_count_in_cluster"] = len(duplicates)
            merged["is_canonical"] = True
            merged.pop("_method", None)
            if duplicates:
                report.same_class_drops += len(duplicates)
                report.same_class_drops_by_key[ks] = (
                    report.same_class_drops_by_key.get(ks, 0) + len(duplicates)
                )
            out.append(merged)

    report.clusters = len(out)
    report.total_out = len(out)
    assert report.total_out <= report.total_in, (
        "merge_across_methods violated count invariant"
    )
    return out, report


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
    ap.add_argument("--out", default=None, help="Output path (default: <input>.dedup.json)")
    ap.add_argument(
        "--mode",
        choices=["collapse", "mark"],
        default="collapse",
        help="collapse = drop duplicates (default); mark = annotate only.",
    )
    args = ap.parse_args(argv)

    src = Path(args.input)
    original, items = _load_findings(src)
    if args.mode == "collapse":
        new_items, report = collapse_to_canonical(items)
    else:
        new_items, report = mark_duplicates(items)

    dest = Path(args.out) if args.out else src.with_suffix(".dedup.json")
    _write_back(original, new_items, report, dest)

    print(
        f"class_dedup({args.mode}): "
        f"{report.total_in} -> {report.total_out} "
        f"(drops={report.same_class_drops}, "
        f"crit_protected={report.critical_collapsed_count})"
    )
    print(f"Saved: {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
