"""Problem-class deduplication.

Two findings are class-duplicates if they share:
  (problem_class, normalised(affected_system), interface_type, discipline_pair)

When `problem_class` is absent (baseline prompts) we fall back to a
category + canonicalised problem-string key with a similarity cutoff.

Used in two places:
  1. Pre-critic: cluster identical-class findings, mark duplicates with
     `internal_duplicate_of`, pass the full list (with markers) to the
     critic. The critic can confirm or reject the clustering.
  2. Post-critic / reviewer: final pass that collapses duplicates that
     the critic confirmed.

No LLM calls. Pure Python.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

# Re-use parent stand's Finding dataclass for consistency. The parent has
# its own `runners` package which collides with this folder's `runners` —
# we import the file by absolute path under a unique module name.
_EXP_ROOT = Path(__file__).resolve().parents[2]
import importlib.util as _ilu
_MOD_NAME = "_parent_unified_output_schema"
if _MOD_NAME in sys.modules:
    _mod = sys.modules[_MOD_NAME]
else:
    _schema_path = _EXP_ROOT / "runners" / "unified_output_schema.py"
    _spec = _ilu.spec_from_file_location(_MOD_NAME, _schema_path)
    _mod = _ilu.module_from_spec(_spec)
    sys.modules[_MOD_NAME] = _mod
    _spec.loader.exec_module(_mod)
Finding = _mod.Finding


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


def _sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b, autojunk=False).ratio()


@dataclass
class ClassKey:
    problem_class: str
    affected_system: str
    interface_type: str | None
    discipline_pair: str | None  # alphabetised CSV

    def tuple(self) -> tuple:
        return (
            self.problem_class,
            self.affected_system,
            self.interface_type or "",
            self.discipline_pair or "",
        )

    def to_str(self) -> str:
        return "|".join(self.tuple())


def derive_class_key(f: Finding | dict) -> ClassKey:
    """Build a class key from a Finding or a raw dict."""
    if isinstance(f, Finding):
        raw = f.__dict__
    else:
        raw = dict(f)

    problem_class = raw.get("problem_class") or ""
    if not problem_class:
        # Fallback for baseline (no class field): derive from category +
        # problem-string signature.
        problem_class = (raw.get("category") or "other") + ":" + _short_signature(
            raw.get("problem") or raw.get("description") or "", 60
        )

    affected_system = _normalise(raw.get("affected_system") or "")
    if not affected_system:
        # Fallback: short signature of evidence quote.
        affected_system = _short_signature(raw.get("evidence_quote") or "", 50)

    interface_type = raw.get("interface_type") or None
    if interface_type:
        interface_type = _normalise(str(interface_type))

    dp = raw.get("discipline_pair") or raw.get("cross_discipline_with") or None
    if isinstance(dp, list):
        if dp:
            dp = ",".join(sorted([_normalise(str(x)) for x in dp if x]))
        else:
            dp = None

    return ClassKey(
        problem_class=str(problem_class),
        affected_system=str(affected_system),
        interface_type=interface_type,
        discipline_pair=dp,
    )


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


def _canonical_score(f: dict) -> tuple:
    """Higher tuple = better canonical candidate."""
    # Prefer: higher confidence, longer description, longer evidence,
    # explicit norm citation.
    desc_len = len(f.get("description") or "")
    ev_len = len(f.get("evidence_quote") or "")
    norm_filled = 1 if (f.get("norm") or "").strip() else 0
    sev_weight = {
        "КРИТИЧЕСКОЕ": 5, "ЭКОНОМИЧЕСКОЕ": 4,
        "ЭКСПЛУАТАЦИОННОЕ": 3, "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ": 2,
        "РЕКОМЕНДАТЕЛЬНОЕ": 1,
    }.get(str(f.get("severity") or ""), 0)
    conf = float(f.get("confidence") or 0.0)
    return (sev_weight, conf, norm_filled, desc_len, ev_len)


def cluster_findings(findings: list[dict]) -> tuple[list[ClusterEntry], DedupReport]:
    """Group findings into clusters by class key. Returns clusters and a report.

    Each cluster has exactly one canonical finding and zero+ duplicates.
    """
    report = DedupReport(total_in=len(findings))
    clusters_by_key: dict[str, ClusterEntry] = {}

    for f in findings:
        key = derive_class_key(f)
        ks = key.to_str()
        if ks not in clusters_by_key:
            clusters_by_key[ks] = ClusterEntry(canonical=f, class_key=key)
            continue

        existing = clusters_by_key[ks]
        if _canonical_score(f) > _canonical_score(existing.canonical):
            # Demote previous canonical.
            existing.duplicates.append(existing.canonical)
            existing.canonical = f
        else:
            existing.duplicates.append(f)
        report.same_class_drops += 1
        report.same_class_drops_by_key[ks] = (
            report.same_class_drops_by_key.get(ks, 0) + 1
        )

    report.clusters = len(clusters_by_key)
    report.total_out = report.clusters
    return list(clusters_by_key.values()), report


def collapse_to_canonical(findings: list[dict]) -> tuple[list[dict], DedupReport]:
    """Single-pass dedup: keep only canonical from each class cluster."""
    clusters, report = cluster_findings(findings)
    canonical_list = []
    for cluster in clusters:
        canon = dict(cluster.canonical)
        # Annotate canonical with the set of source agents from the cluster.
        source_agents = {canon.get("source_agent") or canon.get("source", "")}
        for d in cluster.duplicates:
            source_agents.add(d.get("source_agent") or d.get("source", ""))
        canon["source_agents"] = sorted([s for s in source_agents if s])
        canon["class_key"] = cluster.class_key.to_str() if cluster.class_key else None
        canon["duplicate_count_in_cluster"] = len(cluster.duplicates)
        canonical_list.append(canon)
    return canonical_list, report


def fuzzy_dedup(findings: list[dict], sim_threshold: float = 0.7) -> tuple[list[dict], DedupReport]:
    """Similarity-based dedup for baseline outputs WITHOUT `problem_class`.

    Compares (category, problem, affected_system, evidence_quote-snippet)
    pairwise; collapses when similarity exceeds threshold.

    Used as a *comparator* for retroactive dedup on baseline-prompt data.
    Less precise than class-key dedup but works on un-tagged findings.
    """
    report = DedupReport(total_in=len(findings))
    kept: list[dict] = []
    kept_sigs: list[str] = []
    for f in findings:
        sig = _normalise(
            (f.get("category") or "")
            + " | " + (f.get("problem") or "")
            + " | " + (f.get("affected_system") or "")
            + " | " + (f.get("evidence_quote") or "")[:120]
        )
        is_dup = False
        best_idx = -1
        best_sim = 0.0
        for i, prev in enumerate(kept_sigs):
            s = _sim(sig, prev)
            if s > best_sim:
                best_sim = s
                best_idx = i
        if best_sim >= sim_threshold and best_idx >= 0:
            existing = kept[best_idx]
            if _canonical_score(f) > _canonical_score(existing):
                kept[best_idx] = f
                kept_sigs[best_idx] = sig
            report.same_class_drops += 1
            is_dup = True
        if not is_dup:
            kept.append(f)
            kept_sigs.append(sig)
    report.clusters = len(kept)
    report.total_out = len(kept)
    return kept, report


def mark_duplicates(findings: list[dict]) -> tuple[list[dict], DedupReport]:
    """Mark duplicates without dropping them. Used as PRE-CRITIC input.

    Returns the same findings (in order) with `internal_duplicate_of`
    set on the non-canonical ones to the canonical finding id.
    """
    clusters, report = cluster_findings(findings)
    id_to_canonical: dict[str, str] = {}
    canonical_ids: set[str] = set()
    for cluster in clusters:
        canon_id = cluster.canonical.get("id") or cluster.canonical.get(
            "temp_id", ""
        )
        canonical_ids.add(canon_id)
        for dup in cluster.duplicates:
            dup_id = dup.get("id") or dup.get("temp_id", "")
            id_to_canonical[dup_id] = canon_id

    annotated: list[dict] = []
    for f in findings:
        fid = f.get("id") or f.get("temp_id", "")
        f2 = dict(f)
        if fid in id_to_canonical:
            f2["internal_duplicate_of"] = id_to_canonical[fid]
            f2["is_canonical"] = False
        else:
            f2["internal_duplicate_of"] = None
            f2["is_canonical"] = fid in canonical_ids
        f2["class_key"] = derive_class_key(f).to_str()
        annotated.append(f2)
    return annotated, report


def merge_across_methods(
    method_to_findings: dict[str, list[dict]],
    priority: list[str] | None = None,
) -> tuple[list[dict], DedupReport]:
    """Merge findings from multiple methods/agents with priority preference.

    `priority` is an ordered list of method names; when two findings clash on
    class key, the canonical comes from the earlier method in the priority
    list. Defaults to insertion order.
    """
    ordered = []
    for name in priority or list(method_to_findings):
        for f in method_to_findings.get(name, []):
            ff = dict(f)
            ff["_method"] = name
            ordered.append(ff)

    # Use the same clustering but choose canonical by priority first, then by score.
    report = DedupReport(total_in=len(ordered))
    clusters_by_key: dict[str, ClusterEntry] = {}
    pri_index = {m: i for i, m in enumerate(priority or list(method_to_findings))}

    def _score(f: dict) -> tuple:
        return (-pri_index.get(f.get("_method", ""), 1000),) + _canonical_score(f)

    for f in ordered:
        key = derive_class_key(f)
        ks = key.to_str()
        if ks not in clusters_by_key:
            clusters_by_key[ks] = ClusterEntry(canonical=f, class_key=key)
            continue
        existing = clusters_by_key[ks]
        if _score(f) > _score(existing.canonical):
            existing.duplicates.append(existing.canonical)
            existing.canonical = f
        else:
            existing.duplicates.append(f)
        report.same_class_drops += 1
        report.same_class_drops_by_key[ks] = (
            report.same_class_drops_by_key.get(ks, 0) + 1
        )
    report.clusters = len(clusters_by_key)
    report.total_out = report.clusters

    out: list[dict] = []
    for cluster in clusters_by_key.values():
        canon = dict(cluster.canonical)
        canon["source_agents"] = sorted(set(
            [canon.get("_method") or canon.get("source_agent", "")]
            + [d.get("_method") or d.get("source_agent", "") for d in cluster.duplicates]
        ))
        canon["class_key"] = cluster.class_key.to_str()
        canon["duplicate_count_in_cluster"] = len(cluster.duplicates)
        canon.pop("_method", None)
        out.append(canon)

    return out, report


if __name__ == "__main__":
    # Standalone usage: collapse a file in-place.
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", help="Path to JSON file with 'findings' list")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    src = Path(args.input)
    data = json.loads(src.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "findings" in data:
        items = data["findings"]
    else:
        items = data
    deduped, report = collapse_to_canonical(items)
    if isinstance(data, dict):
        out = dict(data)
        out["findings"] = deduped
        out["meta"] = {**(out.get("meta") or {}), "dedup_report": report.__dict__}
    else:
        out = deduped

    dest = Path(args.out) if args.out else src.with_suffix(".dedup.json")
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Dedup: {report.total_in} -> {report.total_out} (drops {report.same_class_drops})")
    print(f"Saved: {dest}")
