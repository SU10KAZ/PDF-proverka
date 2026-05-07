#!/usr/bin/env python3
"""
benchmark_critic_v2_against_human.py
--------------------------------------
Benchmark offline critic v2 against human expert decisions.

Data sources (read-only):
  - {project_dir}/_output/expert_review.json  — per-project expert decisions
  - {project_dir}/_output/03_findings.json    — source findings
  - knowledge_base/decisions_log.json          — global knowledge base log (optional)

NOT connected to production pipeline.
Does NOT modify any production artifacts.
Writes only to --output-dir.

Human decision fields (from expert_review.json):
  item_id, item_type, decision ("accepted"|"rejected"), rejection_reason

Benchmark record per finding:
  project_name, finding_id, human_decision, human_reason,
  critic_decision, critic_reject_reason, critic_score, evidence_quality,
  match_confidence, title, description, recommendation

Key metrics:
  human_accepted, human_rejected,
  critic_accepted, critic_rejected, critic_borderline,
  true_accept, true_reject,
  false_reject  ← MOST IMPORTANT: critic rejected what human kept
  false_accept  ← critic accepted what human rejected
  false_reject_rate, false_accept_rate, agreement_rate

Classification:
  agreement, critic_too_strict, critic_too_soft, needs_llm, unmapped

Usage:
    # All projects with human decisions (auto-discover)
    python backend/scripts/benchmark_critic_v2_against_human.py \\
        --output-dir /tmp/benchmark_human

    # Specific section
    python backend/scripts/benchmark_critic_v2_against_human.py \\
        --section AR --output-dir /tmp/benchmark_ar

    # With blocks index and LLM gate
    python backend/scripts/benchmark_critic_v2_against_human.py \\
        --section KJ --with-blocks --llm-gate --llm-provider mock \\
        --output-dir /tmp/benchmark_kj

    # Specific projects by path
    python backend/scripts/benchmark_critic_v2_against_human.py \\
        --project "projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР4.1-К3" \\
        --output-dir /tmp/benchmark_ar4
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ─── Imports ──────────────────────────────────────────────────────────────────

from backend.app.pipeline.stages.findings_review.critic_v2 import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    run_critic_v2_offline,
)
from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
    merge_llm_decisions,
    run_llm_gate,
)

# ─── Constants ────────────────────────────────────────────────────────────────

DEFAULT_OUTPUT_DIR = Path("/tmp/benchmark_human")
PROJECTS_ROOT = _PROJECT_ROOT / "projects"
KB_PATH = _PROJECT_ROOT / "knowledge_base" / "decisions_log.json"

# Match confidence levels
CONF_EXACT = "exact"       # finding_id matches exactly
CONF_HIGH = "high"         # item_id from expert_review matches finding id
CONF_MEDIUM = "medium"     # normalized title + sheet match
CONF_LOW = "low"           # fuzzy title similarity
CONF_UNMAPPED = "unmapped" # no match found

# Classification labels
CLASS_AGREEMENT = "agreement"
CLASS_TOO_STRICT = "critic_too_strict"   # false_reject: critic rejected human-accepted
CLASS_TOO_SOFT = "critic_too_soft"       # false_accept: critic accepted human-rejected
CLASS_NEEDS_LLM = "needs_llm"            # borderline on human-accepted → uncertain
CLASS_UNMAPPED = "unmapped"              # could not map human decision to finding


# ─── Human decisions loader ───────────────────────────────────────────────────

def load_human_decisions_for_project(project_dir: Path) -> list[dict]:
    """
    Load human expert decisions from expert_review.json.
    Returns list of decision dicts with fields:
      item_id, item_type, decision, rejection_reason, reviewer, timestamp
    Only item_type="finding" entries are returned.
    """
    review_path = project_dir / "_output" / "expert_review.json"
    if not review_path.exists():
        return []
    try:
        data = json.loads(review_path.read_text(encoding="utf-8"))
        decisions = data.get("decisions", [])
        return [
            d for d in decisions
            if d.get("item_type", "finding") == "finding"
        ]
    except (json.JSONDecodeError, OSError):
        return []


def load_findings_for_project(project_dir: Path) -> list[dict]:
    """Load 03_findings.json findings list."""
    path = project_dir / "_output" / "03_findings.json"
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw.get("findings", raw.get("items", []))
    except (json.JSONDecodeError, OSError):
        return []


def load_blocks_index(project_dir: Path) -> Optional[set[str]]:
    """Extract block_ids from 02_blocks_analysis.json if present."""
    path = project_dir / "_output" / "02_blocks_analysis.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ids: set[str] = set()
        for item in data.get("block_analyses", []):
            if isinstance(item, dict) and item.get("block_id"):
                ids.add(str(item["block_id"]))
        return ids if ids else None
    except (json.JSONDecodeError, OSError):
        return None


# ─── Project discovery ────────────────────────────────────────────────────────

def discover_projects_with_human_decisions(
    section: Optional[str] = None,
    limit: Optional[int] = None,
    explicit_paths: Optional[list[Path]] = None,
) -> list[Path]:
    """
    Find project directories that have BOTH:
      - _output/03_findings.json
      - _output/expert_review.json with at least one finding decision

    Returns list of project dirs (parent of _output/).
    """
    if explicit_paths:
        result = []
        for p in explicit_paths:
            has_findings = p.exists() and (p / "_output" / "03_findings.json").exists()
            has_review = (p / "_output" / "expert_review.json").exists()
            if has_findings and has_review:
                result.append(p)
            else:
                print(
                    f"  WARN: --project path skipped "
                    f"(findings={has_findings}, expert_review={has_review}): {p}",
                    file=sys.stderr,
                )
        return result

    candidates = sorted(PROJECTS_ROOT.rglob("expert_review.json"))
    result = []

    for review_path in candidates:
        project_dir = review_path.parent.parent
        findings_path = project_dir / "_output" / "03_findings.json"
        if not findings_path.exists():
            continue

        # Quick check: has finding decisions?
        try:
            data = json.loads(review_path.read_text(encoding="utf-8"))
            finding_decs = [
                d for d in data.get("decisions", [])
                if d.get("item_type", "finding") == "finding"
            ]
            if not finding_decs:
                continue
        except (json.JSONDecodeError, OSError):
            continue

        # Section filter
        if section:
            proj_section = _detect_section(project_dir)
            if proj_section.upper() != section.upper():
                continue

        result.append(project_dir)

    if limit:
        result = result[:limit]
    return result


def _detect_section(project_dir: Path) -> str:
    info = project_dir / "project_info.json"
    if info.exists():
        try:
            data = json.loads(info.read_text(encoding="utf-8"))
            s = data.get("section", "")
            if s:
                return str(s).upper()
        except (json.JSONDecodeError, OSError):
            pass
    # Scan parent dirs for 2-5 char alpha segments (discipline codes)
    for part in reversed(project_dir.parts):
        if 2 <= len(part) <= 5 and part.isalpha():
            return part.upper()
    return "unknown"


# ─── Finding ↔ human decision matcher ────────────────────────────────────────

def _normalize_title(text: str) -> str:
    """Normalise title/description for fuzzy matching."""
    if not text:
        return ""
    t = text.lower().strip()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t


def _jaccard(a: str, b: str) -> float:
    ta = set(_normalize_title(a).split())
    tb = set(_normalize_title(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _get_finding_text(f: dict) -> str:
    """Return combined title/problem text from a finding dict."""
    return " ".join(filter(None, [
        f.get("title") or f.get("problem") or "",
        f.get("description") or "",
    ]))


def match_human_to_findings(
    human_decisions: list[dict],
    findings: list[dict],
) -> dict[str, dict]:
    """
    Match human decisions to findings.

    Priority:
      1. Exact item_id == finding["id"]                  → CONF_EXACT
      2. item_id == finding stable/original_id variant   → CONF_HIGH
      3. Normalised title + sheet match                  → CONF_MEDIUM
      4. Jaccard similarity ≥ 0.45                       → CONF_LOW
      5. No match                                        → CONF_UNMAPPED

    Returns dict: finding_id → {human_decision, human_reason, match_confidence, human_item_id}
    """
    # Build lookup maps
    by_id: dict[str, dict] = {}
    for f in findings:
        fid = f.get("id", "")
        if fid:
            by_id[fid] = f

    # Build title+sheet index
    title_index: list[tuple[str, str, dict]] = []  # (norm_title, sheet, finding)
    for f in findings:
        title = _get_finding_text(f)
        sheet = str(f.get("sheet") or f.get("page") or "")
        title_index.append((_normalize_title(title), sheet.lower(), f))

    result: dict[str, dict] = {}

    for hdec in human_decisions:
        item_id = hdec.get("item_id", "")
        h_decision = hdec.get("decision", "unknown")
        h_reason = hdec.get("rejection_reason") or hdec.get("reason") or ""

        matched_fid: Optional[str] = None
        confidence = CONF_UNMAPPED

        # 1. Exact id match
        if item_id in by_id:
            matched_fid = item_id
            confidence = CONF_EXACT
        else:
            # 2. Try variant IDs (some findings have original_id, stable_id)
            for f in findings:
                for field in ("original_id", "stable_id", "source_id"):
                    if f.get(field) == item_id:
                        matched_fid = f.get("id", item_id)
                        confidence = CONF_HIGH
                        break
                if matched_fid:
                    break

        if not matched_fid:
            # 3. Title + sheet match
            norm_item_title = _normalize_title(item_id)  # item_id sometimes has description
            best_score = 0.0
            best_fid = None
            for norm_title, sheet, f in title_index:
                j = _jaccard(norm_item_title, norm_title)
                if j > best_score:
                    best_score = j
                    best_fid = f.get("id")
            if best_score >= 0.6:
                matched_fid = best_fid
                confidence = CONF_MEDIUM
            elif best_score >= 0.45:
                matched_fid = best_fid
                confidence = CONF_LOW

        if matched_fid and matched_fid not in result:
            result[matched_fid] = {
                "human_decision": h_decision,
                "human_reason": h_reason,
                "match_confidence": confidence,
                "human_item_id": item_id,
            }
        elif matched_fid and matched_fid in result:
            # Keep best confidence match
            existing_conf = result[matched_fid]["match_confidence"]
            conf_rank = {CONF_EXACT: 0, CONF_HIGH: 1, CONF_MEDIUM: 2, CONF_LOW: 3}
            if conf_rank.get(confidence, 4) < conf_rank.get(existing_conf, 4):
                result[matched_fid] = {
                    "human_decision": h_decision,
                    "human_reason": h_reason,
                    "match_confidence": confidence,
                    "human_item_id": item_id,
                }

    return result


# ─── Per-project benchmark ────────────────────────────────────────────────────

def benchmark_one_project(
    project_dir: Path,
    with_blocks: bool = False,
    llm_gate: bool = False,
    llm_provider: str = "mock",
    max_candidates: int = 50,
    prompt_path: Optional[Path] = None,
    llm_model: Optional[str] = None,
    llm_timeout: int = 180,
    llm_temperature: float = 0.0,
    context_enrichment: bool = False,
    context_output_dir: Optional[Path] = None,
) -> dict:
    """
    Run critic v2 on one project and compare against human decisions.
    Returns benchmark result dict including before/after LLM metrics.
    """
    findings = load_findings_for_project(project_dir)
    if not findings:
        return {"project": project_dir.name, "skipped": True, "error": "no findings"}

    human_decisions = load_human_decisions_for_project(project_dir)
    if not human_decisions:
        return {"project": project_dir.name, "skipped": True, "error": "no human decisions"}

    blocks_index = load_blocks_index(project_dir) if with_blocks else None
    findings_by_id = {f.get("id", f"idx_{i}"): f for i, f in enumerate(findings)}

    # Run deterministic critic v2
    t0 = time.monotonic()
    det_result = run_critic_v2_offline(findings, blocks_index=blocks_index)
    det_ms = int((time.monotonic() - t0) * 1000)

    final_decisions = det_result.decisions
    det_decisions_snapshot = det_result.decisions  # keep for before/after diff
    llm_gate_result = None
    llm_gate_errors: list[str] = []
    llm_decisions_raw: list[dict] = []      # raw LLMCriticDecision dicts for artifacts

    # Context enrichment (optional)
    context_pkgs: Optional[dict] = None
    context_stats_dict: Optional[dict] = None
    if llm_gate and context_enrichment:
        try:
            from backend.app.pipeline.stages.findings_review.critic_v2.context import (
                ContextCollector,
            )
            collector = ContextCollector.from_project_dir(project_dir)
            packages, ctx_stats = collector.collect_all(findings)
            context_pkgs = {pkg.finding_id: pkg for pkg in packages}
            context_stats_dict = ctx_stats.to_dict()
            print(
                f"\n  [context] collected: {ctx_stats.findings_with_any_context}/"
                f"{ctx_stats.total_findings} with context "
                f"(notes={ctx_stats.findings_with_common_notes}, "
                f"neighbors={ctx_stats.findings_with_neighbors}, "
                f"xrefs={ctx_stats.findings_with_cross_refs})",
                flush=True,
            )
            # Save context artifact if output dir given
            if context_output_dir:
                from backend.app.pipeline.stages.findings_review.critic_v2.context import (
                    ContextCollector as _CC,
                )
                _CC.save_artifact(packages, ctx_stats, context_output_dir)
        except Exception as e:
            print(f"\n  [context] WARNING: context collection failed: {e}", flush=True)
            context_pkgs = None
            context_stats_dict = None

    # Optional LLM gate
    if llm_gate:
        gate = run_llm_gate(
            det_result.decisions,
            findings_by_id,
            provider=llm_provider,
            prompt_path=prompt_path,
            max_candidates=max_candidates,
            model=llm_model,
            timeout=llm_timeout,
            temperature=llm_temperature,
            context_packages=context_pkgs,
        )
        llm_gate_errors = gate.errors

        if gate.errors:
            print(f"\n  [llm_gate] provider errors: {gate.errors}", flush=True)

        final_decisions, _, _, _ = merge_llm_decisions(
            det_result.decisions, gate.decisions, findings_by_id,
        )
        llm_gate_result = gate

        # Serialize LLM decisions for artifact output
        for ld in gate.decisions:
            llm_decisions_raw.append({
                "finding_id": ld.finding_id,
                "llm_decision": ld.llm_decision,
                "usefulness_score": ld.usefulness_score,
                "reject_reason": ld.reject_reason,
                "explanation": ld.explanation,
                "human_taxonomy_reason": ld.human_taxonomy_reason,
                "confidence": ld.confidence,
                "evidence_checked": ld.evidence_checked,
                "source_dependency": ld.source_dependency,
                "provider": ld.provider,
            })

    # Map critic decisions by finding_id (after LLM merge)
    critic_map = {d.finding_id: d for d in final_decisions}
    # Before-LLM map (deterministic only)
    det_map = {d.finding_id: d for d in det_decisions_snapshot}

    # Match human decisions to findings
    human_map = match_human_to_findings(human_decisions, findings)

    # Build per-finding benchmark records
    records: list[dict] = []
    section = _detect_section(project_dir)

    for fid, finding in findings_by_id.items():
        critic_dec = critic_map.get(fid)
        det_dec = det_map.get(fid)
        human_info = human_map.get(fid)

        # Human decision
        if human_info:
            h_decision = human_info["human_decision"]
            h_reason = human_info["human_reason"]
            match_conf = human_info["match_confidence"]
        else:
            h_decision = "unknown"
            h_reason = ""
            match_conf = CONF_UNMAPPED

        # Critic decision (after LLM merge)
        if critic_dec:
            c_decision = critic_dec.decision
            c_reason = critic_dec.reject_reason or ""
            c_score = critic_dec.usefulness_score
            c_ev = critic_dec.evidence_quality
        else:
            c_decision = "unknown"
            c_reason = ""
            c_score = 0
            c_ev = EVIDENCE_NONE

        # Deterministic-only decision (before LLM)
        det_decision_val = det_dec.decision if det_dec else "unknown"

        # Classification (based on final decision)
        classification = _classify(h_decision, c_decision, match_conf)

        title = finding.get("title") or finding.get("problem") or ""
        description = finding.get("description") or ""
        recommendation = finding.get("solution") or finding.get("recommendation") or ""

        records.append({
            "project_name": project_dir.name,
            "project_path": str(project_dir),
            "section": section,
            "finding_id": fid,
            "human_decision": h_decision,
            "human_reason": h_reason,
            "critic_decision": c_decision,
            "critic_decision_before_llm": det_decision_val,
            "critic_reject_reason": c_reason,
            "critic_score": c_score,
            "evidence_quality": c_ev,
            "match_confidence": match_conf,
            "title": title[:200],
            "description": description[:400],
            "recommendation": recommendation[:200],
            "severity": finding.get("severity") or "",
            "category": finding.get("category") or "",
            "sheet": str(finding.get("sheet") or ""),
            "page": finding.get("page"),
            "classification": classification,
        })

    # Compute per-project metrics (final, after LLM)
    metrics = _compute_metrics(records)

    # Compute before-LLM metrics (using deterministic decisions)
    records_before_llm = [
        {**r, "critic_decision": r["critic_decision_before_llm"]}
        for r in records
    ]
    metrics_before_llm = _compute_metrics(records_before_llm) if llm_gate else None

    # Before/after LLM delta
    llm_delta: Optional[dict] = None
    if metrics_before_llm is not None:
        llm_delta = {
            "false_accept_before_llm": metrics_before_llm["false_accept"],
            "false_accept_after_llm": metrics["false_accept"],
            "false_accept_reduced_count": (
                metrics_before_llm["false_accept"] - metrics["false_accept"]
            ),
            "false_reject_before_llm": metrics_before_llm["false_reject"],
            "false_reject_after_llm": metrics["false_reject"],
            "false_reject_introduced_by_llm": max(
                0, metrics["false_reject"] - metrics_before_llm["false_reject"]
            ),
        }

    # Taxonomy breakdown from LLM decisions
    taxonomy_breakdown: dict[str, int] = {}
    needs_human_count = 0
    reject_by_llm_count = 0
    downgraded_reject_count = 0
    if llm_gate_result:
        from collections import Counter
        taxonomy_counter: Counter = Counter()
        for ld in llm_gate_result.decisions:
            if ld.human_taxonomy_reason:
                taxonomy_counter[ld.human_taxonomy_reason] += 1
            if ld.llm_decision == "needs_human":
                needs_human_count += 1
            if ld.llm_decision == "reject":
                reject_by_llm_count += 1
            if "[low-confidence" in ld.explanation or "[taxonomy-safe" in ld.explanation:
                downgraded_reject_count += 1
        taxonomy_breakdown = dict(taxonomy_counter)

    return {
        "project": project_dir.name,
        "project_path": str(project_dir),
        "section": section,
        "skipped": False,
        "error": None,
        "records": records,
        "metrics": metrics,
        "metrics_before_llm": metrics_before_llm,
        "llm_delta": llm_delta,
        "det_ms": det_ms,
        "blocks_index_used": blocks_index is not None,
        "llm_gate_used": llm_gate,
        "llm_provider": llm_provider if llm_gate else None,
        "llm_model": llm_model if llm_gate else None,
        "llm_gate_errors": llm_gate_errors,
        "llm_decisions": llm_decisions_raw,
        "taxonomy_breakdown": taxonomy_breakdown,
        "needs_human_count": needs_human_count,
        "reject_by_llm_count": reject_by_llm_count,
        "downgraded_reject_count": downgraded_reject_count,
        "context_enrichment_used": context_enrichment and context_pkgs is not None,
        "context_stats": context_stats_dict,
    }


def _classify(human_dec: str, critic_dec: str, match_conf: str) -> str:
    """Classify the critic vs human comparison outcome."""
    if match_conf == CONF_UNMAPPED or human_dec == "unknown":
        return CLASS_UNMAPPED
    if human_dec == "accepted" and critic_dec == "accept":
        return CLASS_AGREEMENT
    if human_dec == "rejected" and critic_dec == "reject":
        return CLASS_AGREEMENT
    if human_dec == "accepted" and critic_dec == "reject":
        return CLASS_TOO_STRICT   # FALSE REJECT — dangerous
    if human_dec == "rejected" and critic_dec == "accept":
        return CLASS_TOO_SOFT     # FALSE ACCEPT
    if human_dec == "accepted" and critic_dec in ("borderline", "low_priority"):
        return CLASS_NEEDS_LLM    # Human accepted, critic uncertain
    if human_dec == "rejected" and critic_dec in ("borderline", "low_priority"):
        return CLASS_AGREEMENT    # Both uncertain about it
    if human_dec == "accepted" and critic_dec == "merge":
        return CLASS_AGREEMENT    # Merged duplicates are still handled
    if human_dec == "rejected" and critic_dec == "merge":
        return CLASS_AGREEMENT
    return CLASS_AGREEMENT  # Default for other combinations


def _compute_metrics(records: list[dict]) -> dict:
    """Compute benchmark metrics from records list."""
    human_accepted = [r for r in records if r["human_decision"] == "accepted"]
    human_rejected = [r for r in records if r["human_decision"] == "rejected"]
    mapped = [r for r in records if r["match_confidence"] != CONF_UNMAPPED and r["human_decision"] != "unknown"]

    # Confusion matrix (on mapped records only)
    true_accept = [r for r in mapped if r["human_decision"] == "accepted" and r["critic_decision"] == "accept"]
    true_reject = [r for r in mapped if r["human_decision"] == "rejected" and r["critic_decision"] == "reject"]
    false_reject = [r for r in mapped if r["human_decision"] == "accepted" and r["critic_decision"] == "reject"]
    false_accept = [r for r in mapped if r["human_decision"] == "rejected" and r["critic_decision"] == "accept"]
    borderline_on_accepted = [r for r in mapped if r["human_decision"] == "accepted" and r["critic_decision"] in ("borderline", "low_priority")]
    borderline_on_rejected = [r for r in mapped if r["human_decision"] == "rejected" and r["critic_decision"] in ("borderline", "low_priority")]

    n_mapped = len(mapped)
    n_human_acc = len(human_accepted)
    n_human_rej = len(human_rejected)

    agreement = len(true_accept) + len(true_reject)
    agreement_rate = round(agreement / n_mapped, 3) if n_mapped else 0.0
    false_reject_rate = round(len(false_reject) / n_human_acc, 3) if n_human_acc else 0.0
    false_accept_rate = round(len(false_accept) / n_human_rej, 3) if n_human_rej else 0.0

    # Critic decisions overall
    critic_accepted = [r for r in records if r["critic_decision"] == "accept"]
    critic_rejected = [r for r in records if r["critic_decision"] == "reject"]
    critic_borderline = [r for r in records if r["critic_decision"] in ("borderline", "low_priority")]
    critic_merged = [r for r in records if r["critic_decision"] == "merge"]

    # Classification counts
    from collections import Counter
    class_counts = Counter(r["classification"] for r in records)

    # Rejection reason analysis (human reasons for rejected)
    human_rejection_reasons = [r["human_reason"] for r in human_rejected if r["human_reason"]]
    critic_rejection_reasons = [r["critic_reject_reason"] for r in records if r["critic_reject_reason"]]

    return {
        "total_findings": len(records),
        "total_mapped": n_mapped,
        "total_unmapped": len(records) - n_mapped,
        "human_accepted": n_human_acc,
        "human_rejected": n_human_rej,
        "human_unknown": sum(1 for r in records if r["human_decision"] == "unknown"),
        "critic_accepted": len(critic_accepted),
        "critic_rejected": len(critic_rejected),
        "critic_borderline": len(critic_borderline),
        "critic_merged": len(critic_merged),
        "true_accept": len(true_accept),
        "true_reject": len(true_reject),
        "false_reject": len(false_reject),        # ← MOST IMPORTANT
        "false_accept": len(false_accept),
        "critic_borderline_on_human_accepted": len(borderline_on_accepted),
        "critic_borderline_on_human_rejected": len(borderline_on_rejected),
        "agreement": agreement,
        "agreement_rate": agreement_rate,
        "false_reject_rate": false_reject_rate,
        "false_accept_rate": false_accept_rate,
        "classification_counts": dict(class_counts),
        "human_rejection_reasons_sample": human_rejection_reasons[:5],
        "critic_rejection_reasons_freq": dict(Counter(critic_rejection_reasons).most_common(10)),
    }


# ─── Aggregate summary ────────────────────────────────────────────────────────

def _aggregate_context_stats(ok_results: list[dict]) -> dict:
    """Aggregate context enrichment statistics across all projects."""
    used = [r for r in ok_results if r.get("context_enrichment_used")]
    if not used:
        return {"context_enrichment_used": False}

    total_f = sum((r.get("context_stats") or {}).get("total_findings", 0) for r in used)
    with_notes = sum((r.get("context_stats") or {}).get("findings_with_common_notes", 0) for r in used)
    with_neighbors = sum((r.get("context_stats") or {}).get("findings_with_neighbors", 0) for r in used)
    with_xrefs = sum((r.get("context_stats") or {}).get("findings_with_cross_refs", 0) for r in used)
    with_table = sum((r.get("context_stats") or {}).get("findings_with_table_context", 0) for r in used)
    with_related = sum((r.get("context_stats") or {}).get("findings_with_related_findings", 0) for r in used)
    with_any = sum((r.get("context_stats") or {}).get("findings_with_any_context", 0) for r in used)
    avg_blocks = (
        sum((r.get("context_stats") or {}).get("avg_context_blocks", 0.0) for r in used) / len(used)
        if used else 0.0
    )

    return {
        "context_enrichment_used": True,
        "projects_with_context": len(used),
        "total_findings": total_f,
        "findings_with_common_notes": with_notes,
        "findings_with_neighbors": with_neighbors,
        "findings_with_cross_refs": with_xrefs,
        "findings_with_table_context": with_table,
        "findings_with_related_findings": with_related,
        "findings_with_any_context": with_any,
        "avg_context_blocks": round(avg_blocks, 2),
        "pct_with_any_context": round(with_any / total_f * 100, 1) if total_f else 0.0,
    }


def build_benchmark_summary(results: list[dict]) -> dict:
    """Build cross-project benchmark summary including before/after LLM metrics."""
    ok = [r for r in results if not r.get("skipped")]
    skipped = [r for r in results if r.get("skipped")]

    # Aggregate all records
    all_records: list[dict] = []
    for r in ok:
        all_records.extend(r.get("records", []))

    overall_metrics = _compute_metrics(all_records)

    # Before-LLM aggregate metrics
    has_llm = any(r.get("llm_gate_used") for r in ok)
    overall_metrics_before_llm = None
    if has_llm:
        all_records_before_llm = [
            {**rec, "critic_decision": rec.get("critic_decision_before_llm", rec["critic_decision"])}
            for rec in all_records
        ]
        overall_metrics_before_llm = _compute_metrics(all_records_before_llm)

    # Aggregate before/after LLM deltas
    def _get_delta(r: dict, key: str, fallback_metrics_key: str) -> int:
        delta = r.get("llm_delta") or {}
        if key in delta:
            return delta[key]
        return (r.get("metrics") or {}).get(fallback_metrics_key, 0)

    total_false_accept_before = sum(
        _get_delta(r, "false_accept_before_llm", "false_accept") for r in ok
    )
    total_false_accept_after = overall_metrics["false_accept"]
    total_false_reject_before = sum(
        _get_delta(r, "false_reject_before_llm", "false_reject") for r in ok
    )
    total_false_reject_after = overall_metrics["false_reject"]
    total_needs_human = sum(r.get("needs_human_count", 0) for r in ok)
    total_reject_by_llm = sum(r.get("reject_by_llm_count", 0) for r in ok)
    total_downgraded_reject = sum(r.get("downgraded_reject_count", 0) for r in ok)

    # Aggregate taxonomy reasons
    from collections import Counter, defaultdict
    taxonomy_counter: Counter = Counter()
    for r in ok:
        for reason, cnt in r.get("taxonomy_breakdown", {}).items():
            taxonomy_counter[reason] += cnt

    # False reject cases (the most important)
    false_rejects = [rec for rec in all_records if rec["classification"] == CLASS_TOO_STRICT]
    false_accepts = [rec for rec in all_records if rec["classification"] == CLASS_TOO_SOFT]
    borderline_cases = [rec for rec in all_records if rec["classification"] == CLASS_NEEDS_LLM]

    # By section
    by_section: dict[str, dict] = defaultdict(lambda: {
        "projects": 0, "human_accepted": 0, "human_rejected": 0,
        "false_reject": 0, "false_accept": 0, "agreement": 0, "total_mapped": 0,
    })
    for r in ok:
        m = r.get("metrics", {})
        sec = r.get("section", "unknown")
        by_section[sec]["projects"] += 1
        by_section[sec]["human_accepted"] += m.get("human_accepted", 0)
        by_section[sec]["human_rejected"] += m.get("human_rejected", 0)
        by_section[sec]["false_reject"] += m.get("false_reject", 0)
        by_section[sec]["false_accept"] += m.get("false_accept", 0)
        by_section[sec]["agreement"] += m.get("agreement", 0)
        by_section[sec]["total_mapped"] += m.get("total_mapped", 0)

    # Reason analysis
    human_reasons_counter: Counter = Counter()
    for rec in all_records:
        if rec["human_decision"] == "rejected" and rec["human_reason"]:
            key = rec["human_reason"][:60].strip()
            human_reasons_counter[key] += 1

    critic_reasons_counter: Counter = Counter()
    for rec in all_records:
        if rec["critic_reject_reason"]:
            critic_reasons_counter[rec["critic_reject_reason"]] += 1

    # Per-project summary (compact)
    per_project = []
    for r in ok:
        m = r.get("metrics", {})
        delta = r.get("llm_delta") or {}
        per_project.append({
            "project": r["project"],
            "section": r.get("section", "?"),
            "total": m.get("total_findings", 0),
            "mapped": m.get("total_mapped", 0),
            "human_acc": m.get("human_accepted", 0),
            "human_rej": m.get("human_rejected", 0),
            "c_accept": m.get("critic_accepted", 0),
            "c_reject": m.get("critic_rejected", 0),
            "c_border": m.get("critic_borderline", 0),
            "false_reject": m.get("false_reject", 0),
            "false_accept": m.get("false_accept", 0),
            "agreement_rate": m.get("agreement_rate", 0.0),
            "false_reject_rate": m.get("false_reject_rate", 0.0),
            "false_accept_before_llm": delta.get("false_accept_before_llm"),
            "false_accept_reduced_count": delta.get("false_accept_reduced_count"),
            "false_reject_introduced_by_llm": delta.get("false_reject_introduced_by_llm"),
            "needs_human_count": r.get("needs_human_count", 0),
            "llm_gate_errors": r.get("llm_gate_errors", []),
        })

    # Collect all provider errors
    all_provider_errors: list[dict] = []
    for r in ok:
        errs = r.get("llm_gate_errors", [])
        if errs:
            all_provider_errors.append({
                "project": r["project"],
                "provider": r.get("llm_provider"),
                "errors": errs,
            })

    return {
        "run_config": {
            "projects_processed": len(ok),
            "projects_skipped": len(skipped),
            "llm_gate_used": has_llm,
            "llm_provider": next((r.get("llm_provider") for r in ok if r.get("llm_provider")), None),
            "llm_model": next((r.get("llm_model") for r in ok if r.get("llm_model")), None),
            "context_enrichment_used": any(r.get("context_enrichment_used") for r in ok),
        },
        "overall_metrics": overall_metrics,
        "overall_metrics_before_llm": overall_metrics_before_llm,
        "llm_impact": {
            "false_accept_before_llm": total_false_accept_before if has_llm else None,
            "false_accept_after_llm": total_false_accept_after if has_llm else None,
            "false_accept_reduced_count": (
                total_false_accept_before - total_false_accept_after
            ) if has_llm else None,
            "false_reject_before_llm": total_false_reject_before if has_llm else None,
            "false_reject_after_llm": total_false_reject_after if has_llm else None,
            "false_reject_introduced_by_llm": max(
                0, total_false_reject_after - total_false_reject_before
            ) if has_llm else None,
            "needs_human_count": total_needs_human if has_llm else None,
            "reject_by_llm_count": total_reject_by_llm if has_llm else None,
            "downgraded_reject_due_to_confidence_count": total_downgraded_reject if has_llm else None,
            "taxonomy_reason_breakdown": dict(taxonomy_counter) if has_llm else None,
        },
        "context_enrichment": _aggregate_context_stats(ok),
        "by_section": {k: dict(v) for k, v in by_section.items()},
        "per_project": per_project,
        "false_reject_count": len(false_rejects),
        "false_accept_count": len(false_accepts),
        "borderline_on_accepted_count": len(borderline_cases),
        "top_human_rejection_reasons": dict(human_reasons_counter.most_common(15)),
        "top_critic_rejection_reasons": dict(critic_reasons_counter.most_common(10)),
        "provider_errors": all_provider_errors,
        "skipped": [{"project": r["project"], "error": r.get("error")} for r in skipped],
    }


# ─── Markdown report ──────────────────────────────────────────────────────────

def render_markdown(
    summary: dict,
    false_rejects: list[dict],
    false_accepts: list[dict],
    borderline_cases: list[dict],
) -> str:
    om = summary["overall_metrics"]
    cfg = summary["run_config"]
    llm_impact = summary.get("llm_impact", {})

    lines = [
        "# Critic V2 vs Human Benchmark Report",
        "",
        "## Overview",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Projects processed | {cfg['projects_processed']} |",
        f"| Projects skipped | {cfg['projects_skipped']} |",
        f"| LLM gate | {cfg.get('llm_provider', 'none')} / {cfg.get('llm_model') or 'default'} |" if cfg.get("llm_gate_used") else f"| LLM gate | disabled |",
        f"| Total findings | {om['total_findings']} |",
        f"| Total mapped (human↔critic) | {om['total_mapped']} |",
        f"| Total unmapped | {om['total_unmapped']} |",
        "",
        "## Human Decision Totals",
        "",
        f"| Decision | Count |",
        f"|----------|-------|",
        f"| Human accepted | {om['human_accepted']} |",
        f"| Human rejected | {om['human_rejected']} |",
        f"| Unknown (unmapped) | {om['human_unknown']} |",
        "",
        "## Critic Decision Totals",
        "",
        f"| Decision | Count |",
        f"|----------|-------|",
        f"| Critic accepted | {om['critic_accepted']} |",
        f"| Critic rejected | {om['critic_rejected']} |",
        f"| Critic borderline | {om['critic_borderline']} |",
        f"| Critic merged | {om['critic_merged']} |",
        "",
        "## Agreement Metrics",
        "",
        f"| Metric | Value | Rate |",
        f"|--------|-------|------|",
        f"| True accept | {om['true_accept']} | — |",
        f"| True reject | {om['true_reject']} | — |",
        f"| **False reject** ⚠️ | **{om['false_reject']}** | **{om['false_reject_rate']*100:.1f}%** |",
        f"| False accept | {om['false_accept']} | {om['false_accept_rate']*100:.1f}% |",
        f"| Borderline on human-accepted | {om['critic_borderline_on_human_accepted']} | — |",
        f"| Borderline on human-rejected | {om['critic_borderline_on_human_rejected']} | — |",
        f"| **Overall agreement** | **{om['agreement']}** | **{om['agreement_rate']*100:.1f}%** |",
        "",
    ]

    # LLM Impact
    if llm_impact.get("false_accept_before_llm") is not None:
        fa_reduced = llm_impact.get("false_accept_reduced_count", 0)
        fr_intro = llm_impact.get("false_reject_introduced_by_llm", 0)
        taxonomy = llm_impact.get("taxonomy_reason_breakdown") or {}
        lines += [
            "## LLM Gate Impact",
            "",
            "| Metric | Before LLM | After LLM | Delta |",
            "|--------|-----------|-----------|-------|",
            f"| **False accept** | {llm_impact['false_accept_before_llm']} | "
            f"{llm_impact['false_accept_after_llm']} | "
            f"{'▼ '+str(fa_reduced) if fa_reduced > 0 else ('= 0' if fa_reduced == 0 else '▲ '+str(abs(fa_reduced)))} |",
            f"| **False reject** ⚠️ | {llm_impact['false_reject_before_llm']} | "
            f"{llm_impact['false_reject_after_llm']} | "
            f"{'✓ safe' if fr_intro == 0 else '⚠️ +'+str(fr_intro)} |",
            f"| needs_human | — | {llm_impact.get('needs_human_count', 0)} | — |",
            f"| reject_by_llm | — | {llm_impact.get('reject_by_llm_count', 0)} | — |",
            f"| downgraded_reject (low-conf) | — | {llm_impact.get('downgraded_reject_due_to_confidence_count', 0)} | — |",
            "",
        ]
        if taxonomy:
            lines += ["### Taxonomy Reasons Used by LLM", "", "| Reason | Count |", "|--------|-------|"]
            for reason, cnt in sorted(taxonomy.items(), key=lambda x: -x[1]):
                lines.append(f"| {reason} | {cnt} |")
            lines.append("")

    # Provider errors
    provider_errors = summary.get("provider_errors", [])
    if provider_errors:
        lines += [
            "## ⚠️ Provider Errors",
            "",
            "_LLM gate had errors on the following projects:_",
            "",
        ]
        for pe in provider_errors:
            lines.append(f"- **{pe['project']}** ({pe['provider']}): {'; '.join(pe['errors'][:2])}")
        lines += [
            "",
            "_Use `--llm-provider mock` for offline testing without real LLM._",
            "",
        ]

    # By section
    by_sec = summary.get("by_section", {})
    if by_sec:
        lines += [
            "## By Section",
            "",
            "| Section | Projects | H.Acc | H.Rej | False Rej | False Acc | Agreement |",
            "|---------|----------|-------|-------|-----------|-----------|-----------|",
        ]
        for sec, s in sorted(by_sec.items()):
            tm = s.get("total_mapped", 1) or 1
            agr_rate = f"{s['agreement']*100//tm}%" if tm else "n/a"
            lines.append(
                f"| {sec} | {s['projects']} | {s['human_accepted']} | {s['human_rejected']} "
                f"| **{s['false_reject']}** | {s['false_accept']} | {agr_rate} |"
            )
        lines.append("")

    # Per-project table
    per = summary.get("per_project", [])
    if per:
        lines += [
            "## Per-Project Results",
            "",
            "| Project | Sec | Map | H.Acc | H.Rej | C.Acc | C.Rej | C.Brd | FR | FA | Agr% |",
            "|---------|-----|-----|-------|-------|-------|-------|-------|----|----|------|",
        ]
        for r in per:
            name = r["project"][:30]
            fr_flag = "⚠️" if r["false_reject"] > 0 else ""
            lines.append(
                f"| {name} | {r['section']} | {r['mapped']} "
                f"| {r['human_acc']} | {r['human_rej']} "
                f"| {r['c_accept']} | {r['c_reject']} | {r['c_border']} "
                f"| **{r['false_reject']}**{fr_flag} | {r['false_accept']} "
                f"| {r['agreement_rate']*100:.0f}% |"
            )
        lines.append("")

    # FALSE REJECTS — most important section
    if false_rejects:
        lines += [
            f"## ⚠️ False Rejects ({len(false_rejects)}) — Critic Rejected What Human Accepted",
            "",
            "_These are the MOST DANGEROUS cases: the critic would suppress a finding the human expert validated._",
            "",
        ]
        for i, rec in enumerate(false_rejects[:30], 1):
            lines += [
                f"### FR-{i:03d}: {rec.get('finding_id', '?')} ({rec.get('project_name', rec.get('project', '?'))})",
                f"- **Section:** {rec.get('section', '?')} / **Severity:** {rec.get('severity', '?')}",
                f"- **Title:** {rec['title'][:120]}",
                f"- **Human decision:** {rec['human_decision']} — _{rec['human_reason'][:150] if rec['human_reason'] else 'no reason'}_",
                f"- **Critic decision:** {rec['critic_decision']} / reason: `{rec['critic_reject_reason']}`",
                f"- **Evidence quality:** {rec['evidence_quality']} / score: {rec['critic_score']}",
                f"- **Why dangerous:** Critic would remove a human-validated finding. If promoted to production, this finding would be silently suppressed.",
                f"- **Match confidence:** {rec['match_confidence']}",
                "",
            ]
    else:
        lines += ["## ✅ False Rejects: NONE", "", "_Critic does not reject any human-accepted findings._", ""]

    # FALSE ACCEPTS
    if false_accepts:
        lines += [
            f"## False Accepts ({len(false_accepts)}) — Critic Accepted What Human Rejected",
            "",
        ]
        for i, rec in enumerate(false_accepts[:20], 1):
            lines += [
                f"### FA-{i:03d}: {rec.get('finding_id', '?')} ({rec.get('project_name', rec.get('project', '?'))})",
                f"- **Section:** {rec.get('section', '?')} / **Severity:** {rec.get('severity', '?')}",
                f"- **Title:** {rec['title'][:120]}",
                f"- **Human rejection reason:** _{rec['human_reason'][:200] if rec['human_reason'] else 'no reason'}_",
                f"- **Critic score:** {rec['critic_score']} / evidence: {rec['evidence_quality']}",
                f"- **Why critic passed it:** Critic sees this as a real finding (score ≥ 7), human expert disagreed.",
                "",
            ]
    else:
        lines += ["## False Accepts: NONE", ""]

    # Borderline on human-accepted
    if borderline_cases:
        lines += [
            f"## Borderline on Human-Accepted ({len(borderline_cases)}) — Needs LLM Review",
            "",
            "| Finding | Project | Score | EV | Reason |",
            "|---------|---------|-------|----|--------|",
        ]
        for rec in borderline_cases[:20]:
            fid = rec["finding_id"][:18]
            proj = rec["project_name"][:20]
            lines.append(
                f"| {fid} | {proj} | {rec['critic_score']} | {rec['evidence_quality']} "
                f"| {rec['critic_reject_reason'] or '—'} |"
            )
        lines.append("")

    # Top rejection reasons comparison
    top_human = summary.get("top_human_rejection_reasons", {})
    top_critic = summary.get("top_critic_rejection_reasons", {})

    if top_human:
        lines += [
            "## Top Human Rejection Reasons",
            "",
            "| Reason (first 60 chars) | Count |",
            "|------------------------|-------|",
        ]
        for reason, count in list(top_human.items())[:10]:
            lines.append(f"| {reason} | {count} |")
        lines.append("")

    if top_critic:
        lines += [
            "## Top Critic Rejection Reasons",
            "",
            "| Reason | Count |",
            "|--------|-------|",
        ]
        for reason, count in list(top_critic.items())[:10]:
            lines.append(f"| {reason} | {count} |")
        lines.append("")

    lines += [
        "---",
        "_Generated by benchmark_critic_v2_against_human.py. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


# ─── Output writer ────────────────────────────────────────────────────────────

def write_outputs(
    output_dir: Path,
    summary: dict,
    all_records: list[dict],
    false_rejects: list[dict],
    false_accepts: list[dict],
    borderline_cases: list[dict],
    results: Optional[list[dict]] = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    def _w(fname, data):
        (output_dir / fname).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    _w("human_benchmark_summary.json", summary)
    _w("human_benchmark_records.json", all_records)
    _w("false_rejects.json", false_rejects)
    _w("false_accepts.json", false_accepts)
    _w("borderline_cases.json", borderline_cases)

    # Reason comparison
    from collections import Counter
    reason_comparison = {
        "human_rejection_reasons": summary.get("top_human_rejection_reasons", {}),
        "critic_rejection_reasons": summary.get("top_critic_rejection_reasons", {}),
        "false_reject_reasons": dict(
            Counter(
                r["critic_reject_reason"] for r in false_rejects if r["critic_reject_reason"]
            ).most_common(10)
        ),
    }
    _w("reason_comparison.json", reason_comparison)

    # Taxonomy artifacts (LLM gate decisions)
    if results:
        all_llm_decisions = [
            d for r in results if not r.get("skipped")
            for d in r.get("llm_decisions", [])
        ]
        if all_llm_decisions:
            _w("critic_v2_llm_taxonomy_decisions.json", all_llm_decisions)

            needs_human = [d for d in all_llm_decisions if d.get("llm_decision") == "needs_human"]
            if needs_human:
                _w("critic_v2_llm_needs_human.json", needs_human)

            # False accept risk: accepted findings that LLM reviewed
            false_accept_risk = [
                d for d in all_llm_decisions
                if d.get("llm_decision") in ("accept", "borderline")
                and d.get("confidence", 1.0) < 0.8
            ]
            if false_accept_risk:
                _w("critic_v2_llm_false_accept_risk.json", false_accept_risk)

        # Provider error report
        provider_errors = summary.get("provider_errors", [])
        if provider_errors:
            _w("provider_errors.json", provider_errors)

    # Markdown
    md = render_markdown(summary, false_rejects, false_accepts, borderline_cases)
    (output_dir / "human_benchmark_summary.md").write_text(md, encoding="utf-8")


# ─── Console summary ──────────────────────────────────────────────────────────

def print_benchmark_summary(summary: dict) -> None:
    om = summary["overall_metrics"]
    cfg = summary["run_config"]
    llm_impact = summary.get("llm_impact", {})
    print()
    print("=" * 72)
    print("CRITIC V2 vs HUMAN BENCHMARK")
    print("=" * 72)
    print(f"  Projects processed : {cfg['projects_processed']}  (skipped: {cfg['projects_skipped']})")
    if cfg.get("llm_gate_used"):
        provider = cfg.get("llm_provider", "?")
        model = cfg.get("llm_model") or "(default)"
        print(f"  LLM gate           : {provider} / {model}")
    print()
    print(f"  ── Human Decisions ────────────────────────────────────────────")
    print(f"  Total findings     : {om['total_findings']}")
    print(f"  Mapped (h↔c)       : {om['total_mapped']}")
    print(f"  Human accepted     : {om['human_accepted']}")
    print(f"  Human rejected     : {om['human_rejected']}")
    print()
    print(f"  ── Critic Decisions ───────────────────────────────────────────")
    print(f"  Critic accepted    : {om['critic_accepted']}")
    print(f"  Critic rejected    : {om['critic_rejected']}")
    print(f"  Critic borderline  : {om['critic_borderline']}")
    print()
    print(f"  ── Key Metrics ────────────────────────────────────────────────")
    print(f"  Agreement          : {om['agreement']}  ({om['agreement_rate']*100:.1f}%)")
    print(f"  True accept        : {om['true_accept']}")
    print(f"  True reject        : {om['true_reject']}")
    fr_flag = "  ← DANGER" if om['false_reject'] > 0 else "  ✓ SAFE"
    print(f"  False reject ⚠️    : {om['false_reject']}  ({om['false_reject_rate']*100:.1f}%){fr_flag}")
    print(f"  False accept       : {om['false_accept']}  ({om['false_accept_rate']*100:.1f}%)")
    print(f"  Borderline/h.acc   : {om['critic_borderline_on_human_accepted']}")
    print()

    # LLM gate impact
    if llm_impact.get("false_accept_before_llm") is not None:
        fa_before = llm_impact["false_accept_before_llm"]
        fa_after = llm_impact["false_accept_after_llm"]
        fa_reduced = llm_impact["false_accept_reduced_count"]
        fr_before = llm_impact["false_reject_before_llm"]
        fr_after = llm_impact["false_reject_after_llm"]
        fr_intro = llm_impact["false_reject_introduced_by_llm"]
        needs_h = llm_impact.get("needs_human_count", 0)
        reject_llm = llm_impact.get("reject_by_llm_count", 0)
        downgraded = llm_impact.get("downgraded_reject_due_to_confidence_count", 0)
        taxonomy = llm_impact.get("taxonomy_reason_breakdown") or {}

        print(f"  ── LLM Gate Impact ────────────────────────────────────────────")
        fa_arrow = f"  ▼ {fa_reduced}" if fa_reduced > 0 else ("  =" if fa_reduced == 0 else f"  ▲ {abs(fa_reduced)}")
        fr_arrow = "  ✓ SAFE (no new false rejects)" if fr_intro == 0 else f"  ← DANGER (+{fr_intro} new)"
        print(f"  False accept before LLM : {fa_before}")
        print(f"  False accept after  LLM : {fa_after}{fa_arrow}")
        print(f"  False reject before LLM : {fr_before}")
        print(f"  False reject after  LLM : {fr_after}{fr_arrow}")
        print(f"  needs_human_count       : {needs_h}")
        print(f"  reject_by_llm           : {reject_llm}")
        print(f"  downgraded_reject (conf): {downgraded}")
        if taxonomy:
            print(f"  Taxonomy reasons used   :")
            for reason, cnt in sorted(taxonomy.items(), key=lambda x: -x[1]):
                print(f"    {reason:<40}: {cnt}")
        print()

    per = summary.get("per_project", [])
    if per:
        has_delta = any(r.get("false_accept_before_llm") is not None for r in per)
        print(f"  ── Per Project ────────────────────────────────────────────────")
        if has_delta:
            print(f"  {'Project':<32} {'Sec':>4} {'Map':>4} {'FR':>4} {'FA':>4} {'FAb4':>5} {'ΔFA':>4} {'Agr%':>6}")
            print(f"  {'-'*32} {'-'*4} {'-'*4} {'-'*4} {'-'*4} {'-'*5} {'-'*4} {'-'*6}")
            for r in per:
                fr_marker = " ⚠" if r["false_reject"] > 0 else ""
                fa_before = r.get("false_accept_before_llm")
                fa_delta = r.get("false_accept_reduced_count")
                fa_before_str = str(fa_before) if fa_before is not None else "  -"
                fa_delta_str = f"-{fa_delta}" if fa_delta and fa_delta > 0 else ("0" if fa_delta is not None else " -")
                print(f"  {r['project'][:32]:<32} {r['section']:>4} "
                      f"{r['mapped']:>4} {r['false_reject']:>4}{fr_marker:<2} "
                      f"{r['false_accept']:>4} {fa_before_str:>5} {fa_delta_str:>4}  {r['agreement_rate']*100:>5.1f}%")
        else:
            print(f"  {'Project':<32} {'Sec':>4} {'Map':>4} {'FR':>4} {'FA':>4} {'Agr%':>6}")
            print(f"  {'-'*32} {'-'*4} {'-'*4} {'-'*4} {'-'*4} {'-'*6}")
            for r in per:
                fr_marker = " ⚠" if r["false_reject"] > 0 else ""
                print(f"  {r['project'][:32]:<32} {r['section']:>4} "
                      f"{r['mapped']:>4} {r['false_reject']:>4}{fr_marker:<2} "
                      f"{r['false_accept']:>4}  {r['agreement_rate']*100:>5.1f}%")
        print()

    top_cr = summary.get("top_critic_rejection_reasons", {})
    if top_cr:
        print(f"  ── Top Critic Rejection Reasons ───────────────────────────────")
        for reason, count in list(top_cr.items())[:6]:
            print(f"  {reason:<35}: {count}")
        print()

    # Context enrichment stats
    ctx_stats = summary.get("context_enrichment", {})
    if ctx_stats.get("context_enrichment_used"):
        total_f = ctx_stats.get("total_findings", 0)
        print(f"  ── Context Enrichment Stats ───────────────────────────────────")
        print(f"  Findings total                  : {total_f}")
        print(f"  With any context                : {ctx_stats.get('findings_with_any_context', 0)} ({ctx_stats.get('pct_with_any_context', 0):.1f}%)")
        print(f"  With common notes / gen.notes   : {ctx_stats.get('findings_with_common_notes', 0)}")
        print(f"  With neighbor blocks            : {ctx_stats.get('findings_with_neighbors', 0)}")
        print(f"  With cross-references           : {ctx_stats.get('findings_with_cross_refs', 0)}")
        print(f"  With table context              : {ctx_stats.get('findings_with_table_context', 0)}")
        print(f"  With related findings           : {ctx_stats.get('findings_with_related_findings', 0)}")
        print(f"  Avg context blocks / finding    : {ctx_stats.get('avg_context_blocks', 0):.2f}")
        print()

    # Provider errors
    provider_errors = summary.get("provider_errors", [])
    if provider_errors:
        print(f"  ── Provider Errors ────────────────────────────────────────────")
        for pe in provider_errors:
            print(f"  {pe['project']}: {pe['errors']}")
        print()
        print(f"  NOTE: LLM gate had errors. Use --llm-provider mock for offline testing.")
        print()

    print("=" * 72)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark critic v2 against human expert decisions (read-only, offline).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # All projects with human decisions (auto-discover)
  python %(prog)s --output-dir /tmp/benchmark_human

  # AR section only
  python %(prog)s --section AR --output-dir /tmp/benchmark_ar

  # Specific project
  python %(prog)s \\
      --project "projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР4.1-К3" \\
      --output-dir /tmp/benchmark_ar4

  # With blocks + mock LLM gate
  python %(prog)s --section KJ --with-blocks \\
      --llm-gate --llm-provider mock --output-dir /tmp/benchmark_kj
""",
    )
    parser.add_argument(
        "--projects-root", type=Path, default=PROJECTS_ROOT,
        help=f"Root directory for projects (default: {PROJECTS_ROOT}).",
    )
    parser.add_argument(
        "--project", dest="project_paths", action="append", type=Path, default=None,
        help="Explicit project path (can repeat). Skips auto-discovery.",
    )
    parser.add_argument(
        "--section", default=None,
        help="Filter by discipline section (AR, KJ, EOM, ...).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max projects to process.",
    )
    parser.add_argument(
        "--human-source", default="auto",
        choices=["auto", "json", "knowledge_base"],
        help="Human decision source (default: auto → expert_review.json).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--with-blocks", action="store_true",
        help="Load 02_blocks_analysis.json per project for phantom block detection.",
    )
    parser.add_argument(
        "--llm-gate", action="store_true",
        help="Run optional LLM gate after deterministic critic.",
    )
    parser.add_argument(
        "--llm-provider", default="mock",
        choices=["mock", "noop", "claude_runner", "openrouter"],
        help="LLM provider (default: mock). Use claude_runner or openrouter for real LLM.",
    )
    parser.add_argument(
        "--llm-model", default=None,
        help="Model override for real LLM providers "
             "(e.g. 'claude-haiku-4-5-20251001' for claude_runner, "
             "'openai/gpt-4o-mini' for openrouter). "
             "Default: claude-sonnet-4-6 / openai/gpt-4o-mini.",
    )
    parser.add_argument(
        "--llm-timeout", type=int, default=180,
        help="Request timeout in seconds for real LLM providers (default: 180).",
    )
    parser.add_argument(
        "--llm-temperature", type=float, default=0.0,
        help="Sampling temperature for real LLM providers (default: 0.0 = deterministic).",
    )
    parser.add_argument(
        "--max-candidates", type=int, default=50,
        help="Max candidates per project for LLM gate (default: 50).",
    )
    parser.add_argument(
        "--prompt-path", type=Path, default=None,
        help="Custom critic prompt path.",
    )
    parser.add_argument(
        "--context-enrichment", action="store_true",
        help=(
            "Enable offline context enrichment before LLM gate. "
            "Loads document_graph.json and 02_blocks_analysis.json for each project "
            "to provide LLM with neighbor blocks, common notes, cross-references. "
            "Requires --llm-gate. Does NOT modify production artifacts."
        ),
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-project progress output.",
    )
    args = parser.parse_args()

    # Update projects root if overridden
    if args.projects_root != PROJECTS_ROOT:
        import backend.scripts.benchmark_critic_v2_against_human as _self
        _self.PROJECTS_ROOT = args.projects_root

    # Discover projects
    projects = discover_projects_with_human_decisions(
        section=args.section,
        limit=args.limit,
        explicit_paths=args.project_paths,
    )

    if not projects:
        print("ERROR: No projects found with both 03_findings.json and expert_review.json",
              file=sys.stderr)
        if args.section:
            print(f"  Section filter: {args.section}", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not args.quiet:
        print(f"\nBenchmark critic v2 vs human decisions")
        print(f"  Projects found   : {len(projects)}")
        print(f"  Section filter   : {args.section or 'all'}")
        llm_gate_str = f"yes ({args.llm_provider})" if args.llm_gate else "no"
        if args.llm_gate and args.context_enrichment:
            llm_gate_str += " [+context enrichment]"
        print(f"  LLM gate         : {llm_gate_str}")
        print(f"  Blocks index     : {'yes' if args.with_blocks else 'no'}")
        print(f"  Output dir       : {args.output_dir}")
        print()

    results = []
    t_start = time.monotonic()

    for i, project_dir in enumerate(projects, 1):
        if not args.quiet:
            print(f"  [{i:03d}/{len(projects):03d}] {project_dir.name} ...", end=" ", flush=True)
        t0 = time.monotonic()

        result = benchmark_one_project(
            project_dir=project_dir,
            with_blocks=args.with_blocks,
            llm_gate=args.llm_gate,
            llm_provider=args.llm_provider,
            max_candidates=args.max_candidates,
            prompt_path=args.prompt_path,
            llm_model=args.llm_model,
            llm_timeout=args.llm_timeout,
            llm_temperature=args.llm_temperature,
            context_enrichment=args.context_enrichment,
            context_output_dir=args.output_dir if args.context_enrichment else None,
        )
        results.append(result)

        if not args.quiet:
            elapsed = int((time.monotonic() - t0) * 1000)
            if result.get("skipped"):
                print(f"SKIP ({result.get('error', '')})")
            else:
                m = result["metrics"]
                print(
                    f"OK  {m['total_findings']}F "
                    f"h.acc={m['human_accepted']} h.rej={m['human_rejected']} "
                    f"FR={m['false_reject']} FA={m['false_accept']} "
                    f"agr={m['agreement_rate']*100:.0f}%  [{elapsed}ms]"
                )

    batch_ms = int((time.monotonic() - t_start) * 1000)

    # Build summary
    summary = build_benchmark_summary(results)

    # Extract special record lists
    all_records = [rec for r in results if not r.get("skipped") for rec in r.get("records", [])]
    false_rejects = [r for r in all_records if r["classification"] == CLASS_TOO_STRICT]
    false_accepts = [r for r in all_records if r["classification"] == CLASS_TOO_SOFT]
    borderline_cases = [r for r in all_records if r["classification"] == CLASS_NEEDS_LLM]

    # Write outputs
    write_outputs(
        args.output_dir,
        summary,
        all_records,
        false_rejects,
        false_accepts,
        borderline_cases,
        results=results,
    )

    print_benchmark_summary(summary)
    print(f"  Total time: {batch_ms}ms ({batch_ms//1000}s)")
    print(f"  Output files:")
    print(f"    {args.output_dir}/human_benchmark_summary.json")
    print(f"    {args.output_dir}/human_benchmark_summary.md")
    print(f"    {args.output_dir}/human_benchmark_records.json")
    print(f"    {args.output_dir}/false_rejects.json")
    print(f"    {args.output_dir}/false_accepts.json")
    print(f"    {args.output_dir}/borderline_cases.json")
    print(f"    {args.output_dir}/reason_comparison.json")
    if args.llm_gate:
        print(f"    {args.output_dir}/critic_v2_llm_taxonomy_decisions.json  (if LLM reviewed any)")
        print(f"    {args.output_dir}/critic_v2_llm_needs_human.json         (if any needs_human)")
        print(f"    {args.output_dir}/critic_v2_llm_false_accept_risk.json   (if any low-conf accepts)")
        if summary.get("provider_errors"):
            print(f"    {args.output_dir}/provider_errors.json")
            print(f"\n  WARNING: LLM provider had errors. Check provider_errors.json.")
            print(f"           To test without real LLM: --llm-provider mock")
    print()
    print("  NOTE: Production pipeline NOT modified.")
    print("        No production artifacts changed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
