"""Unified algorithm dispatcher.

Usage examples:

  # A0 — copy current.json from parent stand into A0 results.
  python algorithm_runner.py --algorithm A0 --case cross_01_eom_ov_loads

  # A1 — Hybrid Lite with optimized_prompts_v1 (Conservative).
  python algorithm_runner.py --algorithm A1 --prompt-set v1 --case cross_01_eom_ov_loads

  # A4 — full production candidate with v2 prompts, skip cached.
  python algorithm_runner.py --algorithm A4 --prompt-set v2 --case ov_01_ventilation --skip-existing

  # Bulk over all 8 cases.
  python algorithm_runner.py --algorithm A1 --prompt-set v1 --all --skip-existing

Each algorithm writes to
`algorithm_research/results/<algorithm>__<prompt_set>/<case_id>.json`
using the parent stand's `RunResult` schema, so the scoring code can be
re-used without changes.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runners._common import (
    EXP_ROOT, RESEARCH_ROOT, RESULTS_DIR,
    PROMPT_SET_DIRS, load_prompt, load_checklist, load_case,
    algorithm_results_dir, result_path, should_skip_existing, write_result,
    run_current_method, run_lens, run_critic, run_reviewer,
    finding_to_dict, make_run_result, cfg,
)
from runners.class_dedup import (
    mark_duplicates, collapse_to_canonical, merge_across_methods, derive_class_key,
)
from runners.conditional_router import should_run_cross_discipline, reviewer_trigger

def _discover_all_cases() -> list[str]:
    """Discover dataset cases dynamically — picks up new cases as they are
    added under `datasets/`. Returns a sorted list of case_id strings.
    """
    from pathlib import Path as _P
    datasets = _P(__file__).resolve().parents[2] / "datasets"
    if not datasets.exists():
        return []
    return sorted([
        p.name for p in datasets.iterdir()
        if p.is_dir() and (p / "case.json").exists()
    ])


ALL_CASES = _discover_all_cases()


# ----- per-algorithm runners ------------------------------------------------


def run_A0(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A0 = baseline current_method. Reuses parent stand's `current.json`.

    If prompt_set is 'baseline', we just symlink/copy the parent's result.
    Otherwise we re-run current_method with the optimized prompt.
    """
    out_path = result_path("A0_baseline_current", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    if prompt_set == "baseline":
        parent_path = EXP_ROOT / "results" / case_id / "current.json"
        if not parent_path.exists():
            raise FileNotFoundError(f"Parent baseline result not found: {parent_path}")
        shutil.copyfile(parent_path, out_path)
        return {"used_cache": False, "copied_from_parent": str(parent_path), "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    findings, res, duration = run_current_method(md, discipline, prompt_set, case_id)
    out = make_run_result(
        method=f"A0_baseline_current__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=findings, duration=duration,
        meta={"prompt_set": prompt_set, "exit_code": res.exit_code},
        errors=[] if res.ok else [f"current_method exit={res.exit_code}"],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(findings)}


def run_A1(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A1 = current_method + completeness lens + class-dedup merge."""
    out_path = result_path("A1_hybrid_lite", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    document_type = info.get("document_type", "full_rd")
    checklist = load_checklist(discipline)

    started = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_cur = ex.submit(run_current_method, md, discipline, prompt_set, case_id)
        f_comp = ex.submit(run_lens, "completeness", md, discipline, prompt_set, case_id,
                            checklist, document_type)
        cur_findings, cur_res, cur_dur = f_cur.result()
        comp_findings, comp_res, comp_dur = f_comp.result()
    duration = time.time() - started

    merged_dicts, dedup_report = merge_across_methods(
        {"current_method": [finding_to_dict(f) for f in cur_findings],
         "completeness":  [finding_to_dict(f) for f in comp_findings]},
        priority=["current_method", "completeness"],
    )

    # Re-coerce to Finding objects so the schema stays consistent.
    final_findings = []
    from runners._common import coerce_finding  # noqa: F401
    for i, raw in enumerate(merged_dicts, start=1):
        try:
            final_findings.append(coerce_finding(raw, i,
                                                  source_agent=raw.get("source_agent", "")))
        except Exception:
            continue

    out = make_run_result(
        method=f"A1_hybrid_lite__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=final_findings, duration=duration,
        meta={
            "prompt_set": prompt_set,
            "document_type": document_type,
            "current_method_findings": len(cur_findings),
            "completeness_findings": len(comp_findings),
            "post_dedup_findings": len(final_findings),
            "dedup_report": dedup_report.__dict__,
            "current_method_duration": cur_dur,
            "completeness_duration": comp_dur,
        },
        errors=[],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(final_findings)}


def run_A2(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A2 = A1 + conditional cross_discipline lens (router-gated)."""
    out_path = result_path("A2_hybrid_cross_conditional", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    document_type = info.get("document_type", "full_rd")
    checklist = load_checklist(discipline)
    decision = should_run_cross_discipline(md, discipline)

    started = time.time()
    futs = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_cur = ex.submit(run_current_method, md, discipline, prompt_set, case_id)
        f_comp = ex.submit(run_lens, "completeness", md, discipline, prompt_set, case_id,
                            checklist, document_type)
        if decision.cross_discipline_triggered:
            f_xd = ex.submit(run_lens, "cross_discipline", md, discipline, prompt_set, case_id,
                              None, document_type)
        else:
            f_xd = None
        cur_findings, cur_res, cur_dur = f_cur.result()
        comp_findings, comp_res, comp_dur = f_comp.result()
        if f_xd is not None:
            xd_findings, xd_res, xd_dur = f_xd.result()
        else:
            xd_findings, xd_res, xd_dur = [], None, 0.0
    duration = time.time() - started

    merged_dicts, dedup_report = merge_across_methods(
        {"current_method":   [finding_to_dict(f) for f in cur_findings],
         "completeness":     [finding_to_dict(f) for f in comp_findings],
         "cross_discipline": [finding_to_dict(f) for f in xd_findings]},
        priority=["current_method", "completeness", "cross_discipline"],
    )

    from runners._common import coerce_finding  # noqa: F401
    final_findings = []
    for i, raw in enumerate(merged_dicts, start=1):
        try:
            final_findings.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
        except Exception:
            continue

    out = make_run_result(
        method=f"A2_hybrid_cross_conditional__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=final_findings, duration=duration,
        meta={
            "prompt_set": prompt_set,
            "router_decision": decision.to_dict(),
            "current_method_findings": len(cur_findings),
            "completeness_findings": len(comp_findings),
            "cross_discipline_findings": len(xd_findings),
            "post_dedup_findings": len(final_findings),
            "dedup_report": dedup_report.__dict__,
        },
        errors=[],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(final_findings)}


def run_A3(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A3 = A1 + improved critic + post-critic class-dedup.

    Mode of operation:
      Stage 1: run current_method + completeness in parallel.
      Stage 2: pre-critic class-dedup (mark, not collapse).
      Stage 3: critic re-grades all findings.
      Stage 4: apply verdicts + post-critic class-collapse.
    """
    out_path = result_path("A3_hybrid_critic_controlled", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    document_type = info.get("document_type", "full_rd")
    checklist = load_checklist(discipline)

    started = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_cur = ex.submit(run_current_method, md, discipline, prompt_set, case_id)
        f_comp = ex.submit(run_lens, "completeness", md, discipline, prompt_set, case_id,
                            checklist, document_type)
        cur_findings, cur_res, cur_dur = f_cur.result()
        comp_findings, comp_res, comp_dur = f_comp.result()

    all_dicts = ([finding_to_dict(f) for f in cur_findings]
                 + [finding_to_dict(f) for f in comp_findings])
    marked, pre_report = mark_duplicates(all_dicts)

    critic_json, _, critic_dur = run_critic(prompt_set, md, discipline, marked, case_id)

    final_dicts = _apply_critic_verdicts(marked, critic_json)
    collapsed, post_report = collapse_to_canonical(final_dicts)
    duration = time.time() - started

    from runners._common import coerce_finding  # noqa: F401
    final_findings = []
    for i, raw in enumerate(collapsed, start=1):
        try:
            final_findings.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
        except Exception:
            continue

    out = make_run_result(
        method=f"A3_hybrid_critic_controlled__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=final_findings, duration=duration,
        meta={
            "prompt_set": prompt_set,
            "current_method_findings": len(cur_findings),
            "completeness_findings": len(comp_findings),
            "pre_critic_count": len(marked),
            "after_pre_dedup_clusters": pre_report.clusters,
            "critic_summary": (critic_json or {}).get("summary") if isinstance(critic_json, dict) else None,
            "post_critic_collapse": post_report.__dict__,
            "post_critic_findings": len(final_findings),
        },
        errors=[],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(final_findings)}


def run_A4(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A4 = A3 + conditional cross_discipline + conditional reviewer."""
    out_path = result_path("A4_hybrid_production_candidate", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    document_type = info.get("document_type", "full_rd")
    checklist = load_checklist(discipline)
    decision = should_run_cross_discipline(md, discipline)

    started = time.time()
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_cur = ex.submit(run_current_method, md, discipline, prompt_set, case_id)
        f_comp = ex.submit(run_lens, "completeness", md, discipline, prompt_set, case_id,
                            checklist, document_type)
        if decision.cross_discipline_triggered:
            f_xd = ex.submit(run_lens, "cross_discipline", md, discipline, prompt_set, case_id,
                              None, document_type)
        else:
            f_xd = None
        cur_findings, cur_res, cur_dur = f_cur.result()
        comp_findings, comp_res, comp_dur = f_comp.result()
        if f_xd is not None:
            xd_findings, xd_res, xd_dur = f_xd.result()
        else:
            xd_findings = []

    all_dicts = (
        [finding_to_dict(f) for f in cur_findings]
        + [finding_to_dict(f) for f in comp_findings]
        + [finding_to_dict(f) for f in xd_findings]
    )
    marked, pre_report = mark_duplicates(all_dicts)
    critic_json, _, critic_dur = run_critic(prompt_set, md, discipline, marked, case_id)

    final_dicts = _apply_critic_verdicts(marked, critic_json)
    collapsed, post_report = collapse_to_canonical(final_dicts)

    # Conditional reviewer
    missed_warnings = (critic_json or {}).get("missed_findings_warning") or []
    rev_decision = reviewer_trigger(len(collapsed), missed_warnings, discipline)
    reviewer_json = None
    if rev_decision.get("reviewer_triggered"):
        reviewer_json, _, _ = run_reviewer(prompt_set, md, discipline, collapsed, critic_json, case_id)
        if reviewer_json and isinstance(reviewer_json, dict):
            rev_findings = reviewer_json.get("findings") or []
            collapsed = rev_findings  # reviewer rewrites the full list

    duration = time.time() - started

    from runners._common import coerce_finding  # noqa: F401
    final_findings = []
    for i, raw in enumerate(collapsed, start=1):
        try:
            final_findings.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
        except Exception:
            continue

    out = make_run_result(
        method=f"A4_hybrid_production_candidate__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=final_findings, duration=duration,
        meta={
            "prompt_set": prompt_set,
            "router_decision": decision.to_dict(),
            "reviewer_decision": rev_decision,
            "current_method_findings": len(cur_findings),
            "completeness_findings": len(comp_findings),
            "cross_discipline_findings": len(xd_findings),
            "pre_critic_count": len(marked),
            "after_pre_dedup_clusters": pre_report.clusters,
            "critic_summary": (critic_json or {}).get("summary") if isinstance(critic_json, dict) else None,
            "reviewer_stats": (reviewer_json or {}).get("stats") if isinstance(reviewer_json, dict) else None,
            "post_critic_findings": len(final_findings),
        },
        errors=[],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(final_findings)}


def run_A5(case_id: str, prompt_set: str, skip_existing: bool) -> dict:
    """A5 = reduced multi-agent: completeness + cross_discipline + critic + reviewer.

    No current_method leg; this is the prompt-quality H11 test.
    """
    out_path = result_path("A5_reduced_multi_agent", prompt_set, case_id)
    cache = should_skip_existing(out_path, skip_existing)
    if cache.used_cache:
        return {"used_cache": True, "path": str(out_path)}

    info, md, _ = load_case(case_id)
    discipline = info.get("discipline", "MULTI")
    document_type = info.get("document_type", "full_rd")
    checklist = load_checklist(discipline)

    started = time.time()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_comp = ex.submit(run_lens, "completeness", md, discipline, prompt_set, case_id,
                            checklist, document_type)
        f_xd = ex.submit(run_lens, "cross_discipline", md, discipline, prompt_set, case_id,
                          None, document_type)
        comp_findings, comp_res, comp_dur = f_comp.result()
        xd_findings, xd_res, xd_dur = f_xd.result()

    all_dicts = (
        [finding_to_dict(f) for f in comp_findings]
        + [finding_to_dict(f) for f in xd_findings]
    )
    marked, pre_report = mark_duplicates(all_dicts)
    critic_json, _, _ = run_critic(prompt_set, md, discipline, marked, case_id)
    final_dicts = _apply_critic_verdicts(marked, critic_json)
    collapsed, post_report = collapse_to_canonical(final_dicts)

    reviewer_json, _, _ = run_reviewer(prompt_set, md, discipline, collapsed, critic_json, case_id)
    if reviewer_json and isinstance(reviewer_json, dict):
        rev_findings = reviewer_json.get("findings") or []
        if rev_findings:
            collapsed = rev_findings

    duration = time.time() - started

    from runners._common import coerce_finding  # noqa: F401
    final_findings = []
    for i, raw in enumerate(collapsed, start=1):
        try:
            final_findings.append(coerce_finding(raw, i, source_agent=raw.get("source_agent", "")))
        except Exception:
            continue

    out = make_run_result(
        method=f"A5_reduced_multi_agent__{prompt_set}",
        case_id=case_id, discipline=discipline,
        findings=final_findings, duration=duration,
        meta={
            "prompt_set": prompt_set,
            "completeness_findings": len(comp_findings),
            "cross_discipline_findings": len(xd_findings),
            "pre_critic_count": len(marked),
            "after_pre_dedup_clusters": pre_report.clusters,
            "critic_summary": (critic_json or {}).get("summary") if isinstance(critic_json, dict) else None,
            "reviewer_stats": (reviewer_json or {}).get("stats") if isinstance(reviewer_json, dict) else None,
            "post_critic_findings": len(final_findings),
        },
        errors=[],
    )
    write_result(out_path, out)
    return {"used_cache": False, "path": str(out_path), "n_findings": len(final_findings)}


# ----- critic verdict application ------------------------------------------


REJECT_VERDICTS = {
    "no_evidence", "speculation", "out_of_scope",
    "non_actionable", "duplicate_same_issue", "duplicate_same_class",
}


def _apply_critic_verdicts(findings: list[dict], critic_json: dict | None) -> list[dict]:
    """Apply critic verdicts (v1/v2 12-verdict set) to findings."""
    if not critic_json or not isinstance(critic_json, dict):
        return findings

    verdicts = {v.get("finding_id"): v for v in (critic_json.get("verdicts") or [])}
    out: list[dict] = []
    for f in findings:
        fid = f.get("id") or f.get("temp_id", "")
        v = verdicts.get(fid)
        if not v:
            out.append(f)
            continue
        verdict = (v.get("verdict") or "pass").lower()

        # Drop the non-canonical of internally-flagged duplicate sets.
        if not f.get("is_canonical", True):
            continue

        if verdict in REJECT_VERDICTS:
            # Class-level dedup verdict: keep only the canonical. Non-canonical
            # already dropped above. If the critic says the canonical itself
            # is a duplicate of another, mark this for removal too.
            if verdict in ("duplicate_same_class", "duplicate_same_issue"):
                dup_of = v.get("duplicate_of")
                if dup_of and dup_of != fid:
                    continue
            elif verdict == "checklist_gap_weak":
                # Downgrade rather than drop.
                f2 = dict(f)
                f2["severity"] = "РЕКОМЕНДАТЕЛЬНОЕ"
                out.append(f2)
                continue
            else:
                continue

        if verdict == "weak_evidence":
            sug = v.get("suggested_severity")
            if sug:
                f = {**f, "severity": sug}
            else:
                # default downgrade
                f = {**f, "severity": _downgrade(f.get("severity", ""))}
        elif verdict == "wrong_severity":
            sug = v.get("suggested_severity")
            if sug:
                f = {**f, "severity": sug}
        elif verdict == "pass_beyond_gt_useful":
            f = {**f, "is_beyond_gt_useful": True}

        out.append(f)
    return out


def _downgrade(sev: str) -> str:
    order = ["КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
             "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ", "РЕКОМЕНДАТЕЛЬНОЕ"]
    if sev in order:
        i = min(order.index(sev) + 1, len(order) - 1)
        return order[i]
    return sev


ALGORITHMS = {
    "A0": run_A0,
    "A1": run_A1,
    "A2": run_A2,
    "A3": run_A3,
    "A4": run_A4,
    "A5": run_A5,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--algorithm", required=True, choices=list(ALGORITHMS))
    ap.add_argument("--prompt-set", default="v1", choices=list(PROMPT_SET_DIRS))
    ap.add_argument("--case", help="Single case ID")
    ap.add_argument("--all", action="store_true",
                    help="Run on all 8 cases.")
    ap.add_argument("--cases", nargs="*", help="Explicit list of cases")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    cases: list[str] = []
    if args.case:
        cases = [args.case]
    elif args.cases:
        cases = args.cases
    elif args.all:
        cases = list(ALL_CASES)
    else:
        sys.exit("Provide --case, --cases, or --all")

    runner = ALGORITHMS[args.algorithm]
    for case_id in cases:
        print(f"\n=== {args.algorithm} on {case_id} (prompts: {args.prompt_set}) ===", flush=True)
        try:
            res = runner(case_id, args.prompt_set, args.skip_existing)
            print(f"  -> {res}", flush=True)
        except Exception as exc:
            print(f"  !! FAILED: {exc}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
