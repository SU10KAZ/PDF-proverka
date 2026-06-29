#!/usr/bin/env python3
"""
scripts/validate_findings_kb.py
--------------------------------
CLI runner: validates findings in a project using the KB-augmented agent.

Usage:
    # Single project
    python scripts/validate_findings_kb.py projects/213*/EOM/133_23-??-??1

    # All projects in a discipline
    python scripts/validate_findings_kb.py --discipline EOM

    # All projects
    python scripts/validate_findings_kb.py --all

    # Dry run (no LLM, just show KB matches)
    python scripts/validate_findings_kb.py projects/XXX --dry-run

Output per project:
    _output/kb_validation.json  - full results
    _output/kb_validation_report.md - human-readable report
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from backend.app.pipeline.stages.findings_review.critic_v2.kb_gate import KBGate, KBGateResult


def find_projects(base: Path, discipline: str = "", all_projects: bool = False):
    if all_projects:
        return sorted(base.glob("**/03_findings.json"))
    if discipline:
        return sorted(base.glob(f"**/{discipline}/*/_output/03_findings.json"))
    return []


def validate_project(project_dir: Path, gate: KBGate, dry_run: bool = False) -> dict:
    findings_path = project_dir / "_output" / "03_findings.json"
    if not findings_path.exists():
        # Try project_dir as direct path
        if (project_dir / "03_findings.json").exists():
            findings_path = project_dir / "03_findings.json"
            output_dir = project_dir
        else:
            return {"error": "03_findings.json not found in " + str(project_dir)}
    else:
        output_dir = project_dir / "_output"

    with open(findings_path, encoding="utf-8") as f:
        data = json.load(f)

    findings = data.get("findings", [])
    if not findings:
        return {"error": "No findings", "path": str(findings_path)}

    # Infer section from path
    section = _infer_section_from_path(project_dir)
    print(f"  [{section}] {project_dir.name}: {len(findings)} замечаний", end="", flush=True)

    if dry_run:
        # Just show KB matches, no LLM call
        retriever = gate._retriever
        dry_results = []
        for f in findings[:3]:
            f_with_section = {**f, "section": section}
            matches = retriever.find_similar(f_with_section, top_k=3)
            dry_results.append({
                "finding_id": f.get("id"),
                "summary": f.get("problem", "")[:100],
                "kb_matches": [
                    {"id": m.decision_id, "score": m.similarity_score, "reason": m.expert_reason[:100]}
                    for m in matches
                ],
            })
        print(" [DRY RUN]")
        return {"dry_run": True, "sample": dry_results, "total_findings": len(findings)}

    result: KBGateResult = gate.validate(findings, section=section)
    print(f" -> reject={result.rejected} borderline={result.borderline} accept={result.accepted} ({result.elapsed_sec}s)")

    # Save results
    output = {
        "project": str(project_dir),
        "section": section,
        "total_findings": len(findings),
        "model": result.model_used,
        "elapsed_sec": result.elapsed_sec,
        "summary": {
            "rejected": result.rejected,
            "borderline": result.borderline,
            "accepted": result.accepted,
            "needs_human": result.needs_human,
            "errors": result.errors,
        },
        "decisions": [
            {
                "finding_id": d.finding_id,
                "llm_decision": d.llm_decision,
                "human_taxonomy_reason": d.human_taxonomy_reason,
                "explanation": d.explanation,
                "confidence": d.confidence,
                "kb_examples_used": d.kb_examples_used,
            }
            for d in result.decisions
        ],
    }

    out_path = output_dir / "kb_validation.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Generate markdown report
    _write_md_report(output, output_dir / "kb_validation_report.md", findings)

    return output


def _infer_section_from_path(path: Path) -> str:
    parts = path.parts
    known = {"EOM", "OV", "AR", "VK", "SS", "KR", "TX", "GP", "ITP", "PT", "AI", "DOC", "KM", "POS"}
    for part in reversed(parts):
        if part in known:
            return part
    return ""


def _write_md_report(output: dict, path: Path, findings: list):
    findings_by_id = {f.get("id"): f for f in findings}
    lines = [
        f"# KB Validation Report",
        f"",
        f"**Проект:** {output['project']}",
        f"**Раздел:** {output['section']}",
        f"**Модель:** {output['model']}",
        f"**Замечаний:** {output['total_findings']}",
        f"",
        f"## Итог",
        f"",
        f"| Решение | Кол-во |",
        f"|--------|--------|",
        f"| reject | {output['summary']['rejected']} |",
        f"| borderline | {output['summary']['borderline']} |",
        f"| accept | {output['summary']['accepted']} |",
        f"| needs_human | {output['summary']['needs_human']} |",
        f"",
        f"## Решения по замечаниям",
        f"",
    ]

    for d in output["decisions"]:
        f = findings_by_id.get(d["finding_id"], {})
        icon = {"reject": "X", "accept": "V", "borderline": "?", "needs_human": "~"}.get(d["llm_decision"], "?")
        lines.append(f"### [{icon}] {d['finding_id']} -> {d['llm_decision'].upper()}")
        if f.get("severity"):
            lines.append(f"**Severity:** {f['severity']}")
        if f.get("problem"):
            lines.append(f"**Замечание:** {f['problem'][:200]}")
        if d.get("human_taxonomy_reason"):
            lines.append(f"**Причина:** `{d['human_taxonomy_reason']}`")
        if d.get("explanation"):
            lines.append(f"**Объяснение:** {d['explanation']}")
        lines.append(f"**Уверенность:** {d['confidence']}")
        if d.get("kb_examples_used"):
            lines.append(f"**KB-примеры:** {', '.join(d['kb_examples_used'])}")
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="KB-augmented findings validator")
    parser.add_argument("project_dir", nargs="?", help="Path to project directory")
    parser.add_argument("--discipline", help="Filter by discipline code (e.g. EOM)")
    parser.add_argument("--all", action="store_true", help="Run on all projects")
    parser.add_argument("--dry-run", action="store_true", help="No LLM, just show KB matches")
    parser.add_argument("--model", default="", help="Override model (e.g. anthropic/claude-3.5-haiku)")
    args = parser.parse_args()

    if args.model:
        os.environ["KB_GATE_MODEL"] = args.model

    print("Загружаю KB...")
    gate = KBGate.from_env()
    stats = gate._retriever.stats()
    print(f"KB загружена: {stats['total']} записей (rejected={stats['rejected']}, accepted={stats['accepted']})")
    print()

    projects_root = ROOT / "projects"
    results_summary = []

    if args.all or args.discipline:
        pattern = f"**/{args.discipline}/*" if args.discipline else "**/*"
        project_dirs = []
        for findings_file in sorted(projects_root.glob(pattern + "/_output/03_findings.json")):
            project_dirs.append(findings_file.parent.parent)
        print(f"Найдено проектов: {len(project_dirs)}")
        for pd in project_dirs:
            r = validate_project(pd, gate, dry_run=args.dry_run)
            results_summary.append(r)
    elif args.project_dir:
        pd = Path(args.project_dir)
        if not pd.is_absolute():
            pd = ROOT / pd
        r = validate_project(pd, gate, dry_run=args.dry_run)
        results_summary.append(r)
        if not args.dry_run and "decisions" in r:
            print()
            print("=== Отклоненные замечания ===")
            findings_path = pd / "_output" / "03_findings.json"
            if findings_path.exists():
                with open(findings_path) as f:
                    fdata = json.load(f)
                fmap = {f.get("id"): f for f in fdata.get("findings", [])}
                for d in r["decisions"]:
                    if d["llm_decision"] == "reject":
                        f = fmap.get(d["finding_id"], {})
                        print(f"\n  {d['finding_id']} [{d.get('human_taxonomy_reason', '?')}]")
                        print(f"  Замечание: {f.get('problem', '')[:120]}")
                        print(f"  Причина: {d.get('explanation', '')[:200]}")
                        print(f"  Уверенность: {d['confidence']}")
    else:
        parser.print_help()
        sys.exit(1)

    if len(results_summary) > 1:
        total_r = sum(r.get("summary", {}).get("rejected", 0) for r in results_summary if "summary" in r)
        total_f = sum(r.get("total_findings", 0) for r in results_summary)
        print(f"\nИтого: {total_f} замечаний, {total_r} отклонено ({100*total_r//max(total_f,1)}%)")


if __name__ == "__main__":
    main()
