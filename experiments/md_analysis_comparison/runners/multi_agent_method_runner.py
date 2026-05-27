"""Multi-agent runner — parallel Sonnet agents + Opus critic + Python synth.

Pipeline:
  R1: N parallel Sonnet agents (normative, calculations, contradictions,
      completeness, cross_discipline, safety). Each runs `claude -p`
      as a separate subprocess.
  R2: Opus critic sees the full MD plus every agent's partial findings,
      assigns verdicts, flags duplicates and missed issues.
  R3: Opus reviewer applies critic verdicts, merges duplicates, renumbers,
      sorts, optionally adds missed findings, emits the final list.
  R4: Python deduplication safety-net (final pass, no LLM).

Everything goes through `claude -p` subprocess on the Claude Code subscription.
No OpenAI, no Gemini, no local LLMs.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners._common import run_claude, read_md  # noqa: E402
from runners.unified_output_schema import RunResult, coerce_finding, Finding  # noqa: E402

DEFAULT_AGENTS = [
    "normative",
    "calculations",
    "contradictions",
    "completeness",
    "cross_discipline",
    "safety",
]

BASE_PROMPT_PATH = cfg.PROMPTS_DIR / "system" / "00_base.md"
AGENT_PROMPTS_DIR = cfg.PROMPTS_DIR / "agents"
CRITIC_PROMPT_PATH = cfg.PROMPTS_DIR / "critic" / "critic_task.md"
REVIEWER_PROMPT_PATH = cfg.PROMPTS_DIR / "reviewer" / "final_review_task.md"


@dataclass
class AgentRun:
    name: str
    findings: list[Finding]
    duration: float
    raw_json: dict | None
    error: str | None


def _build_agent_prompt(agent: str, md_content: str, discipline: str) -> str:
    base = BASE_PROMPT_PATH.read_text(encoding="utf-8")
    role = (AGENT_PROMPTS_DIR / f"{agent}.md").read_text(encoding="utf-8")
    prompt = (
        "=== BASE RULES ===\n"
        + base
        + "\n\n=== AGENT ROLE ===\n"
        + role
    )
    return (prompt
            .replace("{AGENT_NAME}", agent)
            .replace("{DISCIPLINE}", discipline)
            .replace("{MD_CONTENT}", md_content))


def _run_one_agent(agent: str, md_content: str, discipline: str, case_id: str) -> AgentRun:
    started = time.time()
    prompt = _build_agent_prompt(agent, md_content, discipline)
    res = run_claude(
        prompt=prompt,
        model=cfg.MODEL_SONNET,
        timeout=cfg.AGENT_TIMEOUT_SEC,
        label=f"multi_agent/{case_id}/{agent}",
    )
    duration = time.time() - started
    findings: list[Finding] = []
    error = None
    if res.parsed_json and isinstance(res.parsed_json, dict):
        applic = (res.parsed_json.get("applicability") or "applicable").lower()
        if applic == "not_applicable":
            return AgentRun(agent, [], duration, res.parsed_json, None)
        for i, f in enumerate(res.parsed_json.get("findings") or [], start=1):
            try:
                f.setdefault("source_agent", agent)
                findings.append(coerce_finding(f, i, source_agent=agent))
            except Exception as exc:
                error = f"coerce[{agent}/{i}] failed: {exc}"
    else:
        error = f"agent {agent}: no parseable JSON (exit={res.exit_code})"
    return AgentRun(agent, findings, duration, res.parsed_json, error)


def _run_critic(md_content: str, discipline: str, agent_findings: list[Finding], case_id: str) -> dict | None:
    template = CRITIC_PROMPT_PATH.read_text(encoding="utf-8")
    all_findings_json = json.dumps(
        [_finding_to_dict(f) for f in agent_findings],
        ensure_ascii=False, indent=2,
    )
    prompt = (template
              .replace("{DISCIPLINE}", discipline)
              .replace("{MD_CONTENT}", md_content)
              .replace("{ALL_FINDINGS_JSON}", all_findings_json))
    res = run_claude(
        prompt=prompt,
        model=cfg.MODEL_OPUS,
        timeout=cfg.CRITIC_TIMEOUT_SEC,
        label=f"multi_agent/{case_id}/critic",
    )
    return res.parsed_json


def _run_reviewer(md_content: str, discipline: str, agent_findings: list[Finding],
                  critic_json: dict | None, case_id: str) -> dict | None:
    template = REVIEWER_PROMPT_PATH.read_text(encoding="utf-8")
    agents_json = json.dumps([_finding_to_dict(f) for f in agent_findings], ensure_ascii=False, indent=2)
    critic_blob = json.dumps(critic_json or {}, ensure_ascii=False, indent=2)
    prompt = (template
              .replace("{DISCIPLINE}", discipline)
              .replace("{MD_CONTENT}", md_content)
              .replace("{AGENT_FINDINGS_JSON}", agents_json)
              .replace("{CRITIC_JSON}", critic_blob))
    res = run_claude(
        prompt=prompt,
        model=cfg.MODEL_OPUS,
        timeout=cfg.CRITIC_TIMEOUT_SEC,
        label=f"multi_agent/{case_id}/reviewer",
    )
    return res.parsed_json


def _finding_to_dict(f: Finding) -> dict:
    return {
        "id": f.id, "severity": f.severity, "category": f.category,
        "problem": f.problem, "description": f.description,
        "norm": f.norm, "norm_quote": f.norm_quote, "norm_confidence": f.norm_confidence,
        "recommendation": f.recommendation, "risk": f.risk,
        "evidence_quote": f.evidence_quote, "md_excerpt": f.md_excerpt,
        "discipline": f.discipline, "cross_discipline_with": f.cross_discipline_with,
        "source_agent": f.source_agent, "confidence": f.confidence,
    }


def _python_dedup_safetynet(findings: list[Finding]) -> list[Finding]:
    """Second-pass dedup in pure Python — catches anything reviewer missed.
    Group by (severity, normalized_problem_prefix). Keep first, drop rest."""
    seen: dict[tuple[str, str], Finding] = {}
    result = []
    for f in findings:
        key = (f.severity, f.problem.lower()[:80])
        if key in seen:
            continue
        seen[key] = f
        result.append(f)
    return result


def run(case_dir: Path, output_path: Path, agents: list[str] | None = None) -> RunResult:
    info = json.loads((case_dir / "case.json").read_text(encoding="utf-8"))
    case_id = info.get("id", case_dir.name)
    discipline = info.get("discipline", "MULTI")
    md_path = case_dir / info.get("md_file", "input.md")
    md = read_md(md_path)

    selected = agents or DEFAULT_AGENTS

    started = time.time()
    agent_results: list[AgentRun] = []
    with ThreadPoolExecutor(max_workers=cfg.MULTI_AGENT_PARALLELISM) as ex:
        futures = {ex.submit(_run_one_agent, a, md, discipline, case_id): a for a in selected}
        for fut in as_completed(futures):
            agent_results.append(fut.result())

    all_findings: list[Finding] = []
    errors: list[str] = []
    agents_meta = []
    for ar in agent_results:
        all_findings.extend(ar.findings)
        agents_meta.append({"agent": ar.name, "findings": len(ar.findings), "duration": round(ar.duration, 1), "error": ar.error})
        if ar.error:
            errors.append(ar.error)

    critic_json = None
    reviewer_json = None
    final_findings: list[Finding] = []

    if all_findings:
        critic_json = _run_critic(md, discipline, all_findings, case_id)
        reviewer_json = _run_reviewer(md, discipline, all_findings, critic_json, case_id)

        if reviewer_json and isinstance(reviewer_json, dict):
            for i, f in enumerate(reviewer_json.get("findings") or [], start=1):
                try:
                    final_findings.append(coerce_finding(f, i, source_agent=f.get("source_agent", "reviewer")))
                except Exception as exc:
                    errors.append(f"reviewer coerce[{i}]: {exc}")
        else:
            errors.append("reviewer returned no JSON; falling back to agent findings minus duplicates")
            final_findings = _python_dedup_safetynet(all_findings)
    else:
        errors.append("no agent produced findings; multi-agent pipeline returned empty")

    final_findings = _python_dedup_safetynet(final_findings)
    for i, f in enumerate(final_findings, start=1):
        f.id = f"F-{i:03d}"

    duration = time.time() - started
    result = RunResult(
        method="multi_agent",
        case_id=case_id,
        discipline=discipline,
        model_main=cfg.MODEL_OPUS,
        duration_sec=duration,
        findings=final_findings,
        meta={
            "md_chars": len(md),
            "agents_run": agents_meta,
            "total_agent_findings": sum(len(ar.findings) for ar in agent_results),
            "critic_summary": (critic_json or {}).get("summary") if isinstance(critic_json, dict) else None,
            "reviewer_stats": (reviewer_json or {}).get("stats") if isinstance(reviewer_json, dict) else None,
            "parallelism": cfg.MULTI_AGENT_PARALLELISM,
        },
        errors=errors,
    )
    result.save(output_path)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", required=True)
    ap.add_argument("--out", default=None)
    ap.add_argument("--agents", nargs="*", default=None,
                    help="Subset of agents to run; defaults to all.")
    args = ap.parse_args()

    case = cfg.DATASETS_DIR / args.case
    if not case.exists():
        sys.exit(f"Case not found: {case}")
    out = Path(args.out) if args.out else cfg.RESULTS_DIR / args.case / "multi_agent.json"

    result = run(case, out, agents=args.agents)
    print(f"[multi_agent] {args.case}: {len(result.findings)} findings in {result.duration_sec:.1f}s")
    if result.errors:
        print(f"  errors: {result.errors[:3]}", file=sys.stderr)
    print(f"  agents: {[a['agent']+'='+str(a['findings']) for a in result.meta['agents_run']]}")
    print(f"  saved: {out}")


if __name__ == "__main__":
    main()
