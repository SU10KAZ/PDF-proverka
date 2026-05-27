"""Shared helpers for algorithm_research runners.

Re-uses the parent stand's `run_claude` subprocess wrapper so all LLM
calls go through `claude -p` on the Claude Code subscription. Adds:

- A unified caching layer (`--skip-existing` semantics).
- Path conventions for `algorithm_research/results/<algorithm>__<prompt>/`.
- Prompt-template loading from `algorithm_research/prompt_optimization/`.
"""
from __future__ import annotations

import hashlib
import importlib.util as _ilu
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# Add parent stand for `configs` re-use. The parent has a `runners` package
# that collides with this folder's `runners`, so we import its modules by
# absolute path under unique sys.modules names.
EXP_ROOT = Path(__file__).resolve().parents[2]
if str(EXP_ROOT) not in sys.path:
    sys.path.insert(0, str(EXP_ROOT))

from configs import config as cfg  # noqa: E402


def _import_parent(module_name: str, file_relative: str):
    abs_path = EXP_ROOT / file_relative
    unique = f"_parent__{module_name}"
    if unique in sys.modules:
        return sys.modules[unique]
    spec = _ilu.spec_from_file_location(unique, abs_path)
    mod = _ilu.module_from_spec(spec)
    sys.modules[unique] = mod
    spec.loader.exec_module(mod)
    return mod


_parent_common = _import_parent("runners_common", "runners/_common.py")
_parent_schema = _import_parent("unified_output_schema", "runners/unified_output_schema.py")

run_claude = _parent_common.run_claude
read_md = _parent_common.read_md
ClaudeResult = _parent_common.ClaudeResult
Finding = _parent_schema.Finding
coerce_finding = _parent_schema.coerce_finding
RunResult = _parent_schema.RunResult

RESEARCH_ROOT = Path(__file__).resolve().parents[1]
PROMPT_BASELINE_DIR = RESEARCH_ROOT / "prompt_optimization" / "baseline_prompts"
PROMPT_V1_DIR = RESEARCH_ROOT / "prompt_optimization" / "optimized_prompts_v1"
PROMPT_V2_DIR = RESEARCH_ROOT / "prompt_optimization" / "optimized_prompts_v2"
CHECKLIST_DIR = RESEARCH_ROOT / "prompt_optimization" / "checklists"
RESULTS_DIR = RESEARCH_ROOT / "results"
LOGS_DIR = RESEARCH_ROOT / "logs"
TEMP_DIR = RESEARCH_ROOT / "temp"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

PROMPT_SET_DIRS = {
    "baseline": PROMPT_BASELINE_DIR,
    "v1": PROMPT_V1_DIR,
    "v2": PROMPT_V2_DIR,
}

DISCIPLINE_FULL_NAMES = {
    "EOM": "Электроснабжение и силовое электрооборудование",
    "OV":  "Отопление, вентиляция, кондиционирование",
    "VK":  "Внутреннее водоснабжение и канализация",
    "AR":  "Архитектурные решения",
    "KJ":  "Конструкции железобетонные",
    "KM":  "Конструкции металлические",
    "SS":  "Сети связи / слаботочные системы",
    "MULTI": "Междисциплинарный",
}


def algorithm_results_dir(algorithm: str, prompt_set: str) -> Path:
    d = RESULTS_DIR / f"{algorithm}__{prompt_set}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def result_path(algorithm: str, prompt_set: str, case_id: str) -> Path:
    return algorithm_results_dir(algorithm, prompt_set) / f"{case_id}.json"


def load_prompt(prompt_set: str, name: str) -> str:
    base = PROMPT_SET_DIRS[prompt_set]
    p = base / f"{name}.md"
    if not p.exists():
        # Fallback to baseline if v1/v2 doesn't define it.
        fallback = PROMPT_BASELINE_DIR / f"{name}.md"
        if fallback.exists():
            return fallback.read_text(encoding="utf-8")
        raise FileNotFoundError(f"Prompt {name} not found in {prompt_set}")
    return p.read_text(encoding="utf-8")


def load_checklist(discipline: str) -> str:
    d = (discipline or "").upper()
    p = CHECKLIST_DIR / f"{d}.md"
    if not p.exists():
        p = CHECKLIST_DIR / "cross_discipline.md"
    return p.read_text(encoding="utf-8")


def load_case(case_id: str) -> tuple[dict, str, Path]:
    case_dir = cfg.DATASETS_DIR / case_id
    info = json.loads((case_dir / "case.json").read_text(encoding="utf-8"))
    md_path = case_dir / info.get("md_file", "input.md")
    return info, read_md(md_path), case_dir


def hash_prompt(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]


@dataclass
class CachedDecision:
    used_cache: bool
    path: Path
    age_seconds: float | None = None


def should_skip_existing(path: Path, skip_existing: bool) -> CachedDecision:
    if skip_existing and path.exists():
        try:
            age = time.time() - path.stat().st_mtime
        except OSError:
            age = None
        return CachedDecision(used_cache=True, path=path, age_seconds=age)
    return CachedDecision(used_cache=False, path=path)


def write_result(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


def run_current_method(md: str, discipline: str, prompt_set: str, case_id: str) -> tuple[list[Finding], ClaudeResult, float]:
    prompt_tmpl = load_prompt(prompt_set, "current_method")
    prompt = (prompt_tmpl
              .replace("{DISCIPLINE}", discipline)
              .replace("{DISCIPLINE_FULL_NAME}", DISCIPLINE_FULL_NAMES.get(discipline, discipline))
              .replace("{MD_CONTENT}", md))
    started = time.time()
    res = run_claude(
        prompt=prompt, model=cfg.MODEL_OPUS,
        timeout=cfg.DEFAULT_TIMEOUT_SEC,
        label=f"algorithm_research/{case_id}/current_method_{prompt_set}",
    )
    duration = time.time() - started
    findings: list[Finding] = []
    if res.parsed_json and isinstance(res.parsed_json, dict):
        for i, f in enumerate(res.parsed_json.get("findings") or [], start=1):
            try:
                findings.append(coerce_finding(f, i, source_agent="current_method"))
            except Exception:
                continue
    return findings, res, duration


def run_lens(lens: str, md: str, discipline: str, prompt_set: str, case_id: str,
              checklist: str | None = None,
              document_type: str | None = None) -> tuple[list[Finding], ClaudeResult, float]:
    base = load_prompt(prompt_set, "00_base")
    role = load_prompt(prompt_set, lens)
    prompt = "=== BASE RULES ===\n" + base + "\n\n=== AGENT ROLE ===\n" + role
    prompt = (prompt
              .replace("{AGENT_NAME}", lens)
              .replace("{DISCIPLINE}", discipline)
              .replace("{MD_CONTENT}", md)
              .replace("{DOCUMENT_TYPE}", document_type or "full_rd"))
    if "{CHECKLIST_CONTENT}" in prompt:
        prompt = prompt.replace("{CHECKLIST_CONTENT}", checklist or "")
    started = time.time()
    res = run_claude(
        prompt=prompt, model=cfg.MODEL_SONNET,
        timeout=cfg.AGENT_TIMEOUT_SEC,
        label=f"algorithm_research/{case_id}/lens_{lens}_{prompt_set}",
    )
    duration = time.time() - started
    findings: list[Finding] = []
    if res.parsed_json and isinstance(res.parsed_json, dict):
        applic = (res.parsed_json.get("applicability") or "applicable").lower()
        if applic == "not_applicable":
            return [], res, duration
        for i, f in enumerate(res.parsed_json.get("findings") or [], start=1):
            try:
                f.setdefault("source_agent", lens)
                findings.append(coerce_finding(f, i, source_agent=lens))
            except Exception:
                continue
    return findings, res, duration


def finding_to_dict(f: Finding) -> dict:
    return {
        "id": f.id, "severity": f.severity, "category": f.category,
        "problem": f.problem, "description": f.description,
        "norm": f.norm, "norm_quote": f.norm_quote, "norm_confidence": f.norm_confidence,
        "recommendation": f.recommendation, "risk": f.risk,
        "evidence_quote": f.evidence_quote, "md_excerpt": f.md_excerpt,
        "discipline": f.discipline, "cross_discipline_with": f.cross_discipline_with,
        "source_agent": f.source_agent, "confidence": f.confidence,
    }


def run_critic(prompt_set: str, md: str, discipline: str, all_findings: list[dict], case_id: str) -> tuple[dict | None, ClaudeResult, float]:
    template = load_prompt(prompt_set, "critic")
    all_findings_json = json.dumps(all_findings, ensure_ascii=False, indent=2)
    prompt = (template
              .replace("{DISCIPLINE}", discipline)
              .replace("{MD_CONTENT}", md)
              .replace("{ALL_FINDINGS_JSON}", all_findings_json))
    started = time.time()
    res = run_claude(
        prompt=prompt, model=cfg.MODEL_OPUS,
        timeout=cfg.CRITIC_TIMEOUT_SEC,
        label=f"algorithm_research/{case_id}/critic_{prompt_set}",
    )
    duration = time.time() - started
    return res.parsed_json, res, duration


def run_reviewer(prompt_set: str, md: str, discipline: str,
                 all_findings: list[dict], critic_json: dict | None, case_id: str) -> tuple[dict | None, ClaudeResult, float]:
    template = load_prompt(prompt_set, "reviewer")
    agents_json = json.dumps(all_findings, ensure_ascii=False, indent=2)
    critic_blob = json.dumps(critic_json or {}, ensure_ascii=False, indent=2)
    prompt = (template
              .replace("{DISCIPLINE}", discipline)
              .replace("{MD_CONTENT}", md)
              .replace("{AGENT_FINDINGS_JSON}", agents_json)
              .replace("{CRITIC_JSON}", critic_blob))
    started = time.time()
    res = run_claude(
        prompt=prompt, model=cfg.MODEL_OPUS,
        timeout=cfg.CRITIC_TIMEOUT_SEC,
        label=f"algorithm_research/{case_id}/reviewer_{prompt_set}",
    )
    duration = time.time() - started
    return res.parsed_json, res, duration


def make_run_result(method: str, case_id: str, discipline: str,
                     findings: list[Finding], duration: float,
                     meta: dict, errors: list[str]) -> dict:
    """Build a serialisable RunResult-shaped dict."""
    r = RunResult(
        method=method,
        case_id=case_id,
        discipline=discipline,
        model_main=cfg.MODEL_OPUS,
        duration_sec=duration,
        findings=findings,
        meta=meta,
        errors=errors,
    )
    return r.to_dict()
