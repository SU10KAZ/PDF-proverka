"""
kb_gate.py
----------
KB-augmented validator gate - uses Claude CLI (claude -p --max-turns 1).

Pipeline:
    03_findings.json
        -> KBRetriever (retrieve similar expert decisions from decisions_log.json)
        -> build_prompt (inject KB examples + findings into prompt template)
        -> call_claude_cli (claude -p --max-turns 1 --output-format json)
        -> parse_response
        -> KBGateResult

Run standalone via: scripts/validate_findings_kb.py

Env / config:
    CLAUDE_CLI path resolved from backend.app.core.config
    KB_GATE_MODEL  - optional, default: claude-sonnet-5
    KB_GATE_BATCH_SIZE - optional, default: 8
    KB_GATE_TOP_K  - optional, default: 5
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .kb_retriever import KBRetriever, SimilarDecision

_PROMPT_PATH = Path(__file__).parent / "prompts" / "kb_augmented.ru.md"

_VALID_DECISIONS = {"accept", "reject", "borderline", "needs_human"}
_VALID_TAXONOMY = {
    "visual_or_ocr_misread", "duplicate_or_already_covered",
    "wrong_norm_context", "acceptable_design_solution",
    "not_functionally_significant", "value_already_correct",
    "already_resolved_by_project_note", "false_positive_due_to_missing_context",
    "requirement_not_mandatory", "other",
}

REJECT_CONFIDENCE_THRESHOLD = 0.75
_DEFAULT_MODEL = "claude-sonnet-5"
_CLAUDE_CLI_DEFAULT = "/home/coder/.local/bin/claude"


def _get_claude_cli() -> str:
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent.parent.parent))
        from backend.app.core.config import get_claude_cli
        return get_claude_cli()
    except Exception:
        return _CLAUDE_CLI_DEFAULT


@dataclass
class KBGateDecision:
    finding_id: str
    llm_decision: str
    human_taxonomy_reason: Optional[str]
    explanation: Optional[str]
    confidence: float
    kb_examples_used: list
    evidence_checked: bool
    raw_llm: Optional[dict] = None


@dataclass
class KBGateResult:
    decisions: list = field(default_factory=list)
    model_used: str = ""
    total_input: int = 0
    rejected: int = 0
    accepted: int = 0
    borderline: int = 0
    needs_human: int = 0
    errors: int = 0
    elapsed_sec: float = 0.0


def _as_text(value, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _format_kb_examples(examples: list, max_chars: int = 2500) -> str:
    parts = []
    total = 0
    for ex in examples:
        block = (
            "[" + _as_text(ex.decision_id, "?") + "] "
            + _as_text(ex.section) + " / " + _as_text(ex.category) + " / " + _as_text(ex.severity) + "\n"
            + "Замечание: " + _as_text(ex.summary)[:150] + "\n"
            + "Решение: отклонено экспертом\n"
            + "Причина: " + _as_text(ex.expert_reason)[:250]
        )
        if total + len(block) > max_chars:
            break
        parts.append(block)
        total += len(block)
    return "\n---\n".join(parts) if parts else "(нет похожих экспертных решений)"


def _format_finding(f: dict) -> str:
    lines = [
        "finding_id: " + _as_text(f.get("id"), "?"),
        "severity: " + _as_text(f.get("severity")),
        "category: " + _as_text(f.get("category")),
        "problem: " + _as_text(f.get("problem", f.get("description", "")))[:300],
    ]
    if f.get("norm"):
        lines.append("norm: " + _as_text(f.get("norm"))[:120])
    if f.get("solution"):
        lines.append("solution: " + _as_text(f.get("solution"))[:150])
    if f.get("grounding_level"):
        lines.append("grounding: " + _as_text(f.get("grounding_level")))
    return "\n".join(lines)


def _build_prompt(template: str, findings: list, retriever, top_k: int):
    all_examples = []
    example_map = {}
    seen = set()

    for f in findings:
        fid = str(f.get("id", "?"))
        examples = retriever.find_similar(f, top_k=top_k)
        example_map[fid] = [ex.decision_id for ex in examples]
        for ex in examples:
            if ex.decision_id not in seen:
                seen.add(ex.decision_id)
                all_examples.append(ex)

    kb_block = _format_kb_examples(all_examples[:top_k * 3])
    findings_block = "\n\n".join(
        "### Замечание " + str(i + 1) + "\n```\n" + _format_finding(f) + "\n```"
        for i, f in enumerate(findings)
    )
    prompt = template.replace("{{KB_EXAMPLES}}", kb_block).replace("{{FINDINGS_BATCH}}", findings_block)
    return prompt, example_map


def _call_claude_cli(prompt: str, model: str, timeout: int = 180) -> str:
    """Call claude -p with prompt via stdin, return response text."""
    claude_bin = _get_claude_cli()

    cmd = [
        claude_bin, "-p",
        "--model", model,
        "--allowedTools", "none",
        "--output-format", "json",
        "--max-turns", "1",
    ]

    # Запуск вне репозитория, чтобы не подгружались project CLAUDE.md / hooks /
    # memory. Корень вычисляется (TMPDIR-aware), а не задан литералом: на
    # воркере общий `/tmp` — это запись мимо каталога попытки.
    from backend.app.core.config import clean_cli_cwd_root

    clean_cwd = clean_cli_cwd_root()
    os.makedirs(clean_cwd, exist_ok=True)

    clean_env = {k: v for k, v in os.environ.items() if k in (
        "HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL", "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_USE_BEDROCK",
    )}

    result = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=clean_cwd,
        env=clean_env,
    )

    if result.returncode != 0 and not result.stdout:
        raise RuntimeError("Claude CLI error: " + (result.stderr or "")[:300])

    raw = result.stdout.strip()
    if not raw:
        raise RuntimeError("Claude CLI returned empty output. stderr: " + (result.stderr or "")[:200])

    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            if data.get("is_error"):
                errors = data.get("errors", [])
                raise RuntimeError("Claude CLI error: " + str(errors)[:200])
            # Extract text from result field
            text = data.get("result", "")
            if text:
                return text
    except json.JSONDecodeError:
        pass

    return raw


def _json_array_from_text(text: str) -> list:
    text = text.strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("decisions", "items", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "[":
            continue
        try:
            data, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            return data

    m = re.search(r"\[.*\]", text, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    return []


def _coerce_confidence(value) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        confidence = 0.5
    return max(0.0, min(1.0, confidence))


def _parse_response(text: str, example_map: dict, expected_ids: Optional[set[str]] = None) -> list:
    items = _json_array_from_text(text)
    if not items:
        return []

    decisions = []
    seen_ids = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        fid = str(item.get("finding_id", "")).strip()
        if not fid or fid in seen_ids:
            continue
        if expected_ids is not None and fid not in expected_ids:
            continue
        seen_ids.add(fid)

        decision = item.get("llm_decision", "borderline")
        if decision not in _VALID_DECISIONS:
            decision = "borderline"

        confidence = _coerce_confidence(item.get("confidence", 0.5))
        if decision == "reject" and confidence < REJECT_CONFIDENCE_THRESHOLD:
            decision = "borderline"

        reason = item.get("human_taxonomy_reason")
        if reason and reason not in _VALID_TAXONOMY:
            reason = "other"

        raw_examples = item.get("kb_examples_used", [])
        allowed_examples = set(example_map.get(fid, []))
        if isinstance(raw_examples, list):
            used_examples = [str(x) for x in raw_examples if str(x) in allowed_examples]
        else:
            used_examples = []
        if not used_examples:
            used_examples = example_map.get(fid, [])

        decisions.append(KBGateDecision(
            finding_id=fid,
            llm_decision=decision,
            human_taxonomy_reason=reason,
            explanation=item.get("explanation"),
            confidence=confidence,
            kb_examples_used=used_examples,
            evidence_checked=bool(item.get("evidence_checked", False)),
            raw_llm=item,
        ))
    return decisions


def _missing_decision(finding: dict) -> KBGateDecision:
    return KBGateDecision(
        finding_id=str(finding.get("id", "?")),
        llm_decision="needs_human",
        human_taxonomy_reason=None,
        explanation="KB-агент не получил корректное решение от Claude CLI; нужна ручная проверка.",
        confidence=0.0,
        kb_examples_used=[],
        evidence_checked=False,
    )


class KBGate:
    """KB-augmented LLM validator gate using Claude CLI."""

    def __init__(self, retriever, model: str = _DEFAULT_MODEL,
                 batch_size: int = 8, top_k: int = 5) -> None:
        self._retriever = retriever
        self._model = model
        self._batch_size = batch_size
        self._top_k = top_k
        self._template = _PROMPT_PATH.read_text(encoding="utf-8")

    @classmethod
    def from_env(cls, kb_path=None) -> "KBGate":
        model = os.environ.get("KB_GATE_MODEL", _DEFAULT_MODEL)
        batch_size = int(os.environ.get("KB_GATE_BATCH_SIZE", "8"))
        top_k = int(os.environ.get("KB_GATE_TOP_K", "5"))
        retriever = KBRetriever.from_path(Path(kb_path)) if kb_path else KBRetriever.from_default()
        return cls(retriever=retriever, model=model, batch_size=batch_size, top_k=top_k)

    def validate(self, findings: list, section: str = "") -> KBGateResult:
        t0 = time.time()
        result = KBGateResult(model_used=self._model, total_input=len(findings))
        enriched = [{**f, "section": f.get("section", section)} for f in findings]

        for i in range(0, len(enriched), self._batch_size):
            batch = enriched[i: i + self._batch_size]
            try:
                prompt, example_map = _build_prompt(self._template, batch, self._retriever, self._top_k)
                raw = _call_claude_cli(prompt, self._model)
                expected_ids = {str(f.get("id", "?")) for f in batch}
                batch_decisions = _parse_response(raw, example_map, expected_ids=expected_ids)
                decided_ids = {d.finding_id for d in batch_decisions}
                missing = [f for f in batch if str(f.get("id", "?")) not in decided_ids]
                if missing:
                    result.errors += len(missing)
                    batch_decisions.extend(_missing_decision(f) for f in missing)
                result.decisions.extend(batch_decisions)
                for d in batch_decisions:
                    if d.llm_decision == "reject":
                        result.rejected += 1
                    elif d.llm_decision == "accept":
                        result.accepted += 1
                    elif d.llm_decision == "borderline":
                        result.borderline += 1
                    else:
                        result.needs_human += 1
            except Exception as exc:
                result.errors += 1
                for f in batch:
                    result.decisions.append(KBGateDecision(
                        finding_id=str(f.get("id", "?")),
                        llm_decision="needs_human",
                        human_taxonomy_reason=None,
                        explanation="Claude CLI error: " + str(exc),
                        confidence=0.0,
                        kb_examples_used=[],
                        evidence_checked=False,
                    ))
                    result.needs_human += 1

        result.elapsed_sec = round(time.time() - t0, 2)
        return result
