#!/usr/bin/env python3
"""Benchmark Codex subscription vision models on 10 GPT-reviewed Alia blocks.

Existing GPT/OpenRouter Stage 02 findings are the findings baseline. A paid
OpenRouter GPT-5.4 call creates the missing block-level optimization baseline.
Project artifacts are read-only; every experiment artifact is written under
``comparison/codex_subscription_vision_benchmark``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import tempfile
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    build_block_user_text,
    build_system_prompt,
    get_enrichment,
    load_page_text,
)
from backend.app.services.common.process_runner import run_command
from backend.app.services.llm.codex_runner import find_codex_cli
from backend.app.services.llm.llm_runner import make_image_content, run_llm
from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
)
from backend.scripts.run_stage02_codex_block_ab import (
    BlockCandidate,
    collect_candidates,
    extract_codex_tokens,
    load_json,
    safe_part,
    select_balanced,
    write_json,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_ROOT = REPO_ROOT / "comparison" / "codex_subscription_vision_benchmark"
DEFAULT_MODELS = ("gpt-5.4", "gpt-5.6-sol")

FINDING_SCHEMA = {
    "type": "object",
    "properties": {
        "severity": {"type": "string"},
        "category": {"type": "string"},
        "finding": {"type": "string"},
        "value_found": {"type": "string"},
        "recommendation": {"type": "string"},
    },
    "required": ["severity", "category", "finding", "value_found", "recommendation"],
    "additionalProperties": False,
}
OPT_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "enum": ["cheaper_analog", "faster_install", "simpler_design", "lifecycle"],
        },
        "current": {"type": "string"},
        "proposed": {"type": "string"},
        "evidence": {"type": "string"},
        "estimated_effect": {"type": "string"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
    },
    "required": ["type", "current", "proposed", "evidence", "estimated_effect", "confidence"],
    "additionalProperties": False,
}
COMBINED_SCHEMA = {
    "type": "object",
    "properties": {
        "findings": {"type": "array", "items": FINDING_SCHEMA},
        "optimizations": {"type": "array", "items": OPT_SCHEMA},
    },
    "required": ["findings", "optimizations"],
    "additionalProperties": False,
}
GPT_OPT_SCHEMA = {
    "type": "object",
    "properties": {"optimizations": {"type": "array", "items": OPT_SCHEMA}},
    "required": ["optimizations"],
    "additionalProperties": False,
}
JUDGE_SCHEMA = {
    "type": "object",
    "properties": {
        "reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "verdict": {
                        "type": "string",
                        "enum": ["supported", "needs_context", "invalid"],
                    },
                    "reason": {"type": "string"},
                    "visible_evidence": {"type": "string"},
                },
                "required": ["candidate_id", "verdict", "reason", "visible_evidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["reviews"],
    "additionalProperties": False,
}

CONCEPT_PATTERNS: dict[str, tuple[str, ...]] = {
    "placeholder": ("placeholder", "xxx", "?", "условн", "незаполн", "шаблон"),
    "marking": ("марк", "обознач", "позици", "расшифров", "идентиф"),
    "dimension": ("размер", "габарит", "привяз", "диаметр", "сечен", "отметк"),
    "reference": ("ссылк", "смежн", "номер лист", "узел", "чертеж"),
    "load": ("нагруз", "несущ", "грузопод", "усили"),
    "reinforcement": ("арматур", "анкер", "защитн слой", "стерж", "сетк"),
    "ventilation": ("вентиляц", "воздух", "расход", "клапан", "cav"),
    "electrical": ("кабел", "автомат", "ток", "кз", "заземл", "питан", "pe"),
    "fire": ("пожар", "огнестой", "дым", "спз"),
    "vendor": ("производител", "бренд", "аналог", "замен"),
    "unification": ("унифиц", "типоразмер", "номенклатур", "стандартиз"),
    "installation": ("монтаж", "заводск", "модул", "сборн", "трудозатрат"),
    "lifecycle": ("эксплуатац", "энерго", "сервис", "ремонт", "жизненн"),
}
STOP_WORDS = {
    "блок", "лист", "проект", "данный", "данные", "указать", "проверить",
    "предусмотреть", "отсутствует", "требуется", "необходимо", "система",
    "решение", "предложение", "использовать", "заменить", "который", "для",
    "или", "при", "что", "это", "как", "без", "есть", "может", "нельзя",
}


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def normalize(value: Any) -> str:
    text = str(value or "").lower().replace("ё", "е").replace("×", "x").replace("х", "x")
    return re.sub(r"\s+", " ", re.sub(r"[^0-9a-zа-я?.-]+", " ", text)).strip()


def item_text(item: dict[str, Any], kind: str) -> str:
    keys = (
        ("severity", "category", "finding", "value_found", "recommendation")
        if kind == "findings"
        else ("type", "current", "proposed", "evidence", "estimated_effect")
    )
    return normalize(" ".join(str(item.get(key) or "") for key in keys))


def content_tokens(text: str) -> set[str]:
    return {
        token for token in text.split()
        if token not in STOP_WORDS and (len(token) >= 4 or any(ch.isdigit() for ch in token))
    }


def entities(text: str) -> set[str]:
    return {
        token.strip(".-") for token in text.split()
        if any(ch.isdigit() for ch in token) and len(token.strip(".-")) >= 2
    }


def concepts(text: str) -> set[str]:
    return {
        name for name, patterns in CONCEPT_PATTERNS.items()
        if any(pattern in text for pattern in patterns)
    }


def overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def semantic_score(left: dict[str, Any], right: dict[str, Any], kind: str) -> float:
    lt, rt = item_text(left, kind), item_text(right, kind)
    lexical = overlap(content_tokens(lt), content_tokens(rt))
    entity = overlap(entities(lt), entities(rt))
    concept = overlap(concepts(lt), concepts(rt))
    if not concepts(lt) and not concepts(rt):
        concept = lexical
    score = 0.45 * concept + 0.35 * entity + 0.20 * lexical
    if kind == "optimizations" and left.get("type") == right.get("type"):
        score += 0.08
    return round(min(score, 1.0), 3)


def greedy_match(
    baseline: list[dict[str, Any]], candidate: list[dict[str, Any]], kind: str,
    threshold: float = 0.38,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pairs = []
    for bi, left in enumerate(baseline):
        for ci, right in enumerate(candidate):
            score = semantic_score(left, right, kind)
            if score >= threshold:
                pairs.append((score, bi, ci))
    pairs.sort(reverse=True)
    used_b: set[int] = set()
    used_c: set[int] = set()
    matches = []
    for score, bi, ci in pairs:
        if bi in used_b or ci in used_c:
            continue
        used_b.add(bi)
        used_c.add(ci)
        matches.append({"score": score, "baseline_index": bi, "candidate_index": ci})
    missed = [item for idx, item in enumerate(baseline) if idx not in used_b]
    extra = [item for idx, item in enumerate(candidate) if idx not in used_c]
    return matches, missed, extra


def build_context(candidate: BlockCandidate) -> dict[str, Any]:
    project_info = load_version_project_info(candidate.version_dir)
    discipline = str(project_info.get("section") or candidate.discipline or "_generic")
    graph = load_json(candidate.latest_dir / "document_graph.json")
    enrichment, enrichment_source = get_enrichment(
        candidate.version_dir, {}, project_info, candidate.block_id
    )
    page_text = load_page_text(graph, candidate.page)
    return {
        "discipline": discipline,
        "system_prompt": build_system_prompt(discipline, extended=True),
        "user_text": build_block_user_text(
            candidate.block_id, candidate.page, enrichment, page_text
        ),
        "enrichment_source": enrichment_source,
        "page_text_chars": len(page_text or ""),
    }


def combined_prompt(candidate: BlockCandidate, context: dict[str, Any]) -> str:
    return f"""You are independently reviewing one construction drawing block.

Perform TWO separate passes over the attached image and supplied context.

PASS A — FINDINGS:
- Find concrete contradictions, unsafe/incomplete design data, coordination risks,
  placeholders and documentation defects visible in this block.
- Do not invent missing facts. Use severity: КРИТИЧЕСКОЕ, ЭКСПЛУАТАЦИОННОЕ,
  РЕКОМЕНДАТЕЛЬНОЕ or ПРОВЕРИТЬ ПО СМЕЖНЫМ.
- value_found must contain exact visible evidence when possible.

PASS B — VALUE ENGINEERING OPTIMIZATIONS:
- Start independently from the image, not from the findings list.
- Suggest only cost, installation-time, design-simplification or lifecycle improvements.
- A correction of an error, missing dimension, typo or absent label is NOT optimization.
- Every proposal must name the current visible solution and evidence in this block.
- Return an empty optimizations array when this isolated block gives no defensible OPT.

Return exactly one JSON object matching this schema:
{json.dumps(COMBINED_SCHEMA, ensure_ascii=False, indent=2)}

Document: {candidate.object_slug}/{candidate.discipline}/{candidate.document}/{candidate.version}
Block: {candidate.block_id}; PDF page: {candidate.page}

<PRODUCTION_STAGE02_SYSTEM_PROMPT>
{context['system_prompt']}
</PRODUCTION_STAGE02_SYSTEM_PROMPT>

<BLOCK_CONTEXT>
{context['user_text']}
</BLOCK_CONTEXT>
""".strip()


def gpt_optimization_messages(
    candidate: BlockCandidate, context: dict[str, Any], image_path: Path,
    profile: str,
) -> list[dict[str, Any]]:
    exploratory = ""
    if profile == "exploratory":
        exploratory = """

Use this block as a value-engineering candidate search, not as a final approval.
Systematically inspect: material/equipment analogs, reduction of type sizes,
unification of repeated elements, prefabricated assemblies, fewer installation
operations, simplified routing/layout, controls and lifecycle energy/service cost.
Lack of prices is not a reason to omit a technically grounded candidate: use
estimated_effect="requires commercial/engineering calculation" and medium/low
confidence. Do not force a candidate when the visible solution gives no basis.
""".rstrip()
    text = f"""Analyze one construction drawing block for value engineering.

Find only defensible optimization opportunities: cheaper equivalent, faster
installation, simpler design/unification, or lifecycle improvement. Do not turn
errors, missing dimensions, typos, norm violations or absent labels into OPT.
Use only the attached image and supplied block context. Each proposal must cite
visible evidence. An empty array is correct when the block has no independent OPT.
{exploratory}

Document: {candidate.discipline}/{candidate.document}; block {candidate.block_id}; page {candidate.page}

<BLOCK_CONTEXT>
{context['user_text']}
</BLOCK_CONTEXT>
""".strip()
    return [
        {"role": "system", "content": "You are a senior construction value-engineering reviewer."},
        {"role": "user", "content": [
            {"type": "text", "text": text},
            make_image_content(image_path, detail="high"),
        ]},
    ]


def codex_optimization_prompt(
    candidate: BlockCandidate, context: dict[str, Any], profile: str,
) -> str:
    exploratory = ""
    if profile == "exploratory":
        exploratory = """
Systematically inspect material/equipment analogs, reduction of type sizes,
unification of repeated elements, prefabricated assemblies, fewer installation
operations, simplified routing/layout, controls and lifecycle energy/service cost.
Lack of prices is not a reason to omit a grounded candidate: set
estimated_effect to "requires commercial/engineering calculation" and use
medium/low confidence. Do not force a candidate without visible evidence.
    """.strip()


def optimization_judge_prompt(
    candidate: BlockCandidate, context: dict[str, Any], anonymized: list[dict[str, Any]],
) -> str:
    return f"""You are the independent quality judge for value-engineering proposals.

Inspect the attached construction drawing block and its text context. Review EVERY
anonymous candidate below. Candidate letters do not identify their source.

Verdicts:
- supported: the visible block supports a real value-engineering direction; normal
  engineering/calculation validation may still be required.
- needs_context: plausible optimization, but this isolated block does not contain
  enough evidence to say it applies to this project.
- invalid: not an optimization (just an error correction), contradicts the visible
  design, invents an object/solution, or has no meaningful basis in the block.

Do not reward quantity or eloquence. Do not reject merely because prices are absent.
Return one review for every candidate_id and no unrequested candidates.

Schema:
{json.dumps(JUDGE_SCHEMA, ensure_ascii=False, indent=2)}

Document: {candidate.discipline}/{candidate.document}; block {candidate.block_id}; page {candidate.page}

<BLOCK_CONTEXT>
{context['user_text']}
</BLOCK_CONTEXT>

<ANONYMOUS_CANDIDATES>
{json.dumps(anonymized, ensure_ascii=False, indent=2)}
</ANONYMOUS_CANDIDATES>
""".strip()
    return f"""You are a senior construction value-engineering reviewer.

Analyze this single drawing block ONLY for independent value-engineering
optimizations: cheaper equivalent, faster installation, simpler design or
unification, and lifecycle improvement. Do not spend attention on producing an
audit findings list. An error correction, missing dimension, typo, absent label
or norm violation is NOT optimization. Every item must cite visible evidence.
{exploratory}

Return exactly one JSON object matching this schema:
{json.dumps(GPT_OPT_SCHEMA, ensure_ascii=False, indent=2)}

Document: {candidate.discipline}/{candidate.document}; block {candidate.block_id}; page {candidate.page}

<BLOCK_CONTEXT>
{context['user_text']}
</BLOCK_CONTEXT>
""".strip()


def parse_json(text: str, required_key: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get(required_key), list):
            return data, None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", text or "")
    if match:
        try:
            data = json.loads(match.group(0))
            if isinstance(data, dict) and isinstance(data.get(required_key), list):
                return data, "parsed_from_fallback"
        except json.JSONDecodeError:
            pass
    return None, f"valid JSON with {required_key}[] not found"


async def run_codex(
    *, prompt: str, image_path: Path, model: str, effort: str,
    timeout: int, project_id: str, required_key: str = "findings",
) -> dict[str, Any]:
    cli = find_codex_cli()
    if not cli:
        return {"ok": False, "error": "codex_cli_not_found"}
    fd, out_name = tempfile.mkstemp(prefix="codex_vision_benchmark_", suffix=".json")
    os.close(fd)
    out_file = Path(out_name)
    cmd = [
        cli, "exec", "--ephemeral", "--ignore-user-config", "--ignore-rules",
        "--skip-git-repo-check", "--sandbox", "read-only", "--model", model,
        "-c", f'model_reasoning_effort="{effort}"', "--image", str(image_path),
        "-C", str(REPO_ROOT), "-o", str(out_file), "-",
    ]
    started = time.monotonic()
    try:
        exit_code, stdout, stderr = await run_command(
            cmd, input_text=prompt, timeout=timeout, cwd=str(REPO_ROOT),
            project_id=project_id,
            env_overrides={key: None for key in os.environ if key.startswith("CLAUDE")},
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        final_text = out_file.read_text(encoding="utf-8", errors="replace") if out_file.is_file() else ""
        combined = "\n".join(part for part in (stdout, stderr, final_text) if part)
        parsed, parse_error = parse_json(final_text or stdout, required_key)
        return {
            "ok": exit_code == 0 and parsed is not None,
            "exit_code": exit_code,
            "error": parse_error,
            "duration_ms": duration_ms,
            "tokens_used": extract_codex_tokens(combined),
            "data": parsed,
            "raw_text": final_text,
            "combined_tail": combined[-2000:],
        }
    finally:
        try:
            out_file.unlink()
        except OSError:
            pass


async def run_gpt_opt(
    candidate: BlockCandidate, context: dict[str, Any], image_path: Path,
    timeout: int, run_id: str, profile: str,
) -> dict[str, Any]:
    result = await run_llm(
        stage="optimization",
        messages=gpt_optimization_messages(candidate, context, image_path, profile),
        model_override="openai/gpt-5.4",
        strict_schema=GPT_OPT_SCHEMA,
        schema_name="block_optimizations",
        temperature=0.1,
        timeout=timeout,
        max_retries=2,
        max_tokens_override=12000,
        project_id=candidate.document,
        version_id=candidate.version,
        job_id=run_id,
        source="codex_subscription_vision_benchmark",
    )
    data = result.json_data if isinstance(result.json_data, dict) else None
    return {
        "ok": not result.is_error and data is not None,
        "error": result.error_message,
        "data": data,
        "raw_text": result.text,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "duration_ms": result.duration_ms,
        "model": result.model,
        "profile": profile,
    }


def traceability_rate(items: list[dict[str, Any]], kind: str) -> float | None:
    if not items:
        return None
    key = "value_found" if kind == "findings" else "evidence"
    return round(sum(bool(str(item.get(key) or "").strip()) for item in items) / len(items), 3)


def correction_like_rate(items: list[dict[str, Any]]) -> float | None:
    if not items:
        return None
    correction = ("исправ", "указать", "добавить", "уточнить", "устранить", "заполнить")
    value = ("эконом", "сократ", "унифиц", "аналог", "монтаж", "модул", "типоразмер", "эксплуатац")
    count = 0
    for item in items:
        text = item_text(item, "optimizations")
        if any(word in text for word in correction) and not any(word in text for word in value):
            count += 1
    return round(count / len(items), 3)


def clean_token_count(value: Any) -> int | None:
    """Repair old parser values where the following ISO year was appended."""
    if not isinstance(value, int) or value < 0:
        return None
    text = str(value)
    if value > 1_000_000 and text.endswith("2026"):
        repaired = text[:-4]
        return int(repaired) if repaired else None
    return value


def aggregate(results: list[dict[str, Any]], models: list[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "blocks": len(results),
        "gpt_findings_total": sum(len(row["gpt_findings"]) for row in results),
        "gpt_optimizations_total": sum(len(row["gpt_optimizations"]) for row in results),
        "gpt_optimization_cost_usd": round(sum(float(row.get("gpt_cost_usd") or 0) for row in results), 6),
        "models": {},
    }
    for model in models:
        rows = [row for row in results if (row.get("codex") or {}).get(model, {}).get("ok")]
        findings = [item for row in rows for item in row["codex"][model]["findings"]]
        opt_rows = []
        for row in rows:
            dedicated = (row.get("codex_dedicated_opt") or {}).get(model) or {}
            opt_rows.append(dedicated if dedicated.get("ok") else row["codex"][model])
        opts = [item for data in opt_rows for item in data.get("optimizations", [])]
        f_matches = sum(len(row["codex"][model]["finding_comparison"]["matches"]) for row in rows)
        o_matches = sum(
            len(data.get("optimization_comparison", {}).get("matches", []))
            for data in opt_rows
        )
        gpt_f = sum(len(row["gpt_findings"]) for row in rows)
        gpt_o = sum(len(row["gpt_optimizations"]) for row in rows)
        tokens = [clean_token_count(row["codex"][model].get("tokens_used")) for row in rows]
        tokens = [value for value in tokens if value is not None]
        summary["models"][model] = {
            "blocks_ok": len(rows),
            "findings_total": len(findings),
            "gpt_findings_matched": f_matches,
            "finding_recall_vs_gpt": round(f_matches / gpt_f, 3) if gpt_f else None,
            "finding_precision_vs_gpt": round(f_matches / len(findings), 3) if findings else None,
            "finding_f1_vs_gpt": (
                round(2 * (f_matches / gpt_f) * (f_matches / len(findings))
                      / ((f_matches / gpt_f) + (f_matches / len(findings))), 3)
                if gpt_f and findings and f_matches else None
            ),
            "finding_traceability_rate": traceability_rate(findings, "findings"),
            "optimizations_total": len(opts),
            "gpt_optimizations_matched": o_matches,
            "optimization_overlap_vs_gpt": round(o_matches / gpt_o, 3) if gpt_o else None,
            "optimization_traceability_rate": traceability_rate(opts, "optimizations"),
            "optimization_correction_like_rate": correction_like_rate(opts),
            "optimization_mode": (
                "dedicated" if any(
                    ((row.get("codex_dedicated_opt") or {}).get(model) or {}).get("ok")
                    for row in rows
                ) else "combined_with_findings"
            ),
            "tokens_total": sum(tokens) if tokens else None,
            "dedicated_optimization_tokens_total": sum(
                int(clean_token_count(dedicated.get("tokens_used")) or 0)
                for row in rows
                for dedicated in [((row.get("codex_dedicated_opt") or {}).get(model) or {})]
                if dedicated.get("ok")
            ) or None,
            "dedicated_optimization_duration_sec_total": round(sum(
                int(dedicated.get("duration_ms") or 0)
                for row in rows
                for dedicated in [((row.get("codex_dedicated_opt") or {}).get(model) or {})]
                if dedicated.get("ok")
            ) / 1000, 1),
            "duration_sec_total": round(sum(row["codex"][model]["duration_ms"] for row in rows) / 1000, 1),
        }

    if len(models) >= 2:
        finding_union = finding_intersection = finding_baseline = 0
        opt_union = opt_intersection = opt_baseline = 0
        for row in results:
            finding_sets = [
                {
                    match["baseline_index"]
                    for match in (row.get("codex", {}).get(model, {}).get("finding_comparison", {}).get("matches") or [])
                }
                for model in models
            ]
            opt_sets = [
                {
                    match["baseline_index"]
                    for match in (
                        ((row.get("codex_dedicated_opt") or {}).get(model) or row.get("codex", {}).get(model, {}))
                        .get("optimization_comparison", {}).get("matches") or []
                    )
                }
                for model in models
            ]
            finding_baseline += len(row.get("gpt_findings") or [])
            opt_baseline += len(row.get("gpt_optimizations") or [])
            finding_union += len(set().union(*finding_sets))
            opt_union += len(set().union(*opt_sets))
            finding_intersection += len(set.intersection(*finding_sets)) if finding_sets else 0
            opt_intersection += len(set.intersection(*opt_sets)) if opt_sets else 0
        summary["codex_model_union"] = {
            "finding_gpt_matches": finding_union,
            "finding_recall_vs_gpt": round(finding_union / finding_baseline, 3) if finding_baseline else None,
            "finding_matches_shared_by_all_models": finding_intersection,
            "optimization_gpt_matches": opt_union,
            "optimization_recall_vs_gpt": round(opt_union / opt_baseline, 3) if opt_baseline else None,
            "optimization_matches_shared_by_all_models": opt_intersection,
        }
    judge_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"supported": 0, "needs_context": 0, "invalid": 0, "unreviewed": 0}
    )
    for row in results:
        for source, reviews in (row.get("optimization_judgement") or {}).items():
            for review in reviews or []:
                verdict = str(review.get("verdict") or "unreviewed")
                if verdict not in judge_counts[source]:
                    verdict = "unreviewed"
                judge_counts[source][verdict] += 1
    if judge_counts:
        summary["optimization_judge"] = {}
        for source, counts in sorted(judge_counts.items()):
            total = sum(counts.values())
            summary["optimization_judge"][source] = {
                **counts,
                "total": total,
                "supported_rate": round(counts["supported"] / total, 3) if total else None,
                "usable_rate": round(
                    (counts["supported"] + counts["needs_context"]) / total, 3
                ) if total else None,
            }
        for model in models:
            supported_matched = supported_unique = 0
            for row in results:
                dedicated = (row.get("codex_dedicated_opt") or {}).get(model) or {}
                items = dedicated.get("optimizations") or []
                matched_indexes = {
                    match["candidate_index"]
                    for match in (dedicated.get("optimization_comparison", {}).get("matches") or [])
                }
                for review in (row.get("optimization_judgement") or {}).get(model, []):
                    if review.get("verdict") != "supported":
                        continue
                    try:
                        item_index = items.index(review.get("item"))
                    except ValueError:
                        continue
                    if item_index in matched_indexes:
                        supported_matched += 1
                    else:
                        supported_unique += 1
            if model in summary["optimization_judge"]:
                summary["optimization_judge"][model]["supported_matching_gpt"] = supported_matched
                summary["optimization_judge"][model]["supported_unique_vs_gpt"] = supported_unique
    return summary


def write_report(run_dir: Path, results: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        "# Codex subscription vision benchmark",
        "",
        "> GPT/OpenRouter is a comparison baseline, not expert ground truth. Semantic matches are deterministic candidates for manual review.",
        "",
        f"- Blocks: {summary['blocks']}",
        f"- Existing GPT findings: {summary['gpt_findings_total']}",
        f"- Paid GPT optimizations: {summary['gpt_optimizations_total']}",
        f"- Paid GPT optimization cost: ${summary['gpt_optimization_cost_usd']:.4f}",
        "",
        "## Models",
        "",
        "| Model | OK | Findings | Finding recall | Finding evidence | OPT | OPT overlap | OPT evidence | Correction-like OPT | Tokens |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for model, stats in summary["models"].items():
        lines.append(
            f"| {model} | {stats['blocks_ok']} | {stats['findings_total']} | "
            f"{stats['finding_recall_vs_gpt']} | {stats['finding_traceability_rate']} | "
            f"{stats['optimizations_total']} | {stats['optimization_overlap_vs_gpt']} | "
            f"{stats['optimization_traceability_rate']} | {stats['optimization_correction_like_rate']} | "
            f"{stats['tokens_total']} |"
        )
    if summary.get("optimization_judge"):
        lines.extend([
            "", "## Blind OPT judge (gpt-5.5/high)", "",
            "| Source | Supported | Needs context | Invalid | Supported rate | Usable rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ])
        for source, stats in summary["optimization_judge"].items():
            lines.append(
                f"| {source} | {stats['supported']} | {stats['needs_context']} | "
                f"{stats['invalid']} | {stats['supported_rate']} | {stats['usable_rate']} |"
            )
    lines.extend(["", "## Blocks", ""])
    for row in results:
        candidate = row["candidate"]
        lines.append(
            f"### {candidate['discipline']} / {candidate['document']} / {candidate['block_id']}"
        )
        lines.append(
            f"- GPT baseline: findings={len(row['gpt_findings'])}, optimizations={len(row['gpt_optimizations'])}"
        )
        for model, data in row["codex"].items():
            fcmp = data.get("finding_comparison") or {}
            ocmp = data.get("optimization_comparison") or {}
            lines.append(
                f"- {model}: ok={data.get('ok')}, findings={len(data.get('findings') or [])}, "
                f"finding matches={len(fcmp.get('matches') or [])}, OPT={len(data.get('optimizations') or [])}, "
                f"OPT matches={len(ocmp.get('matches') or [])}, tokens={data.get('tokens_used')}"
            )
            dedicated = (row.get("codex_dedicated_opt") or {}).get(model) or {}
            if dedicated:
                dcmp = dedicated.get("optimization_comparison") or {}
                lines.append(
                    f"  - dedicated OPT: ok={dedicated.get('ok')}, "
                    f"items={len(dedicated.get('optimizations') or [])}, "
                    f"matches={len(dcmp.get('matches') or [])}, tokens={dedicated.get('tokens_used')}"
                )
        lines.append("")
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


async def run_dedicated_optimizations(
    *, run_dir: Path, results: list[dict[str, Any]], candidates: list[BlockCandidate],
    contexts: dict[str, dict[str, Any]], models: list[str], args: argparse.Namespace,
) -> list[dict[str, Any]]:
    """Add fair, optimization-only Codex passes to an existing benchmark run."""
    semaphore = asyncio.Semaphore(max(1, args.parallel))
    by_label = {candidate.label: candidate for candidate in candidates}

    async def run_one(index: int, row: dict[str, Any], model: str):
        candidate = by_label[row["candidate"]["label"]]
        context = contexts[candidate.label]
        block_dir = Path(row["paths"]["block_dir"])
        image_path = Path(row["paths"]["image"])
        effort = args.baseline_effort if model == "gpt-5.4" else args.effort
        path = block_dir / f"codex_{safe_part(model)}_optimizations_dedicated.json"
        if args.resume and path.is_file():
            saved = load_json(path)
        else:
            print(
                f"[vision-benchmark] OPT-only {index:02d}/{len(results)} "
                f"{model}/{effort} {candidate.block_id}", flush=True,
            )
            async with semaphore:
                raw = await run_codex(
                    prompt=codex_optimization_prompt(candidate, context, args.gpt_opt_profile),
                    image_path=image_path, model=model, effort=effort,
                    timeout=args.timeout, project_id=candidate.document,
                    required_key="optimizations",
                )
            opts = [
                item for item in ((raw.get("data") or {}).get("optimizations") or [])
                if isinstance(item, dict)
            ]
            saved = {
                **{key: value for key, value in raw.items() if key not in {"data", "raw_text"}},
                "optimizations": opts,
                "reasoning_effort": effort,
                "raw_text": raw.get("raw_text", ""),
            }
        saved["tokens_used"] = clean_token_count(saved.get("tokens_used"))
        gpt_opts = row.get("gpt_optimizations") or []
        matches, missed, extra = greedy_match(gpt_opts, saved.get("optimizations") or [], "optimizations")
        saved["optimization_comparison"] = {
            "matches": matches, "missed": missed, "extra": extra,
        }
        write_json(path, saved)
        return index, model, saved

    completed = await asyncio.gather(*(
        run_one(index, row, model)
        for index, row in enumerate(results, start=1)
        for model in models
    ))
    for index, model, saved in completed:
        results[index - 1].setdefault("codex_dedicated_opt", {})[model] = saved
    write_json(run_dir / "results.json", results)
    return results


async def run_blind_judge(
    *, run_dir: Path, results: list[dict[str, Any]], candidates: list[BlockCandidate],
    contexts: dict[str, dict[str, Any]], args: argparse.Namespace,
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(max(1, args.parallel))
    by_label = {candidate.label: candidate for candidate in candidates}

    async def judge_row(index: int, row: dict[str, Any]):
        candidate = by_label[row["candidate"]["label"]]
        context = contexts[candidate.label]
        block_dir = Path(row["paths"]["block_dir"])
        image_path = Path(row["paths"]["image"])
        source_items = {
            "gpt_openrouter": row.get("gpt_optimizations") or [],
            "gpt-5.4": ((row.get("codex_dedicated_opt") or {}).get("gpt-5.4") or {}).get("optimizations") or [],
            "gpt-5.6-sol": ((row.get("codex_dedicated_opt") or {}).get("gpt-5.6-sol") or {}).get("optimizations") or [],
        }
        sources = list(source_items)
        shift = index % len(sources)
        rotated = sources[shift:] + sources[:shift]
        source_codes = {source: chr(ord("A") + position) for position, source in enumerate(rotated)}
        candidate_map: dict[str, dict[str, Any]] = {}
        anonymized = []
        for source, items in source_items.items():
            code = source_codes[source]
            for item_index, item in enumerate(items, start=1):
                candidate_id = f"{code}-{item_index:02d}"
                candidate_map[candidate_id] = {"source": source, "item": item}
                anonymized.append({"candidate_id": candidate_id, **item})
        path = block_dir / f"optimization_blind_judge_{safe_part(args.judge_model)}.json"
        if args.resume and path.is_file():
            saved = load_json(path)
        elif not anonymized:
            saved = {"ok": True, "reviews": [], "candidate_map": candidate_map}
        else:
            print(
                f"[vision-benchmark] JUDGE {index:02d}/{len(results)} "
                f"{args.judge_model}/{args.judge_effort} {candidate.block_id} "
                f"candidates={len(anonymized)}",
                flush=True,
            )
            async with semaphore:
                raw = await run_codex(
                    prompt=optimization_judge_prompt(candidate, context, anonymized),
                    image_path=image_path, model=args.judge_model,
                    effort=args.judge_effort, timeout=args.timeout,
                    project_id=candidate.document, required_key="reviews",
                )
            reviews = [
                review for review in ((raw.get("data") or {}).get("reviews") or [])
                if isinstance(review, dict)
            ]
            saved = {
                **{key: value for key, value in raw.items() if key not in {"data", "raw_text"}},
                "reviews": reviews,
                "candidate_map": candidate_map,
                "raw_text": raw.get("raw_text", ""),
            }
        write_json(path, saved)
        by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
        reviewed_ids = set()
        for review in saved.get("reviews") or []:
            candidate_id = str(review.get("candidate_id") or "")
            mapped = candidate_map.get(candidate_id)
            if not mapped:
                continue
            reviewed_ids.add(candidate_id)
            by_source[mapped["source"]].append({
                **review, "item": mapped["item"],
            })
        for candidate_id, mapped in candidate_map.items():
            if candidate_id not in reviewed_ids:
                by_source[mapped["source"]].append({
                    "candidate_id": candidate_id,
                    "verdict": "unreviewed",
                    "reason": "judge omitted candidate",
                    "visible_evidence": "",
                    "item": mapped["item"],
                })
        return index, dict(by_source), saved.get("ok", False)

    judged = await asyncio.gather(*(
        judge_row(index, row) for index, row in enumerate(results, start=1)
    ))
    for index, by_source, ok in judged:
        results[index - 1]["optimization_judgement"] = by_source
        results[index - 1]["optimization_judge_ok"] = ok
    write_json(run_dir / "results.json", results)
    return results


async def async_main(args: argparse.Namespace) -> int:
    if not find_codex_cli():
        raise RuntimeError("Codex CLI not found")
    models = [part.strip() for part in args.models.split(",") if part.strip()]
    candidates = select_balanced(
        collect_candidates(), limit=args.limit, object_filter=args.object,
        discipline_filter=None, document_filter=None,
    )
    if len(candidates) < args.limit:
        raise RuntimeError(f"Only {len(candidates)} candidates selected, requested {args.limit}")

    run_id = utc_stamp()
    run_dir = Path(args.run_dir) if args.run_dir else OUT_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    contexts = {candidate.label: build_context(candidate) for candidate in candidates}
    write_json(run_dir / "selection.json", {
        "run_id": run_id,
        "object_filter": args.object,
        "models": models,
        "reasoning_effort": {
            model: args.baseline_effort if model == "gpt-5.4" else args.effort
            for model in models
        },
        "gpt_optimization_profile": args.gpt_opt_profile,
        "selected": [{
            "label": candidate.label,
            "discipline": candidate.discipline,
            "document": candidate.document,
            "version": candidate.version,
            "block_id": candidate.block_id,
            "page": candidate.page,
            "gpt_findings_count": len(candidate.gpt_findings),
            "image_path": str(candidate.image_path),
        } for candidate in candidates],
    })
    print(f"[vision-benchmark] run_dir={run_dir}", flush=True)

    if args.dedicated_opt_only:
        results_path = run_dir / "results.json"
        if not results_path.is_file():
            raise RuntimeError("--dedicated-opt-only requires an existing results.json")
        results = load_json(results_path)
        results = await run_dedicated_optimizations(
            run_dir=run_dir, results=results, candidates=candidates,
            contexts=contexts, models=models, args=args,
        )
        summary = aggregate(results, models)
        write_json(run_dir / "summary.json", summary)
        write_report(run_dir, results, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return 0

    if args.judge_only:
        results_path = run_dir / "results.json"
        if not results_path.is_file():
            raise RuntimeError("--judge-only requires an existing results.json")
        results = load_json(results_path)
        results = await run_blind_judge(
            run_dir=run_dir, results=results, candidates=candidates,
            contexts=contexts, args=args,
        )
        summary = aggregate(results, models)
        write_json(run_dir / "summary.json", summary)
        write_report(run_dir, results, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return 0

    semaphore = asyncio.Semaphore(max(1, args.parallel))
    results: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        block_dir = run_dir / "blocks" / f"{index:02d}_{safe_part(candidate.block_id)}"
        block_dir.mkdir(parents=True, exist_ok=True)
        image_copy = block_dir / candidate.image_path.name
        shutil.copy2(candidate.image_path, image_copy)
        context = contexts[candidate.label]
        write_json(block_dir / "context.json", context)
        write_json(block_dir / "gpt_findings_existing.json", {"findings": candidate.gpt_findings})

        gpt_path = block_dir / f"gpt_optimizations_openrouter_{args.gpt_opt_profile}.json"
        legacy_gpt_path = block_dir / "gpt_optimizations_openrouter.json"
        if (
            args.gpt_opt_profile == "conservative"
            and not gpt_path.is_file()
            and legacy_gpt_path.is_file()
        ):
            shutil.copy2(legacy_gpt_path, gpt_path)
        if args.resume and gpt_path.is_file():
            gpt_result = load_json(gpt_path)
        else:
            print(f"[vision-benchmark] {index:02d}/{len(candidates)} GPT OPT {candidate.label}", flush=True)
            async with semaphore:
                gpt_result = await run_gpt_opt(
                    candidate, context, image_copy, args.timeout, run_id,
                    args.gpt_opt_profile,
                )
            write_json(gpt_path, gpt_result)
        gpt_opts = [
            item for item in ((gpt_result.get("data") or {}).get("optimizations") or [])
            if isinstance(item, dict)
        ]
        row = {
            "candidate": {
                "label": candidate.label,
                "discipline": candidate.discipline,
                "document": candidate.document,
                "version": candidate.version,
                "block_id": candidate.block_id,
                "page": candidate.page,
            },
            "paths": {"block_dir": str(block_dir), "image": str(image_copy)},
            "gpt_findings": candidate.gpt_findings,
            "gpt_optimizations": gpt_opts,
            "gpt_optimization_ok": bool(gpt_result.get("ok")),
            "gpt_cost_usd": float(gpt_result.get("cost_usd") or 0),
            "codex": {},
        }

        async def run_model(model: str) -> tuple[str, dict[str, Any]]:
            model_path = block_dir / f"codex_{safe_part(model)}.json"
            if args.resume and model_path.is_file():
                saved = load_json(model_path)
                findings = [item for item in saved.get("findings", []) if isinstance(item, dict)]
                opts = [item for item in saved.get("optimizations", []) if isinstance(item, dict)]
                f_matches, f_missed, f_extra = greedy_match(
                    candidate.gpt_findings, findings, "findings"
                )
                o_matches, o_missed, o_extra = greedy_match(
                    gpt_opts, opts, "optimizations"
                )
                saved["finding_comparison"] = {
                    "matches": f_matches, "missed": f_missed, "extra": f_extra,
                }
                saved["optimization_comparison"] = {
                    "matches": o_matches, "missed": o_missed, "extra": o_extra,
                }
                return model, saved
            effort = args.baseline_effort if model == "gpt-5.4" else args.effort
            print(
                f"[vision-benchmark] {index:02d}/{len(candidates)} Codex "
                f"{model}/{effort} {candidate.block_id}",
                flush=True,
            )
            async with semaphore:
                raw = await run_codex(
                    prompt=combined_prompt(candidate, context), image_path=image_copy,
                    model=model, effort=effort, timeout=args.timeout,
                    project_id=candidate.document,
                )
            data = raw.get("data") or {}
            findings = [item for item in data.get("findings", []) if isinstance(item, dict)]
            opts = [item for item in data.get("optimizations", []) if isinstance(item, dict)]
            f_matches, f_missed, f_extra = greedy_match(candidate.gpt_findings, findings, "findings")
            o_matches, o_missed, o_extra = greedy_match(gpt_opts, opts, "optimizations")
            result = {
                **{key: value for key, value in raw.items() if key not in {"data", "raw_text"}},
                "findings": findings,
                "optimizations": opts,
                "reasoning_effort": effort,
                "finding_comparison": {"matches": f_matches, "missed": f_missed, "extra": f_extra},
                "optimization_comparison": {"matches": o_matches, "missed": o_missed, "extra": o_extra},
                "raw_text": raw.get("raw_text", ""),
            }
            write_json(model_path, result)
            return model, result

        model_results = await asyncio.gather(*(run_model(model) for model in models))
        row["codex"] = dict(model_results)
        results.append(row)
        write_json(run_dir / "results.partial.json", results)

    summary = aggregate(results, models)
    write_json(run_dir / "results.json", results)
    write_json(run_dir / "summary.json", summary)
    write_report(run_dir, results, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--object", default="214_Alia")
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS))
    parser.add_argument("--effort", choices=("low", "medium", "high", "xhigh"), default="xhigh")
    parser.add_argument(
        "--baseline-effort", choices=("low", "medium", "high", "xhigh"),
        default="medium", help="Reasoning effort for the current gpt-5.4 baseline",
    )
    parser.add_argument("--parallel", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument(
        "--gpt-opt-profile", choices=("conservative", "exploratory"),
        default="conservative",
    )
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dedicated-opt-only", action="store_true")
    parser.add_argument("--judge-only", action="store_true")
    parser.add_argument("--judge-model", default="gpt-5.5")
    parser.add_argument(
        "--judge-effort", choices=("low", "medium", "high", "xhigh"), default="high"
    )
    return parser.parse_args()


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
