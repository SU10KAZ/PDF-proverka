#!/usr/bin/env python3
"""A/B Codex subscription models as the Stage 01 dual-detector reviewer.

The benchmark reuses the frozen 10-block Alia vision corpus. GPT/OpenRouter and
Codex detector findings stay fixed; only the semantic reviewer model changes.
All output is isolated under comparison/stage01_judge_model_ab/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_analysis.dual_review import (
    REVIEW_SYSTEM_PROMPT,
    ensure_detector_refs,
    normalize_review_payload,
)
from backend.scripts.run_codex_subscription_vision_benchmark import run_codex


ROOT = Path(__file__).resolve().parents[2]
SOURCE_RUN = ROOT / "comparison/codex_subscription_vision_benchmark/20260711T113639Z"
OUT_ROOT = ROOT / "comparison/stage01_judge_model_ab"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def safe_part(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("_") or "item"


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def prompt_finding(item: dict[str, Any]) -> dict[str, Any]:
    model = str(item.get("_detector_model") or "").lower()
    detector = "codex" if model.startswith("codex/") else "gpt_openrouter"
    return {
        "ref": item.get("_detector_ref"),
        "detector": detector,
        "severity": item.get("severity"),
        "category": item.get("category"),
        "finding": item.get("finding"),
        "norm_quote": item.get("norm_quote"),
        "value_found": item.get("value_found"),
        "recommendation": item.get("recommendation"),
    }


def detector_findings(block_dir: Path) -> list[dict[str, Any]]:
    gpt = load_json(block_dir / "gpt_findings_existing.json").get("findings") or []
    codex = load_json(block_dir / "codex_gpt-5.4.json").get("findings") or []
    combined = [
        {**item, "_detector_model": "openai/gpt-5.4"}
        for item in gpt if isinstance(item, dict)
    ]
    combined.extend(
        {**item, "_detector_model": "codex/gpt-5.4"}
        for item in codex if isinstance(item, dict)
    )
    return ensure_detector_refs(combined)


def reviewer_prompt(block_id: str, page: int, context: str, findings: list[dict[str, Any]]) -> str:
    payload = {
        "block_id": block_id,
        "page": page,
        "gap_search_enabled": True,
        "block_context": context,
        "detector_findings": [prompt_finding(item) for item in findings],
    }
    return (
        REVIEW_SYSTEM_PROMPT.rstrip()
        + "\n\nВход пользователя:\n"
        + json.dumps(payload, ensure_ascii=False)
    )


def arbiter_prompt(
    *, block_id: str, page: int, context: str, findings: list[dict[str, Any]],
    anonymous_candidates: list[dict[str, Any]],
) -> str:
    return f"""Ты независимый старший проверяющий проектной документации.

Слепо оцени два варианта работы СУДЬИ после независимых детекторов GPT и Codex.
Ты не знаешь модели кандидатов. Проверь по изображению, контексту и исходным
замечаниям:

1. correct_relationships: сколько заявленных пар действительно описывают один факт;
2. incorrect_relationships: сколько пар ошибочно объединяют разные проблемы;
3. obvious_pairs_missed: сколько очевидных смысловых пар кандидат не связал;
4. supported_gaps: сколько новых gap_findings подтверждены и не повторяют детекторы;
5. duplicate_gaps: сколько gap_findings повторяют исходные замечания;
6. unsupported_gaps: сколько gap_findings не подтверждены данным блоком/контекстом.

overall_score от 0 до 100 оценивает именно качество судьи: корректность связей,
полноту сопоставления и точность новых gap_findings. Не награждай за большее число
gap_findings само по себе. preferred_candidate: A, B или tie.

Верни строго JSON без Markdown:
{{
  "candidate_reviews": [
    {{
      "candidate_id": "A",
      "correct_relationships": 0,
      "incorrect_relationships": 0,
      "obvious_pairs_missed": 0,
      "supported_gaps": 0,
      "duplicate_gaps": 0,
      "unsupported_gaps": 0,
      "overall_score": 0,
      "reason": "краткое обоснование"
    }}
  ],
  "preferred_candidate": "A|B|tie",
  "summary": "краткий итог"
}}

Блок {block_id}, страница {page}.

<BLOCK_CONTEXT>
{context}
</BLOCK_CONTEXT>

<DETECTOR_FINDINGS>
{json.dumps([prompt_finding(item) for item in findings], ensure_ascii=False, indent=2)}
</DETECTOR_FINDINGS>

<ANONYMOUS_JUDGES>
{json.dumps(anonymous_candidates, ensure_ascii=False, indent=2)}
</ANONYMOUS_JUDGES>
""".strip()


def model_specs(raw: str) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    for part in raw.split(","):
        model, _, effort = part.strip().partition(":")
        if model:
            specs.append((model, effort or "high"))
    return specs


def public_candidate(saved: dict[str, Any]) -> dict[str, Any]:
    report = saved.get("report") or {}
    return {
        "relationships": report.get("relationships") or [],
        "unmatched_refs": report.get("unmatched_refs") or [],
        "gap_findings": saved.get("gap_findings") or [],
        "gap_search": report.get("gap_search") or {},
    }


async def run_candidate(
    *, block_dir: Path, block_id: str, page: int, context: str,
    findings: list[dict[str, Any]], image_path: Path, model: str, effort: str,
    timeout: int, semaphore: asyncio.Semaphore, resume: bool,
) -> dict[str, Any]:
    out_path = block_dir / f"judge_{safe_part(model)}_{safe_part(effort)}.json"
    if resume and out_path.is_file():
        return load_json(out_path)
    async with semaphore:
        raw = await run_codex(
            prompt=reviewer_prompt(block_id, page, context, findings),
            image_path=image_path,
            model=model,
            effort=effort,
            timeout=timeout,
            project_id=f"judge-ab-{block_id}",
            required_key="relationships",
        )
    payload = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    normalized = normalize_review_payload(
        payload,
        findings,
        reviewer_model=f"codex/{model}",
        gap_search_enabled=True,
        status="ok" if raw.get("ok") else "failed",
        review_error=str(raw.get("error") or ""),
    )
    saved = {
        "ok": bool(raw.get("ok")),
        "model": model,
        "effort": effort,
        "duration_ms": int(raw.get("duration_ms") or 0),
        "tokens_used": raw.get("tokens_used"),
        "error": raw.get("error"),
        "report": normalized.get("report") or {},
        "gap_findings": normalized.get("gap_findings") or [],
        "raw_payload": payload,
        "raw_text": raw.get("raw_text") or "",
    }
    write_json(out_path, saved)
    return saved


async def run_arbiter(
    *, block_dir: Path, block_id: str, page: int, context: str,
    findings: list[dict[str, Any]], image_path: Path,
    candidates: dict[str, dict[str, Any]], index: int,
    model: str, effort: str, timeout: int, semaphore: asyncio.Semaphore,
    resume: bool,
) -> dict[str, Any]:
    out_path = block_dir / f"blind_arbiter_{safe_part(model)}_{safe_part(effort)}.json"
    if resume and out_path.is_file():
        return load_json(out_path)
    sources = list(candidates)
    if index % 2:
        sources.reverse()
    candidate_map = {chr(ord("A") + pos): source for pos, source in enumerate(sources)}
    anonymous = [
        {"candidate_id": code, **public_candidate(candidates[source])}
        for code, source in candidate_map.items()
    ]
    async with semaphore:
        raw = await run_codex(
            prompt=arbiter_prompt(
                block_id=block_id, page=page, context=context,
                findings=findings, anonymous_candidates=anonymous,
            ),
            image_path=image_path,
            model=model,
            effort=effort,
            timeout=timeout,
            project_id=f"judge-arbiter-{block_id}",
            required_key="candidate_reviews",
        )
    payload = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    reviews = []
    for review in payload.get("candidate_reviews") or []:
        if not isinstance(review, dict):
            continue
        code = str(review.get("candidate_id") or "")
        source = candidate_map.get(code)
        if source:
            reviews.append({**review, "source": source})
    preferred_code = str(payload.get("preferred_candidate") or "tie")
    saved = {
        "ok": bool(raw.get("ok")),
        "model": model,
        "effort": effort,
        "duration_ms": int(raw.get("duration_ms") or 0),
        "tokens_used": raw.get("tokens_used"),
        "error": raw.get("error"),
        "candidate_map": candidate_map,
        "candidate_reviews": reviews,
        "preferred_source": candidate_map.get(preferred_code, "tie"),
        "summary": payload.get("summary") or "",
        "raw_text": raw.get("raw_text") or "",
    }
    write_json(out_path, saved)
    return saved


def aggregate(rows: list[dict[str, Any]], specs: list[tuple[str, str]]) -> dict[str, Any]:
    summary: dict[str, Any] = {"blocks": len(rows), "models": {}}
    for model, effort in specs:
        key = f"{model}:{effort}"
        candidates = [row["candidates"].get(key) or {} for row in rows]
        reviews = [
            review
            for row in rows
            for review in (row.get("arbiter") or {}).get("candidate_reviews") or []
            if review.get("source") == key
        ]
        def total(field: str) -> int:
            return sum(int(review.get(field) or 0) for review in reviews)
        scores = [float(review.get("overall_score") or 0) for review in reviews]
        summary["models"][key] = {
            "blocks_ok": sum(bool(item.get("ok")) for item in candidates),
            "relationships": sum(len((item.get("report") or {}).get("relationships") or []) for item in candidates),
            "gap_findings": sum(len(item.get("gap_findings") or []) for item in candidates),
            "duration_sec_total": round(sum(int(item.get("duration_ms") or 0) for item in candidates) / 1000, 1),
            "tokens_total": sum(int(item.get("tokens_used") or 0) for item in candidates),
            "arbiter_score_mean": round(sum(scores) / len(scores), 1) if scores else None,
            "correct_relationships": total("correct_relationships"),
            "incorrect_relationships": total("incorrect_relationships"),
            "obvious_pairs_missed": total("obvious_pairs_missed"),
            "supported_gaps": total("supported_gaps"),
            "duplicate_gaps": total("duplicate_gaps"),
            "unsupported_gaps": total("unsupported_gaps"),
            "wins": sum((row.get("arbiter") or {}).get("preferred_source") == key for row in rows),
        }
    summary["ties"] = sum((row.get("arbiter") or {}).get("preferred_source") == "tie" for row in rows)
    summary["arbiter"] = {
        "blocks_ok": sum(bool((row.get("arbiter") or {}).get("ok")) for row in rows),
    }
    return summary


def report_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stage 01 judge model A/B",
        "",
        "| Model | OK | Score | Correct pairs | Wrong pairs | Missed pairs | Supported gaps | Duplicate gaps | Unsupported gaps | Wins | Time | Tokens |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for model, data in summary.get("models", {}).items():
        lines.append(
            f"| {model} | {data['blocks_ok']} | {data['arbiter_score_mean']} | "
            f"{data['correct_relationships']} | {data['incorrect_relationships']} | "
            f"{data['obvious_pairs_missed']} | {data['supported_gaps']} | "
            f"{data['duplicate_gaps']} | {data['unsupported_gaps']} | {data['wins']} | "
            f"{data['duration_sec_total']}s | {data['tokens_total']} |"
        )
    lines.extend([
        "",
        f"Blind-arbiter ties: {summary.get('ties', 0)}.",
        "",
        "The arbiter is an independent model-assisted comparison, not human ground truth.",
    ])
    return "\n".join(lines) + "\n"


async def async_main(args: argparse.Namespace) -> int:
    specs = model_specs(args.models)
    source_blocks = sorted((SOURCE_RUN / "blocks").iterdir())[: args.limit]
    if len(source_blocks) < args.limit:
        raise RuntimeError(f"Only {len(source_blocks)} source blocks available")
    run_dir = Path(args.run_dir) if args.run_dir else OUT_ROOT / utc_stamp()
    run_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max(1, args.parallel))

    async def one(index: int, source_dir: Path) -> dict[str, Any]:
        context_data = load_json(source_dir / "context.json")
        findings = detector_findings(source_dir)
        image_path = next(source_dir.glob("block_*.png"))
        block_id = image_path.stem.removeprefix("block_")
        gpt_findings = load_json(source_dir / "gpt_findings_existing.json").get("findings") or []
        page = 0
        if gpt_findings:
            page = int(gpt_findings[0].get("page") or 0)
        if not page:
            match = re.search(r"страница PDF\s+(\d+)", str(context_data.get("user_text") or ""))
            page = int(match.group(1)) if match else 0
        block_dir = run_dir / "blocks" / source_dir.name
        block_dir.mkdir(parents=True, exist_ok=True)
        (block_dir / image_path.name).write_bytes(image_path.read_bytes())
        write_json(block_dir / "detector_findings.json", findings)

        print(f"[judge-ab] {index:02d}/{len(source_blocks)} {block_id}: candidate judges", flush=True)
        candidate_results = await asyncio.gather(*(
            run_candidate(
                block_dir=block_dir, block_id=block_id, page=page,
                context=str(context_data.get("user_text") or ""), findings=findings,
                image_path=image_path, model=model, effort=effort,
                timeout=args.timeout, semaphore=semaphore, resume=args.resume,
            )
            for model, effort in specs
        ))
        candidates = {
            f"{model}:{effort}": result
            for (model, effort), result in zip(specs, candidate_results, strict=True)
        }
        print(f"[judge-ab] {index:02d}/{len(source_blocks)} {block_id}: blind arbiter", flush=True)
        arbiter = await run_arbiter(
            block_dir=block_dir, block_id=block_id, page=page,
            context=str(context_data.get("user_text") or ""), findings=findings,
            image_path=image_path, candidates=candidates, index=index,
            model=args.arbiter_model, effort=args.arbiter_effort,
            timeout=args.timeout, semaphore=semaphore, resume=args.resume,
        )
        return {
            "index": index,
            "block_id": block_id,
            "source_dir": str(source_dir),
            "gpt_findings": sum(str(item.get("_detector_model") or "").startswith("openai/") for item in findings),
            "codex_findings": sum(str(item.get("_detector_model") or "").startswith("codex/") for item in findings),
            "candidates": candidates,
            "arbiter": arbiter,
        }

    rows = list(await asyncio.gather(*(one(index, path) for index, path in enumerate(source_blocks, start=1))))
    rows.sort(key=lambda row: row["index"])
    summary = aggregate(rows, specs)
    summary["source_run"] = str(SOURCE_RUN)
    summary["candidate_models"] = [
        {"model": model, "reasoning_effort": effort}
        for model, effort in specs
    ]
    summary["arbiter"].update({
        "model": args.arbiter_model,
        "reasoning_effort": args.arbiter_effort,
        "blind_order_alternated": True,
    })
    write_json(run_dir / "results.json", rows)
    write_json(run_dir / "summary.json", summary)
    (run_dir / "report.md").write_text(report_markdown(summary), encoding="utf-8")
    print(json.dumps({"run_dir": str(run_dir), **summary}, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", default="gpt-5.4:medium,gpt-5.6-sol:xhigh")
    parser.add_argument("--arbiter-model", default="gpt-5.5")
    parser.add_argument("--arbiter-effort", default="high")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--parallel", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--run-dir")
    parser.add_argument("--resume", action="store_true")
    return asyncio.run(async_main(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
