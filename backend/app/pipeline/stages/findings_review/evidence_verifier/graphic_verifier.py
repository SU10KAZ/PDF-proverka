"""Graphic block verification via local vision LLM (ngrok) — EV2 perception + policy."""
from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Optional

from .context_loader import FindingContext
from .parse import EVDecision, missing_decision

_PROMPT_PATH = Path(__file__).parent / "prompts" / "verify_graphic.ru.md"
_DEFAULT_MODEL = os.environ.get("EV_GRAPHIC_MODEL", "") or os.environ.get(
    "STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b"
)
_CONTRA = {"yes", "no", "cannot_tell"}


def _format_finding(finding: dict, section: str = "") -> str:
    parts = [
        f"ID: {finding.get('id', '?')}",
        f"Раздел: {section or finding.get('section', '?')}",
        f"Критичность: {finding.get('severity', '?')}",
        f"Категория: {finding.get('category', '?')}",
        f"Лист: {finding.get('sheet', '?')}",
        f"Норма: {finding.get('norm', '')}",
        f"Замечание: {finding.get('problem') or finding.get('description') or finding.get('summary', '')}",
        f"Рекомендация: {finding.get('solution') or finding.get('recommendation', '')}",
        f"grounding_level: {finding.get('grounding_level', '')}",
    ]
    return "\n".join(p for p in parts if p)


def _build_prompt(ctx: FindingContext) -> str:
    template = _PROMPT_PATH.read_text(encoding="utf-8")
    gemma_parts = []
    for b in ctx.blocks:
        if b.gemma_text:
            gemma_parts.append(f"### {b.block_id}\n{b.gemma_text[:3000]}")
    return (
        template.replace("{{FINDING}}", _format_finding(ctx.finding, ctx.section))
        .replace("{{GEMMA_TEXT}}", "\n\n".join(gemma_parts) or "(нет OCR)")
    )


def _parse_perception(text: str) -> dict | None:
    text = (text or "").strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj[0]
    except json.JSONDecodeError:
        pass
    dec = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                obj, _ = dec.raw_decode(text[i:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                continue
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return None


def _perception_to_decision(finding_id: str, obj: dict | None, block_ids: list) -> EVDecision:
    if not obj:
        return missing_decision({"id": finding_id}, verification_path="graphic")
    contradicts = str(obj.get("contradicts_finding", "cannot_tell")).strip().lower()
    if contradicts not in _CONTRA:
        contradicts = "cannot_tell"
    quote = str(obj.get("evidence_quote", "")).strip()
    if contradicts == "yes" and not quote:
        contradicts = "cannot_tell"

    if contradicts == "yes":
        decision, conf, reason = "reject", 0.8, "Чертёж опровергает замечание (есть цитата)."
    elif contradicts == "no":
        decision, conf, reason = "accept", 0.7, "Чертёж подтверждает проблему."
    else:
        decision, conf, reason = "needs_human", 0.4, "По блоку нельзя проверить."

    note = str(obj.get("note", "")).strip()
    value = str(obj.get("value_on_drawing", "")).strip()
    explanation = reason
    if value:
        explanation = f"{reason} {value[:200]}"
    if quote:
        explanation = f"{explanation} Цитата: «{quote[:120]}»"
    if note:
        explanation = f"{explanation} {note[:120]}"

    return EVDecision(
        finding_id=finding_id,
        llm_decision=decision,
        human_taxonomy_reason="visual_or_ocr_misread" if decision == "reject" else None,
        explanation=explanation,
        confidence=conf,
        verification_path="graphic",
        block_ids_used=block_ids,
        evidence_checked=True,
        raw_llm=obj,
    )


async def verify_graphic_async(
    ctx: FindingContext,
    *,
    model: Optional[str] = None,
) -> EVDecision:
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local

    model = (model or _DEFAULT_MODEL).strip()
    primary = next((b for b in ctx.blocks if b.png_path and b.png_path.is_file()), None)
    if not primary:
        return missing_decision(
            ctx.finding,
            verification_path="graphic",
            explanation="PNG графического блока не найден.",
        )

    prompt = _build_prompt(ctx)
    result = await describe_image_local(primary.png_path, prompt, model=model)
    raw_text = (result.full_raw_response or result.raw_response_excerpt or "").strip()
    if result.parsed and not raw_text:
        raw_text = json.dumps(result.parsed, ensure_ascii=False)

    if not raw_text or result.status in ("error", "provider_unavailable", "timeout"):
        return missing_decision(
            ctx.finding,
            verification_path="graphic",
            explanation=f"Vision LLM error: {result.error or result.status}",
        )

    obj = _parse_perception(raw_text)
    d = _perception_to_decision(
        str(ctx.finding.get("id", "")),
        obj,
        [primary.block_id],
    )
    d.model_used = result.model_used or model
    return d


def verify_graphic(ctx: FindingContext, *, model: Optional[str] = None) -> EVDecision:
    return asyncio.run(verify_graphic_async(ctx, model=model))
