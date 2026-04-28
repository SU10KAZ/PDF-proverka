"""
OpenRouter stage 02 (block_batch) runner — experimental path.

Обёртка над llm_runner.run_llm с:
  - загрузкой strict schema (webapp/schemas/block_batch.openrouter.json)
  - completeness checks (missing / duplicate / extra block_ids)
  - single-block block_id inference (ТОЛЬКО для 1-in-1-out случая)
  - deterministic byte-cap splitter (9000 KB raw PNG)
  - hard cap 12 блоков в batch

Production path (webapp.services.llm_runner.run_llm через claude_runner) не трогает
эти guard'ы — это отдельный experimental слой для scripts/run_gemini_openrouter_stage02_experiment.py.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from webapp.config import (
    OPENROUTER_STAGE02_HARD_CAP_BLOCKS,
    OPENROUTER_STAGE02_RAW_BYTE_CAP_KB,
    OPENROUTER_STAGE02_TIMEOUT_SEC,
    OPENROUTER_STAGE02_MAX_OUTPUT_TOKENS,
    SCHEMAS_DIR,
)
from webapp.models.usage import LLMResult
from webapp.services.llm_runner import run_llm

logger = logging.getLogger(__name__)


OPENROUTER_STRICT_SCHEMA_FILENAME = "block_batch.openrouter.json"


def load_openrouter_block_batch_schema() -> dict:
    """Загрузить strict JSON schema для block_batch OpenRouter запросов."""
    path = SCHEMAS_DIR / OPENROUTER_STRICT_SCHEMA_FILENAME
    return json.loads(path.read_text(encoding="utf-8"))


# ──────────────────────────────────────────────────────────────────────────
# Completeness checks
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class CompletenessResult:
    """Результат проверки полноты batch ответа."""
    missing: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    block_id_inferred_from_input: bool = False
    inferred_block_id_count: int = 0

    @property
    def ok(self) -> bool:
        return not self.missing and not self.duplicates and not self.extra


def check_completeness(
    input_block_ids: list[str],
    parsed_data: dict,
    *,
    single_block_inference: bool = True,
) -> CompletenessResult:
    """Проверить полноту ответа.

    Если режим single_block_inference=True и было ровно 1 input block,
    ровно 1 analysis, block_id пустой/None/отсутствует, но все прочие
    обязательные поля заполнены — подставить входной block_id.
    В batch-режиме (≥2 блока) никакого inference не делаем.
    """
    result = CompletenessResult()

    analyses = parsed_data.get("block_analyses") if isinstance(parsed_data, dict) else None
    if not isinstance(analyses, list):
        result.missing = list(input_block_ids)
        return result

    # Single-block inference: ровно 1 вход, ровно 1 выход, block_id пуст, но поля заполнены.
    if (
        single_block_inference
        and len(input_block_ids) == 1
        and len(analyses) == 1
    ):
        one = analyses[0]
        if isinstance(one, dict):
            returned_bid = one.get("block_id") or ""
            if not returned_bid:
                # Проверим что поля заполнены (минимальный валидный payload)
                summary_ok = isinstance(one.get("summary"), str) and one["summary"].strip()
                label_ok = isinstance(one.get("label"), str) and one["label"].strip()
                page_ok = isinstance(one.get("page"), int)
                kv_ok = isinstance(one.get("key_values_read"), list)
                findings_ok = isinstance(one.get("findings"), list)
                if summary_ok and label_ok and page_ok and kv_ok and findings_ok:
                    one["block_id"] = input_block_ids[0]
                    result.block_id_inferred_from_input = True
                    result.inferred_block_id_count = 1

    # Собираем фактические block_id (с учётом возможного inference)
    seen: dict[str, int] = {}
    for a in analyses:
        if not isinstance(a, dict):
            continue
        bid = a.get("block_id") or ""
        if not bid:
            continue
        seen[bid] = seen.get(bid, 0) + 1

    input_set = set(input_block_ids)
    returned_set = set(seen.keys())

    result.missing = sorted(input_set - returned_set)
    result.extra = sorted(returned_set - input_set)
    result.duplicates = sorted([b for b, n in seen.items() if n > 1])
    return result


# ──────────────────────────────────────────────────────────────────────────
# Deterministic byte-cap splitter
# ──────────────────────────────────────────────────────────────────────────

def split_batch_by_byte_cap(
    blocks: list[dict],
    *,
    byte_cap_kb: int | None = None,
    hard_cap: int | None = None,
) -> list[list[dict]]:
    """Разбить batch на саб-batches по RAW PNG payload (KB) + по hard cap блоков.

    Детерминированный greedy: сохраняем порядок, начинаем новый саб-batch
    при превышении ЛЮБОГО порога (size_kb ИЛИ count).

    Args:
        blocks: список dict с хотя бы ключом "size_kb" (float/int).
        byte_cap_kb: порог суммы size_kb; по умолчанию OPENROUTER_STAGE02_RAW_BYTE_CAP_KB.
        hard_cap: макс блоков в саб-batch; по умолчанию OPENROUTER_STAGE02_HARD_CAP_BLOCKS.

    Returns:
        Список саб-batches (sequence of lists).
    """
    if byte_cap_kb is None:
        byte_cap_kb = OPENROUTER_STAGE02_RAW_BYTE_CAP_KB
    if hard_cap is None:
        hard_cap = OPENROUTER_STAGE02_HARD_CAP_BLOCKS

    if not blocks:
        return []

    result: list[list[dict]] = []
    cur: list[dict] = []
    cur_kb = 0.0

    for b in blocks:
        try:
            b_kb = float(b.get("size_kb", 0) or 0)
        except (TypeError, ValueError):
            b_kb = 0.0

        # Если добавление этого блока превысит byte cap или hard cap — flush.
        projected_kb = cur_kb + b_kb
        projected_n = len(cur) + 1
        if cur and (projected_kb > byte_cap_kb or projected_n > hard_cap):
            result.append(cur)
            cur = []
            cur_kb = 0.0

        cur.append(b)
        cur_kb += b_kb

    if cur:
        result.append(cur)
    return result


# ──────────────────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class OpenRouterBlockBatchResult:
    """Расширенный результат одного OpenRouter block_batch запроса."""
    llm: LLMResult
    completeness: CompletenessResult
    # Прямые shortcuts (удобно агрегировать в метриках)
    missing: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    block_id_inferred_from_input: bool = False
    inferred_block_id_count: int = 0

    @property
    def is_error(self) -> bool:
        return self.llm.is_error


async def run_openrouter_block_batch(
    messages: list[dict],
    input_block_ids: list[str],
    *,
    model: str,
    batch_id: int = 0,
    strict_schema: bool = True,
    response_healing: bool = True,
    require_parameters: bool = True,
    provider_data_collection: str | None = None,
    temperature: float = 0.2,
    timeout: int | None = None,
    max_retries: int = 3,
    max_output_tokens: int | None = None,
    single_block_inference: bool = True,
) -> OpenRouterBlockBatchResult:
    """Запустить один block_batch запрос через OpenRouter с guard'ами.

    Args:
        messages: готовые messages для OpenRouter (system + user).
        input_block_ids: список block_id, ожидаемых в ответе.
        model: OpenRouter model id (напр. "google/gemini-2.5-flash").
        batch_id: для логирования.
        strict_schema: использовать strict JSON schema (block_batch.openrouter.json).
        response_healing: добавить OpenRouter plugin "response-healing".
        require_parameters: выставить provider.require_parameters=true.
        provider_data_collection: "allow" | "deny" | None (опционально).
        single_block_inference: разрешить inference block_id если 1-in-1-out.
    """
    if timeout is None:
        timeout = OPENROUTER_STAGE02_TIMEOUT_SEC
    if max_output_tokens is None:
        max_output_tokens = OPENROUTER_STAGE02_MAX_OUTPUT_TOKENS

    schema_arg = load_openrouter_block_batch_schema() if strict_schema else None

    # В single-block режиме не разрешаем hard cap issues — проверим:
    if len(input_block_ids) > OPENROUTER_STAGE02_HARD_CAP_BLOCKS:
        logger.warning(
            "[batch %03d] input has %d blocks > hard cap %d",
            batch_id, len(input_block_ids), OPENROUTER_STAGE02_HARD_CAP_BLOCKS,
        )

    # В batch-режиме (≥2 блока) inference выключен принудительно
    effective_inference = single_block_inference and len(input_block_ids) == 1

    llm = await run_llm(
        stage="block_batch",
        messages=messages,
        model_override=model,
        temperature=temperature,
        timeout=timeout,
        max_retries=max_retries,
        strict_schema=schema_arg,
        schema_name="block_batch",
        response_healing=response_healing,
        require_parameters=require_parameters,
        provider_data_collection=provider_data_collection,
        max_tokens_override=max_output_tokens,
    )

    if llm.is_error:
        empty = CompletenessResult(missing=list(input_block_ids))
        return OpenRouterBlockBatchResult(
            llm=llm,
            completeness=empty,
            missing=list(input_block_ids),
        )

    parsed = llm.json_data if isinstance(llm.json_data, dict) else {}
    completeness = check_completeness(
        input_block_ids,
        parsed,
        single_block_inference=effective_inference,
    )

    return OpenRouterBlockBatchResult(
        llm=llm,
        completeness=completeness,
        missing=completeness.missing,
        duplicates=completeness.duplicates,
        extra=completeness.extra,
        block_id_inferred_from_input=completeness.block_id_inferred_from_input,
        inferred_block_id_count=completeness.inferred_block_id_count,
    )
