# -*- coding: utf-8 -*-
"""Pipeline V2 — local vision runner (адаптер к локальному LM Studio vision).

Тонкая обвязка контракта ``VisionRunner`` из
:mod:`pipeline_v2_graphic_vision_enrichment` поверх существующей локальной
vision-инфраструктуры (:func:`graphic_llm_local.compare_images_local` —
OpenAI-compatible endpoint, two-image input в одном сообщении, basic auth,
timeout из конфига).

Принципы:

* НЕ импортирует текстовые LLM-провайдеры, не создаёт jobs, ничего не пишет;
* OLD передаётся ПЕРВОЙ картинкой, NEW — второй (prompt слоя это
  проговаривает); порядок аргументов не переставляется;
* обе стороны обязательны: текущий локальный вызов — парный compare;
  односторонний item → честная ошибка (gv пометит item failed);
* fail-soft по контракту gv: транспорт/timeout/непарсабельный ответ →
  исключение или partial-результат с ``raw_text`` — слой сам разрулит;
* ответ нормализуется к структуре gv (`old_description` … `confidence`),
  плюс ``raw_text`` (excerpt сырого ответа) для диагностики.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

# Поля контракта gv-результата, которые пробуем достать из ответа модели.
_RESULT_KEYS = (
    "old_description", "new_description", "observed_changes",
    "engineering_entities_old", "engineering_entities_new",
    "possible_risks", "confidence",
)


def _run_async(coro):
    """Выполнить coroutine из sync-контекста runner'а."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError(
        "local vision runner is sync-only; call it from a thread without "
        "a running event loop (e.g. asyncio.to_thread)")


def _extract_result_payload(parsed: Any, raw_text: str) -> dict:
    """Привести parsed-ответ модели к gv-структуре + raw_text."""
    out: dict[str, Any] = {"raw_text": raw_text}
    if isinstance(parsed, dict):
        for key in _RESULT_KEYS:
            if key in parsed:
                out[key] = parsed[key]
        # модели иногда вкладывают полезное в обёртку {"result": {...}}
        inner = parsed.get("result")
        if isinstance(inner, dict):
            for key in _RESULT_KEYS:
                out.setdefault(key, inner.get(key))
    return out


def build_local_vision_runner(*, model: Optional[str] = None,
                              max_tokens: Optional[int] = None,
                              timeout_sec: Optional[int] = None):
    """Собрать sync ``vision_runner`` поверх локального vision endpoint'а.

    Конфиг (endpoint/auth/timeout/image_long_side) — стандартный
    ``load_local_graphic_llm_config()`` (env). Параметры-override опциональны.
    Возвращаемая функция соответствует контракту
    ``vision_runner(prompt, left_image_path, right_image_path, options) -> dict``.
    """
    # ленивый импорт: модуль gv остаётся без транспортных зависимостей,
    # а сам адаптер можно импортировать в окружении без httpx до первого вызова
    from backend.app.services.stage_comparison.graphic_llm_local import (
        compare_images_local,
        load_local_graphic_llm_config,
        salvage_partial_json,
    )
    import dataclasses

    cfg = load_local_graphic_llm_config()
    if max_tokens or timeout_sec:
        cfg = dataclasses.replace(
            cfg,
            max_tokens=int(max_tokens or cfg.max_tokens),
            timeout_sec=int(timeout_sec or cfg.timeout_sec))

    def vision_runner(prompt: str, left_image_path: Optional[str],
                      right_image_path: Optional[str],
                      options: dict) -> dict:
        if not left_image_path or not right_image_path:
            raise ValueError(
                "local vision runner requires both OLD and NEW images "
                f"(left={bool(left_image_path)}, right={bool(right_image_path)})")
        result = _run_async(compare_images_local(
            left_image_path, right_image_path, prompt=prompt,
            model=model, cfg=cfg))
        raw_text = result.raw_response_excerpt or ""
        if result.status in ("error", "timeout", "provider_unavailable"):
            raise RuntimeError(
                f"local vision call failed: {result.status}: {result.error}")

        parsed = result.parsed
        if not isinstance(parsed, dict):
            parsed = salvage_partial_json(raw_text)
        if not isinstance(parsed, dict):
            try:
                parsed = json.loads(raw_text)
            except (ValueError, TypeError):
                parsed = None
        out = _extract_result_payload(parsed, raw_text)
        out["model_used"] = result.model_used or result.model
        out["duration_sec"] = round(result.duration_sec, 1)
        return out

    return vision_runner


__all__ = ["build_local_vision_runner"]
