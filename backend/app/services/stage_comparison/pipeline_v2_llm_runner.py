# -*- coding: utf-8 -*-
"""Pipeline V2 — постоянный инъектируемый Claude-runner для Delta Explanation.

Назначение: дать готовый ``llm_runner`` для
``run_pipeline_v2_dry_run(..., llm_runner=...)`` /
``explain_entity_diff_report(..., llm_runner=...)``. Runner объясняет ТОЛЬКО
уже найденные deterministic deltas (промпт строит
[pipeline_v2_delta_explanation](pipeline_v2_delta_explanation.py) — с запретом
искать новые отличия); сам runner — это просто «prompt → dict-ответ».

Контракт ответа (читается ``_invoke_runner`` delta explanation):

```json
{"provider": "claude", "model": "sonnet", "raw_status": "ok|failed|skipped",
 "raw_response": "...", "error": null}
```

Принципы:
  * при импорте НИЧЕГО не запускается; real runner создаётся только явным
    вызовом ``build_pipeline_v2_claude_runner(...)``;
  * default dry-run НЕ меняется: ``llm_runner=None`` остаётся канонический
    «выключено» (deltas → ``skipped_no_runner``);
  * вызов идёт через существующий subscription-wrapper
    [text_llm_provider.ClaudeCodeProvider](text_llm_provider.py)
    (``claude -p``, без shell, stdin, изолированный CWD) — никакие локальные
    LM Studio модели и batch-jobs сравнения НЕ запускаются;
  * fail-soft: любое исключение/сбой провайдера → ``raw_status="failed"`` у
    конкретной explanation, dry-run не падает;
  * ``claude -p --output-format json`` отдаёт envelope ``{"result": "..."}``
    — runner разворачивает его до текста ответа модели
    (``unwrap_claude_cli_response``); plain-JSON explanation проходит как есть,
    битый JSON возвращается строкой (парсер дальше fail-soft).
"""
from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.stage_comparison.text_llm_provider import (
    BaseTextLLMProvider,
    ClaudeCodeProvider,
)

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "sonnet"
DEFAULT_TIMEOUT_SEC = 240

# Тип runner'а совпадает с LLMRunner из pipeline_v2_delta_explanation:
# callable(prompt: str) -> dict
PipelineV2LLMRunner = Callable[[str], dict]

_OK_STATUSES = {"done", "ok", "completed", "success"}
_SKIPPED_STATUSES = {"skipped", "disabled"}


def _opt(options: Optional[dict], key: str, default: Any) -> Any:
    if options and options.get(key) is not None:
        return options[key]
    return default


# ─── CLI envelope ────────────────────────────────────────────────────────────


def unwrap_claude_cli_response(raw_response: Any) -> str:
    """Развернуть envelope ``claude -p --output-format json``.

    ``{"result": "<текст ответа модели>", ...}`` → текст ответа. Plain-JSON
    explanation (без строкового ``result``) и любой не-JSON/битый JSON
    возвращаются как есть — дальнейший разбор делает fail-soft парсер
    ``parse_delta_explanation_response``.
    """
    s = "" if raw_response is None else str(raw_response)
    try:
        env = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return s
    if isinstance(env, dict) and isinstance(env.get("result"), str):
        return env["result"]
    return s


# ─── нормализация результата провайдера ──────────────────────────────────────


def normalize_llm_runner_result(result: Any, provider: str = "claude",
                                model: Optional[str] = None) -> dict:
    """Привести результат провайдера к контракту runner'а.

    Принимает ``ProviderResult`` (атрибуты status/raw_response/error/model),
    dict с такими же ключами или сырую строку (= успешный ответ). Статусы:
    ``done/ok/completed/success`` → ``ok``; ``skipped/disabled`` → ``skipped``;
    всё остальное (``error``/``timeout``/``provider_not_available``/…) →
    ``failed`` с заполненным ``error``.
    """
    if hasattr(result, "status"):  # ProviderResult-подобный объект
        status = str(getattr(result, "status", "") or "")
        raw = str(getattr(result, "raw_response", "") or "")
        error = getattr(result, "error", None)
        model = model or (getattr(result, "model", None) or None)
    elif isinstance(result, dict):
        status = str(result.get("status") or result.get("raw_status") or "")
        raw = str(result.get("raw_response") or result.get("text")
                  or result.get("response") or "")
        error = result.get("error")
        model = model or result.get("model")
    else:  # сырая строка = успешный ответ
        status, raw, error = "ok", str(result or ""), None

    low = status.strip().lower()
    if low in _OK_STATUSES:
        raw_status = "ok"
    elif low in _SKIPPED_STATUSES:
        raw_status = "skipped"
        error = error or (low or "skipped")
    else:
        raw_status = "failed"
        error = error or (low or "unknown_error")
    return {
        "provider": provider,
        "model": model,
        "raw_status": raw_status,
        "raw_response": raw,
        "error": error,
    }


# ─── noop runner ─────────────────────────────────────────────────────────────


def make_noop_llm_runner(reason: str = "disabled") -> PipelineV2LLMRunner:
    """Runner-заглушка: всегда ``raw_status="skipped"`` с указанной причиной.

    Канонический «выключено» для dry-run — это ``llm_runner=None``
    (deltas получают ``skipped_no_runner``); noop нужен, когда runner строится
    централизованно, но провайдер выключен/недоступен.
    """
    def _noop(prompt: str) -> dict:  # noqa: ARG001 — контракт callable(prompt)
        return {"provider": "none", "model": None, "raw_status": "skipped",
                "raw_response": "", "error": reason}
    return _noop


# ─── one-shot вызов ──────────────────────────────────────────────────────────


def run_pipeline_v2_claude_prompt(prompt: str, options: Optional[dict] = None) -> dict:
    """Один вызов Claude по готовому промпту дельты → контрактный dict.

    ``options``: ``model`` (default ``sonnet``), ``timeout_sec`` (default 240),
    ``work_dir`` (абсолютизируется; default — временная папка), ``provider``
    (инъекция ``BaseTextLLMProvider`` для тестов), ``system_prompt``
    (default пустой — весь контракт уже в промпте дельты).

    Fail-soft: исключение провайдера → ``raw_status="failed"``.
    """
    options = options or {}
    model = str(_opt(options, "model", DEFAULT_MODEL))
    timeout_sec = int(_opt(options, "timeout_sec", DEFAULT_TIMEOUT_SEC))
    system_prompt = str(_opt(options, "system_prompt", ""))
    provider: BaseTextLLMProvider = _opt(options, "provider", None) or ClaudeCodeProvider()

    work_dir = _opt(options, "work_dir", None)
    if work_dir is None:
        work_dir = tempfile.mkdtemp(prefix="pipeline_v2_llm_runner_")
    # абсолютный путь обязателен: провайдер ставит cwd=work_dir
    work_dir = Path(work_dir).resolve()

    try:
        res = provider.invoke(system_prompt=system_prompt, user_prompt=prompt,
                              model=model, timeout_sec=timeout_sec,
                              work_dir=work_dir)
    except Exception as exc:  # noqa: BLE001 — fail-soft по контракту runner'а
        logger.warning("pipeline_v2_llm_runner: provider exception: %s", exc)
        return {"provider": "claude", "model": model, "raw_status": "failed",
                "raw_response": "", "error": f"{type(exc).__name__}: {exc}"}

    out = normalize_llm_runner_result(res, provider="claude", model=model)
    if out["raw_status"] == "ok":
        out["raw_response"] = unwrap_claude_cli_response(out["raw_response"])
    return out


# ─── фабрика runner'а ────────────────────────────────────────────────────────


def build_pipeline_v2_claude_runner(
        options: Optional[dict] = None) -> PipelineV2LLMRunner:
    """Построить инъектируемый Claude-runner для Pipeline V2 dry-run.

    Ничего не вызывает до первого ``runner(prompt)``, кроме опционального
    ``check_availability`` (быстрый ``claude --version``; отключается
    ``options={"check_availability": False}`` — например, в тестах с
    mock-провайдером проверка не нужна).

    * ``options={"enabled": False}`` → noop runner (``skipped``/``disabled``);
    * CLI недоступен → noop runner с причиной ``provider_not_available: …``;
    * иначе — closure, зовущая ``run_pipeline_v2_claude_prompt`` с одним
      ``work_dir`` на всё время жизни runner'а (изолированный CWD).

    Использование (controlled smoke, max_deltas=5/10):

    ```python
    runner = build_pipeline_v2_claude_runner()
    summary = run_pipeline_v2_dry_run(left, right, out_dir,
                                      options=opts, llm_runner=runner)
    ```
    """
    options = dict(options or {})
    if not bool(_opt(options, "enabled", True)):
        return make_noop_llm_runner("disabled")

    provider: BaseTextLLMProvider = _opt(options, "provider", None) or ClaudeCodeProvider()
    options["provider"] = provider

    if bool(_opt(options, "check_availability", True)):
        try:
            ok, reason = provider.check_availability()
        except Exception as exc:  # noqa: BLE001 — fail-soft
            ok, reason = False, f"{type(exc).__name__}: {exc}"
        if not ok:
            logger.warning("pipeline_v2_llm_runner: provider unavailable: %s", reason)
            return make_noop_llm_runner(f"provider_not_available: {reason}")

    if _opt(options, "work_dir", None) is None:
        options["work_dir"] = tempfile.mkdtemp(prefix="pipeline_v2_llm_runner_")

    def _runner(prompt: str) -> dict:
        return run_pipeline_v2_claude_prompt(prompt, options)

    return _runner


__all__ = [
    "DEFAULT_MODEL",
    "DEFAULT_TIMEOUT_SEC",
    "PipelineV2LLMRunner",
    "build_pipeline_v2_claude_runner",
    "run_pipeline_v2_claude_prompt",
    "unwrap_claude_cli_response",
    "normalize_llm_runner_result",
    "make_noop_llm_runner",
]
