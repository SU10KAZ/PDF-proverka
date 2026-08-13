"""Судья ансамбля блоков через мост провайдера воркера (этап 11I).

Зачем отдельный транспорт, а не второй судья.

На центре судья ходит прямым `codex exec`: `run_codex_json_messages` собирает
argv, отдаёт промпт в stdin и читает JSON. На воркере этот путь недостижим по
построению — окружение процесса конвейера собирается с нуля по белому списку
(`PATH/LANG/LC_ALL/TZ`), CLI провайдера живёт в своей раскладке под своим HOME,
а каждый оплачиваемый вызов обязан пройти журнал exactly-once.

Соблазн «сделать на воркере судью попроще» здесь особенно велик и особенно
вреден: два судьи с разными промптами разошлись бы на первой же правке, и
сравнить удалённый прогон с центральным стало бы нельзя — а именно ради
сравнимости весь этап 11I и затевался. Поэтому подменяется РОВНО транспорт:
модуль отдаёт функцию, которую `review_dual_findings` зовёт вместо
`run_codex_json_messages`, и возвращает результат той же формы.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

#: Поля, которые судья обязан вернуть.
#:
#: Имена взяты из ЕГО СХЕМЫ (`dual_review.REVIEW_SYSTEM_PROMPT`), а не из схемы
#: детектора: судья возвращает `relationships` и `gap_findings`, ключа
#: `findings` в его ответе нет и быть не может. Ошибка здесь не «строже, чем
#: надо»: проверка обязательных полей падала бы на КАЖДОМ блоке уже ПОСЛЕ
#: оплаченного вызова, вердикт выбрасывался бы, и этап тихо шёл бы на
#: `fallback_dual_review` — то есть судья был бы оплачен и не применён.
#:
#: `gap_search` в список не входит намеренно: при выключенном gap-search модель
#: вправе его не возвращать, а `normalize_review_payload` восстанавливает блок
#: сам.
JUDGE_REQUIRED_FIELDS: tuple[str, ...] = ("relationships", "gap_findings")


@dataclass
class JudgeCallResult:
    """Форма ответа `run_codex_json_messages` — ровно та, которую ждёт судья.

    Дублирование формы намеренно: импортировать сюда класс транспорта Codex
    значило бы связать провайдерский путь с CLI, которого на воркере нет.

    Поля `text` и `model` обязательны, хотя провайдерскому пути они не нужны:
    их читает `review_dual_findings` на ОБЕИХ ветках — и успешной, и
    отказной. Без них первое же обращение к `result.text` давало бы
    `AttributeError`, который внешний fail-soft превратил бы в «судья не
    сработал» — после того как вызов уже оплачен, и без единого слова о
    причине в журнале.
    """

    json_data: Optional[dict[str, Any]] = None
    is_error: bool = False
    error_message: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    duration_ms: int = 0
    content: str = ""
    #: Сырой текст ответа. Совпадает с `content`; отдельное имя — потому что
    #: под ним его читает разбор судьи.
    text: str = ""
    #: Фактическая модель. На провайдерском пути её называет локальная политика
    #: воркера, а не конвейер, поэтому здесь пусто — вызывающий подставит метку
    #: действия плана.
    model: str = ""
    usage: dict[str, Any] = field(default_factory=dict)


def build_judge_call(
    *,
    action: Any,
    block_id: str,
    blocks_dir: Path,
    timeout_sec: float,
):
    """Собрать транспорт судьи для одного блока.

    `action` — действие плана роли `judge_gap_search`. Из него берутся
    провайдер, способность и усилие; точной модели здесь нет и быть не может —
    её выберет локальная политика воркера по способности.
    """

    async def _call(messages: Sequence[dict], images: Optional[Sequence[Path]]):
        import asyncio as _asyncio

        from audit_worker.providers import pipeline_bridge
        from audit_worker.providers.pipeline_bridge import ProviderBridgeError
        from backend.app.pipeline.stages.block_analysis import provider_transport as _pt

        system = ""
        user = ""
        for item in messages:
            role = str(item.get("role") or "")
            if role == "system":
                system = str(item.get("content") or "")
            elif role == "user":
                user = str(item.get("content") or "")
        built = _pt.build_provider_prompt(system_prompt=system, user_text=user)
        if built["prompt_chars"] > _pt.MAX_PROMPT_CHARS:
            return JudgeCallResult(
                is_error=True,
                error_message=(
                    f"промпт судьи {built['prompt_chars']} симв. > потолка "
                    f"{_pt.MAX_PROMPT_CHARS}: нарезки на этом пути нет, а "
                    "молчаливое усечение входа сделало бы вердикт неполным"
                ),
            )

        attachments: list[tuple[str, bytes]] = []
        for path in images or ():
            try:
                attachments.append(
                    (_pt.CROP_MEDIA_TYPE, Path(path).read_bytes())
                )
            except OSError as exc:
                # gap-search без картинки — это НЕ gap-search. Отказ честнее
                # текстового вызова, который вернёт правдоподобный пустой ответ.
                return JudgeCallResult(
                    is_error=True,
                    error_message=f"кроп для gap-search не прочитан: {exc}",
                )

        try:
            outcome = await _asyncio.to_thread(
                lambda: pipeline_bridge.run_stage_inference(
                    job_dir=pipeline_bridge.attempt_dir(),
                    stage="block_analysis",
                    prompt=built["prompt"],
                    purpose=f"block_analysis_judge:{block_id}",
                    action_id=str(getattr(action, "action_id", "") or "judge_gap_search"),
                    provider=str(getattr(action, "provider", "") or ""),
                    capability=str(getattr(action, "capability", "") or ""),
                    reasoning_effort=str(getattr(action, "reasoning_effort", "") or ""),
                    required_result_fields=JUDGE_REQUIRED_FIELDS,
                    timeout_sec=float(timeout_sec),
                    images=attachments,
                )
            )
        except ProviderBridgeError as exc:
            return JudgeCallResult(is_error=True, error_message=f"provider_bridge: {exc}")

        result = outcome.provider_result
        usage = dict(result.usage or {})
        payload = result.result if isinstance(result.result, dict) else None
        if not outcome.ok or payload is None:
            detail = getattr(result, "detail", "") or getattr(result, "error_code", "")
            return JudgeCallResult(
                is_error=True,
                error_message=str(detail or "judge_json_missing"),
                input_tokens=int(usage.get("input_tokens") or 0),
                output_tokens=int(usage.get("output_tokens") or 0),
                duration_ms=int(getattr(result, "duration_ms", 0) or 0),
                usage=usage,
            )
        raw = json.dumps(payload, ensure_ascii=False)
        return JudgeCallResult(
            json_data=payload,
            input_tokens=int(usage.get("input_tokens") or 0),
            output_tokens=int(usage.get("output_tokens") or 0),
            duration_ms=int(getattr(result, "duration_ms", 0) or 0),
            content=raw,
            text=raw,
            usage=usage,
        )

    return _call
