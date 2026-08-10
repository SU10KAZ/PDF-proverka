"""Требование центра к провайдеру для удалённого аудита (этап 11G).

Зачем отдельный модуль, а не три строки в `RemoteWorkerExecutionBackend`.

До 11G требование существовало только в диагностических скриптах: центр умел
его ПРИНЯТЬ (`create_audit_job(provider_requirement=…)`), но штатный путь
запуска его не формировал вовсе. Из-за этого боевое задание уезжало на воркер
без единого слова о провайдере, воркер честно не активировал мост, и оператору
приходилось выписывать привязку руками. Разрыв закрывается здесь.

Три вещи, которых в этом модуле НЕТ и быть не может:

  * **идентификатора модели.** Центр называет СПОСОБНОСТЬ (`strong_audit`), а
    какая строка ей соответствует на конкретном VPS — знает только локальная
    политика воркера. Причина не стилистическая: строка задания, дошедшая до
    argv стороннего CLI, ломает инвариант I-P5, а «центр назначает модель»
    означает распоряжение чужой подпиской;
  * **пути, argv, окружения** — как и во всей нагрузке задания;
  * **произвольного числа вызовов.** Потолок считается из документа
    детерминированной функцией и зажимается сверху настройкой центра.

Ответ на вопрос «сколько вызовов» обязан быть КОНСЕРВАТИВНЫМ с двух сторон
сразу. Заниженный потолок обрывает аудит в середине, уже оплатив часть
вызовов; завышенный — открывает чужую подписку шире, чем нужно. Поэтому
оценка снимается со структуры документа (по одному вызову на графический блок
плюс фиксированные текстовые этапы), а не берётся круглым числом.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    CAPABILITY_STRONG_AUDIT,
    KNOWN_CAPABILITIES,
    ProviderRequirementPayload,
)

#: Провайдер профиля `remote_audit_pilot_v1`. Константа профиля, а не поле
#: запроса: реализация worker-участка написана против Claude-адаптера, и
#: «выбери провайдера сам» здесь означало бы «выполни неизвестно чем».
AUDIT_PROVIDER = "claude"

#: Способность, которую требует боевой аудит.
AUDIT_CAPABILITY = CAPABILITY_STRONG_AUDIT

#: Этапы worker-участка, которым РАЗРЕШЕНО обращаться к модели. Белый список:
#: этап, которого здесь нет, получает отказ моста, а не молчаливый обход.
#:
#: Имена сверены с фактическим прогоном 11F (`11F_STAGE_PROVIDER_MAP.json`):
#: `findings_review` зовёт модель условно (страж отсутствия), `optimization_*`
#: разложены на три самостоятельных обращения.
AUDIT_MODEL_STAGES: tuple[str, ...] = (
    "block_analysis",
    "text_analysis",
    "findings_merge",
    "findings_review",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
)

#: Фиксированная часть оценки: текстовые этапы, число которых не зависит от
#: документа. text_analysis(1) + findings_merge(1) + findings_review(1) +
#: optimization(1) + optimization_critic(1) + optimization_corrector(1).
_FIXED_STAGE_CALLS = 6

#: Запас на технические повторы. Ровно два (§27 задания 11G): один повтор на
#: одно неудавшееся логическое действие, не более двух на задание.
_TECHNICAL_RETRY_HEADROOM = 2

#: Потолок, когда структуру документа прочитать не удалось. Не «побольше на
#: всякий случай»: это худший случай, который центр готов авторизовать вслепую.
_BLIND_BLOCK_ESTIMATE = 12

#: Жёсткая верхняя граница центра. Схема допускает 64; столько центр не
#: заказывает никогда — это рубеж, а не рабочее значение.
CENTER_MAX_INFERENCES = 24


class ProviderRequirementError(RuntimeError):
    """Требование невозможно сформировать либо воркер его не потянет."""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _iter_blocks(payload: Any) -> list[dict[str, Any]]:
    """Блоки из `*_result.json`: список страниц → список блоков."""
    pages = payload.get("pages") if isinstance(payload, dict) else None
    if not isinstance(pages, list):
        return []
    blocks: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        for block in page.get("blocks") or []:
            if isinstance(block, dict):
                blocks.append(block)
    return blocks


def count_graphic_blocks(version_dir: Path) -> Optional[int]:
    """Сколько блоков потребуют отдельного обращения к модели.

    Возвращает `None`, если структуру прочитать не удалось — и это ЧЕСТНЫЙ
    ответ, а не ноль. Ноль означал бы «графики нет», то есть бюджет без
    `block_analysis`, и первый же блок упёрся бы в потолок.

    Считаются блоки с `crop_url` ЛИБО с координатами: и то и другое даёт кроп
    (сеть или офлайн-рендер из PDF), а архитектура Stage 02 — строго «один
    блок = один вызов».
    """
    root = Path(version_dir) / "01_input"
    candidates = sorted(root.glob("*_result.json")) if root.is_dir() else []
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        blocks = _iter_blocks(payload)
        if not blocks:
            continue
        graphic = 0
        for block in blocks:
            kind = str(block.get("type") or block.get("block_type") or "").lower()
            if kind and kind not in ("image", "figure", "picture", "chart", "drawing"):
                continue
            if block.get("crop_url") or block.get("coords") or block.get("bbox"):
                graphic += 1
        return graphic
    return None


def estimate_inferences(version_dir: Path) -> dict[str, Any]:
    """Оценка числа обращений к модели за worker-участок. Прозрачная и цитируемая."""
    blocks = count_graphic_blocks(Path(version_dir))
    blind = blocks is None
    effective_blocks = _BLIND_BLOCK_ESTIMATE if blind else int(blocks)
    natural = effective_blocks + _FIXED_STAGE_CALLS
    ceiling = max(1, min(CENTER_MAX_INFERENCES, _env_int(
        "DISTRIBUTED_AUDIT_MAX_INFERENCES", CENTER_MAX_INFERENCES
    )))
    budget = min(ceiling, natural + _TECHNICAL_RETRY_HEADROOM)
    return {
        "graphic_blocks": None if blind else int(blocks),
        "blind_estimate": blind,
        "natural_calls": natural,
        "technical_retry_headroom": _TECHNICAL_RETRY_HEADROOM,
        "center_ceiling": ceiling,
        "max_inferences": budget,
        "formula": (
            f"{effective_blocks} графических блоков + {_FIXED_STAGE_CALLS} "
            f"текстовых этапов + {_TECHNICAL_RETRY_HEADROOM} на технические "
            f"повторы, зажато потолком центра {ceiling}"
        ),
    }


def worker_supports(worker: dict[str, Any], *, provider: str, capability: str) -> tuple[bool, str]:
    """Объявляет ли воркер эту способность. Отказ — ДО создания задания.

    Молчание считается отказом намеренно. Воркер, не приславший поле, — это
    либо старая сборка, либо машина без локальной политики моделей; и в том и
    в другом случае задание, требующее вызовов, дошло бы до отказа уже ПОСЛЕ
    сборки пакета и выдачи, потратив время и место на обеих сторонах.
    """
    from backend.app.services.distributed_workers import job_service

    caps = job_service.worker_capabilities(worker)
    if not bool(caps.get("real_llm_enabled")):
        return False, (
            "на воркере выключены настоящие модели "
            "(AUDIT_WORKER_ALLOW_REAL_LLM=false) — задание с вызовами модели "
            "ему выдавать нельзя"
        )
    if not bool(caps.get("pipeline_provider_bridge_enabled")):
        return False, (
            "на воркере не разрешён мост конвейера к провайдеру "
            "(AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED=false)"
        )
    declared = caps.get("provider_capabilities")
    if not isinstance(declared, dict) or not declared:
        return False, (
            "воркер не объявляет способностей провайдеров: локальная политика "
            "моделей на машине отсутствует либо не читается"
        )
    offered = declared.get(provider)
    if not isinstance(offered, list) or capability not in offered:
        return False, (
            f"воркер не объявляет способность {capability!r} для провайдера "
            f"{provider!r} (объявлено: {json.dumps(declared, ensure_ascii=False)})"
        )
    return True, ""


def build_audit_requirement(
    *,
    version_dir: Path,
    worker: Optional[dict[str, Any]] = None,
    action: str = "full",
) -> tuple[ProviderRequirementPayload, dict[str, Any]]:
    """Собрать требование для боевого аудита. Возвращает (требование, обоснование).

    Проверка воркера здесь, а не в вызывающем коде: требование и способность
    машины — две половины одного утверждения, и разнесённые по разным местам
    они разъезжаются (ровно так «центр умеет принять требование» и «центр его
    формирует» разъехались до 11G).
    """
    if AUDIT_CAPABILITY not in KNOWN_CAPABILITIES:      # pragma: no cover — защита от правки реестра
        raise ProviderRequirementError(
            f"способность {AUDIT_CAPABILITY!r} исчезла из реестра центра"
        )
    estimate = estimate_inferences(Path(version_dir))
    if worker is not None:
        ok, why = worker_supports(
            worker, provider=AUDIT_PROVIDER, capability=AUDIT_CAPABILITY
        )
        if not ok:
            raise ProviderRequirementError(why)
    requirement = ProviderRequirementPayload(
        provider=AUDIT_PROVIDER,
        capability=AUDIT_CAPABILITY,
        allowed_stages=list(AUDIT_MODEL_STAGES),
        max_inferences=int(estimate["max_inferences"]),
    )
    rationale = {
        "provider": AUDIT_PROVIDER,
        "capability": AUDIT_CAPABILITY,
        "action": action,
        "allowed_stages": list(AUDIT_MODEL_STAGES),
        "budget": estimate,
        # Утверждение, которое проверяет отдельный тест: точной модели в
        # требовании нет ни в каком виде.
        "exact_model_in_payload": False,
    }
    return requirement, rationale
