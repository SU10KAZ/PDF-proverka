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

#: Провайдер профиля `remote_audit_pilot_v1` по умолчанию. Полем ЗАПРОСА он не
#: становится и на 11H: «выбери провайдера сам» по-прежнему означало бы «выполни
#: неизвестно чем». Но и константой он быть перестал — до 11H строка `claude`
#: была вписана в код, и заказать Codex-воркера было нечем при полностью готовом
#: Codex-адаптере.
#:
#: Источник значения — настройка ЦЕНТРА (`DISTRIBUTED_AUDIT_PROVIDER`), то есть
#: решение администратора платформы, а не данные задания и не выбор клиента API.
#: Список закрыт схемой `ProviderRequirementPayload.provider`; неизвестное имя —
#: отказ при сборке требования, а не тихий возврат к умолчанию.
DEFAULT_AUDIT_PROVIDER = "claude"

#: Провайдеры, которых центр умеет заказывать. Совпадает со схемой нагрузки.
SUPPORTED_AUDIT_PROVIDERS: tuple[str, ...] = ("claude", "codex")

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

#: Потолок, когда структуру документа прочитать не удалось. Не «побольше на
#: всякий случай»: это худший случай, который центр готов авторизовать вслепую.
_BLIND_BLOCK_ESTIMATE = 12

#: Жёсткая верхняя граница центра — она же граница схемы нагрузки.
#:
#: До 11H здесь стояло 24, и это был не рубеж, а работающий обрыв: документ с
#: сорока графическими блоками получал бюджет 24 и упирался в потолок на
#: середине аудита, УЖЕ оплатив две трети вызовов. Рубеж обязан быть выше
#: любого честно посчитанного бюджета, иначе он превращается в скрытый лимит
#: на размер документа. Ограничивает же реальный расход не он, а формула
#: `estimate_inferences` и потолок локальной политики воркера — тот всегда у́же.
CENTER_MAX_INFERENCES = 64


def technical_retry_headroom(natural_calls: int) -> int:
    """Запас на ТЕХНИЧЕСКИЕ повторы: `max(3, ceil(N × 0.10))`.

    До 11H здесь стояла константа 2. Для документа на восемь вызовов это
    осмысленный запас, для документа на полсотни — нет: одна сорвавшаяся сеть
    на десятый блок съедала половину запаса, вторая обрывала аудит целиком.

    Повторы по КАЧЕСТВУ этим запасом не покрываются и покрываться не могут:
    в бюджет входят только повторы после таймаута, обрыва транспорта и ответа,
    непригодного к разбору. «Мне не понравился результат» — не техническая
    причина, и второго оплаченного вызова за неё не бывает.
    """
    import math

    return max(3, math.ceil(max(0, int(natural_calls)) * 0.10))


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


#: Где у версии лежит структура блоков. Порядок — порядок доверия.
#:
#: `02_work/result.json` стоит ПЕРВЫМ, потому что это канонический вход
#: конвейера: именно его читает кропинг (`crop_blocks/blocks.py`), и именно по
#: нему считает блоки Stage 02. Раньше искали только `01_input/*_result.json`, и
#: на новом трёхфайловом комплекте портала (pdf + `_results.md` + `_results.html`
#: + `_blocks.json`, БЕЗ `result.json`) поиск не находил ничего — оценка молча
#: уходила в слепые 12 блоков при фактических сорока с лишним.
_BLOCK_STRUCTURE_CANDIDATES: tuple[str, ...] = (
    "02_work/result.json",
    "01_input/result.json",
)

#: Типы блока, которые Stage 02 отдаёт модели поштучно.
_GRAPHIC_BLOCK_TYPES = ("image", "figure", "picture", "chart", "drawing")


def _graphic_blocks_in(payload: Any) -> Optional[int]:
    """Сколько блоков этой структуры дойдут до Stage 02. `None` — не разобрали.

    Правило отбора списано с фактического кода кропинга, а не придумано заново:
    блок обязан быть графическим, иметь кроп (`crop_url`) или координаты для
    офлайн-рендера — и НЕ быть штампом.

    Штампы отсеиваются по `category_code`, как это делает
    `crop_blocks/blocks.py`. Без этой строки оценка завышалась ровно на число
    штампов (у пилотного документа 11H — на 13 из 54), а завышенный бюджет
    открывает чужую подписку шире, чем нужно.
    """
    blocks = _iter_blocks(payload)
    if not blocks:
        return None
    graphic = 0
    for block in blocks:
        kind = str(block.get("type") or block.get("block_type") or "").lower()
        if kind and kind not in _GRAPHIC_BLOCK_TYPES:
            continue
        if str(block.get("category_code") or "").lower() == "stamp":
            continue
        if block.get("crop_url") or block.get("coords") or block.get("coords_px") or block.get("bbox"):
            graphic += 1
    return graphic


def count_graphic_blocks(version_dir: Path) -> Optional[int]:
    """Сколько блоков потребуют отдельного обращения к модели.

    Возвращает `None`, если структуру прочитать не удалось — и это ЧЕСТНЫЙ
    ответ, а не ноль. Ноль означал бы «графики нет», то есть бюджет без
    `block_analysis`, и первый же блок упёрся бы в потолок.
    """
    root = Path(version_dir)
    candidates: list[Path] = [root / rel for rel in _BLOCK_STRUCTURE_CANDIDATES]
    input_dir = root / "01_input"
    if input_dir.is_dir():
        candidates.extend(sorted(input_dir.glob("*_result.json")))
    for path in candidates:
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        found = _graphic_blocks_in(payload)
        if found is not None:
            return found
    return None


def estimate_inferences(version_dir: Path) -> dict[str, Any]:
    """Оценка числа обращений к модели за worker-участок. Прозрачная и цитируемая."""
    blocks = count_graphic_blocks(Path(version_dir))
    blind = blocks is None
    effective_blocks = _BLIND_BLOCK_ESTIMATE if blind else int(blocks)
    natural = effective_blocks + _FIXED_STAGE_CALLS
    headroom = technical_retry_headroom(natural)
    ceiling = max(1, min(CENTER_MAX_INFERENCES, _env_int(
        "DISTRIBUTED_AUDIT_MAX_INFERENCES", CENTER_MAX_INFERENCES
    )))
    budget = min(ceiling, natural + headroom)
    return {
        "graphic_blocks": None if blind else int(blocks),
        "blind_estimate": blind,
        "natural_calls": natural,
        "technical_retry_headroom": headroom,
        "center_ceiling": ceiling,
        "max_inferences": budget,
        # Бюджет, УРЕЗАННЫЙ потолком, — отдельное наблюдаемое утверждение, а не
        # деталь строки `formula`. Обрыв аудита на середине из-за потолка
        # выглядит в журнале как ошибка этапа, и связать его с настройкой центра
        # задним числом можно только по этому полю.
        "clamped_by_ceiling": bool(natural + headroom > ceiling),
        "formula": (
            f"{effective_blocks} графических блоков + {_FIXED_STAGE_CALLS} "
            f"текстовых этапов + {headroom} на технические повторы "
            f"(max(3, ceil(N×0.10))), зажато потолком центра {ceiling}"
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


def audit_provider() -> str:
    """Какого провайдера заказывает ЭТОТ центр. Настройка платформы, не задания.

    Неизвестное имя — отказ. Молчаливый возврат к умолчанию означал бы, что
    оператор, опечатавшийся в настройке, получает Claude-задание там, где
    рассчитывал на Codex, и узнаёт об этом из счёта за подписку.
    """
    raw = (os.environ.get("DISTRIBUTED_AUDIT_PROVIDER") or "").strip().lower()
    if not raw:
        return DEFAULT_AUDIT_PROVIDER
    if raw not in SUPPORTED_AUDIT_PROVIDERS:
        raise ProviderRequirementError(
            f"DISTRIBUTED_AUDIT_PROVIDER={raw!r}: центр умеет заказывать только "
            f"{list(SUPPORTED_AUDIT_PROVIDERS)}"
        )
    return raw


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
    provider = audit_provider()
    estimate = estimate_inferences(Path(version_dir))
    if worker is not None:
        ok, why = worker_supports(
            worker, provider=provider, capability=AUDIT_CAPABILITY
        )
        if not ok:
            raise ProviderRequirementError(why)
    requirement = ProviderRequirementPayload(
        provider=provider,
        capability=AUDIT_CAPABILITY,
        allowed_stages=list(AUDIT_MODEL_STAGES),
        max_inferences=int(estimate["max_inferences"]),
    )
    rationale = {
        "provider": provider,
        "provider_source": (
            "DISTRIBUTED_AUDIT_PROVIDER" if os.environ.get("DISTRIBUTED_AUDIT_PROVIDER")
            else "умолчание профиля"
        ),
        "capability": AUDIT_CAPABILITY,
        "action": action,
        "allowed_stages": list(AUDIT_MODEL_STAGES),
        "budget": estimate,
        # Утверждение, которое проверяет отдельный тест: точной модели в
        # требовании нет ни в каком виде.
        "exact_model_in_payload": False,
    }
    return requirement, rationale
