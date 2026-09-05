"""Компилятор плана маршрутизации: пресет + флаги → неизменяемый план.

Единственное место, где маршрут вообще возникает.

До 11I «маршрут» не существовал как объект: он складывался в момент исполнения
из трёх независимых источников — таблицы `stage_models.json`, переменных
окружения (часть из которых читается на импорте модуля) и глобальной ручки
`config.get_claude_model()`. Ни один из трёх не был привязан к заданию, и любой
мог измениться между этапами одного прогона.

Компилятор собирает все три в один снимок и замораживает его. После этого
вопрос «на чём пойдёт этот аудит» имеет ровно один ответ, и он записан.

Правило, которого компилятор держится жёстко: **описывать рантайм, а не
интерфейс**. Из десяти строк таблицы моделей три не соответствуют тому, что
происходит (Верификатор и F OPT Fix не зовут модель вовсе, Верификатор-фикс
всегда уходит на Claude мимо строки таблицы). План повторяет РАНТАЙМ; строки
таблицы, которые ни на что не влияют, попадают в план как детерминированные
действия либо не попадают вовсе — с пояснением в `note`.
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from backend.app.services.audit_routing import presets, registry, validator
from backend.app.services.audit_routing.plan import (
    RoutingAction,
    RoutingCondition,
    RoutingMultiplicity,
    RoutingPlan,
    RoutingPlanError,
    RoutingStage,
)

#: Дисциплины, для которых Codex-путь свода выполняет дисциплинарный
#: targeted-проход. Список закрыт в `codex_targeted_findings.py:704-712`;
#: любая другая дисциплина прохода не получает.
TARGETED_DISCIPLINES: tuple[str, ...] = ("AR", "EOM", "SS", "KM")

#: Соответствие «строка таблицы моделей → стадия конвейера». Нужно и мосту
#: (белый список), и области исполнения.
PIPELINE_STAGE_OF: dict[str, str] = {
    "block_batch": "block_analysis",
    "text_analysis": "text_analysis",
    "findings_merge": "findings_merge",
    "findings_critic": "findings_review",
    "findings_corrector": "findings_review",
    "norm_verify": "norm_verify",
    "norm_fix": "norm_verify",
    "norm_requote": "norm_verify",
    "optimization": "optimization",
    "optimization_critic": "optimization_critic",
    "optimization_corrector": "optimization_corrector",
}

#: Где исполняется стадия конвейера. Снято с фактического кода (реестр
#: `CENTRAL_ONLY_STAGES` + белый список `AUDIT_MODEL_STAGES`), а не с описания.
SCOPE_OF_PIPELINE_STAGE: dict[str, str] = {
    "block_analysis": registry.SCOPE_WORKER,
    "text_analysis": registry.SCOPE_WORKER,
    "findings_merge": registry.SCOPE_WORKER,
    "findings_review": registry.SCOPE_WORKER,
    "optimization": registry.SCOPE_WORKER,
    "optimization_critic": registry.SCOPE_WORKER,
    "optimization_corrector": registry.SCOPE_WORKER,
    "norm_verify": registry.SCOPE_CENTER,
}


@dataclass(frozen=True)
class CompilerInputs:
    """Всё, из чего собирается план. Ничего сверх этого компилятор не читает.

    Явная структура вместо чтения окружения внутри компилятора — не стиль, а
    условие воспроизводимости: тест обязан уметь скомпилировать план, не трогая
    `os.environ` живого процесса.
    """

    stage_models: Mapping[str, str]
    feature_flags: Mapping[str, Any]
    #: Класс модели глобальной ручки Claude (`config.get_claude_model()`).
    claude_default_model_class: str = registry.MODEL_CLASS_CHEAP
    #: Дисциплина попытки в канонической форме (AR/EOM/…). Известна центру уже
    #: при создании задания, поэтому дисциплинарное условие разрешается сразу.
    discipline_id: str = ""
    #: Строка, в которую фронтенд раскрывает плейсхолдер `__codex_exec__`.
    codex_model_id: str = "codex/gpt-6-astra"
    pipeline_revision: str = ""


def _flag(flags: Mapping[str, Any], name: str, default: bool = False) -> bool:
    """Прочитать булев флаг из ЗАМОРОЖЕННОГО снимка, а не из окружения."""
    if name not in flags:
        return default
    raw = flags[name]
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def collect_feature_flags(env: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    """Снять routing-relevant флаги окружения ЦЕНТРА.

    Копируется не `.env` целиком, а закрытый список: в окружении лежат ключи и
    токены, и «снимок конфигурации» не должен становиться каналом их утечки на
    чужой VPS. Отсутствующий флаг в снимок не попадает — «не задан» и «задан
    пустым» разные утверждения, и дефолт обязан остаться дефолтом КОДА.
    """
    source = env if env is not None else os.environ
    snapshot: dict[str, str] = {}
    for name in registry.ROUTING_FEATURE_FLAGS:
        if name in source:
            snapshot[name] = str(source[name])
    return snapshot


def claude_default_model_class() -> str:
    """Класс глобальной ручки Claude на ЭТОТ момент. Без имени модели."""
    try:
        from backend.app.core import config as _cfg

        model = str(_cfg.get_claude_model() or "")
    except Exception:                                   # noqa: BLE001 — офлайн
        return registry.MODEL_CLASS_CHEAP
    low = model.lower()
    if "opus" in low:
        return registry.MODEL_CLASS_STRONG
    return registry.MODEL_CLASS_CHEAP


# ─── Компиляция отдельных этапов ─────────────────────────────────────────────
def _stage(stage_id: str, actions: list[RoutingAction], *, note: str = "") -> RoutingStage:
    pipeline_stage = PIPELINE_STAGE_OF[stage_id]
    return RoutingStage(
        stage_id=stage_id,
        pipeline_stage=pipeline_stage,
        execution_scope=SCOPE_OF_PIPELINE_STAGE[pipeline_stage],
        actions=tuple(actions),
        note=note,
    )


def _compile_block_batch(inputs: CompilerInputs, selector: presets.ModelSelector) -> RoutingStage:
    """Этап 01 «Блоки»: ансамбль детекторов + судья.

    Топология снята с `gemma_findings_only.py:2315-2485`: две обязательные ноги
    и одна за флагом уходят ОДНИМ `asyncio.gather`, затем детерминированное
    объединение, затем судья — но только если ответили ВСЕ ноги.
    """
    flags = inputs.feature_flags
    if not selector.is_ensemble:
        # Одиночный детектор: разрешённая, но не пресетная раскладка.
        if selector.provider is None or selector.capability is None:
            raise RoutingPlanError("block_batch: селектор не разобран")
        return _stage(
            "block_batch",
            [
                RoutingAction(
                    action_id="detector_single",
                    role=registry.ROLE_DETECTOR,
                    provider=selector.provider,
                    capability=registry.CAP_BLOCK_DETECTOR,
                    reasoning_effort=(
                        registry.EFFORT_LOW
                        if registry.effort_allowed(selector.provider) else None
                    ),
                    parallel_group=None,
                    multiplicity=RoutingMultiplicity.per_graphic_block(),
                    note="одиночный детектор: раскладка без ансамбля",
                ),
            ],
            note="ансамбль не выбран — судья и gap-search не выполняются",
        )

    actions: list[RoutingAction] = [
        RoutingAction(
            action_id="detector_openrouter",
            role=registry.ROLE_DETECTOR,
            provider=registry.PROVIDER_OPENROUTER,
            capability=registry.CAP_BLOCK_DETECTOR,
            reasoning_effort=registry.EFFORT_LOW,
            parallel_group="detectors",
            multiplicity=RoutingMultiplicity.per_graphic_block(),
            note="внешний шлюз, единственный платный вызов конвейера",
        ),
        RoutingAction(
            action_id="detector_codex_standard",
            role=registry.ROLE_DETECTOR,
            provider=registry.PROVIDER_CODEX,
            capability=registry.CAP_BLOCK_DETECTOR,
            reasoning_effort=registry.EFFORT_LOW,
            parallel_group="detectors",
            multiplicity=RoutingMultiplicity.per_graphic_block(),
        ),
    ]
    third_leg = _flag(flags, "STAGE01_THIRD_LEG_ENABLED", False)
    if third_leg:
        actions.append(
            RoutingAction(
                action_id="detector_codex_strong",
                role=registry.ROLE_DETECTOR,
                provider=registry.PROVIDER_CODEX,
                capability=registry.CAP_BLOCK_DETECTOR_STRONG,
                reasoning_effort=registry.EFFORT_LOW,
                parallel_group="detectors",
                condition=RoutingCondition.feature("STAGE01_THIRD_LEG_ENABLED"),
                multiplicity=RoutingMultiplicity.per_graphic_block(),
                note="третья нога: другая модель ловит другие находки",
            )
        )
    actions.append(
        RoutingAction(
            action_id="combine_detectors",
            role=registry.ROLE_DETECTOR_COMBINE,
            kind=registry.KIND_DETERMINISTIC,
            depends_on=("detectors",),
            multiplicity=RoutingMultiplicity.per_graphic_block(),
            note="combine_detector_results: объединение без дедупликации",
        )
    )
    if _flag(flags, "STAGE01_DUAL_REVIEW_ENABLED", True):
        actions.append(
            RoutingAction(
                action_id="judge_gap_search",
                role=registry.ROLE_JUDGE_GAP_SEARCH,
                provider=registry.PROVIDER_CODEX,
                capability=registry.CAP_BLOCK_JUDGE,
                depends_on=("combine_detectors",),
                # Судья пропускается целиком, если хоть одна нога не ответила.
                condition=RoutingCondition.of(registry.COND_DETECTORS_COMPLETE),
                multiplicity=RoutingMultiplicity.per_graphic_block(),
                note=(
                    "сопоставление находок И gap-search ОДНИМ обращением; "
                    "reasoning effort не задаётся — действует умолчание CLI"
                ),
            )
        )
    return _stage(
        "block_batch",
        actions,
        note=(
            "этап одинаков в обоих пресетах: строка таблицы у них совпадает "
            "(ensemble/gpt-codex)"
        ),
    )


def _compile_text_analysis(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """Этап 02 «Текст». Здесь проходит настоящая граница пресетов."""
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("text_analysis: селектор не разобран")
    codex = provider == registry.PROVIDER_CODEX
    return _stage(
        "text_analysis",
        [
            RoutingAction(
                action_id="text_audit",
                role=registry.ROLE_TEXT_AUDIT,
                provider=provider,
                capability=capability,
                multiplicity=(
                    # Codex-путь режет MD по листам, когда промпт не влезает в
                    # бюджет входа; Claude-путь не режет никогда.
                    RoutingMultiplicity.per_chunk(chunk_source="md_pages")
                    if codex else RoutingMultiplicity.per_document()
                ),
                note=(
                    "JSON-режим, нарезка по листам при превышении бюджета входа; "
                    "падение любого чанка — отказ этапа"
                    if codex else
                    "агентный режим с файловыми инструментами, один вызов"
                ),
            ),
        ],
    )


def _compile_findings_merge(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """Этап 03 «Свод». На Codex-пути к нему добавляются targeted-проходы."""
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("findings_merge: селектор не разобран")
    actions = [
        RoutingAction(
            action_id="merge_base",
            role=registry.ROLE_MERGE,
            provider=provider,
            capability=capability,
            multiplicity=RoutingMultiplicity.per_document(),
        ),
    ]
    # Targeted-проходов на Claude-пути НЕ существует: `_run_codex_targeted_
    # findings_merge` вызывается единственным местом — внутри codex-ветки.
    if provider == registry.PROVIDER_CODEX and _flag(
        inputs.feature_flags, "AUDIT_CODEX_TARGETED_FINDINGS", True
    ):
        discipline = str(inputs.discipline_id or "").upper()
        if discipline in TARGETED_DISCIPLINES:
            actions.append(
                RoutingAction(
                    action_id="targeted_discipline",
                    role=registry.ROLE_TARGETED_DISCIPLINE,
                    provider=provider,
                    capability=capability,
                    depends_on=("merge_base",),
                    # Дисциплина уже проверена компилятором (действие в план
                    # просто не попало бы), поэтому в условии остаётся то, что
                    # центру заранее неизвестно: сборщик проходов возвращает
                    # ПУСТОЙ список, если MD проекта не существует, — и тогда
                    # гаснут все три прохода разом.
                    condition=RoutingCondition.of(registry.COND_HAS_MD_FILE),
                    multiplicity=RoutingMultiplicity.per_document(),
                    note=(
                        "дисциплинарный проход (дисциплина входит в "
                        f"{list(TARGETED_DISCIPLINES)}); отказ нефатален"
                    ),
                )
            )
        actions.append(
            RoutingAction(
                action_id="targeted_docnorm",
                role=registry.ROLE_TARGETED_DOCNORM,
                provider=provider,
                capability=capability,
                depends_on=("merge_base",),
                condition=RoutingCondition.of(registry.COND_HAS_MD_FILE),
                multiplicity=RoutingMultiplicity.per_document(),
                note="безусловный при наличии MD; отказ нефатален",
            )
        )
        if _flag(inputs.feature_flags, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", False):
            actions.append(
                RoutingAction(
                    action_id="targeted_mark_system",
                    role=registry.ROLE_TARGETED_MARK_SYSTEM,
                    provider=provider,
                    capability=capability,
                    depends_on=("merge_base",),
                    # Флаг уже применён компилятором; остаётся условие данных,
                    # общее для всех targeted-проходов.
                    condition=RoutingCondition.of(registry.COND_HAS_MD_FILE),
                    multiplicity=RoutingMultiplicity.per_document(),
                    note="за флагом FINDING_EVIDENCE_OCR_OBSERVER_ENABLED",
                )
            )
    return _stage(
        "findings_merge",
        actions,
        note=(
            "targeted-проходы существуют только на Codex-пути и идут строго "
            "последовательно после базового свода"
        ),
    )


def _compile_findings_critic(inputs: CompilerInputs) -> RoutingStage:
    """Верификатор. Обращений к модели — ноль, и это не оговорка.

    `run_deterministic_critic` вызывается с `llm_call=None` литералом; строка
    таблицы `findings_critic` конвейером не читается вообще.
    """
    if not _flag(inputs.feature_flags, "PIPELINE_VERIFIER_ENABLED", True):
        # Килсвитч гасит стадию ЦЕЛИКОМ, до первой фазы. Оставить в плане
        # действие с условием на тот же флаг значило бы описать шаг, которого
        # не будет: флаг заморожен, ответ известен уже сейчас.
        return _stage("findings_critic", [], note="этап выключен PIPELINE_VERIFIER_ENABLED")
    return _stage(
        "findings_critic",
        [
            RoutingAction(
                action_id="structural_checks",
                role=registry.ROLE_STRUCTURAL_CRITIC,
                kind=registry.KIND_DETERMINISTIC,
                multiplicity=RoutingMultiplicity.per_document(),
                note="run_deterministic_critic(llm_call=None): 0 обращений к модели",
            ),
        ],
        note="строка таблицы моделей для этого этапа не читается конвейером",
    )


def _compile_findings_corrector(inputs: CompilerInputs) -> RoutingStage:
    """Верификатор (фикс) = страж отсутствия. ВСЕГДА Claude, в любом пресете.

    Модель берётся из глобальной ручки `get_claude_model()`, а не из строки
    таблицы. В пресете «Full Codex» таблица обещает Codex — рантайм идёт на
    Claude. План повторяет рантайм.
    """
    capability = registry.CLASS_TO_CAPABILITY.get(
        inputs.claude_default_model_class, registry.CAP_CHEAP_REVIEW
    )
    if not _flag(inputs.feature_flags, "PIPELINE_VERIFIER_ENABLED", True):
        # Тот же килсвитч: обе фазы живут внутри одной стадии `findings_review`
        # и гаснут вместе. Страж отсутствия — самое дорогое из того, что здесь
        # гаснет, и бюджет обязан это учитывать.
        return _stage(
            "findings_corrector", [],
            note="этап выключен PIPELINE_VERIFIER_ENABLED",
        )
    return _stage(
        "findings_corrector",
        [
            RoutingAction(
                action_id="apply_verdicts",
                role=registry.ROLE_DETERMINISTIC_FIX,
                kind=registry.KIND_DETERMINISTIC,
                multiplicity=RoutingMultiplicity.per_document(),
                note="run_deterministic_corrector: применение вердиктов, 0 вызовов",
            ),
            RoutingAction(
                action_id="absence_guard",
                role=registry.ROLE_ABSENCE_GUARD,
                provider=registry.PROVIDER_CLAUDE,
                capability=capability,
                depends_on=("apply_verdicts",),
                condition=RoutingCondition.of(registry.COND_HAS_ABSENCE_CANDIDATES),
                multiplicity=RoutingMultiplicity.per_chunk(chunk_source="md_chars"),
                note=(
                    "модель берётся из глобальной ручки центра, а НЕ из строки "
                    "таблицы: в «Full Codex» это тоже Claude"
                ),
            ),
        ],
    )


def _compile_norm_verify(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """04 Нормы. Проверка цитат — Python; модель нужна только на привязке пунктов."""
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("norm_verify: селектор не разобран")
    actions = [
        RoutingAction(
            action_id="paragraph_verification",
            role=registry.ROLE_NORM_PARAGRAPH_VERIFICATION,
            kind=registry.KIND_DETERMINISTIC,
            multiplicity=RoutingMultiplicity.per_document(),
            note=(
                "verify_paragraphs_native: локальный индекс норм, 0 вызовов. "
                "Ветка на модель существует только как обработка ИСКЛЮЧЕНИЯ "
                "native-пути и маршрутом не является"
            ),
        ),
    ]
    if _flag(inputs.feature_flags, "NORM_CLAUSE_BINDING_ENABLED", False):
        actions.append(
            RoutingAction(
                action_id="clause_binding",
                role=registry.ROLE_NORM_BINDING,
                provider=provider,
                capability=capability,
                condition=RoutingCondition.of(registry.COND_HAS_CLAUSE_BINDING_TARGETS),
                multiplicity=RoutingMultiplicity.per_batch(
                    batch_size=25, max_rounds=2, target_source="findings_without_clause"
                ),
                note="батчи по 25 целей последовательно, до 2 раундов на батч",
            )
        )
    return _stage("norm_verify", actions)


def _compile_norm_fix(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """04b Пересмотр. ДВА разных обращения, оба условные и последовательные."""
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("norm_fix: селектор не разобран")
    return _stage(
        "norm_fix",
        [
            RoutingAction(
                action_id="review_findings",
                role=registry.ROLE_NORM_REVIEW_FINDINGS,
                provider=provider,
                capability=capability,
                condition=RoutingCondition.of(
                    registry.COND_HAS_FINDINGS_NEEDING_REVISION
                ),
                multiplicity=RoutingMultiplicity.per_document(),
            ),
            RoutingAction(
                action_id="review_optimization",
                role=registry.ROLE_NORM_REVIEW_OPTIMIZATION,
                provider=provider,
                capability=capability,
                depends_on=("review_findings",),
                condition=RoutingCondition.of(registry.COND_HAS_OPTIMIZATION_ARTIFACT),
                multiplicity=RoutingMultiplicity.per_document(),
                note="отдельной строки в таблице моделей у этого шага нет",
            ),
        ],
    )


def _compile_norm_requote(inputs: CompilerInputs) -> RoutingStage:
    """norm_requote. Строки в интерфейсе нет; штатный путь — Python."""
    return _stage(
        "norm_requote",
        [
            RoutingAction(
                action_id="requote_native",
                role=registry.ROLE_NORM_REQUOTE,
                kind=registry.KIND_DETERMINISTIC,
                multiplicity=RoutingMultiplicity.per_document(),
                note=(
                    "requote_norms_native: локальный семантический поиск, 0 вызовов. "
                    "Codex-fallback ЯВНО отключён, Claude-fallback — только "
                    "обработка исключения"
                ),
            ),
        ],
        note="строки в интерфейсе нет, значение таблицы оператору не видно",
    )


def _compile_optimization(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """Этап 05. Две ноги параллельно, объединение — Python. Одинаков в обоих пресетах."""
    if not selector.is_ensemble:
        provider = selector.provider
        capability = selector.capability
        if provider is None or capability is None:
            raise RoutingPlanError("optimization: селектор не разобран")
        return _stage(
            "optimization",
            [
                RoutingAction(
                    action_id="optimization_single",
                    role=registry.ROLE_OPTIMIZATION_PRIMARY,
                    provider=provider,
                    capability=capability,
                    multiplicity=RoutingMultiplicity.per_document(),
                ),
            ],
            note="ансамбль не выбран — вторая нога и детерминированный мерж отсутствуют",
        )
    images = _flag(inputs.feature_flags, "AUDIT_CODEX_OPTIMIZATION_IMAGES", True)
    return _stage(
        "optimization",
        [
            RoutingAction(
                action_id="optimization_primary",
                role=registry.ROLE_OPTIMIZATION_PRIMARY,
                provider=registry.PROVIDER_CLAUDE,
                capability=registry.CAP_STRONG_AUDIT,
                parallel_group="optimization_legs",
                multiplicity=RoutingMultiplicity.per_document(),
                note="без изображений: ветка Claude визуальный контекст не собирает",
            ),
            RoutingAction(
                action_id="optimization_visual",
                role=registry.ROLE_OPTIMIZATION_VISUAL,
                provider=registry.PROVIDER_CODEX,
                capability=registry.CAP_VISUAL_REASONING,
                reasoning_effort=registry.EFFORT_XHIGH,
                parallel_group="optimization_legs",
                multiplicity=RoutingMultiplicity.per_document(),
                note=(
                    "с PNG графических блоков"
                    if images else
                    "визуальный контекст выключен флагом — нога работает как текстовая"
                ),
            ),
            RoutingAction(
                action_id="optimization_merge",
                role=registry.ROLE_OPTIMIZATION_MERGE,
                kind=registry.KIND_DETERMINISTIC,
                depends_on=("optimization_legs",),
                multiplicity=RoutingMultiplicity.per_document(),
                note="merge_optimization_documents: консервативный дедуп, 0 вызовов",
            ),
        ],
        note="ансамбль одинаков в обоих пресетах, включая «Full Codex»",
    )


def _compile_optimization_critic(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("optimization_critic: селектор не разобран")
    actions = [
        RoutingAction(
            action_id="critic",
            role=registry.ROLE_OPTIMIZATION_CRITIC,
            provider=provider,
            capability=capability,
            multiplicity=RoutingMultiplicity.per_document(),
        ),
    ]
    if _flag(inputs.feature_flags, "OPTIMIZATION_CRITIC_DETERMINISTIC", False):
        actions.append(
            RoutingAction(
                action_id="critic_augment",
                role=registry.ROLE_CRITIC_AUGMENT,
                kind=registry.KIND_DETERMINISTIC,
                depends_on=("critic",),
                condition=RoutingCondition.feature("OPTIMIZATION_CRITIC_DETERMINISTIC"),
                multiplicity=RoutingMultiplicity.per_document(),
                note="run_deterministic_critic_augment: 100% покрытие вердиктами",
            )
        )
    return _stage("optimization_critic", actions)


def _compile_optimization_corrector(
    inputs: CompilerInputs, selector: presets.ModelSelector
) -> RoutingStage:
    """F OPT Fix. При включённом детерминированном режиме — НОЛЬ вызовов.

    Ранний `return` детерминированной ветки стоит ВЫШЕ агентного вызова, то есть
    агентная ветка при `OPTIMIZATION_CRITIC_DETERMINISTIC=true` недостижима.
    Изображать здесь модельное действие «для симметрии» значило бы завысить
    бюджет ровно на один оплаченный вызов на каждый прогон.
    """
    if _flag(inputs.feature_flags, "OPTIMIZATION_CRITIC_DETERMINISTIC", False):
        return _stage(
            "optimization_corrector",
            [
                RoutingAction(
                    action_id="deterministic_fix",
                    role=registry.ROLE_DETERMINISTIC_FIX,
                    kind=registry.KIND_DETERMINISTIC,
                    condition=RoutingCondition.of(registry.COND_HAS_CRITIC_ISSUES),
                    multiplicity=RoutingMultiplicity.per_document(),
                    note=(
                        "run_deterministic_corrector + ранний return: агентная "
                        "ветка недостижима, строка таблицы не читается"
                    ),
                ),
            ],
            note="строка таблицы моделей для этого этапа не читается конвейером",
        )
    provider = selector.provider
    capability = selector.capability
    if provider is None or capability is None:
        raise RoutingPlanError("optimization_corrector: селектор не разобран")
    return _stage(
        "optimization_corrector",
        [
            RoutingAction(
                action_id="agentic_fix",
                role=registry.ROLE_OPTIMIZATION_CRITIC,
                provider=provider,
                capability=capability,
                condition=RoutingCondition.of(registry.COND_HAS_CRITIC_ISSUES),
                multiplicity=RoutingMultiplicity.per_document(),
                note="агентная ветка: достижима только при выключенном детерминизме",
            ),
        ],
    )


# ─── Публичная точка входа ───────────────────────────────────────────────────
class AuditRoutingPlanCompiler:
    """Единственный источник планов маршрутизации.

    Ни фронтенд, ни `PipelineManager`, ни воркер планов НЕ строят: фронтенд
    выбирает пресет, остальные — читают готовое. Иначе «что запустили» имело бы
    три ответа, и все три расходились бы по-своему.
    """

    def compile(self, inputs: CompilerInputs) -> RoutingPlan:
        parsed = presets.validate_stage_models(inputs.stage_models)
        if inputs.claude_default_model_class not in registry.KNOWN_MODEL_CLASSES:
            raise RoutingPlanError(
                f"claude_default_model_class={inputs.claude_default_model_class!r}: "
                f"допустимы {list(registry.KNOWN_MODEL_CLASSES)}"
            )

        stages = [
            _compile_block_batch(inputs, parsed["block_batch"]),
            _compile_text_analysis(inputs, parsed["text_analysis"]),
            _compile_findings_merge(inputs, parsed["findings_merge"]),
            _compile_findings_critic(inputs),
            _compile_findings_corrector(inputs),
            _compile_norm_verify(inputs, parsed["norm_verify"]),
            _compile_norm_fix(inputs, parsed["norm_fix"]),
            _compile_norm_requote(inputs),
            _compile_optimization(inputs, parsed["optimization"]),
            _compile_optimization_critic(inputs, parsed["optimization_critic"]),
            _compile_optimization_corrector(inputs, parsed["optimization_corrector"]),
        ]

        flags = dict(inputs.feature_flags)
        flags[registry.GLOBAL_CLAUDE_DEFAULT_MODEL_CLASS] = inputs.claude_default_model_class

        plan = RoutingPlan(
            preset_id=presets.detect_preset(
                inputs.stage_models, codex_model_id=inputs.codex_model_id
            ),
            stages=tuple(stages),
            feature_flags=tuple(sorted((str(k), v) for k, v in flags.items())),
            pipeline_revision=str(inputs.pipeline_revision or ""),
            routing_plan_id=f"rp_{uuid.uuid4().hex[:16]}",
            created_at=time.time(),
        )
        validator.validate(plan)
        return plan


def compile_from_center(
    *,
    discipline_id: str = "",
    pipeline_revision: str = "",
    stage_models: Optional[Mapping[str, str]] = None,
    env: Optional[Mapping[str, str]] = None,
) -> RoutingPlan:
    """Скомпилировать план из ТЕКУЩЕГО состояния центра.

    Вызывается в момент создания задания и только тогда. Всё, что меняется
    позже, — уже не про это задание.
    """
    from backend.app.core import config as _cfg

    models = (
        dict(stage_models)
        if stage_models is not None
        else {str(k): str(v) for k, v in (getattr(_cfg, "STAGE_MODEL_CONFIG", {}) or {}).items()}
    )
    inputs = CompilerInputs(
        stage_models=models,
        feature_flags=collect_feature_flags(env),
        claude_default_model_class=claude_default_model_class(),
        discipline_id=discipline_id,
        codex_model_id=str(
            getattr(_cfg, "CODEX_STAGE_MODEL_ID", "codex/gpt-6-astra")
        ),
        pipeline_revision=pipeline_revision,
    )
    return AuditRoutingPlanCompiler().compile(inputs)
