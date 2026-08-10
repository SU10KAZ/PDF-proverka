"""Пара «провайдер + способность» → модель ЦЕНТРА (этап 11J).

Зачем понадобилась обратная сторона реестра.

План маршрутизации не несёт идентификаторов моделей, и это его главное
свойство: точную строку выбирает та машина, которая платит (см.
`registry.looks_like_exact_model` и машинный запрет в валидаторе). На воркере
выбор делает локальная политика администратора VPS.

Но центральный хвост — тоже исполнитель, и ему тоже нужно чем-то звать модель.
До 11J он брал строку из ТЕКУЩЕЙ глобальной таблицы `stage_models.json`, то
есть из мутабельного состояния процесса: оператор, переключивший пресет между
возвратом результата воркера и началом нормативного этапа, менял провайдера
привязки нормативных пунктов уже идущего задания (KI-11I-3).

Этот модуль — «локальная политика моделей ЦЕНТРА», ровно в той же роли, в
какой `audit_worker/providers/model_policy.py` служит воркеру. Разница ровно
одна: центр — своя машина, и его политика может быть константами кода, а не
файлом администратора.

Почему константы, а не чтение таблицы. Смысл всего 11J в том, что маршрут
задания заморожен. Если бы центр разрешал способность через ту же таблицу,
которую оператор правит из интерфейса, заморозка кончалась бы на границе
worker/center — и переключённый пресет по-прежнему менял бы хвост уже идущего
аудита. Значения совпадают с эталонными раскладками пресетов
(`presets.reference_config`), и расхождение ловит отдельный тест.
"""
from __future__ import annotations

from typing import Optional

from backend.app.services.audit_routing import registry

#: Строки Claude. Совпадают с эталоном пресета «Claude+GPT+Codex» дословно.
CLAUDE_STRONG_MODEL = "claude-opus-5"
CLAUDE_CHEAP_MODEL = "claude-sonnet-5"

#: Модель, которую центр применяет к внешнему шлюзу для текстовых этапов.
#: Отдельно от ноги детектора: у той свой класс и своя цена.
OPENROUTER_STRONG_MODEL = "openai/gpt-5.4"
OPENROUTER_CHEAP_MODEL = "openai/gpt-5.4"
OPENROUTER_BLOCK_DETECTOR_MODEL = "openai/gpt-5.4"


def codex_model_id() -> str:
    """Строка Codex центра. Читается из окружения, как и всё про Codex.

    Это не «мутабельная ручка»: `AUDIT_CODEX_STAGE_MODEL` задаётся при запуске
    процесса и интерфейсом не переключается. Ровно та же строка, которой
    компилятор раскрывает плейсхолдер `__codex_exec__` пресета.
    """
    from backend.app.core import config as _cfg

    return str(getattr(_cfg, "CODEX_STAGE_MODEL_ID", "") or "").strip() or "codex/gpt-5.4"


def model_for(provider: Optional[str], capability: Optional[str]) -> str:
    """Модель центра для пары плана. Пустая строка — пары нет.

    Пустая строка, а не исключение и не умолчание: вызывающий (`get_stage_model`)
    обязан уметь пережить «плана на этот этап нет» возвратом к прежнему
    поведению. Умолчание же вида «возьмём сильную» вернуло бы ту самую тихую
    подмену модели, ради устранения которой заведены способности.
    """
    name = str(provider or "").strip()
    cap = str(capability or "").strip()
    if not name or not cap:
        return ""
    if name == registry.PROVIDER_CLAUDE:
        if cap == registry.CAP_STRONG_AUDIT:
            return CLAUDE_STRONG_MODEL
        if cap == registry.CAP_CHEAP_REVIEW:
            return CLAUDE_CHEAP_MODEL
        return ""
    if name == registry.PROVIDER_CODEX:
        # У Codex один класс модели на все способности: подписка предлагает
        # одну рабочую модель, и «дешёвая» от «сильной» там не отличается.
        # Притворяться, что отличается, значило бы описывать не рантайм.
        return codex_model_id()
    if name == registry.PROVIDER_OPENROUTER:
        if cap == registry.CAP_BLOCK_DETECTOR:
            return OPENROUTER_BLOCK_DETECTOR_MODEL
        if cap == registry.CAP_STRONG_AUDIT:
            return OPENROUTER_STRONG_MODEL
        if cap == registry.CAP_CHEAP_REVIEW:
            return OPENROUTER_CHEAP_MODEL
        return ""
    return ""


#: Строка таблицы моделей → роль ОСНОВНОГО действия этапа.
#:
#: Нужна там, где этап содержит несколько модельных действий, а вопрос «какая
#: у него модель» всё же осмыслен: свод на Codex-пути — это базовое обращение
#: плюс targeted-проходы, и модель этапа — модель базового. Без явной роли
#: пришлось бы либо выбирать первое попавшееся действие, либо отказываться
#: отвечать — и второе вернуло бы этап к глобальной таблице, то есть к
#: незамороженному маршруту.
#:
#: Этапы-АНСАМБЛИ (01 и 05) сюда не входят намеренно: у них ноги разных
#: провайдеров, «модель этапа» для них не определена, и выбирать одну за
#: вызывающего нельзя. Такие этапы читают план через `active_plan`
#: поимённо — `block_detector_legs()`, `optimization_legs()`.
PRIMARY_ROLE_OF_STAGE: dict[str, str] = {
    "text_analysis": registry.ROLE_TEXT_AUDIT,
    "findings_merge": registry.ROLE_MERGE,
    "findings_critic": registry.ROLE_STRUCTURAL_CRITIC,
    "findings_corrector": registry.ROLE_ABSENCE_GUARD,
    "norm_verify": registry.ROLE_NORM_BINDING,
    "norm_fix": registry.ROLE_NORM_REVIEW_FINDINGS,
    "optimization_critic": registry.ROLE_OPTIMIZATION_CRITIC,
}


def stage_model_from_plan(stage_id: str) -> str:
    """Модель этапа по ЗАМОРОЖЕННОМУ плану текущего прогона.

    Пустая строка означает: плана нет, этапа в плане нет, основное действие
    этапа не модельное либо этап — ансамбль. Во всех случаях вызывающий
    работает как до 11J, читая глобальную таблицу.
    """
    from backend.app.services.audit_routing import active_plan

    plan = active_plan.get_plan()
    if plan is None:
        return ""
    key = str(stage_id)
    stage = plan.stage(key)
    if stage is None:
        return ""
    actions = [item for item in stage.actions if item.is_model]
    if not actions:
        return ""
    if len(actions) > 1:
        role = PRIMARY_ROLE_OF_STAGE.get(key)
        if not role:
            return ""
        actions = [item for item in actions if item.role == role]
        if len(actions) != 1:
            return ""
    return model_for(actions[0].provider, actions[0].capability)


__all__ = [
    "CLAUDE_STRONG_MODEL",
    "CLAUDE_CHEAP_MODEL",
    "OPENROUTER_STRONG_MODEL",
    "OPENROUTER_BLOCK_DETECTOR_MODEL",
    "codex_model_id",
    "model_for",
    "stage_model_from_plan",
]
