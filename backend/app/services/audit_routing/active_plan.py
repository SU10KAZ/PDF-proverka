"""План маршрутизации ТЕКУЩЕГО прогона: единственная точка чтения для этапов.

Зачем модуль-держатель, а не параметр этапа.

Этапы конвейера получают маршрут не из аргументов: `get_stage_model(stage)`
читается прямо внутри стадии, а флаги ансамбля — из окружения на импорте
модуля. Протащить план параметром через все стадии значило бы переписать их
сигнатуры целиком, а это не то изменение, за которое стоит платить на 11I:
план должен ЗАМЕНИТЬ источник, а не добавить второй.

Поэтому держатель повторяет форму того, что заменяет: одно место, откуда
стадия спрашивает «что мне делать». Разница в том, что значение сюда
устанавливается ОДИН раз — при запуске прогона, из задания, — и дальше не
меняется. Ровно это и есть заморозка.

Что модуль НЕ делает: не читает файлы, не лезет в окружение и не строит план
сам. Отсутствие плана — законное состояние (классический прогон на центре до
11I), и все читатели обязаны его переживать, возвращаясь к прежнему поведению.
"""
from __future__ import annotations

import threading
from typing import Any, Optional

from backend.app.services.audit_routing import registry
from backend.app.services.audit_routing.plan import RoutingAction, RoutingPlan

_lock = threading.Lock()
_plan: Optional[RoutingPlan] = None


def set_plan(plan: Optional[RoutingPlan]) -> None:
    """Установить план прогона. Вызывается один раз на процесс."""
    global _plan
    with _lock:
        _plan = plan


def clear() -> None:
    set_plan(None)


def get_plan() -> Optional[RoutingPlan]:
    with _lock:
        return _plan


def plan_hash() -> str:
    plan = get_plan()
    return plan.plan_hash() if plan is not None else ""


def stage_actions(stage_id: str) -> tuple[RoutingAction, ...]:
    """Действия этапа. Пустой кортеж — плана нет либо этапа в нём нет."""
    plan = get_plan()
    if plan is None:
        return ()
    stage = plan.stage(stage_id)
    return tuple(stage.actions) if stage is not None else ()


def block_detector_legs() -> tuple[RoutingAction, ...]:
    """Ноги детектора этапа 01 в порядке плана."""
    return tuple(
        item for item in stage_actions("block_batch")
        if item.role == registry.ROLE_DETECTOR and item.is_model
    )


def block_judge_action() -> Optional[RoutingAction]:
    """Действие судьи этапа 01, если план его содержит."""
    for item in stage_actions("block_batch"):
        if item.role == registry.ROLE_JUDGE_GAP_SEARCH and item.is_model:
            return item
    return None


def optimization_legs() -> tuple[RoutingAction, ...]:
    """Ноги этапа 05 в порядке плана."""
    return tuple(
        item for item in stage_actions("optimization")
        if item.is_model and item.role in (
            registry.ROLE_OPTIMIZATION_PRIMARY, registry.ROLE_OPTIMIZATION_VISUAL
        )
    )


def single_action(stage_id: str, role: str) -> Optional[RoutingAction]:
    """Единственное модельное действие этапа с данной ролью."""
    for item in stage_actions(stage_id):
        if item.is_model and item.role == role:
            return item
    return None


def describe() -> dict[str, Any]:
    """Короткое описание для журнала и evidence. Без точных моделей."""
    plan = get_plan()
    if plan is None:
        return {"active": False}
    return {
        "active": True,
        "preset_id": plan.preset_id,
        "routing_plan_id": plan.routing_plan_id,
        "routing_plan_hash": plan.plan_hash(),
        "stages": {
            stage.stage_id: [
                {
                    "action_id": item.action_id,
                    "role": item.role,
                    "kind": item.kind,
                    "provider": item.provider,
                    "capability": item.capability,
                    "reasoning_effort": item.reasoning_effort,
                    "parallel_group": item.parallel_group,
                }
                for item in stage.actions
            ]
            for stage in plan.stages
        },
    }


#: Синтетический идентификатор «модели» для провенанса ноги плана.
#:
#: Точной модели на воркере конвейер не знает и знать не должен — её выбирает
#: локальная политика. Но провенанс находки обязан различать ноги, иначе
#: «нашли три детектора» и «нашёл один» выглядят одинаково.
def leg_model_label(action: RoutingAction) -> str:
    return f"provider/plan:{action.provider}:{action.action_id}"
