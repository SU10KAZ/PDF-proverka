"""Оценка числа обращений к модели ПО ПЛАНУ, а не по константе рядом с ним.

Что чинится.

До 11I бюджет считался формулой `graphic_blocks + 6 + max(3, ⌈N×0.10⌉)` с
потолком 64. В ней ровно ОДИН вызов на графический блок — при том что этап 01
делает четыре (три ноги ансамбля плюс судья). Документ на сорок графических
блоков требует около 165 обращений, а контракт авторизовал максимум 64: аудит
упирался в потолок на середине, УЖЕ оплатив две трети вызовов, и в журнале это
выглядело как ошибка этапа, а не как исчерпанный бюджет.

Формула не «занижена» — она описывает другую топологию: одноногий worker-участок,
в который ансамбль схлопывал мост. Как только ансамбль перестаёт схлопываться,
формула обязана выводиться из плана.

Второе, что здесь появляется: разбивка ПО ПРОВАЙДЕРАМ. Три провайдера тратят
три разные подписки (и одна из них — деньги), а один общий итог не отвечает на
вопрос «хватит ли квоты Codex до утра».

Повторы в естественную оценку НЕ входят. Повтор после таймаута — не вторая нога
ансамбля, и смешение этих понятий и породило бюджет, где ансамбль из трёх ног
выглядел как один вызов с запасом на ошибки.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional

from backend.app.services.audit_routing import registry
from backend.app.services.audit_routing.plan import RoutingAction, RoutingPlan


@dataclass(frozen=True)
class DocumentShape:
    """Что известно о документе на момент оценки.

    `graphic_blocks=None` — честный ответ «структуру прочитать не удалось», а не
    ноль: ноль означал бы бюджет без этапа 01, и первый же блок упёрся бы в
    потолок.
    """

    graphic_blocks: Optional[int] = None
    #: Оценка числа чанков по источникам нарезки (`chunk_source` действия).
    chunks: dict[str, int] = field(default_factory=dict)
    #: Оценка числа целей по источникам батчей (`target_source` действия).
    batch_targets: dict[str, int] = field(default_factory=dict)

    #: Потолок вслепую: худший случай, который центр готов авторизовать, не
    #: прочитав структуру. Совпадает с прежним `_BLIND_BLOCK_ESTIMATE`.
    BLIND_BLOCKS = 12

    @property
    def blocks(self) -> int:
        return self.BLIND_BLOCKS if self.graphic_blocks is None else int(self.graphic_blocks)

    @property
    def blind(self) -> bool:
        return self.graphic_blocks is None


#: Умолчания для неизвестных источников чанков/батчей. Единица — не «мало», а
#: «условное действие исполняется хотя бы раз»: занижать здесь опаснее, чем
#: завышать, потому что заниженный потолок обрывает уже оплаченный прогон.
_DEFAULT_CHUNKS = 1
_DEFAULT_BATCH_TARGETS = 0


def action_calls(action: RoutingAction, shape: DocumentShape) -> int:
    """Сколько ЛОГИЧЕСКИХ обращений к модели даёт одно действие.

    Детерминированное действие даёт ноль — и это утверждение плана, а не
    приближение. Изображать здесь вызов «для симметрии структуры» значило бы
    завысить бюджет ровно на число детерминированных шагов.
    """
    if not action.is_model:
        return 0
    mult = action.multiplicity
    if mult.type == registry.MULT_PER_DOCUMENT:
        return 1
    if mult.type == registry.MULT_PER_GRAPHIC_BLOCK:
        return shape.blocks
    if mult.type == registry.MULT_PER_CHUNK:
        source = str(mult.param("chunk_source") or "")
        count = int(shape.chunks.get(source, _DEFAULT_CHUNKS))
        cap = mult.param("max_chunks")
        if isinstance(cap, int) and cap > 0:
            count = min(count, cap)
        return max(0, count)
    if mult.type == registry.MULT_PER_BATCH:
        source = str(mult.param("target_source") or "")
        targets = int(shape.batch_targets.get(source, _DEFAULT_BATCH_TARGETS))
        size = max(1, int(mult.param("batch_size") or 1))
        rounds = max(1, int(mult.param("max_rounds") or 1))
        return math.ceil(targets / size) * rounds
    return 0


def estimate(
    plan: RoutingPlan,
    shape: DocumentShape,
    *,
    scope: Optional[str] = None,
) -> dict[str, Any]:
    """Оценка по плану. `scope=None` — весь аудит, иначе только этот участок.

    Возвращает не одно число, а разбивку: по провайдерам, по этапам и отдельно
    «обязательные» против «условных». Оператор, увидевший «169», не может
    решить, много это или мало; увидевший «160 из них — этап 01» — может.
    """
    per_provider: dict[str, int] = {p: 0 for p in registry.KNOWN_PROVIDERS}
    per_stage: dict[str, int] = {}
    per_action: list[dict[str, Any]] = []
    mandatory = 0
    conditional = 0

    for stage, action in plan.iter_actions():
        if scope is not None and stage.execution_scope != scope:
            continue
        calls = action_calls(action, shape)
        per_stage.setdefault(stage.stage_id, 0)
        if calls and action.provider:
            per_provider[action.provider] = per_provider.get(action.provider, 0) + calls
            per_stage[stage.stage_id] += calls
        if action.is_model:
            if action.condition.is_always:
                mandatory += calls
            else:
                conditional += calls
            per_action.append({
                "stage_id": stage.stage_id,
                "action_id": action.action_id,
                "role": action.role,
                "provider": action.provider,
                "capability": action.capability,
                "multiplicity": action.multiplicity.type,
                "calls": calls,
                "conditional": not action.condition.is_always,
            })

    natural = mandatory + conditional
    return {
        "scope": scope or "all",
        "graphic_blocks": None if shape.blind else int(shape.blocks),
        "blind_estimate": shape.blind,
        "natural_calls": natural,
        "mandatory_calls": mandatory,
        "conditional_calls": conditional,
        "per_provider": {k: v for k, v in sorted(per_provider.items()) if v},
        "per_stage": {k: v for k, v in sorted(per_stage.items()) if v},
        "per_action": per_action,
        "deterministic_actions": sum(
            1 for _s, a in plan.iter_actions() if not a.is_model
        ),
    }


def technical_retry_headroom(natural_calls: int) -> int:
    """Запас на ТЕХНИЧЕСКИЕ повторы: `max(3, ⌈N × 0.10⌉)`.

    Значение и формула сохранены с 11H дословно. Повторы по КАЧЕСТВУ этим
    запасом не покрываются: в бюджет входят только повторы после таймаута,
    обрыва транспорта и ответа, непригодного к разбору.
    """
    return max(3, math.ceil(max(0, int(natural_calls)) * 0.10))


def worker_budget(
    plan: RoutingPlan,
    shape: DocumentShape,
    *,
    ceiling: Optional[int] = None,
) -> dict[str, Any]:
    """Бюджет worker-участка: естественные вызовы + запас, зажатые потолком.

    Потолок теперь ОБЯЗАН выводиться из топологии. Прежние 64 были не рубежом,
    а работающим обрывом: при трёх ногах и судье их не хватает уже на документ
    из пятнадцати графических блоков.
    """
    detail = estimate(plan, shape, scope=registry.SCOPE_WORKER)
    natural = int(detail["natural_calls"])
    headroom = technical_retry_headroom(natural)
    requested = natural + headroom
    effective_ceiling = int(ceiling) if ceiling is not None else requested
    budget = max(1, min(effective_ceiling, requested))
    return {
        **detail,
        "technical_retry_headroom": headroom,
        "requested": requested,
        "ceiling": effective_ceiling,
        "max_inferences": budget,
        "clamped_by_ceiling": bool(requested > effective_ceiling),
        "formula": (
            f"{natural} естественных обращений по плану "
            f"({detail['mandatory_calls']} обязательных + "
            f"{detail['conditional_calls']} условных) + {headroom} на "
            f"технические повторы (max(3, ceil(N×0.10)))"
        ),
    }
