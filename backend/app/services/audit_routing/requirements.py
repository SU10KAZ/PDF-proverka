"""Требования плана к воркеру и проверка совместимости ДО выдачи задания.

Почему одной пары «провайдер + способность» больше не хватает.

Контракт 11G нёс `provider=claude, capability=strong_audit` на весь
worker-участок. Фактический пресет требует трёх провайдеров и шести классов
моделей одновременно: OpenRouter — на GPT-ногу детектора, Codex — на две
оставшиеся ноги, судью, визуальную ногу оптимизации и текстовые этапы, Claude —
на стража отсутствия и основную ногу оптимизации. Заказав `provider=codex`,
центр терял Claude-ногу; заказав `claude` — терял всё остальное.

Отсюда два свойства этого модуля.

**Требования выводятся из плана, а не задаются рядом с ним.** Список, живущий
отдельно, разъезжается с планом на первой же правке — ровно так «центр умеет
принять требование» и «центр его формирует» разъехались до 11G.

**Условные действия разделены на два класса.** Условие, которое центр может
вычислить уже при создании задания (дисциплина, замороженный флаг), либо
снимает требование, либо подтверждает его. Условие, зависящее от будущего
результата этапа, требует способность ЗАРАНЕЕ: узнать о нехватке провайдера в
середине оплаченного прогона — это и есть та самая тихая деградация, ради
запрета которой всё и делается.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from backend.app.services.audit_routing import registry
from backend.app.services.audit_routing.plan import RoutingPlan


@dataclass(frozen=True)
class CapabilityRequirement:
    """Одна пара «провайдер + способность», нужная плану."""

    provider: str
    capability: str
    #: `True` — действие выполняется всегда; `False` — только при условии,
    #: которое центр заранее не вычисляет.
    required_always: bool
    #: Действия плана, из-за которых требование возникло. Для сообщения
    #: оператору: «чего именно не хватает и ради чего».
    actions: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "capability": self.capability,
            "required_always": self.required_always,
            "actions": list(self.actions),
        }


def extract(plan: RoutingPlan, *, scope: Optional[str] = registry.SCOPE_WORKER) -> list[CapabilityRequirement]:
    """Какие пары «провайдер + способность» нужны плану в этой области.

    `scope=None` — весь аудит целиком (нужно для отчёта оператору);
    `scope="worker"` — то, что обязан уметь удалённый исполнитель.
    """
    buckets: dict[tuple[str, str], list[tuple[str, bool]]] = {}
    for stage, action in plan.model_actions():
        if scope is not None and stage.execution_scope != scope:
            continue
        if not action.provider or not action.capability:
            continue
        key = (action.provider, action.capability)
        # Условие, разрешимое при создании задания, УЖЕ применено компилятором:
        # действия, которых не будет, в план не попали. Значит оставшиеся
        # условные действия — это те, чей ответ появится только по ходу
        # прогона, и способность для них обязана быть заранее.
        always = action.condition.is_always or action.condition.resolvable_at_creation
        buckets.setdefault(key, []).append(
            (f"{stage.stage_id}.{action.action_id}", always)
        )
    out: list[CapabilityRequirement] = []
    for (provider, capability), items in sorted(buckets.items()):
        out.append(
            CapabilityRequirement(
                provider=provider,
                capability=capability,
                required_always=any(flag for _a, flag in items),
                actions=tuple(sorted(a for a, _f in items)),
            )
        )
    return out


def as_payload(requirements: list[CapabilityRequirement]) -> list[dict[str, Any]]:
    """Сгруппированный вид «провайдер → способности» для нагрузки задания."""
    grouped: dict[str, dict[str, Any]] = {}
    for item in requirements:
        block = grouped.setdefault(
            item.provider, {"provider": item.provider, "capabilities": []}
        )
        block["capabilities"].append(
            {"capability": item.capability, "required_always": item.required_always}
        )
    for block in grouped.values():
        block["capabilities"].sort(key=lambda c: c["capability"])
    return [grouped[key] for key in sorted(grouped)]


def declared_capabilities(worker_capabilities: Mapping[str, Any]) -> dict[str, list[str]]:
    """Что воркер объявил о себе. Молчание — пустой словарь, а не «наверное умеет»."""
    declared = worker_capabilities.get("provider_capabilities")
    if not isinstance(declared, dict):
        return {}
    out: dict[str, list[str]] = {}
    for provider, caps in declared.items():
        if isinstance(caps, list):
            out[str(provider)] = [str(c) for c in caps]
    return out


@dataclass(frozen=True)
class CompatibilityVerdict:
    """Может ли этот воркер исполнить этот план. Со списком причин, не с флагом."""

    compatible: bool
    missing: tuple[CapabilityRequirement, ...] = ()
    satisfied: tuple[CapabilityRequirement, ...] = ()
    reasons: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "compatible": self.compatible,
            "missing": [m.as_dict() for m in self.missing],
            "satisfied": [s.as_dict() for s in self.satisfied],
            "reasons": list(self.reasons),
        }


def check_worker(
    plan: RoutingPlan,
    worker_capabilities: Mapping[str, Any],
    *,
    scope: str = registry.SCOPE_WORKER,
) -> CompatibilityVerdict:
    """Проверить воркера против плана. Отказ — ДО создания задания.

    Молчаливого понижения здесь нет и быть не может. «OpenRouter недоступен —
    пропустим ногу» превращает трёхногий ансамбль в двуногий незаметно для
    оператора, а «заменим на Codex» — ещё и делает вид, что состав тот же.
    Правильный ответ на нехватку провайдера — отказать в назначении задания.
    """
    reasons: list[str] = []
    if not bool(worker_capabilities.get("real_llm_enabled")):
        reasons.append(
            "на воркере выключены настоящие модели (AUDIT_WORKER_ALLOW_REAL_LLM=false)"
        )
    if not bool(worker_capabilities.get("pipeline_provider_bridge_enabled")):
        reasons.append(
            "на воркере не разрешён мост конвейера к провайдеру "
            "(AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED=false)"
        )

    declared = declared_capabilities(worker_capabilities)
    if not declared and not reasons:
        reasons.append(
            "воркер не объявляет способностей провайдеров: локальной политики "
            "моделей на машине нет либо она не читается"
        )

    required = extract(plan, scope=scope)
    missing: list[CapabilityRequirement] = []
    satisfied: list[CapabilityRequirement] = []
    for item in required:
        offered = declared.get(item.provider) or []
        if item.capability in offered:
            satisfied.append(item)
        else:
            missing.append(item)
            reasons.append(
                f"воркер не объявляет способность {item.capability!r} для "
                f"провайдера {item.provider!r} — она нужна действиям "
                f"{', '.join(item.actions)}"
            )
    return CompatibilityVerdict(
        compatible=not reasons,
        missing=tuple(missing),
        satisfied=tuple(satisfied),
        reasons=tuple(reasons),
    )


def explain(verdict: CompatibilityVerdict) -> str:
    """Человеческое объяснение отказа. Без JSON-простыни в первой строке."""
    if verdict.compatible:
        return "воркер объявляет все способности, которых требует план"
    head = "воркер не может исполнить план маршрутизации"
    return head + ": " + "; ".join(verdict.reasons)


def worker_capabilities_json(declared: Mapping[str, Any]) -> str:
    """Отладочное представление объявленных способностей."""
    return json.dumps(dict(declared), ensure_ascii=False, sort_keys=True)
