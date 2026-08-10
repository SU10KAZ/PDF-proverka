"""Строгая проверка плана маршрутизации. Невалидный план не стартует.

Почему проверка отдельным модулем и почему такая подробная.

Инвентаризация 11I нашла источник целого класса тихих отказов: `stage_models.json`
грузится БЕЗ валидации. Ручная правка файла — а он не закоммичен и правится на
проде руками — молча превращала ансамбль `ensemble/gpt-codex` в одну модель, и
аудит продолжал идти, просто хуже. Ошибка конфигурации, не приводящая к отказу,
хуже отказа: она портит результат, оставаясь незаметной.

План — единственная точка, через которую маршрут попадает в задание. Значит
именно здесь обязаны падать все ошибки конфигурации, и падать ДО того, как
задание создано, пакет собран и первый вызов оплачен.

Отдельно про пункт «в контракте центра нет точной модели». Это не стилистика:
строка задания, дошедшая до argv стороннего CLI, ломает инвариант I-P5, а
«центр назначает модель» означает распоряжение чужой подпиской. Поэтому запрет
проверяется машиной по ВСЕМ значениям плана, а не соблюдается по договорённости.
"""
from __future__ import annotations

from typing import Iterable

from backend.app.services.audit_routing import registry
from backend.app.services.audit_routing.plan import (
    RoutingAction,
    RoutingPlan,
    RoutingPlanError,
    RoutingStage,
)

#: Этапы, которые обязаны присутствовать в плане ЛЮБОГО пресета.
#:
#: Список — не «желательный набор», а перечень этапов, отсутствие которых
#: означает урезанный аудит. Этап 11F показал цену молчаливого пропуска: центр
#: объявлял аудит завершённым без нормативного этапа, и оператор узнавал об
#: этом из отчёта, в котором не было ни одной цитаты нормы.
MANDATORY_STAGES: tuple[str, ...] = (
    "block_batch",
    "text_analysis",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "norm_verify",
    "norm_fix",
    "norm_requote",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
)

#: Стадии конвейера, которые ЗАПРЕЩЕНО исполнять на воркере (E-19).
#: Дублирует `models.distributed_workers.CENTRAL_ONLY_STAGES`; расхождение
#: ловит отдельный тест.
CENTER_ONLY_PIPELINE_STAGES: tuple[str, ...] = (
    "norm_verify",
    "debt_control",
    "decision_carryover",
    "excel",
)


class RoutingPlanValidationError(RoutingPlanError):
    """План не прошёл проверку. Содержит ВСЕ найденные нарушения, не первое."""

    def __init__(self, problems: Iterable[str]) -> None:
        items = list(problems)
        self.problems = items
        super().__init__(
            "План маршрутизации отвергнут (%d нарушени%s):\n  • %s"
            % (
                len(items),
                "е" if len(items) == 1 else "й",
                "\n  • ".join(items),
            )
        )


def _check_action_shape(stage: RoutingStage, action: RoutingAction, out: list[str]) -> None:
    where = f"{stage.stage_id}.{action.action_id or '<без id>'}"
    if not action.action_id:
        out.append(f"{stage.stage_id}: у действия нет action_id")
    if action.kind not in registry.KNOWN_KINDS:
        out.append(f"{where}: неизвестный kind {action.kind!r}")
    if action.role not in registry.KNOWN_ROLES:
        out.append(f"{where}: неизвестная роль {action.role!r}")

    if action.kind == registry.KIND_DETERMINISTIC:
        # Детерминированное действие не «обычно не зовёт модель», а НЕ МОЖЕТ её
        # позвать. Провайдер у него — это либо ошибка компилятора, либо попытка
        # изобразить вызов ради единообразия структуры; и то и другое ломает
        # оценку бюджета, потому что 0 вызовов превращается в N.
        if action.provider:
            out.append(f"{where}: детерминированное действие с провайдером {action.provider!r}")
        if action.capability:
            out.append(f"{where}: детерминированное действие со способностью")
        if action.reasoning_effort:
            out.append(f"{where}: детерминированное действие с reasoning_effort")
        if action.role not in registry.DETERMINISTIC_ROLES:
            out.append(f"{where}: роль {action.role!r} не является детерминированной")
        return

    # Дальше — модельное действие.
    if action.role not in registry.MODEL_ROLES:
        out.append(f"{where}: роль {action.role!r} не является модельной")
    if not action.provider:
        out.append(f"{where}: модельное действие без провайдера")
    elif action.provider not in registry.KNOWN_PROVIDERS:
        out.append(f"{where}: неизвестный провайдер {action.provider!r}")
    if not action.capability:
        out.append(f"{where}: модельное действие без способности")
    elif action.capability not in registry.KNOWN_CAPABILITIES:
        out.append(f"{where}: неизвестная способность {action.capability!r}")
    if (
        action.provider in registry.KNOWN_PROVIDERS
        and action.capability in registry.KNOWN_CAPABILITIES
        and not registry.capability_allowed(action.provider, action.capability)
    ):
        out.append(
            f"{where}: провайдер {action.provider!r} не предоставляет способность "
            f"{action.capability!r} (разрешены: "
            f"{list(registry.PROVIDER_CAPABILITIES.get(action.provider, ()))})"
        )
    if action.reasoning_effort:
        if action.reasoning_effort not in registry.KNOWN_EFFORTS:
            out.append(f"{where}: недопустимый reasoning_effort {action.reasoning_effort!r}")
        elif action.provider and not registry.effort_allowed(action.provider):
            # Claude CLI параметра reasoning effort не имеет. Молча его
            # проигнорировать значило бы, что план обещает одно, а происходит
            # другое — ровно та ложь, ради устранения которой план и вводится.
            out.append(
                f"{where}: провайдер {action.provider!r} не принимает reasoning_effort"
            )


def _check_condition_and_multiplicity(
    stage: RoutingStage, action: RoutingAction, out: list[str]
) -> None:
    where = f"{stage.stage_id}.{action.action_id or '<без id>'}"
    cond = action.condition
    if cond.type not in registry.KNOWN_CONDITIONS:
        out.append(f"{where}: неизвестный тип условия {cond.type!r}")
    else:
        allowed = set(registry.CONDITION_PARAMS.get(cond.type, ()))
        given = {k for k, _ in cond.params}
        extra = given - allowed
        if extra:
            out.append(f"{where}: условие {cond.type!r} — лишние параметры {sorted(extra)}")
        missing = allowed - given
        if missing:
            out.append(f"{where}: условие {cond.type!r} — нет параметров {sorted(missing)}")
        if cond.type == registry.COND_FEATURE_ENABLED:
            flag = dict(cond.params).get("flag")
            if flag not in registry.ROUTING_FEATURE_FLAGS:
                out.append(
                    f"{where}: условие ссылается на флаг {flag!r}, которого нет в "
                    "списке routing-relevant флагов — он не попадёт в снимок и "
                    "условие будет вычисляться по ЖИВОМУ окружению"
                )

    mult = action.multiplicity
    if mult.type not in registry.KNOWN_MULTIPLICITIES:
        out.append(f"{where}: неизвестная мультипликативность {mult.type!r}")
    else:
        allowed = set(registry.MULTIPLICITY_PARAMS.get(mult.type, ()))
        given = {k for k, _ in mult.params}
        extra = given - allowed
        if extra:
            out.append(
                f"{where}: мультипликативность {mult.type!r} — лишние параметры {sorted(extra)}"
            )
        if mult.type == registry.MULT_PER_BATCH:
            size = mult.param("batch_size")
            if not isinstance(size, int) or size <= 0:
                out.append(f"{where}: per_batch требует положительный batch_size")
            rounds = mult.param("max_rounds", 1)
            if not isinstance(rounds, int) or rounds <= 0:
                out.append(f"{where}: per_batch требует положительный max_rounds")


def _check_dependencies(plan: RoutingPlan, out: list[str]) -> None:
    """Зависимости, параллельные группы и отсутствие циклов.

    Зависимость разрешено выражать двумя способами: на конкретное действие и на
    параллельную группу целиком. Второе — не сахар: судья этапа 01 ждёт ВСЕ
    ноги, и перечислять их поимённо значило бы менять зависимость судьи каждый
    раз, когда третья нога включается или выключается флагом.
    """
    for stage in plan.stages:
        ids = {a.action_id for a in stage.actions if a.action_id}
        groups = {a.parallel_group for a in stage.actions if a.parallel_group}
        # Граф внутри этапа: узлы — действия, рёбра — depends_on.
        adjacency: dict[str, set[str]] = {}
        for action in stage.actions:
            targets: set[str] = set()
            for ref in action.depends_on:
                if ref in ids:
                    targets.add(ref)
                elif ref in groups:
                    targets.update(
                        a.action_id for a in stage.actions if a.parallel_group == ref
                    )
                else:
                    out.append(
                        f"{stage.stage_id}.{action.action_id}: зависимость {ref!r} "
                        "не разрешается ни в действие, ни в параллельную группу "
                        "этого этапа"
                    )
            targets.discard(action.action_id)
            adjacency[action.action_id] = targets

        # Поиск цикла обходом в глубину: цвет 1 — в стеке, 2 — закрыт.
        color: dict[str, int] = {}

        def visit(node: str, path: tuple[str, ...]) -> None:
            state = color.get(node, 0)
            if state == 1:
                cycle = " → ".join(path[path.index(node):] + (node,))
                out.append(f"{stage.stage_id}: цикл зависимостей {cycle}")
                return
            if state == 2:
                return
            color[node] = 1
            for nxt in sorted(adjacency.get(node, ())):
                visit(nxt, path + (node,))
            color[node] = 2

        for node in sorted(adjacency):
            visit(node, ())

        # Параллельная группа из одного действия — почти всегда ошибка
        # компиляции: «группа» перестала быть группой, когда флаг выключил
        # соседей, и топология молча выродилась в последовательность.
        for group in sorted(groups):
            members = [a for a in stage.actions if a.parallel_group == group]
            if len(members) < 2:
                out.append(
                    f"{stage.stage_id}: параллельная группа {group!r} содержит "
                    f"{len(members)} действие — это не группа"
                )
            # Все члены группы обязаны иметь ОДИНАКОВЫЙ набор зависимостей вовне
            # группы: иначе «параллельно» неисполнимо.
            outer = {
                frozenset(m.depends_on) - {group} for m in members
            }
            if len(outer) > 1:
                out.append(
                    f"{stage.stage_id}: члены группы {group!r} имеют разные "
                    "внешние зависимости — параллельное исполнение невозможно"
                )


def _check_no_exact_model(plan: RoutingPlan, out: list[str]) -> None:
    """Ни одно значение плана не должно выглядеть точным идентификатором модели."""

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")
        elif registry.looks_like_exact_model(node):
            out.append(
                f"{path}: значение {node!r} похоже на точный идентификатор модели. "
                "Центр называет только СПОСОБНОСТЬ; модель выбирает локальная "
                "политика воркера (инвариант I-P5)"
            )

    walk(plan.content_dict(), "")


def _check_scopes(plan: RoutingPlan, out: list[str]) -> None:
    for stage in plan.stages:
        if stage.execution_scope not in registry.KNOWN_SCOPES:
            out.append(
                f"{stage.stage_id}: недопустимая область исполнения "
                f"{stage.execution_scope!r}"
            )
        pipeline_stage = stage.pipeline_stage or stage.stage_id
        # Центральный этап, назначенный воркеру, — это не «оптимизация», а
        # потеря нормативной базы: на VPS нет ни норм-БД, ни norms-MCP, и
        # цитата по памяти модели хуже невыполненного этапа (E-19).
        if (
            pipeline_stage in CENTER_ONLY_PIPELINE_STAGES
            and stage.execution_scope == registry.SCOPE_WORKER
        ):
            out.append(
                f"{stage.stage_id}: стадия конвейера {pipeline_stage!r} — "
                "исключительно центральная, воркеру назначена быть не может"
            )


def validate(plan: RoutingPlan) -> None:
    """Проверить план целиком. Все нарушения собираются, потом одно исключение.

    Собираются ВСЕ: оператор, правящий конфигурацию, должен увидеть список, а
    не чинить по одной ошибке за прогон.
    """
    problems: list[str] = []

    if plan.schema_version != 1:
        problems.append(f"schema_version={plan.schema_version!r}: поддерживается 1")
    if not plan.preset_id:
        problems.append("preset_id пуст")

    seen_stages: set[str] = set()
    seen_actions: set[str] = set()
    for stage in plan.stages:
        if not stage.stage_id:
            problems.append("этап без stage_id")
        elif stage.stage_id in seen_stages:
            problems.append(f"этап {stage.stage_id!r} встречается дважды")
        seen_stages.add(stage.stage_id)
        for action in stage.actions:
            key = f"{stage.stage_id}.{action.action_id}"
            if action.action_id and key in seen_actions:
                problems.append(f"действие {key!r} встречается дважды")
            seen_actions.add(key)
            _check_action_shape(stage, action, problems)
            _check_condition_and_multiplicity(stage, action, problems)

    _check_scopes(plan, problems)
    _check_dependencies(plan, problems)
    _check_no_exact_model(plan, problems)

    missing = [s for s in MANDATORY_STAGES if s not in seen_stages]
    if missing:
        problems.append(
            "в плане нет обязательных этапов: " + ", ".join(missing)
        )

    if problems:
        raise RoutingPlanValidationError(problems)


def validated(plan: RoutingPlan) -> RoutingPlan:
    """Удобная обёртка: вернуть план, если он прошёл проверку."""
    validate(plan)
    return plan
