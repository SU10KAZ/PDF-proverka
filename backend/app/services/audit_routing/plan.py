"""Неизменяемый план маршрутизации аудита: доменные типы и канонический хэш.

Что такое план и зачем он появился.

До 11I выбранный пользователем пресет был ГЛОБАЛЬНЫМ состоянием процесса:
`stage_models.json` читался в момент старта каждого отдельного этапа, а не в
момент запуска аудита. Оператор, переключивший пресет между этапами, менял
маршрут уже идущего задания — и узнать об этом постфактум было неоткуда.
Одновременно контракт удалённого задания нёс ОДНУ пару «провайдер + способность»
на весь worker-участок, то есть не мог выразить ни ансамбль из трёх ног этапа
01, ни две параллельные ноги оптимизации, ни судью, ни targeted-проходы свода.

План закрывает обе дыры одной конструкцией: пресет компилируется в
типизированный список действий один раз — в момент создания задания, — получает
хэш и уезжает вместе с заданием. Дальше он не меняется НИКОГДА, а исполнители
(конвейер на воркере и хвост на центре) читают только его.

Три свойства, ради которых типы именно такие:

  * **`frozen=True` у всех узлов.** План хэшируется и уезжает на чужую машину;
    изменение его в памяти после сборки означало бы, что хэш описывает не то,
    что исполнилось;
  * **топология выражена явно** (`parallel_group`, `depends_on`), а не
    подразумевается порядком списка. Исполнитель, превративший параллельную
    группу в последовательность, изменил бы семантику этапа, и заметить это по
    плоскому списку было бы нечем;
  * **детерминированное действие отличается от модельного по ТИПУ**, а не по
    заполненности полей. Верификатор и F OPT Fix не делают ни одного обращения
    к модели — и план обязан это утверждать, а не изображать вызов ради
    единообразия структуры.

Хэш считается по КОНТЕНТУ (пресет, версия, флаги, этапы, действия) и НЕ
включает идентификатор экземпляра плана и время создания. Причина практическая:
freeze-тест обязан отвечать на вопрос «изменился ли МАРШРУТ», а два задания,
созданные с одинаковым пресетом и одинаковыми флагами, обязаны дать одинаковый
хэш — иначе сверка «центр против воркера» ловила бы время, а не смысл.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from backend.app.services.audit_routing import registry

#: Версия СХЕМЫ плана. Меняется при изменении набора полей, а не при изменении
#: содержимого пресета. Незнакомая версия — отказ, а не «прочитаем, что понятно».
ROUTING_PLAN_SCHEMA_VERSION = 1

#: Версия ВЫЧИСЛИТЕЛЯ условий. Отдельно от схемы: смысл условия может
#: измениться при неизменной структуре плана, и исполнитель обязан это заметить.
CONDITION_EVALUATOR_VERSION = 1


class RoutingPlanError(ValueError):
    """План невозможно построить, разобрать или он не прошёл проверку."""


def _frozen_map(value: Optional[Mapping[str, Any]]) -> tuple[tuple[str, Any], ...]:
    """Словарь → отсортированный кортеж пар. Нужен для хэшируемости dataclass."""
    if not value:
        return ()
    return tuple(sorted((str(k), v) for k, v in value.items()))


def _as_map(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    return {k: v for k, v in pairs}


@dataclass(frozen=True)
class RoutingCondition:
    """Типизированное условие исполнения действия.

    Ни строки Python, ни выражения: только идентификатор из закрытого реестра и
    его параметры. `eval` над данными задания — это удалённое исполнение кода на
    чужом VPS, и «мы же сами формируем строку» ничего здесь не меняет: план
    проходит через сериализацию, сеть и чужой диск.
    """

    type: str = registry.COND_ALWAYS
    params: tuple[tuple[str, Any], ...] = ()

    @staticmethod
    def always() -> "RoutingCondition":
        return RoutingCondition(type=registry.COND_ALWAYS)

    @staticmethod
    def feature(flag: str) -> "RoutingCondition":
        return RoutingCondition(
            type=registry.COND_FEATURE_ENABLED, params=(("flag", str(flag)),)
        )

    @staticmethod
    def of(type_: str, **params: Any) -> "RoutingCondition":
        return RoutingCondition(type=str(type_), params=_frozen_map(params))

    @property
    def is_always(self) -> bool:
        return self.type == registry.COND_ALWAYS

    @property
    def resolvable_at_creation(self) -> bool:
        """Может ли центр ответить на условие уже при создании задания."""
        return self.type in registry.RESOLVABLE_AT_CREATION

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.params:
            payload["params"] = _as_map(self.params)
        return payload

    @staticmethod
    def from_dict(payload: Any) -> "RoutingCondition":
        if payload is None:
            return RoutingCondition.always()
        if not isinstance(payload, dict):
            raise RoutingPlanError("condition: ожидается объект")
        unknown = set(payload) - {"type", "params"}
        if unknown:
            raise RoutingPlanError(f"condition: недопустимые ключи {sorted(unknown)}")
        params = payload.get("params") or {}
        if not isinstance(params, dict):
            raise RoutingPlanError("condition.params: ожидается объект")
        return RoutingCondition(
            type=str(payload.get("type") or registry.COND_ALWAYS),
            params=_frozen_map(params),
        )


@dataclass(frozen=True)
class RoutingMultiplicity:
    """Сколько раз действие исполняется за прогон.

    Нужна не только исполнителю, но и оценщику бюджета: до 11I бюджет считал
    один вызов на графический блок, тогда как этап 01 делает четыре, и документ
    на сорок блоков упирался в потолок 64 на середине УЖЕ ОПЛАЧЕННОГО прогона.
    Формула обязана выводиться из плана, а не жить константой рядом с ним.
    """

    type: str = registry.MULT_PER_DOCUMENT
    params: tuple[tuple[str, Any], ...] = ()

    @staticmethod
    def per_document() -> "RoutingMultiplicity":
        return RoutingMultiplicity(type=registry.MULT_PER_DOCUMENT)

    @staticmethod
    def per_graphic_block() -> "RoutingMultiplicity":
        return RoutingMultiplicity(type=registry.MULT_PER_GRAPHIC_BLOCK)

    @staticmethod
    def per_chunk(*, chunk_source: str, max_chunks: Optional[int] = None) -> "RoutingMultiplicity":
        params: dict[str, Any] = {"chunk_source": str(chunk_source)}
        if max_chunks is not None:
            params["max_chunks"] = int(max_chunks)
        return RoutingMultiplicity(type=registry.MULT_PER_CHUNK, params=_frozen_map(params))

    @staticmethod
    def per_batch(
        *, batch_size: int, max_rounds: int = 1, target_source: str = ""
    ) -> "RoutingMultiplicity":
        params: dict[str, Any] = {
            "batch_size": int(batch_size),
            "max_rounds": int(max_rounds),
        }
        if target_source:
            params["target_source"] = str(target_source)
        return RoutingMultiplicity(type=registry.MULT_PER_BATCH, params=_frozen_map(params))

    def param(self, name: str, default: Any = None) -> Any:
        return _as_map(self.params).get(name, default)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.params:
            payload["params"] = _as_map(self.params)
        return payload

    @staticmethod
    def from_dict(payload: Any) -> "RoutingMultiplicity":
        if payload is None:
            return RoutingMultiplicity.per_document()
        if not isinstance(payload, dict):
            raise RoutingPlanError("multiplicity: ожидается объект")
        unknown = set(payload) - {"type", "params"}
        if unknown:
            raise RoutingPlanError(f"multiplicity: недопустимые ключи {sorted(unknown)}")
        params = payload.get("params") or {}
        if not isinstance(params, dict):
            raise RoutingPlanError("multiplicity.params: ожидается объект")
        return RoutingMultiplicity(
            type=str(payload.get("type") or registry.MULT_PER_DOCUMENT),
            params=_frozen_map(params),
        )


@dataclass(frozen=True)
class RoutingAction:
    """Одно логическое действие маршрута.

    «Логическое» — ключевое слово: повтор после таймаута НЕ является вторым
    действием, а вторая нога ансамбля НЕ является повтором первой. Смешение
    этих понятий и породило бюджет, в котором ансамбль из трёх ног выглядел как
    один вызов с запасом на ошибки.
    """

    action_id: str
    role: str
    kind: str = registry.KIND_MODEL
    provider: Optional[str] = None
    capability: Optional[str] = None
    reasoning_effort: Optional[str] = None
    parallel_group: Optional[str] = None
    depends_on: tuple[str, ...] = ()
    condition: RoutingCondition = field(default_factory=RoutingCondition.always)
    multiplicity: RoutingMultiplicity = field(default_factory=RoutingMultiplicity.per_document)
    #: Короткое человеческое пояснение. В хэш входит: изменение смысла действия
    #: обязано менять хэш, даже если провайдер тот же.
    note: str = ""

    @property
    def is_model(self) -> bool:
        return self.kind == registry.KIND_MODEL

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action_id": self.action_id,
            "role": self.role,
            "kind": self.kind,
            "condition": self.condition.to_dict(),
            "multiplicity": self.multiplicity.to_dict(),
        }
        if self.provider:
            payload["provider"] = self.provider
        if self.capability:
            payload["capability"] = self.capability
        if self.reasoning_effort:
            payload["reasoning_effort"] = self.reasoning_effort
        if self.parallel_group:
            payload["parallel_group"] = self.parallel_group
        if self.depends_on:
            payload["depends_on"] = list(self.depends_on)
        if self.note:
            payload["note"] = self.note
        return payload

    @staticmethod
    def from_dict(payload: Any) -> "RoutingAction":
        if not isinstance(payload, dict):
            raise RoutingPlanError("action: ожидается объект")
        allowed = {
            "action_id", "role", "kind", "provider", "capability",
            "reasoning_effort", "parallel_group", "depends_on", "condition",
            "multiplicity", "note",
        }
        unknown = set(payload) - allowed
        if unknown:
            raise RoutingPlanError(f"action: недопустимые ключи {sorted(unknown)}")
        depends = payload.get("depends_on") or []
        if not isinstance(depends, list) or any(not isinstance(x, str) for x in depends):
            raise RoutingPlanError("action.depends_on: список строк")
        return RoutingAction(
            action_id=str(payload.get("action_id") or ""),
            role=str(payload.get("role") or ""),
            kind=str(payload.get("kind") or registry.KIND_MODEL),
            provider=(str(payload["provider"]) if payload.get("provider") else None),
            capability=(str(payload["capability"]) if payload.get("capability") else None),
            reasoning_effort=(
                str(payload["reasoning_effort"]) if payload.get("reasoning_effort") else None
            ),
            parallel_group=(
                str(payload["parallel_group"]) if payload.get("parallel_group") else None
            ),
            depends_on=tuple(str(x) for x in depends),
            condition=RoutingCondition.from_dict(payload.get("condition")),
            multiplicity=RoutingMultiplicity.from_dict(payload.get("multiplicity")),
            note=str(payload.get("note") or ""),
        )


@dataclass(frozen=True)
class RoutingStage:
    """Этап конвейера как контейнер действий.

    `stage_id` — имя этапа В КОНВЕЙЕРЕ (`block_analysis`, `text_analysis`, …),
    а не строка таблицы UI. Строки UI и рантайм расходятся в трёх местах из
    десяти (см. `UI_RUNTIME_MISMATCHES.md`), и план обязан описывать рантайм.
    """

    #: Ключ таблицы `stage_models.json` — единица МАРШРУТИЗАЦИИ.
    stage_id: str
    execution_scope: str
    #: Имя этапа В КОНВЕЙЕРЕ. Отличается от `stage_id` там, где одна стадия
    #: конвейера обслуживает несколько строк таблицы: `findings_critic` и
    #: `findings_corrector` — это две фазы одной стадии `findings_review`, а
    #: `norm_fix` и `norm_requote` — шаги внутри `norm_verify`.
    #:
    #: Различие не косметическое: именно `pipeline_stage` проверяет белый
    #: список моста воркера (`pipeline_bridge.py:253-257`), и вызов под именем
    #: строки таблицы получил бы отказ.
    pipeline_stage: str = ""
    actions: tuple[RoutingAction, ...] = ()
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "stage_id": self.stage_id,
            "execution_scope": self.execution_scope,
            "pipeline_stage": self.pipeline_stage or self.stage_id,
            "actions": [a.to_dict() for a in self.actions],
        }
        if self.note:
            payload["note"] = self.note
        return payload

    @staticmethod
    def from_dict(payload: Any) -> "RoutingStage":
        if not isinstance(payload, dict):
            raise RoutingPlanError("stage: ожидается объект")
        unknown = set(payload) - {
            "stage_id", "execution_scope", "pipeline_stage", "actions", "note",
        }
        if unknown:
            raise RoutingPlanError(f"stage: недопустимые ключи {sorted(unknown)}")
        actions = payload.get("actions") or []
        if not isinstance(actions, list):
            raise RoutingPlanError("stage.actions: ожидается список")
        return RoutingStage(
            stage_id=str(payload.get("stage_id") or ""),
            execution_scope=str(payload.get("execution_scope") or ""),
            pipeline_stage=str(payload.get("pipeline_stage") or ""),
            actions=tuple(RoutingAction.from_dict(a) for a in actions),
            note=str(payload.get("note") or ""),
        )


@dataclass(frozen=True)
class RoutingPlan:
    """Неизменяемый план маршрутизации одного задания аудита."""

    preset_id: str
    stages: tuple[RoutingStage, ...]
    feature_flags: tuple[tuple[str, Any], ...] = ()
    schema_version: int = ROUTING_PLAN_SCHEMA_VERSION
    condition_evaluator_version: int = CONDITION_EVALUATOR_VERSION
    #: Ревизия конвейера центра на момент компиляции. В хэш входит: тот же
    #: пресет на другой ревизии кода может означать другой маршрут.
    pipeline_revision: str = ""
    #: Метаданные экземпляра. В ХЭШ НЕ ВХОДЯТ (см. докстринг модуля).
    routing_plan_id: str = ""
    created_at: float = 0.0

    # ── доступ ──────────────────────────────────────────────────────────────
    def stage(self, stage_id: str) -> Optional[RoutingStage]:
        for item in self.stages:
            if item.stage_id == stage_id:
                return item
        return None

    def iter_actions(self):
        for stage in self.stages:
            for action in stage.actions:
                yield stage, action

    def model_actions(self):
        for stage, action in self.iter_actions():
            if action.is_model:
                yield stage, action

    @property
    def flags(self) -> dict[str, Any]:
        return _as_map(self.feature_flags)

    def flag(self, name: str, default: Any = None) -> Any:
        return self.flags.get(name, default)

    # ── сериализация ────────────────────────────────────────────────────────
    def content_dict(self) -> dict[str, Any]:
        """Содержательная часть: то, что определяет МАРШРУТ. Основа хэша."""
        return {
            "schema_version": int(self.schema_version),
            "condition_evaluator_version": int(self.condition_evaluator_version),
            "preset_id": self.preset_id,
            "pipeline_revision": self.pipeline_revision,
            "feature_flags": _as_map(self.feature_flags),
            "stages": [s.to_dict() for s in self.stages],
        }

    def canonical_json(self) -> bytes:
        """Каноническое представление содержания: сортировка ключей, без пробелов.

        Хэш обязан зависеть только от ЗНАЧЕНИЙ: иначе переупаковка того же плана
        меняла бы хэш, и сверка «центр против воркера» ловила бы форматирование.
        """
        return json.dumps(
            self.content_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def plan_hash(self) -> str:
        return "sha256:" + hashlib.sha256(self.canonical_json()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Полное представление (содержание + метаданные + хэш)."""
        payload = self.content_dict()
        payload["routing_plan_id"] = self.routing_plan_id
        payload["created_at"] = float(self.created_at)
        payload["routing_plan_hash"] = self.plan_hash()
        return payload

    def to_package_bytes(self) -> bytes:
        """Читаемое представление для файла в пакете задания."""
        return json.dumps(
            self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8")

    @staticmethod
    def from_dict(payload: Any) -> "RoutingPlan":
        """Разобрать план. Строго: неизвестный ключ — отказ.

        Хэш здесь НЕ сверяется: сверка — отдельное решение вызывающего
        (`assert_hash`), потому что «разобрать, чтобы показать оператору» и
        «принять к исполнению» — разные операции с разными последствиями.
        """
        if not isinstance(payload, dict):
            raise RoutingPlanError("routing_plan: ожидается объект")
        allowed = {
            "schema_version", "condition_evaluator_version", "preset_id",
            "pipeline_revision", "feature_flags", "stages", "routing_plan_id",
            "created_at", "routing_plan_hash",
        }
        unknown = set(payload) - allowed
        if unknown:
            raise RoutingPlanError(f"routing_plan: недопустимые ключи {sorted(unknown)}")
        version = payload.get("schema_version")
        if version != ROUTING_PLAN_SCHEMA_VERSION:
            raise RoutingPlanError(
                f"routing_plan.schema_version={version!r}, "
                f"поддерживается {ROUTING_PLAN_SCHEMA_VERSION}"
            )
        stages = payload.get("stages") or []
        if not isinstance(stages, list):
            raise RoutingPlanError("routing_plan.stages: ожидается список")
        flags = payload.get("feature_flags") or {}
        if not isinstance(flags, dict):
            raise RoutingPlanError("routing_plan.feature_flags: ожидается объект")
        plan = RoutingPlan(
            preset_id=str(payload.get("preset_id") or ""),
            stages=tuple(RoutingStage.from_dict(s) for s in stages),
            feature_flags=_frozen_map(flags),
            schema_version=int(version),
            condition_evaluator_version=int(
                payload.get("condition_evaluator_version") or CONDITION_EVALUATOR_VERSION
            ),
            pipeline_revision=str(payload.get("pipeline_revision") or ""),
            routing_plan_id=str(payload.get("routing_plan_id") or ""),
            created_at=float(payload.get("created_at") or 0.0),
        )
        declared = str(payload.get("routing_plan_hash") or "")
        if declared and declared != plan.plan_hash():
            raise RoutingPlanError(
                "routing_plan_hash не совпадает с содержимым плана: "
                f"объявлен {declared}, посчитан {plan.plan_hash()}. "
                "План отвергается целиком — маршрут, не совпадающий со своим "
                "хэшем, невозможно ни исполнить, ни проследить"
            )
        return plan

    def assert_hash(self, expected: str) -> None:
        """Сверить хэш плана с ожидаемым. Несовпадение — отказ (fail closed)."""
        actual = self.plan_hash()
        if str(expected or "") != actual:
            raise RoutingPlanError(
                f"хэш плана маршрутизации не совпал: ожидался {expected!r}, "
                f"получен {actual!r}. Исполнение не начинается: расхождение "
                "означает, что центр и воркер держат РАЗНЫЕ маршруты"
            )
