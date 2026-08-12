"""ProviderResolver — ГДЕ воркер решает, каким провайдером пойдёт задание.

До этого модуля выбор провайдера в подсистеме существовал в двух видах, и ни
один не был точкой решения:

  * `ProviderManager._ADAPTERS` — таблица «имя → класс». Она отвечает на вопрос
    «какие провайдеры бывают», а не «кем исполнять это задание»;
  * `audit_runner.build_env` — переменные окружения `AUDIT_WORKER_PROVIDER_MODE`
    и путь к подделкам. Это выбор МЕХАНИЗМА (настоящий CLI или подделка), а не
    выбор провайдера, и он ничего не знает ни о задании, ни об авторизации.

Резолвер закрывает разрыв, не заводя второй слой: он берёт ЛОГИЧЕСКОЕ требование
центра (`provider`, `model`, политика исполнения) и ФАКТИЧЕСКОЕ состояние машины
из `ProviderManager` — и выдаёт `ProviderBinding`: единственный документ, по
которому процесс конвейера позднее строит адаптер.

Что резолвер НЕ делает и делать не должен:

  * не читает учётные данные — он их даже не видит: адаптер сам находит их по
    режиму авторизации;
  * не принимает от центра путей к исполняемым файлам — путь к CLI знает
    администратор VPS (`AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE`), и это правило
    этапа 11 здесь не ослабляется;
  * не разрешает вызов модели. Разрешение — отдельный документ
    (`inference_grant`), и резолвер только СВЕРЯЕТ его наличие.

Разделение «требование центра» / «решение воркера» здесь то же, что и во всей
подсистеме: центр передаёт логическое требование, воркер отвечает «могу/не могу»
и решает, чем именно.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

# Потолок вызовов попытки живёт в нейтральной конфигурации воркера: то же
# значение проверяет `audit_runner` на приёме задания, а импортировать
# провайдерский слой ему запрещено по построению.
from audit_worker.config import MAX_INFERENCES_CEILING
from audit_worker.providers import errors, identity as identity_mod, model_policy
from audit_worker.providers.auth_mode import (
    AUTH_MODE_AMBIENT_USER,
    AUTH_MODE_UNAVAILABLE,
)
from audit_worker.providers.paths import (
    SUPPORTED_PROVIDERS,
    is_http_provider,
    require_provider,
)

#: Версия схемы файла привязки. Процесс конвейера отвергает неизвестную версию.
BINDING_SCHEMA_VERSION = 1

#: Имя файла привязки внутри `metadata/` каталога попытки.
BINDING_FILENAME = "provider_binding.json"

#: Переменная, которой воркер сообщает процессу конвейера путь к привязке.
#: Единственный канал: без неё мост провайдеров в конвейере не активируется
#: вовсе, и код платформы ведёт себя ровно как до этапа 11C.
BINDING_ENV = "AUDIT_WORKER_PROVIDER_BINDING"


class ProviderResolutionError(RuntimeError):
    """Требование задания не может быть исполнено этим воркером."""


@dataclass(frozen=True)
class ProviderRequirement:
    """ЛОГИЧЕСКОЕ требование центра. Ни путей, ни учётных данных, ни токенов."""

    provider: str
    model: Optional[str] = None
    #: Этапы, которым разрешено обращаться к модели через мост. Список закрытый:
    #: этап, которого здесь нет, получает отказ, а не молчаливый обход моста.
    allowed_stages: tuple[str, ...] = ()
    #: Сколько оплачиваемых вызовов допускает политика исполнения. Ноль означает
    #: «модель не звать» — это законное требование, а не ошибка.
    max_inferences: int = 0
    #: ЛОГИЧЕСКАЯ способность («нужна сильная модель для аудита») вместо точного
    #: идентификатора. Что она означает НА ЭТОЙ машине, решает локальная политика
    #: воркера (`model_policy`), а не центр: см. её докстринг о том, почему точный
    #: `model` из задания — это одновременно нарушение I-P5 и передача чужой
    #: подписки в распоряжение центра. Взаимоисключима с `model`.
    capability: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Any) -> Optional["ProviderRequirement"]:
        """Разбор поля задания. Отсутствие поля — не ошибка, а «как раньше»."""
        if not payload:
            return None
        if not isinstance(payload, dict):
            raise ProviderResolutionError(
                "provider_requirement: ожидается объект"
            )
        unknown = set(payload) - {
            "provider", "model", "allowed_stages", "max_inferences", "capability",
        }
        if unknown:
            raise ProviderResolutionError(
                f"provider_requirement: недопустимые поля {sorted(unknown)}"
            )
        try:
            provider = require_provider(str(payload.get("provider") or ""))
        except ValueError as exc:
            raise ProviderResolutionError(str(exc)) from None
        stages = payload.get("allowed_stages") or []
        if not isinstance(stages, list) or not all(isinstance(x, str) for x in stages):
            raise ProviderResolutionError(
                "provider_requirement.allowed_stages: ожидается список строк"
            )
        for stage in stages:
            if not stage.replace("_", "").isalnum() or len(stage) > 64:
                raise ProviderResolutionError(
                    f"provider_requirement.allowed_stages: недопустимое имя {stage!r}"
                )
        try:
            max_inferences = int(payload.get("max_inferences") or 0)
        except (TypeError, ValueError):
            raise ProviderResolutionError(
                "provider_requirement.max_inferences: ожидается целое число"
            ) from None
        if max_inferences < 0 or max_inferences > MAX_INFERENCES_CEILING:
            # Верхняя граница — не догма, а рубеж: задание не имеет права
            # заказать «сто вызовов» на чужой подписке. Реальный потолок всё
            # равно задаёт разрешение оператора.
            raise ProviderResolutionError(
                f"provider_requirement.max_inferences={max_inferences} вне "
                f"[0, {MAX_INFERENCES_CEILING}]"
            )
        model = payload.get("model")
        if model is not None:
            # Точный идентификатор модели от ЦЕНТРА не принимается вовсе
            # (этап 11D). Раньше поле проходило как «строка ≤128» и уезжало в
            # `binding.model`, а оттуда — в argv; от попадания туда произвольной
            # строки центра спасала лишь проверка тремя слоями ниже («модель
            # назначена, а список допустимых пуст → отказ»). Инвариант I-P5 не
            # имеет права держаться на побочном эффекте чужой проверки.
            raise ProviderResolutionError(
                "provider_requirement.model больше не принимается: точную модель "
                "выбирает ЛОКАЛЬНАЯ политика воркера по логической способности "
                "(capability). Центру идентификатор модели не принадлежит"
            )
        capability = payload.get("capability")
        if capability is not None:
            if not isinstance(capability, str) or not capability.strip():
                raise ProviderResolutionError(
                    "provider_requirement.capability: непустая строка"
                )
            capability = capability.strip()
            if capability not in model_policy.KNOWN_CAPABILITIES:
                raise ProviderResolutionError(
                    f"provider_requirement.capability={capability!r} неизвестна "
                    f"(известны: {list(model_policy.KNOWN_CAPABILITIES)})"
                )
        if max_inferences > 0 and not capability:
            # Fail closed (этап 11G). Без этой проверки требование «зови модель,
            # но способность не назову» доходило бы до `resolve()`, привязка
            # получила бы `model=None`, адаптер не передал бы CLI флаг `--model`
            # — и вызов ушёл бы на модель учётной записи ПО УМОЛЧАНИЮ. Ровно эта
            # тихая подмена (11C: ожидали `claude-opus-5`, ответил
            # `claude-opus-4-8[1m]`) и породила саму идею способностей, поэтому
            # умолчания здесь нет и быть не может.
            raise ProviderResolutionError(
                f"provider_requirement требует вызовов модели "
                f"(max_inferences={max_inferences}), но не назвал capability. "
                "Точную модель выбирает локальная политика воркера, и выбирать "
                "ей не из чего"
            )
        return cls(
            provider=provider,
            model=None,
            allowed_stages=tuple(stages),
            max_inferences=max_inferences,
            capability=capability or None,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "allowed_stages": list(self.allowed_stages),
            "max_inferences": int(self.max_inferences),
            "capability": self.capability,
        }


@dataclass(frozen=True)
class RouteBinding:
    """Один разрешённый маршрут: «провайдер + способность» → точная модель.

    Появился на 11I. До него привязка описывала ОДНУ модель на всю попытку — и
    этого хватало ровно потому, что мост схлопывал ансамбли: этап 01 из трёх
    ног превращался в одну, этап 05 из двух — в одну. Как только план требует
    исполнить ансамбль целиком, «одна модель на попытку» перестаёт быть
    ограничением безопасности и становится ошибкой воспроизведения.

    Что здесь по-прежнему НЕ приходит от центра: точный идентификатор модели.
    Его выдаёт локальная политика воркера по способности — ровно как и раньше,
    просто теперь таких выдач несколько.
    """

    provider: str
    capability: str
    model: str
    accepted_reported_models: tuple[str, ...] = field(default_factory=tuple)
    model_report: str = "required"
    auth_mode: str = ""
    provider_root: str = ""
    executable: Optional[str] = None
    timeout_sec: float = 60.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "capability": self.capability,
            "model": self.model,
            "accepted_reported_models": list(self.accepted_reported_models),
            "model_report": self.model_report,
            "auth_mode": self.auth_mode,
            "provider_root": self.provider_root,
            "executable": self.executable,
            "timeout_sec": float(self.timeout_sec),
        }

    def as_public_dict(self) -> dict[str, Any]:
        """Вид для центра: без путей файловой системы и БЕЗ строки модели.

        Точные идентификаторы — собственность машины, и объявленный инвариант
        прямо это утверждает: «наружу уходит только „claude умеет
        strong_audit"». До 11I наружу утекала одна строка (`binding.model`); с
        маршрутами утекла бы вся таблица локальной политики — то есть состав и
        уровень чужих подписок целиком.

        Что остаётся: провайдер, способность, режим сверки модели и режим
        авторизации. Этого хватает, чтобы разобрать прогон, и не хватает, чтобы
        восстановить конфигурацию VPS.
        """
        return {
            "provider": self.provider,
            "capability": self.capability,
            "model_report": self.model_report,
            "auth_mode": self.auth_mode,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RouteBinding":
        data = payload or {}
        try:
            provider = require_provider(str(data.get("provider") or ""))
        except ValueError as exc:
            raise ProviderResolutionError(str(exc)) from None
        capability = str(data.get("capability") or "").strip()
        if not capability:
            raise ProviderResolutionError("маршрут привязки без способности")
        try:
            model = model_policy.validate_model_id(
                data.get("model"), where=f"маршрут {provider}/{capability}.model",
                provider=provider,
            )
            accepted = tuple(
                model_policy.validate_model_id(
                    item, where=f"маршрут {provider}/{capability}.accepted",
                    provider=provider,
                )
                for item in (data.get("accepted_reported_models") or [])
            )
        except model_policy.ProviderPolicyError as exc:
            raise ProviderResolutionError(str(exc)) from None
        report = str(data.get("model_report") or model_policy.MODEL_REPORT_REQUIRED)
        if report not in model_policy.MODEL_REPORT_MODES:
            raise ProviderResolutionError(
                f"маршрут {provider}/{capability}.model_report={report!r}: "
                f"допустимы {list(model_policy.MODEL_REPORT_MODES)}"
            )
        executable = data.get("executable")
        if executable is not None:
            # Путь к CLI — это argv[0] следующего оплачиваемого вызова, и
            # привязка лежит в каталоге, которым владеет процесс конвейера.
            # Ровно эта угроза названа причиной валидировать строку модели;
            # относительный путь здесь означал бы «взять что-нибудь из cwd».
            executable = str(executable)
            if not executable.startswith("/"):
                raise ProviderResolutionError(
                    f"маршрут {provider}/{capability}.executable={executable!r}: "
                    "ожидается абсолютный путь"
                )
        timeout = float(data.get("timeout_sec") or 60.0)
        if not 0 < timeout <= 24 * 3600:
            raise ProviderResolutionError(
                f"маршрут {provider}/{capability}.timeout_sec={timeout!r} вне "
                "разумных границ"
            )
        return cls(
            provider=provider,
            capability=capability,
            model=model,
            accepted_reported_models=accepted,
            model_report=report,
            auth_mode=str(data.get("auth_mode") or ""),
            provider_root=str(data.get("provider_root") or ""),
            executable=executable,
            timeout_sec=timeout,
        )


@dataclass(frozen=True)
class ProviderBinding:
    """РЕШЕНИЕ воркера: чем и на каких условиях исполнять этот вызов модели.

    Это единственный документ, который процесс конвейера получает о провайдерах.
    Учётных данных в нём нет и быть не может: он лежит файлом в каталоге попытки
    и уезжает в пакет результата как evidence.
    """

    schema_version: int
    provider: str
    auth_mode: str
    #: Корень раскладки `ProviderHome`. В ambient-режиме это каталог ВНУТРИ
    #: попытки: из него используются только пустой `runtime` (cwd подпроцесса) и
    #: `metadata`, а HOME берётся из базы учётных записей, а не отсюда.
    provider_root: str
    #: Абсолютный путь к CLI либо None (тогда адаптер берёт штатный путь
    #: установщика). Значение задаёт АДМИНИСТРАТОР VPS, не центр.
    executable: Optional[str]
    timeout_sec: float
    job_id: str
    attempt_id: str
    task_id: str
    grant_id: str
    max_inferences: int
    allowed_stages: tuple[str, ...]
    #: ТОЧНЫЙ идентификатор модели, который уйдёт в CLI флагом `--model`.
    #: С этапа 11D он берётся из ЛОКАЛЬНОЙ политики воркера по логической
    #: способности задания, а не из самого задания.
    model: Optional[str] = None
    #: Значения, которых не должно быть в ответе модели. Приходят от оператора
    #: в момент прогона и в репозитории не хранятся (иначе «не нашли» ничего
    #: не доказывает).
    forbidden_literals: tuple[str, ...] = field(default_factory=tuple)
    #: Логическая способность, по которой выбрана модель. Хранится ради разбора
    #: чужого прогона: без неё «почему именно эта модель» восстановить нечем.
    capability: Optional[str] = None
    #: Идентификаторы, которые CLI имеет право назвать ФАКТИЧЕСКИ применёнными
    #: для той же модели (см. `model_policy.default_accepted_reported`). Пустой
    #: кортеж при заданном `model` означает «сверять не с чем» и трактуется как
    #: несовпадение — умолчания «сойдёт любое» здесь нет.
    accepted_reported_models: tuple[str, ...] = field(default_factory=tuple)
    #: Умеет ли CLI провайдера называть фактически применённую модель
    #: (`model_policy.MODEL_REPORT_MODES`). Значение приходит из ЛОКАЛЬНОЙ
    #: политики машины и едет в привязку затем, чтобы разбор чужого прогона
    #: отвечал на вопрос «сверяли ли модель» по файлу, а не по догадке о версии
    #: CLI.
    model_report: str = "required"
    #: ВСЕ маршруты, разрешённые локальной политикой для этой попытки (11I).
    #: Пустой кортеж — привязка прежней формы: один провайдер, одна модель.
    routes: tuple["RouteBinding", ...] = field(default_factory=tuple)
    #: Хэш замороженного плана маршрутизации. Сверяется мостом перед каждым
    #: обращением: расхождение означает, что центр и воркер держат РАЗНЫЕ
    #: маршруты, и исполнять в таком состоянии нечего.
    routing_plan_hash: str = ""

    def route_for(self, provider: str, capability: str) -> Optional["RouteBinding"]:
        """Маршрут для пары. `None` — политика такой пары не описывает."""
        for item in self.routes:
            if item.provider == provider and item.capability == capability:
                return item
        return None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": int(self.schema_version),
            "provider": self.provider,
            "auth_mode": self.auth_mode,
            "provider_root": self.provider_root,
            "executable": self.executable,
            "timeout_sec": float(self.timeout_sec),
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "task_id": self.task_id,
            "grant_id": self.grant_id,
            "max_inferences": int(self.max_inferences),
            "allowed_stages": list(self.allowed_stages),
            "model": self.model,
            "forbidden_literals": list(self.forbidden_literals),
            "capability": self.capability,
            "accepted_reported_models": list(self.accepted_reported_models),
            "model_report": self.model_report,
            "routes": [item.as_dict() for item in self.routes],
            "routing_plan_hash": self.routing_plan_hash,
        }

    def as_public_dict(self) -> dict[str, Any]:
        """Вид для центра и оператора: без абсолютных путей и без литералов."""
        return {
            "provider": self.provider,
            "auth_mode": self.auth_mode,
            "model": self.model,
            "capability": self.capability,
            "accepted_reported_models": list(self.accepted_reported_models),
            "model_report": self.model_report,
            "max_inferences": int(self.max_inferences),
            "allowed_stages": list(self.allowed_stages),
            "grant_id": self.grant_id,
            "routing_plan_hash": self.routing_plan_hash,
            "routes": [item.as_public_dict() for item in self.routes],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ProviderBinding":
        data = payload or {}
        version = data.get("schema_version")
        if version != BINDING_SCHEMA_VERSION:
            raise ProviderResolutionError(
                f"привязка провайдера: schema_version={version!r}, "
                f"поддерживается {BINDING_SCHEMA_VERSION}"
            )
        try:
            provider = require_provider(str(data.get("provider") or ""))
        except ValueError as exc:
            raise ProviderResolutionError(str(exc)) from None
        stages = data.get("allowed_stages") or []
        if not isinstance(stages, list):
            raise ProviderResolutionError("привязка: allowed_stages не список")
        literals = data.get("forbidden_literals") or []
        if not isinstance(literals, list):
            raise ProviderResolutionError("привязка: forbidden_literals не список")
        accepted = data.get("accepted_reported_models") or []
        if not isinstance(accepted, list):
            raise ProviderResolutionError(
                "привязка: accepted_reported_models не список"
            )
        # Строка модели из ФАЙЛА проходит ту же валидацию, что и строка из
        # политики. Инвариант «в argv только безопасные константы» иначе
        # держался бы на честном слове: привязка лежит в каталоге попытки,
        # которым процесс конвейера владеет целиком, и перечитывается на каждый
        # вызов. Любой обход пути в любом этапе превращался бы в контроль над
        # токеном argv следующего вызова.
        try:
            raw_model = data.get("model")
            model_value = (
                model_policy.validate_model_id(
                    raw_model, where="привязка.model", provider=provider,
                )
                if raw_model else None
            )
            accepted_values = tuple(
                model_policy.validate_model_id(
                    item, where="привязка.accepted_reported_models",
                    provider=provider,
                )
                for item in accepted
            )
        except model_policy.ProviderPolicyError as exc:
            raise ProviderResolutionError(str(exc)) from None
        return cls(
            schema_version=int(version),
            provider=provider,
            auth_mode=str(data.get("auth_mode") or ""),
            provider_root=str(data.get("provider_root") or ""),
            executable=(str(data["executable"]) if data.get("executable") else None),
            timeout_sec=float(data.get("timeout_sec") or 60.0),
            job_id=str(data.get("job_id") or ""),
            attempt_id=str(data.get("attempt_id") or ""),
            task_id=str(data.get("task_id") or ""),
            grant_id=str(data.get("grant_id") or ""),
            max_inferences=int(data.get("max_inferences") or 0),
            allowed_stages=tuple(str(x) for x in stages),
            model=model_value,
            forbidden_literals=tuple(str(x) for x in literals),
            capability=(str(data["capability"]) if data.get("capability") else None),
            accepted_reported_models=accepted_values,
            model_report=str(
                data.get("model_report") or model_policy.MODEL_REPORT_REQUIRED
            ),
            routes=tuple(
                RouteBinding.from_dict(item)
                for item in (data.get("routes") or [])
            ),
            routing_plan_hash=str(data.get("routing_plan_hash") or ""),
        )

    def write(self, metadata_dir: Path) -> Path:
        target = Path(metadata_dir) / BINDING_FILENAME
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # 0600: в привязке нет секретов, но есть абсолютные пути чужой машины и
        # контрольные литералы оператора.
        try:
            target.chmod(0o600)
        except OSError:
            pass
        return target

    @classmethod
    def read(cls, path: Path) -> "ProviderBinding":
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise ProviderResolutionError(f"привязка провайдера не найдена: {path}") from None
        except (OSError, ValueError) as exc:
            raise ProviderResolutionError(f"привязка провайдера не читается: {exc}") from None
        if not isinstance(payload, dict):
            raise ProviderResolutionError("привязка провайдера: ожидается объект")
        return cls.from_dict(payload)


class ProviderResolver:
    """Сводит требование центра с фактическим состоянием машины."""

    def __init__(self, manager: Any, *, worker_root: Path) -> None:
        self.manager = manager
        self.worker_root = Path(worker_root)

    def available(self) -> dict[str, dict[str, Any]]:
        """Что воркер может предложить прямо сейчас. Без вызовов модели."""
        out: dict[str, dict[str, Any]] = {}
        for name in SUPPORTED_PROVIDERS:
            adapter = self.manager.adapters.get(name)
            identity = self.manager.identity(name)
            out[name] = {
                "installed": bool(adapter and adapter.installed()),
                "auth_mode": adapter.home.auth_mode if adapter else None,
                "auth_state": identity.auth_state if identity else identity_mod.AUTH_UNKNOWN,
                "policy_blocked": bool(adapter and adapter.policy_blocked),
            }
        return out

    def resolve(
        self,
        requirement: ProviderRequirement,
        *,
        job_id: str,
        attempt_id: str,
        task_id: str,
        grant_id: str,
        provider_root: Path,
        forbidden_literals: Sequence[str] = (),
        required_routes: Sequence[tuple[str, str]] = (),
        routing_plan_hash: str = "",
    ) -> ProviderBinding:
        """Выбрать провайдера и собрать привязку. Бросает при невозможности.

        Отказ здесь — это отказ ДО запуска процесса конвейера и до списания
        разрешения. Порядок проверок именно такой: сперва то, что решил
        оператор (политика, режим авторизации), потом то, что есть на машине
        (установлен, авторизован).
        """
        name = requirement.provider
        adapter = self.manager.adapters.get(name)
        if adapter is None:
            raise ProviderResolutionError(f"провайдер {name!r} не поддерживается воркером")
        if adapter.policy_blocked:
            raise ProviderResolutionError(
                f"провайдер {name!r} отключён политикой на этом воркере "
                f"({errors.ERR_POLICY_BLOCKED})"
            )
        auth_mode = adapter.home.auth_mode
        if auth_mode == AUTH_MODE_UNAVAILABLE:
            raise ProviderResolutionError(
                f"провайдер {name!r}: режим авторизации unavailable — оператор "
                "объявил, что учётных данных здесь нет"
            )
        if not adapter.installed():
            raise ProviderResolutionError(
                (
                    f"провайдер {name!r}: канал к шлюзу недоступен — HTTP-клиент "
                    f"отсутствует в окружении ({errors.ERR_CLI_MISSING})"
                    if is_http_provider(name)
                    else (
                        f"CLI провайдера {name!r} не установлен по штатному пути "
                        f"({errors.ERR_CLI_MISSING})"
                    )
                )
            )
        identity = self.manager.identity(name)
        if identity is None or identity.auth_state != identity_mod.AUTH_LOGGED_IN:
            state = identity.auth_state if identity else identity_mod.AUTH_UNKNOWN
            raise ProviderResolutionError(
                f"провайдер {name!r}: авторизация не подтверждена (auth_state={state!r})"
            )
        # Точную модель выбирает ВОРКЕР по своей политике (этап 11D). До 11D
        # поля `model` в привязке хватало на «ожидание», которое никто не
        # предъявлял CLI; теперь это строка, которая уйдёт в argv, и её
        # источником обязан быть файл администратора машины.
        resolved_model = requirement.model
        accepted_reported: tuple[str, ...] = ()
        model_report = model_policy.MODEL_REPORT_REQUIRED
        if requirement.capability:
            try:
                policy = model_policy.load_policy(self.worker_root)
                capability = policy.resolve(name, requirement.capability)
            except model_policy.ProviderPolicyError as exc:
                raise ProviderResolutionError(
                    f"локальная политика моделей не покрывает требование: {exc}"
                ) from None
            resolved_model = capability.model
            accepted_reported = capability.accepted_reported_models
            model_report = capability.model_report
        if int(requirement.max_inferences) > 0 and not resolved_model:
            # Второй рубеж того же утверждения. `from_payload` защищает разбор
            # ТРЕБОВАНИЯ ЦЕНТРА, а сюда приходят и требования, собранные кодом
            # (диагностические прогоны). Привязка без модели — это привязка,
            # после которой CLI молча возьмёт модель учётной записи; писать
            # такую на диск нельзя ни по какому пути.
            raise ProviderResolutionError(
                "привязка без точной модели при разрешённых вызовах: локальная "
                "политика не выдала идентификатор. Вызов молча ушёл бы на "
                "модель учётной записи по умолчанию"
            )
        executable = adapter.executable_path()
        routes = self._resolve_routes(required_routes)
        return ProviderBinding(
            schema_version=BINDING_SCHEMA_VERSION,
            provider=name,
            auth_mode=auth_mode,
            provider_root=str(Path(provider_root)),
            executable=str(executable) if executable else None,
            timeout_sec=float(adapter.timeout_sec),
            job_id=str(job_id),
            attempt_id=str(attempt_id),
            task_id=str(task_id),
            grant_id=str(grant_id),
            max_inferences=int(requirement.max_inferences),
            allowed_stages=tuple(requirement.allowed_stages),
            model=resolved_model,
            forbidden_literals=tuple(
                value for value in forbidden_literals if value and len(value) >= 8
            ),
            capability=requirement.capability,
            accepted_reported_models=accepted_reported,
            model_report=model_report,
            routes=routes,
            routing_plan_hash=str(routing_plan_hash or ""),
        )

    def _resolve_routes(
        self, required: Sequence[tuple[str, str]]
    ) -> tuple[RouteBinding, ...]:
        """Разрешить КАЖДУЮ пару «провайдер + способность» плана.

        Проверки те же, что и для основного провайдера, и в том же порядке:
        адаптер существует, не заблокирован политикой, авторизация подтверждена,
        локальная политика описывает способность. Отсутствие любой пары —
        отказ, а не «выполним чем есть»: план требует ансамбль, и ансамбль,
        собранный не из тех ног, — это не тот же аудит.
        """
        if not required:
            return ()
        try:
            policy = model_policy.load_policy(self.worker_root)
        except model_policy.ProviderPolicyError as exc:
            raise ProviderResolutionError(
                f"локальная политика моделей не читается: {exc}"
            ) from None
        out: list[RouteBinding] = []
        for provider_name, capability_name in sorted(set(required)):
            adapter = self.manager.adapters.get(provider_name)
            if adapter is None:
                raise ProviderResolutionError(
                    f"план требует провайдера {provider_name!r}, которого этот "
                    f"воркер не поддерживает"
                )
            if adapter.policy_blocked:
                raise ProviderResolutionError(
                    f"провайдер {provider_name!r} отключён политикой воркера"
                )
            auth = adapter.home.auth_mode
            if auth == AUTH_MODE_UNAVAILABLE:
                raise ProviderResolutionError(
                    f"провайдер {provider_name!r}: режим авторизации unavailable"
                )
            if not adapter.installed():
                raise ProviderResolutionError(
                    f"провайдер {provider_name!r}: HTTP-клиент недоступен"
                    if is_http_provider(provider_name)
                    else f"CLI провайдера {provider_name!r} не установлен"
                )
            identity = self.manager.identity(provider_name)
            if identity is None or identity.auth_state != identity_mod.AUTH_LOGGED_IN:
                state = identity.auth_state if identity else identity_mod.AUTH_UNKNOWN
                raise ProviderResolutionError(
                    f"провайдер {provider_name!r}: авторизация не подтверждена "
                    f"(auth_state={state!r})"
                )
            try:
                resolved = policy.resolve(provider_name, capability_name)
            except model_policy.ProviderPolicyError as exc:
                raise ProviderResolutionError(
                    f"локальная политика не покрывает маршрут плана: {exc}"
                ) from None
            executable = adapter.executable_path()
            out.append(RouteBinding(
                provider=provider_name,
                capability=capability_name,
                model=resolved.model,
                accepted_reported_models=resolved.accepted_reported_models,
                model_report=resolved.model_report,
                auth_mode=auth,
                provider_root=str(adapter.home.root),
                executable=str(executable) if executable else None,
                timeout_sec=float(adapter.timeout_sec),
            ))
        return tuple(out)


def ambient_root_for_attempt(job_dir: Path, provider: str) -> Path:
    """Корень раскладки провайдера ДЛЯ ОДНОЙ ПОПЫТКИ (ambient-режим).

    В ambient-режиме от `ProviderHome` нужны только `runtime` (пустой cwd
    подпроцесса) и `metadata`; сам HOME берётся из базы учётных записей. Поэтому
    корень уводится внутрь попытки: процессу конвейера незачем знать путь к
    каталогу данных воркера, где лежат `worker.db` и токен.
    """
    return Path(job_dir) / "providers" / require_provider(provider)
