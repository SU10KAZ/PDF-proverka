"""Локальная политика моделей ВОРКЕРА: логическая способность → точная модель.

Зачем она появилась именно на 11D.

Этап 11C показал расхождение, которое до него нечем было заметить: конфигурация
центра называла для этапа `text_analysis` модель `claude-opus-5`, а фактически
ответила `claude-opus-4-8[1m]` — модель учётной записи по умолчанию. Причина не
в ошибке: `claude_adapter._inference_argv()` намеренно НЕ передавал `--model`,
потому что идентификатор модели пришёл бы из задания, а инвариант I-P5
запрещает данным задания попадать в argv. Модель выбирал CLI, а конвейер о
подмене узнавал только постфактум из поля `model` ответа — если вообще смотрел.

Для синтетической проверки канала это было безразлично. Для рабочего этапа —
нет: аудит, молча уехавший на другое поколение модели, даёт результат, который
нельзя сравнивать ни с историей, ни с соседним прогоном.

Развилка, и почему выбран второй путь:

  * разрешить центру присылать точный `model` — тогда argv снова наполняется
    данными задания (I-P5 падает), и чужая машина получает право распоряжаться
    подпиской человека, на чьём VPS она исполняется;
  * дать ВОРКЕРУ собственную политику. Центр присылает логическую СПОСОБНОСТЬ
    («нужна сильная модель для аудита»), а какая именно строка соответствует
    ей на этой машине — решает администратор VPS файлом. В argv тогда попадает
    константа локальной конфигурации, а не поле задания: I-P5 сохраняется
    дословно, потому что «извне» для воркера — это центр, а не собственный
    файл политики рядом с `worker.env`.

Что политика НЕ делает:

  * не выбирает провайдера — это дело `ProviderResolver`;
  * не разрешает вызов — это дело `inference_grant`;
  * не имеет умолчания «если модели нет, возьмём какую-нибудь». Отсутствие
    записи — отказ. Умолчание здесь означало бы ровно ту тихую подмену модели,
    ради устранения которой модуль и написан.

Формат файла (`provider_policy.json` в корне данных воркера):

    {
      "policy_version": 1,
      "claude": {
        "auth_mode": "ambient_user",
        "capabilities": {
          "strong_audit": {"model": "claude-opus-5"}
        }
      }
    }

Необязательное поле `accepted_reported_models` внутри способности перечисляет
идентификаторы, которые CLI имеет право вернуть как фактические для ЭТОЙ же
модели. Умолчание — `[<model>, "<model>[1m]"]`: суффикс `[1m]` обозначает
вариант той же модели с окном в миллион токенов (в бинаре CLI 2.1.220
присутствуют обе формы: `claude-opus-5` и `claude-opus-5[1m]`), и на 11C
фактическим ответом был именно суффиксный вид. Всё, чего нет в этом списке, —
несовпадение и отказ; никакого «ну это тоже Opus».
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from audit_worker.providers.paths import SUPPORTED_PROVIDERS, require_provider

#: Версия схемы. Незнакомая версия — отказ, а не «прочитаем, что понятно».
POLICY_SCHEMA_VERSION = 1

#: Имя файла политики в корне данных воркера.
POLICY_FILENAME = "provider_policy.json"

#: Переменная, которой администратор VPS может указать другой путь. Задаёт её
#: администратор машины, НЕ центр и НЕ задание.
POLICY_ENV = "AUDIT_WORKER_PROVIDER_POLICY"

#: Логические способности. Список закрыт намеренно: способность, которой нет в
#: этом кортеже, отвергается на разборе требования, а не превращается в
#: «модель по умолчанию».
#:
#: До 11I способность была ровно одна, и различить «сильная модель для свода» и
#: «дешёвая для критика» было нечем — а фактический прогон использует шесть
#: разных классов моделей одновременно. Набор обязан быть НАДмножеством реестра
#: центра (`backend/app/services/audit_routing/registry.py`): центр не должен
#: иметь возможности заказать способность, которую ни один воркер не в
#: состоянии разрешить. Это проверяет отдельный тест.
#:
#: Имя `strong_audit` сохранено дословно: политики, написанные до 11I,
#: продолжают работать без правки файла администратором VPS.
CAPABILITY_STRONG_AUDIT = "strong_audit"
CAPABILITY_CHEAP_REVIEW = "cheap_review"
CAPABILITY_BLOCK_DETECTOR = "block_detector"
CAPABILITY_BLOCK_DETECTOR_STRONG = "block_detector_strong"
CAPABILITY_BLOCK_JUDGE = "block_judge"
CAPABILITY_VISUAL_REASONING = "visual_reasoning"

KNOWN_CAPABILITIES: tuple[str, ...] = (
    CAPABILITY_STRONG_AUDIT,
    CAPABILITY_CHEAP_REVIEW,
    CAPABILITY_BLOCK_DETECTOR,
    CAPABILITY_BLOCK_DETECTOR_STRONG,
    CAPABILITY_BLOCK_JUDGE,
    CAPABILITY_VISUAL_REASONING,
)

#: Суффикс варианта модели с окном 1M токенов. Не «алиас» и не «похожая
#: модель»: та же модель, другое окно контекста.
_LONG_CONTEXT_SUFFIX = "[1m]"

#: Сообщает ли CLI провайдера, КАКАЯ модель фактически ответила.
#:
#: `required` (умолчание) — сообщает, и несовпадение либо молчание считаются
#: отказом. Так работает Claude: `--output-format json` несёт `modelUsage`.
#:
#: `unsupported` — НЕ сообщает, и это свойство CLI, а не наша слепота.
#: Измерено на Codex 0.147.0: поток `exec --json` состоит из `thread.started`
#: (только `thread_id`), `turn.started`, `item.completed` и `turn.completed`
#: (только `usage`) — идентификатора модели нет ни в одном событии.
#:
#: Почему это ОТДЕЛЬНОЕ ЯВНОЕ поле, а не «если провайдер codex, то не сверяем».
#: Ослабление гейта обязано быть решением администратора машины, записанным в
#: его файле, а не веткой в коде: ветка расползается на новые провайдеры молча,
#: запись — нет. И она же документирует, ЧТО именно перестало проверяться.
MODEL_REPORT_REQUIRED = "required"
MODEL_REPORT_UNSUPPORTED = "unsupported"
MODEL_REPORT_MODES: tuple[str, ...] = (MODEL_REPORT_REQUIRED, MODEL_REPORT_UNSUPPORTED)


class ProviderPolicyError(RuntimeError):
    """Политика отсутствует, не читается или не покрывает запрошенное."""


@dataclass(frozen=True)
class CapabilityPolicy:
    """Что именно эта машина понимает под одной логической способностью."""

    provider: str
    capability: str
    #: Точный идентификатор, который уйдёт в CLI флагом `--model`.
    model: str
    #: Идентификаторы, которые CLI имеет право назвать фактическими.
    accepted_reported_models: tuple[str, ...]
    #: `required` | `unsupported` — см. MODEL_REPORT_MODES.
    model_report: str = MODEL_REPORT_REQUIRED

    def reported_matches(self, reported: Optional[str]) -> bool:
        """Совпал ли фактически применённый идентификатор с разрешённым.

        `None`/пустое значение НЕ считается совпадением, пока политика не
        объявила `model_report="unsupported"`. «Мы не смогли узнать, какая
        модель ответила» и «ответила нужная» — разные утверждения, и подменять
        второе первым значило бы вернуть ровно ту слепоту, которую этот модуль
        устраняет. Но и требовать от CLI того, чего он не умеет, — не строгость,
        а неработающий провайдер: см. MODEL_REPORT_MODES.
        """
        value = (reported or "").strip()
        if not value:
            return self.model_report == MODEL_REPORT_UNSUPPORTED
        return value in self.accepted_reported_models

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "capability": self.capability,
            "model": self.model,
            "accepted_reported_models": list(self.accepted_reported_models),
            "model_report": self.model_report,
        }


def default_accepted_reported(model: str) -> tuple[str, ...]:
    """Разрешённые формы фактического идентификатора для точной модели."""
    base = str(model or "").strip()
    if not base:
        return ()
    if base.endswith(_LONG_CONTEXT_SUFFIX):
        stripped = base[: -len(_LONG_CONTEXT_SUFFIX)]
        return (base, stripped)
    return (base, base + _LONG_CONTEXT_SUFFIX)


def policy_path(worker_root: Optional[Path] = None) -> Optional[Path]:
    """Где лежит политика. Переменная окружения побеждает корень данных."""
    raw = os.environ.get(POLICY_ENV, "").strip()
    if raw:
        return Path(raw)
    if worker_root is None:
        return None
    return Path(worker_root) / POLICY_FILENAME


def validate_model_id(value: Any, *, where: str) -> str:
    """Проверить идентификатор модели. Публичная: её зовёт и разбор привязки."""
    if not isinstance(value, str):
        raise ProviderPolicyError(f"{where}: ожидается строка")
    model = value.strip()
    if not model:
        raise ProviderPolicyError(f"{where}: пустой идентификатор модели")
    if len(model) > 128:
        raise ProviderPolicyError(f"{where}: идентификатор длиннее 128 символов")
    if model.startswith("-"):
        # Ведущий дефис делает «идентификатор модели» ещё одним ФЛАГОМ CLI.
        # Разбор опций отдаёт обязательному значению следующий токен
        # безусловно, поэтому сегодня такая строка стала бы просто неизвестной
        # моделью — но полагаться на разбор чужого CLI в вопросе «что попадёт
        # в argv» нельзя.
        raise ProviderPolicyError(
            f"{where}: идентификатор не может начинаться с дефиса ({model!r})"
        )
    # Закрытый набор символов: идентификатор уходит в argv, и «почти любая
    # строка» там нам не нужна. Скобки разрешены ради суффикса `[1m]`.
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_[]")
    bad = sorted(set(model) - allowed)
    if bad:
        raise ProviderPolicyError(
            f"{where}: недопустимые символы {bad} в идентификаторе модели"
        )
    return model


#: Прежнее приватное имя. Оставлено, чтобы правка не разъехалась по файлу.
_validate_model_id = validate_model_id


@dataclass(frozen=True)
class ProviderPolicy:
    """Разобранный файл политики. Только чтение."""

    policy_version: int
    source_path: Optional[Path]
    capabilities: dict[tuple[str, str], CapabilityPolicy]
    auth_modes: dict[str, str]

    def resolve(self, provider: str, capability: str) -> CapabilityPolicy:
        """Точная модель для пары «провайдер + способность». Иначе — отказ."""
        name = require_provider(str(provider or ""))
        cap = str(capability or "").strip()
        found = self.capabilities.get((name, cap))
        if found is None:
            known = sorted(c for (p, c) in self.capabilities if p == name)
            raise ProviderPolicyError(
                f"локальная политика воркера не описывает способность {cap!r} "
                f"для провайдера {name!r} (известны: {known}). Умолчания нет "
                f"намеренно: молчаливый выбор модели запрещён"
            )
        return found

    def as_public_dict(self) -> dict[str, Any]:
        """Вид для отчёта. Путей файловой системы здесь нет."""
        return {
            "policy_version": self.policy_version,
            "auth_modes": dict(self.auth_modes),
            "capabilities": [value.as_dict() for value in self.capabilities.values()],
        }


def parse_policy(payload: Any, *, source_path: Optional[Path] = None) -> ProviderPolicy:
    """Разобрать содержимое политики. Строго: неизвестное поле — отказ."""
    if not isinstance(payload, dict):
        raise ProviderPolicyError("политика провайдеров: ожидается объект")
    version = payload.get("policy_version")
    if version != POLICY_SCHEMA_VERSION:
        raise ProviderPolicyError(
            f"политика провайдеров: policy_version={version!r}, "
            f"поддерживается {POLICY_SCHEMA_VERSION}"
        )
    unknown = set(payload) - {"policy_version", *SUPPORTED_PROVIDERS}
    if unknown:
        raise ProviderPolicyError(
            f"политика провайдеров: недопустимые ключи {sorted(unknown)}"
        )
    capabilities: dict[tuple[str, str], CapabilityPolicy] = {}
    auth_modes: dict[str, str] = {}
    for provider in SUPPORTED_PROVIDERS:
        block = payload.get(provider)
        if block is None:
            continue
        if not isinstance(block, dict):
            raise ProviderPolicyError(f"политика {provider}: ожидается объект")
        block_unknown = set(block) - {"auth_mode", "capabilities"}
        if block_unknown:
            raise ProviderPolicyError(
                f"политика {provider}: недопустимые ключи {sorted(block_unknown)}"
            )
        auth_mode = block.get("auth_mode")
        if auth_mode is not None:
            if not isinstance(auth_mode, str) or not auth_mode.strip():
                raise ProviderPolicyError(f"политика {provider}.auth_mode: строка")
            auth_modes[provider] = auth_mode.strip()
        caps = block.get("capabilities") or {}
        if not isinstance(caps, dict):
            raise ProviderPolicyError(
                f"политика {provider}.capabilities: ожидается объект"
            )
        for cap_name, cap_body in caps.items():
            if cap_name not in KNOWN_CAPABILITIES:
                raise ProviderPolicyError(
                    f"политика {provider}: неизвестная способность {cap_name!r} "
                    f"(известны: {list(KNOWN_CAPABILITIES)})"
                )
            if not isinstance(cap_body, dict):
                raise ProviderPolicyError(
                    f"политика {provider}.{cap_name}: ожидается объект"
                )
            cap_unknown = set(cap_body) - {
                "model", "accepted_reported_models", "model_report",
            }
            if cap_unknown:
                raise ProviderPolicyError(
                    f"политика {provider}.{cap_name}: недопустимые ключи "
                    f"{sorted(cap_unknown)}"
                )
            model = _validate_model_id(
                cap_body.get("model"), where=f"политика {provider}.{cap_name}.model"
            )
            raw_accepted = cap_body.get("accepted_reported_models")
            if raw_accepted is None:
                accepted = default_accepted_reported(model)
            else:
                if not isinstance(raw_accepted, list) or not raw_accepted:
                    raise ProviderPolicyError(
                        f"политика {provider}.{cap_name}.accepted_reported_models: "
                        "непустой список строк"
                    )
                accepted = tuple(
                    _validate_model_id(
                        item,
                        where=f"политика {provider}.{cap_name}.accepted_reported_models",
                    )
                    for item in raw_accepted
                )
                if model not in accepted:
                    raise ProviderPolicyError(
                        f"политика {provider}.{cap_name}: запрошенная модель "
                        f"{model!r} обязана входить в accepted_reported_models"
                    )
            model_report = cap_body.get("model_report", MODEL_REPORT_REQUIRED)
            if model_report not in MODEL_REPORT_MODES:
                raise ProviderPolicyError(
                    f"политика {provider}.{cap_name}.model_report={model_report!r}: "
                    f"допустимы {list(MODEL_REPORT_MODES)}"
                )
            capabilities[(provider, cap_name)] = CapabilityPolicy(
                provider=provider,
                capability=cap_name,
                model=model,
                accepted_reported_models=accepted,
                model_report=model_report,
            )
    if not capabilities:
        raise ProviderPolicyError(
            "политика провайдеров не описывает ни одной способности: "
            "пустая политика равносильна её отсутствию"
        )
    return ProviderPolicy(
        policy_version=int(version),
        source_path=Path(source_path) if source_path else None,
        capabilities=capabilities,
        auth_modes=auth_modes,
    )


def load_policy(worker_root: Optional[Path] = None) -> ProviderPolicy:
    """Прочитать политику с диска. Отсутствие файла — отказ, не умолчание."""
    path = policy_path(worker_root)
    if path is None:
        raise ProviderPolicyError(
            f"не задан ни {POLICY_ENV}, ни корень данных воркера: "
            "локальную политику моделей неоткуда прочитать"
        )
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ProviderPolicyError(
            f"локальная политика моделей не найдена: {path}. Рабочий вызов "
            "модели без явно назначенной модели не выполняется"
        ) from None
    except OSError as exc:
        raise ProviderPolicyError(f"политика моделей не читается: {exc}") from None
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise ProviderPolicyError(f"политика моделей не разбирается: {exc}") from None
    return parse_policy(payload, source_path=Path(path))
