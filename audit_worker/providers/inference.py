"""Структурированный результат РАБОЧЕГО вызова модели и его проверка.

Этап 11 дал слою провайдеров ровно один вид обращения к модели —
`minimal_probe()`: фиксированная фраза, фиксированный ответ, «совпало или нет».
Для конвейера этого мало: конвейеру нужен РЕЗУЛЬТАТ, а значит нужен контракт,
в котором результат отделён от служебных сведений о запуске и в котором
«модель ответила» и «ответ пригоден» — разные утверждения.

Почему отдельный модуль, а не поля в `ProbeResult`:

  * `ProbeResult` описывает ПРОВЕРКУ канала («канал жив, фраза совпала»), а не
    работу. Смешать их значило бы, что успешный probe и успешный этап аудита
    неразличимы в отчётах и в heartbeat;
  * проверка результата (§7 задания) — это девять независимых утверждений, и
    им нужно место, где их можно перечислить поимённо и вернуть по одному.
    «Валидно/невалидно» одним булевым значением не даёт разобрать, ЧТО именно
    не сошлось, а разбирать приходится на чужой машине по журналу;
  * сырой ответ модели не имеет права уехать в heartbeat и в EventOutbox
    (§6 задания). Поэтому в контракте живёт ОТПЕЧАТОК ответа, а сам текст
    остаётся в защищённом артефакте попытки.

Ничего про конкретного провайдера здесь нет: и Claude, и Codex приводятся к
этому виду своими адаптерами.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Sequence

#: Значения поля `status`. Закрытый список: «частично получилось» не бывает —
#: либо результат пригоден дальше по конвейеру, либо это ошибка.
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"

#: Имя конвейера. Константа модуля, а не строка из задания: подставить сюда
#: чужое значение значило бы разрешить воркеру заявить любой конвейер.
PIPELINE_NAME = "audit_pipeline_v1"

#: Формы, похожие на учётные данные. Дубликат правил `redaction` — намеренный:
#: редактор ЧИСТИТ, а этот список ЛОВИТ. Чистка, сработавшая молча, оставила бы
#: нас в уверенности, что секрета не было вовсе.
_CREDENTIAL_LIKE = (
    re.compile(r"sk-ant-[A-Za-z0-9_\-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}"),
    re.compile(r"\b(wtk_|etk_|clm_)[A-Za-z0-9_\-]{8,}"),
    re.compile(r"eyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}\.[A-Za-z0-9_\-]{4,}"),
    re.compile(r"(?i)\bauthorization\s*:\s*\S+"),
    re.compile(r"(?i)\b[A-Z0-9_]*(TOKEN|SECRET|PASSWORD|API_KEY)\s*[:=]\s*\S+"),
)

#: Абсолютные приватные пути. Домашний каталог человека и каталог учётных
#: данных не имеют права оказаться в ответе модели: он уезжает на центр.
_PRIVATE_PATHS = (
    re.compile(r"/home/[A-Za-z0-9._-]+"),
    re.compile(r"/root/"),
    re.compile(r"\.credentials\.json"),
    re.compile(r"/\.codex/auth\.json"),
)


@dataclass(frozen=True)
class ProviderInferenceResult:
    """Итог ОДНОГО рабочего обращения к модели через ProviderAdapter.

    Поля ровно те, что перечислены §6 задания, плюс два добавленных по факту
    работы слоя: `auth_mode` (иначе «откуда взялась авторизация» неизвестно
    задним числом) и `raw_sha256` (отпечаток сырого ответа — он позволяет
    сверить артефакт с тем, что было возвращено, не таская сам текст).
    """

    provider: str
    model: Optional[str]
    status: str
    result: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    exit_code: Optional[int] = None
    auth_mode: str = ""
    error_code: Optional[str] = None
    detail: Optional[str] = None
    #: sha256 сырого текста ответа. Сам текст здесь не хранится намеренно.
    raw_sha256: str = ""
    raw_bytes: int = 0

    @property
    def ok(self) -> bool:
        return self.status == STATUS_SUCCESS

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "status": self.status,
            "result": dict(self.result),
            "usage": dict(self.usage),
            "duration_ms": int(self.duration_ms),
            "exit_code": self.exit_code,
            "auth_mode": self.auth_mode,
            "error_code": self.error_code,
            "detail": self.detail,
            "raw_sha256": self.raw_sha256,
            "raw_bytes": int(self.raw_bytes),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ProviderInferenceResult":
        data = payload or {}
        return cls(
            provider=str(data.get("provider") or ""),
            model=data.get("model"),
            status=str(data.get("status") or STATUS_ERROR),
            result=dict(data.get("result") or {}),
            usage=dict(data.get("usage") or {}),
            duration_ms=int(data.get("duration_ms") or 0),
            exit_code=data.get("exit_code"),
            auth_mode=str(data.get("auth_mode") or ""),
            error_code=data.get("error_code"),
            detail=data.get("detail"),
            raw_sha256=str(data.get("raw_sha256") or ""),
            raw_bytes=int(data.get("raw_bytes") or 0),
        )


def sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", "replace")).hexdigest()


@dataclass(frozen=True)
class ValidationCheck:
    """Одно утверждение проверки. Именованное — иначе разбирать нечего."""

    name: str
    passed: bool
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "passed": self.passed, "detail": self.detail}


@dataclass(frozen=True)
class ValidationReport:
    """Свод проверок. `passed` — конъюнкция, а не отдельно хранимый флаг."""

    checks: tuple[ValidationCheck, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_names(self) -> list[str]:
        return [check.name for check in self.checks if not check.passed]

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failed": self.failed_names,
            "checks": [check.as_dict() for check in self.checks],
        }


def _find_credential_like(text: str) -> Optional[str]:
    for pattern in _CREDENTIAL_LIKE:
        match = pattern.search(text or "")
        if match:
            # В отчёт уезжает ИМЯ правила, а не совпавшая подстрока: §19
            # задания прямо запрещает копировать секрет в отчёт даже ради
            # доказательства.
            return pattern.pattern
    return None


def _find_private_path(text: str) -> Optional[str]:
    for pattern in _PRIVATE_PATHS:
        if pattern.search(text or ""):
            return pattern.pattern
    return None


def validate_inference(
    result: ProviderInferenceResult,
    *,
    expected_provider: str,
    expected_auth_mode: str,
    required_result_fields: Sequence[str] = (),
    field_types: Optional[dict[str, type | tuple[type, ...]]] = None,
    expected_semantics: Optional[dict[str, Any]] = None,
    forbidden_literals: Iterable[str] = (),
    task_id: str = "",
    attempt_id: str = "",
    claim_task_id: str = "",
    claim_attempt_id: str = "",
) -> ValidationReport:
    """Проверить результат по §7 задания. Возвращает ИМЕНОВАННЫЕ утверждения.

    Проверка сознательно не бросает исключений: вызывающему нужен полный
    список того, что не сошлось, а не первое несовпадение. Решение «годен /
    не годен» принимает он же, по `report.passed`.

    `forbidden_literals` — значения, которых в ответе быть не должно (например
    содержимое контрольного файла). Литералы приходят ИЗВНЕ и в репозитории не
    хранятся: хранить контрольную строку в Git значило бы, что её «не нашли»
    ничего не доказывает.
    """
    checks: list[ValidationCheck] = []
    payload = result.result if isinstance(result.result, dict) else {}
    # Текст, по которому идут запретительные проверки: и разобранный результат,
    # и служебные строки. Проверять только `result` мало — `detail` тоже уезжает
    # в пакет.
    haystack = json.dumps(
        {"result": payload, "detail": result.detail, "model": result.model},
        ensure_ascii=False,
        sort_keys=True,
    )

    checks.append(ValidationCheck(
        "exit_code", result.exit_code == 0,
        f"код возврата CLI: {result.exit_code!r}",
    ))
    checks.append(ValidationCheck(
        "status", result.status == STATUS_SUCCESS,
        f"status={result.status!r} error_code={result.error_code!r}",
    ))
    checks.append(ValidationCheck(
        "json_parsed", bool(payload),
        "ответ модели разобран как JSON-объект" if payload
        else "ответ модели не разобран как JSON-объект",
    ))

    missing = [name for name in required_result_fields if name not in payload]
    checks.append(ValidationCheck(
        "required_fields", not missing,
        f"нет обязательных полей: {missing}" if missing else "все поля на месте",
    ))

    type_problems: list[str] = []
    for name, expected_type in (field_types or {}).items():
        if name not in payload:
            continue
        if not isinstance(payload[name], expected_type):
            type_problems.append(
                f"{name}: {type(payload[name]).__name__}"
            )
    checks.append(ValidationCheck(
        "field_types", not type_problems,
        f"неверные типы: {type_problems}" if type_problems else "типы полей верны",
    ))

    semantic_problems: list[str] = []
    for name, expected in (expected_semantics or {}).items():
        actual = payload.get(name)
        if isinstance(expected, (list, tuple, set)):
            if actual not in expected:
                semantic_problems.append(f"{name}={actual!r} не в {sorted(expected)}")
        elif actual != expected:
            semantic_problems.append(f"{name}={actual!r} вместо {expected!r}")
    checks.append(ValidationCheck(
        "expected_semantics", not semantic_problems,
        "; ".join(semantic_problems) if semantic_problems
        else "смысловые ожидания выполнены",
    ))

    credential = _find_credential_like(haystack)
    checks.append(ValidationCheck(
        "no_credential_like", credential is None,
        f"сработало правило {credential}" if credential else "форм учётных данных нет",
    ))

    private_path = _find_private_path(haystack)
    checks.append(ValidationCheck(
        "no_private_paths", private_path is None,
        f"сработало правило {private_path}" if private_path
        else "абсолютных приватных путей нет",
    ))

    literals = [value for value in forbidden_literals if value and len(value) >= 8]
    hit = next((value for value in literals if value in haystack), None)
    checks.append(ValidationCheck(
        "no_forbidden_literals", hit is None,
        # Само значение в отчёт не попадает — только факт совпадения.
        "найден запрещённый литерал" if hit is not None
        else f"проверено запрещённых литералов: {len(literals)}",
    ))

    checks.append(ValidationCheck(
        "provider_matches_task", result.provider == expected_provider,
        f"провайдер {result.provider!r}, ожидался {expected_provider!r}",
    ))
    checks.append(ValidationCheck(
        "auth_mode_matches_task", result.auth_mode == expected_auth_mode,
        f"режим авторизации {result.auth_mode!r}, ожидался {expected_auth_mode!r}",
    ))
    checks.append(ValidationCheck(
        "identity_matches_claim",
        bool(task_id) and task_id == claim_task_id
        and bool(attempt_id) and attempt_id == claim_attempt_id,
        f"task_id={task_id!r}/{claim_task_id!r} attempt_id={attempt_id!r}/{claim_attempt_id!r}",
    ))
    return ValidationReport(checks=tuple(checks))


def build_pipeline_result(
    *,
    task_id: str,
    attempt_id: str,
    provider_result: ProviderInferenceResult,
    validation: ValidationReport,
    artifacts: Sequence[str] = (),
) -> dict[str, Any]:
    """Результат КОНВЕЙЕРА (§6 задания), а не провайдера.

    Разделение существенно: «модель ответила успешно» и «конвейер принял
    результат» — независимые утверждения, и второе сильнее первого. Пакет
    считается успешным только по второму.
    """
    return {
        "task_id": task_id,
        "attempt_id": attempt_id,
        "pipeline": PIPELINE_NAME,
        "status": "completed" if (provider_result.ok and validation.passed) else "failed",
        "provider_result": provider_result.as_dict(),
        "artifacts": list(artifacts),
        "validation": validation.as_dict(),
    }
