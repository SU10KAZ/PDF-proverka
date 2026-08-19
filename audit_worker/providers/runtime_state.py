"""Доказанная работоспособность провайдера: что ответил ПРОВАЙДЕР, а не CLI.

Зачем это отдельно от авторизации
─────────────────────────────────
19.08.2026 на боевом воркере 11l Claude показывал `installed` + `logged_in` +
`subscriptionType=max`, и карточка называла его доступным. Первый же настоящий
запрос вернул HTTP 403: «Your organization has disabled Claude subscription
access for Claude Code». То есть учётные данные были верны, вход выполнен, а
работать провайдер не мог.

Отсюда правило, которое этот модуль и реализует:

    АВТОРИЗОВАН ≠ ПРИГОДЕН К РАБОТЕ

`claude auth status` отвечает на вопрос «приняты ли учётные данные». На вопрос
«ответит ли провайдер на запрос» отвечает только сам провайдер — и только
настоящим обращением. Значит вывод о пригодности можно делать лишь по факту
уже состоявшегося обращения, а его результат обязан пережить перезапуск
процесса: иначе после каждого рестарта воркер снова полчаса рассказывал бы
центру, что всё в порядке, — до следующего задания, которое опять упало бы.

Что здесь хранится и чего здесь нет
───────────────────────────────────
Хранится ровно исход: состояние, код ошибки, когда наблюдалось, и когда в
последний раз был УСПЕХ. Не хранится ни промпт, ни ответ, ни учётные данные,
ни текст ошибки провайдера дословно — оператору в интерфейс уходит код, а не
чужая строка.

Защёлки нет
───────────
Состояние всегда описывает ПОСЛЕДНЕЕ обращение. Успешный вызов снимает
`entitlement_blocked` немедленно и без ручного вмешательства — запрет
организации могут снять, и воркер обязан это заметить сам. Симметрично: новый
403 возвращает блокировку, даже если час назад всё работало.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from audit_worker.providers import errors
from audit_worker.providers.paths import providers_root, require_provider

FILENAME = "runtime_result.json"

#: Обращений ещё не было — про пригодность НИЧЕГО не известно. Это не «всё
#: хорошо» и не «всё плохо»: третье состояние обязано существовать, иначе
#: свежеустановленный воркер пришлось бы объявлять либо годным без оснований,
#: либо негодным без вины.
RUNTIME_UNKNOWN = "unknown"
#: Последнее обращение прошло: провайдер ответил.
RUNTIME_READY = "ready"
#: Провайдер отказал в доступе самой учётной записи (403 организации).
RUNTIME_ENTITLEMENT_BLOCKED = "entitlement_blocked"
#: Обращение не удалось по другой причине (сеть, таймаут, сбой CLI). Отдельно
#: от `entitlement_blocked`, потому что действие оператора другое: тут ждать и
#: смотреть логи, там — идти к администратору организации.
RUNTIME_ERROR = "error"

RUNTIME_STATES: tuple[str, ...] = (
    RUNTIME_UNKNOWN,
    RUNTIME_READY,
    RUNTIME_ENTITLEMENT_BLOCKED,
    RUNTIME_ERROR,
)

#: Коды отказа, означающие «учётная запись есть, но работать ей не дают».
_ENTITLEMENT_CODES: frozenset[str] = frozenset({errors.ERR_ENTITLEMENT_BLOCKED})


@dataclass(frozen=True)
class RuntimeResult:
    """Что известно о пригодности провайдера по последнему обращению."""

    provider: str
    state: str = RUNTIME_UNKNOWN
    error_code: Optional[str] = None
    observed_at: Optional[float] = None
    last_success_at: Optional[float] = None

    @property
    def blocked(self) -> bool:
        return self.state == RUNTIME_ENTITLEMENT_BLOCKED

    @property
    def proven_usable(self) -> bool:
        """Доказано ли обращением, что провайдер отвечает."""
        return self.state == RUNTIME_READY

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "error_code": self.error_code,
            "observed_at": self.observed_at,
            "last_success_at": self.last_success_at,
        }


def state_path(worker_root: Path, provider: str) -> Path:
    """Файл состояния рядом с прочими нашими заметками о провайдере.

    Каталог `metadata/` принадлежит воркеру в ЛЮБОМ режиме авторизации (см.
    `paths`), поэтому состояние не оказывается в личном каталоге человека даже
    в ambient-режиме.
    """
    return providers_root(worker_root) / provider / "metadata" / FILENAME


def state_path_for_provider_root(provider_root: Path) -> Path:
    """Тот же файл, но от каталога ПРОВАЙДЕРА, а не от корня воркера.

    Нужен мосту конвейера: он работает внутри каталога попытки и знает только
    `provider_root` из привязки. Гонять корень воркера через привязку ради
    одной заметки не стоит — путь и так однозначен.
    """
    return Path(provider_root) / "metadata" / FILENAME


def record_at_provider_root(
    provider_root: Path,
    provider: str,
    *,
    success: bool,
    error_code: Optional[str] = None,
    now: Optional[float] = None,
) -> None:
    """Записать исход, зная каталог провайдера. Ошибки записи не поднимаются."""
    name = require_provider(provider)
    target = state_path_for_provider_root(provider_root)
    moment = float(now) if now is not None else time.time()
    previous_success: Optional[float] = None
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            previous_success = _opt_float(raw.get("last_success_at"))
    except (OSError, ValueError, TypeError):
        previous_success = None
    if success:
        state, code, last_success = RUNTIME_READY, None, moment
    else:
        code = str(error_code or errors.ERR_UNKNOWN)
        state = (
            RUNTIME_ENTITLEMENT_BLOCKED if code in _ENTITLEMENT_CODES else RUNTIME_ERROR
        )
        last_success = previous_success
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(
                {
                    "provider": name,
                    "state": state,
                    "error_code": code if not success else None,
                    "observed_at": moment,
                    "last_success_at": last_success,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        os.replace(tmp, target)
    except OSError:
        pass


def read(worker_root: Path, provider: str) -> RuntimeResult:
    """Прочитать состояние. Любая беда с файлом читается как «неизвестно»."""
    name = require_provider(provider)
    try:
        raw = json.loads(state_path(worker_root, name).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return RuntimeResult(provider=name)
    if not isinstance(raw, dict):
        return RuntimeResult(provider=name)
    state = str(raw.get("state") or RUNTIME_UNKNOWN)
    if state not in RUNTIME_STATES:
        state = RUNTIME_UNKNOWN
    return RuntimeResult(
        provider=name,
        state=state,
        error_code=_opt_str(raw.get("error_code")),
        observed_at=_opt_float(raw.get("observed_at")),
        last_success_at=_opt_float(raw.get("last_success_at")),
    )


def record(
    worker_root: Path,
    provider: str,
    *,
    success: bool,
    error_code: Optional[str] = None,
    now: Optional[float] = None,
) -> RuntimeResult:
    """Записать исход НАСТОЯЩЕГО обращения к провайдеру.

    Вызывается только там, где обращение действительно состоялось: контрольный
    запрос и рабочий вызов конвейера. Неудача ЗАПУСКА (CLI не найден, нет
    разрешения) сюда не попадает — она ничего не говорит о провайдере.

    `last_success_at` не стирается при неудаче: «когда в последний раз
    работало» — самостоятельная новость, и особенно ценная в момент, когда
    перестало.
    """
    name = require_provider(provider)
    moment = float(now) if now is not None else time.time()
    previous = read(worker_root, name)
    if success:
        state, code, last_success = RUNTIME_READY, None, moment
    else:
        code = str(error_code or errors.ERR_UNKNOWN)
        state = (
            RUNTIME_ENTITLEMENT_BLOCKED if code in _ENTITLEMENT_CODES else RUNTIME_ERROR
        )
        last_success = previous.last_success_at
    payload = {
        "provider": name,
        "state": state,
        "error_code": code if not success else None,
        "observed_at": moment,
        "last_success_at": last_success,
    }
    target = state_path(worker_root, name)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, target)
    except OSError:
        # Не поднимаем наверх: невозможность записать заметку не должна
        # проваливать сам вызов, ради которого всё делалось.
        pass
    return RuntimeResult(
        provider=name,
        state=state,
        error_code=payload["error_code"],
        observed_at=moment,
        last_success_at=last_success,
    )


def _opt_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text[:64] or None


def _opt_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if number == number and number > 0 else None
