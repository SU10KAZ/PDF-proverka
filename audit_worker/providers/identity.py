"""ProviderIdentity — что воркер знает о провайдере, и что из этого едет в центр.

Разделение на два представления здесь не стилистическое. `as_dict()` — полное
локальное состояние (для диагностики на самом VPS), `as_center_payload()` —
СТРОГО подмножество, которое уезжает в heartbeat. Всё, чего нет во втором
методе, до центра не доедет никогда, потому что второй метод перечисляет поля
поимённо, а не вычитает запрещённые из первого. Список запрещённого рано или
поздно отстаёт от списка полей; список разрешённого — нет.

Чего в центральном представлении нет намеренно (§8 задания):
  * абсолютных путей provider home — они описывают раскладку и имя пользователя
    чужой машины;
  * e-mail учётной записи — по умолчанию не отправляется вовсе;
  * любых токенов и содержимого credential-файлов — воркер их не читает.

`account_fingerprint` — солёный отпечаток, а не идентификатор. Соль своя у
каждого воркера и лежит в `providers/<p>/metadata/`, поэтому:
  * восстановить по нему учётную запись нельзя;
  * СОПОСТАВИТЬ два воркера по нему тоже нельзя — и это сделано специально:
    объединение одинаковых аккаунтов на разных VPS выполняет оператор вручную
    через `account_group_id` (§8), а не догадка по секретным данным.
Отпечаток отвечает ровно на один вопрос: «на ЭТОМ воркере сменилась учётная
запись?» — и на него отвечает надёжно.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from audit_worker.providers.auth_mode import AUTH_MODES, DEFAULT_AUTH_MODE

# ─── Состояния установки ─────────────────────────────────────────────────────
INSTALL_INSTALLED = "installed"
INSTALL_MISSING = "missing"
INSTALL_BROKEN = "broken"

INSTALLATION_STATES: tuple[str, ...] = (
    INSTALL_INSTALLED,
    INSTALL_MISSING,
    INSTALL_BROKEN,
)

# ─── Состояния авторизации ───────────────────────────────────────────────────
AUTH_LOGGED_IN = "logged_in"
AUTH_LOGGED_OUT = "logged_out"
AUTH_EXPIRED = "expired"
AUTH_UNKNOWN = "unknown"
AUTH_ERROR = "error"

AUTH_STATES: tuple[str, ...] = (
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    AUTH_EXPIRED,
    AUTH_UNKNOWN,
    AUTH_ERROR,
)

# ─── Политика использования ──────────────────────────────────────────────────
POLICY_ALLOWED = "allowed"
POLICY_REVIEW_REQUIRED = "review_required"
POLICY_BLOCKED = "policy_blocked"

POLICY_STATES: tuple[str, ...] = (
    POLICY_ALLOWED,
    POLICY_REVIEW_REQUIRED,
    POLICY_BLOCKED,
)

_FINGERPRINT_SALT_FILE = "account_fingerprint.salt"
_FINGERPRINT_PREFIX_LEN = 16


def _load_or_create_salt(metadata_dir: Path) -> bytes:
    """Соль отпечатка. Создаётся один раз и переживает обновление воркера.

    Пишется с режимом 0600 через временный файл: половинчатая соль сделала бы
    отпечаток нестабильным, и центр увидел бы «аккаунт сменился» на ровном месте.
    """
    path = Path(metadata_dir) / _FINGERPRINT_SALT_FILE
    try:
        raw = path.read_bytes().strip()
        if len(raw) >= 32:
            return raw
    except OSError:
        pass
    salt = secrets.token_bytes(32)
    try:
        metadata_dir.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "wb", opener=lambda p, f: os.open(p, f, 0o600)) as fh:
            fh.write(salt)
        os.replace(tmp, path)
    except OSError:
        # Не смогли сохранить — отпечаток станет эфемерным. Это хуже, но не
        # фатально: он используется только для «сменился ли аккаунт».
        pass
    return salt


def account_fingerprint(
    metadata_dir: Path, *, provider: str, stable_identifier: Optional[str]
) -> Optional[str]:
    """Солёный отпечаток учётной записи. `None`, если идентификатора нет.

    `stable_identifier` — НЕ секрет: это то, что провайдер сам показывает как
    имя учётной записи (например, e-mail из `account/read` Codex). Секретные
    данные сюда не передаются ни при каких условиях — по ним идентифицировать
    аккаунт прямо запрещено (§8).
    """
    text = str(stable_identifier or "").strip().lower()
    if not text:
        return None
    salt = _load_or_create_salt(Path(metadata_dir))
    digest = hmac.new(salt, f"{provider}\x00{text}".encode("utf-8"), hashlib.sha256)
    return digest.hexdigest()[:_FINGERPRINT_PREFIX_LEN]


def credential_file_facts(path: Path) -> dict[str, Any]:
    """Факты о файле учётных данных БЕЗ его открытия.

    Разрешено проверять только существование, режим и владельца (§5 задания).
    `open()` в этом модуле нет и быть не должно: даже чтение «на всякий случай»
    означало бы, что содержимое побывало в памяти процесса, который пишет логи.
    """
    try:
        info = os.stat(path)
    except OSError:
        return {
            "exists": False,
            "mode": None,
            "owner_uid": None,
            "group_readable": None,
            "world_readable": None,
            "size_bytes": None,
        }
    mode = stat.S_IMODE(info.st_mode)
    return {
        "exists": True,
        "mode": f"{mode:04o}",
        "owner_uid": info.st_uid,
        "owner_is_current_user": info.st_uid == os.getuid(),
        "group_readable": bool(mode & stat.S_IRGRP),
        "world_readable": bool(mode & stat.S_IROTH),
        # Размер — не содержимое. Он нужен ровно для одного: отличить пустой
        # файл-заглушку от настоящего, не открывая его.
        "size_bytes": info.st_size,
    }


@dataclass(frozen=True)
class ProviderIdentity:
    """Состояние одного провайдера на одном воркере (§8 задания)."""

    provider: str
    installation_status: str
    auth_state: str
    auth_method: str
    policy_state: str
    inference_allowed: bool
    last_auth_check_at: float
    #: ОТКУДА взята авторизация (`auth_mode.AUTH_MODES`). Отдельно от
    #: `auth_method`: тот отвечает «чем вошли» (`claudeai`, `chatgpt`, `apiKey`),
    #: этот — «где лежат учётные данные». Оператору нужны оба: «вошли по
    #: подписке» и «из личного каталога пользователя VPS» — разные факты.
    auth_mode: str = DEFAULT_AUTH_MODE
    cli_version: Optional[str] = None
    plan_type: Optional[str] = None
    account_group_id: Optional[str] = None
    account_fingerprint: Optional[str] = None
    #: Абсолютный путь. Живёт ТОЛЬКО локально — в центральное представление не
    #: попадает (см. `as_center_payload`).
    provider_home: Optional[Path] = None
    executable_path: Optional[Path] = None
    credential_facts: dict[str, Any] = field(default_factory=dict)
    capability: dict[str, Any] = field(default_factory=dict)
    error_code: Optional[str] = None
    detail: Optional[str] = None

    def __post_init__(self) -> None:
        if self.installation_status not in INSTALLATION_STATES:
            raise ValueError(f"installation_status={self.installation_status!r}")
        if self.auth_state not in AUTH_STATES:
            raise ValueError(f"auth_state={self.auth_state!r}")
        if self.policy_state not in POLICY_STATES:
            raise ValueError(f"policy_state={self.policy_state!r}")
        if self.auth_mode not in AUTH_MODES:
            raise ValueError(f"auth_mode={self.auth_mode!r}")

    @property
    def usable_for_inference(self) -> bool:
        return (
            self.installation_status == INSTALL_INSTALLED
            and self.auth_state == AUTH_LOGGED_IN
            and self.policy_state == POLICY_ALLOWED
            and self.inference_allowed
        )

    def as_dict(self) -> dict[str, Any]:
        """Полное локальное представление (диагностика на самом VPS)."""
        return {
            "provider": self.provider,
            "installation_status": self.installation_status,
            "cli_version": self.cli_version,
            "auth_state": self.auth_state,
            "auth_method": self.auth_method,
            "auth_mode": self.auth_mode,
            "plan_type": self.plan_type,
            "account_group_id": self.account_group_id,
            "account_fingerprint": self.account_fingerprint,
            "last_auth_check_at": self.last_auth_check_at,
            "provider_home": str(self.provider_home) if self.provider_home else None,
            "executable_path": str(self.executable_path) if self.executable_path else None,
            "inference_allowed": self.inference_allowed,
            "policy_state": self.policy_state,
            "credential_facts": dict(self.credential_facts),
            "capability": dict(self.capability),
            "error_code": self.error_code,
            "detail": self.detail,
        }

    def as_center_payload(self) -> dict[str, Any]:
        """Подмножество ДЛЯ ЦЕНТРА. Перечисление, а не вычитание.

        Абсолютные пути заменены на факт наличия; credential_facts урезаны до
        режима и признаков доступности — этого достаточно, чтобы показать
        «права слишком широкие», и недостаточно, чтобы что-то узнать о машине.
        """
        creds = dict(self.credential_facts or {})
        return {
            "provider": self.provider,
            "installation_status": self.installation_status,
            "cli_version": self.cli_version,
            "auth_state": self.auth_state,
            "auth_method": self.auth_method,
            # Режим — не путь. Слово `ambient_user` не раскрывает ни имени
            # пользователя, ни раскладки каталогов; оно отвечает на вопрос
            # «почему этот воркер авторизован», без которого центр не может
            # отличить настроенный воркер от случайно унаследовавшего доступ.
            "auth_mode": self.auth_mode,
            "plan_type": self.plan_type,
            "account_group_id": self.account_group_id,
            "account_fingerprint": self.account_fingerprint,
            "last_auth_check_at": self.last_auth_check_at,
            "inference_allowed": self.inference_allowed,
            "policy_state": self.policy_state,
            "provider_home_present": bool(
                self.provider_home and Path(self.provider_home).is_dir()
            ),
            "credential_present": bool(creds.get("exists")),
            "credential_mode": creds.get("mode"),
            "credential_group_readable": creds.get("group_readable"),
            "credential_world_readable": creds.get("world_readable"),
            "credential_owner_is_current_user": creds.get("owner_is_current_user"),
            "capability": dict(self.capability or {}),
            "error_code": self.error_code,
            "detail": self.detail,
        }
