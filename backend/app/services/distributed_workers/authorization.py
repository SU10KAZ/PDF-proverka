"""Ролевая модель распределённых audit-worker: единственный источник прав.

Зачем отдельный модуль. До этого этапа операторский контур опирался только на
факт входа в портал: любой аутентифицированный пользователь мог отменить
попытку, признать её потерянной, заказать удаление данных с VPS и ротировать
worker-token. Журнал фиксировал, КТО это сделал, но не ограничивал НИКОГО
(§29.2 отчёта этапа 3.5). Реальный аудит дороже тестового, поэтому «вошёл =
можно всё» перестаёт быть приемлемым.

Что здесь есть и чего здесь нет:

  * есть три разрешения — `view`, `operate`, `admin` — и ровно один способ их
    получить: стабильный субъект фактической портальной аутентификации,
    перечисленный в конфигурации подсистемы;
  * НЕТ второй системы пользователей. Портал (`core/portal_auth`) остаётся
    единственным местом, где проверяются пароли и выдаётся сессия. Здесь
    решается только «что этому субъекту позволено в подсистеме воркеров»;
  * НЕТ ни одного пути, по которому роль пришла бы из запроса: тело, query,
    заголовок и localStorage не читаются вовсе (R-03). Единственный вход —
    `portal_auth.request_username()`, то есть подписанная HMAC-cookie.

Fail-closed — не лозунг, а порядок ветвлений: любая неопределённость
(аутентификация выключена, субъекта нет, конфигурация не разобрана, субъект
не перечислен) заканчивается отказом ДО обращения к БД, до создания
WorkerCommand и до выдачи живого токена (R-04, R-05, R-10).
"""
from __future__ import annotations

import os
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import HTTPException, Request

from backend.app.core import portal_auth
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersSettings,
    get_settings,
)

# ─── Разрешения ──────────────────────────────────────────────────────────────
PERM_VIEW = "distributed_workers.view"
PERM_OPERATE = "distributed_workers.operate"
PERM_ADMIN = "distributed_workers.admin"

ROLE_VIEWER = "viewer"
ROLE_OPERATOR = "operator"
ROLE_ADMIN = "admin"

#: Роль → её разрешения. `admin` ⊃ `operator` ⊃ `viewer` — вложенность задана
#: здесь один раз, а не проверками «или admin, или operator» по роутерам.
ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    ROLE_VIEWER: frozenset({PERM_VIEW}),
    ROLE_OPERATOR: frozenset({PERM_VIEW, PERM_OPERATE}),
    ROLE_ADMIN: frozenset({PERM_VIEW, PERM_OPERATE, PERM_ADMIN}),
}

#: Старшинство: субъект, перечисленный в нескольких списках, получает высшую
#: роль. Это не «ошибка конфигурации» — admin и так включает operate и view,
#: и запрещать администратору быть заодно в списке операторов бессмысленно.
_ROLE_RANK = {ROLE_VIEWER: 1, ROLE_OPERATOR: 2, ROLE_ADMIN: 3}

ENV_VIEWERS = "DISTRIBUTED_WORKERS_VIEWER_SUBJECTS"
ENV_OPERATORS = "DISTRIBUTED_WORKERS_OPERATOR_SUBJECTS"
ENV_ADMINS = "DISTRIBUTED_WORKERS_ADMIN_SUBJECTS"

_ENV_BY_ROLE = {
    ROLE_VIEWER: ENV_VIEWERS,
    ROLE_OPERATOR: ENV_OPERATORS,
    ROLE_ADMIN: ENV_ADMINS,
}

#: Разделители списка субъектов. Запятая — основной, перевод строки и точка с
#: запятой допущены, потому что длинный список в .env удобнее писать в столбик.
_SEPARATORS = (",", ";", "\n")

AUTH_SOURCE_PORTAL = "portal_session"
AUTH_SOURCE_NONE = "anonymous"


class AuthorizationConfigError(RuntimeError):
    """Конфигурация ролей не разобрана. Следствие — отказ, а не «пропустить»."""


# ─── Разбор конфигурации ─────────────────────────────────────────────────────
def normalize_subject(raw: str) -> str:
    """Канонический вид субъекта.

    NFC — та же нормализация, что у `identifiers.normalize_external_id`: «й» в
    двух кодировках не должно давать двух разных пользователей. Регистр НЕ
    приводится: логины портала регистрозависимы (`PORTAL_AUTH_USERS` сверяет
    строку как есть), и «Ivan» с правами админа при логине `ivan` — это тихое
    расширение прав, а не удобство.
    """
    return unicodedata.normalize("NFC", (raw or "").strip())


@dataclass(frozen=True)
class RoleConfig:
    """Снимок конфигурации ролей. Пересобирается на каждый запрос (дёшево)."""

    subjects: dict[str, str] = field(default_factory=dict)   # subject → роль
    errors: tuple[str, ...] = ()
    configured: bool = False

    @property
    def ok(self) -> bool:
        return not self.errors

    def role_for(self, subject: Optional[str]) -> Optional[str]:
        if not subject or not self.ok:
            return None
        return self.subjects.get(normalize_subject(subject))

    def diagnostics(self) -> Optional[str]:
        """Понятная администратору причина, почему прав нет."""
        if self.errors:
            return "Конфигурация ролей подсистемы не принята: " + "; ".join(self.errors)
        if not self.configured:
            return (
                "Роли подсистемы не настроены: ни один из "
                f"{ENV_VIEWERS} / {ENV_OPERATORS} / {ENV_ADMINS} не заполнен. "
                "Пока список пуст, прав нет ни у кого — это намеренное "
                "fail-closed поведение, а не сбой."
            )
        return None


def _parse_list(raw: str, *, env_name: str, errors: list[str]) -> list[str]:
    """Разобрать список субъектов. Любая подозрительная запись — ошибка."""
    text = raw or ""
    for sep in _SEPARATORS[1:]:
        text = text.replace(sep, _SEPARATORS[0])
    out: list[str] = []
    for chunk in text.split(_SEPARATORS[0]):
        subject = normalize_subject(chunk)
        if not subject:
            continue
        if "*" in subject or "?" in subject:
            # Шаблоны запрещены буквой задания: «*» в списке админов — это не
            # конфигурация, а отключение защиты одним символом.
            errors.append(
                f"{env_name}: шаблоны и подстановки запрещены (получено {subject!r})"
            )
            continue
        if any(ch == "\x00" or unicodedata.category(ch) == "Cc" for ch in subject):
            errors.append(f"{env_name}: управляющие символы в имени субъекта")
            continue
        if len(subject) > 200:
            errors.append(f"{env_name}: имя субъекта длиннее 200 символов")
            continue
        out.append(subject)
    return out


def load_role_config(environ: Optional[dict[str, str]] = None) -> RoleConfig:
    """Собрать конфигурацию ролей из окружения.

    Читается на каждый вызов — так же, как `portal_auth.get_settings()` и
    `distributed_workers.settings.get_settings()`. Правка `.env` + рестарт
    backend меняет права; кэша, который пришлось бы инвалидировать, нет.
    """
    env = environ if environ is not None else os.environ
    errors: list[str] = []
    subjects: dict[str, str] = {}
    configured = False
    for role in (ROLE_VIEWER, ROLE_OPERATOR, ROLE_ADMIN):
        env_name = _ENV_BY_ROLE[role]
        raw = env.get(env_name, "")
        if raw and raw.strip():
            configured = True
        for subject in _parse_list(raw, env_name=env_name, errors=errors):
            current = subjects.get(subject)
            if current is None or _ROLE_RANK[role] > _ROLE_RANK[current]:
                subjects[subject] = role
    return RoleConfig(subjects=subjects, errors=tuple(errors), configured=configured)


# ─── Actor ───────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Actor:
    """Кто действует. Собирается ТОЛЬКО сервером, только из сессии портала."""

    subject: Optional[str]
    display_name: str
    authentication_source: str
    role: Optional[str]
    permissions: frozenset[str]
    auth_enabled: bool
    diagnostics: Optional[str] = None

    @property
    def authenticated(self) -> bool:
        return bool(self.subject)

    def has(self, permission: str) -> bool:
        return permission in self.permissions

    def audit_id(self) -> str:
        """Идентификатор для журналов.

        Префикс `operator:` сохранён с этапа 0: `job_service.transition`
        извлекает из него РОЛЬ машины состояний (`actor.split(':', 1)[0]`), и
        менять формат значило бы менять таблицу разрешённых переходов.
        Роль подсистемы едет отдельным полем, а не внутри этой строки.
        """
        return f"operator:{self.subject}" if self.subject else "operator:anonymous"

    def as_view(self) -> dict[str, Any]:
        """Безопасное представление для UI. Ни списков субъектов, ни секретов."""
        return {
            "subject": self.subject,
            "display_name": self.display_name,
            "authentication_source": self.authentication_source,
            "role": self.role,
            "permissions": sorted(self.permissions),
            "auth_enabled": self.auth_enabled,
            "authenticated": self.authenticated,
            "can_view": self.has(PERM_VIEW),
            "can_operate": self.has(PERM_OPERATE),
            "can_admin": self.has(PERM_ADMIN),
            "diagnostics": self.diagnostics,
        }


ANONYMOUS = Actor(
    subject=None,
    display_name="",
    authentication_source=AUTH_SOURCE_NONE,
    role=None,
    permissions=frozenset(),
    auth_enabled=False,
)


def resolve_actor(request: Request) -> Actor:
    """Определить действующее лицо по фактической портальной аутентификации.

    Ни одного обращения к телу запроса, query или заголовкам клиента: субъект
    берётся из подписанной cookie, роль — из конфигурации сервера (R-02, R-03).

    `request.state.portal_user` (его ставит PortalAuthMiddleware) используется
    как подсказка, но НЕ как доказательство: проверка cookie повторяется здесь
    сама. Роутеры подсистемы собираются и без middleware (тестовое приложение,
    `tests/worker_center_app.py`), и полагаться на чужой слой было бы ровно тем
    «UI — граница безопасности», от которого этот модуль и защищает.
    """
    portal = portal_auth.get_settings()
    config = load_role_config()
    if not portal.enabled:
        return Actor(
            subject=None,
            display_name="",
            authentication_source=AUTH_SOURCE_NONE,
            role=None,
            permissions=frozenset(),
            auth_enabled=False,
            diagnostics=(
                "PORTAL_AUTH_ENABLED=false: субъекта нет, поэтому опасные "
                "операторские действия недоступны никому."
            ),
        )
    subject = portal_auth.request_username(request, portal)
    if not subject:
        return Actor(
            subject=None,
            display_name="",
            authentication_source=AUTH_SOURCE_NONE,
            role=None,
            permissions=frozenset(),
            auth_enabled=True,
            diagnostics="Сессия портала не найдена или истекла.",
        )
    role = config.role_for(subject)
    permissions = ROLE_PERMISSIONS.get(role or "", frozenset())
    diagnostics: Optional[str] = None
    if not permissions:
        # Объяснение обязано быть всегда, когда прав нет: «кнопки исчезли» без
        # причины — это заявка в поддержку, а не диагностика.
        diagnostics = config.diagnostics() or (
            f"Пользователь {subject!r} не перечислен ни в одном из списков ролей "
            f"подсистемы ({ENV_VIEWERS} / {ENV_OPERATORS} / {ENV_ADMINS})."
        )
    return Actor(
        subject=normalize_subject(subject),
        display_name=subject,
        authentication_source=AUTH_SOURCE_PORTAL,
        role=role,
        permissions=permissions,
        auth_enabled=True,
        diagnostics=diagnostics,
    )


def has_permission(actor: Actor, permission: str) -> bool:
    return actor.has(permission)


def permissions_for(actor: Actor) -> frozenset[str]:
    return actor.permissions


# ─── Отказ ───────────────────────────────────────────────────────────────────
class PermissionDenied(HTTPException):
    """Отказ с машиночитаемым кодом. Наследник HTTPException — обрабатывается FastAPI."""

    def __init__(self, status_code: int, code: str, message: str, permission: str):
        super().__init__(
            status_code=status_code,
            detail={"error": code, "message": message, "required_permission": permission},
        )
        self.code = code
        self.permission = permission


def _deny(
    request: Request,
    actor: Actor,
    *,
    status_code: int,
    code: str,
    message: str,
    permission: str,
) -> None:
    """Зафиксировать отказ в сквозном журнале и бросить исключение.

    Журнал — существующий `logs/actions/*.jsonl` (kind="worker"), а не новая
    таблица: отклонённый запрос НИЧЕГО не менял в БД, и заводить ради него
    строку в журнале операторских РЕШЕНИЙ значило бы смешать «сделано» с
    «не разрешено». Запись идёт fail-soft: журнал не вправе ронять отказ.
    """
    try:
        from backend.app.core import action_log

        action_log.log_event(
            "worker",
            event="permission_denied",
            actor=actor.subject or "anonymous",
            severity="security",
            path=str(request.url.path),
            method=request.method,
            required_permission=permission,
            reason=code,
            role=actor.role,
            auth_enabled=actor.auth_enabled,
        )
    except Exception:  # noqa: BLE001 — журнал не должен подменять отказ ошибкой
        pass
    raise PermissionDenied(status_code, code, message, permission)


def require_permission(request: Request, permission: str) -> Actor:
    """Проверить право. Возвращает actor либо бросает 401/403/404/503.

    Порядок ветвлений и есть fail-closed:
      1. подсистема выключена флагом  → 404 (маршрута как бы нет);
      2. портальная аутентификация выключена → изменяющие действия 503,
         просмотр допустим только в явно признанном небезопасном режиме;
      3. не аутентифицирован          → 401;
      4. конфигурация ролей битая     → 503 с диагностикой;
      5. роли нет / права нет         → 403.
    Ни одна ветка не заканчивается «раз непонятно — разрешим».
    """
    settings: DistributedWorkersSettings = get_settings()
    if not settings.enabled:
        raise HTTPException(
            status_code=404,
            detail="Подсистема распределённых воркеров отключена "
                   "(DISTRIBUTED_WORKERS_ENABLED=false).",
        )
    actor = resolve_actor(request)
    request.state.distributed_workers_actor = actor

    if not actor.auth_enabled:
        if permission == PERM_VIEW and settings.allow_insecure_admin:
            # Локальный пилот: экран доступен на чтение, потому что читать
            # нечего опасного. Изменяющие действия ниже закрыты безусловно.
            return actor
        _deny(
            request, actor,
            status_code=503,
            code="portal_auth_disabled",
            message=(
                "Операторские действия подсистемы недоступны: PORTAL_AUTH_ENABLED=false. "
                "Субъекта нет, значит проверить право не у кого — включите портальную "
                "аутентификацию и задайте роли подсистемы."
            ),
            permission=permission,
        )
    if not actor.authenticated:
        _deny(
            request, actor,
            status_code=401,
            code="not_authenticated",
            message="Требуется вход в портал.",
            permission=permission,
        )
    config = load_role_config()
    if not config.ok:
        _deny(
            request, actor,
            status_code=503,
            code="role_config_invalid",
            message=config.diagnostics() or "Конфигурация ролей не принята.",
            permission=permission,
        )
    if not actor.has(permission):
        _deny(
            request, actor,
            status_code=403,
            code="permission_denied",
            message=(
                f"Недостаточно прав: требуется {permission}. "
                + (actor.diagnostics or f"Текущая роль: {actor.role or 'нет'}.")
            ),
            permission=permission,
        )
    return actor


# ─── FastAPI-зависимости ─────────────────────────────────────────────────────
async def require_view(request: Request) -> Actor:
    return require_permission(request, PERM_VIEW)


async def require_operator(request: Request) -> Actor:
    return require_permission(request, PERM_OPERATE)


async def require_admin(request: Request) -> Actor:
    return require_permission(request, PERM_ADMIN)


async def current_actor(request: Request) -> Actor:
    """Actor без требования прав.

    Нужен ровно одному эндпоинту — `/api/workers/me`: экран обязан уметь честно
    сказать «прав нет», а для этого ответ должен приходить и без прав.
    """
    actor = resolve_actor(request)
    request.state.distributed_workers_actor = actor
    return actor


def actor_of(request: Request) -> Actor:
    """Actor, уже разрешённый зависимостью. Ре-резолв — страховка, не путь."""
    existing = getattr(request.state, "distributed_workers_actor", None)
    if isinstance(existing, Actor):
        return existing
    return resolve_actor(request)
