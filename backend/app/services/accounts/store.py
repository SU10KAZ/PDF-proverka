"""Аккаунты портала: статусы, роли, права по разделам, версия сессии.

Каждая изменяющая функция — одна транзакция ``BEGIN IMMEDIATE``, в той же
транзакции проверяется, что остался хотя бы один активный администратор, и
пишется событие ``auth_events`` (без паролей и хешей). Функции синхронные —
из async-кода вызывать через ``asyncio.to_thread``.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

from backend.app.core.access import (
    MODE_ACCOUNTS,
    ROLE_ADMIN,
    ROLE_VIEWER,
    SECTIONS,
    SECTIONS_WITHOUT_DOWNLOADS,
    SECTIONS_WITH_UPLOADS,
    AccountContext,
)
from backend.app.services.accounts import db, passwords

STATUSES = ("pending", "active", "rejected", "blocked")
ROLES = (ROLE_ADMIN, ROLE_VIEWER)

_LOGIN_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,31}$")

# Ограничение попыток (план § 3.6).
LOGIN_FAILS, LOGIN_WINDOW, LOGIN_LOCK = 5, 15 * 60, 15 * 60
IP_FAILS, IP_WINDOW, IP_LOCK = 30, 15 * 60, 15 * 60
REG_PER_IP, REG_WINDOW = 3, 60 * 60
MAX_PENDING = 20


class AccountError(Exception):
    """Ошибка операции с аккаунтом: машинный код + текст для человека."""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def normalize_login(raw: str) -> str:
    norm = unicodedata.normalize("NFC", (raw or "").strip()).casefold()
    if not _LOGIN_RE.match(norm):
        raise AccountError(
            "invalid_login",
            "Логин: 3–32 символа, латинские буквы, цифры, «.», «_», «-»; начинается с буквы или цифры.",
        )
    return norm


# ─── чтение ──────────────────────────────────────────────────────────────────

_PUBLIC_FIELDS = (
    "id", "login", "display_name", "contact", "note", "role", "status", "staff_id",
    "must_change_password", "status_reason", "created_at", "updated_at", "decided_at",
    "decided_by", "last_login_at", "registration_ip", "legacy_login", "session_version",
)


def _sections(conn: sqlite3.Connection, account_id: int) -> dict[str, tuple[bool, bool]]:
    rows = conn.execute(
        "SELECT section, can_view, can_download FROM account_sections WHERE account_id=?",
        (account_id,),
    ).fetchall()
    return {r["section"]: (bool(r["can_view"]), bool(r["can_download"])) for r in rows}


def _upload_projects(conn: sqlite3.Connection, account_id: int) -> bool:
    row = conn.execute("SELECT upload_projects FROM account_capabilities WHERE account_id=?",
                       (account_id,)).fetchone()
    return bool(row and row["upload_projects"])


def _edit_schedule(conn: sqlite3.Connection, account_id: int) -> bool:
    row = conn.execute("SELECT edit_schedule FROM account_schedule_permissions WHERE account_id=?",
                       (account_id,)).fetchone()
    return bool(row and row["edit_schedule"])


def _upload_sections(conn: sqlite3.Connection, account_id: int) -> frozenset[str]:
    return frozenset(r["section"] for r in conn.execute(
        "SELECT section FROM account_section_uploads WHERE account_id=?", (account_id,)))


def _uploads(conn: sqlite3.Connection, account_id: int) -> dict[str, bool]:
    granted = _upload_sections(conn, account_id)
    return {s: (_upload_projects(conn, account_id) if s == "audit_results" else s in granted)
            for s in SECTIONS if s in SECTIONS_WITH_UPLOADS}


def _write_upload(conn, target: int, section: str, allowed: bool) -> None:
    if section == "audit_results":
        conn.execute(
            "INSERT INTO account_capabilities(account_id, upload_projects) VALUES (?,?)"
            " ON CONFLICT(account_id) DO UPDATE SET upload_projects=excluded.upload_projects",
            (int(target), int(allowed)),
        )
    elif allowed:
        conn.execute("INSERT OR IGNORE INTO account_section_uploads(account_id, section) VALUES (?,?)",
                     (int(target), section))
    else:
        conn.execute("DELETE FROM account_section_uploads WHERE account_id=? AND section=?",
                     (int(target), section))


def _write_section_uploads(conn, target: int, raw: Mapping[str, Any]) -> None:
    for section in SECTIONS_WITH_UPLOADS:
        value = raw.get(section, {})
        if not value.get("view"):
            _write_upload(conn, target, section, False)
        elif "upload" in value:
            _write_upload(conn, target, section, bool(value["upload"]))


def _public(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    out = {k: row[k] for k in _PUBLIC_FIELDS}
    out["must_change_password"] = bool(out["must_change_password"])
    out["uploads"] = _uploads(conn, row["id"])
    out["upload_projects"] = any(out["uploads"].values())
    out["edit_schedule"] = _edit_schedule(conn, row["id"])
    secs = _sections(conn, row["id"])
    out["sections"] = {
        s: {"view": secs.get(s, (False, False))[0], "download": secs.get(s, (False, False))[1]}
        for s in SECTIONS
    }
    return out


def _context_from_row(conn: sqlite3.Connection, row: sqlite3.Row) -> AccountContext:
    return AccountContext(
        mode=MODE_ACCOUNTS,
        login=row["login_norm"],
        role=row["role"],
        account_id=row["id"],
        status=row["status"],
        staff_id=row["staff_id"],
        session_version=row["session_version"],
        must_change_password=bool(row["must_change_password"]),
        sections=_sections(conn, row["id"]),
        upload_projects=_upload_projects(conn, row["id"]),
        edit_schedule=_edit_schedule(conn, row["id"]),
        upload_sections=_upload_sections(conn, row["id"]),
    )


def get_context(account_id: int) -> Optional[AccountContext]:
    """Актуальный контекст аккаунта (на каждый запрос, без кэша)."""
    with db.read_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (int(account_id),)).fetchone()
        return _context_from_row(conn, row) if row else None


def get_account(account_id: int) -> Optional[dict]:
    with db.read_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (int(account_id),)).fetchone()
        return _public(conn, row) if row else None


def list_accounts(status: Optional[str] = None) -> list[dict]:
    with db.read_conn() as conn:
        if status:
            rows = conn.execute("SELECT * FROM accounts WHERE status=? ORDER BY id", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        return [_public(conn, r) for r in rows]


def list_events(limit: int = 200, target_account_id: Optional[int] = None) -> list[dict]:
    limit = max(1, min(int(limit), 2000))
    with db.read_conn() as conn:
        if target_account_id is not None:
            rows = conn.execute(
                "SELECT * FROM auth_events WHERE target_account_id=? ORDER BY id DESC LIMIT ?",
                (int(target_account_id), limit),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM auth_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [{**dict(r), "details": json.loads(r["details_json"] or "{}")} for r in rows]


def active_admin_count() -> int:
    with db.read_conn() as conn:
        return _admin_count(conn)


# ─── служебное ────────────────────────────────────────────────────────────────

def _admin_count(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT count(*) FROM accounts WHERE role='admin' AND status='active'"
    ).fetchone()[0]


def _event(conn, event: str, *, actor: Optional[int], target: Optional[int], **details: Any) -> None:
    conn.execute(
        "INSERT INTO auth_events(at, actor_account_id, target_account_id, event, details_json)"
        " VALUES (?,?,?,?,?)",
        (db.utc_now(), actor, target, event, json.dumps(details, ensure_ascii=False)),
    )


def _row(conn, account_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM accounts WHERE id=?", (int(account_id),)).fetchone()
    if row is None:
        raise AccountError("not_found", "Аккаунт не найден.", 404)
    return row


def _guard_last_admin(conn) -> None:
    if _admin_count(conn) < 1:
        raise AccountError("last_admin", "Нельзя оставить портал без активного администратора.", 409)


def _not_self(actor: Optional[int], target: int, what: str) -> None:
    if actor is not None and int(actor) == int(target):
        raise AccountError("self_action", f"Нельзя {what} собственный аккаунт.", 409)


def _touch(conn, account_id: int, **fields: Any) -> None:
    fields["updated_at"] = db.utc_now()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE accounts SET {cols} WHERE id=?", (*fields.values(), int(account_id)))


def _bump_sv(conn, account_id: int) -> None:
    conn.execute(
        "UPDATE accounts SET session_version=session_version+1, updated_at=? WHERE id=?",
        (db.utc_now(), int(account_id)),
    )


# ─── ограничение попыток ─────────────────────────────────────────────────────

def _throttle_locked(conn, key: str, now: float) -> float:
    row = conn.execute("SELECT locked_until FROM auth_throttle WHERE key=?", (key,)).fetchone()
    return float(row["locked_until"]) if row and float(row["locked_until"]) > now else 0.0


def _throttle_fail(conn, key: str, now: float, limit: int, window: int, lock: int) -> None:
    row = conn.execute("SELECT * FROM auth_throttle WHERE key=?", (key,)).fetchone()
    if row is None or now - float(row["window_start"]) > window:
        failures, start = 1, now
    else:
        failures, start = int(row["failures"]) + 1, float(row["window_start"])
    locked = now + lock if failures >= limit else 0.0
    conn.execute(
        "INSERT INTO auth_throttle(key, failures, window_start, locked_until) VALUES (?,?,?,?)"
        " ON CONFLICT(key) DO UPDATE SET failures=excluded.failures,"
        " window_start=excluded.window_start, locked_until=excluded.locked_until",
        (key, failures, start, locked),
    )


def _throttle_clear(conn, key: str) -> None:
    conn.execute("DELETE FROM auth_throttle WHERE key=?", (key,))


# ─── вход ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class LoginResult:
    ok: bool
    code: str = ""                    # invalid_credentials | locked | account_pending | …
    retry_after: int = 0
    context: Optional[AccountContext] = None


def authenticate(login: str, password: str, *, ip: str = "") -> LoginResult:
    """Проверить логин и пароль. Статус сообщается только после верного пароля."""
    now = time.time()
    try:
        norm = normalize_login(login)
    except AccountError:
        norm = ""
    db.ensure_ready()
    with db.read_conn() as conn:
        lock_until = max(
            _throttle_locked(conn, f"login:{norm}", now) if norm else 0.0,
            _throttle_locked(conn, f"ip:{ip}", now) if ip else 0.0,
        )
        row = conn.execute("SELECT * FROM accounts WHERE login_norm=?", (norm,)).fetchone() if norm else None
    if lock_until:
        passwords.verify_and_update(password, None)  # то же время ответа
        return LoginResult(False, "locked", retry_after=int(lock_until - now) + 1)
    ok, new_hash = passwords.verify_and_update(password, row["password_hash"] if row else None)
    with db.write_txn() as conn:
        if not ok:
            if norm:
                _throttle_fail(conn, f"login:{norm}", now, LOGIN_FAILS, LOGIN_WINDOW, LOGIN_LOCK)
            if ip:
                _throttle_fail(conn, f"ip:{ip}", now, IP_FAILS, IP_WINDOW, IP_LOCK)
            if row is not None:
                _event(conn, "login_failed", actor=None, target=row["id"], ip=ip)
            return LoginResult(False, "invalid_credentials")
        _throttle_clear(conn, f"login:{norm}")
        if new_hash:
            _touch(conn, row["id"], password_hash=new_hash)
        status = row["status"]
        if status != "active":
            _event(conn, "login_refused", actor=row["id"], target=row["id"], status=status, ip=ip)
            return LoginResult(False, f"account_{status}")
        _touch(conn, row["id"], last_login_at=db.utc_now())
        _event(conn, "login", actor=row["id"], target=row["id"], ip=ip)
        fresh = conn.execute("SELECT * FROM accounts WHERE id=?", (row["id"],)).fetchone()
        return LoginResult(True, context=_context_from_row(conn, fresh))


# ─── регистрация ─────────────────────────────────────────────────────────────

def create_pending(*, login: str, display_name: str, password: str, contact: str = "",
                   note: str = "", ip: str = "") -> dict:
    """Самостоятельная регистрация: всегда viewer + pending + без разделов."""
    norm = normalize_login(login)
    display_name = " ".join((display_name or "").split())[:200]
    if len(display_name) < 3:
        raise AccountError("invalid_name", "Укажите фамилию, имя и отчество.")
    err = passwords.check_policy(password, norm)
    if err:
        raise AccountError("weak_password", err)
    pw_hash = passwords.hash_password(password)
    now = time.time()
    with db.write_txn() as conn:
        if ip:
            row = conn.execute("SELECT * FROM auth_throttle WHERE key=?", (f"reg:{ip}",)).fetchone()
            if row and now - float(row["window_start"]) <= REG_WINDOW and int(row["failures"]) >= REG_PER_IP:
                raise AccountError("too_many_requests", "Слишком много заявок. Попробуйте позже.", 429)
        pending = conn.execute("SELECT count(*) FROM accounts WHERE status='pending'").fetchone()[0]
        if pending >= MAX_PENDING:
            raise AccountError("too_many_requests", "Слишком много необработанных заявок. Попробуйте позже.", 429)
        if conn.execute("SELECT 1 FROM accounts WHERE login_norm=?", (norm,)).fetchone():
            raise AccountError("login_taken", "Такой логин уже занят.", 409)
        stamp = db.utc_now()
        cur = conn.execute(
            "INSERT INTO accounts(login, login_norm, display_name, contact, note, password_hash,"
            " role, status, created_at, updated_at, registration_ip)"
            " VALUES (?,?,?,?,?,?,'viewer','pending',?,?,?)",
            (login.strip(), norm, display_name, (contact or "").strip()[:200],
             (note or "").strip()[:1000], pw_hash, stamp, stamp, ip or None),
        )
        if ip:
            # Счётчик заявок с IP — в той же таблице ограничений.
            _throttle_fail(conn, f"reg:{ip}", now, REG_PER_IP + 1, REG_WINDOW, 0)
        _event(conn, "registered", actor=cur.lastrowid, target=cur.lastrowid, ip=ip)
        return _public(conn, _row(conn, cur.lastrowid))


# ─── действия администратора ─────────────────────────────────────────────────

def approve(actor: Optional[int], target: int) -> dict:
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["status"] not in ("pending", "rejected"):
            raise AccountError("bad_status", "Одобрить можно только заявку.", 409)
        _touch(conn, target, status="active", decided_at=db.utc_now(), decided_by=actor,
               status_reason="")
        _event(conn, "approved", actor=actor, target=target)
        return _public(conn, _row(conn, target))


def reject(actor: Optional[int], target: int, reason: str = "") -> dict:
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["status"] != "pending":
            raise AccountError("bad_status", "Отклонить можно только ожидающую заявку.", 409)
        _touch(conn, target, status="rejected", decided_at=db.utc_now(), decided_by=actor,
               status_reason=(reason or "")[:500])
        _bump_sv(conn, target)
        _event(conn, "rejected", actor=actor, target=target, reason=(reason or "")[:500])
        return _public(conn, _row(conn, target))


def block(actor: Optional[int], target: int, reason: str = "") -> dict:
    _not_self(actor, target, "заблокировать")
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["status"] != "active":
            raise AccountError("bad_status", "Заблокировать можно только действующий аккаунт.", 409)
        _touch(conn, target, status="blocked", status_reason=(reason or "")[:500])
        _bump_sv(conn, target)
        _guard_last_admin(conn)
        _event(conn, "blocked", actor=actor, target=target, reason=(reason or "")[:500])
        return _public(conn, _row(conn, target))


def unblock(actor: Optional[int], target: int) -> dict:
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["status"] != "blocked":
            raise AccountError("bad_status", "Аккаунт не заблокирован.", 409)
        _touch(conn, target, status="active", status_reason="")
        _bump_sv(conn, target)
        _event(conn, "unblocked", actor=actor, target=target)
        return _public(conn, _row(conn, target))


def set_role(actor: Optional[int], target: int, role: str) -> dict:
    if role not in ROLES:
        raise AccountError("invalid_role", "Роль: admin или viewer.")
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["role"] == role:
            return _public(conn, row)
        if role != ROLE_ADMIN:
            _not_self(actor, target, "понизить")
        _touch(conn, target, role=role)
        _bump_sv(conn, target)
        _guard_last_admin(conn)
        _event(conn, "role_changed", actor=actor, target=target, role=role, previous=row["role"])
        return _public(conn, _row(conn, target))


def normalize_sections(raw: Mapping[str, Any]) -> dict[str, tuple[bool, bool]]:
    """{section: {view, download}} → проверенный набор. Скачивание без просмотра
    и скачивание в разделах без выгрузок отклоняются (а не «молча чинятся»)."""
    out: dict[str, tuple[bool, bool]] = {}
    for section, value in (raw or {}).items():
        if section not in SECTIONS:
            raise AccountError("invalid_section", f"Неизвестный раздел: {section}")
        if isinstance(value, Mapping):
            view, download = bool(value.get("view")), bool(value.get("download"))
        else:
            raise AccountError("invalid_section", f"Раздел {section}: ожидается {{view, download}}")
        if download and not view:
            raise AccountError("download_requires_view",
                               f"Раздел {section}: скачивание возможно только вместе с просмотром.")
        if download and section in SECTIONS_WITHOUT_DOWNLOADS:
            raise AccountError("no_downloads_in_section",
                               f"В разделе {section} нет выгрузок — право скачивания не назначается.")
        if value.get("upload"):
            if section not in SECTIONS_WITH_UPLOADS:
                raise AccountError("no_uploads_in_section", f"В разделе {section} нет загрузки проектов.")
            if not view:
                raise AccountError("upload_requires_view", "Для загрузки нужен просмотр этого раздела.")
        out[section] = (view, download)
    return out


def _write_sections(conn, target: int, sections: Mapping[str, tuple[bool, bool]]) -> None:
    conn.execute("DELETE FROM account_sections WHERE account_id=?", (int(target),))
    for section, (view, download) in sections.items():
        if view or download:
            conn.execute(
                "INSERT INTO account_sections(account_id, section, can_view, can_download)"
                " VALUES (?,?,?,?)", (int(target), section, int(view), int(download)),
            )


def set_sections(actor: Optional[int], target: int, raw: Mapping[str, Any]) -> dict:
    raw = raw or {}
    sections = normalize_sections(raw)
    with db.write_txn() as conn:
        row = _row(conn, target)
        existing_uploads = _uploads(conn, target)
        if row["status"] != "active" and any(v.get("upload") and not existing_uploads.get(s)
                                              for s, v in raw.items()):
            raise AccountError("upload_requires_active", "Загрузку можно разрешить только действующему пользователю.")
        _write_sections(conn, target, sections)
        _write_section_uploads(conn, target, raw)
        if not sections.get("schedule", (False, False))[0]:
            conn.execute("DELETE FROM account_schedule_permissions WHERE account_id=?", (int(target),))
        _touch(conn, target)
        _event(conn, "sections_changed", actor=actor, target=target,
               sections={s: list(v) for s, v in sections.items()}, uploads=_uploads(conn, target))
        return _public(conn, _row(conn, target))


def set_upload_projects(actor: Optional[int], target: int, allowed: bool) -> dict:
    with db.write_txn() as conn:
        row = _row(conn, target)
        sections = _sections(conn, target)
        eligible = {s for s in SECTIONS_WITH_UPLOADS if sections.get(s, (False, False))[0]}
        if allowed and (row["status"] != "active" or not eligible):
            raise AccountError("upload_requires_section",
                               "Сначала включите просмотр раздела с загрузкой проектов для действующего пользователя.")
        for section in SECTIONS_WITH_UPLOADS:
            _write_upload(conn, target, section, bool(allowed and section in eligible))
        _touch(conn, target)
        _event(conn, "upload_projects_changed", actor=actor, target=target,
               allowed=bool(allowed), uploads=_uploads(conn, target))
        return _public(conn, _row(conn, target))


def set_edit_schedule(actor: Optional[int], target: int, allowed: bool) -> dict:
    with db.write_txn() as conn:
        row = _row(conn, target)
        if allowed and (row["status"] != "active" or not _sections(conn, target).get("schedule", (False, False))[0]):
            raise AccountError("edit_requires_schedule",
                               "Сначала включите просмотр графика работ для действующего пользователя.")
        conn.execute(
            "INSERT INTO account_schedule_permissions(account_id, edit_schedule) VALUES (?,?)"
            " ON CONFLICT(account_id) DO UPDATE SET edit_schedule=excluded.edit_schedule",
            (int(target), int(allowed)),
        )
        _touch(conn, target)
        _event(conn, "edit_schedule_changed", actor=actor, target=target, allowed=bool(allowed))
        return _public(conn, _row(conn, target))


def set_staff(actor: Optional[int], target: int, staff_id: Optional[str]) -> dict:
    staff_id = (staff_id or "").strip() or None
    if staff_id is not None:
        from backend.app.services.common import user_service

        if user_service.get_user_by_id(staff_id) is None:
            raise AccountError("unknown_staff", "Сотрудник не найден в users.json.", 404)
    with db.write_txn() as conn:
        row = _row(conn, target)
        if staff_id and conn.execute(
            "SELECT 1 FROM accounts WHERE staff_id=? AND id<>?", (staff_id, int(target))
        ).fetchone():
            raise AccountError("staff_taken", "Этот сотрудник уже связан с другим аккаунтом.", 409)
        _touch(conn, target, staff_id=staff_id)
        _event(conn, "staff_linked", actor=actor, target=target, staff_id=staff_id,
               previous=row["staff_id"])
        return _public(conn, _row(conn, target))


def revoke_sessions(actor: Optional[int], target: int) -> dict:
    with db.write_txn() as conn:
        _row(conn, target)
        _bump_sv(conn, target)
        _event(conn, "sessions_revoked", actor=actor, target=target)
        return _public(conn, _row(conn, target))


def reset_password(actor: Optional[int], target: int) -> tuple[dict, str]:
    """Временный пароль (вернуть администратору один раз; в журнал не пишется)."""
    temp = passwords.generate_temporary_password()
    pw_hash = passwords.hash_password(temp)
    with db.write_txn() as conn:
        _row(conn, target)
        _touch(conn, target, password_hash=pw_hash, must_change_password=1)
        _bump_sv(conn, target)
        _event(conn, "password_reset", actor=actor, target=target)
        return _public(conn, _row(conn, target)), temp


def change_password(account_id: int, current: str, new: str) -> AccountContext:
    with db.read_conn() as conn:
        row = _row(conn, account_id)
    ok, _ = passwords.verify_and_update(current, row["password_hash"])
    if not ok:
        raise AccountError("invalid_credentials", "Текущий пароль указан неверно.", 403)
    err = passwords.check_policy(new, row["login_norm"])
    if err:
        raise AccountError("weak_password", err)
    if passwords.verify_and_update(new, row["password_hash"])[0]:
        raise AccountError("same_password", "Новый пароль должен отличаться от текущего.")
    pw_hash = passwords.hash_password(new)
    with db.write_txn() as conn:
        fresh = _row(conn, account_id)
        if fresh["status"] != "active":
            raise AccountError(f"account_{fresh['status']}", "Аккаунт не активен.", 403)
        _touch(conn, account_id, password_hash=pw_hash, must_change_password=0)
        _bump_sv(conn, account_id)
        _event(conn, "password_changed", actor=account_id, target=account_id)
        return _context_from_row(conn, _row(conn, account_id))


def delete(actor: Optional[int], target: int) -> None:
    _not_self(actor, target, "удалить")
    with db.write_txn() as conn:
        row = _row(conn, target)
        if row["status"] not in ("pending", "rejected"):
            raise AccountError("bad_status", "Удалить можно только заявку (ожидающую или отклонённую).", 409)
        conn.execute("DELETE FROM accounts WHERE id=?", (int(target),))
        _event(conn, "deleted", actor=actor, target=target, login=row["login_norm"])


# ─── создание администратором / CLI / миграция ───────────────────────────────

def create_account(*, login: str, display_name: str, role: str, status: str = "active",
                   password: Optional[str] = None, password_hash: Optional[str] = None,
                   staff_id: Optional[str] = None, sections: Optional[Mapping[str, Any]] = None,
                   legacy_login: Optional[str] = None, must_change_password: bool = False,
                   actor: Optional[int] = None, event: str = "created") -> dict:
    norm = normalize_login(login)
    if role not in ROLES or status not in STATUSES:
        raise AccountError("invalid_role", "Неверная роль или статус.")
    if password_hash is None:
        if password is None:
            raise AccountError("weak_password", "Нужен пароль.")
        err = passwords.check_policy(password, norm)
        if err:
            raise AccountError("weak_password", err)
        password_hash = passwords.hash_password(password)
    elif not passwords.is_supported_hash(password_hash):
        raise AccountError("unsupported_hash", f"{norm}: хеш не pbkdf2-sha256.")
    secs = normalize_sections(sections or {})
    with db.write_txn() as conn:
        if conn.execute("SELECT 1 FROM accounts WHERE login_norm=?", (norm,)).fetchone():
            raise AccountError("login_taken", f"Логин {norm} уже занят.", 409)
        if staff_id and conn.execute("SELECT 1 FROM accounts WHERE staff_id=?", (staff_id,)).fetchone():
            raise AccountError("staff_taken", f"Сотрудник {staff_id} уже связан.", 409)
        stamp = db.utc_now()
        cur = conn.execute(
            "INSERT INTO accounts(login, login_norm, display_name, password_hash, role, status,"
            " staff_id, must_change_password, created_at, updated_at, decided_at, decided_by,"
            " legacy_login) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (login.strip(), norm, display_name.strip() or norm, password_hash, role, status,
             staff_id, int(must_change_password), stamp, stamp,
             stamp if status != "pending" else None, actor, legacy_login),
        )
        _write_sections(conn, cur.lastrowid, secs)
        _write_section_uploads(conn, cur.lastrowid, sections or {})
        _event(conn, event, actor=actor, target=cur.lastrowid, role=role, status=status,
               staff_id=staff_id, sections={s: list(v) for s, v in secs.items()},
               uploads=_uploads(conn, cur.lastrowid),
               legacy_login=legacy_login)
        return _public(conn, _row(conn, cur.lastrowid))


def import_legacy(entries: Iterable[tuple[str, str]], mapping: Mapping[str, Mapping[str, Any]],
                  *, dry_run: bool = True) -> list[dict]:
    """Перенос PORTAL_AUTH_USERS → аккаунты по явной таблице ``mapping``.

    ``entries`` — пары (legacy_login, hash). ``mapping[legacy_login]`` —
    ``{"role", "staff_id", "display_name", "sections"}``. Логины без записи в
    mapping не переносятся. Любой конфликт (регистр, занятый логин или
    сотрудник, отсутствие admin, неизвестный хеш) отменяет ВЕСЬ импорт.
    Возвращает строки отчёта без хешей.
    """
    from backend.app.services.common import user_service

    entries = list(entries)
    report: list[dict] = []
    problems: list[str] = []
    seen_norm: dict[str, str] = {}
    seen_staff: dict[str, str] = {}
    for legacy, pw_hash in entries:
        spec = mapping.get(legacy)
        if spec is None:
            report.append({"legacy_login": legacy, "action": "skip (нет в mapping)"})
            continue
        try:
            norm = normalize_login(legacy)
        except AccountError as exc:
            problems.append(f"{legacy}: {exc.message}")
            continue
        if norm in seen_norm:
            problems.append(f"{legacy}: совпадает с {seen_norm[norm]} без учёта регистра")
        seen_norm[norm] = legacy
        role = spec.get("role")
        if role not in ROLES:
            problems.append(f"{legacy}: роль должна быть явно admin или viewer")
        staff_id = (spec.get("staff_id") or None)
        staff = user_service.get_user_by_id(staff_id) if staff_id else None
        if staff_id and staff is None:
            problems.append(f"{legacy}: сотрудник {staff_id} не найден в users.json")
        if staff_id and staff_id in seen_staff:
            problems.append(f"{legacy}: сотрудник {staff_id} уже у {seen_staff[staff_id]}")
        if staff_id:
            seen_staff[staff_id] = legacy
        if not passwords.is_supported_hash(pw_hash):
            problems.append(f"{legacy}: хеш не распознан")
        try:
            secs = normalize_sections(spec.get("sections") or {})
        except AccountError as exc:
            problems.append(f"{legacy}: {exc.message}")
            secs = {}
        report.append({
            "legacy_login": legacy, "login_norm": norm, "role": role, "staff_id": staff_id,
            "display_name": spec.get("display_name") or (staff or {}).get("name") or norm,
            "sections": {s: {"view": v[0], "download": v[1]} for s, v in secs.items()},
            "action": "create",
        })
    unknown = sorted(set(mapping) - {e[0] for e in entries})
    for legacy in unknown:
        problems.append(f"{legacy}: есть в mapping, но нет в PORTAL_AUTH_USERS")
    if not any(r.get("role") == ROLE_ADMIN for r in report if r.get("action") == "create"):
        if not active_admin_count_safe():
            problems.append("в mapping нет ни одного admin")
    db.ensure_ready()
    with db.read_conn() as conn:
        for r in report:
            if r.get("action") != "create":
                continue
            if conn.execute("SELECT 1 FROM accounts WHERE login_norm=?", (r["login_norm"],)).fetchone():
                problems.append(f"{r['legacy_login']}: логин уже есть в базе аккаунтов")
            if r["staff_id"] and conn.execute(
                    "SELECT 1 FROM accounts WHERE staff_id=?", (r["staff_id"],)).fetchone():
                problems.append(f"{r['legacy_login']}: сотрудник уже связан в базе аккаунтов")
    if problems:
        raise AccountError("import_conflict", "Импорт отменён:\n- " + "\n- ".join(problems), 409)
    if dry_run:
        return report
    hashes = dict(entries)
    for r in report:
        if r.get("action") != "create":
            continue
        create_account(
            login=r["login_norm"], display_name=r["display_name"], role=r["role"],
            status="active", password_hash=hashes[r["legacy_login"]], staff_id=r["staff_id"],
            sections=r["sections"], legacy_login=r["legacy_login"], event="legacy_import",
        )
    return report


def active_admin_count_safe() -> int:
    try:
        return active_admin_count() if db.exists() else 0
    except FileNotFoundError:
        return 0


__all__ = [
    "AccountError", "LoginResult", "approve", "authenticate", "block", "change_password",
    "create_account", "create_pending", "delete", "get_account", "get_context",
    "import_legacy", "list_accounts", "list_events", "normalize_login", "normalize_sections",
    "reject", "reset_password", "revoke_sessions", "set_role", "set_sections", "set_staff",
    "unblock", "active_admin_count",
]
