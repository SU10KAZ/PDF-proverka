"""Хеши паролей портала.

Совместимость: существующие хеши ``PORTAL_AUTH_USERS`` — passlib
``pbkdf2_sha256`` с 29 000 раундов — проверяются как есть и перехешируются
при первом успешном входе (``needs_update``). Новые хеши — 600 000 раундов
(``PORTAL_PASSWORD_ROUNDS`` переопределяет, например для тестов).

Проверка дорогая (сотни миллисекунд) — вызывать только из потока, не из
event loop.
"""
from __future__ import annotations

import os
import secrets
from functools import lru_cache

from passlib.context import CryptContext

DEFAULT_ROUNDS = 600_000
MIN_PASSWORD_LENGTH = 10
MAX_PASSWORD_LENGTH = 256


def _rounds() -> int:
    try:
        value = int(os.environ.get("PORTAL_PASSWORD_ROUNDS") or DEFAULT_ROUNDS)
    except ValueError:
        value = DEFAULT_ROUNDS
    return max(1000, value)


@lru_cache(maxsize=4)
def _context(rounds: int) -> CryptContext:
    return CryptContext(schemes=["pbkdf2_sha256"], pbkdf2_sha256__rounds=rounds,
                        deprecated="auto")


def context() -> CryptContext:
    return _context(_rounds())


@lru_cache(maxsize=1)
def _dummy_hash() -> str:
    return _context(_rounds()).hash("portal-dummy-" + secrets.token_hex(8))


def hash_password(password: str) -> str:
    return context().hash(password)


def verify_and_update(password: str, stored_hash: str | None) -> tuple[bool, str | None]:
    """(совпал ли пароль, новый хеш или None). Неизвестный логин — ``stored_hash=None``:
    проверка против dummy-хеша, чтобы время ответа не выдавало наличие логина."""
    ctx = context()
    if not stored_hash:
        try:
            ctx.verify(password or "", _dummy_hash())
        except (ValueError, TypeError):
            pass
        return False, None
    try:
        ok, new_hash = ctx.verify_and_update(password or "", stored_hash)
    except (ValueError, TypeError):
        return False, None
    if not ok:
        return False, None
    # Хеш с меньшим числом раундов, чем текущая настройка, — обновить.
    if new_hash is None:
        try:
            if ctx.needs_update(stored_hash) or _rounds_of(stored_hash) < _rounds():
                new_hash = ctx.hash(password)
        except (ValueError, TypeError):
            new_hash = None
    return True, new_hash


def _rounds_of(stored_hash: str) -> int:
    try:
        return int(stored_hash.split("$")[2])
    except (IndexError, ValueError):
        return 0


def is_supported_hash(stored_hash: str) -> bool:
    return bool(stored_hash) and stored_hash.startswith("$pbkdf2-sha256$") and _rounds_of(stored_hash) > 0


def check_policy(password: str, login: str) -> str | None:
    """Текст ошибки или None."""
    if not isinstance(password, str) or len(password) < MIN_PASSWORD_LENGTH:
        return f"Пароль должен быть не короче {MIN_PASSWORD_LENGTH} символов."
    if len(password) > MAX_PASSWORD_LENGTH:
        return "Пароль слишком длинный."
    if password.strip().casefold() == (login or "").strip().casefold():
        return "Пароль не должен совпадать с логином."
    return None


def generate_temporary_password() -> str:
    """Временный пароль (показывается администратору один раз)."""
    alphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(16))


__all__ = ["check_policy", "context", "generate_temporary_password", "hash_password",
           "is_supported_hash", "verify_and_update"]
