#!/usr/bin/env python3
"""Сгенерировать pbkdf2_sha256 хеш пароля для PORTAL_AUTH_USERS.

Использование:

    # Безопасно (пароль вводится в скрытом prompt'е, не попадает в историю shell):
    python backend/scripts/hash_portal_password.py

    # С логином — печатает готовую строку `login:hash` для PORTAL_AUTH_USERS:
    python backend/scripts/hash_portal_password.py --user ivan

    # Пароль аргументом (НЕ рекомендуется — попадает в историю shell):
    python backend/scripts/hash_portal_password.py "my_password"
    python backend/scripts/hash_portal_password.py --user ivan "my_password"

Несколько сотрудников: сгенерируйте по хешу на каждого и соедините через
запятую в .env (значение обязательно в ОДИНАРНЫХ кавычках — хеш содержит $):

    PORTAL_AUTH_USERS='ivan:$pbkdf2-sha256$...,petr:$pbkdf2-sha256$...'
"""
from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.core.portal_auth import hash_password  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Сгенерировать pbkdf2_sha256 хеш пароля для PORTAL_AUTH_USERS.",
    )
    parser.add_argument("password", nargs="?", help="пароль (если не задан — спросит скрытно)")
    parser.add_argument("--user", "-u", help="логин — тогда печатается строка login:hash")
    args = parser.parse_args()

    password = args.password
    if not password:
        password = getpass.getpass("Пароль: ")
        confirm = getpass.getpass("Повторите пароль: ")
        if password != confirm:
            print("Пароли не совпадают.", file=sys.stderr)
            return 1
    if not password:
        print("Пустой пароль не допускается.", file=sys.stderr)
        return 1

    pw_hash = hash_password(password)
    if args.user:
        print(f"{args.user}:{pw_hash}")
    else:
        print(pw_hash)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
