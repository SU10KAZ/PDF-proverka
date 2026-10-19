#!/usr/bin/env python3
"""CLI аккаунтов портала (план доступа, этапы 1 и 9).

База — ``PORTAL_ACCOUNTS_DB`` или ``--db`` (по умолчанию
``APP_DATA_DIR/portal_accounts.sqlite3``). Пароли и хеши в вывод не попадают.

Команды:
  init                         создать файл и схему
  list [--status S]            таблица аккаунтов (без хешей)
  events [--limit N]           журнал auth_events
  propose-mapping --stdin      шаблон mapping.json по строке PORTAL_AUTH_USERS
                               (роль НЕ угадывается: поле role пустое)
  import-legacy --stdin --mapping F [--dry-run]
                               перенос хешей PORTAL_AUTH_USERS по явной таблице
  create --login L --role R [--staff-id S] [--display-name N]
                               создать аккаунт (пароль вводится скрыто)
  reset-password --login L     временный пароль (печатается один раз)
  block --login L | unblock --login L | revoke-sessions --login L
  set-role --login L --role R | set-staff --login L --staff-id S
  set-sections --login L --json '{"audit_results":{"view":true,"download":true}}'
  backup --dest DIR            онлайн-копия базы

Строка ``PORTAL_AUTH_USERS`` читается только из stdin (``--stdin``), чтобы
хеши не попадали в argv и историю shell:

  sudo grep '^PORTAL_AUTH_USERS=' /etc/auditmanager/backend.phaseb.env \\
    | sudo -u coder …/venv/bin/python backend/scripts/portal_accounts.py \\
        --db …/portal_accounts.sqlite3 import-legacy --stdin --mapping mapping.json --dry-run
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _legacy_entries_from_stdin() -> list[tuple[str, str]]:
    raw = sys.stdin.read().strip()
    if raw.startswith("PORTAL_AUTH_USERS="):
        raw = raw.split("=", 1)[1]
    from backend.app.core.portal_auth import _parse_users

    return list(_parse_users(raw).items())


def _account_id(store, login: str) -> int:
    norm = store.normalize_login(login)
    for acc in store.list_accounts():
        if acc["login"].casefold() == norm or (acc.get("legacy_login") or "") == login:
            return int(acc["id"])
    raise SystemExit(f"аккаунт {login!r} не найден")


def _table(rows: list[dict]) -> str:
    cols = ["id", "login", "display_name", "role", "status", "staff_id", "must_change_password",
            "legacy_login", "sections"]
    lines = ["\t".join(cols)]
    for r in rows:
        secs = ",".join(
            f"{s}:{'V' if v['view'] else '-'}{'D' if v['download'] else '-'}"
            for s, v in (r.get("sections") or {}).items() if v["view"] or v["download"]
        ) or "—"
        lines.append("\t".join(str(r.get(c) if c != "sections" else secs) for c in cols))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", help="путь к portal_accounts.sqlite3")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    p = sub.add_parser("list"); p.add_argument("--status")
    p = sub.add_parser("events"); p.add_argument("--limit", type=int, default=50)
    p = sub.add_parser("propose-mapping"); p.add_argument("--stdin", action="store_true", required=True)
    p = sub.add_parser("import-legacy")
    p.add_argument("--stdin", action="store_true", required=True)
    p.add_argument("--mapping", required=True)
    p.add_argument("--dry-run", action="store_true")
    p = sub.add_parser("create")
    p.add_argument("--login", required=True); p.add_argument("--role", required=True)
    p.add_argument("--staff-id"); p.add_argument("--display-name", default="")
    p.add_argument("--password-stdin", action="store_true", help="пароль одной строкой из stdin")
    for name in ("reset-password", "block", "unblock", "revoke-sessions"):
        p = sub.add_parser(name); p.add_argument("--login", required=True)
    p = sub.add_parser("set-role"); p.add_argument("--login", required=True); p.add_argument("--role", required=True)
    p = sub.add_parser("set-staff"); p.add_argument("--login", required=True); p.add_argument("--staff-id", default="")
    p = sub.add_parser("set-sections"); p.add_argument("--login", required=True); p.add_argument("--json", required=True)
    p = sub.add_parser("backup"); p.add_argument("--dest", required=True)
    args = ap.parse_args(argv)

    if args.db:
        os.environ["PORTAL_ACCOUNTS_DB"] = args.db
    from backend.app.services.accounts import db, store

    try:
        if args.cmd == "init":
            print(db.ensure_ready())
        elif args.cmd == "list":
            print(_table(store.list_accounts(args.status)))
        elif args.cmd == "events":
            for e in store.list_events(args.limit):
                print(e["at"], e["event"], f"actor={e['actor_account_id']}",
                      f"target={e['target_account_id']}", json.dumps(e["details"], ensure_ascii=False))
        elif args.cmd == "propose-mapping":
            from backend.app.services.common import user_service

            out = {}
            for legacy, _hash in _legacy_entries_from_stdin():
                staff = user_service.get_user_by_login(legacy)
                out[legacy] = {
                    "role": "",  # заполнить явно: admin | viewer
                    "staff_id": (staff or {}).get("id"),
                    "display_name": (staff or {}).get("name") or legacy,
                    "sections": {},
                }
            print(json.dumps(out, ensure_ascii=False, indent=2))
        elif args.cmd == "import-legacy":
            mapping = json.loads(Path(args.mapping).read_text(encoding="utf-8"))
            report = store.import_legacy(_legacy_entries_from_stdin(), mapping, dry_run=args.dry_run)
            print(("DRY-RUN: " if args.dry_run else "ИМПОРТ ВЫПОЛНЕН: ") + f"{len(report)} строк")
            print(_table([{**r, "id": "-", "login": r.get("login_norm"), "status": "active",
                           "must_change_password": False} for r in report if r.get("action") == "create"]))
            for r in report:
                if r.get("action") != "create":
                    print(r["legacy_login"], r["action"])
        elif args.cmd == "create":
            if args.password_stdin:
                password = sys.stdin.readline().rstrip("\n")
            else:
                password = getpass.getpass("Пароль: ")
                if getpass.getpass("Ещё раз: ") != password:
                    raise SystemExit("пароли не совпадают")
            acc = store.create_account(login=args.login, display_name=args.display_name or args.login,
                                       role=args.role, password=password, staff_id=args.staff_id)
            print(_table([acc]))
        elif args.cmd == "reset-password":
            acc, temp = store.reset_password(None, _account_id(store, args.login))
            print(f"Временный пароль для {acc['login']} (показывается один раз): {temp}")
        elif args.cmd in ("block", "unblock", "revoke-sessions"):
            fn = {"block": store.block, "unblock": store.unblock,
                  "revoke-sessions": store.revoke_sessions}[args.cmd]
            print(_table([fn(None, _account_id(store, args.login))]))
        elif args.cmd == "set-role":
            print(_table([store.set_role(None, _account_id(store, args.login), args.role)]))
        elif args.cmd == "set-staff":
            print(_table([store.set_staff(None, _account_id(store, args.login), args.staff_id)]))
        elif args.cmd == "set-sections":
            print(_table([store.set_sections(None, _account_id(store, args.login), json.loads(args.json))]))
        elif args.cmd == "backup":
            print(db.backup(Path(args.dest)))
    except store.AccountError as exc:
        print(f"ОШИБКА [{exc.code}]: {exc.message}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
