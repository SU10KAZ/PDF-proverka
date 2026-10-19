#!/usr/bin/env python3
"""Репетиция миграции env_users → accounts и отката на КОПИИ данных
(план доступа, этап 9; поправка 0.6). Боевые аккаунты и конфигурация не
меняются: users.json, decisions_log.json и файлы графика копируются (только
чтение источника); хеши паролей синтетические — для тех же логинов, что в
users.json; настоящие пароли и хеши не читаются.

  python scripts/portal_access/rehearse_migration.py --work <tmp> \\
      --owner-login <логин владельца> --out <tmp>/rehearsal.md

Роль admin получает ТОЛЬКО --owner-login (явное указание), остальные —
viewer с разделами из --viewer-sections (по умолчанию все разделы на
просмотр, скачивание там, где есть выгрузки — это предложение, его
утверждает владелец).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC_APP = Path("/srv/auditmanager/data/backend/app/data")
SRC_KB = Path("/srv/auditmanager/data/knowledge_base")
H = {"X-Requested-With": "AuditManager"}
SECTIONS = ("audit_results", "version_comparison", "stage_comparison", "knowledge_base", "schedule")
NO_DOWNLOADS = {"knowledge_base", "schedule"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "absent"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", required=True)
    ap.add_argument("--owner-login", required=True)
    ap.add_argument("--viewer-sections", default="all")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    work = Path(args.work).resolve()
    assert not str(work).startswith("/srv/auditmanager"), "рабочая папка не может быть в боевых данных"
    if work.exists():
        shutil.rmtree(work)
    (work / "app_data").mkdir(parents=True)
    (work / "knowledge_base").mkdir()
    for name in ("users.json", "objects.json"):
        shutil.copy2(SRC_APP / name, work / "app_data" / name)
    for name in ("decisions_log.json", "patterns.json", "schedule_completion.json", "work_plans.json"):
        if (SRC_KB / name).exists():
            shutil.copy2(SRC_KB / name, work / "knowledge_base" / name)
    for name in ("work_plans.json", "schedule_completion.json"):
        if (SRC_APP / name).exists():
            shutil.copy2(SRC_APP / name, work / "app_data" / name)
    fingerprints_before = {p.name: sha(p) for p in list((work / "knowledge_base").iterdir()) +
                           [work / "app_data" / "users.json"]}

    os.environ.update({
        "AUDIT_DISABLE_DOTENV": "1", "AUDIT_DATA_DIR": str(work), "AUDIT_APP_DATA_DIR": str(work / "app_data"),
        "AUDIT_OBJECTS_FILE": str(work / "app_data" / "objects.json"),
        "AUDIT_PROJECTS_V2_DIR": str(work / "projects_v2"), "AUDIT_PROJECTS_DIR": str(work / "projects"),
        "AUDIT_ACTION_LOG_DIR": str(work / "logs"), "COMPARISON_ROOT": str(work / "comparison"),
        "VERSION_COMPARISON_ROOT": str(work / "version_comparison"),
        "PORTAL_ACCOUNTS_DB": str(work / "app_data" / "portal_accounts.sqlite3"),
        "PORTAL_PASSWORD_ROUNDS": "1000", "PORTAL_COOKIE_SECURE": "false",
        "DISTRIBUTED_WORKERS_ENABLED": "false",
    })
    sys.path.insert(0, str(ROOT))
    users = json.loads((work / "app_data" / "users.json").read_text(encoding="utf-8"))["users"]
    logins = [u.get("login") or u["id"] for u in users]
    assert args.owner_login in logins, "владелец должен быть среди логинов сотрудников"
    from backend.app.core import portal_auth

    passwords = {login: f"rehearsal-{login}-pw" for login in logins}
    legacy_line = "PORTAL_AUTH_USERS=" + ",".join(f"{l}:{portal_auth.hash_password(passwords[l])}" for l in logins)
    report: list[str] = ["# Репетиция миграции аккаунтов и отката (копия данных)", ""]
    checks: list[tuple[str, bool]] = []

    # 0. Исходный режим env_users: что видит график работ.
    os.environ.update({"PORTAL_AUTH_ENABLED": "true", "PORTAL_AUTH_MODE": "env_users",
                       "PORTAL_AUTH_USERS": legacy_line.split("=", 1)[1],
                       "PORTAL_SESSION_SECRET": "old-secret-" + "o" * 40,
                       "PORTAL_SESSION_COOKIE_NAME": "portal_session"})
    from fastapi.testclient import TestClient
    from backend.app.main import app
    import backend.app.services.common.user_service as user_service
    import backend.app.services.common.schedule_service as schedule_service
    import backend.app.services.knowledge_base.knowledge_base_service as kb
    user_service.USERS_FILE = work / "app_data" / "users.json"
    for mod in (kb, schedule_service, user_service):
        for attr, target in (("DECISIONS_LOG_FILE", work / "knowledge_base" / "decisions_log.json"),
                             ("KNOWLEDGE_BASE_DIR", work / "knowledge_base")):
            if hasattr(mod, attr):
                setattr(mod, attr, target)

    def client(login, pw):
        c = TestClient(app, headers=H)
        r = c.post("/api/auth/login", json={"username": login, "password": pw})
        return c, r

    old_c, r = client(args.owner_login, passwords[args.owner_login])
    checks.append(("env_users: владелец входит по прежней строке", r.status_code == 200))
    sched_before = old_c.get("/api/schedule?from=2026-09-01&to=2026-10-31").json()

    # 1. CLI: propose-mapping → mapping (роль только владельцу) → import dry-run → import.
    cli = [sys.executable, str(ROOT / "backend/scripts/portal_accounts.py")]
    env = dict(os.environ)
    proposal = json.loads(subprocess.run(cli + ["propose-mapping", "--stdin"], input=legacy_line, text=True,
                                         capture_output=True, env=env, check=True).stdout)
    checks.append(("propose-mapping не угадывает роли (все role пустые)", all(v["role"] == "" for v in proposal.values())))
    checks.append(("propose-mapping: все логины связаны с сотрудником", all(v["staff_id"] for v in proposal.values())))
    sections = {s: {"view": True, "download": s not in NO_DOWNLOADS} for s in SECTIONS} \
        if args.viewer_sections == "all" else json.loads(args.viewer_sections)
    mapping = {login: {**spec, "role": "admin" if login == args.owner_login else "viewer",
                       "sections": {} if login == args.owner_login else sections}
               for login, spec in proposal.items()}
    mpath = work / "mapping.json"
    mpath.write_text(json.dumps(mapping, ensure_ascii=False, indent=1), encoding="utf-8")
    dry = subprocess.run(cli + ["import-legacy", "--stdin", "--mapping", str(mpath), "--dry-run"],
                         input=legacy_line, text=True, capture_output=True, env=env)
    checks.append(("import-legacy --dry-run без конфликтов", dry.returncode == 0))
    checks.append(("в выводе нет хешей", "pbkdf2" not in dry.stdout + dry.stderr))
    real = subprocess.run(cli + ["import-legacy", "--stdin", "--mapping", str(mpath)],
                          input=legacy_line, text=True, capture_output=True, env=env)
    checks.append(("import-legacy выполнен", real.returncode == 0))

    # 2. Переключение на accounts: новый секрет и имя cookie.
    os.environ.update({"PORTAL_AUTH_MODE": "accounts", "PORTAL_SESSION_SECRET": "new-secret-" + "n" * 40,
                       "PORTAL_SESSION_COOKIE_NAME": "am_session"})
    checks.append(("старая cookie env_users не действует", old_c.get("/api/projects").status_code == 401))
    from backend.app.services.accounts import store
    table = []
    roles_ok = True
    for login in logins:
        c, r = client(login, passwords[login])
        me = c.get("/api/auth/me").json().get("account") or {}
        want = "admin" if login == args.owner_login else "viewer"
        roles_ok &= r.status_code == 200 and me.get("role") == want
        acc = next(a for a in store.list_accounts() if a["login"] == login)
        staff = next((u for u in users if u["id"] == acc["staff_id"]), {})
        secs = ", ".join(f"{s}:{'просмотр' if v['view'] else '—'}{'+скачивание' if v['download'] else ''}"
                         for s, v in acc["sections"].items() if v["view"]) or ("всё (admin)" if want == "admin" else "—")
        table.append(f"| {login} | {acc['staff_id']} | {staff.get('name', '')} | {staff.get('role', '')} | "
                     f"{want} | {secs} |")
        if want == "viewer":
            checks.append((f"{login}: экспертное решение отклонено (403)",
                           c.post("/api/knowledge-base/expert-review/EOM%2FX", json={"decisions": []}).status_code == 403))
            checks.append((f"{login}: график виден", c.get("/api/schedule?from=2026-09-01&to=2026-10-31").status_code == 200))
    checks.append(("роли после импорта: admin только владелец, остальные viewer", roles_ok))
    owner_c, _ = client(args.owner_login, passwords[args.owner_login])
    sched_after = owner_c.get("/api/schedule?from=2026-09-01&to=2026-10-31").json()
    checks.append(("график работ до и после перехода совпадает", sched_before == sched_after))
    fingerprints_after = {p.name: sha(p) for p in list((work / "knowledge_base").iterdir()) +
                          [work / "app_data" / "users.json"]}
    checks.append(("decisions_log.json, users.json и файлы графика не изменились",
                   fingerprints_before == fingerprints_after))

    # 3. Блокировка одного viewer и аварийный откат.
    victim = next(l for l in logins if l != args.owner_login)
    victim_id = next(a["id"] for a in store.list_accounts() if a["login"] == victim)
    store.block(None, victim_id)
    viewer_c, _ = client(next(l for l in logins if l not in (args.owner_login, victim)),
                         passwords[next(l for l in logins if l not in (args.owner_login, victim))])
    os.environ.update({"PORTAL_AUTH_MODE": "emergency_owner", "PORTAL_SESSION_SECRET": "emerg-secret-" + "e" * 40,
                       "PORTAL_SESSION_COOKIE_NAME": "am_emergency",
                       "PORTAL_EMERGENCY_OWNER": f"{args.owner_login}:{portal_auth.hash_password('emergency-owner-pw')}"})
    checks.append(("аварийный режим: cookie accounts не действуют",
                   owner_c.get("/api/projects").status_code == 401 and viewer_c.get("/api/projects").status_code == 401))
    checks.append(("аварийный режим: старые пароли не действуют",
                   all(client(l, passwords[l])[1].status_code == 401 for l in logins)))
    em_c, r = client(args.owner_login, "emergency-owner-pw")
    checks.append(("аварийный режим: владелец входит с новым паролем", r.status_code == 200))
    checks.append(("аварийный режим: регистрация закрыта", TestClient(app, headers=H).post(
        "/api/auth/register", json={"login": "x1x", "display_name": "Икс И.И.", "password": "long-password-1"}).status_code == 403))
    os.environ.update({"PORTAL_AUTH_MODE": "accounts", "PORTAL_SESSION_SECRET": "back-secret-" + "b" * 40,
                       "PORTAL_SESSION_COOKIE_NAME": "am_session"})
    checks.append(("возврат: аварийная cookie не действует", em_c.get("/api/projects").status_code == 401))
    checks.append(("возврат: заблокированный остаётся заблокированным",
                   client(victim, passwords[victim])[1].status_code == 403))
    back_c, r = client(next(l for l in logins if l not in (args.owner_login, victim)),
                       passwords[next(l for l in logins if l not in (args.owner_login, victim))])
    checks.append(("возврат: права viewer сохранены", (back_c.get("/api/auth/me").json().get("account") or {}).get("role") == "viewer"))
    backup = subprocess.run(cli + ["backup", "--dest", str(work / "backups")], text=True, capture_output=True, env=env)
    checks.append(("резервная копия базы аккаунтов создаётся", backup.returncode == 0))

    report += ["Владелец (admin) в репетиции: `" + args.owner_login + "` — указан явно, подтверждает владелец.", "",
               "## Таблица миграции (без паролей и хешей)", "",
               "| Логин | staff_id | Сотрудник | Роль в users.json (описательная) | Новая роль | Разделы |",
               "|---|---|---|---|---|---|", *table, "", "## Проверки", ""]
    report += [f"- {'✅' if ok else '❌'} {name}" for name, ok in checks]
    Path(args.out).write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
