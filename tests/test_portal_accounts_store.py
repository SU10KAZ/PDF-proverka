"""Хранилище аккаунтов портала (план доступа, этап 1). Только временная БД."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from passlib.hash import pbkdf2_sha256

from backend.app.services.accounts import db, store
import backend.app.services.common.user_service as user_service

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def accounts_db(tmp_path, monkeypatch):
    path = tmp_path / "accounts.sqlite3"
    monkeypatch.setenv("PORTAL_ACCOUNTS_DB", str(path))
    monkeypatch.setenv("PORTAL_PASSWORD_ROUNDS", "1000")
    db.reset_for_tests()
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"users": [
        {"id": "boss", "login": "boss", "name": "Босс Б.Б.", "role": "admin"},
        {"id": "kulik", "login": "kulik", "name": "Кулик А. С.", "role": "expert"},
        {"id": "maksheev", "login": "pavel", "name": "Макшеев П.", "role": "expert"},
    ], "current_id": "boss"}, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(user_service, "USERS_FILE", users)
    yield path
    db.reset_for_tests()


def _admin(login="boss", **kw):
    return store.create_account(login=login, display_name="Админ", role="admin",
                                password="admin-password-1", **kw)


def _viewer(login="view1", **kw):
    return store.create_account(login=login, display_name="Зритель", role="viewer",
                                password="viewer-password-1", **kw)


def test_file_permissions_and_schema(accounts_db):
    db.ensure_ready()
    assert oct(accounts_db.stat().st_mode & 0o777) == "0o600"
    with sqlite3.connect(accounts_db) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


@pytest.mark.parametrize("sql", [
    "INSERT INTO account_sections VALUES (1,'audit_results',0,1)",   # скачивание без просмотра
    "INSERT INTO account_sections VALUES (1,'nowhere',1,0)",          # неизвестный раздел
    "INSERT INTO account_sections VALUES (1,'schedule',1,1)",         # раздел без выгрузок
    "UPDATE accounts SET role='root' WHERE id=1",
    "UPDATE accounts SET status='approved' WHERE id=1",
])
def test_check_constraints(sql):
    _admin()
    with sqlite3.connect(db.db_path()) as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(sql)


def test_download_requires_view_and_no_download_sections():
    acc = _viewer()
    with pytest.raises(store.AccountError) as e1:
        store.set_sections(None, acc["id"], {"audit_results": {"view": False, "download": True}})
    assert e1.value.code == "download_requires_view"
    with pytest.raises(store.AccountError) as e2:
        store.set_sections(None, acc["id"], {"knowledge_base": {"view": True, "download": True}})
    assert e2.value.code == "no_downloads_in_section"
    out = store.set_sections(None, acc["id"], {"audit_results": {"view": True, "download": True},
                                               "schedule": {"view": True, "download": False}})
    assert out["sections"]["audit_results"] == {"view": True, "download": True}
    assert out["sections"]["schedule"] == {"view": True, "download": False}
    assert out["sections"]["stage_comparison"] == {"view": False, "download": False}


@pytest.mark.parametrize("variant", ["Ivan", "IVAN", " ivan ", "ivan"])
def test_login_unique_case_insensitive(variant):
    store.create_account(login="ivan", display_name="И", role="viewer", password="password-123")
    with pytest.raises(store.AccountError) as exc:
        store.create_account(login=variant, display_name="И2", role="viewer", password="password-123")
    assert exc.value.code == "login_taken"


@pytest.mark.parametrize("bad", ["", "ab", "иван", "a b", "-x", "x" * 40, "a/b"])
def test_login_format(bad):
    with pytest.raises(store.AccountError):
        store.normalize_login(bad)


def test_staff_unique():
    _admin(staff_id="boss")
    with pytest.raises(store.AccountError) as exc:
        _viewer(staff_id="boss")
    assert exc.value.code == "staff_taken"
    acc = _viewer()
    with pytest.raises(store.AccountError):
        store.set_staff(None, acc["id"], "boss")
    with pytest.raises(store.AccountError) as e2:
        store.set_staff(None, acc["id"], "nobody")
    assert e2.value.code == "unknown_staff"


def test_last_admin_and_self_protection():
    admin = _admin()
    with pytest.raises(store.AccountError) as e1:
        store.block(admin["id"], admin["id"])
    assert e1.value.code == "self_action"
    with pytest.raises(store.AccountError) as e2:
        store.set_role(None, admin["id"], "viewer")       # CLI: последний admin
    assert e2.value.code == "last_admin"
    with pytest.raises(store.AccountError):
        store.block(None, admin["id"])
    assert store.get_account(admin["id"])["status"] == "active"
    second = _admin("boss2")
    store.set_role(second["id"], admin["id"], "viewer")
    assert store.active_admin_count() == 1


def test_registration_is_always_pending_viewer_without_sections():
    acc = store.create_pending(login="Newbie", display_name="Новиков Н. Н.",
                               password="long-password-1", note="подрядчик")
    assert (acc["role"], acc["status"]) == ("viewer", "pending")
    assert all(not v["view"] and not v["download"] for v in acc["sections"].values())
    assert acc["login"] == "Newbie" and store.normalize_login("NEWBIE") == "newbie"


def test_registration_limits(monkeypatch):
    for i in range(3):
        store.create_pending(login=f"reg{i}", display_name="Имя Фамилия", password="long-password-1", ip="1.2.3.4")
    with pytest.raises(store.AccountError) as exc:
        store.create_pending(login="reg9", display_name="Имя Фамилия", password="long-password-1", ip="1.2.3.4")
    assert exc.value.status == 429
    monkeypatch.setattr(store, "MAX_PENDING", 4)
    store.create_pending(login="regx", display_name="Имя Фамилия", password="long-password-1", ip="5.5.5.5")
    with pytest.raises(store.AccountError):
        store.create_pending(login="regy", display_name="Имя Фамилия", password="long-password-1", ip="6.6.6.6")


@pytest.mark.parametrize("pw", ["short", "v1v1v1v1v1"[:4], "registered"])
def test_password_policy(pw):
    with pytest.raises(store.AccountError):
        store.create_pending(login="registered", display_name="Имя Фамилия", password=pw)


def test_authenticate_statuses_and_throttle():
    admin = _admin()
    v = _viewer()
    assert store.authenticate("view1", "viewer-password-1").ok
    assert store.authenticate("VIEW1", "viewer-password-1").ok           # регистр логина не важен
    store.block(admin["id"], v["id"])
    blocked = store.authenticate("view1", "viewer-password-1")
    assert (blocked.ok, blocked.code) == (False, "account_blocked")
    wrong = store.authenticate("view1", "nope-nope-nope")
    assert wrong.code == "invalid_credentials"                         # статус не раскрыт без пароля
    pend = store.create_pending(login="pend", display_name="Имя Фамилия", password="long-password-1")
    assert store.authenticate("pend", "long-password-1").code == "account_pending"
    store.reject(admin["id"], pend["id"])
    assert store.authenticate("pend", "long-password-1").code == "account_rejected"
    for _ in range(store.LOGIN_FAILS):
        store.authenticate("boss", "bad-bad-bad-bad", ip="9.9.9.9")
    locked = store.authenticate("boss", "admin-password-1", ip="9.9.9.9")
    assert locked.code == "locked" and locked.retry_after > 0
    # блокировка переживает «рестарт» (новое соединение/процесс)
    db.reset_for_tests()
    assert store.authenticate("boss", "admin-password-1").code == "locked"
    assert store.authenticate("ghost", "whatever-123").code == "invalid_credentials"


def test_session_version_bumps():
    admin = _admin()
    v = _viewer()
    sv0 = store.get_context(v["id"]).session_version
    store.revoke_sessions(admin["id"], v["id"])
    acc, temp = store.reset_password(admin["id"], v["id"])
    assert acc["must_change_password"] and len(temp) >= 12
    ctx = store.get_context(v["id"])
    assert ctx.session_version == sv0 + 2 and ctx.must_change_password
    assert store.authenticate("view1", temp).ok
    ctx2 = store.change_password(v["id"], temp, "brand-new-password")
    assert not ctx2.must_change_password and ctx2.session_version == sv0 + 3
    store.set_role(admin["id"], v["id"], "admin")
    assert store.get_context(v["id"]).session_version == sv0 + 4
    # права по разделам версию сессии не меняют
    store.set_sections(admin["id"], v["id"], {"audit_results": {"view": True, "download": False}})
    assert store.get_context(v["id"]).session_version == sv0 + 4


def test_events_have_no_secrets():
    admin = _admin()
    v = _viewer()
    _, temp = store.reset_password(admin["id"], v["id"])
    store.authenticate("view1", "wrong-password-x", ip="1.1.1.1")
    raw = Path(db.db_path()).read_bytes()
    events = json.dumps(store.list_events(), ensure_ascii=False)
    assert temp not in events and "pbkdf2" not in events and "viewer-password-1" not in events
    assert "password_hash" not in json.dumps(store.list_accounts())
    assert b"viewer-password-1" not in raw and temp.encode() not in raw


def test_delete_only_requests():
    admin = _admin()
    v = _viewer()
    with pytest.raises(store.AccountError):
        store.delete(admin["id"], v["id"])
    pend = store.create_pending(login="pp1", display_name="Имя Фамилия", password="long-password-1")
    store.delete(admin["id"], pend["id"])
    assert store.get_account(pend["id"]) is None


def test_legacy_import_dry_run_and_conflicts():
    old = {u: pbkdf2_sha256.hash("old-" + u) for u in ("boss", "kulik", "pavel")}
    entries = list(old.items())
    mapping = {
        "boss": {"role": "admin", "staff_id": "boss"},
        "kulik": {"role": "viewer", "staff_id": "kulik",
                  "sections": {"audit_results": {"view": True, "download": True}}},
        "pavel": {"role": "viewer", "staff_id": "maksheev"},
    }
    report = store.import_legacy(entries, mapping, dry_run=True)
    assert [r["role"] for r in report] == ["admin", "viewer", "viewer"]
    assert "pbkdf2" not in json.dumps(report)
    assert store.list_accounts() == []                                  # dry-run ничего не пишет
    for bad, code in [
        ({**mapping, "pavel": {"role": "viewer", "staff_id": "kulik"}}, "сотрудник kulik"),
        ({**mapping, "boss": {"role": "", "staff_id": "boss"}}, "роль"),
        ({k: {**v, "role": "viewer"} for k, v in mapping.items()}, "нет ни одного admin"),
        ({**mapping, "nobody": {"role": "viewer"}}, "нет в PORTAL_AUTH_USERS"),
    ]:
        with pytest.raises(store.AccountError) as exc:
            store.import_legacy(entries, bad, dry_run=True)
        assert code in exc.value.message
    with pytest.raises(store.AccountError):
        store.import_legacy(entries + [("BOSS", old["boss"])], {**mapping, "BOSS": {"role": "viewer"}})
    with pytest.raises(store.AccountError):
        store.import_legacy([("boss", "plain-text")], {"boss": {"role": "admin"}})
    store.import_legacy(entries, mapping, dry_run=False)
    accs = {a["login"]: a for a in store.list_accounts()}
    assert accs["boss"]["role"] == "admin" and accs["kulik"]["role"] == "viewer"
    assert accs["pavel"]["staff_id"] == "maksheev" and accs["pavel"]["legacy_login"] == "pavel"
    login = store.authenticate("kulik", "old-kulik")                    # старый хеш принят
    assert login.ok
    with sqlite3.connect(db.db_path()) as conn:                         # и перехеширован
        h = conn.execute("SELECT password_hash FROM accounts WHERE login_norm='kulik'").fetchone()[0]
    assert h != old["kulik"]
    with pytest.raises(store.AccountError):                             # повторный импорт — конфликт
        store.import_legacy(entries, mapping, dry_run=True)


def test_cli_roundtrip(tmp_path, accounts_db):
    env = {"PATH": "/usr/bin:/bin", "PORTAL_PASSWORD_ROUNDS": "1000", "AUDIT_DISABLE_DOTENV": "1",
           "AUDIT_APP_DATA_DIR": str(tmp_path / "appdata")}
    cli = [sys.executable, str(ROOT / "backend/scripts/portal_accounts.py"), "--db", str(accounts_db)]
    r = subprocess.run(cli + ["create", "--login", "owner", "--role", "admin", "--password-stdin"],
                       input="owner-password-1\n", text=True, capture_output=True, env=env, timeout=60)
    assert r.returncode == 0, r.stderr
    r = subprocess.run(cli + ["list"], text=True, capture_output=True, env=env, timeout=60)
    assert "owner" in r.stdout and "pbkdf2" not in r.stdout
    r = subprocess.run(cli + ["backup", "--dest", str(tmp_path / "bk")], text=True,
                       capture_output=True, env=env, timeout=60)
    assert r.returncode == 0 and Path(r.stdout.strip()).is_file()


@pytest.mark.parametrize("quote", ["", "'", '"'])
def test_cli_propose_mapping_accepts_quoted_env_line(tmp_path, accounts_db, quote):
    """Строка из env-файла службы по ``grep`` может прийти с кавычками вокруг
    значения (systemd их снимает сам) — логины без кавычек."""
    from backend.app.core import portal_auth

    env = {"PATH": "/usr/bin:/bin", "PORTAL_PASSWORD_ROUNDS": "1000", "AUDIT_DISABLE_DOTENV": "1",
           "AUDIT_APP_DATA_DIR": str(tmp_path / "appdata")}
    line = "PORTAL_AUTH_USERS={q}alpha:{h1},beta:{h2}{q}\n".format(
        q=quote, h1=portal_auth.hash_password("alpha-pass"), h2=portal_auth.hash_password("beta-pass"))
    cli = [sys.executable, str(ROOT / "backend/scripts/portal_accounts.py"), "--db", str(accounts_db)]
    r = subprocess.run(cli + ["propose-mapping", "--stdin"], input=line, text=True,
                       capture_output=True, env=env, timeout=60)
    assert r.returncode == 0, r.stderr
    assert sorted(json.loads(r.stdout)) == ["alpha", "beta"]
