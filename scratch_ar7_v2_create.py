"""Шаги 1-4: минт куки → switch объекта 213 → создать AR7 V2 из сандбокса → проверить.
Аудит НЕ запускает (отдельный шаг после проверки)."""
import json
import urllib.request

import backend.app.core.config  # noqa: F401  — триггерит load_dotenv() (.env → os.environ)
from backend.app.core import portal_auth

BASE = "http://127.0.0.1:8081"
SRC = "AR/AR7_norms_pilot"
TGT = "AR/133-23-ГК-АР7"
OBJ = "0b540226"  # 213 Мосфильмовская

settings = portal_auth.get_settings()
token = portal_auth.issue_token("andrey", settings)
COOKIE = f"{settings.cookie_name}={token}"


def call(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Cookie", COOKIE)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else raw)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:500]


# 0) auth sanity
print("0) auth check /api/projects (authed):")
st, _ = call("GET", "/api/projects")
print("   status:", st)

# 1) switch на объект 213
print("1) switch object → 213:")
st, r = call("POST", "/api/objects/switch", {"object_id": OBJ})
print("   status:", st, "| resp:", str(r)[:120])

# 2) создать V2 из сандбокса (сохранить сандбокс: delete_source=false)
print("2) versions/from-project (создать AR7 V2):")
st, r = call("POST", "/api/projects/versions/from-project", {
    "source_project_id": SRC,
    "target_project_id": TGT,
    "comment": "Пилот norms-after-merge → V2 AR7",
    "discard_source_output": True,
    "delete_source": False,
})
print("   status:", st)
print("   resp:", json.dumps(r, ensure_ascii=False)[:400] if isinstance(r, (dict, list)) else str(r)[:400])
