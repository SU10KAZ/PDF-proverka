"""
Сервис управления пользователями (сотрудниками-экспертами платформы).

Пользователь — это сотрудник, который принимает/отклоняет замечания и
оптимизации. Его действия фиксируются в knowledge_base/decisions_log.json
через поле `expert_reviewer`. Активность пользователя (что он сделал на
платформе) агрегируется из этого лога по проектам.

Хранилище — JSON-файл (config.USERS_FILE_PATH), без БД. Аналог
object_service по структуре.
"""
import json
import re
import uuid
from datetime import datetime
from typing import Optional

from backend.app.core.config import USERS_FILE_PATH, DECISIONS_LOG_FILE

USERS_FILE = USERS_FILE_PATH


# ─── базовая загрузка/сохранение ────────────────────────────────────────────

def _load() -> dict:
    if not USERS_FILE.exists():
        return {"users": [], "current_id": None}
    try:
        data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"users": [], "current_id": None}
        data.setdefault("users", [])
        data.setdefault("current_id", None)
        return data
    except (json.JSONDecodeError, OSError):
        return {"users": [], "current_id": None}


def _save(data: dict):
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _now_iso() -> str:
    return datetime.now().isoformat()


def _slugify(surname: str) -> str:
    """Транслитерация фамилии в латинский id-слаг."""
    table = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
        "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    }
    slug = "".join(table.get(ch, ch) for ch in surname.strip().lower())
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return slug or f"user_{uuid.uuid4().hex[:6]}"


# ─── CRUD ────────────────────────────────────────────────────────────────────

def list_users() -> list[dict]:
    return _load()["users"]


def get_current_id() -> Optional[str]:
    data = _load()
    if data["current_id"]:
        return data["current_id"]
    return data["users"][0]["id"] if data["users"] else None


def get_current_user() -> Optional[dict]:
    cid = get_current_id()
    if not cid:
        return None
    return get_user_by_id(cid)


def get_user_by_id(user_id: str) -> Optional[dict]:
    for u in _load()["users"]:
        if u["id"] == user_id:
            return u
    return None


def get_user_by_login(login: Optional[str]) -> Optional[dict]:
    """Найти сотрудника по логину портала (PORTAL_AUTH_USERS).

    Матчим по полю `login`, в fallback — по `id` (для сотрудников без явного
    логина). Регистронезависимо.
    """
    if not login:
        return None
    needle = login.strip().lower()
    if not needle:
        return None
    for u in _load()["users"]:
        candidates = {
            (u.get("login") or "").strip().lower(),
            (u.get("id") or "").strip().lower(),
        }
        candidates.discard("")
        if needle in candidates:
            return u
    return None


def add_user(surname: str, initials: str = "", role: str = "expert", login: str = "") -> dict:
    surname = (surname or "").strip()
    if not surname:
        raise ValueError("Фамилия не может быть пустой")
    initials = (initials or "").strip()
    data = _load()

    # уникальный id на основе фамилии
    base = _slugify(surname)
    existing_ids = {u["id"] for u in data["users"]}
    uid = base
    n = 2
    while uid in existing_ids:
        uid = f"{base}_{n}"
        n += 1

    name = f"{surname} {initials}".strip()
    user = {
        "id": uid,
        "login": (login or "").strip() or uid,   # логин портала (PORTAL_AUTH_USERS)
        "surname": surname,
        "initials": initials,
        "name": name,
        "role": role or "expert",
        "created_at": _now_iso(),
    }
    data["users"].append(user)
    if not data["current_id"]:
        data["current_id"] = uid
    _save(data)
    return user


def update_user(
    user_id: str,
    surname: Optional[str] = None,
    initials: Optional[str] = None,
    role: Optional[str] = None,
    login: Optional[str] = None,
) -> dict:
    data = _load()
    for u in data["users"]:
        if u["id"] == user_id:
            if surname is not None:
                u["surname"] = surname.strip()
            if initials is not None:
                u["initials"] = initials.strip()
            if role is not None:
                u["role"] = role.strip() or "expert"
            if login is not None:
                u["login"] = login.strip()
            u["name"] = f"{u.get('surname', '')} {u.get('initials', '')}".strip()
            _save(data)
            return u
    raise ValueError(f"Пользователь '{user_id}' не найден")


def delete_user(user_id: str):
    data = _load()
    data["users"] = [u for u in data["users"] if u["id"] != user_id]
    if data["current_id"] == user_id:
        data["current_id"] = data["users"][0]["id"] if data["users"] else None
    _save(data)


def switch_user(user_id: str) -> dict:
    data = _load()
    found = next((u for u in data["users"] if u["id"] == user_id), None)
    if not found:
        raise ValueError(f"Пользователь '{user_id}' не найден")
    data["current_id"] = user_id
    _save(data)
    return found


# ─── активность пользователя (из decisions_log.json) ─────────────────────────

def _load_decisions_log() -> list[dict]:
    if not DECISIONS_LOG_FILE.exists():
        return []
    try:
        data = json.loads(DECISIONS_LOG_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    if isinstance(data, dict):
        return data.get("entries", [])
    if isinstance(data, list):
        return data
    return []


def _reviewer_matches(reviewer: str, user: dict) -> bool:
    """Привязка записи decisions_log к пользователю.

    Фронтенд пишет в `expert_reviewer` полное имя (`Узун А. И.`), но на
    всякий случай матчим и по id/фамилии — на случай старых записей или
    ручного импорта.
    """
    if not reviewer:
        return False
    r = reviewer.strip().lower()
    candidates = {
        (user.get("name") or "").strip().lower(),
        (user.get("id") or "").strip().lower(),
        (user.get("surname") or "").strip().lower(),
    }
    candidates.discard("")
    return r in candidates


def get_user_activity(user_id: str) -> dict:
    """Что пользователь сделал на платформе — группировка по проектам.

    Возвращает список проектов, в которых пользователь принял/отклонил
    замечания и оптимизации (и сохранил решение в базу знаний).
    """
    user = get_user_by_id(user_id)
    if not user:
        raise ValueError(f"Пользователь '{user_id}' не найден")

    entries = [e for e in _load_decisions_log() if _reviewer_matches(e.get("expert_reviewer", ""), user)]

    projects: dict[str, dict] = {}
    for e in entries:
        proj = e.get("source_project") or "—"
        bucket = projects.setdefault(proj, {
            "source_project": proj,
            "object_id": e.get("object_id", ""),
            "section": e.get("section", ""),
            "findings_count": 0,
            "optimizations_count": 0,
            "accepted_count": 0,
            "rejected_count": 0,
            "last_date": "",
            "items": [],
        })
        item_type = e.get("item_type", "finding")
        decision = e.get("expert_decision", "")
        if item_type == "optimization":
            bucket["optimizations_count"] += 1
        else:
            bucket["findings_count"] += 1
        if decision == "accepted":
            bucket["accepted_count"] += 1
        elif decision == "rejected":
            bucket["rejected_count"] += 1
        date = e.get("expert_date", "") or ""
        if date > bucket["last_date"]:
            bucket["last_date"] = date
        bucket["items"].append({
            "id": e.get("id", ""),
            "item_id": e.get("item_id", ""),
            "item_type": item_type,
            "decision": decision,
            "summary": e.get("summary", ""),
            "severity": e.get("severity", ""),
            "category": e.get("category", ""),
            "sheet": e.get("sheet", ""),
            "page": e.get("page"),
            "reason": e.get("expert_reason", ""),
            "date": date,
        })

    project_list = sorted(
        projects.values(), key=lambda p: p["last_date"], reverse=True
    )
    for p in project_list:
        p["items"].sort(key=lambda i: i["date"], reverse=True)

    total_decisions = sum(len(p["items"]) for p in project_list)
    return {
        "user": user,
        "projects": project_list,
        "totals": {
            "projects": len(project_list),
            "decisions": total_decisions,
            "findings": sum(p["findings_count"] for p in project_list),
            "optimizations": sum(p["optimizations_count"] for p in project_list),
            "accepted": sum(p["accepted_count"] for p in project_list),
            "rejected": sum(p["rejected_count"] for p in project_list),
        },
    }
