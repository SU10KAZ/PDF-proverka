"""
Сервис графика производства работ (production work schedule).

Агрегирует факты инженерных проверок/действий из
`knowledge_base/decisions_log.json` в формат, удобный для UI-графика:
строки — инженеры, колонки — дни, в ячейке — проект(ы).

Каждая запись decisions_log трактуется как факт выполненной работы. Для
графика берутся поля:
  * `expert_reviewer` — имя инженера (запись без него пропускается);
  * `expert_date`     — дата выполнения (ISO; парсится дата YYYY-MM-DD);
  * `source_project`  — проект;
  * `section`         — раздел/часть проекта;
  * `object_id`       — объект (опциональный фильтр).

Агрегация идёт по тройке (инженер, день, проект): если инженер в один день
сделал N решений по одному проекту — это ОДНО событие; если по нескольким
проектам — несколько событий с одной датой (frontend уже умеет показывать
первый проект + бейдж «+N»).

Чтение лога безопасно: нет файла → пусто; битый JSON → пусто + warning.
Тяжёлый разбор большого файла роутер выполняет через asyncio.to_thread.

Не ломает user_service / get_user_activity — это отдельный read-only слой
поверх того же лога.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional

from backend.app.core.config import DECISIONS_LOG_FILE

# Файл лога вынесен в модульную переменную, чтобы тесты могли его подменить
# (monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", tmp)).
DECISIONS_LOG_FILE = DECISIONS_LOG_FILE

# Транслитерация кириллицы для стабильного engId из ФИО.
_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}

# Порог обрезки длинного названия проекта (символов) для короткой плашки.
_SHORT_MAX = 32

# Системные/импортные «ревьюеры» в decisions_log — это не инженеры, а служебные
# записи (массовый импорт реестра заказчика и т.п.). В графике их не показываем.
# Сравнение регистронезависимое.
_SYSTEM_REVIEWERS = {"su10_registry"}


def eng_slug(name: str) -> str:
    """Стабильный id инженера из ФИО: «Узун А. И.» → «uzun-a-i».

    Транслитерация + схлопывание небуквенно-цифровых в дефис. Детерминированно
    (одинаковый вход → одинаковый id), что важно для ключей строк графика и
    привязки плана.
    """
    s = (name or "").strip().lower()
    s = "".join(_TRANSLIT.get(ch, ch) for ch in s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"


def short_name(full: str) -> str:
    """Короткое имя проекта для плашки в ячейке. Полное сохраняется отдельно.

    Эвристика первого этапа:
      * если строка начинается с номера объекта («214. Alia (ASTERUS)») —
        берём номер + первые понятные слова до скобки → «214. Alia»;
      * иначе ограничиваем длину до ~32 символов (с ellipsis).
    """
    s = (full or "").strip()
    if not s:
        return ""
    m = re.match(r"^(\d+)[.\)]\s*(.+)$", s)
    if m:
        num, rest = m.group(1), m.group(2)
        head = rest.split("(")[0].strip()
        words = head.split()
        if words:
            head = " ".join(words[:2])
        cand = f"{num}. {head}".strip()
        return cand if len(cand) <= _SHORT_MAX else cand[: _SHORT_MAX - 1].rstrip() + "…"
    if len(s) <= _SHORT_MAX:
        return s
    return s[: _SHORT_MAX - 1].rstrip() + "…"


def parse_day(raw) -> Optional[str]:
    """ISO-дата/таймстамп → «YYYY-MM-DD», иначе None (запись будет пропущена)."""
    if not raw or not isinstance(raw, str):
        return None
    head = raw.strip()[:10]
    if len(head) < 10:
        return None
    try:
        datetime.strptime(head, "%Y-%m-%d")
    except ValueError:
        return None
    return head


def aggregate_events(
    entries: list[dict],
    *,
    from_day: str,
    to_day: str,
    object_id: Optional[str] = None,
) -> list[dict]:
    """Свести записи лога в события графика.

    Группировка по (engId, день, проект): дубли-решения по одному проекту в
    один день схлопываются в одно событие. Диапазон [from_day, to_day]
    включительно (строки YYYY-MM-DD сравниваются лексикографически).
    """
    obj_filter = (object_id or "").strip() or None
    out: dict[tuple, dict] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        reviewer = (e.get("expert_reviewer") or "").strip()
        if not reviewer or reviewer.lower() in _SYSTEM_REVIEWERS:
            continue
        day = parse_day(e.get("expert_date"))
        if not day:
            continue
        if not (from_day <= day <= to_day):
            continue
        obj = (e.get("object_id") or "").strip()
        if obj_filter and obj != obj_filter:
            continue
        proj = (e.get("source_project") or "").strip()
        eid = eng_slug(reviewer)
        gkey = (eid, day, proj)
        if gkey in out:
            continue
        out[gkey] = {
            "engId": eid,
            "engineerName": reviewer,
            "date": day,
            "key": day,
            "short": short_name(proj),
            "full": proj,
            "source_project": proj,
            "section": (e.get("section") or "").strip(),
            "object_id": obj,
        }
    events = list(out.values())
    events.sort(key=lambda ev: (ev["date"], ev["engineerName"].lower(), ev["short"].lower()))
    return events


def build_engineers(events: list[dict], *, users: Optional[list[dict]] = None) -> list[dict]:
    """Список инженеров, у которых есть события в периоде (без «пустых» строк).

    Роль берётся из users API по совпадению имени (best-effort); если совпадения
    нет — по умолчанию «expert». engId всегда из ФИО (стабилен).
    """
    by_name = {}
    for u in (users or []):
        nm = (u.get("name") or "").strip().lower()
        if nm:
            by_name[nm] = u
    seen: dict[str, dict] = {}
    for ev in events:
        eid = ev["engId"]
        if eid in seen:
            continue
        name = ev["engineerName"]
        matched = by_name.get(name.strip().lower())
        seen[eid] = {
            "id": eid,
            "name": name,
            "role": (matched.get("role") if matched else None) or "expert",
        }
    return sorted(seen.values(), key=lambda x: x["name"].lower())


def build_schedule(
    entries: list[dict],
    *,
    from_day: str,
    to_day: str,
    object_id: Optional[str] = None,
    users: Optional[list[dict]] = None,
) -> dict:
    """Чистая сборка payload графика из уже загруженных записей лога."""
    events = aggregate_events(entries, from_day=from_day, to_day=to_day, object_id=object_id)
    engineers = build_engineers(events, users=users)
    return {
        "events": events,
        "engineers": engineers,
        "period": {"from": from_day, "to": to_day},
    }


def load_decisions_log() -> tuple[list, Optional[str]]:
    """Безопасное чтение decisions_log.json → (entries, warning).

    Нет файла → ([], None). Битый JSON / ошибка чтения → ([], warning), без
    падения приложения.
    """
    if not DECISIONS_LOG_FILE.exists():
        return [], None
    try:
        raw = DECISIONS_LOG_FILE.read_text(encoding="utf-8")
    except OSError as e:
        return [], f"Не удалось прочитать decisions_log.json: {e}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return [], "decisions_log.json повреждён (невалидный JSON) — показаны пустые данные"
    if isinstance(data, dict):
        entries = data.get("entries", [])
    elif isinstance(data, list):
        entries = data
    else:
        entries = []
    if not isinstance(entries, list):
        entries = []
    return entries, None


def get_schedule(from_day: str, to_day: str, object_id: Optional[str] = None) -> dict:
    """Полный payload графика: читает лог + users, агрегирует, добавляет warning."""
    entries, warning = load_decisions_log()
    try:
        import backend.app.services.common.user_service as user_service
        users = user_service.list_users()
    except Exception:
        users = []
    payload = build_schedule(
        entries, from_day=from_day, to_day=to_day, object_id=object_id, users=users
    )
    payload["warning"] = warning
    return payload
