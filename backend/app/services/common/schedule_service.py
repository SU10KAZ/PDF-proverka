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

Агрегация: ОДИН проект = ОДНО событие. Все решения по проекту (замечания и
оптимизации, в любые дни) схлопываются в одно событие на ЕДИНСТВЕННЫЙ день, а не
размазываются по дням. День события:
  * замороженный «день завершения» из schedule_completion.json, если он есть
    (проставлен, когда эксперт разметил ВСЕ замечания и оптимизации; не меняется
    при последующих правках);
  * иначе — предварительный день: последняя активность по проекту.
Если инженер за период вёл несколько проектов — несколько событий (frontend
показывает первый проект + бейдж «+N» при совпадении дня).

Чтение лога безопасно: нет файла → пусто; битый JSON → пусто + warning.
Тяжёлый разбор большого файла роутер выполняет через asyncio.to_thread.

Не ломает user_service / get_user_activity — это отдельный read-only слой
поверх того же лога.
"""
from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime
from typing import Optional

from backend.app.core.config import DECISIONS_LOG_FILE, KNOWLEDGE_BASE_DIR

# Файлы вынесены в модульные переменные, чтобы тесты могли их подменить
# (monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", tmp)).
DECISIONS_LOG_FILE = DECISIONS_LOG_FILE
# План работ по инженерам на период (week/month) — редактируется админом.
WORK_PLANS_FILE = KNOWLEDGE_BASE_DIR / "work_plans.json"
# Замороженный «день завершения» проекта для графика. Ставится ОДИН раз, когда
# эксперт разметил ВСЕ замечания и ВСЕ оптимизации проекта, и больше не меняется
# при последующих правках. Ключ — (object_id, source_project), значение —
# YYYY-MM-DD. Пишется из knowledge_base_service.save_expert_review.
SCHEDULE_COMPLETION_FILE = KNOWLEDGE_BASE_DIR / "schedule_completion.json"

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


def canonical_project(source_project: str, section: str = "") -> str:
    """Каноническое имя проекта: убрать дисциплинарный префикс «<SECTION>/».

    Один и тот же проект иногда попадает в decisions_log под двумя написаниями
    `source_project`: каноническим `OV/13АВ-РД-ОВ2-К1 V1` и «голым»
    `13АВ-РД-ОВ2-К1 V1` (баг pid-нормализации при сохранении разметки). Для
    графика это ОДИН проект — иначе он двоится в одном дне. Срезаем ведущий
    `<section>/` (если он совпадает с разделом записи), чтобы обе формы дали
    одинаковый ключ. section берётся из самой записи, поэтому разные дисциплины
    с одинаковым «голым» именем НЕ сольются (ключ группы включает section).
    """
    pr = (source_project or "").strip()
    sec = (section or "").strip()
    if sec:
        prefix = f"{sec}/"
        if pr[: len(prefix)].lower() == prefix.lower():
            return pr[len(prefix):]
    return pr


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
    completions: Optional[dict[tuple[str, str], str]] = None,
) -> list[dict]:
    """Свести записи лога в события графика — ОДИН проект = ОДНО событие.

    Группировка по (engId, проект, объект): ВСЕ решения по одному проекту (и
    замечания, и оптимизации, в любые дни) схлопываются в одно событие, чтобы
    проект показывался строго одним днём.

    День события:
      * если для (object_id, source_project) есть замороженный день завершения
        в `completions` — берётся он (проставлен, когда разметка стала полной,
        и не двигается при последующих правках);
      * иначе — предварительный день: последняя активность по проекту
        (максимальная дата среди решений).

    Диапазон [from_day, to_day] включительно применяется к ИТОГОВОМУ дню
    события (а не к каждому решению): проект попадает в период по своему
    единственному дню. Строки YYYY-MM-DD сравниваются лексикографически.
    """
    completions = completions or {}
    obj_filter = (object_id or "").strip() or None
    groups: dict[tuple, dict] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        # Авто-перенос вердикта из предыдущей версии (decision carryover) — это
        # действие СИСТЕМЫ, а не эксперта. Такие записи несут carried_over=True и
        # служебного «ревьюера» вида «Авто-перенос из V1»; в график их не пускаем,
        # чтобы не появлялась фиктивная строка-инженер. Ручное решение по тому же
        # замечанию перезаписывает запись без carried_over и корректно покажется.
        if e.get("carried_over"):
            continue
        reviewer = (e.get("expert_reviewer") or "").strip()
        if not reviewer or reviewer.lower() in _SYSTEM_REVIEWERS:
            continue
        day = parse_day(e.get("expert_date"))
        if not day:
            continue
        obj = (e.get("object_id") or "").strip()
        if obj_filter and obj != obj_filter:
            continue
        raw_proj = (e.get("source_project") or "").strip()
        section = (e.get("section") or "").strip()
        # Каноническое имя схлопывает формы `<name>` и `<SECTION>/<name>` в один
        # проект; section в ключе не даёт слиться разным дисциплинам.
        proj = canonical_project(raw_proj, section)
        eid = eng_slug(reviewer)
        gkey = (eid, section, proj, obj)
        g = groups.get(gkey)
        if g is None:
            g = {
                "engId": eid,
                "engineerName": reviewer,
                "short": short_name(proj),
                "full": proj,
                "source_project": proj,
                "section": section,
                "object_id": obj,
                "_days": [],
                "_raw": set(),
            }
            groups[gkey] = g
        g["_days"].append(day)
        g["_raw"].add(raw_proj)

    events: list[dict] = []
    for (eid, section, proj, obj), g in groups.items():
        # Заморозку ищем по ЛЮБОЙ форме source_project проекта: встреченные в
        # записях формы + каноническая `<name>` + префиксная `<section>/<name>`
        # (стор мог зафиксировать форму, которой в этих записях уже нет). Берём
        # самый ранний зафиксированный день.
        candidate_forms = set(g["_raw"]) | {proj}
        if section:
            candidate_forms.add(f"{section}/{proj}")
        frozen_days = [
            d for form in candidate_forms
            if (d := completions.get((obj, form))) and parse_day(d)
        ]
        date = min(frozen_days) if frozen_days else (max(g["_days"]) if g["_days"] else None)
        if not date:
            continue
        if not (from_day <= date <= to_day):
            continue
        ev = {k: v for k, v in g.items() if k not in ("_days", "_raw")}
        ev["date"] = date
        ev["key"] = date
        events.append(ev)
    events.sort(key=lambda ev: (ev["date"], ev["engineerName"].lower(), ev["short"].lower()))
    return events


def count_remarks_by_engineer(
    entries: list[dict],
    *,
    from_day: str,
    to_day: str,
    object_id: Optional[str] = None,
) -> dict[str, dict]:
    """Счётчики отработанных замечаний по инженеру ЗА ПЕРИОД (по дате решения).

    В отличие от `aggregate_events` (где проект схлопывается в один замороженный
    день завершения), здесь считаем ОТДЕЛЬНЫЕ решения по их собственной дате
    `expert_date`, попадающей в [from_day, to_day]. Это отвечает смыслу «сколько
    замечаний инженер отработал за месяц» и может расходиться со столбцом ФАКТ.

    Возвращает `{eng_slug: {"agreed": N, "disagreed": M}}`, где:
      * agreed    — решений `expert_decision == "accepted"` (согласовано);
      * disagreed — решений `expert_decision == "rejected"` (не согласовано).

    ВАЖНО: считаются ОБА типа решений — и замечания (`item_type == "finding"`),
    и оптимизации (`item_type == "optimization"`); фильтра по `item_type` здесь
    НЕТ намеренно (оптимизации — такая же отработанная экспертом позиция, как и
    замечание). Не добавляйте такой фильтр без явного требования.

    Фильтры повторяют `aggregate_events`: пропускаем авто-перенос (carried_over),
    системных «ревьюеров» и записи без валидной даты. Решения с иным/пустым
    `expert_decision` в счётчики не идут (нечего показывать как «отработанное»).
    """
    obj_filter = (object_id or "").strip() or None
    counts: dict[str, dict] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        if e.get("carried_over"):
            continue
        reviewer = (e.get("expert_reviewer") or "").strip()
        if not reviewer or reviewer.lower() in _SYSTEM_REVIEWERS:
            continue
        day = parse_day(e.get("expert_date"))
        if not day or not (from_day <= day <= to_day):
            continue
        if obj_filter and (e.get("object_id") or "").strip() != obj_filter:
            continue
        decision = (e.get("expert_decision") or "").strip().lower()
        if decision == "accepted":
            key = "agreed"
        elif decision == "rejected":
            key = "disagreed"
        else:
            continue
        eid = eng_slug(reviewer)
        c = counts.get(eid)
        if c is None:
            c = {"agreed": 0, "disagreed": 0}
            counts[eid] = c
        c[key] += 1
    return counts


def build_engineers(
    events: list[dict],
    *,
    users: Optional[list[dict]] = None,
    remark_counts: Optional[dict[str, dict]] = None,
) -> list[dict]:
    """Список инженеров, у которых есть события в периоде (без «пустых» строк).

    Роль берётся из users API по совпадению имени (best-effort); если совпадения
    нет — по умолчанию «expert». engId всегда из ФИО (стабилен).

    `remark_counts` (см. `count_remarks_by_engineer`) добавляет каждому инженеру
    поля `agreed`/`disagreed` — счётчики отработанных за период замечаний. Фронт
    (`_schedRemarkCount`) подхватывает их автоматически; при отсутствии решений у
    инженера — 0/0 (а не «—»: инженер в периоде есть, просто решений нет).
    """
    by_name = {}
    for u in (users or []):
        nm = (u.get("name") or "").strip().lower()
        if nm:
            by_name[nm] = u
    remark_counts = remark_counts or {}
    seen: dict[str, dict] = {}
    for ev in events:
        eid = ev["engId"]
        if eid in seen:
            continue
        name = ev["engineerName"]
        matched = by_name.get(name.strip().lower())
        rc = remark_counts.get(eid) or {}
        seen[eid] = {
            "id": eid,
            "name": name,
            "role": (matched.get("role") if matched else None) or "expert",
            "agreed": int(rc.get("agreed", 0)),
            "disagreed": int(rc.get("disagreed", 0)),
        }
    return sorted(seen.values(), key=lambda x: x["name"].lower())


def build_schedule(
    entries: list[dict],
    *,
    from_day: str,
    to_day: str,
    object_id: Optional[str] = None,
    users: Optional[list[dict]] = None,
    completions: Optional[dict[tuple[str, str], str]] = None,
) -> dict:
    """Чистая сборка payload графика из уже загруженных записей лога."""
    events = aggregate_events(
        entries, from_day=from_day, to_day=to_day, object_id=object_id, completions=completions
    )
    remark_counts = count_remarks_by_engineer(
        entries, from_day=from_day, to_day=to_day, object_id=object_id
    )
    engineers = build_engineers(events, users=users, remark_counts=remark_counts)
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
    """Полный payload графика: читает лог + users + замороженные дни, агрегирует."""
    entries, warning = load_decisions_log()
    completions = load_schedule_completions()
    try:
        import backend.app.services.common.user_service as user_service
        users = user_service.list_users()
    except Exception:
        users = []
    payload = build_schedule(
        entries, from_day=from_day, to_day=to_day, object_id=object_id,
        users=users, completions=completions,
    )
    payload["warning"] = warning
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Замороженный день завершения проекта (schedule_completion.json)
#
# Когда эксперт разметил ВСЕ замечания И ВСЕ оптимизации проекта, день
# завершения фиксируется здесь ОДИН раз. При последующих правках разметки день
# НЕ меняется — проект остаётся в графике на дне завершения. Ключ —
# (object_id, source_project). Стор производный (восстановим из лога), поэтому
# при повреждении просто перезаписываем, без бэкап-плясок.
# ─────────────────────────────────────────────────────────────────────────────

# Отдельный лок (не делим с work_plans), чтобы PUT плана и штамп завершения не
# сериализовались друг через друга без нужды.
_COMPLETION_LOCK = threading.Lock()


def _completion_key(object_id, source_project) -> tuple[str, str]:
    return ((object_id or "").strip(), (source_project or "").strip())


def load_schedule_completions() -> dict[tuple[str, str], str]:
    """{(object_id, source_project): 'YYYY-MM-DD'}. Нет файла/битый → {}.

    Никогда не бросает: график не должен падать из-за производного стора.
    """
    if not SCHEDULE_COMPLETION_FILE.exists():
        return {}
    try:
        data = json.loads(SCHEDULE_COMPLETION_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    items = data.get("completions") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {}
    out: dict[tuple[str, str], str] = {}
    for rec in items:
        if not isinstance(rec, dict):
            continue
        day = parse_day(rec.get("date"))
        if not day:
            continue
        out[_completion_key(rec.get("object_id"), rec.get("source_project"))] = day
    return out


def set_completion_once(*, object_id, source_project, date, reviewer: str = "") -> dict:
    """Зафиксировать день завершения проекта ОДИН раз (идемпотентно).

    Если для (object_id, source_project) запись уже есть — НЕ перезаписываем
    (требование: день не меняется при правках). Возвращает
    {'date': YYYY-MM-DD|None, 'created': bool}. Read-modify-write под локом.
    """
    day = parse_day(date)
    if not day:
        return {"date": None, "created": False}
    key = _completion_key(object_id, source_project)
    with _COMPLETION_LOCK:
        items: list[dict] = []
        if SCHEDULE_COMPLETION_FILE.exists():
            try:
                data = json.loads(SCHEDULE_COMPLETION_FILE.read_text(encoding="utf-8"))
                if isinstance(data, dict) and isinstance(data.get("completions"), list):
                    items = [r for r in data["completions"] if isinstance(r, dict)]
            except (OSError, json.JSONDecodeError):
                items = []  # битый стор перезапишем (он производный)
        for rec in items:
            if _completion_key(rec.get("object_id"), rec.get("source_project")) == key:
                return {"date": parse_day(rec.get("date")) or day, "created": False}
        now = datetime.now().isoformat(timespec="seconds")
        items.append({
            "object_id": key[0],
            "source_project": key[1],
            "date": day,
            "reviewer": reviewer or "",
            "set_at": now,
        })
        _atomic_write_json(
            SCHEDULE_COMPLETION_FILE,
            {"version": 1, "updated_at": now, "completions": items},
        )
    return {"date": day, "created": True}


# ─────────────────────────────────────────────────────────────────────────────
# План работ (work_plans.json) — отдельный редактируемый стор
#
# Ключ плана: (period_type, period_start, period_end, object_id, engineer_id).
# PUT обновляет планы ТОЛЬКО для своего периода, не затирая чужие. Запись
# атомарная (tmp + replace). Битый JSON: GET → пусто + warning; PUT → бэкап
# повреждённого файла, новый период всё равно сохраняется.
# ─────────────────────────────────────────────────────────────────────────────

PLAN_MIN = 0
PLAN_MAX = 999

# Сериализует read-modify-write work_plans.json: save_plans вызывается из роутера
# через asyncio.to_thread, поэтому два PUT идут в разных потоках threadpool.
# Без этого — гонка за общий файл (lost-update + краш на общем tmp).
_WRITE_LOCK = threading.Lock()


def _empty_plans_doc() -> dict:
    return {"version": 1, "updated_at": None, "plans": []}


def _coerce_plan(v) -> int:
    """Привести plan к int в диапазоне [PLAN_MIN, PLAN_MAX]. Не бросает.

    HTTP-путь валидирует через pydantic, но save_plans — публичная функция,
    поэтому защищаемся и на уровне сервиса.
    """
    try:
        n = int(v)
    except (TypeError, ValueError):
        try:
            n = int(float(v))
        except (TypeError, ValueError):
            return PLAN_MIN
    return max(PLAN_MIN, min(PLAN_MAX, n))


def _norm_obj(object_id) -> Optional[str]:
    """Нормализация object_id: пустая строка/None → None (единый ключ)."""
    if not isinstance(object_id, str):
        return None
    v = object_id.strip()
    return v or None


def _plan_period_match(p: dict, *, period_type, period_start, period_end, object_id) -> bool:
    return (
        p.get("period_type") == period_type
        and p.get("period_start") == period_start
        and p.get("period_end") == period_end
        and _norm_obj(p.get("object_id")) == _norm_obj(object_id)
    )


def _plan_public(p: dict) -> dict:
    """Публичная проекция записи плана для ответа API."""
    return {
        "engineer_id": p.get("engineer_id"),
        "engineer_name": p.get("engineer_name", ""),
        "plan": p.get("plan"),
        "period_type": p.get("period_type"),
        "period_start": p.get("period_start"),
        "period_end": p.get("period_end"),
        "object_id": _norm_obj(p.get("object_id")),
    }


def load_work_plans() -> tuple[dict, Optional[str]]:
    """(doc, warning). Нет файла → пустой doc, None. Битый JSON → пустой doc + warning.

    Никогда не бросает — приложение не должно падать из-за повреждённого файла.
    """
    if not WORK_PLANS_FILE.exists():
        return _empty_plans_doc(), None
    try:
        raw = WORK_PLANS_FILE.read_text(encoding="utf-8")
    except OSError as e:
        return _empty_plans_doc(), f"Не удалось прочитать work_plans.json: {e}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _empty_plans_doc(), "work_plans.json повреждён (невалидный JSON) — показан пустой план"
    if not isinstance(data, dict):
        return _empty_plans_doc(), "work_plans.json имеет неожиданный формат — показан пустой план"
    plans = data.get("plans")
    if "plans" in data and not isinstance(plans, list):
        # структурно-валидный JSON, но plans не список — не молчим, чтобы не
        # выглядело как «планов нет».
        return _empty_plans_doc(), "work_plans.json: поле plans не является списком — показан пустой план"
    data["plans"] = [p for p in plans if isinstance(p, dict)] if isinstance(plans, list) else []
    data.setdefault("version", 1)
    data.setdefault("updated_at", None)
    return data, None


def _atomic_write_json(path, data) -> None:
    """Атомарная запись JSON: уникальный tmp в той же папке → replace.

    Имя tmp уникально на вызов (pid+uuid), чтобы параллельные писатели не
    делили один временный файл. tmp чистится только при сбое (на успехе он
    поглощён replace).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def get_plans(*, period_type: str, period_start: str, period_end: str,
              object_id: Optional[str] = None) -> dict:
    """Планы для конкретного периода (week/month + даты + object_id)."""
    doc, warning = load_work_plans()
    out = [
        _plan_public(p)
        for p in doc.get("plans", [])
        if _plan_period_match(
            p, period_type=period_type, period_start=period_start,
            period_end=period_end, object_id=object_id,
        )
    ]
    return {
        "plans": out,
        "period": {"from": period_start, "to": period_end, "period_type": period_type},
        "warning": warning,
    }


def save_plans(*, period_type: str, period_start: str, period_end: str,
               object_id: Optional[str], plans: list[dict],
               updated_by: Optional[str] = None) -> dict:
    """Сохранить планы для одного периода, не затирая другие периоды.

    `plans`: список {engineer_id, engineer_name, plan}. Семантика — merge внутри
    периода: обновляются только присланные engineer_id, остальные записи этого
    периода сохраняются (план инженера не теряется, если его нет в запросе).
    Другие периоды не затрагиваются. Битый/структурно-неверный файл бэкапится
    (байты сохраняются), затем пишется заново. Read-modify-write сериализован
    глобальным локом (защита от lost-update при параллельных PUT).
    """
    # Дедуп входа по engineer_id (последний выигрывает) + устойчивое приведение plan.
    by_eng: dict[str, dict] = {}
    for item in plans:
        eid = str(item.get("engineer_id") or "").strip()
        if not eid:
            continue
        by_eng[eid] = {
            "engineer_name": str(item.get("engineer_name") or ""),
            "plan": _coerce_plan(item.get("plan")),
        }

    with _WRITE_LOCK:
        warning: Optional[str] = None
        data: Optional[dict] = None
        if WORK_PLANS_FILE.exists():
            try:
                parsed = json.loads(WORK_PLANS_FILE.read_text(encoding="utf-8"))
                # plans не список — тоже считаем повреждением, иначе планы молча
                # потерялись бы.
                if not isinstance(parsed, dict) or not isinstance(parsed.get("plans", []), list):
                    raise ValueError("unexpected shape")
                data = parsed
            except (json.JSONDecodeError, ValueError, OSError):
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                backup = WORK_PLANS_FILE.with_name(f"work_plans.json.broken-{ts}")
                try:
                    WORK_PLANS_FILE.replace(backup)  # сохраняем повреждённые байты
                    warning = f"Предыдущий work_plans.json был повреждён, сохранён бэкап: {backup.name}"
                except OSError:
                    warning = "Предыдущий work_plans.json был повреждён (бэкап не удался)"
                data = _empty_plans_doc()
        if data is None:
            data = _empty_plans_doc()

        existing = data.get("plans")
        if not isinstance(existing, list):
            existing = []

        # Делим существующие записи на: другие периоды (kept) и этот период.
        kept, this_period = [], []
        for p in existing:
            if not isinstance(p, dict):
                continue
            if _plan_period_match(p, period_type=period_type, period_start=period_start,
                                  period_end=period_end, object_id=object_id):
                this_period.append(p)
            else:
                kept.append(p)

        # Записи этого периода для инженеров, которых НЕТ в запросе — сохраняем.
        preserved = [
            p for p in this_period
            if str(p.get("engineer_id") or "").strip() not in by_eng
        ]

        now = datetime.now().isoformat(timespec="seconds")
        obj = _norm_obj(object_id)
        new_records, saved = [], []
        for eid, item in by_eng.items():
            rec = {
                "period_type": period_type,
                "period_start": period_start,
                "period_end": period_end,
                "object_id": obj,
                "engineer_id": eid,
                "engineer_name": item["engineer_name"],
                "plan": item["plan"],
                "updated_by": updated_by or "",
                "updated_at": now,
            }
            new_records.append(rec)
            saved.append(_plan_public(rec))

        data["version"] = 1
        data["updated_at"] = now
        data["plans"] = kept + preserved + new_records
        _atomic_write_json(WORK_PLANS_FILE, data)

    return {
        "plans": saved,
        "period": {"from": period_start, "to": period_end, "period_type": period_type},
        "warning": warning,
    }
