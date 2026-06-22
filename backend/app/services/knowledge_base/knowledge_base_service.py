"""
Сервис базы знаний — сбор экспертных решений, хранение, анализ паттернов.
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from backend.app.core.config import KNOWLEDGE_BASE_DIR, DECISIONS_LOG_FILE, PATTERNS_FILE
from backend.app.models.expert_review import (
    ExpertDecision, KnowledgeBaseEntry, PatternSuggestion,
)
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.storage.projects_v2_source_resolver import load_version_project_info


def _version_dir(project_id: str, *, must_exist: bool = False) -> Path:
    """Активная версия проекта (из ContextVar bound_version_id, fallback на latest).

    must_exist=True: для writer-ов — `resolve_project_dir` бросит
    `ProjectNotResolvedError`, если project_id не резолвится в реальную папку
    (вместо возврата несуществующего пути на корне объекта → orphan)."""
    project_dir = resolve_project_dir(project_id, must_exist=must_exist)
    vid = version_service.get_bound_version_id()
    try:
        return version_service.get_version_dir(project_dir, project_id, vid)
    except version_service.VersionNotFoundError:
        return project_dir


def _output_dir(project_id: str, *, must_exist: bool = False) -> Path:
    return _version_dir(project_id, must_exist=must_exist) / "_output"


def _ensure_kb_dir():
    """Создать папку knowledge_base/ если не существует."""
    KNOWLEDGE_BASE_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


# ═══════════════════════════════════════════════════════════════════════════
# Чтение / запись JSON
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _save_json(path: Path, data):
    # Атомарная потокобезопасная запись: decisions_log/patterns — shared-стораж
    # на 140 проектов; plain open('w') рвался при крахе/гонке (reserc.md #7/#81/#87).
    _ensure_kb_dir()
    from backend.app.services.common.atomic_json import atomic_write_json
    atomic_write_json(path, data)


# ═══════════════════════════════════════════════════════════════════════════
# Экспертная оценка (per-project)
# ═══════════════════════════════════════════════════════════════════════════

def save_expert_review(project_id: str, decisions: list[ExpertDecision], reviewer: str = "", removed_ids: list[str] | None = None) -> dict:
    """Сохранить решения эксперта по проекту.

    1. Записывает expert_review.json в _output/ проекта
    2. Обогащает решения контекстом из findings/optimization
    3. Добавляет записи в глобальный decisions_log.json
    """
    # must_exist=True: не создаём orphan `_output` на несуществующем пути —
    # если project_id не резолвится в реальный проект/контейнер, поднимется
    # ProjectNotResolvedError (роутер вернёт 404).
    output_dir = _output_dir(project_id, must_exist=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Сохранить per-project файл (merge с существующими решениями)
    review_path = output_dir / "expert_review.json"
    existing = _load_json(review_path)
    existing_decisions = []
    if existing and "decisions" in existing:
        existing_decisions = existing["decisions"]

    # Новые решения перезаписывают старые по item_id; removed_ids удаляются
    new_ids = {d.item_id for d in decisions}
    excluded_ids = new_ids | set(removed_ids or [])
    merged = [d for d in existing_decisions if d.get("item_id") not in excluded_ids]
    merged.extend([d.model_dump() for d in decisions])

    review_data = {
        "project_id": project_id,
        "reviewer": reviewer,
        "reviewed_at": _now_iso(),
        "decisions": merged,
    }
    _save_json(review_path, review_data)

    # 2. Обогатить и добавить в глобальный лог
    enriched = _enrich_decisions(project_id, decisions, reviewer)
    _append_to_decisions_log(enriched)

    # Step 9/10 dual-write canary: shadow-зеркало проекта в v2 после сохранения
    # expert_review.json (no-op в legacy, fail-soft). decisions_log остаётся
    # общим shared-файлом (его v2-плечо здесь намеренно НЕ форкается).
    try:
        from backend.app.services.storage import storage_write_facade as _swf
        _swf.shadow_mirror_project_id_safe(project_id)
    except Exception:  # noqa: BLE001 — fail-soft, но #91: не молчим (наблюдаемость)
        import logging
        logging.getLogger(__name__).debug(
            "shadow_mirror_project_id_safe failed for %s", project_id, exc_info=True
        )

    return {
        "saved": len(decisions),
        "accepted": sum(1 for d in decisions if d.decision == "accepted"),
        "rejected": sum(1 for d in decisions if d.decision == "rejected"),
    }


def load_expert_review(project_id: str) -> Optional[dict]:
    """Загрузить сохранённые решения эксперта для проекта."""
    path = _output_dir(project_id) / "expert_review.json"
    return _load_json(path)


def _enrich_decisions(project_id: str, decisions: list[ExpertDecision], reviewer: str) -> list[KnowledgeBaseEntry]:
    """Обогатить решения контекстом из findings/optimization JSON."""
    output_dir = _output_dir(project_id)

    # Загрузить findings
    findings_map = {}
    for fname in ["03a_norms_verified.json", "03_findings.json"]:
        fpath = output_dir / fname
        fdata = _load_json(fpath)
        if fdata:
            for item in fdata.get("findings", fdata.get("items", [])):
                findings_map[item.get("id", "")] = item
            break

    # Загрузить optimization
    opt_map = {}
    opt_data = _load_json(output_dir / "optimization.json")
    if opt_data:
        for item in opt_data.get("items", []):
            opt_map[item.get("id", "")] = item

    # Загрузить project_info для section (из активной версии, fallback на корень)
    version_dir = _version_dir(project_id)
    info = load_version_project_info(version_dir) or {}
    if not info:
        info = _load_json(resolve_project_dir(project_id) / "project_info.json") or {}
    section = info.get("section", "")

    # Объект (здание/комплекс): из bound-контекста, иначе текущий выбранный.
    object_id = ""
    try:
        from backend.app.services.common import object_service, project_service
        object_id = project_service._get_bound_object_id() or object_service.get_current_id() or ""
    except Exception:
        object_id = ""

    # Следующий ID (монотонный max+1, НЕ len+1 — см. _next_decision_num)
    existing_log = _load_decisions_log()
    next_num = _next_decision_num(existing_log)

    entries = []
    for dec in decisions:
        source = findings_map.get(dec.item_id) or opt_map.get(dec.item_id) or {}

        # Извлечь norm_refs
        norm_refs = []
        norm = source.get("norm", source.get("norm_ref", ""))
        if norm:
            norm_refs = [norm] if isinstance(norm, str) else norm

        entry = KnowledgeBaseEntry(
            id=f"DEC-{next_num:04d}",
            object_id=object_id,
            source_project=project_id,
            section=section,
            item_id=dec.item_id,
            item_type=dec.item_type,
            severity=source.get("severity", ""),
            category=source.get("category", ""),
            summary=source.get("problem", source.get("description", source.get("summary", ""))),
            norm_refs=norm_refs,
            sheet=str(source.get("sheet", "")),
            page=source.get("page"),
            expert_decision=dec.decision,
            expert_reason=dec.rejection_reason or "",
            expert_reviewer=dec.reviewer or reviewer,
            expert_date=dec.timestamp or _now_iso(),
            customer_response=(source.get("external_register") or {}).get("customer_response", ""),
        )
        entries.append(entry)
        next_num += 1

    return entries


# ═══════════════════════════════════════════════════════════════════════════
# Глобальный лог решений (knowledge_base/decisions_log.json)
# ═══════════════════════════════════════════════════════════════════════════

def _load_decisions_log() -> list[dict]:
    data = _load_json(DECISIONS_LOG_FILE)
    if isinstance(data, dict):
        return data.get("entries", [])
    if isinstance(data, list):
        return data
    return []


def _save_decisions_log(entries: list[dict]):
    _save_json(DECISIONS_LOG_FILE, {"entries": entries})


def _next_decision_num(existing_log: list[dict]) -> int:
    """Следующий монотонный номер для DEC-NNNN: max(существующих) + 1.

    Раньше было len(log)+1 → после revoke номер переиспользовался, и один id
    указывал на десятки решений (audit: 1208 дублей id, 3122 записи). max+1
    гарантирует глобальную уникальность НОВЫХ id даже при пробелах от revoke
    (reserc.md #79/#100). Существующие коллизии лечит мигратор #82 (шаг 28).
    """
    import re
    mx = 0
    for e in existing_log:
        m = re.match(r"DEC-(\d+)$", str(e.get("id") or ""))
        if m:
            mx = max(mx, int(m.group(1)))
    return mx + 1


def _append_to_decisions_log(new_entries: list[KnowledgeBaseEntry]):
    """Добавить записи в глобальный лог (дедупликация по project+item_id)."""
    existing = _load_decisions_log()
    existing_keys = {(e.get("source_project"), e.get("item_id")) for e in existing}

    # Обновить существующие или добавить новые
    updated_map = {(e.get("source_project"), e.get("item_id")): e for e in existing}
    for entry in new_entries:
        key = (entry.source_project, entry.item_id)
        updated_map[key] = entry.model_dump()

    _save_decisions_log(list(updated_map.values()))


def get_knowledge_base(
    status: Optional[str] = None,
    section: Optional[str] = None,
    item_type: Optional[str] = None,
    search: Optional[str] = None,
    object_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Получить записи базы знаний с фильтрацией."""
    entries = _load_decisions_log()

    # Вычислить status для каждой записи
    for e in entries:
        e["status"] = _entry_status(e)

    # Фильтрация
    if object_id:
        entries = [e for e in entries if e.get("object_id") == object_id]
    if status:
        entries = [e for e in entries if e.get("status") == status]
    if section:
        entries = [e for e in entries if e.get("section", "").upper() == section.upper()]
    if item_type:
        entries = [e for e in entries if e.get("item_type") == item_type]
    if search:
        s = search.lower()
        entries = [e for e in entries if s in json.dumps(e, ensure_ascii=False).lower()]

    total = len(entries)

    # Сортировка по дате (новые первые)
    entries.sort(key=lambda e: e.get("expert_date", ""), reverse=True)

    # Пагинация
    paginated = entries[offset:offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "entries": paginated,
    }


def _entry_status(e: dict) -> str:
    """Статус записи для вкладок KB.

    customer_confirmed + ответ заказчика «Внесено» → fixed_by_customer
    (заказчик внёс изменения в РД); остальные согласованные → customer_confirmed.
    """
    if e.get("customer_confirmed"):
        if (e.get("customer_response") or "").strip() == "Внесено":
            return "fixed_by_customer"
        return "customer_confirmed"
    return e.get("expert_decision", "")


def get_kb_stats(object_id: Optional[str] = None) -> dict:
    """Счётчики по вкладкам (опционально в рамках одного объекта)."""
    entries = _load_decisions_log()
    if object_id:
        entries = [e for e in entries if e.get("object_id") == object_id]
    stats = {"rejected": 0, "accepted": 0, "customer_confirmed": 0, "fixed_by_customer": 0}
    for e in entries:
        st = _entry_status(e)
        if st in stats:
            stats[st] += 1
    stats["total"] = sum(stats.values())
    return stats


def mark_customer_confirmed(entry_ids: list[str], note: str = "") -> int:
    """Отметить записи как согласованные заказчиком."""
    entries = _load_decisions_log()
    count = 0
    now = _now_iso()
    for e in entries:
        if e.get("id") in entry_ids and e.get("expert_decision") == "accepted":
            e["customer_confirmed"] = True
            e["customer_date"] = now
            if note:
                e["customer_note"] = note
            count += 1
    _save_decisions_log(entries)
    return count


def unmark_customer_confirmed(entry_ids: list[str]) -> int:
    """Снять отметку согласования заказчиком."""
    entries = _load_decisions_log()
    count = 0
    for e in entries:
        if e.get("id") in entry_ids and e.get("customer_confirmed"):
            e["customer_confirmed"] = False
            e["customer_date"] = None
            e["customer_note"] = None
            count += 1
    _save_decisions_log(entries)
    return count


def _find_project_dir(project_id: str) -> Optional[Path]:
    """Найти папку проекта — пробует resolve_project_dir, fallback через iter_project_dirs."""
    try:
        return resolve_project_dir(project_id)
    except Exception:
        pass
    # Fallback: поиск по имени
    try:
        from backend.app.services.common.project_service import iter_project_dirs
        for pid, path in iter_project_dirs():
            if pid == project_id or pid.endswith("/" + project_id):
                return path
    except Exception:
        pass
    return None


def revoke_decision(entry_id: str, project_id: str, item_id: str) -> int:
    """Отменить решение — удалить из глобального лога и из expert_review проекта.

    Адресуем по УНИКАЛЬНОМУ составному ключу (source_project, item_id): id
    (DEC-NNNN) переиспользуется и один id = десятки записей в разных проектах,
    поэтому revoke по id сносил чужие решения (reserc.md #80/#86; audit: 0
    коллизий составного ключа). Если составного ключа нет — удаляем по id только
    при его уникальности, иначе отказ.
    """
    import logging
    # 1. Удалить из decisions_log.json
    entries = _load_decisions_log()
    before = len(entries)

    if project_id and item_id:
        def _match(e: dict) -> bool:
            return e.get("source_project") == project_id and e.get("item_id") == item_id
    else:
        id_hits = [e for e in entries if e.get("id") == entry_id]
        if len(id_hits) != 1:
            logging.getLogger(__name__).warning(
                "revoke_decision: id=%r неуникален (%d записей) и нет составного "
                "ключа — отказ, чтобы не удалить чужие решения",
                entry_id, len(id_hits),
            )
            return 0

        def _match(e: dict) -> bool:
            return e.get("id") == entry_id

    kept = [e for e in entries if not _match(e)]
    removed = before - len(kept)
    if removed > 1:
        # Составной ключ обязан быть уникальным; >1 = повреждение данных →
        # не сохраняем разрушительное удаление.
        logging.getLogger(__name__).warning(
            "revoke_decision: совпало %d записей по (%s,%s)/id=%s — ожидалась ≤1; "
            "пропуск без удаления", removed, project_id, item_id, entry_id,
        )
        return 0
    _save_decisions_log(kept)

    # 2. Удалить из expert_review.json проекта (активная версия)
    if project_id and item_id:
        project_dir = _find_project_dir(project_id)
        if project_dir:
            review_path = _output_dir(project_id) / "expert_review.json"
            if review_path.exists():
                review_data = _load_json(review_path)
                if review_data and "decisions" in review_data:
                    review_data["decisions"] = [
                        d for d in review_data["decisions"]
                        if d.get("item_id") != item_id
                    ]
                    _save_json(review_path, review_data)

    return removed


# ═══════════════════════════════════════════════════════════════════════════
# Детекция паттернов из отклонённых решений
# ═══════════════════════════════════════════════════════════════════════════

def _load_patterns() -> list[dict]:
    data = _load_json(PATTERNS_FILE)
    if isinstance(data, dict):
        return data.get("patterns", [])
    if isinstance(data, list):
        return data
    return []


def _save_patterns(patterns: list[dict]):
    _save_json(PATTERNS_FILE, {"patterns": patterns})


def detect_patterns(min_frequency: int = 3) -> list[dict]:
    """Найти повторяющиеся паттерны среди отклонённых решений.

    Группирует по (section, category, norm_prefix) и ищет кластеры с >= min_frequency.
    """
    entries = _load_decisions_log()
    rejected = [e for e in entries if e.get("expert_decision") == "rejected"]

    if not rejected:
        return []

    # Группировка по (section, category)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for e in rejected:
        section = e.get("section", "").upper()
        category = e.get("category", "").lower()
        # Извлечь prefix нормы (до пункта)
        norm_prefix = ""
        norms = e.get("norm_refs", [])
        if norms:
            norm_prefix = norms[0].split(",")[0].split("п.")[0].strip()
        key = (section, category, norm_prefix)
        groups[key].append(e)

    # Фильтр по частоте
    existing_patterns = _load_patterns()
    existing_ids = {p.get("pattern_id") for p in existing_patterns}
    next_num = len(existing_patterns) + 1

    new_patterns = []
    for (section, category, norm_prefix), items in groups.items():
        if len(items) < min_frequency:
            continue

        # Собрать уникальные причины отклонения
        reasons = [e.get("expert_reason", "") for e in items if e.get("expert_reason")]
        common_reason = reasons[0] if reasons else "Повторяющееся отклонение"

        # Уникальные проекты
        projects = list({e.get("source_project", "") for e in items})
        example_ids = [e.get("id", "") for e in items[:5]]

        pattern_id = f"PAT-{next_num:03d}"
        # Проверить что не дублирует существующий
        desc = f"[{section}] Категория '{category}'"
        if norm_prefix:
            desc += f", норма {norm_prefix}"
        desc += f" — {len(items)} отклонений"

        # Пропустить если уже есть паттерн с такой же description
        if any(p.get("description") == desc for p in existing_patterns):
            continue

        suggested_fix = f"Не генерировать замечания типа '{category}'"
        if norm_prefix:
            suggested_fix += f" со ссылкой на {norm_prefix}"
        suggested_fix += f". Причина: {common_reason}"

        target_file = f"disciplines/{section}/checklist.md" if section else ""

        new_patterns.append({
            "pattern_id": pattern_id,
            "section": section,
            "description": desc,
            "frequency": len(items),
            "projects_affected": projects,
            "example_ids": example_ids,
            "suggested_fix": suggested_fix,
            "target_file": target_file,
            "status": "pending",
            "proposed_at": _now_iso(),
            "decided_by": None,
            "decided_at": None,
        })
        next_num += 1

    # Сохранить новые + существующие
    if new_patterns:
        all_patterns = existing_patterns + new_patterns
        _save_patterns(all_patterns)

    return _load_patterns()


def get_patterns() -> list[dict]:
    """Получить все паттерны."""
    return _load_patterns()


def update_pattern_status(pattern_id: str, status: str, edited_fix: Optional[str] = None, decided_by: str = "") -> bool:
    """Обновить статус паттерна (approve/dismiss/edit)."""
    patterns = _load_patterns()
    for p in patterns:
        if p.get("pattern_id") == pattern_id:
            p["status"] = status
            p["decided_at"] = _now_iso()
            p["decided_by"] = decided_by
            if edited_fix is not None:
                p["suggested_fix"] = edited_fix
            _save_patterns(patterns)
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# Импорт решений из Excel
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_project_id_from_sheet(ws_title: str) -> str:
    """Найти полный project_id по сокращённому имени листа Excel.

    Имя листа может быть '133_23-ГК-ГРЩ', а нужен 'EOM/133_23-ГК-ГРЩ'.
    """
    from backend.app.services.common.project_service import iter_project_dirs

    # Убрать префикс "ОПТ " если есть
    name = ws_title
    if name.startswith("ОПТ "):
        name = name[4:]

    for pid, path in iter_project_dirs():
        # pid может быть "133_23-ГК-ГРЩ" или "EOM/133_23-ГК-ГРЩ"
        if pid == name or pid.endswith("/" + name) or pid.replace("/", "-") == name:
            return pid
    return name  # fallback — вернуть как есть


def import_decisions_from_excel(file_path: str, default_project_id: Optional[str] = None) -> dict:
    """Импортировать решения из Excel-файла с колонками 'Решение эксперта' и 'Причина отклонения'.

    Возвращает {project_id: {saved, accepted, rejected}} для каждого обнаруженного проекта.

    `default_project_id` — fallback из UI-контекста, когда в Excel ни скрытая
    ячейка, ни имя листа не дают валидный project_id (старые экспорты для V2
    клали в скрытую ячейку "v2" вместо реального ID).
    """
    import openpyxl

    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    results = {}

    for ws in wb.worksheets:
        if ws.title.upper() in ("ИНСТРУКЦИЯ", "СВОДКА", "SUMMARY"):
            continue

        # Найти колонки по заголовку (строка 1)
        headers = {}
        header_row = list(next(ws.iter_rows(min_row=1, max_row=1), []))
        for col_idx, c in enumerate(header_row):
            val = str(c.value or "").strip().lower()
            if val == "решение эксперта" or ("решение" in val and "эксперт" in val):
                headers["decision"] = col_idx
            elif "причина" in val and "отклон" in val:
                headers["reason"] = col_idx
            elif val == "id":
                headers["id"] = col_idx
            elif val == "№" and "id" not in headers:
                headers["num"] = col_idx  # fallback: если нет ID, используем №
            elif val == "project_id":
                headers["project_id"] = col_idx
            elif "тип" in val:
                headers["type"] = col_idx

        # Если нет колонки ID, но есть № — не подходит (в № порядковый номер, а не F-001)
        # Для листов оптимизации колонка ID есть всегда

        if "decision" not in headers or "id" not in headers:
            continue

        # Определить project_id: скрытый столбец (строка 2) → имя листа → fallback.
        # Защита: если в скрытой ячейке или в имени листа лежит просто метка
        # версии ("v1"/"v2"/…) — это не project_id (старые экспорты для V2
        # клали туда basename папки = "v2"). В таких случаях используем
        # default_project_id, переданный из UI-контекста (currentProjectId).
        import re as _re
        def _looks_like_version(s: str) -> bool:
            return bool(s and _re.fullmatch(r"v\d+", s.strip(), flags=_re.IGNORECASE))

        def _pid_resolves(pid: str) -> bool:
            """project_id указывает на реальную папку проекта/контейнер."""
            if not pid:
                return False
            try:
                from backend.app.services.common.project_service import (
                    resolve_project_dir,
                )
                resolve_project_dir(pid, must_exist=True)
                return True
            except Exception:
                return False

        def _strip_version_label(pid: str) -> str:
            """Снять хвостовую метку версии ("<id> V1"/"<id>_v2"/…).

            Версия в импорте передаётся отдельно (`version_id`), поэтому в самом
            project_id метки "V1" быть не должно. Старые/внешние экспорты иногда
            запекали в скрытую ячейку композит вида "KM/1232-ЧМ-КМ-1 V1" —
            он не резолвится (реальная папка называется "KM/1232-ЧМ-КМ-1").
            """
            return _re.sub(r"[ _-]?[vV]\d+$", "", pid).strip() if pid else pid

        # Кандидаты в порядке приоритета. Берём ПЕРВЫЙ, который реально
        # резолвится в папку проекта — это и чинит 500 «Project directory not
        # resolved» на стейл-ячейках, и сохраняет per-sheet идентичность для
        # многопроектных отчётов (валидная скрытая ячейка по-прежнему выигрывает).
        candidates: list[str] = []
        if "project_id" in headers:
            row2 = list(next(ws.iter_rows(min_row=2, max_row=2), []))
            if row2 and headers["project_id"] < len(row2):
                pid_val = str(row2[headers["project_id"]].value or "").strip()
                if pid_val and not _looks_like_version(pid_val):
                    candidates.append(pid_val)
        # Снимаем префикс "ОПТ " вручную, чтобы проверить «голое» имя
        bare_title = ws.title[4:] if ws.title.startswith("ОПТ ") else ws.title
        if not _looks_like_version(bare_title):
            resolved = _resolve_project_id_from_sheet(ws.title)
            if resolved and not _looks_like_version(resolved):
                candidates.append(resolved)
        if default_project_id:
            candidates.append(default_project_id)

        project_id = None
        # Pass 1: первый кандидат, который резолвится как есть.
        for cand in candidates:
            if _pid_resolves(cand):
                project_id = cand
                break
        # Pass 2: ни один не резолвится → пробуем снять хвостовую метку версии
        # (стейл-экспорты "<id> V1"). Принимаем только если stripped резолвится.
        if not project_id:
            for cand in candidates:
                stripped = _strip_version_label(cand)
                if stripped and stripped != cand and _pid_resolves(stripped):
                    project_id = stripped
                    break

        decisions = []
        for row in ws.iter_rows(min_row=3, values_only=False):
            cells = list(row)
            item_id = str(cells[headers["id"]].value or "").strip()
            decision_raw = str(cells[headers["decision"]].value or "").strip().lower()
            reason = ""
            if "reason" in headers:
                reason = str(cells[headers["reason"]].value or "").strip()

            if not item_id or not decision_raw:
                continue

            # Нормализация
            if decision_raw in ("принято", "accepted", "да", "yes", "+"):
                decision = "accepted"
            elif decision_raw in ("отклонено", "отклонить", "rejected", "нет", "no", "-"):
                decision = "rejected"
            else:
                continue

            # Определить тип
            item_type = "finding"
            if item_id.upper().startswith("OPT"):
                item_type = "optimization"
            if "type" in headers:
                type_val = str(cells[headers["type"]].value or "").strip().lower()
                if "opt" in type_val:
                    item_type = "optimization"

            decisions.append(ExpertDecision(
                item_id=item_id,
                item_type=item_type,
                decision=decision,
                rejection_reason=reason if decision == "rejected" else None,
                timestamp=_now_iso(),
            ))

        import logging
        _log = logging.getLogger(__name__)
        _log.warning(
            "[import-excel] sheet=%r project_id=%r decisions=%d headers=%s bound_vid=%s",
            ws.title, project_id, len(decisions), list(headers.keys()),
            version_service.get_bound_version_id(),
        )

        if decisions and project_id:
            result = save_expert_review(project_id, decisions)
            results[project_id] = result
        elif decisions and not project_id:
            _log.warning("[import-excel] decisions=%d but project_id not resolved for sheet=%r", len(decisions), ws.title)

    wb.close()
    return results
