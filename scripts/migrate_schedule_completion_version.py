#!/usr/bin/env python3
"""
migrate_schedule_completion_version.py
--------------------------------------
Проставить version-aware `version_id` СУЩЕСТВУЮЩИМ записям заморозки завершения
(knowledge_base/schedule_completion.json).

Зачем: ключ заморозки стал version-aware — (object_id, source_project, version_id).
Все старые записи безверсионные (`version_id` отсутствует → бакет ''). Пока запись
безверсионная, версионная группа графика её НЕ подхватывает и уезжает на живой
день (это и есть искомый фикс для новых версий). Но для УЖЕ завершённых версионных
проектов это ослабляет «заморозку»: при будущей правке они дрейфнут на живой день.

Миграция закрепляет version_id за теми записями, где это можно вывести НАДЁЖНО:
критерий — среди решений (object_id, source_project) ровно ОДНА непустая версия V,
чей максимальный день решений совпадает с датой заморозки. Тогда заморозка и есть
день завершения версии V → стампим V. Иначе (устаревшая дата, смешанные версии,
только легаси-None) — оставляем безверсионной, чтобы НЕ пере-запинить свежий
переаудит на старый день (регресс АР1.2-К4).

Безопасность:
  * АДДИТИВНО — только добавляет поле version_id к записям; даты/объекты/проекты
    не трогает, записи не удаляет; decisions_log не трогает вовсе;
  * ИДЕМПОТЕНТНО — записи с уже непустым version_id пропускаются;
  * DRY-RUN по умолчанию — без --apply только печатает план;
  * стор производный: при --apply пишется атомарно (tmp+replace) через
    schedule_service._atomic_write_json.

Запуск:
    python scripts/migrate_schedule_completion_version.py            # dry-run
    python scripts/migrate_schedule_completion_version.py --apply    # запись
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.schedule_service as sched  # noqa: E402


def _build_version_day_index(entries: list[dict]) -> dict[tuple[str, str], dict[str, set]]:
    """(object_id, canonical_project) → {version_id: {дни решений}}.

    Повторяет фильтры графика (carried_over / системные ревьюеры / битая дата),
    чтобы «дни версии» совпадали с тем, что реально агрегируется в события.
    """
    idx: dict[tuple[str, str], dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for e in entries:
        if not isinstance(e, dict) or e.get("carried_over"):
            continue
        reviewer = (e.get("expert_reviewer") or "").strip()
        if not reviewer or reviewer.lower() in sched._SYSTEM_REVIEWERS:
            continue
        day = sched.parse_day(e.get("expert_date"))
        if not day:
            continue
        obj = (e.get("object_id") or "").strip()
        section = (e.get("section") or "").strip()
        proj = sched.canonical_project((e.get("source_project") or "").strip(), section)
        ver = sched._norm_version(e.get("current_version_id"))
        idx[(obj, proj)][ver].add(day)
    return idx


def _canon_stored(obj: str, stored_proj: str, idx: dict) -> str:
    """Привести хранимую форму source_project (возможно с префиксом секции) к
    каноническому ключу индекса решений."""
    if (obj, stored_proj) in idx:
        return stored_proj
    for (o, p) in idx:
        if o == obj and (stored_proj.endswith("/" + p) or p == stored_proj):
            return p
    return stored_proj


def _infer_version(obj: str, stored_proj: str, freeze_day: str, idx: dict) -> str:
    """Вывести version_id для записи заморозки или '' если ненадёжно.

    Критерий: ровно одна непустая версия V, у которой max(дни решений) == freeze_day.
    """
    vers = idx.get((obj, _canon_stored(obj, stored_proj, idx)), {})
    match = [v for v, days in vers.items() if v and days and max(days) == freeze_day]
    return match[0] if len(match) == 1 else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="записать изменения (по умолчанию dry-run)")
    args = ap.parse_args()

    path = sched.SCHEDULE_COMPLETION_FILE
    if not path.exists():
        print(f"Файл заморозок не найден: {path} — нечего мигрировать.")
        return 0

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"Не удалось прочитать {path}: {e}")
        return 1
    items = data.get("completions") if isinstance(data, dict) else None
    if not isinstance(items, list):
        print("В файле нет списка completions — нечего мигрировать.")
        return 0

    entries, warn = sched.load_decisions_log()
    if warn:
        print(f"ВНИМАНИЕ: {warn}")
    idx = _build_version_day_index(entries)

    stamped, skipped_has_ver, left_versionless = [], 0, 0
    for rec in items:
        if not isinstance(rec, dict):
            continue
        if sched._norm_version(rec.get("version_id")):
            skipped_has_ver += 1
            continue
        obj = (rec.get("object_id") or "").strip()
        proj = (rec.get("source_project") or "").strip()
        day = sched.parse_day(rec.get("date"))
        if not day:
            left_versionless += 1
            continue
        inferred = _infer_version(obj, proj, day, idx)
        if inferred:
            stamped.append((proj, day, inferred, rec))
        else:
            left_versionless += 1

    print(f"Стор: {path}")
    print(f"Всего записей: {len(items)}")
    print(f"  уже с версией (пропуск): {skipped_has_ver}")
    print(f"  останутся безверсионными: {left_versionless}")
    print(f"  будет проставлена версия: {len(stamped)}")
    print()
    for proj, day, ver, _ in sorted(stamped, key=lambda x: (x[2], x[0])):
        print(f"  [{ver}] {proj:30s} freeze={day}")

    if not stamped:
        print("\nНечего применять.")
        return 0

    if not args.apply:
        print("\nDRY-RUN. Для записи запусти с --apply.")
        return 0

    # Применяем: только добавляем version_id к выбранным записям.
    for _, _, ver, rec in stamped:
        rec["version_id"] = ver
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    sched._atomic_write_json(path, data)
    print(f"\nЗаписано: {len(stamped)} записей получили version_id → {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
