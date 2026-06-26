#!/usr/bin/env python3
"""
backfill_schedule_completion.py
-------------------------------
Зафиксировать «день завершения» для УЖЕ существующих проектов графика работ.

Для каждого (object_id, source_project) из knowledge_base/decisions_log.json,
у которого экспертом размечены ВСЕ замечания и ВСЕ оптимизации (строгая проверка
через kb._project_completion_day), фиксирует день = текущий показываемый день
проекта в графике (последняя активность среди решений). После этого правки
разметки больше не двигают день проекта в графике.

Безопасность:
  * АДДИТИВНО — создаёт/дополняет ОДИН файл knowledge_base/schedule_completion.json,
    ничего не удаляет и не трогает decisions_log;
  * ИДЕМПОТЕНТНО — set_completion_once не перезаписывает уже зафиксированные дни;
  * DRY-RUN по умолчанию — без --apply только печатает план.

Запуск:
    python scripts/backfill_schedule_completion.py            # dry-run
    python scripts/backfill_schedule_completion.py --apply    # запись
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.schedule_service as sched  # noqa: E402
import backend.app.services.knowledge_base.knowledge_base_service as kb  # noqa: E402


def _collect_groups(entries: list[dict]) -> dict[tuple[str, str], dict]:
    """(object_id, source_project) → {reviewer, max_day} из лога решений."""
    groups: dict[tuple[str, str], dict] = {}
    for e in entries:
        if not isinstance(e, dict):
            continue
        reviewer = (e.get("expert_reviewer") or "").strip()
        if not reviewer or reviewer.lower() in sched._SYSTEM_REVIEWERS:
            continue
        proj = (e.get("source_project") or "").strip()
        if not proj:
            continue
        obj = (e.get("object_id") or "").strip()
        day = sched.parse_day(e.get("expert_date"))
        if not day:
            continue
        g = groups.get((obj, proj))
        if g is None:
            groups[(obj, proj)] = {"reviewer": reviewer, "max_day": day}
        else:
            if day > g["max_day"]:
                g["max_day"] = day
    return groups


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="записать в schedule_completion.json (без флага — dry-run)")
    args = ap.parse_args()

    entries, warning = sched.load_decisions_log()
    if warning:
        print(f"WARN: {warning}")
    groups = _collect_groups(entries)
    existing = sched.load_schedule_completions()

    to_stamp: list[tuple[str, str, str, str]] = []
    skipped_incomplete = 0
    for (obj, proj), g in sorted(groups.items()):
        if (obj, proj) in existing:
            continue  # уже заморожен — не трогаем
        try:
            complete = kb._project_completion_day(proj) is not None
        except Exception as exc:  # noqa: BLE001 — fail-soft per project
            print(f"  SKIP (ошибка резолва) {proj}: {exc}")
            continue
        if not complete:
            skipped_incomplete += 1
            continue
        # Замораживаем на ТЕКУЩЕМ показываемом дне (последняя активность в логе),
        # чтобы backfill не сдвинул то, что уже видно в графике.
        to_stamp.append((obj, proj, g["max_day"], g["reviewer"]))

    print(f"\nГрупп в логе: {len(groups)} | уже заморожено: {len(existing)} | "
          f"к фиксации: {len(to_stamp)} | пропущено (не завершено): {skipped_incomplete}")
    mode = "APPLY" if args.apply else "DRY"
    for obj, proj, day, reviewer in to_stamp:
        print(f"  [{mode}] {day}  obj={obj or '—'}  {proj}  ({reviewer})")
        if args.apply:
            sched.set_completion_once(object_id=obj, source_project=proj,
                                      date=day, reviewer=reviewer)

    if args.apply:
        print(f"\nГотово. Файл: {sched.SCHEDULE_COMPLETION_FILE}")
    else:
        print("\nDry-run (ничего не записано). "
              "Для записи: python scripts/backfill_schedule_completion.py --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
