#!/usr/bin/env python3
"""Мигратор-чинилка уникальности id в decisions_log (reserc.md #82, шаг 28).

⚠️ DRY-RUN ПО УМОЛЧАНИЮ. Без --execute НИЧЕГО не пишет.

Проблема: id (DEC-NNNN) генерился как len(log)+1 → после revoke номер
переиспользовался; один id указывает на десятки записей (audit: 1208 дублей id,
3122 записи). Шаг 27 уже устранил функциональную опасность (revoke/адресация по
составному ключу); этот мигратор лишь чистит ИСТОРИЧЕСКИЕ дубли id для
отображения/экспортов.

Стратегия (минимальная): для каждого id с дублями оставить ПЕРВУЮ запись (в
порядке файла), остальным присвоить свежие уникальные DEC-NNNN (от max+1). Так
меняется минимум id. Все прочие поля (в т.ч. customer_confirmed) сохраняются.

Использование:
  python scripts/migrate_decisions_log_ids.py [path]            # dry-run (по умолчанию)
  python scripts/migrate_decisions_log_ids.py [path] --execute  # реальная запись (с .bak)
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

DEFAULT = Path("knowledge_base/decisions_log.json")


def _num(idv: str) -> int | None:
    m = re.match(r"DEC-(\d+)$", str(idv or ""))
    return int(m.group(1)) if m else None


def plan(entries: list[dict]) -> list[tuple[int, str, str, str, str]]:
    """Вернуть список переназначений: (index, old_id, new_id, source_project, item_id)."""
    max_num = 0
    for e in entries:
        n = _num(e.get("id"))
        if n is not None:
            max_num = max(max_num, n)
    seen: set[str] = set()
    nxt = max_num + 1
    reassign: list[tuple[int, str, str, str, str]] = []
    for i, e in enumerate(entries):
        cur = str(e.get("id") or "")
        if cur and cur not in seen:
            seen.add(cur)
            continue
        # дубль (или пустой id) → новый уникальный
        new_id = f"DEC-{nxt:04d}"
        nxt += 1
        seen.add(new_id)
        reassign.append((i, cur, new_id, str(e.get("source_project") or ""),
                         str(e.get("item_id") or "")))
    return reassign


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    execute = "--execute" in sys.argv
    path = Path(args[0]) if args else DEFAULT
    if not path.exists():
        print(f"[migrate] файл не найден: {path}")
        return 2

    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("entries") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        print("[migrate] нет списка entries")
        return 2

    total = len(entries)
    reassign = plan(entries)
    print(f"=== migrate decisions_log ids: {path} ===")
    print(f"режим: {'EXECUTE (запись!)' if execute else 'DRY-RUN (только отчёт)'}")
    print(f"всего записей:                {total}")
    print(f"останутся со своим id:        {total - len(reassign)}")
    print(f"получат новый уникальный id:  {len(reassign)}")
    distinct_ids_before = len({str(e.get('id') or '') for e in entries})
    print(f"различных id до:              {distinct_ids_before}")
    print(f"различных id после:           {total}  (все станут уникальны)")
    print("\nпримеры переназначений (первые 10):")
    for i, old, new, sp, iid in reassign[:10]:
        print(f"  #{i}: {old} → {new}   ({sp} / {iid})")
    # сколько разных source_project задето
    affected_projects = len({r[3] for r in reassign})
    print(f"\nзатронуто записей в {affected_projects} разных source_project")

    # Импакт на patterns.json: example_ids ссылаются на DEC-id (blast radius).
    pat_path = path.parent / "patterns.json"
    if pat_path.exists():
        try:
            pj = json.loads(pat_path.read_text(encoding="utf-8"))
            pats = pj.get("patterns", pj) if isinstance(pj, dict) else pj
            refs = sum(len(p.get("example_ids") or []) for p in pats
                       if isinstance(p, dict))
            print(f"\npatterns.json: {len(pats)} паттернов, {refs} ссылок example_ids на DEC-id")
            print("  keep-first сохраняет старый id у одной записи → example_ids остаются")
            print("  резолвимыми (нет жёсткого слома). Для чистоты после --execute")
            print("  рекомендуется перегенерировать паттерны: POST /api/kb/patterns/detect")
        except Exception:
            pass

    if not execute:
        print("\n[migrate] DRY-RUN: НИЧЕГО не записано. Для реального прогона: --execute")
        return 0

    # EXECUTE: бэкап + запись
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(path.suffix + f".bak_{ts}")
    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"\n[migrate] бэкап: {backup}")
    for i, _old, new, _sp, _iid in reassign:
        entries[i]["id"] = new
    out = {"entries": entries} if isinstance(data, dict) else entries
    from backend.app.services.common.atomic_json import atomic_write_json
    atomic_write_json(path, out)
    # верификация: все id уникальны
    after = Counter(str(e.get("id") or "") for e in entries)
    dups = {k: v for k, v in after.items() if v > 1}
    print(f"[migrate] записано. дублей id после: {len(dups)} (ожидается 0)")
    return 0 if not dups else 1


if __name__ == "__main__":
    raise SystemExit(main())
