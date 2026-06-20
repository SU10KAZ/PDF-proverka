#!/usr/bin/env python3
"""Read-only аудит целостности knowledge_base/decisions_log.json.

reserc.md Этап 2, шаг 25 (#79/#80/#86/#82). НИЧЕГО НЕ МЕНЯЕТ — только читает и
печатает метрики, на которых строится ID-реформа и мигратор:

  1. дубли поля `id` (DEC-NNNN переиспользуется как len()+1 после revoke →
     один id указывает на десятки несвязанных решений в разных проектах);
  2. уникальность составного ключа (source_project, item_id) — основа адресации
     revoke/customer-confirm вместо хрупкого `id`;
  3. орфаны / битые записи (пустой source_project|item_id, version-маркеры);
  4. сводка customer_confirmed / expert_decision / item_type.

Использование:
  python scripts/audit_decisions_log_integrity.py [path] [--strict]

--strict → exit 1 при наличии id-дублей или битых записей (для будущего CI-гейта).
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

DEFAULT = Path("knowledge_base/decisions_log.json")
_VERSION_MARKERS = {"v1", "v2", "v3", "v4", "v5"}


def _s(v) -> str:
    return str(v if v is not None else "").strip()


def audit(path: Path) -> tuple[int, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("entries") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        print("[audit] не найден список entries")
        return 2, {}

    total = len(entries)
    print(f"=== decisions_log integrity audit: {path} ===")
    print(f"всего записей: {total}\n")

    # 1. id-коллизии
    id_counts = Counter(_s(e.get("id")) for e in entries)
    id_to_projects: dict[str, set] = defaultdict(set)
    for e in entries:
        id_to_projects[_s(e.get("id"))].add(_s(e.get("source_project")))
    dup_ids = {i: c for i, c in id_counts.items() if c > 1}
    rows_in_dup = sum(dup_ids.values())
    print("-- 1. Уникальность id (DEC-NNNN) --")
    print(f"  различных id:                 {len(id_counts)}")
    print(f"  id с дублями (>1 записи):     {len(dup_ids)}")
    print(f"  записей с неуникальным id:    {rows_in_dup}")
    for i, c in sorted(dup_ids.items(), key=lambda kv: kv[1], reverse=True)[:5]:
        print(f"    {i}: {c} записей в {len(id_to_projects[i])} разных source_project")

    # 2. составной ключ (source_project, item_id)
    comp = Counter((_s(e.get("source_project")), _s(e.get("item_id"))) for e in entries)
    dup_comp = {k: c for k, c in comp.items() if c > 1}
    print("\n-- 2. Уникальность (source_project, item_id) --")
    print(f"  различных пар:                {len(comp)}")
    print(f"  пар-дублей (>1):              {len(dup_comp)}")
    print(f"  записей в дублях:             {sum(dup_comp.values())}")
    for (sp, iid), c in sorted(dup_comp.items(), key=lambda kv: kv[1], reverse=True)[:5]:
        print(f"    ({sp!r}, {iid!r}): {c}")

    # 3. орфаны / битые
    empty_sp = sum(1 for e in entries if not _s(e.get("source_project")))
    empty_iid = sum(1 for e in entries if not _s(e.get("item_id")))
    susp = [e for e in entries
            if _s(e.get("source_project")).lower() in _VERSION_MARKERS
            or _s(e.get("source_project")).rsplit(" ", 1)[-1].lower() in _VERSION_MARKERS]
    print("\n-- 3. Орфаны / битые записи --")
    print(f"  пустой source_project:        {empty_sp}")
    print(f"  пустой item_id:               {empty_iid}")
    print(f"  version-маркер в source_project: {len(susp)}")
    for sp, c in Counter(_s(e.get("source_project")) for e in susp).most_common(5):
        print(f"    {sp!r}: {c}")

    # 4. сводка
    print("\n-- 4. Сводка --")
    cc = sum(1 for e in entries if e.get("customer_confirmed"))
    print(f"  customer_confirmed=True:      {cc}")
    print(f"  expert_decision: {dict(Counter(_s(e.get('expert_decision')) or '—' for e in entries).most_common())}")
    print(f"  item_type:       {dict(Counter(_s(e.get('item_type')) or '—' for e in entries).most_common())}")

    broken = empty_sp + empty_iid + len(susp)
    stats = {
        "total": total,
        "distinct_ids": len(id_counts),
        "dup_ids": len(dup_ids),
        "rows_with_dup_id": rows_in_dup,
        "distinct_composite": len(comp),
        "dup_composite": len(dup_comp),
        "empty_source_project": empty_sp,
        "empty_item_id": empty_iid,
        "version_marker_sp": len(susp),
        "broken_total": broken,
        "customer_confirmed": cc,
    }
    return 0, stats


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--strict"]
    strict = "--strict" in sys.argv
    path = Path(args[0]) if args else DEFAULT
    if not path.exists():
        print(f"[audit] файл не найден: {path}")
        return 2
    rc, stats = audit(path)
    if rc != 0:
        return rc
    print("\n-- JSON --")
    print(json.dumps(stats, ensure_ascii=False))
    if strict and (stats["dup_ids"] or stats["broken_total"]):
        print("\n[audit] STRICT: найдены id-дубли или битые записи → exit 1")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
