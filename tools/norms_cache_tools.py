#!/usr/bin/env python3
"""Утилита для анализа и сопровождения кеша норм.

Использование:
    python tools/norms_cache_tools.py stats          # статистика norms_db + paragraphs
    python tools/norms_cache_tools.py paragraphs     # анализ paragraph cache
    python tools/norms_cache_tools.py duplicates     # поиск дублей в norms_db
    python tools/norms_cache_tools.py stale          # устаревшие записи
    python tools/norms_cache_tools.py conflicts      # конфликтующие записи
    python tools/norms_cache_tools.py report         # полный отчёт
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Корень проекта
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from norms import (
    load_norms_db, load_norms_paragraphs, normalize_doc_number,
    paragraph_cache_stats,
)


def cmd_stats():
    """Статистика norms_db + paragraph cache."""
    db = load_norms_db()
    norms = db.get("norms", {})
    meta = db.get("meta", {})

    print("═══ norms_db.json ═══")
    print(f"  Всего норм:       {len(norms)}")
    print(f"  Последнее обновление: {meta.get('last_updated', '?')[:19]}")
    print(f"  Stale через:      {meta.get('stale_after_days', 30)} дней")

    by_status = {}
    by_category = {}
    for n in norms.values():
        s = n.get("status", "?")
        by_status[s] = by_status.get(s, 0) + 1
        c = n.get("category", "?")
        by_category[c] = by_category.get(c, 0) + 1

    print("  По статусу:")
    for s, c in sorted(by_status.items()):
        print(f"    {s}: {c}")
    print("  По категории:")
    for s, c in sorted(by_category.items()):
        print(f"    {s}: {c}")

    print()
    ps = paragraph_cache_stats()
    print("═══ norms_paragraphs.json ═══")
    print(f"  Всего цитат:      {ps['total']}")
    print(f"  Пустые quote:     {ps['empty_quote']}")
    print(f"  Последнее обновление: {ps.get('last_updated', '?')}")
    print("  По источнику:")
    for s, c in sorted(ps.get("by_verified_via", {}).items()):
        print(f"    {s}: {c}")


def cmd_paragraphs():
    """Подробный анализ paragraph cache."""
    pdb = load_norms_paragraphs()
    paragraphs = pdb.get("paragraphs", {})
    print(f"Всего записей: {len(paragraphs)}\n")

    empty = []
    short = []
    for key, p in sorted(paragraphs.items()):
        quote = p.get("quote", "")
        if not quote:
            empty.append(key)
        elif len(quote) < 20:
            short.append((key, quote))

    if empty:
        print(f"Пустые quote ({len(empty)}):")
        for k in empty:
            print(f"  - {k}")

    if short:
        print(f"\nКороткие quote ({len(short)}):")
        for k, q in short:
            print(f"  - {k}: \"{q}\"")

    if not empty and not short:
        print("Все записи содержат непустые цитаты длиной >= 20 символов.")


def cmd_duplicates():
    """Поиск дублей в norms_db (нормализация ключей)."""
    db = load_norms_db()
    norms = db.get("norms", {})

    normalized = {}
    for key in norms:
        nk = normalize_doc_number(key)
        normalized.setdefault(nk, []).append(key)

    dupes = {k: v for k, v in normalized.items() if len(v) > 1}
    if dupes:
        print(f"Дубликаты ({len(dupes)}):")
        for nk, keys in dupes.items():
            print(f"  {nk}:")
            for k in keys:
                s = norms[k].get("status", "?")
                print(f"    - \"{k}\" (status={s})")
    else:
        print("Дубликатов не найдено.")


def cmd_stale():
    """Устаревшие записи (last_verified > stale_days)."""
    db = load_norms_db()
    norms = db.get("norms", {})
    stale_days = db.get("meta", {}).get("stale_after_days", 30)
    now = datetime.now()

    stale = []
    no_date = []
    for key, n in norms.items():
        lv = n.get("last_verified", "")
        if not lv:
            no_date.append(key)
            continue
        try:
            vd = datetime.fromisoformat(lv)
            if (now - vd) > timedelta(days=stale_days):
                days = (now - vd).days
                stale.append((key, days, n.get("status", "?")))
        except (ValueError, TypeError):
            no_date.append(key)

    print(f"Stale threshold: {stale_days} дней\n")
    if stale:
        stale.sort(key=lambda x: -x[1])
        print(f"Устаревшие ({len(stale)}):")
        for key, days, status in stale[:30]:
            print(f"  {key}: {days}д (status={status})")
        if len(stale) > 30:
            print(f"  ... и ещё {len(stale) - 30}")
    else:
        print("Устаревших записей нет.")

    if no_date:
        print(f"\nБез даты проверки ({len(no_date)}):")
        for key in no_date[:10]:
            print(f"  - {key}")


def cmd_conflicts():
    """Конфликтующие записи: разные статусы для одной нормы."""
    pdb = load_norms_paragraphs()
    paragraphs = pdb.get("paragraphs", {})
    db = load_norms_db()
    db_norms = db.get("norms", {})

    conflicts = []
    for pkey, p in paragraphs.items():
        norm = p.get("norm", "")
        nk = normalize_doc_number(norm) if norm else ""
        if nk and nk in db_norms:
            db_status = db_norms[nk].get("status", "?")
            if db_status in ("replaced", "cancelled"):
                conflicts.append((pkey, norm, db_status))

    if conflicts:
        print(f"Конфликты ({len(conflicts)}):")
        print("Цитаты для норм с неактивным статусом:")
        for pkey, norm, status in conflicts:
            print(f"  - {pkey}")
            print(f"    norm: {norm}, db_status: {status}")
    else:
        print("Конфликтов не обнаружено.")


def cmd_report():
    """Полный отчёт."""
    print("╔══════════════════════════════════════╗")
    print("║   ОТЧЁТ ПО КЕШУ НОРМ                ║")
    print("╚══════════════════════════════════════╝\n")
    cmd_stats()
    print("\n" + "─" * 40 + "\n")
    cmd_duplicates()
    print("\n" + "─" * 40 + "\n")
    cmd_stale()
    print("\n" + "─" * 40 + "\n")
    cmd_conflicts()
    print("\n" + "─" * 40 + "\n")
    cmd_paragraphs()


COMMANDS = {
    "stats": cmd_stats,
    "paragraphs": cmd_paragraphs,
    "duplicates": cmd_duplicates,
    "stale": cmd_stale,
    "conflicts": cmd_conflicts,
    "report": cmd_report,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[sys.argv[1]]()
