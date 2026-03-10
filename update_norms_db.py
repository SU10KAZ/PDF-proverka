#!/usr/bin/env python3
"""
update_norms_db.py — Обновление централизованной базы норм из результатов верификации.

Использование:
    # Обновить из одного проекта
    python update_norms_db.py projects/133-23-GK-EM1

    # Обновить из всех проектов
    python update_norms_db.py --all

    # Показать статистику базы
    python update_norms_db.py --stats

Логика:
    1. Читает norm_checks.json из проекта (или всех проектов)
    2. Для каждой проверенной нормы:
       - Если нормы нет в базе → добавить
       - Если норма есть, но проверка новее → обновить
       - Если статус изменился (replaced/cancelled) → обновить + добавить в replacements
    3. Обновляет total_norms и last_updated в meta
"""

import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
NORMS_DB_PATH = BASE_DIR / "norms_db.json"
NORMS_PARAGRAPHS_PATH = BASE_DIR / "norms_paragraphs.json"
PROJECTS_DIR = BASE_DIR / "projects"


def load_norms_db() -> dict:
    """Загрузить базу норм."""
    if not NORMS_DB_PATH.exists():
        return {
            "meta": {
                "description": "Централизованная база нормативных документов с автообновлением",
                "last_updated": datetime.now().isoformat(),
                "total_norms": 0,
                "stale_after_days": 30,
                "update_history": [],
            },
            "norms": {},
            "replacements": {},
        }
    with open(NORMS_DB_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_norms_db(db: dict):
    """Сохранить базу норм."""
    db["meta"]["total_norms"] = len(db["norms"])
    db["meta"]["last_updated"] = datetime.now().isoformat()
    with open(NORMS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def load_norms_paragraphs() -> dict:
    """Загрузить справочник проверенных параграфов."""
    if not NORMS_PARAGRAPHS_PATH.exists():
        return {
            "meta": {
                "description": "Проверенные цитаты конкретных пунктов нормативных документов",
                "last_updated": None,
                "total_paragraphs": 0,
            },
            "paragraphs": {},
        }
    with open(NORMS_PARAGRAPHS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_norms_paragraphs(pdb: dict):
    """Сохранить справочник параграфов."""
    pdb["meta"]["total_paragraphs"] = len(pdb["paragraphs"])
    pdb["meta"]["last_updated"] = datetime.now().isoformat()
    with open(NORMS_PARAGRAPHS_PATH, "w", encoding="utf-8") as f:
        json.dump(pdb, f, ensure_ascii=False, indent=2)


def update_paragraphs_from_project(pdb: dict, project_path: Path) -> int:
    """
    Обновить справочник параграфов из paragraph_checks в norm_checks.json.
    Возвращает количество добавленных/обновлённых параграфов.
    """
    norm_checks_path = project_path / "_output" / "norm_checks.json"
    if not norm_checks_path.exists():
        return 0

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    paragraph_checks = checks_data.get("paragraph_checks", [])
    if not paragraph_checks:
        return 0

    count = 0
    for pc in paragraph_checks:
        if not pc.get("paragraph_verified"):
            continue

        norm = pc.get("norm", "")
        actual_quote = pc.get("actual_quote")
        if not norm or not actual_quote:
            continue

        # Ключ: "СП 256.1325800.2016, п.14.9"
        key = norm.strip()
        existing = pdb["paragraphs"].get(key)

        if existing and existing.get("quote") == actual_quote:
            continue  # Уже есть такая же цитата

        pdb["paragraphs"][key] = {
            "norm": key,
            "quote": actual_quote,
            "verified_at": datetime.now().isoformat(),
            "source_project": project_path.name,
        }
        count += 1

    return count


def normalize_doc_number(raw: str) -> str:
    """Нормализовать номер документа для использования как ключ."""
    # Убрать лишние пробелы
    doc = raw.strip()
    # Убрать жирное форматирование markdown
    doc = doc.replace("**", "")
    return doc


def merge_norm_check(db: dict, check: dict, project_id: str) -> str:
    """
    Слить одну проверку нормы в базу.
    Возвращает: 'added', 'updated', 'skipped', 'unchanged'
    """
    doc_number = normalize_doc_number(check.get("doc_number", ""))
    if not doc_number:
        return "skipped"

    status = check.get("status", "not_found")
    if status == "not_found":
        # Не удалось проверить — не обновляем базу мусором
        return "skipped"

    now = datetime.now().isoformat()

    # Маппинг статусов norm_checks → norms_db
    status_map = {
        "active": "active",
        "outdated_edition": "active",  # документ действует, просто старая редакция в проекте
        "replaced": "replaced",
        "cancelled": "cancelled",
    }
    db_status = status_map.get(status, status)

    existing = db.get("norms", {}).get(doc_number)

    if existing:
        # Норма уже есть — проверяем нужно ли обновлять
        old_verified = existing.get("last_verified", "")

        # Обновляем если:
        # 1) Статус изменился
        # 2) Версия обновилась
        # 3) Появилась замена
        changed = False

        if existing.get("status") != db_status:
            existing["status"] = db_status
            changed = True

        new_version = check.get("current_version")
        if new_version and new_version != existing.get("current_version"):
            existing["current_version"] = new_version
            changed = True

        replacement = check.get("replacement_doc")
        if replacement and replacement != existing.get("replacement_doc"):
            existing["replacement_doc"] = replacement
            changed = True
            # Добавить в таблицу замен
            if "replacements" not in db:
                db["replacements"] = {}
            db["replacements"][doc_number] = replacement

        source_url = check.get("source_url")
        if source_url and source_url != existing.get("source_url"):
            existing["source_url"] = source_url
            changed = True

        details = check.get("details")
        if details:
            existing["notes"] = details

        # Всегда обновляем дату проверки
        existing["last_verified"] = now
        existing["verified_by"] = f"websearch:{project_id}"

        return "updated" if changed else "unchanged"
    else:
        # Новая норма — добавляем
        new_entry = {
            "doc_number": doc_number,
            "title": check.get("norm_as_cited", doc_number),
            "status": db_status,
            "current_version": check.get("current_version"),
            "replacement_doc": check.get("replacement_doc"),
            "category": _guess_category(doc_number),
            "notes": check.get("details", ""),
            "source_url": check.get("source_url"),
            "last_verified": now,
            "verified_by": f"websearch:{project_id}",
        }
        db["norms"][doc_number] = new_entry

        # Добавить замену если есть
        replacement = check.get("replacement_doc")
        if replacement and db_status in ("replaced", "cancelled"):
            if "replacements" not in db:
                db["replacements"] = {}
            db["replacements"][doc_number] = replacement

        return "added"


def _guess_category(doc_number: str) -> str:
    """Определить категорию по номеру документа."""
    dn = doc_number.upper()
    if dn.startswith("ФЗ"):
        return "federal_law"
    if dn.startswith("ПП РФ"):
        return "government_decree"
    if dn.startswith("ПУЭ"):
        return "pue"
    if "13130" in dn or "1311500" in dn:
        return "sp_fire"
    if dn.startswith("СП"):
        return "sp"
    if dn.startswith("ГОСТ"):
        return "gost"
    if dn.startswith("СО ") or dn.startswith("ВСН"):
        return "other"
    return "other"


def update_from_project(db: dict, project_path: Path) -> dict:
    """
    Обновить базу из norm_checks.json одного проекта.
    Возвращает статистику: {added, updated, unchanged, skipped}
    """
    norm_checks_path = project_path / "_output" / "norm_checks.json"
    if not norm_checks_path.exists():
        return {"error": f"norm_checks.json не найден в {project_path}"}

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    checks = checks_data.get("checks", [])
    project_id = project_path.name

    stats = {"added": 0, "updated": 0, "unchanged": 0, "skipped": 0}

    for check in checks:
        result = merge_norm_check(db, check, project_id)
        stats[result] = stats.get(result, 0) + 1

    return stats


def get_stale_norms(db: dict) -> list:
    """Получить список норм, которые давно не проверялись."""
    stale_days = db.get("meta", {}).get("stale_after_days", 30)
    threshold = datetime.now() - timedelta(days=stale_days)
    stale = []

    for doc_number, norm in db.get("norms", {}).items():
        last_verified = norm.get("last_verified", "")
        if not last_verified:
            stale.append(doc_number)
            continue
        try:
            verified_dt = datetime.fromisoformat(last_verified)
            if verified_dt < threshold:
                stale.append(doc_number)
        except (ValueError, TypeError):
            stale.append(doc_number)

    return stale


def print_stats(db: dict):
    """Вывести статистику базы."""
    norms = db.get("norms", {})
    total = len(norms)

    by_status = {}
    by_category = {}
    for norm in norms.values():
        status = norm.get("status", "unknown")
        category = norm.get("category", "other")
        by_status[status] = by_status.get(status, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1

    stale = get_stale_norms(db)
    replacements = db.get("replacements", {})

    # Загрузить справочник параграфов
    pdb = load_norms_paragraphs()
    total_paragraphs = len(pdb.get("paragraphs", {}))

    print(f"\n{'='*60}")
    print(f"  БАЗА НОРМАТИВНЫХ ДОКУМЕНТОВ — СТАТИСТИКА")
    print(f"{'='*60}")
    print(f"  Всего норм:         {total}")
    print(f"  Проверенных цитат:  {total_paragraphs}")
    print(f"  Таблица замен:      {len(replacements)} записей")
    print(f"  Устаревших (>{db['meta'].get('stale_after_days', 30)} дн): {len(stale)}")
    print(f"  Последнее обновление: {db['meta'].get('last_updated', 'N/A')}")
    print()
    print("  По статусу:")
    for status, count in sorted(by_status.items()):
        icon = {"active": "+", "replaced": "!", "cancelled": "X", "limited": "~", "voluntary": "?"}.get(status, " ")
        print(f"    [{icon}] {status}: {count}")
    print()
    print("  По категории:")
    for cat, count in sorted(by_category.items()):
        print(f"    {cat}: {count}")

    if stale:
        print(f"\n  Нормы, требующие повторной проверки ({len(stale)}):")
        for doc in stale[:10]:
            norm = norms[doc]
            print(f"    - {doc} (проверена: {norm.get('last_verified', 'никогда')[:10]})")
        if len(stale) > 10:
            print(f"    ... и ещё {len(stale) - 10}")

    print(f"{'='*60}\n")


def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python update_norms_db.py projects/<name>  — обновить из одного проекта")
        print("  python update_norms_db.py --all             — обновить из всех проектов")
        print("  python update_norms_db.py --stats           — показать статистику")
        print("  python update_norms_db.py --stale           — показать устаревшие нормы")
        sys.exit(1)

    db = load_norms_db()

    if sys.argv[1] == "--stats":
        print_stats(db)
        return

    if sys.argv[1] == "--stale":
        stale = get_stale_norms(db)
        if stale:
            print(f"Нормы, требующие проверки ({len(stale)}):")
            for doc in stale:
                norm = db["norms"].get(doc, {})
                print(f"  {doc} — проверена: {norm.get('last_verified', 'никогда')[:10]}")
        else:
            print("Все нормы актуальны.")
        return

    if sys.argv[1] == "--all":
        # Обновить из всех проектов
        if not PROJECTS_DIR.is_dir():
            print(f"Папка проектов не найдена: {PROJECTS_DIR}")
            sys.exit(1)

        total_stats = {"added": 0, "updated": 0, "unchanged": 0, "skipped": 0}
        processed = 0
        pdb = load_norms_paragraphs()
        total_paragraphs = 0

        for project_dir in sorted(PROJECTS_DIR.iterdir()):
            if not project_dir.is_dir():
                continue
            norm_checks = project_dir / "_output" / "norm_checks.json"
            if not norm_checks.exists():
                continue

            stats = update_from_project(db, project_dir)
            if "error" in stats:
                print(f"  [{project_dir.name}] {stats['error']}")
                continue

            processed += 1
            for key in total_stats:
                total_stats[key] += stats.get(key, 0)

            # Обновить справочник параграфов
            p_count = update_paragraphs_from_project(pdb, project_dir)
            total_paragraphs += p_count

            print(
                f"  [{project_dir.name}] +"
                f"{stats['added']} добавлено, "
                f"{stats['updated']} обновлено, "
                f"{stats['unchanged']} без изменений, "
                f"{stats['skipped']} пропущено"
                + (f", {p_count} параграфов" if p_count else "")
            )

        # Записать историю обновления
        db["meta"]["update_history"] = db["meta"].get("update_history", [])[-9:]  # последние 10
        db["meta"]["update_history"].append({
            "date": datetime.now().isoformat(),
            "source": "all_projects",
            "projects_processed": processed,
            "stats": total_stats,
        })

        save_norms_db(db)
        if total_paragraphs > 0:
            save_norms_paragraphs(pdb)
        print(f"\nИтого из {processed} проектов: "
              f"+{total_stats['added']} добавлено, "
              f"{total_stats['updated']} обновлено, "
              f"{total_stats['unchanged']} без изменений")
        if total_paragraphs > 0:
            print(f"Параграфов добавлено: {total_paragraphs} (всего в базе: {len(pdb['paragraphs'])})")
        print(f"База сохранена: {NORMS_DB_PATH}")
        return

    # Один проект
    project_path = Path(sys.argv[1])
    if not project_path.is_absolute():
        project_path = BASE_DIR / project_path

    if not project_path.is_dir():
        print(f"Проект не найден: {project_path}")
        sys.exit(1)

    stats = update_from_project(db, project_path)
    if "error" in stats:
        print(stats["error"])
        sys.exit(1)

    # Обновить справочник параграфов
    pdb = load_norms_paragraphs()
    p_count = update_paragraphs_from_project(pdb, project_path)
    if p_count > 0:
        save_norms_paragraphs(pdb)

    # Записать историю
    db["meta"]["update_history"] = db["meta"].get("update_history", [])[-9:]
    db["meta"]["update_history"].append({
        "date": datetime.now().isoformat(),
        "source": project_path.name,
        "stats": stats,
    })

    save_norms_db(db)
    print(
        f"[{project_path.name}] +"
        f"{stats['added']} добавлено, "
        f"{stats['updated']} обновлено, "
        f"{stats['unchanged']} без изменений, "
        f"{stats['skipped']} пропущено"
        + (f", {p_count} параграфов" if p_count else "")
    )
    print(f"База сохранена: {NORMS_DB_PATH}")


if __name__ == "__main__":
    main()
