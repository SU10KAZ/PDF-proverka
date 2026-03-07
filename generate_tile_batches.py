"""
generate_tile_batches.py
------------------------
Разбивает тайлы проекта на пакеты для пакетного анализа Claude.

Принцип: «один чертёж = один батч».
- Страница с 2+ тайлами → отдельный батч (Claude видит все части листа целиком)
- Страницы с 1 тайлом → объединяются до ~batch_size (нет пространственного контекста)
- Большие страницы (> batch_size) → разбиваются по рядам

Использование:
  python generate_tile_batches.py projects/133-23-GK-EM1
  python generate_tile_batches.py projects/133-23-GK-EM1 --batch-size 8
  python generate_tile_batches.py                         # все проекты
"""

import os
import sys
import json
import glob
import argparse

BASE_DIR = r"D:\Отедел Системного Анализа\1. Calude code"
DEFAULT_BATCH_SIZE = 10


def find_project_tiles(project_path, pages_filter=None):
    """Находит все тайлы проекта, сгруппированные по страницам.

    Args:
        project_path: путь к папке проекта
        pages_filter: опциональный список номеров страниц (например [7, 9, 11]).
                      Если задан — возвращает тайлы только для этих страниц.

    Возвращает: [{page, label, grid, tile_count, tiles}, ...] — отсортировано по page_num.
    """
    tiles_dir = os.path.join(project_path, "_output", "tiles")
    if not os.path.isdir(tiles_dir):
        return []

    pages = []
    # Ищем подпапки page_XX с index.json
    for entry in sorted(os.listdir(tiles_dir)):
        page_dir = os.path.join(tiles_dir, entry)
        index_path = os.path.join(page_dir, "index.json")
        if not os.path.isdir(page_dir) or not os.path.isfile(index_path):
            continue

        with open(index_path, "r", encoding="utf-8") as f:
            index = json.load(f)

        page_num = index.get("page", 0)

        # Фильтрация по списку страниц
        if pages_filter and page_num not in pages_filter:
            continue

        label = index.get("label", entry)
        grid = index.get("grid", "?x?")
        tiles = []

        for tile in index.get("tiles", []):
            tiles.append({
                "page": page_num,
                "file": f"{entry}/{tile['file']}",
                "row": tile.get("row", 0),
                "col": tile.get("col", 0),
                "size_kb": tile.get("size_kb", 0),
            })

        if tiles:
            pages.append({
                "page": page_num,
                "label": label,
                "grid": grid,
                "tile_count": len(tiles),
                "tiles": tiles,
            })

    pages.sort(key=lambda p: p["page"])
    return pages


def split_page_by_rows(page_info, batch_size):
    """Разбивает большую страницу на группы по рядам.

    Если страница содержит > batch_size тайлов (например 5x7 = 35),
    разбиваем по рядам так, чтобы каждая группа <= batch_size.
    """
    tiles = page_info["tiles"]
    page_num = page_info["page"]

    if len(tiles) <= batch_size:
        return [tiles]

    # Группируем тайлы по рядам
    rows_dict = {}
    for tile in tiles:
        row = tile.get("row", 0)
        if row not in rows_dict:
            rows_dict[row] = []
        rows_dict[row].append(tile)

    # Собираем группы из рядов, не превышая batch_size
    groups = []
    current_group = []

    for row_num in sorted(rows_dict.keys()):
        row_tiles = rows_dict[row_num]

        if current_group and (len(current_group) + len(row_tiles)) > batch_size:
            groups.append(current_group)
            current_group = []

        current_group.extend(row_tiles)

        if len(current_group) >= batch_size:
            groups.append(current_group)
            current_group = []

    if current_group:
        groups.append(current_group)

    return groups


def _make_batch(batches, tiles, pages_included, pages_lookup):
    """Создаёт батч с метаданными о типе и сетке."""
    page_set = set(t["page"] for t in tiles)
    is_single_page = len(page_set) == 1

    batch = {
        "batch_id": len(batches) + 1,
        "tiles": tiles,
        "pages_included": pages_included,
        "tile_count": len(tiles),
        "batch_type": "single_page" if is_single_page else "multi_page",
    }

    # Для single_page батчей — добавляем сетку для визуализации
    if is_single_page:
        pnum = pages_included[0]
        pinfo = pages_lookup.get(pnum, {})
        batch["page_grid"] = pinfo.get("grid", "?x?")
        batch["page_label"] = pinfo.get("label", f"page_{pnum}")
    else:
        # Для multi_page — краткая сводка по страницам
        batch["pages_detail"] = [
            {"page": p, "grid": pages_lookup.get(p, {}).get("grid", "1x1"),
             "tile_count": pages_lookup.get(p, {}).get("tile_count", 1)}
            for p in pages_included
        ]

    return batch


def generate_batches(pages, batch_size=DEFAULT_BATCH_SIZE):
    """Формирует пакеты из страниц.

    Принцип: «один чертёж = один батч».
    - Страница с 2+ тайлами → отдельный батч (Claude фокусируется на одном листе)
    - Страницы с 1 тайлом → объединяются до ~batch_size
    - Большие страницы (> batch_size) → разбиваются по рядам

    Это позволяет Claude при анализе сетки 2×2 или 2×3 рассматривать
    тайлы как фрагменты одного чертежа и прослеживать кабельные трассы,
    связи элементов и подписи через границы тайлов.
    """
    batches = []
    current_batch_tiles = []  # буфер для одиночных страниц
    current_batch_pages = []

    # Lookup для быстрого доступа к метаданным страницы
    pages_lookup = {p["page"]: p for p in pages}

    for page_info in pages:
        page_tiles = page_info["tiles"]
        page_num = page_info["page"]

        # ─── Большая страница (> batch_size): разбиваем по рядам ───
        if len(page_tiles) > batch_size:
            # Сначала закрываем буфер одиночных
            if current_batch_tiles:
                batches.append(_make_batch(
                    batches, current_batch_tiles, current_batch_pages, pages_lookup
                ))
                current_batch_tiles = []
                current_batch_pages = []

            groups = split_page_by_rows(page_info, batch_size)
            for group in groups:
                batches.append(_make_batch(
                    batches, group, [page_num], pages_lookup
                ))
            continue

        # ─── Мульти-тайл страница (2+ тайлов): отдельный батч ───
        if len(page_tiles) >= 2:
            # Сначала закрываем буфер одиночных
            if current_batch_tiles:
                batches.append(_make_batch(
                    batches, current_batch_tiles, current_batch_pages, pages_lookup
                ))
                current_batch_tiles = []
                current_batch_pages = []

            batches.append(_make_batch(
                batches, page_tiles, [page_num], pages_lookup
            ))
            continue

        # ─── Одиночная страница (1 тайл): буферизуем ───
        if current_batch_tiles and (len(current_batch_tiles) + 1) > batch_size:
            batches.append(_make_batch(
                batches, current_batch_tiles, current_batch_pages, pages_lookup
            ))
            current_batch_tiles = []
            current_batch_pages = []

        current_batch_tiles.extend(page_tiles)
        current_batch_pages.append(page_num)

        if len(current_batch_tiles) >= batch_size:
            batches.append(_make_batch(
                batches, current_batch_tiles, current_batch_pages, pages_lookup
            ))
            current_batch_tiles = []
            current_batch_pages = []

    # Остаток одиночных
    if current_batch_tiles:
        batches.append(_make_batch(
            batches, current_batch_tiles, current_batch_pages, pages_lookup
        ))

    return batches


def process_project(project_path, batch_size=DEFAULT_BATCH_SIZE, pages_filter=None):
    """Генерирует tile_batches.json для одного проекта.

    Args:
        project_path: путь к папке проекта
        batch_size: целевое количество тайлов в пакете
        pages_filter: опциональный список номеров страниц [7, 9, 11]
    """
    # Читаем project_info.json
    info_path = os.path.join(project_path, "project_info.json")
    if not os.path.isfile(info_path):
        print(f"  [SKIP] Нет project_info.json: {project_path}")
        return None

    with open(info_path, "r", encoding="utf-8") as f:
        info = json.load(f)

    project_id = info.get("project_id", os.path.basename(project_path))

    # Находим тайлы (с опциональной фильтрацией по страницам)
    pages = find_project_tiles(project_path, pages_filter=pages_filter)
    if not pages:
        print(f"  [SKIP] Нет тайлов: {project_path}")
        return None

    total_tiles = sum(p["tile_count"] for p in pages)
    total_pages = len(pages)

    # Формируем пакеты
    batches = generate_batches(pages, batch_size)

    result = {
        "project_id": project_id,
        "project_path": project_path,
        "total_tiles": total_tiles,
        "total_pages": total_pages,
        "batch_size_target": batch_size,
        "total_batches": len(batches),
        "tile_config_source": info.get("tile_config_source", "unknown"),
        "pages_summary": [
            {
                "page": p["page"],
                "label": p["label"],
                "grid": p["grid"],
                "tile_count": p["tile_count"],
            }
            for p in pages
        ],
        "batches": batches,
    }

    # Сохраняем
    out_path = os.path.join(project_path, "_output", "tile_batches.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"  Проект: {project_id}")
    print(f"  Страниц: {total_pages}, тайлов: {total_tiles}")
    print(f"  Пакетов: {len(batches)} (целевой размер: {batch_size})")
    single_count = sum(1 for b in batches if b.get("batch_type") == "single_page")
    multi_count = sum(1 for b in batches if b.get("batch_type") == "multi_page")
    print(f"  Из них: {single_count} одностраничных, {multi_count} сборных")
    for b in batches:
        pages_str = ", ".join(str(p) for p in b["pages_included"])
        btype = "[1pg]" if b.get("batch_type") == "single_page" else "[mix]"
        grid = f" [{b['page_grid']}]" if b.get("page_grid") else ""
        print(f"    {btype} Пакет {b['batch_id']:3d}: {b['tile_count']:3d} тайлов  (стр. {pages_str}){grid}")
    print(f"  Сохранено: {out_path}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generate tile batches for batch Claude analysis")
    parser.add_argument("project", nargs="?", default=None,
                        help="Project folder (e.g. projects/133-23-GK-EM1)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Target tiles per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--pages", type=str, default=None,
                        help="Comma-separated page numbers to include (e.g. --pages 7,9,11)")
    args = parser.parse_args()

    # Парсинг --pages
    pages_filter = None
    if args.pages:
        pages_filter = [int(p.strip()) for p in args.pages.split(",") if p.strip().isdigit()]
        if not pages_filter:
            print("[ERROR] --pages: укажите номера страниц через запятую")
            sys.exit(1)
        print(f"  [FILTER] Только страницы: {pages_filter}")

    if args.project:
        # Один проект
        project_path = args.project
        if not os.path.isabs(project_path):
            project_path = os.path.join(BASE_DIR, project_path)
        process_project(project_path, args.batch_size, pages_filter=pages_filter)
    else:
        # Все проекты
        projects_dir = os.path.join(BASE_DIR, "projects")
        if not os.path.isdir(projects_dir):
            print(f"[ERROR] Папка проектов не найдена: {projects_dir}")
            sys.exit(1)

        count = 0
        for entry in sorted(os.listdir(projects_dir)):
            proj_path = os.path.join(projects_dir, entry)
            info_path = os.path.join(proj_path, "project_info.json")
            if os.path.isdir(proj_path) and os.path.isfile(info_path):
                print(f"\n{'='*60}")
                result = process_project(proj_path, args.batch_size)
                if result:
                    count += 1

        print(f"\n{'='*60}")
        print(f"Обработано проектов: {count}")


if __name__ == "__main__":
    main()
