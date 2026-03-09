#!/usr/bin/env python3
"""
Группировка image-блоков в пакеты для параллельного анализа Claude.

Использование:
    python generate_block_batches.py projects/<name>
    python generate_block_batches.py projects/<name> --block-ids 7LPC-FCK4-PET,6XVP-EQJH-G9J
    python generate_block_batches.py projects/<name> --batch-size 8
"""
import argparse
import json
import os
import sys
from pathlib import Path


DEFAULT_BATCH_SIZE = 8  # блоков на пакет


def generate_block_batches(
    project_dir: str,
    block_ids: list[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """
    Сгруппировать image-блоки в пакеты.

    Args:
        project_dir: путь к папке проекта
        block_ids: фильтр по block_id (None = все)
        batch_size: максимум блоков в пакете

    Returns:
        dict с батчами
    """
    output_dir = Path(project_dir) / "_output"
    index_path = output_dir / "blocks" / "index.json"

    if not index_path.exists():
        print(f"[ERROR] {index_path} не найден. Сначала запустите crop_blocks.py")
        return {"error": "blocks/index.json not found"}

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    blocks = index_data.get("blocks", [])

    # Фильтрация
    if block_ids:
        blocks = [b for b in blocks if b["block_id"] in block_ids]

    if not blocks:
        print("[WARN] Нет блоков для группировки")
        return {"total_batches": 0, "batches": []}

    # Группировка блоков по страницам
    pages_map: dict[int, list[dict]] = {}
    for block in blocks:
        page = block.get("page", 0)
        pages_map.setdefault(page, []).append(block)

    # Формирование пакетов: блоки одной страницы в один пакет,
    # если на странице > batch_size блоков — разбиваем
    batches = []
    batch_id = 0

    for page_num in sorted(pages_map.keys()):
        page_blocks = pages_map[page_num]

        # Разбиваем на чанки по batch_size
        for i in range(0, len(page_blocks), batch_size):
            batch_id += 1
            chunk = page_blocks[i:i + batch_size]
            batches.append({
                "batch_id": batch_id,
                "blocks": [
                    {
                        "block_id": b["block_id"],
                        "page": b["page"],
                        "file": b["file"],
                        "size_kb": b.get("size_kb", 0),
                        "ocr_label": b.get("ocr_label", "image"),
                    }
                    for b in chunk
                ],
                "pages_included": sorted(set(b["page"] for b in chunk)),
                "block_count": len(chunk),
            })

    result = {
        "total_batches": len(batches),
        "total_blocks": sum(b["block_count"] for b in batches),
        "batch_size": batch_size,
        "batches": batches,
    }

    # Записать
    out_path = output_dir / "block_batches.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"  Сгенерировано {len(batches)} пакетов ({result['total_blocks']} блоков)")
    print(f"  Записано: {out_path}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Группировка image-блоков в пакеты для Claude"
    )
    parser.add_argument("project_dir", help="Путь к папке проекта")
    parser.add_argument("--block-ids", help="Список block_id через запятую")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Максимум блоков в пакете (по умолчанию {DEFAULT_BATCH_SIZE})")
    args = parser.parse_args()

    if not os.path.isdir(args.project_dir):
        print(f"[ERROR] Папка не найдена: {args.project_dir}")
        sys.exit(1)

    block_ids = None
    if args.block_ids:
        block_ids = [bid.strip() for bid in args.block_ids.split(",")]

    result = generate_block_batches(
        args.project_dir,
        block_ids=block_ids,
        batch_size=args.batch_size,
    )

    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
