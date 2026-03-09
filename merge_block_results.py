#!/usr/bin/env python3
"""
Слияние результатов анализа блоков (block_batch_NNN.json) в 02_blocks_analysis.json.

Использование:
    python merge_block_results.py projects/<name>
    python merge_block_results.py projects/<name> --cleanup
"""
import argparse
import json
import os
import sys
from pathlib import Path


def merge_block_results(project_dir: str, cleanup: bool = False) -> dict:
    """
    Слить все block_batch_NNN.json в один 02_blocks_analysis.json.

    Args:
        project_dir: путь к папке проекта
        cleanup: удалить промежуточные файлы после слияния

    Returns:
        dict с результатами слияния
    """
    output_dir = Path(project_dir) / "_output"

    # Найти все block_batch_*.json
    batch_files = sorted(output_dir.glob("block_batch_*.json"))
    if not batch_files:
        print("[ERROR] Нет файлов block_batch_*.json")
        return {"error": "no batch files found"}

    print(f"  Найдено пакетов: {len(batch_files)}")

    # Загрузить и слить
    all_block_analyses = []
    all_findings = []
    total_blocks_reviewed = 0
    merged_sources = []

    for bf in batch_files:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                batch_data = json.load(f)

            # Стандартизированная структура из Claude:
            # - block_analyses[] или page_summaries[] — анализ блоков/страниц
            # - preliminary_findings[] или findings[] — замечания

            # Блоковые анализы
            analyses = (
                batch_data.get("block_analyses", [])
                or batch_data.get("page_summaries", [])
                or batch_data.get("blocks_reviewed", [])
            )
            all_block_analyses.extend(analyses)
            total_blocks_reviewed += len(analyses)

            # Замечания
            findings = (
                batch_data.get("preliminary_findings", [])
                or batch_data.get("findings", [])
            )
            all_findings.extend(findings)

            merged_sources.append(bf.name)
            print(f"    {bf.name}: {len(analyses)} блоков, {len(findings)} замечаний")

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  [WARN] Ошибка чтения {bf.name}: {e}")

    # Загрузить ожидаемое количество блоков
    batches_path = output_dir / "block_batches.json"
    expected_blocks = 0
    if batches_path.exists():
        with open(batches_path, "r", encoding="utf-8") as f:
            batches_meta = json.load(f)
        expected_blocks = batches_meta.get("total_blocks", 0)

    coverage = (
        round(total_blocks_reviewed / expected_blocks * 100, 1)
        if expected_blocks > 0 else 0
    )

    # Сформировать итоговый файл
    result = {
        "stage": "02_blocks_analysis",
        "meta": {
            "blocks_reviewed": total_blocks_reviewed,
            "total_blocks_expected": expected_blocks,
            "coverage_pct": coverage,
            "batches_merged": len(batch_files),
            "sources": merged_sources,
        },
        "block_analyses": all_block_analyses,
        "preliminary_findings": all_findings,
    }

    # Записать
    out_path = output_dir / "02_blocks_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n  Итого: {total_blocks_reviewed} блоков, {len(all_findings)} замечаний")
    print(f"  Покрытие: {coverage}%")
    print(f"  Записано: {out_path}")

    # Очистка промежуточных файлов
    if cleanup:
        for bf in batch_files:
            bf.unlink()
            print(f"  [DEL] {bf.name}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Слияние block_batch_*.json в 02_blocks_analysis.json"
    )
    parser.add_argument("project_dir", help="Путь к папке проекта")
    parser.add_argument("--cleanup", action="store_true",
                        help="Удалить промежуточные файлы после слияния")
    args = parser.parse_args()

    if not os.path.isdir(args.project_dir):
        print(f"[ERROR] Папка не найдена: {args.project_dir}")
        sys.exit(1)

    result = merge_block_results(args.project_dir, cleanup=args.cleanup)

    if result.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
