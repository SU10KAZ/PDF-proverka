"""
merge_tile_results.py
---------------------
Сливает пакетные результаты анализа тайлов (tile_batch_*.json)
в единый файл 02_tiles_analysis.json.

Проверяет покрытие: все ли пакеты обработаны.
Перенумерует ID находок (G-001, G-002, ...) без дублей.

Использование:
  python merge_tile_results.py projects/133-23-GK-EM1
  python merge_tile_results.py projects/133-23-GK-EM1 --cleanup
  python merge_tile_results.py                         # все проекты
"""

import os
import sys
import json
import glob
import re
import argparse
from datetime import datetime

BASE_DIR = r"D:\Отедел Системного Анализа\1. Calude code"


def _merge_partial_page_summaries(parts, page_num):
    """Сливает частичные page_summaries одной страницы (при разбивке по рядам).

    Если страница была разбита на несколько пакетов (is_partial=true),
    объединяет текст, ключевые значения и замечания.
    """
    if len(parts) == 1:
        result = dict(parts[0])
        result["is_partial"] = False
        return result

    # Сортируем по первому ряду в rows_covered
    parts_sorted = sorted(parts, key=lambda p: min(p.get("rows_covered", [0])))

    # sheet_type — берём из первого непустого
    sheet_type = "other"
    sheet_type_label = "Прочее"
    for p in parts_sorted:
        if p.get("sheet_type") and p["sheet_type"] != "other":
            sheet_type = p["sheet_type"]
            sheet_type_label = p.get("sheet_type_label", sheet_type)
            break

    # Объединяем rows_covered
    all_rows = set()
    rows_total = 0
    for p in parts_sorted:
        all_rows.update(p.get("rows_covered", []))
        rows_total = max(rows_total, p.get("rows_total", 0))

    # Склеиваем full_text_content по порядку рядов
    text_parts = [p.get("full_text_content", "") for p in parts_sorted if p.get("full_text_content")]
    full_text = "\n".join(text_parts)

    # key_values — без дублей, сохраняя порядок
    seen_kv = set()
    key_values = []
    for p in parts_sorted:
        for kv in p.get("key_values", []):
            if kv not in seen_kv:
                seen_kv.add(kv)
                key_values.append(kv)

    # findings_on_page — без дублей
    findings = []
    seen_findings = set()
    for p in parts_sorted:
        for fid in p.get("findings_on_page", []):
            if fid not in seen_findings:
                seen_findings.add(fid)
                findings.append(fid)

    # tile_count — сумма
    tile_count = sum(p.get("tile_count", 0) for p in parts_sorted)

    # summary — объединяем (или берём самый длинный)
    summaries = [p.get("summary", "") for p in parts_sorted if p.get("summary")]
    summary = " ".join(summaries) if summaries else ""

    return {
        "page": page_num,
        "sheet_type": sheet_type,
        "sheet_type_label": sheet_type_label,
        "is_partial": False,
        "rows_covered": sorted(all_rows),
        "rows_total": rows_total,
        "full_text_content": full_text,
        "key_values": key_values,
        "findings_on_page": findings,
        "tile_count": tile_count,
        "summary": summary,
    }


def merge_project(project_path, cleanup=False):
    """Сливает tile_batch_*.json в 02_tiles_analysis.json для одного проекта."""
    output_dir = os.path.join(project_path, "_output")

    # Читаем project_info
    info_path = os.path.join(project_path, "project_info.json")
    if not os.path.isfile(info_path):
        print(f"  [SKIP] Нет project_info.json: {project_path}")
        return False

    with open(info_path, "r", encoding="utf-8") as f:
        info = json.load(f)

    project_id = info.get("project_id", os.path.basename(project_path))

    # Читаем tile_batches.json для проверки покрытия
    batches_path = os.path.join(output_dir, "tile_batches.json")
    expected_batches = 0
    expected_tiles = 0
    if os.path.isfile(batches_path):
        with open(batches_path, "r", encoding="utf-8") as f:
            batches_info = json.load(f)
        expected_batches = batches_info.get("total_batches", 0)
        expected_tiles = batches_info.get("total_tiles", 0)

    # Находим все tile_batch_*.json
    pattern = os.path.join(output_dir, "tile_batch_*.json")
    batch_files = sorted(glob.glob(pattern))

    if not batch_files:
        print(f"  [ERROR] Нет файлов tile_batch_*.json в: {output_dir}")
        return False

    print(f"  Проект: {project_id}")
    print(f"  Найдено пакетных файлов: {len(batch_files)}")
    if expected_batches:
        print(f"  Ожидается пакетов: {expected_batches}")

    # Собираем данные
    all_tiles_reviewed = []
    all_items_verified = []
    all_findings = []
    all_page_summaries = {}  # {page_num: [partial_summary, ...]}
    processed_batch_ids = set()
    errors = []

    for bf in batch_files:
        fname = os.path.basename(bf)
        try:
            with open(bf, "r", encoding="utf-8") as f:
                batch_data = json.load(f)
        except json.JSONDecodeError as e:
            errors.append(f"  [ERROR] {fname}: невалидный JSON — {e}")
            continue

        batch_id = batch_data.get("batch_id", 0)
        processed_batch_ids.add(batch_id)

        # Собираем tiles_reviewed
        for tile in batch_data.get("tiles_reviewed", []):
            all_tiles_reviewed.append(tile)

        # Собираем items_verified_from_stage_01
        for item in batch_data.get("items_verified_from_stage_01", []):
            all_items_verified.append(item)

        # Собираем preliminary_findings
        for finding in batch_data.get("preliminary_findings", []):
            all_findings.append(finding)

        # Собираем page_summaries
        for ps in batch_data.get("page_summaries", []):
            page_num = ps.get("page", 0)
            if page_num not in all_page_summaries:
                all_page_summaries[page_num] = []
            all_page_summaries[page_num].append(ps)

        tile_count = len(batch_data.get("tiles_reviewed", []))
        finding_count = len(batch_data.get("preliminary_findings", []))
        ps_count = len(batch_data.get("page_summaries", []))
        print(f"    Пакет {batch_id:3d}: {tile_count} тайлов, {finding_count} находок, {ps_count} page_summaries")

    # Выводим ошибки
    for err in errors:
        print(err)

    # Проверяем покрытие
    if expected_batches > 0:
        missing = []
        for i in range(1, expected_batches + 1):
            if i not in processed_batch_ids:
                missing.append(i)
        if missing:
            print(f"\n  [WARN] Необработанные пакеты: {missing}")
            print(f"  Покрытие: {len(processed_batch_ids)}/{expected_batches} пакетов")
        else:
            print(f"\n  Покрытие: {len(processed_batch_ids)}/{expected_batches} пакетов (100%)")

    # Перенумеровываем ID находок (G-001, G-002, ...)
    finding_counter = 1
    id_map = {}  # старый ID → новый ID

    for finding in all_findings:
        old_id = finding.get("id", "")
        new_id = f"G-{finding_counter:03d}"
        id_map[old_id] = new_id
        finding["id"] = new_id
        finding_counter += 1

    # Перенумеровываем ID в tiles_reviewed -> findings
    for tile in all_tiles_reviewed:
        for f in tile.get("findings", []):
            old_id = f.get("id", "")
            if old_id in id_map:
                f["id"] = id_map[old_id]
            else:
                new_id = f"G-{finding_counter:03d}"
                id_map[old_id] = new_id
                f["id"] = new_id
                finding_counter += 1

    # Обновляем ссылки в items_verified
    for item in all_items_verified:
        old_fid = item.get("finding_id", "")
        if old_fid in id_map:
            item["finding_id"] = id_map[old_fid]

    # Сливаем page_summaries и обновляем ID замечаний
    merged_page_summaries = []
    for page_num in sorted(all_page_summaries.keys()):
        parts = all_page_summaries[page_num]
        merged = _merge_partial_page_summaries(parts, page_num)
        # Обновляем ID замечаний через id_map
        merged["findings_on_page"] = [
            id_map.get(fid, fid) for fid in merged.get("findings_on_page", [])
        ]
        merged_page_summaries.append(merged)

    # Формируем итоговый JSON
    result = {
        "meta": {
            "tiles_reviewed": len(all_tiles_reviewed),
            "total_tiles_expected": expected_tiles,
            "coverage_pct": round(len(all_tiles_reviewed) / expected_tiles * 100, 1) if expected_tiles > 0 else 0,
            "batches_merged": len(batch_files),
            "findings_count": len(all_findings),
            "page_summaries_count": len(merged_page_summaries),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        },
        "tiles_reviewed": all_tiles_reviewed,
        "items_verified_from_stage_01": all_items_verified,
        "preliminary_findings": all_findings,
        "page_summaries": merged_page_summaries,
    }

    # Сохраняем 02_tiles_analysis.json
    out_path = os.path.join(output_dir, "02_tiles_analysis.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n  Результат:")
    print(f"    Тайлов проанализировано: {len(all_tiles_reviewed)}")
    print(f"    Находок (G-): {len(all_findings)}")
    print(f"    Верификаций из этапа 01: {len(all_items_verified)}")
    print(f"    Page summaries: {len(merged_page_summaries)}")
    if merged_page_summaries:
        types = {}
        for ps in merged_page_summaries:
            st = ps.get("sheet_type", "other")
            types[st] = types.get(st, 0) + 1
        print(f"    Типы листов: {', '.join(f'{t}={c}' for t, c in types.items())}")
    print(f"    Сохранено: {out_path}")

    # Очистка промежуточных файлов
    if cleanup:
        print(f"\n  Очистка промежуточных файлов...")
        for bf in batch_files:
            os.remove(bf)
            print(f"    Удалён: {os.path.basename(bf)}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Merge tile batch results into 02_tiles_analysis.json")
    parser.add_argument("project", nargs="?", default=None,
                        help="Project folder (e.g. projects/133-23-GK-EM1)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete tile_batch_*.json after merge")
    args = parser.parse_args()

    if args.project:
        # Один проект
        project_path = args.project
        if not os.path.isabs(project_path):
            project_path = os.path.join(BASE_DIR, project_path)
        success = merge_project(project_path, args.cleanup)
        sys.exit(0 if success else 1)
    else:
        # Все проекты
        projects_dir = os.path.join(BASE_DIR, "projects")
        count = 0
        for entry in sorted(os.listdir(projects_dir)):
            proj_path = os.path.join(projects_dir, entry)
            # Проверяем есть ли tile_batch файлы
            batch_pattern = os.path.join(proj_path, "_output", "tile_batch_*.json")
            if glob.glob(batch_pattern):
                print(f"\n{'='*60}")
                if merge_project(proj_path, args.cleanup):
                    count += 1

        print(f"\n{'='*60}")
        print(f"Обработано проектов: {count}")


if __name__ == "__main__":
    main()
