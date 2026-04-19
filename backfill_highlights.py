"""
backfill_highlights.py
---------------------
Восстановление highlight_regions в 03_findings.json из 02_blocks_analysis.json.

При findings_merge (этап 4) Opus иногда теряет highlight_regions из G-замечаний.
Этот скрипт подтягивает координаты обратно по связке source_block_ids/related_block_ids.

Использование:
  python backfill_highlights.py projects/EOM/133_23-ГК-ЭМ2        # один проект
  python backfill_highlights.py --all                               # все проекты
  python backfill_highlights.py --all --dry-run                     # только показать что изменится
"""

import json
import os
import sys
import argparse
from pathlib import Path
from difflib import SequenceMatcher

BASE_DIR = Path("/home/coder/projects/PDF-proverka")


def _text_similarity(a: str, b: str) -> float:
    """Быстрое сравнение текстов (0.0–1.0)."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a[:200].lower(), b[:200].lower()).ratio()


def _build_g_index(blocks_analysis: dict) -> dict:
    """Построить индекс: block_id → [G-findings с highlight_regions]."""
    index = {}
    for analysis in blocks_analysis.get("block_analyses", []):
        bid = analysis.get("block_id", "")
        if not bid:
            continue
        for gf in analysis.get("findings", []):
            hr = gf.get("highlight_regions", [])
            if hr:
                index.setdefault(bid, []).append(gf)
    return index


def _find_best_g_match(f_finding: dict, g_findings: list[dict]) -> list[dict] | None:
    """Найти лучший G-match для F-замечания по тексту."""
    f_text = f_finding.get("problem", "") or f_finding.get("description", "") or f_finding.get("finding", "")
    if not f_text:
        return None

    best_hr = None
    best_score = 0.0

    for gf in g_findings:
        g_text = gf.get("finding", "") or gf.get("description", "")
        score = _text_similarity(f_text, g_text)
        if score > best_score:
            best_score = score
            best_hr = gf.get("highlight_regions", [])

    if best_score >= 0.3 and best_hr:
        return best_hr
    return None


def backfill_project(project_dir: Path, dry_run: bool = False) -> dict:
    """Восстановить highlight_regions для одного проекта.

    Returns:
        {"project": str, "total": int, "fixed": int, "skipped": int}
    """
    output_dir = project_dir / "_output"
    findings_path = output_dir / "03_findings.json"
    blocks_path = output_dir / "02_blocks_analysis.json"

    result = {
        "project": str(project_dir.relative_to(BASE_DIR)),
        "total": 0,
        "fixed": 0,
        "skipped": 0,
        "no_blocks_file": False,
    }

    if not findings_path.exists():
        return result
    if not blocks_path.exists():
        result["no_blocks_file"] = True
        return result

    with open(findings_path, "r", encoding="utf-8") as f:
        findings_data = json.load(f)
    with open(blocks_path, "r", encoding="utf-8") as f:
        blocks_data = json.load(f)

    g_index = _build_g_index(blocks_data)
    findings = findings_data.get("findings", [])
    result["total"] = len(findings)

    changed = False
    for f in findings:
        hr = f.get("highlight_regions", [])
        if hr:
            continue  # уже есть

        # Собираем все block_id из разных полей
        block_ids = set()
        for bid in f.get("source_block_ids", []):
            block_ids.add(bid)
        for bid in f.get("related_block_ids", []):
            block_ids.add(bid)
        for ev in f.get("evidence", []):
            if ev.get("type") == "image" and ev.get("block_id"):
                block_ids.add(ev["block_id"])

        if not block_ids:
            result["skipped"] += 1
            continue

        # Собираем все G-findings для этих блоков
        all_g_findings = []
        for bid in block_ids:
            all_g_findings.extend(g_index.get(bid, []))

        if not all_g_findings:
            result["skipped"] += 1
            continue

        # Ищем лучшее совпадение по тексту
        best_hr = _find_best_g_match(f, all_g_findings)
        if best_hr:
            f["highlight_regions"] = best_hr
            result["fixed"] += 1
            changed = True
        else:
            # Fallback: берём HR от первого G-замечания первого блока
            f["highlight_regions"] = all_g_findings[0].get("highlight_regions", [])
            result["fixed"] += 1
            changed = True

    if changed and not dry_run:
        with open(findings_path, "w", encoding="utf-8") as f:
            json.dump(findings_data, f, ensure_ascii=False, indent=2)

    return result


def find_all_projects() -> list[Path]:
    """Найти все проекты с 03_findings.json."""
    projects = []
    projects_dir = BASE_DIR / "projects"
    for root, dirs, files in os.walk(projects_dir):
        if "03_findings.json" in files and "_output" in root:
            project_dir = Path(root).parent
            projects.append(project_dir)
    return sorted(projects)


def main():
    parser = argparse.ArgumentParser(description="Восстановление highlight_regions в 03_findings.json")
    parser.add_argument("project_dir", nargs="?", default=None)
    parser.add_argument("--all", action="store_true", help="Обработать все проекты")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, не записывать")
    args = parser.parse_args()

    if args.all:
        projects = find_all_projects()
        print(f"Найдено проектов: {len(projects)}")
        total_fixed = 0
        for proj in projects:
            r = backfill_project(proj, dry_run=args.dry_run)
            if r["fixed"] > 0:
                tag = "[DRY-RUN] " if args.dry_run else ""
                print(f"  {tag}{r['project']}: {r['fixed']} восстановлено из {r['total']}")
                total_fixed += r["fixed"]
        print(f"\nИтого: {total_fixed} highlight_regions восстановлено")
        if args.dry_run:
            print("(dry-run — файлы не изменены)")
    elif args.project_dir:
        proj = Path(args.project_dir)
        if not proj.is_absolute():
            proj = BASE_DIR / proj
        r = backfill_project(proj, dry_run=args.dry_run)
        tag = "[DRY-RUN] " if args.dry_run else ""
        print(f"{tag}{r['project']}: {r['fixed']} восстановлено из {r['total']} "
              f"(пропущено: {r['skipped']})")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
