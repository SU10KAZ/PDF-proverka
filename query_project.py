"""
query_project.py
----------------
Query project findings from JSON pipeline outputs WITHOUT re-reading the full project.
Claude should run this tool to answer user questions about findings.

Usage:
  python query_project.py <project_folder>              -- show all findings
  python query_project.py <project_folder> --status     -- pipeline stage status
  python query_project.py <project_folder> --critical   -- only critical findings
  python query_project.py <project_folder> --cat okl    -- by category
  python query_project.py <project_folder> --sheet 7    -- by sheet/page
  python query_project.py <project_folder> --id F-001   -- single finding detail
  python query_project.py <project_folder> --summary    -- brief summary only
  python query_project.py                               -- list all projects + status
"""

import os, sys, json, argparse
from datetime import datetime


def _iter_project_dirs(root):
    """Рекурсивно найти все папки проектов (включая подпапки-группы)."""
    results = []
    for name in sorted(os.listdir(root)):
        entry = os.path.join(root, name)
        if not os.path.isdir(entry) or name.startswith("_"):
            continue
        info = os.path.join(entry, "project_info.json")
        has_pdf = any(f.endswith(".pdf") for f in os.listdir(entry))
        if os.path.exists(info) or has_pdf:
            results.append((name, entry))
        else:
            for sub in sorted(os.listdir(entry)):
                sub_path = os.path.join(entry, sub)
                if os.path.isdir(sub_path) and not sub.startswith("_"):
                    results.append((sub, sub_path))
    return results

# Фикс кодировки Windows (cp1251 -> utf-8 в консоли)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE = r"D:\Отедел Системного Анализа\1. Audit Manager"

# ── Каноничные категории (по CLAUDE.md) ─────────────────────────────────
SEV_ORDER = [
    "КРИТИЧЕСКОЕ",
    "СУЩЕСТВЕННОЕ",
    "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
    "СНЯТО",
]

# Все варианты → каноничное имя
SEV_NORMALIZE = {
    # уже каноничные
    "КРИТИЧЕСКОЕ":        "КРИТИЧЕСКОЕ",
    "СУЩЕСТВЕННОЕ":       "СУЩЕСТВЕННОЕ",
    "ЭКОНОМИЧЕСКОЕ":      "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ":   "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ":   "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ":"ПРОВЕРИТЬ ПО СМЕЖНЫМ",
    "СНЯТО":              "СНЯТО",
    # старые русские
    "КРИТИЧНО":           "КРИТИЧЕСКОЕ",
    "СУЩЕСТВЕННО":        "СУЩЕСТВЕННОЕ",
    "РЕКОМЕНДАЦИЯ":       "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ":          "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
    # английские (из пакетного анализа тайлов)
    "CRITICAL":           "КРИТИЧЕСКОЕ",
    "SUBSTANTIAL":        "СУЩЕСТВЕННОЕ",
    "ECONOMIC":           "ЭКОНОМИЧЕСКОЕ",
    "OPERATIONAL":        "ЭКСПЛУАТАЦИОННОЕ",
    "RECOMMENDATION":     "РЕКОМЕНДАТЕЛЬНОЕ",
    "INFORMATIONAL":      "РЕКОМЕНДАТЕЛЬНОЕ",
    "CHECK_RELATED":      "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

SEV_MARK = {
    "КРИТИЧЕСКОЕ":        "🔴",
    "СУЩЕСТВЕННОЕ":       "🟡",
    "ЭКОНОМИЧЕСКОЕ":      "🟠",
    "ЭКСПЛУАТАЦИОННОЕ":   "🟡",
    "РЕКОМЕНДАТЕЛЬНОЕ":   "🔵",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ":"⚪",
    "СНЯТО":              "⬜",
}


def normalize_sev(sev: str) -> str:
    """Нормализует любой вариант severity к каноничному имени."""
    return SEV_NORMALIZE.get(sev.upper(), sev)


def load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def pipeline_status(out_dir):
    stages = {
        "00_init":          os.path.join(out_dir, "00_init.json"),
        "01_text_analysis": os.path.join(out_dir, "01_text_analysis.json"),
        "02_tiles_analysis":os.path.join(out_dir, "02_tiles_analysis.json"),
        "03_findings":      os.path.join(out_dir, "03_findings.json"),
    }
    audits = sorted([f for f in (os.listdir(out_dir) if os.path.isdir(out_dir) else [])
                     if f.startswith("audit_results")])
    return {s: ("DONE" if os.path.exists(p) else "pending") for s, p in stages.items()}, audits


def print_status(proj_dir):
    out_dir = os.path.join(proj_dir, "_output")
    info    = load_json(os.path.join(proj_dir, "project_info.json")) or {}
    status, audits = pipeline_status(out_dir)

    print(f"\nProject: {info.get('project_id', os.path.basename(proj_dir))}")
    print(f"  {info.get('description','')}")
    print(f"\nPipeline status:")
    for stage, st in status.items():
        mark = "[OK]" if st == "DONE" else "[ ]"
        print(f"  {mark} {stage}")
    if audits:
        print(f"\nAudit results:")
        for a in audits:
            size = round(os.path.getsize(os.path.join(out_dir, a)) / 1024, 1)
            print(f"  {a}  ({size} KB)")
    else:
        print(f"\n  No audit results yet.")


def print_findings(findings_data, filter_severity=None, filter_cat=None,
                   filter_sheet=None, finding_id=None, summary_only=False):
    if not findings_data:
        print("  [no findings data]")
        return

    meta = findings_data.get("meta", {})
    findings = findings_data.get("findings", [])

    # Нормализуем severity во всех замечаниях
    for f in findings:
        f["severity"] = normalize_sev(f.get("severity", ""))

    # Пересчитаем by_severity из реальных данных (meta может быть пустым)
    by_sev = {}
    for f in findings:
        s = f["severity"]
        by_sev[s] = by_sev.get(s, 0) + 1

    # Фильтры (после нормализации — точное совпадение)
    if finding_id:
        findings = [f for f in findings if f["id"] == finding_id]
    if filter_severity:
        norm_filter = normalize_sev(filter_severity)
        findings = [f for f in findings if f["severity"] == norm_filter]
    if filter_cat:
        findings = [f for f in findings if filter_cat.lower() in f.get("category", "").lower()]
    if filter_sheet:
        # Точное совпадение по номеру страницы (ищем "Лист 7" или "page_07")
        sheet_str = str(filter_sheet)
        findings = [f for f in findings
                    if f.get("sheet", "") == sheet_str
                    or f"Лист {sheet_str}" in f.get("sheet", "")
                    or f"page_{sheet_str.zfill(2)}" in f.get("sheet", "")]

    total = len(findings) if (finding_id or filter_severity or filter_cat or filter_sheet) else sum(by_sev.values())

    print(f"\nProject: {meta.get('project_id','?')}")
    print(f"Audit:   {meta.get('audit_completed','?')[:10]}")

    # Сводка по категориям
    sev_parts = []
    for s in SEV_ORDER:
        cnt = by_sev.get(s, 0)
        if cnt > 0:
            icon = SEV_MARK.get(s, "")
            sev_parts.append(f"{icon}{s}:{cnt}")
    print(f"Total:   {total} замечаний  |  {' '.join(sev_parts) if sev_parts else 'нет данных'}")

    if summary_only:
        idx = findings_data.get("quick_index", {})
        if idx.get("needs_client_action"):
            print(f"\nТребуют действий заказчика: {', '.join(idx['needs_client_action'])}")
        return

    # Сортировка по каноничному порядку
    findings.sort(key=lambda f: (SEV_ORDER.index(f["severity"]) if f["severity"] in SEV_ORDER else 99))

    print(f"\nПоказано {len(findings)} замечание(й):\n")
    for f in findings:
        icon = SEV_MARK.get(f["severity"], "  ")
        print(f"  {icon} {f['id']} — {f['severity']}")
        print(f"     Лист:       {f.get('sheet','?')}")
        print(f"     Категория:  {f.get('category','?')}")
        # Поддержка обоих ключей: "finding" (старый) и "problem"/"description" (новый)
        issue = f.get('finding') or f.get('problem') or '?'
        desc  = f.get('description', '')
        print(f"     Проблема:   {issue}")
        if desc and desc != issue:
            print(f"     Описание:   {desc[:120]}{'...' if len(desc) > 120 else ''}")
        print(f"     Норма:      {f.get('norm','?')}")
        fix = f.get('recommendation') or f.get('solution') or '?'
        print(f"     Решение:    {fix}")
        src = f.get("source", {})
        if src.get("tile"):
            print(f"     Источник:   tile/{src['tile']}")
        elif src.get("file_or_tile"):
            print(f"     Источник:   {src['file_or_tile']} (стр. {src.get('page_pdf','?')})")
        disc = f.get("md_pdf_discrepancy")
        if disc:
            print(f"     MD/PDF:     {disc.get('verdict','?')}")
        print()


def list_all_projects():
    proj_root = os.path.join(BASE, "projects")
    if not os.path.isdir(proj_root):
        print(f"No projects/ folder found at: {proj_root}")
        return

    print(f"\nAll projects in: {proj_root}\n")
    for name, proj_dir in _iter_project_dirs(proj_root):
        out_dir = os.path.join(proj_dir, "_output")
        status, audits = pipeline_status(out_dir)
        done = sum(1 for s in status.values() if s == "DONE")
        total = len(status)

        info = load_json(os.path.join(proj_dir, "project_info.json")) or {}
        has_pdf = os.path.exists(os.path.join(proj_dir, "document.pdf"))

        findings_file = os.path.join(out_dir, "03_findings.json")
        findings_data = load_json(findings_file)
        findings_summary = ""
        if findings_data:
            # Пересчитываем из реальных данных
            by_sev = {}
            for f in findings_data.get("findings", []):
                s = normalize_sev(f.get("severity", ""))
                by_sev[s] = by_sev.get(s, 0) + 1
            SEV_SHORT = {
                "КРИТИЧЕСКОЕ": "Крит", "СУЩЕСТВЕННОЕ": "Сущ",
                "ЭКОНОМИЧЕСКОЕ": "Эконом", "ЭКСПЛУАТАЦИОННОЕ": "Экспл",
                "РЕКОМЕНДАТЕЛЬНОЕ": "Рек", "ПРОВЕРИТЬ ПО СМЕЖНЫМ": "Смеж",
                "СНЯТО": "Снято",
            }
            parts = []
            for s in SEV_ORDER:
                cnt = by_sev.get(s, 0)
                if cnt > 0:
                    icon = SEV_MARK.get(s, "")
                    label = SEV_SHORT.get(s, s[:4])
                    parts.append(f"{icon}{label}:{cnt}")
            findings_summary = f"  {' '.join(parts)}" if parts else "  нет замечаний"

        pdf_mark = "[PDF]" if has_pdf else "[---]"
        pipe_mark = f"[{done}/{total} stages]"
        audit_mark = f"[{len(audits)} audits]" if audits else "[no audit]"

        print(f"  {pdf_mark} {pipe_mark} {audit_mark}  {name}")
        if info.get("description"):
            print(f"           {info['description']}")
        if findings_summary:
            print(f"           Findings:{findings_summary}")


def main():
    parser = argparse.ArgumentParser(description="Query project audit findings from JSON pipeline")
    parser.add_argument("project", nargs="?", default=None,
                        help="Project folder path (relative or absolute). Omit to list all.")
    parser.add_argument("--status",   action="store_true", help="Show pipeline stage status")
    parser.add_argument("--critical", action="store_true", help="Show only КРИТИЧЕСКОЕ findings")
    parser.add_argument("--cat",      default=None,        help="Filter by category")
    parser.add_argument("--sheet",    default=None,        help="Filter by sheet/page number")
    parser.add_argument("--id",       default=None,        help="Show single finding by ID")
    parser.add_argument("--summary",  action="store_true", help="Brief summary only")
    args = parser.parse_args()

    if args.project is None:
        list_all_projects()
        return

    # Resolve project path
    proj_dir = args.project
    if not os.path.isabs(proj_dir):
        proj_dir = os.path.join(BASE, proj_dir)

    if not os.path.isdir(proj_dir):
        # Try searching in projects/
        candidate = os.path.join(BASE, "projects", args.project)
        if os.path.isdir(candidate):
            proj_dir = candidate
        else:
            print(f"[ERROR] Project folder not found: {proj_dir}")
            sys.exit(1)

    out_dir       = os.path.join(proj_dir, "_output")
    findings_file = os.path.join(out_dir, "03_findings.json")

    if args.status:
        print_status(proj_dir)
        return

    findings_data = load_json(findings_file)
    if not findings_data:
        print(f"[!] 03_findings.json not found at: {findings_file}")
        print(f"    Run audit first: .\\run_all_projects.ps1 -Only \"{os.path.basename(proj_dir)}\"")
        print_status(proj_dir)
        return

    print_findings(
        findings_data,
        filter_severity="КРИТИЧЕСКОЕ" if args.critical else None,
        filter_cat=args.cat,
        filter_sheet=args.sheet,
        finding_id=args.id,
        summary_only=args.summary,
    )


if __name__ == "__main__":
    main()
