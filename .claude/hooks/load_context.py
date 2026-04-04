# -*- coding: utf-8 -*-
"""
load_context.py -- SessionStart hook (multi-project version)
Runs automatically on every Claude Code session start.
Scans projects/ folder and reports status of all projects.
"""

import os
import sys
import json

sys.stdout.reconfigure(encoding="utf-8")

BASE        = r"D:\Отдел Системного Анализа\1. Audit Manager"
PROJ_ROOT   = os.path.join(BASE, "projects")
NORM_FILE   = os.path.join(BASE, "norms_reference.md")


def check(path):
    return os.path.exists(path)


def file_size_kb(path):
    try:
        return round(os.path.getsize(path) / 1024, 0)
    except:
        return 0


def _iter_project_dirs():
    """Рекурсивно найти все папки проектов (включая подпапки-группы)."""
    results = []
    for name in sorted(os.listdir(PROJ_ROOT)):
        entry = os.path.join(PROJ_ROOT, name)
        if not os.path.isdir(entry) or name.startswith("_"):
            continue
        info = os.path.join(entry, "project_info.json")
        has_pdf = any(f.endswith(".pdf") for f in os.listdir(entry))
        if os.path.exists(info) or has_pdf:
            results.append((name, entry))
        else:
            # Подпапка-группа — заходим внутрь
            for sub in sorted(os.listdir(entry)):
                sub_path = os.path.join(entry, sub)
                if os.path.isdir(sub_path) and not sub.startswith("_"):
                    results.append((sub, sub_path))
    return results


def scan_projects():
    """Scan projects/ folder and return list of project status dicts."""
    if not os.path.isdir(PROJ_ROOT):
        return []

    projects = []
    for name, proj_dir in _iter_project_dirs():
        pdf_path  = os.path.join(proj_dir, "document.pdf")
        info_path = os.path.join(proj_dir, "project_info.json")
        out_dir   = os.path.join(proj_dir, "_output")
        txt_path  = os.path.join(out_dir, "extracted_text.txt")
        tiles_dir = os.path.join(out_dir, "tiles")

        # Count tiles
        tiles_n = 0
        if os.path.isdir(tiles_dir):
            for d in os.listdir(tiles_dir):
                dpath = os.path.join(tiles_dir, d)
                if os.path.isdir(dpath):
                    tiles_n += sum(1 for f in os.listdir(dpath) if f.endswith(".png"))

        # Find latest audit result
        latest_audit = None
        if os.path.isdir(out_dir):
            audits = sorted([f for f in os.listdir(out_dir) if f.startswith("audit_results")])
            if audits:
                latest_audit = audits[-1]

        # Load project metadata
        proj_name = name
        proj_desc = ""
        if os.path.exists(info_path):
            try:
                with open(info_path, "r", encoding="utf-8") as f:
                    info = json.load(f)
                proj_name = info.get("name", name)
                proj_desc = info.get("description", "")
            except:
                pass

        projects.append({
            "folder":       name,
            "name":         proj_name,
            "description":  proj_desc,
            "has_pdf":      check(pdf_path),
            "has_txt":      check(txt_path),
            "txt_size_kb":  file_size_kb(txt_path),
            "tiles_n":      tiles_n,
            "latest_audit": latest_audit,
        })

    return projects


# ── Build context output ─────────────────────────────────────────────────────
projects = scan_projects()
norm_ok  = check(NORM_FILE)

lines = [
    "=" * 65,
    "PROJECT AUDIT SYSTEM — Session Context (auto-loaded)",
    "=" * 65,
    "",
    f"Norms reference:  {'[OK]' if norm_ok else '[MISSING] -> check norms_reference.md'}",
    f"Projects folder:  {PROJ_ROOT}",
    f"Total projects:   {len(projects)}",
    "",
]

if projects:
    lines.append("── PROJECTS STATUS ──────────────────────────────────────────────")
    for p in projects:
        pdf_s   = "[OK]      " if p["has_pdf"]  else "[NO PDF]  "
        txt_s   = f"[TXT:{int(p['txt_size_kb'])}KB]" if p["has_txt"] else "[NO TXT]  "
        tiles_s = f"[{p['tiles_n']} tiles]" if p["tiles_n"] > 0 else "[NO TILES]"
        audit_s = f"[audit: {p['latest_audit']}]" if p["latest_audit"] else "[no audit yet]"

        lines.append(f"")
        lines.append(f"  [{p['folder']}]")
        lines.append(f"    Name:   {p['name']} — {p['description']}")
        lines.append(f"    PDF:    {pdf_s}  TXT: {txt_s}  Tiles: {tiles_s}")
        lines.append(f"    Audit:  {audit_s}")
else:
    lines.append("  No projects found.")
    lines.append(f"  Add project folders to: {PROJ_ROOT}")
    lines.append("  Each folder needs: document.pdf + project_info.json")

lines += [
    "",
    "── HOW TO WORK ──────────────────────────────────────────────────",
    "  Single project:  python process_project.py projects/<name>",
    "  All projects:    python process_project.py",
    "  Batch audit:     .\\run_all_projects.ps1",
    "",
    "── AUTONOMOUS MODE RULES ────────────────────────────────────────",
    "  1. Work autonomously — do NOT ask questions during analysis",
    "  2. All tools pre-approved in .claude/settings.json",
    "  3. Run python scripts without asking permission",
    "  4. Complete ALL checklist items before reporting results",
    "  5. Priority: PDF > TXT > MD (MD may have OCR errors)",
    "",
    "── PROJECT FILE PATHS ───────────────────────────────────────────",
    "  projects/<name>/document.pdf              <- input (source of truth)",
    "  projects/<name>/project_info.json         <- tile config, metadata",
    "  projects/<name>/_output/extracted_text.txt <- extracted PDF text",
    "  projects/<name>/_output/tiles/page_XX/    <- drawing tiles",
    "  projects/<name>/_output/audit_results_*.md <- audit output",
    "=" * 65,
]

# Read stdin (hook mode - Claude Code sends JSON event)
try:
    sys.stdin.read()
except:
    pass

print("\n".join(lines))
