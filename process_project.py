"""
process_project.py
------------------
Подготовка проекта: детект MD-файла, построение Document Knowledge Graph.

Usage:
  python process_project.py <project_folder>
  python process_project.py projects/133-23-GK-EM1

The project folder must contain:
  - *_result.json      (canonical JSON от Chandra OCR)
  - *_document.md      (MD-файл от Chandra OCR)
  - project_info.json  (optional, uses defaults if absent)

Output is created in:
  <project_folder>/_output/
    document_graph.json
"""

import os
import sys
import json
import argparse
import re

from graph_builder import build_document_graph_v2, generate_locality_debug

BASE_DIR = r"D:\Отдел Системного Анализа\1. Audit Manager"







def enrich_document_graph(output_dir):
    """
    Обогатить document_graph.json данными из blocks/index.json.

    Добавляет file и size_kb к image_blocks по совпадению block_id.
    Вызывается после blocks.py crop (когда graph уже построен, а блоки скачаны позже).
    """
    graph_path = os.path.join(output_dir, "document_graph.json")
    index_path = os.path.join(output_dir, "blocks", "index.json")

    if not os.path.exists(graph_path) or not os.path.exists(index_path):
        return

    with open(graph_path, "r", encoding="utf-8") as f:
        graph = json.load(f)
    with open(index_path, "r", encoding="utf-8") as f:
        index = json.load(f)

    index_map = {}
    for b in index.get("blocks", []):
        bid = b.get("block_id", "")
        if bid:
            index_map[bid] = b

    enriched = 0
    for page in graph.get("pages", []):
        for img_block in page.get("image_blocks", []):
            bid = img_block.get("id", "")
            if bid in index_map:
                idx_entry = index_map[bid]
                img_block["file"] = idx_entry.get("file")
                img_block["size_kb"] = idx_entry.get("size_kb")
                enriched += 1

    graph["blocks_enriched"] = enriched

    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    print(f"  [GRAPH] Обогащено {enriched} блоков данными из index.json")



# ─────────────────────────────────────────────────────────────────────────────

def detect_md_file(project_dir, pdf_name):
    """
    Автодетекция MD-файла (структурированный текст документа) рядом с PDF.

    Приоритет поиска:
      1. <имя_pdf>_document.md  (точный паттерн)
      2. Любой *_document.md в папке
      3. Единственный .md файл (не audit_, не CLAUDE.md, не README)

    Returns:
        (filename, size_kb) или (None, 0)
    """
    exclude_prefixes = ("audit_", "readme", "claude")
    exclude_names = {"CLAUDE.md", "README.md"}

    # Приоритет 1: точное совпадение <pdf_stem>_document.md
    pdf_stem = os.path.splitext(pdf_name)[0]
    exact_name = pdf_stem + "_document.md"
    exact_path = os.path.join(project_dir, exact_name)
    if os.path.exists(exact_path):
        size_kb = round(os.path.getsize(exact_path) / 1024, 1)
        return exact_name, size_kb

    # Приоритет 2: любой *_document.md
    for f in sorted(os.listdir(project_dir)):
        if f.endswith("_document.md") and not f.lower().startswith(exclude_prefixes):
            fpath = os.path.join(project_dir, f)
            if os.path.isfile(fpath):
                size_kb = round(os.path.getsize(fpath) / 1024, 1)
                return f, size_kb

    # Приоритет 3: единственный .md файл (не служебный)
    md_files = [
        f for f in os.listdir(project_dir)
        if f.endswith(".md")
        and f not in exclude_names
        and not f.lower().startswith(exclude_prefixes)
        and os.path.isfile(os.path.join(project_dir, f))
    ]
    if len(md_files) == 1:
        fpath = os.path.join(project_dir, md_files[0])
        size_kb = round(os.path.getsize(fpath) / 1024, 1)
        return md_files[0], size_kb

    return None, 0


def detect_all_md_files(project_dir, info):
    """Найти все MD-файлы для проекта (поддержка нескольких PDF).

    Приоритет:
      1. md_files из project_info.json (если есть)
      2. Автодетекция для каждого PDF из pdf_files
      3. Fallback на detect_md_file (один файл)

    Returns:
        list[tuple[str, float]] — [(filename, size_kb), ...]
    """
    # Приоритет 1: явный список в project_info
    md_files_list = info.get("md_files", [])
    if md_files_list:
        result = []
        for mf in md_files_list:
            fpath = os.path.join(project_dir, mf)
            if os.path.isfile(fpath):
                size_kb = round(os.path.getsize(fpath) / 1024, 1)
                result.append((mf, size_kb))
        if result:
            return result

    # Приоритет 2: автодетекция по каждому PDF
    pdf_files = info.get("pdf_files", [])
    if not pdf_files:
        pf = info.get("pdf_file", "document.pdf")
        pdf_files = [pf] if pf else []

    if len(pdf_files) > 1:
        result = []
        seen = set()
        for pf in pdf_files:
            md_name, md_size = detect_md_file(project_dir, pf)
            if md_name and md_name not in seen:
                result.append((md_name, md_size))
                seen.add(md_name)
        if result:
            return result

    # Приоритет 3: обычная автодетекция (один файл)
    pdf_name = info.get("pdf_file", "document.pdf")
    md_name, md_size = detect_md_file(project_dir, pdf_name)
    if md_name:
        return [(md_name, md_size)]
    return []


def load_project_info(project_dir):
    info_path = os.path.join(project_dir, "project_info.json")
    if os.path.exists(info_path):
        with open(info_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "project_id": os.path.basename(project_dir),
        "pdf_file": "document.pdf",
    }


def save_project_info(project_dir, info):
    """Сохраняет обновлённый project_info.json обратно в папку проекта."""
    info_path = os.path.join(project_dir, "project_info.json")
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)
    print(f"  [SAVED] project_info.json updated")


def process(project_dir, force=False):
    """Подготовка проекта: детект MD, построение Document Knowledge Graph."""
    info     = load_project_info(project_dir)
    pdf_name = info.get("pdf_file", "document.pdf")
    pdf_path = os.path.join(project_dir, pdf_name)

    # Проверяем наличие хотя бы одного PDF
    pdf_files = info.get("pdf_files", [pdf_name])
    has_any_pdf = any(
        os.path.exists(os.path.join(project_dir, pf)) for pf in pdf_files
    )
    if not has_any_pdf:
        print(f"  [ERROR] PDF not found: {pdf_path}")
        return False

    out_dir = os.path.join(project_dir, "_output")
    os.makedirs(out_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  PROJECT: {info.get('project_id', os.path.basename(project_dir))}")
    if len(pdf_files) > 1:
        print(f"  PDF:     {', '.join(pdf_files)} (multi-PDF)")
    else:
        print(f"  PDF:     {pdf_path}")
    print(f"{'='*60}")

    # ── Step 0: Detect MD file(s) ──
    all_md = detect_all_md_files(project_dir, info)

    if not all_md:
        print(f"  [ERROR] MD-файл не найден в {project_dir}")
        print(f"  Создайте MD-файл через Chandra OCR и положите в папку проекта.")
        return False

    if len(all_md) == 1:
        md_file, md_size_kb = all_md[0]
        info["md_file"] = md_file
        info["md_file_size_kb"] = md_size_kb
        print(f"  [MD] Found: {md_file} ({md_size_kb} KB)")
    else:
        # Несколько MD — конкатенируем во временный файл
        combined_name = "_combined_document.md"
        combined_path = os.path.join(out_dir, combined_name)
        with open(combined_path, "w", encoding="utf-8") as out_f:
            for i, (mf, sz) in enumerate(all_md):
                mpath = os.path.join(project_dir, mf)
                with open(mpath, "r", encoding="utf-8") as in_f:
                    content = in_f.read()
                if i > 0:
                    out_f.write("\n\n")
                out_f.write(content)
                print(f"  [MD] Part {i+1}: {mf} ({sz} KB)")

        md_size_kb = round(os.path.getsize(combined_path) / 1024, 1)
        info["md_file"] = all_md[0][0]
        info["md_files"] = [mf for mf, _ in all_md]
        info["md_file_size_kb"] = md_size_kb
        print(f"  [MD] Combined: {len(all_md)} файлов -> {md_size_kb} KB")

    info["text_source"] = "md"
    save_project_info(project_dir, info)
    print(f"  [OK] MD — первичный источник текста")

    # ── Step 1: Build Document Knowledge Graph (v2, из *_result.json) ──
    graph_v2 = build_document_graph_v2(project_dir, out_dir)
    if graph_v2:
        debug_path = generate_locality_debug(graph_v2, out_dir)
        if debug_path:
            print(f"  [GRAPH v2] Debug: {debug_path.name}")
    else:
        project_id = info.get("project_id", os.path.basename(project_dir))
        print(f"  [ERROR] [{project_id}] *_result.json не найден — document_graph не построен")
        return False

    print(f"\n  DONE: {info.get('project_id', '')}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Подготовка проекта: детект MD, построение Document Knowledge Graph")
    parser.add_argument("project_dir", nargs="?", default=None,
                        help="Path to project folder (default: scan projects/ dir)")
    parser.add_argument("--force", action="store_true",
                        help="Re-create even if already exists")
    args = parser.parse_args()

    if args.project_dir:
        project_dir = args.project_dir
        if not os.path.isabs(project_dir):
            project_dir = os.path.join(BASE_DIR, project_dir)
        process(project_dir, force=args.force)
    else:
        # Process all projects in projects/ folder
        projects_root = os.path.join(BASE_DIR, "projects")
        if not os.path.isdir(projects_root):
            print(f"[ERROR] projects/ folder not found: {projects_root}")
            sys.exit(1)

        projects = sorted([
            os.path.join(projects_root, d)
            for d in os.listdir(projects_root)
            if os.path.isdir(os.path.join(projects_root, d))
            and os.path.exists(os.path.join(projects_root, d, "project_info.json"))
        ])

        if not projects:
            print("No projects found in projects/ folder.")
            sys.exit(1)

        print(f"Found {len(projects)} project(s):")
        for p in projects:
            print(f"  - {os.path.basename(p)}")

        for project_dir in projects:
            process(project_dir, force=args.force)

        print(f"\n{'='*60}")
        print(f"ALL PROJECTS PROCESSED: {len(projects)} total")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
