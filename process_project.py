"""
process_project.py
------------------
Universal project processor: extracts PDF text + renders tiles.
Works with any project in the projects/ folder.

Usage:
  python process_project.py <project_folder>
  python process_project.py projects/133-23-GK-EM1
  python process_project.py projects/MyNewProject

The project folder must contain:
  - document.pdf        (or whatever is set in project_info.json)
  - project_info.json   (optional, uses defaults if absent)

Output is created in:
  <project_folder>/_output/
    extracted_text.txt
    tiles/page_XX/page_XX_rYcZ.png
    pages_png/page_XX.png   (optional, --full-pages flag)
"""

import fitz
import os
import sys
import json
import argparse
import math
import re
import shutil

# pdf_text_utils больше не используется — текст берётся из MD-файла (Chandra OCR)

# graph_builder v2: canonical JSON → document_graph
try:
    from graph_builder import build_document_graph_v2, generate_locality_debug
except ImportError:
    build_document_graph_v2 = None
    generate_locality_debug = None
    print("  [WARNING] graph_builder.py not found — document_graph v2 unavailable, using MD-only v1")

BASE_DIR = r"D:\Отедел Системного Анализа\1. Calude code"

# Default tile config: page -> (rows, cols, scale, overlap_pct)
DEFAULT_TILE_CONFIG = {
    "7":  [2, 4, 3.5, 8, "Odnolineinaya schema"],
    "8":  [3, 4, 3.0, 8, "Plan - level 1"],
    "9":  [2, 4, 3.5, 8, "Panel schedules"],
    "11": [3, 4, 3.0, 8, "Plan - all systems"],
    "13": [2, 3, 3.0, 8, "Main cable routes"],
    "14": [2, 3, 3.5, 8, "Entry point + table"],
}

# ─── Адаптивная нарезка тайлов ────────────────────────────────────────────
# Claude масштабирует изображения так, что длинная сторона ≤ ~1568 px.
# Тайлы >2000 px теряют детализацию при downscale.
# Оптимальный размер тайла для Claude: 1200-1800 px.

AREA_SKIP = 550_000   # A4 и ниже — текстовая страница, не нарезаем

QUALITY_PROFILES = {
    "draft": {
        "target_tile_px": 1800,   # целевой размер тайла (px)
        "max_tile_px":    2500,   # абсолютный максимум
        "scale":          2.5,    # масштаб рендеринга (180 DPI)
        "overlap_pct":    5,      # перекрытие между тайлами (%)
    },
    "standard": {
        "target_tile_px": 1500,
        "max_tile_px":    2000,
        "scale":          3.0,    # 216 DPI
        "overlap_pct":    5,
    },
    "high": {
        "target_tile_px": 1200,
        "max_tile_px":    1600,
        "scale":          3.0,    # 216 DPI
        "overlap_pct":    5,
    },
    "detailed": {
        "target_tile_px": 1400,   # оптимум: чуть ниже лимита Claude (1568px)
        "max_tile_px":    1560,   # жёсткий лимит — Claude НЕ уменьшает
        "scale":          3.5,    # 252 DPI — на 18% чётче чем standard/high
        "overlap_pct":    5,
    },
    "speed": {
        "target_tile_px": 2500,   # крупные тайлы — меньше нарезка, быстрее аудит
        "max_tile_px":    3500,   # Claude уменьшит до 1568 — это ОК
        "scale":          2.5,    # 180 DPI — достаточно для основного текста
        "overlap_pct":    3,      # минимальный overlap
    },
}

MAX_TILES_PER_PAGE = 80  # увеличено для detailed-профиля (A1 ~48, A0 ~33)


def compute_adaptive_grid(page_width_pt, page_height_pt, quality="standard"):
    """
    Вычисляет оптимальную сетку нарезки исходя из размеров страницы
    и целевого размера тайла для LLM (Claude).

    Claude масштабирует изображения так, что длинная сторона ≤ 1568 px.
    Тайлы >2000 px теряют детализацию. Оптимум: 1200-1800 px.

    Args:
        page_width_pt:  ширина страницы в PDF-точках (72 pt = 1 дюйм)
        page_height_pt: высота страницы в PDF-точках
        quality:        профиль качества ("draft", "standard", "high")

    Returns:
        (rows, cols, scale, overlap_pct) или None если страница текстовая (≤ A4)
    """
    area = page_width_pt * page_height_pt
    if area <= AREA_SKIP:
        return None  # текстовая страница

    profile     = QUALITY_PROFILES.get(quality, QUALITY_PROFILES["standard"])
    scale       = profile["scale"]
    target      = profile["target_tile_px"]
    max_px      = profile["max_tile_px"]
    overlap_pct = profile["overlap_pct"]

    full_w_px = page_width_pt * scale
    full_h_px = page_height_pt * scale

    # ceil гарантирует, что ни один тайл (с учётом overlap) не превысит max_px
    # overlap добавляет ~2*overlap_pct% к размеру тайла → корректируем max_px
    effective_max = max_px / (1 + 2 * overlap_pct / 100)
    cols_min = max(1, math.ceil(full_w_px / effective_max))
    rows_min = max(1, math.ceil(full_h_px / effective_max))

    # round приближает размер тайла к target (может дать больше тайлов)
    cols_target = max(1, round(full_w_px / target))
    rows_target = max(1, round(full_h_px / target))

    # Берём максимум: гарантируем и ≤max, и близость к target
    cols = max(cols_min, cols_target)
    rows = max(rows_min, rows_target)

    # Защита от слишком мелкой нарезки на ленточных/огромных страницах
    if rows * cols > MAX_TILES_PER_PAGE:
        factor = math.sqrt(rows * cols / MAX_TILES_PER_PAGE)
        cols = max(1, round(cols / factor))
        rows = max(1, round(rows / factor))

    return rows, cols, scale, overlap_pct


def auto_configure_tiles(pdf_path, skip_first_n=0, quality="standard"):
    """
    Автоматически определяет tile_config по размерам страниц PDF.
    Использует адаптивный алгоритм: вычисляет сетку на основе целевого
    размера тайла (оптимально для анализа Claude).

    Профили качества:
      draft    → крупные тайлы (~1800 px), быстро, 8% overlap
      standard → оптимальные (~1500 px), баланс качества и количества, 10% overlap
      high     → мелкие тайлы (~1200 px), максимум деталей, 12% overlap

    Пропускает текстовые страницы (≤ A4 по площади).
    """
    doc = fitz.open(pdf_path)
    tile_cfg = {}
    for i in range(len(doc)):
        page = doc[i]
        page_num = i + 1

        if page_num <= skip_first_n:
            continue

        result = compute_adaptive_grid(
            page.rect.width, page.rect.height, quality=quality
        )
        if result is None:
            continue  # текстовая страница

        rows, cols, scale, overlap_pct = result
        tile_cfg[str(page_num)] = [rows, cols, scale, overlap_pct, f"page_{page_num}"]

    doc.close()
    return tile_cfg

# ─── MD-ориентированная классификация страниц ────────────────────────────────

def analyze_md_pages(md_path):
    """
    Анализирует MD-файл и определяет тип контента каждой страницы.
    MD-файл создаётся внешним инструментом (Chandra) и содержит маркеры:
      ## СТРАНИЦА N        — начало страницы
      ### BLOCK [TEXT]: ID  — текстовый блок
      ### BLOCK [IMAGE]: ID — графический блок (чертёж, план, схема)

    Returns:
        dict: {page_num: {"has_text": bool, "has_image": bool, "image_types": [str]}}
    """
    pages = {}
    current_page = None

    with open(md_path, "r", encoding="utf-8") as f:
        for line in f:
            # Заголовок страницы: ## СТРАНИЦА 7
            page_match = re.match(r'^## СТРАНИЦА\s+(\d+)', line)
            if page_match:
                current_page = int(page_match.group(1))
                pages[current_page] = {
                    "has_text": False,
                    "has_image": False,
                    "image_types": [],
                }
                continue

            if current_page is None:
                continue

            # Блок TEXT
            if re.match(r'^### BLOCK \[TEXT\]:', line):
                pages[current_page]["has_text"] = True
                continue

            # Блок IMAGE
            if re.match(r'^### BLOCK \[IMAGE\]:', line):
                pages[current_page]["has_image"] = True
                continue

            # Тип изображения: **[ИЗОБРАЖЕНИЕ]** | Тип: План этажа | Оси: ...
            type_match = re.match(
                r'^\*\*\[ИЗОБРАЖЕНИЕ\]\*\*\s*\|\s*Тип:\s*([^|]+)', line
            )
            if type_match and current_page in pages:
                img_type = type_match.group(1).strip()
                if img_type and img_type not in pages[current_page]["image_types"]:
                    pages[current_page]["image_types"].append(img_type)

    return pages


def build_document_graph(md_path, output_dir):
    """
    Преобразует MD-файл в структурированный Document Knowledge Graph.

    Парсит MD построчно, извлекая для каждой страницы:
    - sheet_no, sheet_name (метаданные из штампа)
    - text_blocks [{id, text}] — полный текст каждого TEXT-блока
    - image_blocks [{id, type, ocr}] — OCR-описания IMAGE-блоков

    Результат сохраняется в _output/document_graph.json.
    Обогащается файлами блоков после blocks.py crop (enrich_document_graph).

    Returns:
        dict: document_graph или None при ошибке
    """
    if not os.path.exists(md_path):
        return None

    pages = []
    pages_by_num = {}  # page_num → index in pages[]

    current_page_num = 0
    current_page = None
    current_block_type = None  # "text" | "image"
    current_block_id = ""
    current_block_lines = []
    current_image_type = None

    def _flush_block():
        nonlocal current_block_type, current_block_lines, current_block_id, current_image_type
        if not current_block_type or not current_page:
            current_block_type = None
            current_block_lines = []
            current_block_id = ""
            current_image_type = None
            return

        text = "\n".join(current_block_lines).strip()

        if current_block_type == "text" and text:
            # Пропускаем ошибки и пустые блоки
            if "[Ошибка" not in text and "*(нет данных)*" not in text:
                current_page["text_blocks"].append({
                    "id": current_block_id,
                    "text": text,
                })

        elif current_block_type == "image":
            # Извлекаем тип из OCR-описания если ещё не определён
            img_type = current_image_type
            entry = {
                "id": current_block_id,
                "type": img_type,
                "ocr": text if text else None,
                # Обогащаются после blocks.py crop:
                "file": None,
                "size_kb": None,
            }
            current_page["image_blocks"].append(entry)

        current_block_type = None
        current_block_lines = []
        current_block_id = ""
        current_image_type = None

    with open(md_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            # Начало новой страницы
            page_match = re.match(r'^## СТРАНИЦА\s+(\d+)', line)
            if page_match:
                _flush_block()
                current_page_num = int(page_match.group(1))
                current_page = {
                    "page": current_page_num,
                    "sheet_no": None,
                    "sheet_name": None,
                    "text_blocks": [],
                    "image_blocks": [],
                }
                pages.append(current_page)
                pages_by_num[current_page_num] = len(pages) - 1
                continue

            if current_page is None:
                continue

            # Метаданные страницы
            if line.startswith("**Лист:**"):
                current_page["sheet_no"] = line.replace("**Лист:**", "").strip()
                continue
            if line.startswith("**Наименование листа:**"):
                current_page["sheet_name"] = line.replace("**Наименование листа:**", "").strip()
                continue

            # Начало TEXT-блока
            if line.startswith("### BLOCK [TEXT]:"):
                _flush_block()
                current_block_type = "text"
                current_block_id = line.split(":", 1)[-1].strip()
                current_block_lines = []
                continue

            # Начало IMAGE-блока
            if line.startswith("### BLOCK [IMAGE]:"):
                _flush_block()
                current_block_type = "image"
                current_block_id = line.split(":", 1)[-1].strip()
                current_block_lines = []
                current_image_type = None
                continue

            # Любой другой блок — сбрасываем
            if line.startswith("### BLOCK "):
                _flush_block()
                continue

            # Тип изображения внутри IMAGE-блока
            if current_block_type == "image":
                type_match = re.match(
                    r'^\*\*\[ИЗОБРАЖЕНИЕ\]\*\*\s*\|\s*Тип:\s*([^|]+)', line
                )
                if type_match:
                    current_image_type = type_match.group(1).strip()

            # Накапливаем строки текущего блока
            if current_block_type:
                current_block_lines.append(line)

    _flush_block()  # последний блок

    graph = {
        "version": 1,
        "total_pages": len(pages),
        "total_text_blocks": sum(len(p["text_blocks"]) for p in pages),
        "total_image_blocks": sum(len(p["image_blocks"]) for p in pages),
        "pages": pages,
    }

    # Сохраняем
    os.makedirs(output_dir, exist_ok=True)
    graph_path = os.path.join(output_dir, "document_graph.json")
    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    print(f"  [GRAPH] document_graph.json: {len(pages)} страниц, "
          f"{graph['total_text_blocks']} текст., {graph['total_image_blocks']} граф.")
    return graph


def enrich_document_graph(output_dir):
    """
    Обогатить document_graph.json данными из blocks/index.json.

    Добавляет file и size_kb к image_blocks по совпадению block_id.
    Вызывается после blocks.py crop.
    """
    graph_path = os.path.join(output_dir, "document_graph.json")
    index_path = os.path.join(output_dir, "blocks", "index.json")

    if not os.path.exists(graph_path) or not os.path.exists(index_path):
        return

    with open(graph_path, "r", encoding="utf-8") as f:
        graph = json.load(f)
    with open(index_path, "r", encoding="utf-8") as f:
        index = json.load(f)

    # Построить маппинг block_id → данные из index
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


def auto_configure_tiles_from_md(pdf_path, md_pages, quality="standard"):
    """
    Конфигурация тайлов на основе MD-анализа:
    нарезать ТОЛЬКО страницы с IMAGE-блоками.

    Для каждой IMAGE-страницы использует compute_adaptive_grid()
    по реальным размерам из PDF.

    Страницы <=A4 с IMAGE получают сетку 1x1 (один тайл целиком).

    Args:
        pdf_path: путь к PDF (нужен для размеров страниц)
        md_pages: результат analyze_md_pages()
        quality: профиль качества

    Returns:
        dict: tile_config (тот же формат что auto_configure_tiles)
    """
    doc = fitz.open(pdf_path)
    tile_cfg = {}
    profile = QUALITY_PROFILES.get(quality, QUALITY_PROFILES["standard"])

    for page_num, info in md_pages.items():
        if not info["has_image"]:
            continue  # чисто текстовая — пропускаем

        page_idx = page_num - 1
        if page_idx < 0 or page_idx >= len(doc):
            continue

        page = doc[page_idx]
        label = ", ".join(info["image_types"][:3]) or f"page_{page_num}"

        result = compute_adaptive_grid(
            page.rect.width, page.rect.height, quality=quality
        )

        if result is None:
            # Страница <=A4, но MD говорит — там есть графика → 1x1 тайл
            # Ограничиваем scale, чтобы тайл не превысил max_tile_px
            max_px = profile["max_tile_px"]
            max_page_dim = max(page.rect.width, page.rect.height)
            safe_scale = min(profile["scale"], max_px / max_page_dim) if max_page_dim > 0 else profile["scale"]
            tile_cfg[str(page_num)] = [
                1, 1, round(safe_scale, 2), 0, label  # overlap=0 для 1x1
            ]
        else:
            rows, cols, scale, overlap_pct = result
            tile_cfg[str(page_num)] = [rows, cols, scale, overlap_pct, label]

    doc.close()
    return tile_cfg


# ─── Инкрементальная нарезка выбранных страниц ───────────────────────────────

def tile_specific_pages(project_dir, pages_list, quality="speed", force=False):
    """
    Нарезать тайлы ТОЛЬКО для указанных страниц (инкрементально).

    Не удаляет существующие тайлы других страниц.
    Если тайлы для запрошенной страницы уже есть и force=False — пропускает.

    Args:
        project_dir: путь к папке проекта
        pages_list: список номеров страниц [7, 9, 11]
        quality: профиль качества
        force: пересоздать даже если тайлы уже существуют

    Returns:
        dict: {page_num: tile_count} — сколько тайлов создано для каждой страницы
    """
    info = load_project_info(project_dir)
    pdf_name = info.get("pdf_file", "document.pdf")
    pdf_path = os.path.join(project_dir, pdf_name)

    if not os.path.exists(pdf_path):
        print(f"  [ERROR] PDF not found: {pdf_path}")
        return {}

    out_dir = os.path.join(project_dir, "_output")
    tiles_dir = os.path.join(out_dir, "tiles")
    os.makedirs(tiles_dir, exist_ok=True)

    doc = fitz.open(pdf_path)
    total_pdf_pages = len(doc)
    tile_cfg = info.get("tile_config", {})
    result = {}

    print(f"\n  [SMART] Инкрементальная нарезка: страницы {pages_list}")

    for page_num in pages_list:
        if page_num < 1 or page_num > total_pdf_pages:
            print(f"  [WARN] Страница {page_num} вне диапазона (1-{total_pdf_pages})")
            continue

        page_dir = os.path.join(tiles_dir, f"page_{page_num:02d}")

        # Проверка: уже нарезана?
        if not force and os.path.isdir(page_dir) and any(
            f.endswith(".png") for f in os.listdir(page_dir)
        ):
            existing = len([f for f in os.listdir(page_dir) if f.endswith(".png")])
            print(f"  [SKIP] Страница {page_num} — уже нарезана ({existing} тайлов)")
            result[page_num] = existing
            continue

        # Вычислить сетку для этой страницы
        page = doc[page_num - 1]
        grid = compute_adaptive_grid(page.rect.width, page.rect.height, quality=quality)

        if grid is None:
            # Страница <= A4 — один тайл целиком
            profile = QUALITY_PROFILES.get(quality, QUALITY_PROFILES["standard"])
            max_px = profile["max_tile_px"]
            max_dim = max(page.rect.width, page.rect.height)
            safe_scale = min(profile["scale"], max_px / max_dim) if max_dim > 0 else profile["scale"]
            rows, cols, scale, overlap_pct = 1, 1, round(safe_scale, 2), 0
            label = f"page_{page_num}"
        else:
            rows, cols, scale, overlap_pct = grid
            label = f"page_{page_num}"

        # Очистка при force
        if force and os.path.isdir(page_dir):
            old_files = [f for f in os.listdir(page_dir) if f.endswith(".png") or f == "index.json"]
            for f in old_files:
                os.remove(os.path.join(page_dir, f))

        print(f"  Tiling page {page_num} ({rows}x{cols}, scale={scale}):")
        n = tile_page(doc, page_num - 1, rows, cols, scale, overlap_pct, label, tiles_dir)
        result[page_num] = n

        # Обновить tile_config (инкрементально)
        tile_cfg[str(page_num)] = [rows, cols, scale, overlap_pct, label]

    doc.close()

    # Сохранить обновлённый tile_config
    info["tile_config"] = tile_cfg
    save_project_info(project_dir, info)

    total = sum(result.values())
    print(f"\n  [SMART] Готово: {len(result)} страниц, {total} тайлов")
    return result


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
    # Defaults if no project_info.json
    return {
        "project_id": os.path.basename(project_dir),
        "pdf_file": "document.pdf",
        "tile_config": DEFAULT_TILE_CONFIG,
    }




def tile_page(doc, page_idx, rows, cols, scale, overlap_pct, label, out_dir):
    page    = doc[page_idx]
    pw, ph  = page.rect.width, page.rect.height
    tw, th  = pw / cols, ph / rows
    ox, oy  = tw * overlap_pct / 100, th * overlap_pct / 100
    mat     = fitz.Matrix(scale, scale)
    page_num = page_idx + 1
    page_dir = os.path.join(out_dir, f"page_{page_num:02d}")
    os.makedirs(page_dir, exist_ok=True)

    index = []
    for r in range(rows):
        for c in range(cols):
            x0 = max(0,  c * tw - ox)
            y0 = max(0,  r * th - oy)
            x1 = min(pw, (c + 1) * tw + ox)
            y1 = min(ph, (r + 1) * th + oy)
            pix  = page.get_pixmap(matrix=mat, clip=fitz.Rect(x0, y0, x1, y1), alpha=False)
            fname = f"page_{page_num:02d}_r{r+1}c{c+1}.png"
            fpath = os.path.join(page_dir, fname)
            pix.save(fpath)
            size_kb = os.path.getsize(fpath) / 1024
            index.append({"file": fname, "row": r+1, "col": c+1,
                           "x0_pct": round(x0/pw*100,1), "y0_pct": round(y0/ph*100,1),
                           "x1_pct": round(x1/pw*100,1), "y1_pct": round(y1/ph*100,1),
                           "size_kb": round(size_kb, 0)})
            print(f"    r{r+1}c{c+1}  {fname}  ({size_kb:.0f} KB)")

    idx_path = os.path.join(page_dir, "index.json")
    meta = {
        "page": page_num,
        "label": label,
        "grid": f"{rows}x{cols}",
        "scale": scale,
        "overlap_pct": overlap_pct,
        "page_size_pts": [round(pw, 1), round(ph, 1)],
        "tile_size_px": [round(tw * scale), round(th * scale)],
        "tiles": index,
    }
    with open(idx_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return len(index)


def render_full_pages(doc, out_dir, scale=2.0):
    print(f"  Rendering full pages (scale={scale})...")
    os.makedirs(out_dir, exist_ok=True)
    mat = fitz.Matrix(scale, scale)
    for i, page in enumerate(doc):
        pix   = page.get_pixmap(matrix=mat, alpha=False)
        fpath = os.path.join(out_dir, f"page_{i+1:02d}.png")
        pix.save(fpath)
        size_kb = os.path.getsize(fpath) / 1024
        print(f"    page_{i+1:02d}.png  ({size_kb:.0f} KB)")


def save_project_info(project_dir, info):
    """Сохраняет обновлённый project_info.json обратно в папку проекта."""
    info_path = os.path.join(project_dir, "project_info.json")
    with open(info_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)
    print(f"  [SAVED] project_info.json updated (tile_config: {len(info.get('tile_config', {}))} pages)")


def needs_upgrade(project_dir):
    """
    Проверяет, нужно ли переобработать проект через MD-ориентированный алгоритм.

    Returns True если:
      - MD-файл существует, НО tile_config_source != "md_analysis" или text_source != "md"
    Returns False если:
      - MD-файла нет (area_based корректен)
      - Проект уже обработан через md_analysis
    """
    info = load_project_info(project_dir)
    pdf_name = info.get("pdf_file", "document.pdf")
    md_file, _ = detect_md_file(project_dir, pdf_name)
    if not md_file:
        return False  # MD нет — area_based корректен, upgrade не нужен
    tile_source = info.get("tile_config_source")
    text_source = info.get("text_source")
    if tile_source == "md_analysis" and text_source == "md":
        return False  # Уже обработан через MD — всё в порядке
    return True  # MD есть, но обработка устаревшая


def process(project_dir, full_pages=False, force=False, quality="standard"):
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

    # Для tile_config используем первый существующий PDF
    if not os.path.exists(pdf_path):
        for pf in pdf_files:
            candidate = os.path.join(project_dir, pf)
            if os.path.exists(candidate):
                pdf_path = candidate
                break

    out_dir   = os.path.join(project_dir, "_output")
    tiles_dir = os.path.join(out_dir, "tiles")
    pages_dir = os.path.join(out_dir, "pages_png")
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
    md_pages = None

    if not all_md:
        print(f"  [ERROR] MD-файл не найден в {project_dir}")
        print(f"  Анализ без MD-файла не поддерживается.")
        print(f"  Создайте MD-файл через Chandra OCR и положите в папку проекта.")
        return False

    if len(all_md) == 1:
        md_file, md_size_kb = all_md[0]
        md_path = os.path.join(project_dir, md_file)
        info["md_file"] = md_file
        info["md_file_size_kb"] = md_size_kb
        print(f"  [MD] Found: {md_file} ({md_size_kb} KB)")
    else:
        # Несколько MD — конкатенируем во временный файл
        combined_name = "_combined_document.md"
        combined_path = os.path.join(out_dir, combined_name)
        total_size = 0
        with open(combined_path, "w", encoding="utf-8") as out_f:
            for i, (mf, sz) in enumerate(all_md):
                mpath = os.path.join(project_dir, mf)
                with open(mpath, "r", encoding="utf-8") as in_f:
                    content = in_f.read()
                if i > 0:
                    out_f.write("\n\n")  # разделитель между частями
                out_f.write(content)
                total_size += sz
                print(f"  [MD] Part {i+1}: {mf} ({sz} KB)")

        md_file = combined_name
        md_path = combined_path
        md_size_kb = round(os.path.getsize(combined_path) / 1024, 1)
        info["md_file"] = all_md[0][0]  # основной — первый
        info["md_files"] = [mf for mf, _ in all_md]
        info["md_file_size_kb"] = md_size_kb
        print(f"  [MD] Combined: {len(all_md)} файлов -> {md_size_kb} KB")

    # Анализ MD: определяем страницы с графикой
    md_pages = analyze_md_pages(md_path)
    image_pages = sorted(p for p, v in md_pages.items() if v["has_image"])
    text_only   = sorted(p for p, v in md_pages.items() if v["has_text"] and not v["has_image"])
    print(f"  [MD] Страниц: {len(md_pages)} всего, "
          f"{len(image_pages)} с графикой, {len(text_only)} чисто текстовых")

    info["text_source"] = "md"
    info["md_page_classification"] = {
        "total_pages": len(md_pages),
        "image_pages": image_pages,
        "text_only_pages": text_only,
    }
    save_project_info(project_dir, info)
    print(f"  [OK] MD — первичный источник текста")

    # ── Step 1: Build Document Knowledge Graph ──
    # Приоритет: v2 (из *_result.json с coords) → fallback v1 (из MD)
    graph_built = False
    if build_document_graph_v2 is not None:
        graph_v2 = build_document_graph_v2(project_dir, out_dir)
        if graph_v2:
            graph_built = True
            # Debug output для locality binding
            if generate_locality_debug is not None:
                debug_path = generate_locality_debug(graph_v2, out_dir)
                if debug_path:
                    print(f"  [GRAPH v2] Debug: {debug_path.name}")
    if not graph_built:
        project_id = info.get("project_id", os.path.basename(project_dir))
        print(f"  [WARNING] [{project_id}] graph v2 not built — using MD-only v1 fallback")
        build_document_graph(md_path, out_dir)

    # ── Step 2: Configure tiles ──
    tile_cfg = info.get("tile_config", {})
    tile_quality = info.get("tile_quality", quality)

    # Пересоздать tile_config при --force
    if force:
        tile_cfg = {}

    if not tile_cfg:
        if md_pages:
            # MD-ориентированная конфигурация: только страницы с IMAGE
            print(f"  [AUTO-MD] Конфигурация тайлов по MD-анализу (quality={tile_quality})...")
            tile_cfg = auto_configure_tiles_from_md(pdf_path, md_pages, quality=tile_quality)
            if tile_cfg:
                info["tile_config"] = tile_cfg
                info["tile_config_source"] = "md_analysis"
                save_project_info(project_dir, info)
                print(f"  [AUTO-MD] {len(tile_cfg)} страниц с графикой для нарезки")
            else:
                print(f"  [WARN] MD-анализ не нашёл страниц с графикой")
        else:
            # Стандартная конфигурация: по площади страниц PDF
            print(f"  [AUTO] tile_config пуст — адаптивная конфигурация (quality={tile_quality})...")
            tile_cfg = auto_configure_tiles(pdf_path, quality=tile_quality)
            if tile_cfg:
                info["tile_config"] = tile_cfg
                info["tile_config_source"] = "area_based"
                save_project_info(project_dir, info)
                print(f"  [AUTO] Найдено {len(tile_cfg)} чертёжных страниц для нарезки")
            else:
                print(f"  [WARN] Авто-определение не нашло чертёжных страниц (все ≤ A4)")

    # ── Step 2.5: Очистка тайлов-сирот (папки page_XX не в tile_config) ──
    if force and os.path.isdir(tiles_dir) and tile_cfg:
        valid_pages = {f"page_{int(p):02d}" for p in tile_cfg.keys()}
        for d in os.listdir(tiles_dir):
            if d.startswith("page_") and os.path.isdir(os.path.join(tiles_dir, d)):
                if d not in valid_pages:
                    shutil.rmtree(os.path.join(tiles_dir, d))
                    print(f"  [CLEAN] Удалена устаревшая папка {d}")

    # ── Step 3: Tile drawings ──
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    os.makedirs(tiles_dir, exist_ok=True)

    total_tiles = 0
    for page_str, cfg in tile_cfg.items():
        page_num = int(page_str)
        if page_num < 1 or page_num > total_pages:
            continue
        page_dir = os.path.join(tiles_dir, f"page_{page_num:02d}")
        if not force and os.path.isdir(page_dir) and any(f.endswith(".png") for f in os.listdir(page_dir)):
            print(f"  [SKIP] Tiles page {page_num} already exist")
            total_tiles += len([f for f in os.listdir(page_dir) if f.endswith(".png")])
            continue
        # При --force: удалить все старые PNG и index.json в папке page_XX
        # (сетка могла уменьшиться, например 6x9→1x2, старые файлы останутся)
        if force and os.path.isdir(page_dir):
            old_files = [f for f in os.listdir(page_dir) if f.endswith(".png") or f == "index.json"]
            if old_files:
                for f in old_files:
                    os.remove(os.path.join(page_dir, f))
                print(f"  [CLEAN] page_{page_num:02d}: удалено {len(old_files)} старых файлов")
        rows, cols, scale, overlap = cfg[0], cfg[1], cfg[2], cfg[3]
        label = cfg[4] if len(cfg) > 4 else f"page_{page_num}"
        print(f"  Tiling page {page_num} ({rows}x{cols}, scale={scale}):")
        n = tile_page(doc, page_num - 1, rows, cols, scale, overlap, label, tiles_dir)
        total_tiles += n

    if total_tiles == 0 and tile_cfg:
        print(f"  [ERROR] tile_config задан ({len(tile_cfg)} страниц), но тайлы не созданы!")

    # Step 4: Full pages (optional)
    if full_pages:
        render_full_pages(doc, pages_dir)

    doc.close()

    src = info.get("tile_config_source", "manual")
    print(f"\n  DONE: {info.get('project_id', '')} — {total_tiles} tiles "
          f"(source: {src}, overlap: 5%)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Process a project: extract PDF text + create tiles")
    parser.add_argument("project_dir", nargs="?", default=None,
                        help="Path to project folder (default: scan projects/ dir)")
    parser.add_argument("--full-pages", action="store_true",
                        help="Also render full pages to pages_png/")
    parser.add_argument("--force", action="store_true",
                        help="Re-create even if already exists")
    parser.add_argument("--quality", choices=["draft", "standard", "high", "detailed", "speed"],
                        default="speed",
                        help="Tile quality profile: speed (fast, default), draft, standard, high, detailed (max quality)")
    parser.add_argument("--pages", type=str, default=None,
                        help="Comma-separated page numbers to tile (e.g. --pages 7,9,11). "
                             "Incremental: existing tiles are preserved.")
    parser.add_argument("--upgrade", action="store_true",
                        help="Re-process only projects that have MD files but use old area_based algorithm")
    args = parser.parse_args()

    if args.project_dir:
        # Process single project
        project_dir = args.project_dir
        if not os.path.isabs(project_dir):
            project_dir = os.path.join(BASE_DIR, project_dir)

        # --pages: инкрементальная нарезка выбранных страниц
        if args.pages:
            pages_list = [int(p.strip()) for p in args.pages.split(",") if p.strip().isdigit()]
            if not pages_list:
                print("[ERROR] --pages: укажите номера страниц через запятую (например: --pages 7,9,11)")
                sys.exit(1)
            tile_specific_pages(project_dir, pages_list,
                                quality=args.quality, force=args.force)
            sys.exit(0)

        if args.upgrade:
            if needs_upgrade(project_dir):
                name = os.path.basename(project_dir)
                print(f"  [UPGRADE] {name} — MD найден, обработка устаревшая, переобработка...")
                # Удалить старые тайлы (набор страниц изменится)
                tiles_dir = os.path.join(project_dir, "_output", "tiles")
                if os.path.isdir(tiles_dir):
                    shutil.rmtree(tiles_dir)
                    print(f"  [CLEAN] Удалена папка tiles/ (старые тайлы)")
                process(project_dir, full_pages=args.full_pages, force=True,
                        quality=args.quality)
            else:
                print(f"  [SKIP] {os.path.basename(project_dir)} — уже актуален (md_analysis или нет MD)")
        else:
            process(project_dir, full_pages=args.full_pages, force=args.force,
                    quality=args.quality)
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
            print("Each project needs: document.pdf + project_info.json")
            sys.exit(1)

        print(f"Found {len(projects)} project(s):")
        for p in projects:
            print(f"  - {os.path.basename(p)}")

        if args.upgrade:
            upgraded = 0
            skipped = 0
            for project_dir in projects:
                name = os.path.basename(project_dir)
                if needs_upgrade(project_dir):
                    print(f"\n  [UPGRADE] {name} — MD найден, переобработка...")
                    import shutil
                    tiles_dir = os.path.join(project_dir, "_output", "tiles")
                    if os.path.isdir(tiles_dir):
                        shutil.rmtree(tiles_dir)
                        print(f"  [CLEAN] Удалена папка tiles/")
                    process(project_dir, full_pages=args.full_pages, force=True,
                            quality=args.quality)
                    upgraded += 1
                else:
                    print(f"  [SKIP] {name} — уже актуален")
                    skipped += 1

            print(f"\n{'='*60}")
            print(f"UPGRADE COMPLETE: {upgraded} обновлено, {skipped} пропущено")
            print(f"{'='*60}")
        else:
            for project_dir in projects:
                process(project_dir, full_pages=args.full_pages, force=args.force,
                        quality=args.quality)

            print(f"\n{'='*60}")
            print(f"ALL PROJECTS PROCESSED: {len(projects)} total")
            print(f"{'='*60}")


if __name__ == "__main__":
    main()
