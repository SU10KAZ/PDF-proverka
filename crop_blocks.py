#!/usr/bin/env python3
"""
Кропинг image-блоков из PDF по координатам из OCR result.json.

Использование:
    python crop_blocks.py projects/<name>
    python crop_blocks.py projects/<name> --block-ids 7LPC-FCK4-PET,6XVP-EQJH-G9J
    python crop_blocks.py projects/<name> --force
"""
import argparse
import json
import os
import sys
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("[ERROR] PyMuPDF не установлен: pip install PyMuPDF")
    sys.exit(1)


# Claude оптимально работает с 1200-1800 px по длинной стороне
TARGET_LONG_SIDE_PX = 1500
MIN_BLOCK_AREA_PX2 = 50000  # фильтр мелких блоков (штампы, подписи)


def detect_result_json(project_dir: str) -> Path | None:
    """Найти *_result.json в папке проекта."""
    project_path = Path(project_dir)
    candidates = list(project_path.glob("*_result.json"))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        # Приоритет: файл с именем PDF
        info_path = project_path / "project_info.json"
        if info_path.exists():
            with open(info_path, "r", encoding="utf-8") as f:
                info = json.load(f)
            pdf_stem = Path(info.get("pdf_file", "")).stem
            for c in candidates:
                if c.stem.replace("_result", "") == pdf_stem:
                    return c
        return candidates[0]
    return None


def detect_pdf_file(project_dir: str) -> Path | None:
    """Найти PDF файл проекта."""
    project_path = Path(project_dir)
    info_path = project_path / "project_info.json"
    if info_path.exists():
        with open(info_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        pdf_file = info.get("pdf_file")
        if pdf_file:
            pdf_path = project_path / pdf_file
            if pdf_path.exists():
                return pdf_path
    # Fallback: любой PDF в папке
    pdfs = list(project_path.glob("*.pdf"))
    return pdfs[0] if pdfs else None


def extract_ocr_label(block: dict) -> str:
    """Извлечь краткую метку из ocr_text блока."""
    ocr_text = block.get("ocr_text", "")
    if not ocr_text:
        return "image"

    # ocr_text может быть JSON-строкой с analysis
    try:
        parsed = json.loads(ocr_text)
        if isinstance(parsed, dict):
            # Формат: {"analysis": {"content_summary": "..."}}
            analysis = parsed.get("analysis", parsed)
            summary = analysis.get("content_summary", "")
            if summary:
                return summary[:80]
            location = analysis.get("location", {})
            zone = location.get("zone_name", "")
            if zone:
                return zone[:80]
    except (json.JSONDecodeError, TypeError):
        pass

    # Fallback: первые 80 символов текста
    clean = ocr_text.strip()[:80]
    return clean if clean else "image"


def crop_blocks(
    project_dir: str,
    block_ids: list[str] | None = None,
    force: bool = False,
) -> dict:
    """
    Кропить image-блоки из PDF по координатам OCR.

    Args:
        project_dir: путь к папке проекта
        block_ids: список block_id для кропинга (None = все image-блоки)
        force: перезаписать существующие PNG

    Returns:
        dict с результатами: {total_blocks, cropped, skipped, errors, blocks[]}
    """
    # Найти файлы
    result_json_path = detect_result_json(project_dir)
    if not result_json_path:
        print(f"[ERROR] *_result.json не найден в {project_dir}")
        return {"error": "result.json not found"}

    pdf_path = detect_pdf_file(project_dir)
    if not pdf_path:
        print(f"[ERROR] PDF не найден в {project_dir}")
        return {"error": "PDF not found"}

    print(f"  OCR result: {result_json_path.name}")
    print(f"  PDF: {pdf_path.name}")

    # Загрузить OCR-данные
    with open(result_json_path, "r", encoding="utf-8") as f:
        ocr_data = json.load(f)

    pages = ocr_data.get("pages", [])
    if not pages:
        print("[ERROR] Нет страниц в result.json")
        return {"error": "no pages in result.json"}

    # Собрать все image-блоки
    all_image_blocks = []
    for page in pages:
        page_num = page.get("page_number", 0)
        page_width = page.get("width", 1)
        page_height = page.get("height", 1)
        for block in page.get("blocks", []):
            if block.get("block_type") != "image":
                continue

            # Фильтр штампов по category_code
            category = block.get("category_code", "")
            if category == "stamp":
                bid = block.get("id", "")
                print(f"  [SKIP] {bid}: штамп (category_code=stamp)")
                continue

            bid = block.get("id", "")
            coords = block.get("coords_px", [0, 0, 0, 0])
            # coords = [x_left, y_top, x_right, y_bottom]
            x1, y1, x2, y2 = coords
            w = x2 - x1
            h = y2 - y1
            area = w * h

            # Фильтр по block_ids
            if block_ids and bid not in block_ids:
                continue

            # Фильтр мелких блоков
            if area < MIN_BLOCK_AREA_PX2:
                print(f"  [SKIP] {bid}: слишком мелкий ({w}x{h} = {area} px²)")
                continue

            # page_index в result.json = 1-based (совпадает с page_number)
            # Для PyMuPDF нужен 0-based индекс
            block_page_index = block.get("page_index", page_num)

            all_image_blocks.append({
                "block_id": bid,
                "page_num": page_num,
                "page_index": block_page_index - 1,  # конвертируем в 0-based
                "coords_px": coords,
                "block_w": w,
                "block_h": h,
                "page_width": page_width,
                "page_height": page_height,
                "ocr_text": block.get("ocr_text", ""),
                "ocr_label": extract_ocr_label(block),
            })

    if not all_image_blocks:
        print("[WARN] Нет image-блоков для кропинга")
        return {"total_blocks": 0, "cropped": 0, "skipped": 0, "errors": 0, "blocks": []}

    print(f"  Image-блоков для кропинга: {len(all_image_blocks)}")

    # Открыть PDF
    doc = fitz.open(str(pdf_path))

    # Подготовить выходную папку
    output_dir = Path(project_dir) / "_output" / "blocks"
    output_dir.mkdir(parents=True, exist_ok=True)

    cropped = 0
    skipped = 0
    errors = 0
    index_blocks = []

    for block_info in all_image_blocks:
        bid = block_info["block_id"]
        out_file = output_dir / f"block_{bid}.png"

        # Пропустить если уже существует
        if out_file.exists() and not force:
            size_kb = out_file.stat().st_size / 1024
            if size_kb > 1:
                print(f"  [EXISTS] {bid} ({size_kb:.0f} KB)")
                index_blocks.append({
                    "block_id": bid,
                    "page": block_info["page_num"],
                    "file": f"block_{bid}.png",
                    "size_kb": round(size_kb, 1),
                    "crop_px": block_info["coords_px"],
                    "block_type": "image",
                    "ocr_label": block_info["ocr_label"],
                    "ocr_text_len": len(block_info["ocr_text"]),
                })
                skipped += 1
                continue

        try:
            page_idx = block_info["page_index"]  # уже 0-based (конвертировано выше)
            if page_idx < 0 or page_idx >= len(doc):
                print(f"  [ERROR] {bid}: page_index {block_info['page_index']} "
                      f"(page_num={block_info['page_num']}) out of range (0-{len(doc)-1})")
                errors += 1
                continue

            pdf_page = doc[page_idx]
            pdf_w = pdf_page.rect.width   # в пунктах
            pdf_h = pdf_page.rect.height  # в пунктах

            ocr_w = block_info["page_width"]   # в пикселях OCR
            ocr_h = block_info["page_height"]  # в пикселях OCR

            # Пересчёт OCR-координат в PDF-пункты
            x1, y1, x2, y2 = block_info["coords_px"]
            scale_x = pdf_w / ocr_w
            scale_y = pdf_h / ocr_h

            # Координаты в PDF-пунктах
            pdf_x0 = x1 * scale_x
            pdf_y0 = y1 * scale_y
            pdf_x1 = x2 * scale_x
            pdf_y1 = y2 * scale_y

            # Вычислить масштаб рендеринга для оптимального размера
            block_w_pt = pdf_x1 - pdf_x0
            block_h_pt = pdf_y1 - pdf_y0
            long_side_pt = max(block_w_pt, block_h_pt)

            if long_side_pt < 1:
                print(f"  [SKIP] {bid}: нулевой размер")
                skipped += 1
                continue

            render_scale = TARGET_LONG_SIDE_PX / long_side_pt

            # Ограничения: не менее 1.0 (иначе мелкий блок станет размытым)
            # и не более 8.0 (иначе огромный рендер)
            render_scale = max(1.0, min(8.0, render_scale))

            # Проверяем что результат будет хотя бы 2x2 пикселя
            expected_w = int(block_w_pt * render_scale)
            expected_h = int(block_h_pt * render_scale)
            if expected_w < 2 or expected_h < 2:
                print(f"  [SKIP] {bid}: слишком узкий блок ({expected_w}x{expected_h}px)")
                skipped += 1
                continue

            # Рендерим и кропим
            mat = fitz.Matrix(render_scale, render_scale)
            clip = fitz.Rect(pdf_x0, pdf_y0, pdf_x1, pdf_y1)
            pix = pdf_page.get_pixmap(matrix=mat, clip=clip, alpha=False)
            pix.save(str(out_file))

            size_kb = out_file.stat().st_size / 1024
            print(f"  [CROP] {bid}: стр.{block_info['page_num']}, "
                  f"{pix.width}x{pix.height}px, {size_kb:.0f} KB")

            index_blocks.append({
                "block_id": bid,
                "page": block_info["page_num"],
                "file": f"block_{bid}.png",
                "size_kb": round(size_kb, 1),
                "crop_px": block_info["coords_px"],
                "render_size": [pix.width, pix.height],
                "block_type": "image",
                "ocr_label": block_info["ocr_label"],
                "ocr_text_len": len(block_info["ocr_text"]),
            })
            cropped += 1

        except Exception as e:
            print(f"  [ERROR] {bid}: {e}")
            errors += 1

    doc.close()

    # Удалить PNG, которых нет в index (остатки от предыдущих прогонов)
    valid_files = {f"block_{b['block_id']}.png" for b in index_blocks}
    for old_png in output_dir.glob("block_*.png"):
        if old_png.name not in valid_files:
            print(f"  [CLEANUP] {old_png.name}")
            old_png.unlink()

    # Записать index.json
    index_data = {
        "total_blocks": len(index_blocks),
        "source_pdf": pdf_path.name,
        "source_result_json": result_json_path.name,
        "blocks": index_blocks,
    }
    index_path = output_dir / "index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    result = {
        "total_blocks": len(index_blocks),
        "cropped": cropped,
        "skipped": skipped,
        "errors": errors,
        "blocks": index_blocks,
    }

    print(f"\n  Итого: {len(index_blocks)} блоков ({cropped} кропнуто, "
          f"{skipped} пропущено, {errors} ошибок)")
    print(f"  Index: {index_path}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Кропинг image-блоков из PDF по координатам OCR"
    )
    parser.add_argument("project_dir", help="Путь к папке проекта")
    parser.add_argument("--block-ids", help="Список block_id через запятую")
    parser.add_argument("--force", action="store_true",
                        help="Перезаписать существующие PNG")
    args = parser.parse_args()

    if not os.path.isdir(args.project_dir):
        print(f"[ERROR] Папка не найдена: {args.project_dir}")
        sys.exit(1)

    block_ids = None
    if args.block_ids:
        block_ids = [bid.strip() for bid in args.block_ids.split(",")]

    print(f"Кропинг блоков: {args.project_dir}")
    result = crop_blocks(args.project_dir, block_ids=block_ids, force=args.force)

    if result.get("error"):
        sys.exit(1)

    # Вывести JSON-summary для pipeline_service
    print(json.dumps({
        "total_blocks": result["total_blocks"],
        "cropped": result["cropped"],
        "skipped": result["skipped"],
        "errors": result["errors"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
