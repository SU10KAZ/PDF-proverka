#!/usr/bin/env python3
"""
Блоковый конвейер: скачивание, группировка, слияние результатов анализа блоков.

Использование:
    python blocks.py crop projects/<name>                    # скачать блоки по crop_url
    python blocks.py crop projects/<name> --block-ids A,B    # только указанные блоки
    python blocks.py crop projects/<name> --force            # перезаписать

    python blocks.py batches projects/<name>                 # сгенерировать пакеты
    python blocks.py batches projects/<name> --batch-size 8  # размер пакета

    python blocks.py merge projects/<name>                   # слить результаты
    python blocks.py merge projects/<name> --cleanup         # + удалить промежуточные
"""
import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

try:
    import fitz  # PyMuPDF — для конвертации PDF→PNG
except ImportError:
    print("[ERROR] PyMuPDF не установлен: pip install PyMuPDF")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# CROP — скачивание image-блоков по crop_url из result.json
# ═══════════════════════════════════════════════════════════════════════════════

TARGET_LONG_SIDE_PX = 1500
MIN_BLOCK_AREA_PX2 = 50000


def detect_result_json(project_dir: str) -> Path | None:
    """Найти *_result.json в папке проекта."""
    project_path = Path(project_dir)
    candidates = list(project_path.glob("*_result.json"))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
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


def extract_ocr_label(block: dict) -> str:
    """Извлечь краткую метку из ocr_text блока."""
    ocr_text = block.get("ocr_text", "")
    if not ocr_text:
        return "image"
    try:
        parsed = json.loads(ocr_text)
        if isinstance(parsed, dict):
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
    clean = ocr_text.strip()[:80]
    return clean if clean else "image"


def download_and_convert(crop_url: str, out_png: Path, timeout: int = 30) -> tuple[int, int]:
    """Скачать PDF-кроп по URL и конвертировать в PNG."""
    req = urllib.request.Request(crop_url, headers={"User-Agent": "crop_blocks/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        pdf_bytes = resp.read()

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]

    long_side_pt = max(page.rect.width, page.rect.height)
    if long_side_pt < 1:
        doc.close()
        raise ValueError("Нулевой размер страницы в PDF-кропе")

    render_scale = TARGET_LONG_SIDE_PX / long_side_pt
    render_scale = max(1.0, min(8.0, render_scale))

    mat = fitz.Matrix(render_scale, render_scale)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    pix.save(str(out_png))

    w, h = pix.width, pix.height
    doc.close()
    return w, h


def crop_from_pdf(
    pdf_path: Path,
    page_num: int,
    coords_px: list,
    page_width: int,
    page_height: int,
    out_png: Path,
) -> tuple[int, int]:
    """Вырезать блок из PDF по координатам (fallback при ошибке скачивания).

    coords_px: [x1, y1, x2, y2] в пиксельной системе result.json
    page_width, page_height: размеры страницы в пикселях из result.json
    """
    doc = fitz.open(str(pdf_path))
    page = doc[page_num - 1]  # page_num 1-based

    # Конвертируем пиксельные координаты в координаты PDF (points)
    x1, y1, x2, y2 = coords_px
    scale_x = page.rect.width / page_width
    scale_y = page.rect.height / page_height

    clip = fitz.Rect(
        x1 * scale_x,
        y1 * scale_y,
        x2 * scale_x,
        y2 * scale_y,
    )

    # Масштаб рендеринга: длинная сторона вырезки → TARGET_LONG_SIDE_PX
    clip_w = clip.width
    clip_h = clip.height
    long_side_pt = max(clip_w, clip_h)
    if long_side_pt < 1:
        doc.close()
        raise ValueError("Нулевой размер блока")

    render_scale = TARGET_LONG_SIDE_PX / long_side_pt
    render_scale = max(0.5, min(8.0, render_scale))  # допускаем уменьшение до 0.5×

    mat = fitz.Matrix(render_scale, render_scale)
    pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
    pix.save(str(out_png))

    w, h = pix.width, pix.height
    doc.close()
    return w, h


def crop_blocks(
    project_dir: str,
    block_ids: list[str] | None = None,
    force: bool = False,
) -> dict:
    """Скачать image-блоки по crop_url из result.json и сохранить как PNG."""
    result_json_path = detect_result_json(project_dir)
    if not result_json_path:
        print(f"[ERROR] *_result.json не найден в {project_dir}")
        return {"error": "result.json not found"}

    print(f"  OCR result: {result_json_path.name}")

    with open(result_json_path, "r", encoding="utf-8") as f:
        ocr_data = json.load(f)

    pages = ocr_data.get("pages", [])
    if not pages:
        print("[ERROR] Нет страниц в result.json")
        return {"error": "no pages in result.json"}

    # Собираем размеры страниц для fallback-кропинга из PDF
    page_dimensions: dict[int, tuple[int, int]] = {}
    for pg in pages:
        pn = pg.get("page_number", 0)
        pw = pg.get("width", 0)
        ph = pg.get("height", 0)
        if pw and ph:
            page_dimensions[pn] = (pw, ph)

    # Ищем PDF для fallback-кропинга
    project_path = Path(project_dir)
    pdf_path: Path | None = None
    info_path = project_path / "project_info.json"
    if info_path.exists():
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                info = json.load(f)
            pdf_name = info.get("pdf_file", "")
            if pdf_name:
                candidate = project_path / pdf_name
                if candidate.exists():
                    pdf_path = candidate
        except Exception:
            pass
    if not pdf_path:
        # Fallback: первый PDF в папке
        pdfs = list(project_path.glob("*.pdf"))
        if pdfs:
            pdf_path = pdfs[0]

    all_image_blocks = []
    no_url_count = 0
    for page in pages:
        page_num = page.get("page_number", 0)
        for block in page.get("blocks", []):
            if block.get("block_type") != "image":
                continue
            category = block.get("category_code", "")
            if category == "stamp":
                bid = block.get("id", "")
                print(f"  [SKIP] {bid}: штамп (category_code=stamp)")
                continue

            bid = block.get("id", "")
            crop_url = block.get("crop_url", "")

            if block_ids and bid not in block_ids:
                continue
            if not crop_url and not pdf_path:
                print(f"  [SKIP] {bid}: нет crop_url и PDF не найден")
                no_url_count += 1
                continue

            coords = block.get("coords_px", [0, 0, 0, 0])
            x1, y1, x2, y2 = coords
            w = x2 - x1
            h = y2 - y1
            area = w * h
            if area < MIN_BLOCK_AREA_PX2:
                print(f"  [SKIP] {bid}: слишком мелкий ({w}x{h} = {area} px²)")
                continue

            all_image_blocks.append({
                "block_id": bid,
                "page_num": page_num,
                "crop_url": crop_url,
                "coords_px": coords,
                "ocr_text": block.get("ocr_text", ""),
                "ocr_label": extract_ocr_label(block),
            })

    if not all_image_blocks:
        print("[WARN] Нет image-блоков для скачивания")
        if no_url_count:
            print(f"  ({no_url_count} блоков без crop_url)")
        return {"total_blocks": 0, "cropped": 0, "skipped": 0, "errors": 0, "blocks": []}

    print(f"  Image-блоков для скачивания: {len(all_image_blocks)}")
    if no_url_count:
        print(f"  ({no_url_count} блоков пропущено — нет crop_url)")

    output_dir = Path(project_dir) / "_output" / "blocks"
    output_dir.mkdir(parents=True, exist_ok=True)

    cropped = 0
    skipped = 0
    errors = 0
    index_blocks = []

    for block_info in all_image_blocks:
        bid = block_info["block_id"]
        out_file = output_dir / f"block_{bid}.png"

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

        source = "cloud"
        crop_url = block_info["crop_url"]
        download_error = None

        if crop_url:
            try:
                w, h = download_and_convert(crop_url, out_file)
            except Exception as e:
                download_error = e
        else:
            download_error = "нет crop_url"

        if download_error is not None:
            e = download_error
            # Fallback: вырезаем из PDF по координатам
            pn = block_info["page_num"]
            dims = page_dimensions.get(pn)
            if pdf_path and dims:
                try:
                    w, h = crop_from_pdf(
                        pdf_path, pn,
                        block_info["coords_px"],
                        dims[0], dims[1],
                        out_file,
                    )
                    source = "pdf_fallback"
                    print(f"  [FALLBACK] {bid}: облако недоступно ({e}), вырезан из PDF")
                except Exception as e2:
                    print(f"  [ERROR] {bid}: облако ({e}), PDF ({e2})")
                    errors += 1
                    continue
            else:
                print(f"  [ERROR] {bid}: {e}" +
                      ("" if pdf_path else " (PDF не найден для fallback)"))
                errors += 1
                continue

        size_kb = out_file.stat().st_size / 1024
        label = "DOWNLOAD" if source == "cloud" else "PDF-CROP"
        print(f"  [{label}] {bid}: стр.{block_info['page_num']}, "
              f"{w}x{h}px, {size_kb:.0f} KB")
        index_blocks.append({
            "block_id": bid,
            "page": block_info["page_num"],
            "file": f"block_{bid}.png",
            "size_kb": round(size_kb, 1),
            "crop_px": block_info["coords_px"],
            "render_size": [w, h],
            "block_type": "image",
            "ocr_label": block_info["ocr_label"],
            "ocr_text_len": len(block_info["ocr_text"]),
            "source": source,
        })
        cropped += 1

    # Cleanup только при полном прогоне
    if not block_ids:
        valid_files = {f"block_{b['block_id']}.png" for b in index_blocks}
        for old_png in output_dir.glob("block_*.png"):
            if old_png.name not in valid_files:
                print(f"  [CLEANUP] {old_png.name}")
                old_png.unlink()

    index_data = {
        "total_blocks": len(index_blocks),
        "total_expected": len(all_image_blocks),
        "errors": errors,
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

    print(f"\n  Итого: {len(index_blocks)} блоков ({cropped} скачано, "
          f"{skipped} пропущено, {errors} ошибок)")
    print(f"  Index: {index_path}")

    # Обогатить document_graph.json данными из index.json
    try:
        from process_project import enrich_document_graph
        enrich_document_graph(str(output_dir.parent))  # output_dir = _output/blocks, parent = _output
    except Exception as e:
        print(f"  [WARN] Не удалось обогатить document_graph: {e}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# BATCHES — группировка блоков в пакеты для Claude
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_BATCH_SIZE = 10

# Гибридная стратегия: ограничение по объёму И по количеству
MAX_BATCH_SIZE_KB = 5 * 1024   # 5 MB целевой объём пакета
MAX_BLOCKS_PER_BATCH = 15      # макс блоков (даже если суммарно мало весят)
MIN_BLOCKS_PER_BATCH = 3       # мин блоков (не дробить на слишком мелкие пакеты)
SOLO_BLOCK_THRESHOLD_KB = 3 * 1024  # блок > 3 MB — отдельный пакет


def _make_batch_entry(batch_id: int, blocks_list: list[dict]) -> dict:
    """Сформировать запись пакета из списка блоков."""
    return {
        "batch_id": batch_id,
        "blocks": [
            {
                "block_id": b["block_id"],
                "page": b["page"],
                "file": b["file"],
                "size_kb": b.get("size_kb", 0),
                "ocr_label": b.get("ocr_label", "image"),
            }
            for b in blocks_list
        ],
        "pages_included": sorted(set(b["page"] for b in blocks_list)),
        "block_count": len(blocks_list),
        "total_size_kb": sum(b.get("size_kb", 0) for b in blocks_list),
    }


def _pack_blocks_adaptive(
    blocks: list[dict],
    max_size_kb: int = MAX_BATCH_SIZE_KB,
    max_blocks: int = MAX_BLOCKS_PER_BATCH,
    min_blocks: int = MIN_BLOCKS_PER_BATCH,
    solo_threshold_kb: int = SOLO_BLOCK_THRESHOLD_KB,
) -> list[list[dict]]:
    """Разбить блоки на пакеты по гибридной стратегии (объём + количество).

    Правила:
    1. Блок > solo_threshold_kb → отдельный пакет (крупный чертёж)
    2. Набираем блоки пока не упрёмся в max_size_kb или max_blocks
    3. Если остаток < min_blocks → присоединяем к предыдущему пакету
    """
    if not blocks:
        return []

    # Разделяем: крупные блоки (соло) и обычные
    solo = []
    normal = []
    for b in blocks:
        if b.get("size_kb", 0) >= solo_threshold_kb:
            solo.append(b)
        else:
            normal.append(b)

    # Пакуем обычные блоки по объёму + количеству
    packed: list[list[dict]] = []
    current: list[dict] = []
    current_size = 0

    for b in normal:
        b_size = b.get("size_kb", 0)

        # Текущий пакет переполнится → закрываем его
        if current and (current_size + b_size > max_size_kb or len(current) >= max_blocks):
            packed.append(current)
            current = []
            current_size = 0

        current.append(b)
        current_size += b_size

    if current:
        packed.append(current)

    # Если последний пакет слишком мелкий — присоединяем к предыдущему
    if len(packed) >= 2 and len(packed[-1]) < min_blocks:
        tail = packed.pop()
        packed[-1].extend(tail)

    # Добавляем соло-блоки как отдельные пакеты
    for b in solo:
        packed.append([b])

    return packed


def generate_block_batches(
    project_dir: str,
    block_ids: list[str] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    adaptive: bool = True,
    max_size_kb: int = MAX_BATCH_SIZE_KB,
    max_blocks: int = MAX_BLOCKS_PER_BATCH,
) -> dict:
    """Сгруппировать image-блоки в пакеты.

    adaptive=True (по умолчанию): гибридная стратегия по объёму + количеству.
    adaptive=False: старая стратегия (фиксированный batch_size).
    """
    output_dir = Path(project_dir) / "_output"
    index_path = output_dir / "blocks" / "index.json"

    if not index_path.exists():
        print(f"[ERROR] {index_path} не найден. Сначала запустите: python blocks.py crop")
        return {"error": "blocks/index.json not found"}

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    blocks = index_data.get("blocks", [])
    if block_ids:
        blocks = [b for b in blocks if b["block_id"] in block_ids]

    if not blocks:
        print("[WARN] Нет блоков для группировки")
        return {"total_batches": 0, "batches": []}

    # Группируем по страницам (сохраняем контекст страницы)
    pages_map: dict[int, list[dict]] = {}
    for block in blocks:
        page = block.get("page", 0)
        pages_map.setdefault(page, []).append(block)

    batches = []
    batch_id = 0

    if adaptive:
        # Гибридная стратегия: собираем блоки по страницам, пакуем адаптивно
        # Блоки одной страницы стараемся держать вместе
        page_groups: list[list[dict]] = []
        for page_num in sorted(pages_map.keys()):
            page_groups.append(pages_map[page_num])

        # Собираем «суперсписок» с сохранением порядка страниц
        ordered_blocks: list[dict] = []
        for pg in page_groups:
            ordered_blocks.extend(pg)

        packed = _pack_blocks_adaptive(ordered_blocks, max_size_kb=max_size_kb, max_blocks=max_blocks)

        for chunk in packed:
            batch_id += 1
            batches.append(_make_batch_entry(batch_id, chunk))

        strategy = "adaptive"
        total_size_kb = sum(b.get("size_kb", 0) for b in blocks)
        print(f"  Стратегия: адаптивная (лимит {max_size_kb}KB / {max_blocks} блоков)")
        print(f"  Общий объём блоков: {total_size_kb}KB ({total_size_kb / 1024:.1f}MB)")
        if batches:
            sizes = [b["total_size_kb"] for b in batches]
            counts = [b["block_count"] for b in batches]
            print(f"  Размер пакетов: {min(sizes)}-{max(sizes)}KB, блоков: {min(counts)}-{max(counts)}")
    else:
        # Старая стратегия: фиксированный batch_size
        for page_num in sorted(pages_map.keys()):
            page_blocks = pages_map[page_num]
            for i in range(0, len(page_blocks), batch_size):
                batch_id += 1
                chunk = page_blocks[i:i + batch_size]
                batches.append(_make_batch_entry(batch_id, chunk))

        strategy = "fixed"

    result = {
        "total_batches": len(batches),
        "total_blocks": sum(b["block_count"] for b in batches),
        "strategy": strategy,
        "batch_size": batch_size if not adaptive else None,
        "adaptive_params": {
            "max_size_kb": max_size_kb,
            "max_blocks": max_blocks,
            "min_blocks": MIN_BLOCKS_PER_BATCH,
            "solo_threshold_kb": SOLO_BLOCK_THRESHOLD_KB,
        } if adaptive else None,
        "batches": batches,
    }

    out_path = output_dir / "block_batches.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"  Сгенерировано {len(batches)} пакетов ({result['total_blocks']} блоков)")
    print(f"  Записано: {out_path}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE — слияние результатов анализа блоков
# ═══════════════════════════════════════════════════════════════════════════════

def merge_block_results(project_dir: str, cleanup: bool = False) -> dict:
    """Слить все block_batch_NNN.json в один 02_blocks_analysis.json."""
    output_dir = Path(project_dir) / "_output"

    batch_files = sorted(output_dir.glob("block_batch_*.json"))
    if not batch_files:
        print("[ERROR] Нет файлов block_batch_*.json")
        return {"error": "no batch files found"}

    print(f"  Найдено пакетов: {len(batch_files)}")

    all_block_analyses = []
    all_findings = []
    total_blocks_reviewed = 0
    merged_sources = []

    for bf in batch_files:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                batch_data = json.load(f)

            analyses = (
                batch_data.get("block_analyses", [])
                or batch_data.get("page_summaries", [])
                or batch_data.get("blocks_reviewed", [])
            )
            all_block_analyses.extend(analyses)
            total_blocks_reviewed += len(analyses)

            # Собираем замечания из block_analyses[].findings (основной источник)
            for ba in analyses:
                for f in ba.get("findings", []):
                    # Добавляем block_id и page если не указаны
                    if "source" not in f and "block_evidence" not in f:
                        f["block_evidence"] = ba.get("block_id", "")
                    all_findings.append(f)
            # Также собираем из preliminary_findings (legacy)
            legacy_findings = batch_data.get("preliminary_findings", [])
            all_findings.extend(legacy_findings)

            batch_findings_count = sum(len(ba.get("findings", [])) for ba in analyses) + len(legacy_findings)
            merged_sources.append(bf.name)
            print(f"    {bf.name}: {len(analyses)} блоков, {batch_findings_count} замечаний")

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  [WARN] Ошибка чтения {bf.name}: {e}")

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

    out_path = output_dir / "02_blocks_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n  Итого: {total_blocks_reviewed} блоков, {len(all_findings)} замечаний")
    print(f"  Покрытие: {coverage}%")
    print(f"  Записано: {out_path}")

    if cleanup:
        for bf in batch_files:
            bf.unlink()
            print(f"  [DEL] {bf.name}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# CLI — точка входа с подкомандами
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Блоковый конвейер: скачивание, группировка, слияние"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # crop
    p_crop = subparsers.add_parser("crop", help="Скачать блоки по crop_url из result.json")
    p_crop.add_argument("project_dir", help="Путь к папке проекта")
    p_crop.add_argument("--block-ids", help="Список block_id через запятую")
    p_crop.add_argument("--force", action="store_true", help="Перезаписать существующие PNG")

    # batches
    p_batch = subparsers.add_parser("batches", help="Сгенерировать пакеты блоков")
    p_batch.add_argument("project_dir", help="Путь к папке проекта")
    p_batch.add_argument("--block-ids", help="Список block_id через запятую")
    p_batch.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                         help=f"Максимум блоков в пакете (по умолчанию {DEFAULT_BATCH_SIZE})")
    p_batch.add_argument("--no-adaptive", action="store_true",
                         help="Использовать старую стратегию (фиксированный batch_size)")
    p_batch.add_argument("--max-size-mb", type=float, default=5.0,
                         help="Целевой объём пакета в МБ (по умолчанию 5.0)")
    p_batch.add_argument("--max-blocks", type=int, default=MAX_BLOCKS_PER_BATCH,
                         help=f"Макс блоков в пакете (по умолчанию {MAX_BLOCKS_PER_BATCH})")

    # merge
    p_merge = subparsers.add_parser("merge", help="Слить block_batch_*.json в 02_blocks_analysis.json")
    p_merge.add_argument("project_dir", help="Путь к папке проекта")
    p_merge.add_argument("--cleanup", action="store_true",
                         help="Удалить промежуточные файлы после слияния")

    args = parser.parse_args()

    if not os.path.isdir(args.project_dir):
        print(f"[ERROR] Папка не найдена: {args.project_dir}")
        sys.exit(1)

    if args.command == "crop":
        block_ids = [b.strip() for b in args.block_ids.split(",")] if args.block_ids else None
        result = crop_blocks(args.project_dir, block_ids=block_ids, force=args.force)
        if result.get("error"):
            sys.exit(1)
        print(json.dumps({
            "total_blocks": result["total_blocks"],
            "cropped": result["cropped"],
            "skipped": result["skipped"],
            "errors": result["errors"],
        }, ensure_ascii=False))
        if result["errors"] > 0:
            sys.exit(2)  # частичная ошибка: не все блоки скачались

    elif args.command == "batches":
        block_ids = [b.strip() for b in args.block_ids.split(",")] if args.block_ids else None
        use_adaptive = not getattr(args, "no_adaptive", False)
        result = generate_block_batches(
            args.project_dir,
            block_ids=block_ids,
            batch_size=args.batch_size,
            adaptive=use_adaptive,
            max_size_kb=int(getattr(args, "max_size_mb", 5.0) * 1024),
            max_blocks=getattr(args, "max_blocks", MAX_BLOCKS_PER_BATCH),
        )
        if result.get("error"):
            sys.exit(1)

    elif args.command == "merge":
        result = merge_block_results(args.project_dir, cleanup=args.cleanup)
        if result.get("error"):
            sys.exit(1)


if __name__ == "__main__":
    main()
