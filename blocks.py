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
    fitz = None


def _require_pymupdf():
    if fitz is None:
        raise RuntimeError("PyMuPDF не установлен: pip install PyMuPDF")


# ─── Block ID normalization ────────────────────────────────────────────────

def _normalize_block_id(raw: str | None) -> str:
    """Canonical bare block_id: 'block_IMG-001.png' → 'IMG-001'."""
    if not raw:
        return ""
    s = raw.strip()
    if s.startswith("block_"):
        s = s[6:]
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        if s.lower().endswith(ext):
            s = s[:-len(ext)]
            break
    return s


def _normalize_finding_block_ids(finding: dict):
    """Нормализовать block identity в finding (in-place)."""
    if "block_evidence" in finding:
        finding["block_evidence"] = _normalize_block_id(finding["block_evidence"])
    if "related_block_ids" in finding and isinstance(finding["related_block_ids"], list):
        finding["related_block_ids"] = [
            _normalize_block_id(b) for b in finding["related_block_ids"]
            if _normalize_block_id(b)
        ]
    if "evidence" in finding and isinstance(finding["evidence"], list):
        for ev in finding["evidence"]:
            if isinstance(ev, dict) and "block_id" in ev:
                ev["block_id"] = _normalize_block_id(ev["block_id"])


# ═══════════════════════════════════════════════════════════════════════════════
# CROP — скачивание image-блоков по crop_url из result.json
# ═══════════════════════════════════════════════════════════════════════════════

TARGET_DPI = 100               # Единое разрешение: 100 DPI для всех блоков
TARGET_DPI_COMPACT = 50        # Compact-режим: дешевле по токенам
MIN_BLOCK_AREA_PX2 = 50000

# Гибридный режим: при DPI<min блок апскейлится так, чтобы long side ≥ MIN_LONG_SIDE_PX.
# Без этого мелкие узлы (~200pt) на 100 DPI становятся ~280px и плохо читаются LLM.
MIN_LONG_SIDE_PX = 800
MIN_LONG_SIDE_PX_COMPACT = 500

# Legacy constants (используются в recrop и _render_full_page)
TARGET_LONG_SIDE_PX = 1500
TARGET_LONG_SIDE_PX_COMPACT = 800


# ─── Экспериментальный render profile (для resolution A/B — non-invasive) ──
# Production crop_blocks() без явного render_profile работает строго по модульным
# константам TARGET_DPI и MIN_LONG_SIDE_PX. Override-механика задействуется ТОЛЬКО
# experimental runner-ами, которые явно передают render_profile в crop_blocks_to_dir().

def make_block_render_profile(
    *,
    target_dpi: int | None = None,
    min_long_side_px: int | None = None,
    name: str | None = None,
) -> dict:
    """Собрать render-профиль для экспериментального crop.

    Поля, не переданные явно, подменяются production-дефолтами
    (TARGET_DPI и MIN_LONG_SIDE_PX). Возвращает dict с ключами
    name / target_dpi / min_long_side_px.
    """
    dpi = int(target_dpi if target_dpi is not None else TARGET_DPI)
    mls = int(min_long_side_px if min_long_side_px is not None else MIN_LONG_SIDE_PX)
    if dpi <= 0:
        dpi = TARGET_DPI
    if mls <= 0:
        mls = MIN_LONG_SIDE_PX
    return {
        "name": name or f"dpi{dpi}_min{mls}",
        "target_dpi": dpi,
        "min_long_side_px": mls,
    }


def read_block_render_profile_from_env(env: dict | None = None) -> dict | None:
    """Прочитать experimental render profile из ENV.

    Если ни одна из соответствующих переменных не задана — возвращается None
    (production default сохраняется).

    ENV:
      BLOCK_RENDER_MIN_LONG_SIDE — переопределить минимальную длинную сторону (px)
      BLOCK_RENDER_TARGET_DPI    — переопределить target DPI
    """
    env_map = env if env is not None else os.environ
    mls_raw = env_map.get("BLOCK_RENDER_MIN_LONG_SIDE")
    dpi_raw = env_map.get("BLOCK_RENDER_TARGET_DPI")
    if not mls_raw and not dpi_raw:
        return None

    def _pos(raw):
        try:
            val = int(str(raw).strip())
        except (TypeError, ValueError):
            return None
        return val if val > 0 else None

    mls = _pos(mls_raw) if mls_raw else None
    dpi = _pos(dpi_raw) if dpi_raw else None
    if mls is None and dpi is None:
        return None
    return make_block_render_profile(
        target_dpi=dpi,
        min_long_side_px=mls,
        name="env_override",
    )


def _iter_image_blocks_from_ocr(project_dir: str):
    """Yield (image_block_info, page_dimensions_map, pdf_path_for_page) из всех result.json.

    image_block_info: dict c block_id, page_num, crop_url, coords_px, ocr_text, ocr_label.
    Возвращает:
      (all_image_blocks, page_dimensions, page_pdf_map, result_json_paths).
    """
    result_json_paths = detect_all_result_jsons(project_dir)
    if not result_json_paths:
        return [], {}, {}, []

    project_path = Path(project_dir)
    info: dict = {}
    info_path = project_path / "project_info.json"
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            info = {}
    pdf_files = info.get("pdf_files", [])
    if not pdf_files:
        pf = info.get("pdf_file", "")
        pdf_files = [pf] if pf else []

    all_image_blocks: list[dict] = []
    all_page_dimensions: dict[int, tuple[int, int]] = {}
    page_pdf_map: dict[int, Path] = {}

    for rj_path in result_json_paths:
        try:
            ocr_data = json.loads(rj_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pages = ocr_data.get("pages", [])
        if not pages:
            continue

        rj_stem = rj_path.stem.replace("_result", "")
        pdf_path: Path | None = None
        for pf in pdf_files:
            if Path(pf).stem == rj_stem:
                candidate = project_path / pf
                if candidate.exists():
                    pdf_path = candidate
                    break
        if not pdf_path:
            pdfs = list(project_path.glob("*.pdf"))
            if pdfs:
                pdf_path = pdfs[0]

        for pg in pages:
            pn = pg.get("page_number", 0)
            pw, ph = pg.get("width", 0), pg.get("height", 0)
            if pw and ph:
                all_page_dimensions[pn] = (pw, ph)
            if pdf_path:
                page_pdf_map[pn] = pdf_path

        for page in pages:
            page_num = page.get("page_number", 0)
            for block in page.get("blocks", []):
                if block.get("block_type") != "image":
                    continue
                if block.get("category_code", "") == "stamp":
                    continue
                bid = block.get("id", "")
                coords = block.get("coords_px", [0, 0, 0, 0])
                x1, y1, x2, y2 = coords
                area = (x2 - x1) * (y2 - y1)
                if area < MIN_BLOCK_AREA_PX2:
                    continue
                all_image_blocks.append({
                    "block_id": bid,
                    "page_num": page_num,
                    "crop_url": block.get("crop_url", ""),
                    "coords_px": coords,
                    "ocr_text": block.get("ocr_text", ""),
                    "ocr_label": extract_ocr_label(block),
                })

    return all_image_blocks, all_page_dimensions, page_pdf_map, result_json_paths


def crop_blocks_to_dir(
    project_dir: str,
    output_blocks_dir: Path,
    render_profile: dict,
    block_ids: list[str] | None = None,
    force: bool = False,
) -> dict:
    """Экспериментальный crop с явным render_profile в изолированный output_blocks_dir.

    Не трогает production `_output/blocks/`, не делает compact/full pair,
    не обогащает document_graph. Записывает свой `index.json` с полем
    `render_profile` (для cache-проверки).

    Если `output_blocks_dir/index.json` уже существует И его render_profile совпадает
    с переданным — считается валидным кэшем. В этом случае без `force` повторного
    скачивания нет; функция просто возвращает существующий индекс.
    """
    render_profile = dict(render_profile)  # shallow copy
    dpi = int(render_profile["target_dpi"])
    min_long = int(render_profile["min_long_side_px"])
    profile_name = render_profile.get("name") or f"dpi{dpi}_min{min_long}"

    output_blocks_dir = Path(output_blocks_dir)
    output_blocks_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_blocks_dir / "index.json"

    # Cache check
    if index_path.exists() and not force:
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
            ep = existing.get("render_profile") or {}
            if (int(ep.get("target_dpi", -1)) == dpi
                and int(ep.get("min_long_side_px", -1)) == min_long):
                print(f"  [CACHE] render_profile={profile_name} уже закэширован в {output_blocks_dir}")
                return {
                    "total_blocks": existing.get("total_blocks", 0),
                    "cropped": 0,
                    "skipped": existing.get("total_blocks", 0),
                    "errors": existing.get("errors", 0),
                    "blocks": existing.get("blocks", []),
                    "render_profile": render_profile,
                    "cache_hit": True,
                }
        except Exception:
            pass

    all_image_blocks, all_page_dimensions, page_pdf_map, result_json_paths = (
        _iter_image_blocks_from_ocr(project_dir)
    )
    if not result_json_paths:
        return {"error": "result.json not found"}
    if not all_image_blocks:
        return {"total_blocks": 0, "cropped": 0, "skipped": 0, "errors": 0, "blocks": []}

    if block_ids is not None:
        wanted = set(block_ids)
        all_image_blocks = [b for b in all_image_blocks if b["block_id"] in wanted]

    print(f"  [CROP/{profile_name}] dpi={dpi}, min_long_side={min_long}, "
          f"blocks={len(all_image_blocks)}, out={output_blocks_dir}")

    cropped = 0
    skipped = 0
    errors = 0
    index_blocks: list[dict] = []

    for bi in all_image_blocks:
        bid = bi["block_id"]
        out_file = output_blocks_dir / f"block_{bid}.png"

        # Если файл уже существует и force=False — пробуем переиспользовать.
        # Но т.к. без render_profile-кеша мы не знаем, с каким профилем рендерился
        # предыдущий PNG, при любом рассогласовании cache'а пересчитываем.
        source = "cloud"
        download_error: object | None = None
        w = h = 0

        if bi["crop_url"]:
            try:
                w, h = download_and_convert(
                    bi["crop_url"], out_file,
                    dpi=dpi, min_long_side=min_long,
                )
            except Exception as e:
                download_error = e
        else:
            download_error = "нет crop_url"

        if download_error is not None:
            pn = bi["page_num"]
            dims = all_page_dimensions.get(pn)
            fallback_pdf = page_pdf_map.get(pn)
            if fallback_pdf and dims:
                try:
                    w, h = crop_from_pdf(
                        fallback_pdf, pn,
                        bi["coords_px"], dims[0], dims[1],
                        out_file,
                        dpi=dpi, min_long_side=min_long,
                    )
                    source = "pdf_fallback"
                except Exception:
                    errors += 1
                    continue
            else:
                errors += 1
                continue

        size_kb = out_file.stat().st_size / 1024
        index_blocks.append({
            "block_id": bid,
            "page": bi["page_num"],
            "file": f"block_{bid}.png",
            "size_kb": round(size_kb, 1),
            "crop_px": bi["coords_px"],
            "render_size": [w, h],
            "block_type": "image",
            "ocr_label": bi["ocr_label"],
            "ocr_text_len": len(bi["ocr_text"]),
            "source": source,
        })
        cropped += 1

    index_data = {
        "total_blocks": len(index_blocks),
        "total_expected": len(all_image_blocks),
        "errors": errors,
        "compact": False,
        "render_profile": render_profile,
        "source_result_json": [rj.name for rj in result_json_paths],
        "blocks": index_blocks,
    }
    index_path.write_text(
        json.dumps(index_data, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    return {
        "total_blocks": len(index_blocks),
        "cropped": cropped,
        "skipped": skipped,
        "errors": errors,
        "blocks": index_blocks,
        "render_profile": render_profile,
        "cache_hit": False,
    }


def detect_result_json(project_dir: str) -> Path | None:
    """Найти *_result.json в папке проекта (один — основной)."""
    results = detect_all_result_jsons(project_dir)
    return results[0] if results else None


def detect_all_result_jsons(project_dir: str) -> list[Path]:
    """Найти все *_result.json, соответствующие PDF-файлам проекта.

    Если в project_info.json есть pdf_files — возвращает result.json
    для каждого PDF (в порядке pdf_files). Иначе все найденные.
    """
    project_path = Path(project_dir)
    candidates = list(project_path.glob("*_result.json"))
    if not candidates:
        return []

    info_path = project_path / "project_info.json"
    if info_path.exists():
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except (json.JSONDecodeError, OSError):
            info = {}

        pdf_files = info.get("pdf_files", [])
        if not pdf_files:
            pdf_file = info.get("pdf_file", "")
            pdf_files = [pdf_file] if pdf_file else []

        if pdf_files:
            # Сопоставляем pdf_stem -> result.json
            stem_to_candidate = {}
            for c in candidates:
                stem = c.stem.replace("_result", "")
                stem_to_candidate[stem] = c

            ordered = []
            for pf in pdf_files:
                pdf_stem = Path(pf).stem
                if pdf_stem in stem_to_candidate:
                    ordered.append(stem_to_candidate[pdf_stem])
            # Добавить непарные (на всякий случай)
            for c in candidates:
                if c not in ordered:
                    ordered.append(c)
            return ordered

    return sorted(candidates)


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


def _render_pdf_bytes_to_png(
    pdf_bytes: bytes,
    out_png: Path,
    target_px: int = 0,
    dpi: int = 0,
    min_long_side: int = 0,
) -> tuple[int, int]:
    """Рендерить PDF-байты в PNG. Возвращает (w, h).

    Два режима (dpi приоритетнее):
      - dpi > 0:  фиксированная плотность (scale = dpi / 72)
      - target_px > 0: длинная сторона = target_px пикселей (legacy)

    min_long_side: при dpi-режиме гарантирует, что длинная сторона PNG ≥ min_long_side.
    Если на нативном DPI блок получается мельче — scale поднимается до нужного.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]

    long_side_pt = max(page.rect.width, page.rect.height)
    if long_side_pt < 1:
        doc.close()
        raise ValueError("Нулевой размер страницы в PDF-кропе")

    if dpi > 0:
        render_scale = dpi / 72
        if min_long_side > 0:
            render_scale = max(render_scale, min_long_side / long_side_pt)
    elif target_px > 0:
        render_scale = target_px / long_side_pt
    else:
        render_scale = TARGET_DPI / 72

    render_scale = max(1.0, min(8.0, render_scale))

    mat = fitz.Matrix(render_scale, render_scale)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    pix.save(str(out_png))

    w, h = pix.width, pix.height
    doc.close()
    return w, h


def download_and_convert(
    crop_url: str,
    out_png: Path,
    timeout: int = 30,
    target_px: int | None = None,
    also_save_full: Path | None = None,
    full_target_px: int | None = None,
    dpi: int = 0,
    min_long_side: int = 0,
) -> tuple[int, int]:
    """Скачать PDF-кроп по URL и конвертировать в PNG.

    also_save_full: если указан — дополнительно рендерит full-версию в этот путь.
    dpi: если > 0 — рендерить с фиксированной плотностью (приоритет над target_px).
    min_long_side: гибрид — минимальная длинная сторона PNG в DPI-режиме.
    """
    _require_pymupdf()
    req = urllib.request.Request(crop_url, headers={"User-Agent": "crop_blocks/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        pdf_bytes = resp.read()

    w, h = _render_pdf_bytes_to_png(
        pdf_bytes, out_png,
        target_px=target_px or 0, dpi=dpi, min_long_side=min_long_side,
    )

    # Дополнительный рендер full-версии из тех же байт (без повторного скачивания)
    if also_save_full:
        _render_pdf_bytes_to_png(
            pdf_bytes, also_save_full,
            target_px=full_target_px or 0,
            dpi=dpi or TARGET_DPI,
            min_long_side=min_long_side,
        )

    return w, h


def crop_from_pdf(
    pdf_path: Path,
    page_num: int,
    coords_px: list,
    page_width: int,
    page_height: int,
    out_png: Path,
    target_px: int | None = None,
    also_save_full: Path | None = None,
    full_target_px: int | None = None,
    dpi: int = 0,
    min_long_side: int = 0,
) -> tuple[int, int]:
    """Вырезать блок из PDF по координатам (fallback при ошибке скачивания).

    coords_px: [x1, y1, x2, y2] в пиксельной системе result.json
    page_width, page_height: размеры страницы в пикселях из result.json
    also_save_full: если указан — дополнительно рендерит full-версию.
    dpi: если > 0 — рендерить с фиксированной плотностью (приоритет над target_px).
    min_long_side: гибрид — минимальная длинная сторона PNG в DPI-режиме.
    """
    _require_pymupdf()
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

    clip_w = clip.width
    clip_h = clip.height
    long_side_pt = max(clip_w, clip_h)
    if long_side_pt < 1:
        doc.close()
        raise ValueError("Нулевой размер блока")

    def _render_clip(target: int, output: Path, clip_dpi: int = 0):
        if clip_dpi > 0:
            rs = clip_dpi / 72
            if min_long_side > 0:
                rs = max(rs, min_long_side / long_side_pt)
        else:
            rs = target / long_side_pt
        rs = max(0.5, min(8.0, rs))
        mat = fitz.Matrix(rs, rs)
        pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
        pix.save(str(output))
        return pix.width, pix.height

    w, h = _render_clip(target_px or TARGET_LONG_SIDE_PX, out_png, clip_dpi=dpi)

    if also_save_full:
        _render_clip(full_target_px or TARGET_LONG_SIDE_PX, also_save_full, clip_dpi=dpi or TARGET_DPI)

    doc.close()
    return w, h


def crop_blocks(
    project_dir: str,
    block_ids: list[str] | None = None,
    force: bool = False,
    compact: bool = False,
) -> dict:
    """Скачать image-блоки по crop_url из result.json и сохранить как PNG.

    compact=True: сохраняет ДВЕ версии каждого блока:
      - block_<ID>.png       — compact (800px) — используется в батчах
      - block_<ID>_full.png  — full (1500px+)  — для retry нечитаемых

    При наличии нескольких PDF (pdf_files в project_info.json) обрабатывает
    все соответствующие *_result.json и объединяет блоки.
    """
    result_json_paths = detect_all_result_jsons(project_dir)
    if not result_json_paths:
        print(f"[ERROR] *_result.json не найден в {project_dir}")
        return {"error": "result.json not found"}

    project_path = Path(project_dir)

    # Загрузить project_info для списка PDF (fallback)
    info = {}
    info_path = project_path / "project_info.json"
    if info_path.exists():
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception:
            pass

    pdf_files = info.get("pdf_files", [])
    if not pdf_files:
        pf = info.get("pdf_file", "")
        pdf_files = [pf] if pf else []

    if len(result_json_paths) > 1:
        print(f"  Multi-PDF: {len(result_json_paths)} result.json файлов")

    all_image_blocks = []
    all_page_dimensions: dict[int, tuple[int, int]] = {}
    # Карта page_num -> pdf_path для fallback кропинга
    page_pdf_map: dict[int, Path] = {}
    no_url_count = 0

    for rj_path in result_json_paths:
        print(f"  OCR result: {rj_path.name}")
        with open(rj_path, "r", encoding="utf-8") as f:
            ocr_data = json.load(f)

        pages = ocr_data.get("pages", [])
        if not pages:
            print(f"  [WARN] Нет страниц в {rj_path.name}")
            continue

        # Определяем PDF для этого result.json
        rj_stem = rj_path.stem.replace("_result", "")
        pdf_path: Path | None = None
        for pf in pdf_files:
            if Path(pf).stem == rj_stem:
                candidate = project_path / pf
                if candidate.exists():
                    pdf_path = candidate
                    break
        if not pdf_path:
            pdfs = list(project_path.glob("*.pdf"))
            if pdfs:
                pdf_path = pdfs[0]

        for pg in pages:
            pn = pg.get("page_number", 0)
            pw = pg.get("width", 0)
            ph = pg.get("height", 0)
            if pw and ph:
                all_page_dimensions[pn] = (pw, ph)
            if pdf_path:
                page_pdf_map[pn] = pdf_path

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

    if compact:
        print(f"  [COMPACT] Режим compact: {TARGET_DPI_COMPACT} DPI + full-версии ({TARGET_DPI} DPI)")

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
        full_file = output_dir / f"block_{bid}_full.png"

        if out_file.exists() and not force:
            size_kb = out_file.stat().st_size / 1024
            if size_kb > 1:
                has_full = full_file.exists()
                full_kb = round(full_file.stat().st_size / 1024, 1) if has_full else None
                print(f"  [EXISTS] {bid} ({size_kb:.0f} KB)" +
                      (f" +full({full_kb:.0f} KB)" if has_full else ""))
                entry = {
                    "block_id": bid,
                    "page": block_info["page_num"],
                    "file": f"block_{bid}.png",
                    "size_kb": round(size_kb, 1),
                    "crop_px": block_info["coords_px"],
                    "block_type": "image",
                    "ocr_label": block_info["ocr_label"],
                    "ocr_text_len": len(block_info["ocr_text"]),
                }
                if has_full:
                    entry["file_full"] = f"block_{bid}_full.png"
                    entry["size_kb_full"] = full_kb
                    entry["compact"] = True
                index_blocks.append(entry)
                skipped += 1
                continue

        source = "cloud"
        crop_url = block_info["crop_url"]
        download_error = None

        # DPI-режим: единое разрешение для всех блоков
        use_dpi = TARGET_DPI_COMPACT if compact else TARGET_DPI
        use_min_side = MIN_LONG_SIDE_PX_COMPACT if compact else MIN_LONG_SIDE_PX
        save_full = full_file if compact else None

        if crop_url:
            try:
                w, h = download_and_convert(
                    crop_url, out_file,
                    dpi=use_dpi,
                    min_long_side=use_min_side,
                    also_save_full=save_full,
                )
            except Exception as e:
                download_error = e
        else:
            download_error = "нет crop_url"

        if download_error is not None:
            e = download_error
            # Fallback: вырезаем из PDF по координатам
            pn = block_info["page_num"]
            dims = all_page_dimensions.get(pn)
            fallback_pdf = page_pdf_map.get(pn)
            if fallback_pdf and dims:
                try:
                    w, h = crop_from_pdf(
                        fallback_pdf, pn,
                        block_info["coords_px"],
                        dims[0], dims[1],
                        out_file,
                        dpi=use_dpi,
                        min_long_side=use_min_side,
                        also_save_full=save_full,
                    )
                    source = "pdf_fallback"
                    print(f"  [FALLBACK] {bid}: облако недоступно ({e}), вырезан из PDF")
                except Exception as e2:
                    print(f"  [ERROR] {bid}: облако ({e}), PDF ({e2})")
                    errors += 1
                    continue
            else:
                print(f"  [ERROR] {bid}: {e}" +
                      ("" if fallback_pdf else " (PDF не найден для fallback)"))
                errors += 1
                continue

        size_kb = out_file.stat().st_size / 1024
        full_kb = round(full_file.stat().st_size / 1024, 1) if compact and full_file.exists() else None
        label = "DOWNLOAD" if source == "cloud" else "PDF-CROP"
        compact_tag = f" +full({full_kb:.0f} KB)" if full_kb else ""
        print(f"  [{label}] {bid}: стр.{block_info['page_num']}, "
              f"{w}x{h}px, {size_kb:.0f} KB{compact_tag}")
        entry = {
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
        }
        if compact and full_kb is not None:
            entry["file_full"] = f"block_{bid}_full.png"
            entry["size_kb_full"] = full_kb
            entry["compact"] = True
        index_blocks.append(entry)
        cropped += 1

    # Cleanup только при полном прогоне
    if not block_ids:
        valid_files = {f"block_{b['block_id']}.png" for b in index_blocks}
        if compact:
            valid_files |= {f"block_{b['block_id']}_full.png" for b in index_blocks}
        for old_png in output_dir.glob("block_*.png"):
            if old_png.name not in valid_files:
                print(f"  [CLEANUP] {old_png.name}")
                old_png.unlink()

    index_data = {
        "total_blocks": len(index_blocks),
        "total_expected": len(all_image_blocks),
        "errors": errors,
        "compact": compact,
        "source_result_json": [rj.name for rj in result_json_paths],
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

# Page-level укрупнение ОТКЛЮЧЕНО. Chandra OCR уже режет чертёж на логические фрагменты
# (разрезы, таблицы, сечения — каждый отдельным PNG). Попытка рендерить полную страницу
# (≥8 мелких блоков) или квадранты (≥15) даёт на формате A0 (1783×841 мм) при 1500 px
# всего ~43 DPI — текст 2 мм превращается в 3-4 px и становится нечитаемым.
# OCR-блоки по отдельности имеют 150-400 DPI и читаются уверенно.
PAGE_MERGE_MIN_BLOCKS = 999     # было 8 — отключено (доверяем OCR)
PAGE_MERGE_THRESHOLD_KB = 100
PAGE_QUADRANT_MIN_BLOCKS = 999  # было 15 — отключено (квадранты A0 = нечитаемо)

# Плотные страницы: если у страницы >= этого числа блоков — её блоки не мешаются
# с другими страницами в одном батче, чтобы модель видела разрезы одной страницы вместе.
DENSE_PAGE_THRESHOLD = 20

# Гибридная стратегия: ограничение по объёму И по количеству.
# Дефолтные лимиты — под Claude Messages API (max 20 images, ~5MB payload).
# Для других моделей (Gemini 3 Pro и т.д.) лимиты переопределяются через MODEL_BATCH_LIMITS ниже.
MAX_BATCH_SIZE_KB = 5 * 1024   # 5 MB целевой объём пакета
MAX_BLOCKS_PER_BATCH = 15      # макс блоков (даже если суммарно мало весят)
MIN_BLOCKS_PER_BATCH = 3       # мин блоков (не дробить на слишком мелкие пакеты)
SOLO_BLOCK_THRESHOLD_KB = 3 * 1024  # блок > 3 MB — отдельный пакет

# Per-model лимиты для этапа block_batch. Ключи — model id из stage_models.json.
# Значения подобраны эмпирически: у каждой модели свой лимит на количество изображений
# и размер payload, а также свой оптимум по attention.
MODEL_BATCH_LIMITS: dict[str, dict[str, int]] = {
    # Claude Vision (Opus 4.7 / Sonnet 4.6) — production-safe risk-aware policy.
    # Hard cap 12 блоков для любого пути. Фактические размеры пакета выбирает
    # risk-aware packer: heavy≈5, normal≈8, light≈10. Compact НЕ раздувает этот cap.
    "claude-opus-4-7":                 {"max_blocks": 12, "max_size_kb": 5120,  "solo_kb": 3072, "min_blocks": 3},
    "claude-sonnet-4-6":               {"max_blocks": 12, "max_size_kb": 5120,  "solo_kb": 3072, "min_blocks": 3},
    # Gemini 3 Pro: технически выдерживает 3072 images, 20MB, 1M контекст.
    # НО: эмпирически (проект 13АВ-РД-КЖ5.17-23.1-К2, 30 blocks/batch → 0 findings + 26/49 unreadable).
    # Attention dilution: модель описывает блоки в summary, но analysis на замечания не делает.
    # Сладкая точка 12-15 блоков (проверено на 4 batches × 12-13 → 20 findings).
    "google/gemini-3.1-pro-preview":   {"max_blocks": 15, "max_size_kb": 12000, "solo_kb": 8000, "min_blocks": 5},
    # GPT-5: до ~50 images, 400K контекст. Консервативный максимум 20.
    "openai/gpt-5.4":                  {"max_blocks": 20, "max_size_kb": 10000, "solo_kb": 5120, "min_blocks": 3},
}

# ─── Claude vision risk-aware batching ─────────────────────────────────────
# Claude Opus 4.7 Vision плохо переваривает большие пачки разнотипных блоков.
# Классификация по уже существующим metadata (size_kb, render_size, ocr_text_len,
# is_full_page, merged_block_ids, quadrant, crop_px) — без повторного OCR.
# Heavy → маленький пакет, light → больше. Hard cap = CLAUDE_HARD_CAP.
CLAUDE_HARD_CAP = 12  # абсолютный потолок батча для Claude, compact его НЕ поднимает
_CLAUDE_RISK_TARGETS: dict[str, dict[str, int]] = {
    "heavy":  {"target": 5,  "max": 6},
    "normal": {"target": 8,  "max": 8},
    "light":  {"target": 10, "max": 10},
}

# ENV overrides — только для экспериментов. Если переменные не заданы, используются
# дефолтные _CLAUDE_RISK_TARGETS. Ни один override не может поднять batch выше
# CLAUDE_HARD_CAP (clamp при чтении).
# Имена: CLAUDE_BATCH_{HEAVY,NORMAL,LIGHT}_{TARGET,MAX}
_RISK_ENV_KEYS: dict[str, dict[str, str]] = {
    "heavy":  {"target": "CLAUDE_BATCH_HEAVY_TARGET",  "max": "CLAUDE_BATCH_HEAVY_MAX"},
    "normal": {"target": "CLAUDE_BATCH_NORMAL_TARGET", "max": "CLAUDE_BATCH_NORMAL_MAX"},
    "light":  {"target": "CLAUDE_BATCH_LIGHT_TARGET",  "max": "CLAUDE_BATCH_LIGHT_MAX"},
}


def _coerce_positive_int(raw: str | None, fallback: int) -> int:
    if not raw:
        return fallback
    try:
        val = int(str(raw).strip())
    except (TypeError, ValueError):
        return fallback
    return val if val >= 1 else fallback


def read_claude_risk_overrides(
    env: dict | None = None,
    hard_cap: int = CLAUDE_HARD_CAP,
) -> dict[str, dict[str, int]]:
    """Прочитать ENV-overrides для risk-aware batching.

    Возвращает словарь той же структуры, что и `_CLAUDE_RISK_TARGETS`.
    Клэмп: target и max не могут превышать `hard_cap`; max >= target.
    Если переменная не задана — берётся дефолт.
    """
    env_map = env if env is not None else os.environ
    result: dict[str, dict[str, int]] = {}
    for risk, defaults in _CLAUDE_RISK_TARGETS.items():
        target = _coerce_positive_int(env_map.get(_RISK_ENV_KEYS[risk]["target"]), defaults["target"])
        mx     = _coerce_positive_int(env_map.get(_RISK_ENV_KEYS[risk]["max"]),    defaults["max"])
        target = min(target, hard_cap)
        mx     = min(mx, hard_cap)
        if mx < target:
            mx = target
        result[risk] = {"target": target, "max": mx}
    return result


def make_claude_risk_profile(
    heavy_target: int, heavy_max: int,
    normal_target: int, normal_max: int,
    light_target: int, light_max: int,
    hard_cap: int = CLAUDE_HARD_CAP,
) -> dict[str, dict[str, int]]:
    """Построить риск-профиль из явных чисел (для экспериментов). Clamp по hard_cap."""
    def _pair(t: int, m: int) -> dict[str, int]:
        t = max(1, min(int(t), hard_cap))
        m = max(t, min(int(m), hard_cap))
        return {"target": t, "max": m}
    return {
        "heavy":  _pair(heavy_target, heavy_max),
        "normal": _pair(normal_target, normal_max),
        "light":  _pair(light_target, light_max),
    }

# Дефолтный профиль для неизвестных моделей — как у Claude (самый строгий).
_DEFAULT_BATCH_LIMITS = {"max_blocks": MAX_BLOCKS_PER_BATCH, "max_size_kb": MAX_BATCH_SIZE_KB,
                         "solo_kb": SOLO_BLOCK_THRESHOLD_KB, "min_blocks": MIN_BLOCKS_PER_BATCH}

_STAGE_MODELS_PATH = Path(__file__).resolve().parent / "webapp" / "data" / "stage_models.json"


def _get_batch_limits_for_current_model(stage: str = "block_batch") -> dict[str, int]:
    """Вернуть лимиты пакетизации для модели, настроенной на данный этап.

    Читает webapp/data/stage_models.json; если файла нет или модели неизвестна — дефолт.
    """
    try:
        if not _STAGE_MODELS_PATH.exists():
            return dict(_DEFAULT_BATCH_LIMITS)
        cfg = json.loads(_STAGE_MODELS_PATH.read_text(encoding="utf-8"))
        model_id = cfg.get(stage, "")
        return dict(MODEL_BATCH_LIMITS.get(model_id, _DEFAULT_BATCH_LIMITS))
    except Exception:
        return dict(_DEFAULT_BATCH_LIMITS)


def _render_full_page(pdf_path: Path, page_num: int, output_path: Path,
                      target_px: int = TARGET_LONG_SIDE_PX) -> dict | None:
    """Рендерить полную страницу PDF как PNG. Возвращает dict с метаданными блока."""
    _require_pymupdf()
    try:
        doc = fitz.open(str(pdf_path))
        # page_num в index.json — 1-based
        page_idx = page_num - 1
        if page_idx < 0 or page_idx >= len(doc):
            doc.close()
            return None

        page = doc[page_idx]
        rect = page.rect
        long_side = max(rect.width, rect.height)
        scale = target_px / long_side if long_side > 0 else 1.0
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat, alpha=False)

        pix.save(str(output_path))
        size_kb = output_path.stat().st_size / 1024
        doc.close()

        return {
            "block_id": f"page_{page_num:03d}",
            "page": page_num,
            "file": output_path.name,
            "size_kb": round(size_kb, 1),
            "ocr_label": f"Полная страница {page_num}",
            "is_full_page": True,
        }
    except Exception as e:
        print(f"  [WARN] Не удалось отрендерить стр. {page_num}: {e}")
        return None


def _render_page_quadrants(pdf_path: Path, page_num: int, blocks_dir: Path,
                           target_px: int = TARGET_LONG_SIDE_PX) -> list[dict]:
    """Рендерить страницу PDF как 4 четверти (2×2). Возвращает список dict-ов блоков."""
    _require_pymupdf()
    try:
        doc = fitz.open(str(pdf_path))
        page_idx = page_num - 1
        if page_idx < 0 or page_idx >= len(doc):
            doc.close()
            return []

        page = doc[page_idx]
        rect = page.rect
        w2, h2 = rect.width / 2, rect.height / 2
        quads = {
            "TL": fitz.Rect(0, 0, w2, h2),
            "TR": fitz.Rect(w2, 0, rect.width, h2),
            "BL": fitz.Rect(0, h2, w2, rect.height),
            "BR": fitz.Rect(w2, h2, rect.width, rect.height),
        }

        results: list[dict] = []
        for name, clip in quads.items():
            long_side = max(clip.width, clip.height)
            scale = target_px / long_side if long_side > 0 else 1.0
            mat = fitz.Matrix(scale, scale)
            pix = page.get_pixmap(matrix=mat, alpha=False, clip=clip)
            out_path = blocks_dir / f"block_page_{page_num:03d}_{name}.png"
            pix.save(str(out_path))
            size_kb = out_path.stat().st_size / 1024
            results.append({
                "block_id": f"page_{page_num:03d}_{name}",
                "page": page_num,
                "file": out_path.name,
                "size_kb": round(size_kb, 1),
                "ocr_label": f"Четверть {name} стр. {page_num}",
                "is_full_page": True,
                "quadrant": name,
            })

        doc.close()
        return results
    except Exception as e:
        print(f"  [WARN] Не удалось отрендерить четверти стр. {page_num}: {e}")
        return []


def _consolidate_small_blocks(
    pages_map: dict[int, list[dict]],
    project_dir: Path,
    blocks_dir: Path,
    min_blocks: int = PAGE_MERGE_MIN_BLOCKS,
    threshold_kb: float = PAGE_MERGE_THRESHOLD_KB,
    quadrant_min_blocks: int = PAGE_QUADRANT_MIN_BLOCKS,
) -> dict[int, list[dict]]:
    """Заменить страницы с множеством мелких блоков на полностраничные изображения.

    Returns: обновлённый pages_map.
    """
    # Найти PDF
    info_path = project_dir / "project_info.json"
    if not info_path.exists():
        return pages_map

    info = json.loads(info_path.read_text(encoding="utf-8"))
    pdf_file = info.get("pdf_file", "")
    if not pdf_file:
        pdf_files = info.get("pdf_files", [])
        pdf_file = pdf_files[0] if pdf_files else ""
    if not pdf_file:
        return pages_map

    pdf_path = project_dir / pdf_file
    if not pdf_path.exists():
        return pages_map

    if fitz is None:
        return pages_map

    consolidated = {}
    for page_num, page_blocks in pages_map.items():
        small = [b for b in page_blocks if b.get("size_kb", 0) < threshold_kb]
        big = [b for b in page_blocks if b.get("size_kb", 0) >= threshold_kb]

        if len(small) >= quadrant_min_blocks:
            # Слишком плотная страница — полностраничный PNG будет нечитаем.
            # Рендерим 4 четверти (каждое сечение получает ≈600-700 px вместо ≈300 px).
            merged_ids = [b["block_id"] for b in small]
            quad_blocks = _render_page_quadrants(pdf_path, page_num, blocks_dir)
            if quad_blocks:
                for qb in quad_blocks:
                    qb["merged_block_ids"] = merged_ids
                    qb["ocr_label"] = f"Четверть {qb['quadrant']} стр. {page_num} ({len(small)} мелких блоков на странице)"
                consolidated[page_num] = big + quad_blocks
                total_kb = sum(qb["size_kb"] for qb in quad_blocks)
                print(f"  Стр. {page_num}: {len(small)} мелких блоков -> 4 четверти ({total_kb:.0f} KB)"
                      + (f" + {len(big)} крупных" if big else ""))
                continue

        if len(small) >= min_blocks:
            # Рендерим полную страницу
            out_path = blocks_dir / f"block_page_{page_num:03d}.png"
            merged_labels = [b.get("ocr_label", "") for b in small[:5]]
            page_block = _render_full_page(pdf_path, page_num, out_path)
            if page_block:
                page_block["ocr_label"] = f"Полная стр. {page_num} ({len(small)} блоков: {', '.join(l[:30] for l in merged_labels if l)[:100]})"
                page_block["merged_block_ids"] = [b["block_id"] for b in small]
                consolidated[page_num] = big + [page_block]
                print(f"  Стр. {page_num}: {len(small)} мелких блоков -> 1 полная страница ({page_block['size_kb']:.0f} KB)"
                      + (f" + {len(big)} крупных" if big else ""))
                continue

        consolidated[page_num] = page_blocks

    return consolidated


def _make_batch_entry(batch_id: int, blocks_list: list[dict]) -> dict:
    """Сформировать запись пакета из списка блоков.

    Для консолидированных блоков (page_NNN / page_NNN_TL/TR/BL/BR)
    сохраняется merged_block_ids — список исходных OCR-блоков,
    контент которых попал в данный synthetic-блок. Нужно для UI:
    показывать «Разобран в составе стр. N / четверти N-TL».
    """
    def _block_info(b: dict) -> dict:
        info = {
            "block_id": b["block_id"],
            "page": b["page"],
            "file": b["file"],
            "size_kb": b.get("size_kb", 0),
            "ocr_label": b.get("ocr_label", "image"),
        }
        if b.get("merged_block_ids"):
            info["merged_block_ids"] = list(b["merged_block_ids"])
        if b.get("quadrant"):
            info["quadrant"] = b["quadrant"]
        if b.get("is_full_page"):
            info["is_full_page"] = True
        return info

    return {
        "batch_id": batch_id,
        "blocks": [_block_info(b) for b in blocks_list],
        "pages_included": sorted(set(b["page"] for b in blocks_list)),
        "block_count": len(blocks_list),
        "total_size_kb": sum(b.get("size_kb", 0) for b in blocks_list),
    }


def _classify_block_risk(block: dict) -> str:
    """Классификация блока как heavy / normal / light по существующим metadata.

    Использует только поля из blocks/index.json (без пикселей и повторного OCR):
      is_full_page, quadrant, merged_block_ids, size_kb, render_size, ocr_text_len, crop_px.

    Heavy — full-page рендер, квадрант страницы, укрупнённый блок, либо «тяжёлый» по
    массе/геометрии/OCR. Normal — средний плотности. Light — всё остальное.
    """
    if block.get("is_full_page"):
        return "heavy"
    if block.get("quadrant"):
        return "heavy"
    if block.get("merged_block_ids"):
        return "heavy"

    try:
        size_kb = float(block.get("size_kb", 0) or 0)
    except (TypeError, ValueError):
        size_kb = 0.0
    try:
        ocr_len = int(block.get("ocr_text_len", 0) or 0)
    except (TypeError, ValueError):
        ocr_len = 0

    render = block.get("render_size") or []
    render_long = 0.0
    if isinstance(render, (list, tuple)) and render:
        try:
            render_long = max(float(x) for x in render)
        except (TypeError, ValueError):
            render_long = 0.0

    crop = block.get("crop_px") or []
    crop_long = 0.0
    if isinstance(crop, (list, tuple)) and len(crop) == 4:
        try:
            crop_long = max(float(crop[2]) - float(crop[0]),
                            float(crop[3]) - float(crop[1]))
        except (TypeError, ValueError):
            crop_long = 0.0

    if size_kb >= 2000 or render_long >= 2500 or ocr_len >= 4000 or crop_long >= 3000:
        return "heavy"
    if size_kb >= 500 or render_long >= 1500 or ocr_len >= 1000 or crop_long >= 1500:
        return "normal"
    return "light"


def _claude_cap_for_risk(risk: str, hard_cap: int = CLAUDE_HARD_CAP,
                         risk_targets: dict[str, dict[str, int]] | None = None) -> int:
    """Максимальное количество блоков в пакете для данного уровня риска (Claude)."""
    targets = risk_targets or _CLAUDE_RISK_TARGETS
    per_risk = targets.get(risk, targets.get("normal", _CLAUDE_RISK_TARGETS["normal"]))["max"]
    return min(per_risk, hard_cap)


def _pack_blocks_claude_risk_aware(
    blocks: list[dict],
    max_size_kb: int = MAX_BATCH_SIZE_KB,
    solo_threshold_kb: int = SOLO_BLOCK_THRESHOLD_KB,
    dense_pages: set | None = None,
    hard_cap: int = CLAUDE_HARD_CAP,
    risk_targets: dict[str, dict[str, int]] | None = None,
) -> list[list[dict]]:
    """Claude-specific packing: heavy → малый пакет, light → крупнее.

    Правила:
      1. solo_threshold_kb: крупные блоки (≥ порога) идут отдельным пакетом.
      2. В процессе набора batch следит за минимальным cap-ом по риску всех блоков.
         Добавление heavy-блока в batch понижает cap до heavy-максимума.
      3. heavy-блоки НЕ набиваются толпой — как только набрали heavy['max'] heavy-блоков,
         начинается новый пакет (даже если cap по другим рискам позволяет больше).
      4. Жёсткий hard_cap на batch независимо от риска (production-safe).
      5. Порядок по страницам сохраняется (blocks уже упорядочены вызывающим кодом).
      6. Плотные страницы (dense_pages) изолируются как в _pack_blocks_adaptive.

    risk_targets: dict с ключами heavy/normal/light и значениями {target, max}.
        По умолчанию — _CLAUDE_RISK_TARGETS. Для экспериментов передавать явно.
        Любой max > hard_cap клэмпится.
    """
    if not blocks:
        return []

    hard_cap = min(hard_cap, CLAUDE_HARD_CAP)
    targets = risk_targets or _CLAUDE_RISK_TARGETS
    _dense = set(dense_pages or [])

    solo: list[dict] = []
    normal_list: list[dict] = []
    for b in blocks:
        if b.get("size_kb", 0) >= solo_threshold_kb:
            solo.append(b)
        else:
            normal_list.append(b)

    packed: list[list[dict]] = []
    current: list[dict] = []
    current_size = 0.0
    current_cap = hard_cap
    current_heavy = 0
    heavy_max = min(targets.get("heavy", _CLAUDE_RISK_TARGETS["heavy"])["max"], hard_cap)

    for b in normal_list:
        risk = _classify_block_risk(b)
        b_cap = _claude_cap_for_risk(risk, hard_cap, risk_targets=targets)
        b_size = float(b.get("size_kb", 0) or 0)
        b_page = b.get("page", 0)

        needs_new = False
        if current:
            proposed_cap = min(current_cap, b_cap)
            if len(current) >= proposed_cap:
                needs_new = True
            elif current_size + b_size > max_size_kb:
                needs_new = True
            elif risk == "heavy" and current_heavy >= heavy_max:
                needs_new = True
            elif _dense:
                current_pages = {x.get("page", 0) for x in current}
                if (current_pages & _dense or b_page in _dense) and b_page not in current_pages:
                    needs_new = True

        if needs_new:
            packed.append(current)
            current = []
            current_size = 0.0
            current_cap = hard_cap
            current_heavy = 0

        current.append(b)
        current_size += b_size
        current_cap = min(current_cap, b_cap)
        if risk == "heavy":
            current_heavy += 1

    if current:
        packed.append(current)

    for b in solo:
        packed.append([b])

    # Defensive: абсолютный hard cap даже если что-то пошло не так
    final: list[list[dict]] = []
    for group in packed:
        if len(group) <= hard_cap:
            final.append(group)
        else:
            for i in range(0, len(group), hard_cap):
                final.append(group[i:i + hard_cap])
    return final


def _pack_blocks_adaptive(
    blocks: list[dict],
    max_size_kb: int = MAX_BATCH_SIZE_KB,
    max_blocks: int = MAX_BLOCKS_PER_BATCH,
    min_blocks: int = MIN_BLOCKS_PER_BATCH,
    solo_threshold_kb: int = SOLO_BLOCK_THRESHOLD_KB,
    dense_pages: set | None = None,
) -> list[list[dict]]:
    """Разбить блоки на пакеты по гибридной стратегии (объём + количество).

    Правила:
    1. Блок > solo_threshold_kb → отдельный пакет (крупный чертёж)
    2. Набираем блоки пока не упрёмся в max_size_kb или max_blocks
    3. Если остаток < min_blocks → присоединяем к предыдущему пакету
    4. dense_pages: блоки из этих страниц не мешаются с другими страницами
       в одном батче (сохраняется контекст плотной страницы).
    """
    if not blocks:
        return []

    _dense = set(dense_pages or [])

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
        b_page = b.get("page", 0)

        needs_new = False
        if current:
            # Переполнение пакета
            if current_size + b_size > max_size_kb or len(current) >= max_blocks:
                needs_new = True
            # Граница плотной страницы: не мешаем блоки плотной страницы
            # с блоками другой страницы
            elif _dense:
                current_pages = {x.get("page", 0) for x in current}
                if (current_pages & _dense or b_page in _dense) and b_page not in current_pages:
                    needs_new = True

        if needs_new:
            packed.append(current)
            current = []
            current_size = 0

        current.append(b)
        current_size += b_size

    if current:
        packed.append(current)

    # Если последний пакет слишком мелкий — присоединяем к предыдущему
    # (но только если итого не превысит max_blocks)
    if len(packed) >= 2 and len(packed[-1]) < min_blocks:
        if len(packed[-2]) + len(packed[-1]) <= max_blocks:
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
    max_size_kb: int | None = None,
    max_blocks: int | None = None,
    min_blocks: int | None = None,
    solo_kb: int | None = None,
    solo: bool = False,
) -> dict:
    """Сгруппировать image-блоки в пакеты.

    adaptive=True (по умолчанию): гибридная стратегия по объёму + количеству.
    adaptive=False: старая стратегия (фиксированный batch_size).

    solo=True: форс по одному блоку в пакете (для retry нечитаемых блоков —
    модель фокусируется на одной картинке).

    Если max_size_kb/max_blocks/min_blocks/solo_kb не переданы — берутся per-model лимиты
    для текущей модели этапа block_batch (из webapp/data/stage_models.json).
    """
    output_dir = Path(project_dir) / "_output"
    index_path = output_dir / "blocks" / "index.json"

    if not index_path.exists():
        print(f"[ERROR] {index_path} не найден. Сначала запустите: python blocks.py crop")
        return {"error": "blocks/index.json not found"}

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    # Выбор лимитов: явные аргументы CLI > per-model профиль > глобальный дефолт
    model_limits = _get_batch_limits_for_current_model("block_batch")
    if max_size_kb is None: max_size_kb = model_limits["max_size_kb"]
    if max_blocks is None:  max_blocks  = model_limits["max_blocks"]
    if min_blocks is None:  min_blocks  = model_limits["min_blocks"]
    if solo_kb is None:     solo_kb     = model_limits["solo_kb"]

    # Solo-режим: по одному блоку в пакете (retry нечитаемых — модель сфокусируется)
    if solo:
        max_blocks = 1
        min_blocks = 1
        print("  [SOLO] Форс по 1 блоку в пакете (retry unreadable)")

    try:
        current_model = json.loads(_STAGE_MODELS_PATH.read_text(encoding="utf-8")).get("block_batch", "?") \
            if _STAGE_MODELS_PATH.exists() else "?"
    except Exception:
        current_model = "?"
    print(f"  Модель этапа block_batch: {current_model}")
    print(f"  Лимиты пакета: max_blocks={max_blocks}, max_size_kb={max_size_kb}, solo_kb={solo_kb}, min_blocks={min_blocks}")

    is_claude_stage = isinstance(current_model, str) and current_model.startswith("claude-")

    # Compact-режим:
    # — для НЕ-Claude моделей разрешаем класть больше мелких блоков в batch;
    # — для Claude hard cap сохраняется независимо от compact (production-safe, attention risk).
    is_compact = index_data.get("compact", False)
    if is_compact:
        if is_claude_stage:
            # Claude batch НЕ раздувается до 30: привязываемся к CLAUDE_HARD_CAP.
            if max_blocks > CLAUDE_HARD_CAP:
                print(f"  [COMPACT/Claude] max_blocks clamp {max_blocks} -> {CLAUDE_HARD_CAP}")
                max_blocks = CLAUDE_HARD_CAP
            else:
                print(f"  [COMPACT/Claude] hard cap {CLAUDE_HARD_CAP} сохранён (без раздувания)")
        elif max_blocks <= 15:
            max_blocks = max(max_blocks, 30)
            print(f"  [COMPACT] Авто-увеличение max_blocks -> {max_blocks}")

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

    # Укрупнение: страницы с множеством мелких блоков → полностраничное изображение
    original_count = len(blocks)
    pages_map = _consolidate_small_blocks(
        pages_map,
        project_dir=Path(project_dir),
        blocks_dir=output_dir / "blocks",
    )
    new_count = sum(len(v) for v in pages_map.values())
    if new_count < original_count:
        print(f"  Укрупнение: {original_count} -> {new_count} блоков")

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

        # Определяем плотные страницы (≥DENSE_PAGE_THRESHOLD блоков).
        # В solo-режиме граница не нужна — всё равно 1 блок = 1 пакет.
        dense_pages: set = set()
        if not solo:
            dense_pages = {p for p, blks in pages_map.items() if len(blks) >= DENSE_PAGE_THRESHOLD}
            if dense_pages:
                print(f"  Плотные страницы (≥{DENSE_PAGE_THRESHOLD} блоков): {sorted(dense_pages)} — не мешаются с другими")

        if is_claude_stage and not solo:
            # Claude: risk-aware packing (heavy≈5, normal≈8, light≈10, hard cap 12).
            # ENV overrides для экспериментов: CLAUDE_BATCH_{HEAVY,NORMAL,LIGHT}_{TARGET,MAX}.
            claude_cap = min(max_blocks, CLAUDE_HARD_CAP)
            risk_targets = read_claude_risk_overrides(hard_cap=claude_cap)
            if risk_targets != _CLAUDE_RISK_TARGETS:
                print(f"  [OVERRIDE] Claude risk targets из ENV: {risk_targets}")
            packed = _pack_blocks_claude_risk_aware(
                ordered_blocks,
                max_size_kb=max_size_kb,
                solo_threshold_kb=solo_kb,
                dense_pages=dense_pages,
                hard_cap=claude_cap,
                risk_targets=risk_targets,
            )
            strategy = "claude_risk_aware"
        else:
            packed = _pack_blocks_adaptive(
                ordered_blocks,
                max_size_kb=max_size_kb,
                max_blocks=max_blocks,
                min_blocks=min_blocks,
                solo_threshold_kb=solo_kb,
                dense_pages=dense_pages,
            )
            strategy = "adaptive"

        for chunk in packed:
            batch_id += 1
            batches.append(_make_batch_entry(batch_id, chunk))

        total_size_kb = sum(b.get("size_kb", 0) for b in blocks)
        if strategy == "claude_risk_aware":
            print(f"  Стратегия: claude_risk_aware (hard cap {min(max_blocks, CLAUDE_HARD_CAP)}, heavy≈5 / normal≈8 / light≈10)")
        else:
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
            "min_blocks": min_blocks,
            "solo_threshold_kb": solo_kb,
            "model_profile": current_model,
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

def _backfill_locality_from_graph(output_dir: Path, block_analyses: list[dict]):
    """Backfill locality полей в block_analyses из document_graph v2.

    Если LLM заполнила selected_text_block_ids — оставляем как есть.
    Если нет — заполняем из local_text_links графа.
    Всегда гарантирует наличие полей (пустые списки если данных нет).
    """
    graph_path = output_dir / "document_graph.json"
    if not graph_path.exists():
        # Гарантируем поля даже без графа
        for ba in block_analyses:
            ba.setdefault("selected_text_block_ids", [])
            ba.setdefault("evidence_text_refs", [])
        return

    try:
        graph = json.loads(graph_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        for ba in block_analyses:
            ba.setdefault("selected_text_block_ids", [])
            ba.setdefault("evidence_text_refs", [])
        return

    if graph.get("version", 1) < 2:
        for ba in block_analyses:
            ba.setdefault("selected_text_block_ids", [])
            ba.setdefault("evidence_text_refs", [])
        return

    # Индекс: block_id → page local_text_links
    block_locality: dict[str, list[dict]] = {}
    for pg in graph.get("pages", []):
        local_links = pg.get("local_text_links", {})
        for img_id, candidates in local_links.items():
            block_locality[img_id] = candidates

    backfilled = 0
    for ba in block_analyses:
        bid = ba.get("block_id", "")

        # Гарантируем поля
        ba.setdefault("selected_text_block_ids", [])
        ba.setdefault("evidence_text_refs", [])

        # Backfill только если LLM не заполнила
        if not ba["selected_text_block_ids"] and bid in block_locality:
            candidates = block_locality[bid]
            ba["selected_text_block_ids"] = [c["text_block_id"] for c in candidates]
            # Создаём базовые evidence_text_refs из locality
            if not ba["evidence_text_refs"]:
                ba["evidence_text_refs"] = [
                    {
                        "text_block_id": c["text_block_id"],
                        "role": "other",
                        "used_for": "cross_check",
                        "confidence": round(c.get("score", 0.5), 2),
                        "source": "graph_backfill",
                    }
                    for c in candidates
                    if c.get("score", 0) > 0.1
                ]
            backfilled += 1

    if backfilled:
        print(f"  [LOCALITY] Backfill: {backfilled} блоков обогащены из document_graph v2")


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

            # Замечания — только из block_analyses[].findings (единственный источник).
            for ba in analyses:
                for f in ba.get("findings", []):
                    if "source" not in f and "block_evidence" not in f:
                        f["block_evidence"] = ba.get("block_id", "")
                    _normalize_finding_block_ids(f)
                    all_findings.append(f)

            batch_findings_count = sum(len(ba.get("findings", [])) for ba in analyses)
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

    # Backfill locality полей из document_graph v2
    # Если LLM не заполнила selected_text_block_ids — заполняем из graph
    _backfill_locality_from_graph(output_dir, all_block_analyses)

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
# PROMOTE — подмена compact→full для нечитаемых блоков (без повторного скачивания)
# ═══════════════════════════════════════════════════════════════════════════════


def promote_to_full(project_dir: str, block_ids: list[str]) -> dict:
    """Подменить compact-версии блоков на full-версии (уже скачанные).

    Для каждого block_id: переименовывает block_<ID>_full.png → block_<ID>.png
    и обновляет index.json (size_kb, compact=False).

    Возвращает {"promoted": N, "missing": N, "block_ids_promoted": [...]}.
    """
    output_dir = Path(project_dir) / "_output" / "blocks"
    index_path = output_dir / "index.json"

    if not index_path.exists():
        return {"error": "index.json не найден"}

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    blocks_by_id = {b["block_id"]: b for b in index_data.get("blocks", [])}

    promoted = 0
    missing = 0
    promoted_ids = []

    for bid in block_ids:
        compact_file = output_dir / f"block_{bid}.png"
        full_file = output_dir / f"block_{bid}_full.png"

        if not full_file.exists():
            print(f"  [SKIP] {bid}: full-версия не найдена")
            missing += 1
            continue

        # Заменяем compact на full
        if compact_file.exists():
            compact_file.unlink()
        full_file.rename(compact_file)

        # Обновляем index
        if bid in blocks_by_id:
            entry = blocks_by_id[bid]
            entry["size_kb"] = round(compact_file.stat().st_size / 1024, 1)
            entry.pop("file_full", None)
            entry.pop("size_kb_full", None)
            entry["compact"] = False
            entry["promoted_to_full"] = True

        promoted += 1
        promoted_ids.append(bid)
        print(f"  [PROMOTE] {bid}: compact -> full ({blocks_by_id.get(bid, {}).get('size_kb', '?')} KB)")

    # Сохраняем обновлённый index
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    print(f"  Итого: {promoted} промоутнуто, {missing} без full-версии")
    return {
        "promoted": promoted,
        "missing": missing,
        "block_ids_promoted": promoted_ids,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# RECROP — перекачка нечитаемых блоков с увеличенным разрешением (×2 итеративно)
# ═══════════════════════════════════════════════════════════════════════════════

MAX_RECROP_SCALE = 8.0  # Максимальный масштаб (предел PyMuPDF)
MAX_RECROP_ITERATIONS = 3  # Макс итераций (1500→3000→6000→стоп)


def find_unreadable_blocks(project_dir: str) -> list[dict]:
    """Найти блоки с unreadable_text=true в batch-файлах или 02_blocks_analysis.json."""
    output_dir = Path(project_dir) / "_output"
    unreadable = []

    # Сначала проверить 02_blocks_analysis.json (результат merge)
    merged = output_dir / "02_blocks_analysis.json"
    if merged.exists():
        try:
            with open(merged, "r", encoding="utf-8") as f:
                data = json.load(f)
            for ba in data.get("block_analyses") or data.get("blocks") or []:
                if ba.get("unreadable_text"):
                    unreadable.append({
                        "block_id": ba["block_id"],
                        "page": ba.get("page"),
                        "details": ba.get("unreadable_details", ""),
                    })
        except Exception:
            pass
        return unreadable

    # Fallback: сканировать batch-файлы
    for bf in sorted(output_dir.glob("block_batch_*.json")):
        try:
            with open(bf, "r", encoding="utf-8") as f:
                data = json.load(f)
            for ba in data.get("block_analyses") or []:
                if ba.get("unreadable_text"):
                    unreadable.append({
                        "block_id": ba["block_id"],
                        "page": ba.get("page"),
                        "details": ba.get("unreadable_details", ""),
                    })
        except Exception:
            continue

    return unreadable


def recrop_blocks(
    project_dir: str,
    block_ids: list[str],
    scale_multiplier: float = 2.0,
) -> dict:
    """Перекачать указанные блоки с увеличенным разрешением.

    Берёт текущее разрешение блока из index.json и умножает на scale_multiplier.
    Ограничен MAX_RECROP_SCALE (8×).
    """
    project_path = Path(project_dir)
    output_dir = project_path / "_output" / "blocks"
    index_path = output_dir / "index.json"

    if not index_path.exists():
        return {"error": "index.json не найден — сначала выполните crop"}

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    # Построить маппинг block_id → index entry
    blocks_by_id = {b["block_id"]: b for b in index_data.get("blocks", [])}

    # Загрузить result.json для crop_url
    result_json_paths = detect_all_result_jsons(project_dir)
    ocr_blocks: dict[str, dict] = {}
    all_page_dimensions: dict[int, tuple[int, int]] = {}
    page_pdf_map: dict[int, Path] = {}

    info = {}
    info_path = project_path / "project_info.json"
    if info_path.exists():
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                info = json.load(f)
        except Exception:
            pass
    pdf_files = info.get("pdf_files", [])
    if not pdf_files:
        pf = info.get("pdf_file", "")
        pdf_files = [pf] if pf else []

    for rj_path in result_json_paths:
        with open(rj_path, "r", encoding="utf-8") as f:
            ocr_data = json.load(f)

        rj_stem = rj_path.stem.replace("_result", "")
        pdf_path: Path | None = None
        for pf in pdf_files:
            if Path(pf).stem == rj_stem:
                candidate = project_path / pf
                if candidate.exists():
                    pdf_path = candidate
                    break
        if not pdf_path:
            pdfs = list(project_path.glob("*.pdf"))
            if pdfs:
                pdf_path = pdfs[0]

        for pg in ocr_data.get("pages", []):
            pn = pg.get("page_number", 0)
            pw, ph = pg.get("width", 0), pg.get("height", 0)
            if pw and ph:
                all_page_dimensions[pn] = (pw, ph)
            if pdf_path:
                page_pdf_map[pn] = pdf_path
            for block in pg.get("blocks", []):
                if block.get("block_type") == "image":
                    ocr_blocks[block.get("id", "")] = block

    recropped = 0
    errors = 0

    for bid in block_ids:
        idx_entry = blocks_by_id.get(bid)
        if not idx_entry:
            print(f"  [SKIP] {bid}: не найден в index.json")
            continue

        # Определить текущее разрешение
        render_size = idx_entry.get("render_size", [TARGET_LONG_SIDE_PX, TARGET_LONG_SIDE_PX])
        current_long_side = max(render_size) if render_size else TARGET_LONG_SIDE_PX
        new_target_px = int(current_long_side * scale_multiplier)

        # Ограничение: нет смысла превышать max scale
        # render_scale = target_px / long_side_pt, max 8.0
        # Практический потолок зависит от размера блока в PDF, но 6000px — разумный лимит
        new_target_px = min(new_target_px, 6000)

        if new_target_px <= current_long_side:
            print(f"  [SKIP] {bid}: уже на максимальном разрешении ({current_long_side}px)")
            continue

        print(f"  [RECROP] {bid}: {current_long_side}px -> {new_target_px}px")

        out_file = output_dir / f"block_{bid}.png"
        ocr_block = ocr_blocks.get(bid, {})
        crop_url = ocr_block.get("crop_url", "")
        page_num = idx_entry.get("page", 0)

        success = False
        if crop_url:
            try:
                w, h = download_and_convert(crop_url, out_file, target_px=new_target_px)
                success = True
                source = "cloud"
            except Exception as e:
                print(f"  [WARN] {bid}: облако ({e}), пробую PDF fallback")

        if not success:
            dims = all_page_dimensions.get(page_num)
            fallback_pdf = page_pdf_map.get(page_num)
            coords = idx_entry.get("crop_px", ocr_block.get("coords_px", [0, 0, 0, 0]))
            if fallback_pdf and dims and coords:
                try:
                    w, h = crop_from_pdf(
                        fallback_pdf, page_num,
                        coords, dims[0], dims[1],
                        out_file, target_px=new_target_px,
                    )
                    success = True
                    source = "pdf_fallback"
                except Exception as e2:
                    print(f"  [ERROR] {bid}: PDF fallback ({e2})")

        if not success:
            errors += 1
            continue

        size_kb = out_file.stat().st_size / 1024
        print(f"  [OK] {bid}: {w}x{h}px, {size_kb:.0f} KB (source: {source})")

        # Обновить index entry
        idx_entry["render_size"] = [w, h]
        idx_entry["size_kb"] = round(size_kb, 1)
        idx_entry["source"] = source
        idx_entry["recrop_target_px"] = new_target_px
        idx_entry["recrop_iteration"] = idx_entry.get("recrop_iteration", 0) + 1
        recropped += 1

    # Сохранить обновлённый index.json
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    print(f"\n  Recrop: {recropped} блоков перекачано, {errors} ошибок")
    return {
        "recropped": recropped,
        "errors": errors,
        "block_ids": block_ids,
    }


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
    p_crop.add_argument("--compact", action="store_true",
                         help="Compact-режим: 800px для батчей + full-версия для retry нечитаемых")

    # batches
    p_batch = subparsers.add_parser("batches", help="Сгенерировать пакеты блоков")
    p_batch.add_argument("project_dir", help="Путь к папке проекта")
    p_batch.add_argument("--block-ids", help="Список block_id через запятую")
    p_batch.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                         help=f"Максимум блоков в пакете (по умолчанию {DEFAULT_BATCH_SIZE})")
    p_batch.add_argument("--no-adaptive", action="store_true",
                         help="Использовать старую стратегию (фиксированный batch_size)")
    p_batch.add_argument("--max-size-mb", type=float, default=None,
                         help="Целевой объём пакета в МБ (по умолчанию — per-model из stage_models.json)")
    p_batch.add_argument("--max-blocks", type=int, default=None,
                         help="Макс блоков в пакете (по умолчанию — per-model из stage_models.json)")
    p_batch.add_argument("--solo", action="store_true",
                         help="По 1 блоку в пакете (для retry нечитаемых — фокус модели)")

    # merge
    p_merge = subparsers.add_parser("merge", help="Слить block_batch_*.json в 02_blocks_analysis.json")
    p_merge.add_argument("project_dir", help="Путь к папке проекта")
    p_merge.add_argument("--cleanup", action="store_true",
                         help="Удалить промежуточные файлы после слияния")

    # recrop
    p_recrop = subparsers.add_parser("recrop", help="Перекачать нечитаемые блоки в повышенном разрешении")
    p_recrop.add_argument("project_dir", help="Путь к папке проекта")
    p_recrop.add_argument("--block-ids", help="Список block_id через запятую (иначе — авто из unreadable_text)")
    p_recrop.add_argument("--scale", type=float, default=2.0,
                          help="Множитель разрешения (по умолчанию 2.0)")

    # promote
    p_promote = subparsers.add_parser("promote",
        help="Подменить compact→full для нечитаемых блоков (без повторного скачивания)")
    p_promote.add_argument("project_dir", help="Путь к папке проекта")
    p_promote.add_argument("--block-ids", help="Список block_id через запятую (иначе — авто из unreadable_text)")

    args = parser.parse_args()

    if not os.path.isdir(args.project_dir):
        print(f"[ERROR] Папка не найдена: {args.project_dir}")
        sys.exit(1)

    if args.command == "crop":
        block_ids = [b.strip() for b in args.block_ids.split(",")] if args.block_ids else None
        result = crop_blocks(args.project_dir, block_ids=block_ids, force=args.force,
                             compact=getattr(args, "compact", False))
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
        max_mb = getattr(args, "max_size_mb", None)
        result = generate_block_batches(
            args.project_dir,
            block_ids=block_ids,
            batch_size=args.batch_size,
            adaptive=use_adaptive,
            max_size_kb=int(max_mb * 1024) if max_mb is not None else None,
            max_blocks=getattr(args, "max_blocks", None),
            solo=getattr(args, "solo", False),
        )
        if result.get("error"):
            sys.exit(1)

    elif args.command == "merge":
        result = merge_block_results(args.project_dir, cleanup=args.cleanup)
        if result.get("error"):
            sys.exit(1)

    elif args.command == "recrop":
        if args.block_ids:
            block_ids = [b.strip() for b in args.block_ids.split(",")]
        else:
            # Авто-обнаружение из unreadable_text
            unreadable = find_unreadable_blocks(args.project_dir)
            if not unreadable:
                print("Нет блоков с unreadable_text=true")
                sys.exit(0)
            block_ids = [u["block_id"] for u in unreadable]
            print(f"Найдено {len(block_ids)} нечитаемых блоков:")
            for u in unreadable:
                print(f"  {u['block_id']}: {u.get('details', '')[:80]}")

        result = recrop_blocks(args.project_dir, block_ids, scale_multiplier=args.scale)
        if result.get("error"):
            sys.exit(1)

    elif args.command == "promote":
        if args.block_ids:
            block_ids = [b.strip() for b in args.block_ids.split(",")]
        else:
            # Авто-обнаружение из unreadable_text
            unreadable = find_unreadable_blocks(args.project_dir)
            if not unreadable:
                print("Нет блоков с unreadable_text=true")
                sys.exit(0)
            block_ids = [u["block_id"] for u in unreadable]
            print(f"Найдено {len(block_ids)} нечитаемых блоков:")
            for u in unreadable:
                print(f"  {u['block_id']}: {u.get('details', '')[:80]}")

        result = promote_to_full(args.project_dir, block_ids)
        if result.get("error"):
            sys.exit(1)
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
