"""
graph_builder.py
----------------
Построение Document Knowledge Graph v2 из канонических JSON-источников
(*_result.json, blocks/index.json) вместо парсинга markdown.

Markdown остаётся fallback/debug-источником.

Использование:
    from graph_builder import build_document_graph_v2

    graph = build_document_graph_v2(project_dir)
    # graph["version"] == 2
"""

import json
import math
import os
import re
from pathlib import Path
from typing import Optional


# ─── Нормализация OCR-текста ───────────────────────────────────────────────

def _normalize_ocr_text(raw: str | None) -> str:
    """Извлечь читаемый текст из OCR-выхода (HTML или plain text).

    Chandra OCR возвращает:
    - Для text-блоков: HTML (<div>, <p>, <table> и т.д.)
    - Для image-блоков: plain text (описание чертежа)
    """
    if not raw:
        return ""

    text = raw

    # Убираем HTML-теги, сохраняя форматирование
    if "<" in text and ">" in text:
        # Сохраняем жирный текст как markdown **bold**
        text = re.sub(r'<(?:b|strong)(?:\s[^>]*)?>', '**', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:b|strong)>', '**', text, flags=re.IGNORECASE)
        # Заменяем <br>, <br/>, </p>, </div>, </tr> на переводы строк
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:p|div|tr|li|h[1-6])>', '\n', text, flags=re.IGNORECASE)
        # Заменяем </td> на табуляцию (сохраняем табличную структуру)
        text = re.sub(r'</td>', '\t', text, flags=re.IGNORECASE)
        # Убираем все оставшиеся теги
        text = re.sub(r'<[^>]+>', '', text)

    # Декодируем HTML-сущности
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&nbsp;", " ")
    text = text.replace("&#39;", "'")

    # Убираем множественные пустые строки
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


# ─── Вычисление sheet_no из stamp_data ─────────────────────────────────────

def _extract_sheet_info(blocks: list[dict]) -> tuple[str | None, str | None, str]:
    """Извлечь sheet_no и sheet_name из stamp_data блоков страницы.

    Returns:
        (sheet_no_raw, sheet_name, confidence)
        confidence: "high" | "medium" | "low" | "missing"
    """
    for block in blocks:
        stamp = block.get("stamp_data")
        if not stamp:
            continue

        sheet_no = stamp.get("sheet_number")
        total = stamp.get("total_sheets")
        sheet_name = stamp.get("sheet_name")

        if sheet_no:
            raw = sheet_no
            if total:
                raw = f"{sheet_no} (из {total})"
            confidence = "high"
            return raw, sheet_name, confidence

    return None, None, "missing"


def _normalize_sheet_no(raw: str | None) -> str | None:
    """Нормализовать номер листа: '1 (из 22)' → '1'."""
    if not raw:
        return None
    # Извлекаем первое число
    m = re.match(r'(\d+)', str(raw).strip())
    return m.group(1) if m else raw


# ─── Locality scoring thresholds (configurable) ───────────────────────────

# Кандидат с distance_norm выше этого порога НЕ считается "good local"
# и НЕ может отключать PAGE GLOBAL CONTEXT fallback
LOCALITY_FAR_DISTANCE_THRESHOLD = 0.25

# Минимальный score для кандидата, чтобы считаться "good local context"
LOCALITY_GOOD_SCORE_THRESHOLD = 0.15


# ─── Geometry-based text ↔ image binding ───────────────────────────────────

def _rect_center(coords: list[float]) -> tuple[float, float]:
    """Центр прямоугольника [x1, y1, x2, y2]."""
    return ((coords[0] + coords[2]) / 2, (coords[1] + coords[3]) / 2)


def _rect_area(coords: list[float]) -> float:
    """Площадь прямоугольника."""
    w = max(0, coords[2] - coords[0])
    h = max(0, coords[3] - coords[1])
    return w * h


def _rect_intersection(a: list[float], b: list[float]) -> float:
    """Площадь пересечения двух прямоугольников."""
    x1 = max(a[0], b[0])
    y1 = max(a[1], b[1])
    x2 = min(a[2], b[2])
    y2 = min(a[3], b[3])
    if x2 <= x1 or y2 <= y1:
        return 0.0
    return (x2 - x1) * (y2 - y1)


def _horizontal_overlap(a: list[float], b: list[float]) -> float:
    """Горизонтальное перекрытие (0..1 от меньшего)."""
    overlap = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    min_width = min(a[2] - a[0], b[2] - b[0])
    return overlap / min_width if min_width > 0 else 0.0


def _vertical_overlap(a: list[float], b: list[float]) -> float:
    """Вертикальное перекрытие (0..1 от меньшего)."""
    overlap = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    min_height = min(a[3] - a[1], b[3] - b[1])
    return overlap / min_height if min_height > 0 else 0.0


def _compute_locality_score(
    image_coords: list[float],
    text_coords: list[float],
    page_diag: float,
) -> dict:
    """Вычислить score близости text-блока к image-блоку.

    Returns:
        dict с полями: score, distance, h_overlap, v_overlap,
                       containment, position_bonus, reason
    """
    if not image_coords or not text_coords or len(image_coords) < 4 or len(text_coords) < 4:
        return {"score": 0.0, "reason": "no_coords"}

    ic = _rect_center(image_coords)
    tc = _rect_center(text_coords)

    # Расстояние между центрами (нормализовано по диагонали страницы)
    dist = math.sqrt((ic[0] - tc[0]) ** 2 + (ic[1] - tc[1]) ** 2)
    norm_dist = dist / page_diag if page_diag > 0 else 1.0

    # Перекрытия
    h_overlap = _horizontal_overlap(image_coords, text_coords)
    v_overlap = _vertical_overlap(image_coords, text_coords)

    # Containment (text внутри image или наоборот)
    intersection = _rect_intersection(image_coords, text_coords)
    text_area = _rect_area(text_coords)
    containment = intersection / text_area if text_area > 0 else 0.0

    # Позиционный бонус: text справа или снизу от image (примечание/легенда)
    position_bonus = 0.0
    reason_parts = []

    # Текст справа от изображения (в пределах 15% ширины страницы)
    if tc[0] > image_coords[2] and (tc[0] - image_coords[2]) / page_diag < 0.15:
        if v_overlap > 0.3:
            position_bonus += 0.2
            reason_parts.append("text_right")

    # Текст снизу от изображения (в пределах 10% высоты)
    if tc[1] > image_coords[3] and (tc[1] - image_coords[3]) / page_diag < 0.10:
        if h_overlap > 0.3:
            position_bonus += 0.15
            reason_parts.append("text_below")

    # Текст сверху (заголовок)
    if tc[1] < image_coords[1] and (image_coords[1] - tc[1]) / page_diag < 0.08:
        if h_overlap > 0.3:
            position_bonus += 0.1
            reason_parts.append("text_above_title")

    # Penalty за удалённость
    distance_penalty = 0.0
    if norm_dist > 0.3:
        distance_penalty = min(0.5, (norm_dist - 0.3) * 2)
        reason_parts.append("far")

    # Итоговый score
    score = (
        (1.0 - norm_dist) * 0.3      # близость
        + h_overlap * 0.15            # горизонтальное перекрытие
        + v_overlap * 0.15            # вертикальное перекрытие
        + containment * 0.2           # вложенность
        + position_bonus              # позиционный бонус
        - distance_penalty            # штраф за удалённость
    )
    score = max(0.0, min(1.0, score))

    if not reason_parts:
        if containment > 0.5:
            reason_parts.append("contained")
        elif norm_dist < 0.15:
            reason_parts.append("nearby")
        else:
            reason_parts.append("proximity")

    return {
        "score": round(score, 3),
        "distance_norm": round(norm_dist, 3),
        "h_overlap": round(h_overlap, 3),
        "v_overlap": round(v_overlap, 3),
        "containment": round(containment, 3),
        "position_bonus": round(position_bonus, 3),
        "reason": "+".join(reason_parts) if reason_parts else "proximity",
    }


def is_good_local_candidate(candidate: dict) -> bool:
    """Проверить, является ли кандидат достаточно качественным local context.

    Кандидат НЕ считается хорошим если:
    - distance_norm > LOCALITY_FAR_DISTANCE_THRESHOLD (слишком далеко)
    - score < LOCALITY_GOOD_SCORE_THRESHOLD (слишком слабый)
    - reason содержит "far" (помечен как далёкий)
    """
    if candidate.get("score", 0) < LOCALITY_GOOD_SCORE_THRESHOLD:
        return False
    if candidate.get("distance_norm", 1.0) > LOCALITY_FAR_DISTANCE_THRESHOLD:
        return False
    reason = candidate.get("reason", "")
    if "far" in reason:
        return False
    return True


def build_local_text_links(
    page: dict,
    top_k: int = 5,
    min_score: float = 0.05,
) -> dict[str, list[dict]]:
    """Построить read-only geometry-based binding между image и text блоками.

    Args:
        page: страница из document_graph v2 (с coords_norm)
        top_k: максимум text-кандидатов на image-блок
        min_score: минимальный score для включения

    Returns:
        {image_block_id: [{"text_block_id": ..., "score": ..., "reason": ...}, ...]}
    """
    text_blocks = page.get("text_blocks", [])
    image_blocks = page.get("image_blocks", [])

    if not text_blocks or not image_blocks:
        return {}

    # Диагональ страницы (по нормализованным координатам, т.е. ~1.41)
    page_diag = math.sqrt(2)  # для coords_norm: [0..1, 0..1]

    result: dict[str, list[dict]] = {}

    for img in image_blocks:
        img_coords = img.get("coords_norm")
        if not img_coords:
            continue

        candidates = []
        for tb in text_blocks:
            tb_coords = tb.get("coords_norm")
            if not tb_coords:
                continue

            score_info = _compute_locality_score(img_coords, tb_coords, page_diag)
            if score_info["score"] >= min_score:
                candidates.append({
                    "text_block_id": tb["id"],
                    "score": score_info["score"],
                    "reason": score_info["reason"],
                    "distance_norm": score_info["distance_norm"],
                })

        # Сортируем по score desc, берём top-K
        candidates.sort(key=lambda x: x["score"], reverse=True)
        result[img["id"]] = candidates[:top_k]

    return result


# ─── Поиск canonical JSON файлов ──────────────────────────────────────────

def _find_result_json(project_dir: str | Path) -> list[Path]:
    """Найти *_result.json файлы в папке проекта."""
    project_dir = Path(project_dir)
    candidates = sorted(project_dir.glob("*_result.json"))
    # Исключаем файлы в _output/
    return [c for c in candidates if "_output" not in str(c)]


def _find_annotation_json(project_dir: str | Path) -> list[Path]:
    """Найти *_annotation.json файлы в папке проекта."""
    project_dir = Path(project_dir)
    candidates = sorted(project_dir.glob("*_annotation.json"))
    return [c for c in candidates if "_output" not in str(c)]


# ─── Основной builder ─────────────────────────────────────────────────────

def build_document_graph_v2(
    project_dir: str | Path,
    output_dir: str | Path | None = None,
    include_locality: bool = True,
) -> dict | None:
    """Построить Document Knowledge Graph v2 из канонических JSON.

    Приоритет источников:
        1. *_result.json (primary) — coords, OCR, stamp_data
        2. blocks/index.json (enrichment) — file, size_kb, crop_px
        3. *_document.md (fallback) — если result.json отсутствует

    Args:
        project_dir: путь к папке проекта
        output_dir: куда сохранить (default: project_dir/_output)
        include_locality: вычислять ли text↔image binding

    Returns:
        dict: document_graph v2 или None при ошибке
    """
    project_dir = Path(project_dir)
    if output_dir is None:
        output_dir = project_dir / "_output"
    output_dir = Path(output_dir)

    # Ищем canonical JSON
    result_jsons = _find_result_json(project_dir)

    if not result_jsons:
        print(f"  [GRAPH v2] *_result.json не найден в {project_dir}")
        print(f"  [GRAPH v2] Fallback на MD-парсинг (v1)")
        return None  # caller должен использовать build_document_graph (v1)

    # Загружаем все result.json (может быть несколько для multi-PDF проектов)
    all_pages: list[dict] = []
    document_id = ""

    for rj_path in result_jsons:
        try:
            rj_data = json.loads(rj_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            print(f"  [GRAPH v2] Ошибка чтения {rj_path.name}: {e}")
            continue

        if not document_id:
            document_id = rj_path.stem.replace("_result", "")

        for rj_page in rj_data.get("pages", []):
            # page_number в result.json — 1-based (human-readable)
            page_number = rj_page.get("page_number", rj_page.get("page_index", 0))
            page_width = rj_page.get("width", 0)
            page_height = rj_page.get("height", 0)
            blocks = rj_page.get("blocks", [])

            # Извлекаем sheet info из stamp_data
            sheet_no_raw, sheet_name, sheet_confidence = _extract_sheet_info(blocks)
            sheet_no_normalized = _normalize_sheet_no(sheet_no_raw)

            text_blocks = []
            image_blocks = []

            for block in blocks:
                bid = block.get("id", "")
                btype = block.get("block_type", "")
                coords_px = block.get("coords_px", [])
                coords_norm = block.get("coords_norm", [])
                source = block.get("source", "")
                ocr_raw = block.get("ocr_text", "")

                # Пропускаем блоки без ID
                if not bid:
                    continue

                if btype == "text":
                    text_blocks.append({
                        "id": bid,
                        "text": _normalize_ocr_text(ocr_raw),
                        "coords_px": coords_px,
                        "coords_norm": coords_norm,
                        "source": source,
                        "page": page_number,  # 1-based (unified)
                    })
                elif btype == "image":
                    image_blocks.append({
                        "id": bid,
                        "type": _extract_image_type(ocr_raw),
                        "ocr_raw": ocr_raw,
                        "ocr_text_normalized": _normalize_ocr_text(ocr_raw),
                        "coords_px": coords_px,
                        "coords_norm": coords_norm,
                        "source": source,
                        "page": page_number,  # 1-based (unified)
                        # Обогащаются из blocks/index.json:
                        "file": None,
                        "size_kb": None,
                    })

            page_entry = {
                "page": page_number,               # 1-based (human-readable)
                "page_index": page_number - 1,      # 0-based (internal, for PyMuPDF etc.)
                "sheet_no_raw": sheet_no_raw,
                "sheet_no_normalized": sheet_no_normalized,
                "sheet_name": sheet_name,
                "sheet_confidence": sheet_confidence,
                "page_width": page_width,
                "page_height": page_height,
                "text_blocks": text_blocks,
                "image_blocks": image_blocks,
            }

            # Вычисляем locality binding
            if include_locality and text_blocks and image_blocks:
                locality = build_local_text_links(page_entry)
                page_entry["local_text_links"] = locality

            all_pages.append(page_entry)

    if not all_pages:
        print(f"  [GRAPH v2] Ни одной страницы не извлечено из result.json")
        return None

    # Сортируем по page number
    all_pages.sort(key=lambda p: p["page"])

    # Проверка на дублированные page numbers
    seen_pages = {}
    for pg in all_pages:
        pn = pg["page"]
        if pn in seen_pages:
            print(f"  [GRAPH v2] WARNING: дублированный page number {pn}")
        seen_pages[pn] = True

    # Обогащаем из blocks/index.json
    index_path = output_dir / "blocks" / "index.json"
    blocks_enriched = 0
    if index_path.exists():
        try:
            index_data = json.loads(index_path.read_text(encoding="utf-8"))
            index_map = {b["block_id"]: b for b in index_data.get("blocks", []) if b.get("block_id")}

            for pg in all_pages:
                for img in pg.get("image_blocks", []):
                    idx_entry = index_map.get(img["id"])
                    if idx_entry:
                        img["file"] = idx_entry.get("file")
                        img["size_kb"] = idx_entry.get("size_kb")
                        blocks_enriched += 1
        except (json.JSONDecodeError, OSError):
            pass

    graph = {
        "version": 2,
        "document_id": document_id,
        "source": "result_json",
        "source_files": [rj.name for rj in result_jsons],
        "total_pages": len(all_pages),
        "total_text_blocks": sum(len(p["text_blocks"]) for p in all_pages),
        "total_image_blocks": sum(len(p["image_blocks"]) for p in all_pages),
        "blocks_enriched": blocks_enriched,
        "pages": all_pages,
    }

    # Сохраняем
    output_dir.mkdir(parents=True, exist_ok=True)
    graph_path = output_dir / "document_graph.json"
    graph_path.write_text(
        json.dumps(graph, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"  [GRAPH v2] document_graph.json: {len(all_pages)} страниц, "
          f"{graph['total_text_blocks']} текст., {graph['total_image_blocks']} граф., "
          f"{blocks_enriched} обогащено")

    return graph


def _extract_image_type(ocr_text: str | None) -> str | None:
    """Извлечь тип изображения из OCR-описания."""
    if not ocr_text:
        return None

    # Паттерн из Chandra: **[ИЗОБРАЖЕНИЕ]** | Тип: План этажа | Оси: ...
    m = re.search(r'Тип:\s*([^|\n]+)', ocr_text)
    if m:
        return m.group(1).strip()

    # Fallback: первые 50 символов как label
    clean = _normalize_ocr_text(ocr_text)
    if clean:
        return clean[:50].strip()

    return None


# ─── Debug output ──────────────────────────────────────────────────────────

def generate_locality_debug(graph: dict, output_dir: str | Path) -> Path | None:
    """Генерировать step1_locality_debug.json для отладки.

    Для каждой страницы выводит:
    - sheet info
    - image blocks с selected text candidates
    - page_global_text_used flag
    """
    output_dir = Path(output_dir)

    if graph.get("version", 1) < 2:
        return None

    debug_pages = []

    for pg in graph.get("pages", []):
        image_blocks = pg.get("image_blocks", [])
        if not image_blocks:
            continue

        local_links = pg.get("local_text_links", {})

        for img in image_blocks:
            candidates = local_links.get(img["id"], [])
            has_good_local = any(is_good_local_candidate(c) for c in candidates)

            debug_pages.append({
                "page": pg["page"],
                "sheet_no_raw": pg.get("sheet_no_raw"),
                "sheet_no_normalized": pg.get("sheet_no_normalized"),
                "sheet_confidence": pg.get("sheet_confidence"),
                "image_block_id": img["id"],
                "image_coords_norm": img.get("coords_norm"),
                "selected_text_block_ids": [c["text_block_id"] for c in candidates],
                "good_local_candidates": [
                    c["text_block_id"] for c in candidates
                    if is_good_local_candidate(c)
                ],
                "local_text_candidates": candidates,
                "page_global_text_used": not has_good_local,
            })

    debug_path = output_dir / "step1_locality_debug.json"
    debug_path.write_text(
        json.dumps(debug_pages, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return debug_path


# ─── Block ID normalization ────────────────────────────────────────────────

def normalize_block_id(raw: str | None) -> str:
    """Нормализовать block identity в canonical bare-id формат.

    Преобразования:
        "block_IMG-001.png"  → "IMG-001"
        "block_IMG-001"      → "IMG-001"
        "IMG-001.png"        → "IMG-001"
        "IMG-001"            → "IMG-001"
        ""                   → ""
        None                 → ""
    """
    if not raw:
        return ""
    s = raw.strip()
    # strip "block_" prefix
    if s.startswith("block_"):
        s = s[6:]
    # strip file extensions
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        if s.lower().endswith(ext):
            s = s[:-len(ext)]
            break
    return s


def normalize_block_ids_in_finding(finding: dict) -> dict:
    """Нормализовать все block-identity поля в finding (in-place).

    Поля: block_evidence, related_block_ids, evidence[].block_id
    """
    # block_evidence
    if "block_evidence" in finding:
        finding["block_evidence"] = normalize_block_id(finding["block_evidence"])

    # related_block_ids
    if "related_block_ids" in finding and isinstance(finding["related_block_ids"], list):
        finding["related_block_ids"] = [
            normalize_block_id(bid) for bid in finding["related_block_ids"]
            if normalize_block_id(bid)
        ]

    # evidence[].block_id
    if "evidence" in finding and isinstance(finding["evidence"], list):
        for ev in finding["evidence"]:
            if isinstance(ev, dict) and "block_id" in ev:
                ev["block_id"] = normalize_block_id(ev["block_id"])

    return finding


# ─── Backward compatibility ───────────────────────────────────────────────

def is_graph_v2(graph: dict) -> bool:
    """Проверить, является ли граф версии 2."""
    return graph.get("version", 1) >= 2


def get_page_sheet_no(page: dict) -> str | None:
    """Получить sheet_no из страницы (совместимо с v1 и v2).

    v1: page["sheet_no"]
    v2: page["sheet_no_raw"] (приоритет) или page["sheet_no_normalized"]
    """
    # v2
    raw = page.get("sheet_no_raw")
    if raw is not None:
        return raw
    # v1 fallback
    return page.get("sheet_no")


def get_text_block_text(tb: dict) -> str:
    """Получить текст из text_block (совместимо с v1 и v2)."""
    return tb.get("text", "")


def get_image_block_ocr(ib: dict) -> str:
    """Получить OCR из image_block (совместимо с v1 и v2).

    v2: ocr_text_normalized (приоритет) → ocr_raw
    v1: ocr
    """
    return ib.get("ocr_text_normalized") or ib.get("ocr") or ib.get("ocr_raw") or ""
