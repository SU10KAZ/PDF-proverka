"""Диагностический вывод новой детерминированной цепочки сравнения.

Строго READ-ONLY витрина поверх УЖЕ посчитанных артефактов объекта:

    comparison/change_detection/change_detection.json      (этап 5Б.4)
    comparison/semantic_diff_v6a2/semantic_diff.json       (этап 6А.2)
    .../prepared_comparison/prepared_document.json         (этап 1)

Модуль осознанно НЕ делает (постановка «увидеть результат как есть»):

  • не читает и не трогает ``comparison_result.json`` старого Opus-пути;
  • не создаёт findings и не меняет экспертные решения;
  • не считает влияние / severity;
  • не вызывает LLM, Vision и OCR;
  • не исправляет и НЕ ДЕДУПЛИЦИРУЕТ semantic diff — одинаковые
    «Было → Стало» специально остаются отдельными строками, лишь помечаются
    счётчиком ``same_semantic_result_as_other_groups``;
  • не перезаписывает артефакты новой цепочки.

Система координат: bbox групп и atomic-регионов заданы в точках страницы
ЛЕВОГО (V2) PDF — так их построил этап 5Б (``change_regions.analyze_pair``
переводит правую страницу в левую матрицей этапа 5А). Для правой стороны
bbox пересчитывается обратной матрицей.
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any

SEMANTIC_DIFF_KIND = "stage_comparison_semantic_diff_v6a2_mass"
CHANGE_DETECTION_KIND = "stage_comparison_change_detection_v5b4"

SEMANTIC_DIFF_RELPATH = ("semantic_diff_v6a2", "semantic_diff.json")
CHANGE_DETECTION_RELPATH = ("change_detection", "change_detection.json")

#: Готовые пилотные кропы 6А.1 — единственные картинки, которые уже лежат на
#: диске. Для остальных групп кроп рендерится на лету и НИКУДА не пишется.
PILOT_CROPS_DIR = ("semantic_diff_v6a1", "diagnostics")

EVIDENCE_LEVELS = ("exact", "strong", "contextual", "insufficient")
SEMANTIC_TYPES = ("text", "table", "vector", "image", "mixed", "stamp", "numeric", "complex_graphic")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def is_enabled() -> bool:
    """Диагностический режим можно выключить одним env-флагом."""
    return _env_flag("STAGE_COMPARISON_NEW_PIPELINE_DIAGNOSTIC_ENABLED", True)


# ─── чтение артефактов ───────────────────────────────────────────────────────


def _read_json(path: Path) -> Any:
    import json

    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def semantic_diff_path(comparison_dir: str | Path) -> Path:
    return Path(comparison_dir).joinpath(*SEMANTIC_DIFF_RELPATH)


def change_detection_path(comparison_dir: str | Path) -> Path:
    return Path(comparison_dir).joinpath(*CHANGE_DETECTION_RELPATH)


# ─── геометрия ───────────────────────────────────────────────────────────────


def _matrix_inverse(matrix: Any) -> list[list[float]] | None:
    """Обратная матрица V2 → V3 к матрице этапа 5А (V3 → V2)."""
    try:
        import numpy as np

        return np.linalg.inv(np.asarray(matrix, dtype=float)).tolist()
    except Exception:  # noqa: BLE001 — вырожденная матрица не должна ронять витрину
        return None


def _apply_matrix(matrix: list[list[float]] | None, bbox: list[float] | None) -> list[float] | None:
    if not matrix or not bbox or len(bbox) != 4:
        return None
    x0, y0, x1, y1 = (float(value) for value in bbox)
    corners = ((x0, y0), (x1, y0), (x1, y1), (x0, y1))
    xs: list[float] = []
    ys: list[float] = []
    for x, y in corners:
        denominator = matrix[2][0] * x + matrix[2][1] * y + matrix[2][2]
        if abs(denominator) < 1e-12:
            return None
        xs.append((matrix[0][0] * x + matrix[0][1] * y + matrix[0][2]) / denominator)
        ys.append((matrix[1][0] * x + matrix[1][1] * y + matrix[1][2]) / denominator)
    return [round(min(xs), 3), round(min(ys), 3), round(max(xs), 3), round(max(ys), 3)]


def _normalized(bbox: list[float] | None, size: dict | None) -> list[float] | None:
    """bbox в точках страницы → доли [0..1] для CSS-оверлея."""
    if not bbox or len(bbox) != 4 or not size:
        return None
    width, height = float(size.get("width") or 0), float(size.get("height") or 0)
    if width <= 0 or height <= 0:
        return None
    values = [bbox[0] / width, bbox[1] / height, bbox[2] / width, bbox[3] / height]
    return [round(min(max(value, 0.0), 1.0), 6) for value in values]


# ─── PreparedDocument ────────────────────────────────────────────────────────


def _page_index(prepared: dict | None) -> dict[int, dict]:
    index: dict[int, dict] = {}
    for page in (prepared or {}).get("pages") or []:
        try:
            number = int(page.get("pdf_page") or 0)
        except (TypeError, ValueError):
            continue
        if number <= 0:
            continue
        index[number] = {
            "sheet_number": page.get("sheet_number"),
            "sheet_name": page.get("sheet_name"),
            "page_size": page.get("page_size") or {},
            "source_type": page.get("source_type"),
        }
    return index


def _slot_index(alignment_items: list[dict] | None) -> dict[tuple[int, int], int]:
    index: dict[tuple[int, int], int] = {}
    for item in alignment_items or []:
        left, right, slot = item.get("left_page"), item.get("right_page"), item.get("slot")
        if left is None or right is None or slot is None:
            continue
        try:
            index[(int(left), int(right))] = int(slot)
        except (TypeError, ValueError):
            continue
    return index


# ─── детали группы ───────────────────────────────────────────────────────────


def _table_cell_changes(item: dict) -> list[dict]:
    """`Позиция / колонка / было / стало` — только уже посчитанные ячейки."""
    rows: list[dict] = []
    for change in item.get("table_changes") or []:
        rows.append({
            "kind": "table_cell",
            "position": change.get("row_label") or (f"строка {change.get('row')}" if change.get("row") is not None else "—"),
            "column": change.get("column_label") or (f"колонка {change.get('column')}" if change.get("column") is not None else "—"),
            "before": change.get("before"),
            "after": change.get("after"),
            "evidence_level": change.get("evidence_level"),
            "left_bbox": change.get("left_bbox"),
            "right_bbox": change.get("right_bbox"),
        })
    for change in item.get("numeric_context_changes") or []:
        rows.append({
            "kind": "number",
            "position": change.get("label") or "—",
            "column": change.get("unit") or "—",
            "before": change.get("before"),
            "after": change.get("after"),
            "evidence_level": change.get("evidence_level"),
            "left_bbox": change.get("left_bbox"),
            "right_bbox": change.get("right_bbox"),
        })
    for change in item.get("stamp_field_changes") or []:
        rows.append({
            "kind": "stamp_field",
            "position": change.get("field") or "—",
            "column": change.get("change") or "—",
            "before": change.get("before"),
            "after": change.get("after"),
            "evidence_level": change.get("evidence_level"),
            "left_bbox": change.get("value_bbox"),
            "right_bbox": None,
        })
    return rows


def _table_row_summary(item: dict) -> dict:
    def _texts(rows: list[dict] | None) -> list[str]:
        result = []
        for row in rows or []:
            cells = [str(cell.get("text") or "").strip() for cell in row.get("cells") or []]
            joined = " | ".join(value for value in cells if value)
            if joined:
                result.append(joined)
        return result

    return {
        "inserted": _texts(item.get("inserted_table_rows")),
        "removed": _texts(item.get("removed_table_rows")),
    }


def _atomic_regions_view(item: dict, matrix_inverse, left_size, right_size) -> list[dict]:
    regions = []
    for region in item.get("atomic_region_evidence") or []:
        bbox = region.get("bbox")
        right_bbox = _apply_matrix(matrix_inverse, bbox)
        regions.append({
            "region_id": region.get("region_id"),
            "bbox": bbox,
            "bbox_norm_left": _normalized(bbox, left_size),
            "bbox_norm_right": _normalized(right_bbox, right_size),
            "change_types": region.get("change_types") or [],
            "region_role": region.get("region_role"),
            "confidence": region.get("confidence"),
            "strength": region.get("strength"),
            "diff_counts": region.get("diff_counts") or {},
            "evidence_ids": region.get("evidence_ids") or [],
            "page_area_ratio": region.get("page_area_ratio"),
            "left_block_ids": region.get("left_block_ids") or [],
            "right_block_ids": region.get("right_block_ids") or [],
        })
    return regions


def _crop_availability(comparison_dir: Path, left_page: int, right_page: int, group_id: str) -> dict:
    """Что из картинок уже лежит на диске, а что придётся рендерить на лету."""
    stem = f"v2_{left_page:03d}_v3_{right_page:03d}_{group_id}"
    pilot = comparison_dir.joinpath(*PILOT_CROPS_DIR)
    existing = {
        side: (pilot / f"{stem}_{side}.png").is_file()
        for side in ("v2", "v3", "overlay")
    }
    return {
        "v2": {"available": True, "source": "pilot_file" if existing["v2"] else "on_demand_render"},
        "v3": {"available": True, "source": "pilot_file" if existing["v3"] else "on_demand_render"},
        # Overlay строится только этапом 6А.1 и только для 12 пилотных групп;
        # заново его не считаем — там своя логика совмещения.
        "overlay": {"available": existing["overlay"], "source": "pilot_file" if existing["overlay"] else None},
    }


# ─── сборка payload ──────────────────────────────────────────────────────────


def build_payload(
    comparison_dir: str | Path,
    left_prepared: dict | None,
    right_prepared: dict | None,
    alignment_items: list[dict] | None = None,
) -> dict:
    """Собрать витрину всех change groups новой цепочки. Ничего не пишет."""
    comparison = Path(comparison_dir)
    semantic_path, detection_path = semantic_diff_path(comparison), change_detection_path(comparison)
    semantic, detection = _read_json(semantic_path), _read_json(detection_path)

    def _unavailable(reason: str) -> dict:
        return {
            "available": False,
            "reason": reason,
            "items": [],
            "summary": {},
            "source": {
                "semantic_diff_path": str(semantic_path),
                "semantic_diff_exists": semantic_path.is_file(),
                "change_detection_path": str(detection_path),
                "change_detection_exists": detection_path.is_file(),
            },
        }

    if not isinstance(semantic, dict) or semantic.get("kind") != SEMANTIC_DIFF_KIND:
        return _unavailable("semantic_diff_v6a2_missing_run_stage_6a2_first")
    if not isinstance(detection, dict) or detection.get("kind") != CHANGE_DETECTION_KIND:
        return _unavailable("change_detection_v5b4_missing_run_stage_5b4_first")

    left_pages, right_pages = _page_index(left_prepared), _page_index(right_prepared)
    slots = _slot_index(alignment_items)

    pair_meta: dict[tuple[int, int], dict] = {}
    for pair_item in detection.get("items") or []:
        try:
            key = (int(pair_item.get("left_page")), int(pair_item.get("right_page")))
        except (TypeError, ValueError):
            continue
        alignment = pair_item.get("alignment") or {}
        matrix = ((alignment.get("transform") or {}).get("matrix"))
        pair_meta[key] = {
            "alignment_status": alignment.get("status"),
            "alignment_quality": (alignment.get("quality") or {}).get("confidence"),
            "matrix": matrix,
            "matrix_inverse": _matrix_inverse(matrix) if matrix else None,
            "pair_status": pair_item.get("status"),
            "review_reasons": pair_item.get("review_reasons") or [],
            "metrics": pair_item.get("metrics") or {},
            "diagnostics": pair_item.get("diagnostics") or {},
        }

    # «Не скрывать дубли»: только СЧИТАЕМ одинаковые «Было → Стало», строки
    # остаются все до единой.
    duplicates: dict[tuple[str, str], list[str]] = defaultdict(list)
    for raw in semantic.get("items") or []:
        key = (str(raw.get("before") or ""), str(raw.get("after") or ""))
        duplicates[key].append(f"{raw.get('left_page')}↔{raw.get('right_page')}·{raw.get('group_id')}")

    items: list[dict] = []
    for raw in semantic.get("items") or []:
        try:
            left_page, right_page = int(raw.get("left_page")), int(raw.get("right_page"))
        except (TypeError, ValueError):
            continue
        group_id = str(raw.get("group_id") or "")
        meta = pair_meta.get((left_page, right_page), {})
        left_info, right_info = left_pages.get(left_page, {}), right_pages.get(right_page, {})
        left_size, right_size = left_info.get("page_size"), right_info.get("page_size")
        bbox = raw.get("bbox")
        bbox_right = _apply_matrix(meta.get("matrix_inverse"), bbox)

        duplicate_key = (str(raw.get("before") or ""), str(raw.get("after") or ""))
        twins = [value for value in duplicates[duplicate_key]
                 if value != f"{left_page}↔{right_page}·{group_id}"]

        items.append({
            "id": f"{left_page}:{right_page}:{group_id}",
            "group_id": group_id,
            "left_page": left_page,
            "right_page": right_page,
            "left_sheet": left_info.get("sheet_number"),
            "left_sheet_name": left_info.get("sheet_name"),
            "right_sheet": right_info.get("sheet_number"),
            "right_sheet_name": right_info.get("sheet_name"),
            "alignment_slot": slots.get((left_page, right_page)),
            "alignment_status": meta.get("alignment_status"),
            "pair_status": meta.get("pair_status"),
            "pair_review_reasons": meta.get("review_reasons") or [],

            # смысловой слой 6А.2 — как есть, без правок
            "semantic_type": raw.get("semantic_type"),
            "region_role": raw.get("region_role"),
            "change_kind": raw.get("change_kind"),
            "change_summary": raw.get("change_summary"),
            "change_types": raw.get("change_types") or [],
            "before": raw.get("before"),
            "after": raw.get("after"),
            "evidence_level": raw.get("evidence_level"),
            "confidence": raw.get("confidence"),
            "source": raw.get("source"),
            "resolution_status": raw.get("resolution_status"),
            "requires_human_review": bool(raw.get("requires_human_review")),
            "next_analysis": raw.get("next_analysis"),
            "unresolved_reason": raw.get("unresolved_reason"),
            "sheet_review_required": bool(raw.get("sheet_review_required")),
            "sheet_review_reasons": raw.get("sheet_review_reasons") or [],
            "exact_entity_used_for_result": raw.get("exact_entity_used_for_result"),

            # геометрия
            "bbox": bbox,
            "bbox_right": bbox_right,
            "bbox_norm_left": _normalized(bbox, left_size),
            "bbox_norm_right": _normalized(bbox_right, right_size),
            "page_size_left": left_size,
            "page_size_right": right_size,
            "block_ids": raw.get("block_ids") or [],

            # подробности
            "atomic_region_ids": raw.get("atomic_region_ids") or [],
            "atomic_regions": _atomic_regions_view(raw, meta.get("matrix_inverse"), left_size, right_size),
            "cell_changes": _table_cell_changes(raw),
            "table_rows": _table_row_summary(raw),
            "entity_location_uncertain": raw.get("entity_location_uncertain"),
            "localized_entities_left": raw.get("localized_entities_left") or [],
            "localized_entities_right": raw.get("localized_entities_right") or [],

            # честный маркер дубля — строки НЕ склеиваем
            "same_semantic_result_as_other_groups": len(twins),
            "same_semantic_result_group_ids": twins[:20],

            "crops": _crop_availability(comparison, left_page, right_page, group_id),
        })

    items.sort(key=lambda row: (row["left_page"], row["right_page"], row["group_id"]))

    return {
        "available": True,
        "reason": None,
        "items": items,
        "summary": semantic.get("summary") or {},
        "settings": semantic.get("settings") or {},
        "detection_summary": detection.get("summary") or {},
        "requires_alignment_fallback": detection.get("requires_alignment_fallback") or [],
        "pilot_v6a1_parity": semantic.get("pilot_v6a1_parity") or {},
        "filters": {"evidence_levels": list(EVIDENCE_LEVELS), "semantic_types": list(SEMANTIC_TYPES)},
        "source": {
            "kind": semantic.get("kind"),
            "detection_kind": detection.get("kind"),
            "semantic_diff_path": str(semantic_path),
            "semantic_diff_exists": True,
            "change_detection_path": str(detection_path),
            "change_detection_exists": True,
            "comparison_dir": str(comparison),
        },
        "notice": (
            "Диагностический просмотр новой детерминированной цепочки. Findings не создаются, "
            "влияние не считается, LLM/Vision не вызываются, дубли специально не склеиваются."
        ),
    }


# ─── кропы для просмотра доказательств ───────────────────────────────────────


def find_group(payload: dict, left_page: int, right_page: int, group_id: str) -> dict | None:
    wanted = f"{left_page}:{right_page}:{group_id}"
    return next((item for item in payload.get("items") or [] if item.get("id") == wanted), None)


def pilot_crop_path(comparison_dir: str | Path, left_page: int, right_page: int,
                    group_id: str, side: str) -> Path:
    stem = f"v2_{left_page:03d}_v3_{right_page:03d}_{group_id}_{side}.png"
    return Path(comparison_dir).joinpath(*PILOT_CROPS_DIR) / stem


def render_crop_bytes(pdf_path: str | Path, page_number: int, bbox: list[float] | None, *,
                      padding_pt: float = 18.0, target_long_side: int = 1100) -> bytes:
    """Отрендерить кроп области прямо из PDF — в память, без записи на диск.

    Никаких новых артефактов: файлы этапов 5Б/6А остаются нетронутыми.
    """
    import fitz

    document = fitz.open(str(pdf_path))
    try:
        if page_number < 1 or page_number > document.page_count:
            raise ValueError(f"page_out_of_range:{page_number}>doc:{document.page_count}")
        page = document[page_number - 1]
        rect = page.rect
        if bbox and len(bbox) == 4:
            clip = fitz.Rect(
                max(rect.x0, float(bbox[0]) - padding_pt),
                max(rect.y0, float(bbox[1]) - padding_pt),
                min(rect.x1, float(bbox[2]) + padding_pt),
                min(rect.y1, float(bbox[3]) + padding_pt),
            )
            if clip.width < 1 or clip.height < 1:
                clip = rect
        else:
            clip = rect
        long_side = max(clip.width, clip.height, 1.0)
        scale = max(0.5, min(12.0, target_long_side / long_side))
        pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
        return pixmap.tobytes("png")
    finally:
        document.close()


__all__ = [
    "CHANGE_DETECTION_KIND",
    "EVIDENCE_LEVELS",
    "SEMANTIC_DIFF_KIND",
    "SEMANTIC_TYPES",
    "build_payload",
    "change_detection_path",
    "find_group",
    "is_enabled",
    "pilot_crop_path",
    "render_crop_bytes",
    "semantic_diff_path",
]
