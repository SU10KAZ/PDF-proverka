"""Этап 6А.1: evidence-first детерминированное «Было → Стало».

Слой читает готовый 5Б.4 и старый 6А, но не изменяет их. Основные отличия:
табличные cell/row/column модели, контекст чисел, явная локализация entities и
честный ``geometric_change`` без попытки угадать инженерный смысл.
"""
from __future__ import annotations

import json
import difflib
import math
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import fitz
import numpy as np

from . import semantic_diff as v6a
from .sheet_alignment import transform_points


SCHEMA_VERSION = 1
_NUMBER = re.compile(r"^[+−-]?(?:\d+(?:[.,]\d+)?|[.,]\d+)(?:[%°]|(?:мм|см|м|м²|м³|а|в|квт))?$", re.I)
_UNIT = re.compile(r"^(?:мм|см|м|м2|м²|м3|м³|шт|а|в|квт|ква|%|°)$", re.I)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise


def normalize_number(value: str) -> dict[str, Any] | None:
    """Осторожная нормализация: формат хранится рядом, значение не округляется."""
    raw = v6a._clean(value).replace("−", "-")
    compact = raw.replace(" ", "")
    match = re.match(r"^([+-]?(?:\d+(?:[.,]\d+)?|[.,]\d+))(.*)$", compact)
    if not match:
        return None
    number, suffix = match.groups()
    if suffix and not re.fullmatch(r"(?:%|°|мм|см|м|м²|м³|а|в|квт|ква)", suffix, re.I):
        return None
    canonical = number.replace(",", ".")
    try: numeric = float(canonical)
    except ValueError: return None
    return {"raw": raw, "canonical": canonical, "numeric": numeric, "unit": suffix.lower()}


def _cluster(values: list[float], tolerance: float = 1.2) -> list[float]:
    result: list[list[float]] = []
    for value in sorted(values):
        if not result or abs(value - sum(result[-1]) / len(result[-1])) > tolerance:
            result.append([value])
        else:
            result[-1].append(value)
    return [round(sum(group) / len(group), 4) for group in result]


def _union_length(intervals: list[tuple[float, float]]) -> float:
    total, end = 0.0, None
    for start, stop in sorted((min(a, b), max(a, b)) for a, b in intervals):
        if end is None or start > end:
            total += stop - start; end = stop
        elif stop > end:
            total += stop - end; end = stop
    return total


def extract_table_boundaries(page: fitz.Page, table_bbox: list[float], matrix: Any | None = None) -> tuple[list[float], list[float]]:
    """Извлечь устойчивые полные линии сетки, объединяя сегменты одного ряда."""
    x0, y0, x1, y1 = table_bbox
    width, height = max(1.0, x1 - x0), max(1.0, y1 - y0)
    horizontal: dict[float, list[tuple[float, float]]] = {}
    vertical: dict[float, list[tuple[float, float]]] = {}
    for drawing in page.get_drawings():
        for item in drawing.get("items") or []:
            if item[0] != "l":
                continue
            points = np.asarray([[item[1].x, item[1].y], [item[2].x, item[2].y]], dtype=float)
            if matrix is not None:
                points = transform_points(matrix, points)
            (ax, ay), (bx, by) = points
            if abs(ay - by) <= 1.2 and max(ax, bx) >= x0 and min(ax, bx) <= x1 and y0 - 2 <= ay <= y1 + 2:
                key = round((ay + by) / 2, 1)
                horizontal.setdefault(key, []).append((max(x0, min(ax, bx)), min(x1, max(ax, bx))))
            elif abs(ax - bx) <= 1.2 and max(ay, by) >= y0 and min(ay, by) <= y1 and x0 - 2 <= ax <= x1 + 2:
                key = round((ax + bx) / 2, 1)
                vertical.setdefault(key, []).append((max(y0, min(ay, by)), min(y1, max(ay, by))))
    ys = _cluster([key for key, intervals in horizontal.items() if _union_length(intervals) >= width * .55])
    xs = _cluster([key for key, intervals in vertical.items() if _union_length(intervals) >= height * .55])
    return [value for value in xs if x0 - 2 <= value <= x1 + 2], [value for value in ys if y0 - 2 <= value <= y1 + 2]


def build_table_model(words: list[dict[str, Any]], xs: list[float], ys: list[float], *, block_id: str | None = None) -> dict[str, Any]:
    """Построить cells с координатами; функция чистая и тестируется отдельно."""
    xs, ys = sorted(set(xs)), sorted(set(ys))
    if len(xs) < 2 or len(ys) < 2:
        return {"valid": False, "reason": "insufficient_grid", "rows": [], "columns": [], "cells": []}
    cells = []
    for row in range(len(ys) - 1):
        for column in range(len(xs) - 1):
            bbox = [xs[column], ys[row], xs[column + 1], ys[row + 1]]
            members = [word for word in words if bbox[0] <= (word["bbox"][0] + word["bbox"][2]) / 2 <= bbox[2]
                       and bbox[1] <= (word["bbox"][1] + word["bbox"][3]) / 2 <= bbox[3]]
            members.sort(key=lambda word: (word["bbox"][1], word["bbox"][0], word.get("word", 0)))
            cells.append({"row": row, "column": column, "bbox": bbox,
                          "text": " ".join(word["text"] for word in members), "words": members})
    rows = []
    for row in range(len(ys) - 1):
        row_cells = [cell for cell in cells if cell["row"] == row]
        texts = [cell["text"] for cell in row_cells]
        label = next((text for text in texts[:max(1, len(texts) // 2)] if text and not normalize_number(text)), "")
        rows.append({"row": row, "bbox": [xs[0], ys[row], xs[-1], ys[row + 1]], "cells": row_cells,
                     "label": label, "signature": [v6a._norm(text) for text in texts if text]})
    columns = []
    for column in range(len(xs) - 1):
        column_cells = [cell for cell in cells if cell["column"] == column]
        header = next((cell["text"] for cell in column_cells[:min(3, len(column_cells))]
                       if cell["text"] and not normalize_number(cell["text"])), "")
        columns.append({"column": column, "bbox": [xs[column], ys[0], xs[column + 1], ys[-1]], "header": header})
    return {"valid": True, "block_id": block_id, "rows": rows, "columns": columns, "cells": cells,
            "bbox": [xs[0], ys[0], xs[-1], ys[-1]], "grid": {"x": xs, "y": ys}}


def _row_similarity(left: dict, right: dict) -> float:
    la, ra = set(left["signature"]), set(right["signature"])
    text = len(la & ra) / max(1, len(la | ra))
    label = 1.0 if left.get("label") and v6a._norm(left["label"]) == v6a._norm(right.get("label")) else 0.0
    ly = (left["bbox"][1] + left["bbox"][3]) / 2
    ry = (right["bbox"][1] + right["bbox"][3]) / 2
    geometry = max(0.0, 1 - abs(ly - ry) / max(20.0, left["bbox"][3] - left["bbox"][1], right["bbox"][3] - right["bbox"][1]) / 4)
    return .55 * text + .30 * label + .15 * geometry


def match_table_rows(left_rows: list[dict], right_rows: list[dict], threshold: float = .38) -> dict[str, Any]:
    candidates = sorted(((-_row_similarity(left, right), li, ri) for li, left in enumerate(left_rows)
                         for ri, right in enumerate(right_rows)), key=lambda item: (item[0], item[1], item[2]))
    used_left, used_right, matched = set(), set(), []
    for negative, li, ri in candidates:
        score = -negative
        if score < threshold or li in used_left or ri in used_right:
            continue
        used_left.add(li); used_right.add(ri)
        matched.append({"left_row": li, "right_row": ri, "score": round(score, 4)})
    return {"matched": sorted(matched, key=lambda item: item["left_row"]),
            "removed_rows": [i for i in range(len(left_rows)) if i not in used_left],
            "inserted_rows": [i for i in range(len(right_rows)) if i not in used_right]}


def compare_table_models(left: dict, right: dict) -> dict[str, Any]:
    if not left.get("valid") and not right.get("valid"):
        return {"applicable": False, "changes": [], "inserted_rows": [], "removed_rows": []}
    if left.get("valid") and not right.get("valid"):
        nonempty = [row for row in left["rows"] if any(cell["text"] for cell in row["cells"])]
        return {"applicable": True, "changes": [], "inserted_rows": [], "removed_rows": nonempty,
                "evidence_level": "strong", "reason": "table_removed_or_not_present_on_right"}
    if right.get("valid") and not left.get("valid"):
        nonempty = [row for row in right["rows"] if any(cell["text"] for cell in row["cells"])]
        return {"applicable": True, "changes": [], "inserted_rows": nonempty, "removed_rows": [],
                "evidence_level": "strong", "reason": "table_added_or_not_present_on_left"}
    def title(model: dict) -> str:
        for row in model["rows"][:4]:
            text = " ".join(cell["text"] for cell in row["cells"] if cell["text"])
            if len(v6a._norm(text)) >= 8:
                return text
        return ""
    left_title, right_title = title(left), title(right)
    title_similarity = difflib.SequenceMatcher(None, v6a._norm(left_title), v6a._norm(right_title)).ratio() if left_title and right_title else 1.0
    if left_title and right_title and title_similarity < .50:
        left_nonempty = [row for row in left["rows"] if any(cell["text"] for cell in row["cells"])]
        right_nonempty = [row for row in right["rows"] if any(cell["text"] for cell in row["cells"])]
        return {"applicable": True, "table_replaced": True, "left_title": left_title, "right_title": right_title,
                "title_similarity": round(title_similarity, 4), "changes": [],
                "inserted_rows": right_nonempty, "removed_rows": left_nonempty,
                "evidence_level": "strong", "reason": "different_local_table_titles_and_grids"}
    rows = match_table_rows(left["rows"], right["rows"])
    changes = []
    for match in rows["matched"]:
        lrow, rrow = left["rows"][match["left_row"]], right["rows"][match["right_row"]]
        limit = min(len(lrow["cells"]), len(rrow["cells"]))
        for column in range(limit):
            lc, rc = lrow["cells"][column], rrow["cells"][column]
            if v6a._norm(lc["text"]) == v6a._norm(rc["text"]):
                continue
            if not lc["text"] and not rc["text"]:
                continue
            header = (left["columns"][column].get("header") or right["columns"][column].get("header") or "")
            row_label = lrow.get("label") or rrow.get("label") or ""
            header_values = {v6a._norm(value.get("header")) for value in left["columns"] + right["columns"] if value.get("header")}
            # Не выдаём перестройку многострочной шапки за изменение data cell.
            if column == 0 or not row_label or not header or v6a._norm(row_label) in header_values:
                continue
            changes.append({"row": match["left_row"], "right_row": match["right_row"], "column": column,
                            "row_label": row_label, "column_label": header, "before": lc["text"], "after": rc["text"],
                            "left_bbox": lc["bbox"], "right_bbox": rc["bbox"], "row_match_score": match["score"],
                            "evidence_level": "exact" if row_label and header else "strong"})
    left_labels = {v6a._norm(row.get("label")) for row in left["rows"] if row.get("label")}
    right_labels = {v6a._norm(row.get("label")) for row in right["rows"] if row.get("label")}
    inserted_rows = [right["rows"][i] for i in rows["inserted_rows"]
                     if right["rows"][i].get("label") and v6a._norm(right["rows"][i]["label"]) not in left_labels]
    removed_rows = [left["rows"][i] for i in rows["removed_rows"]
                    if left["rows"][i].get("label") and v6a._norm(left["rows"][i]["label"]) not in right_labels]
    left_nonempty = sum(any(cell["text"] for cell in row["cells"]) for row in left["rows"])
    right_nonempty = sum(any(cell["text"] for cell in row["cells"]) for row in right["rows"])
    unmatched_share = (len(inserted_rows) + len(removed_rows)) / max(1, left_nonempty + right_nonempty)
    return {"applicable": True, "changes": changes,
            "inserted_rows": inserted_rows, "removed_rows": removed_rows,
            "row_matching": rows, "unmatched_row_share": round(unmatched_share, 4),
            "evidence_level": "exact" if changes and all(c["evidence_level"] == "exact" for c in changes)
                              else "contextual" if unmatched_share > .45 else "strong"}


def _sequence_bbox(words: list[dict]) -> list[float]:
    return [min(w["bbox"][0] for w in words), min(w["bbox"][1] for w in words),
            max(w["bbox"][2] for w in words), max(w["bbox"][3] for w in words)]


def localize_entities(blocks: list[dict], words: list[dict], group_bbox: list[float]) -> list[dict[str, Any]]:
    result = []
    normalized_words = [v6a._norm(word["text"]) for word in words]
    for block in blocks:
        for entity in block.get("entities") or []:
            target = v6a._norm(entity)
            found = None
            for start in range(len(words)):
                combined = ""
                for stop in range(start, min(len(words), start + 8)):
                    combined += normalized_words[stop]
                    if combined == target:
                        found = words[start:stop + 1]; break
                    if len(combined) > len(target): break
                if found: break
            if found:
                bbox = _sequence_bbox(found)
                location = "exact" if v6a._intersects(bbox, group_bbox) else "contextual"
                result.append({"entity": entity, "block_id": block.get("block_id"), "bbox": bbox,
                               "entity_location": location, "method": "pdf_words", "quote": " ".join(w["text"] for w in found)})
            else:
                result.append({"entity": entity, "block_id": block.get("block_id"), "bbox": None,
                               "entity_location": "uncertain", "method": "no_local_text_anchor"})
    unique = {}
    for item in result:
        key = (v6a._norm(item["entity"]), item["block_id"], item["entity_location"])
        unique.setdefault(key, item)
    return list(unique.values())


def number_contexts(words: list[dict], group_bbox: list[float]) -> list[dict[str, Any]]:
    result = []
    for index, word in enumerate(words):
        number = normalize_number(word["text"])
        if not number or not v6a._intersects(word["bbox"], group_bbox):
            continue
        cy = (word["bbox"][1] + word["bbox"][3]) / 2
        neighbors = []
        for other_index, other in enumerate(words):
            if other_index == index or normalize_number(other["text"]): continue
            oy = (other["bbox"][1] + other["bbox"][3]) / 2
            distance = min(abs(word["bbox"][0] - other["bbox"][2]), abs(other["bbox"][0] - word["bbox"][2]))
            if abs(cy - oy) <= max(6.0, (word["bbox"][3] - word["bbox"][1]) * 1.2) and distance <= 100:
                neighbors.append((distance, other_index, other))
        neighbors.sort(key=lambda item: (item[0], item[1]))
        labels = [item[2]["text"] for item in neighbors[:3] if not _UNIT.fullmatch(item[2]["text"])]
        units = [item[2]["text"] for item in neighbors[:3] if _UNIT.fullmatch(item[2]["text"])]
        result.append({"value": word["text"], "normalized": number, "bbox": word["bbox"],
                       "labels": labels, "unit": number["unit"] or (units[0] if units else ""),
                       "context_key": "|".join(v6a._norm(value) for value in labels[:2]),
                       "context_reliable": bool(labels)})
    return result


def match_numeric_contexts(left: list[dict], right: list[dict]) -> list[dict[str, Any]]:
    candidates = []
    for li, lval in enumerate(left):
        for ri, rval in enumerate(right):
            if not lval["context_key"] or lval["context_key"] != rval["context_key"]:
                continue
            if lval["unit"] and rval["unit"] and v6a._norm(lval["unit"]) != v6a._norm(rval["unit"]):
                continue
            distance = math.hypot((lval["bbox"][0] + lval["bbox"][2] - rval["bbox"][0] - rval["bbox"][2]) / 2,
                                  (lval["bbox"][1] + lval["bbox"][3] - rval["bbox"][1] - rval["bbox"][3]) / 2)
            candidates.append((distance, li, ri))
    used_l, used_r, result = set(), set(), []
    for distance, li, ri in sorted(candidates):
        if li in used_l or ri in used_r: continue
        used_l.add(li); used_r.add(ri)
        lval, rval = left[li], right[ri]
        if lval["normalized"]["canonical"] == rval["normalized"]["canonical"]: continue
        result.append({"before": lval["value"], "after": rval["value"], "label": lval["labels"][0],
                       "unit": lval["unit"] or rval["unit"], "left_bbox": lval["bbox"], "right_bbox": rval["bbox"],
                       "geometry_distance": round(distance, 3), "evidence_level": "exact"})
    return result


def stamp_field_changes(left_words: list[dict], right_words: list[dict], group_bbox: list[float]) -> list[dict[str, Any]]:
    """Связать новое/удалённое значение штампа с заголовком той же колонки."""
    left_counts, right_counts = Counter(v6a._norm(word["text"]) for word in left_words), Counter(v6a._norm(word["text"]) for word in right_words)
    stable_labels = []
    for word in left_words:
        normalized = v6a._norm(word["text"])
        if not normalized or normalize_number(word["text"]) or left_counts[normalized] != right_counts[normalized]:
            continue
        if len(word["text"]) <= 12:
            stable_labels.append(word)
    result = []
    for side, words, own, other in (("added", right_words, right_counts, left_counts), ("removed", left_words, left_counts, right_counts)):
        consumed: Counter[str] = Counter()
        for word in words:
            key = v6a._norm(word["text"])
            if not key or own[key] - consumed[key] <= other[key]:
                consumed[key] += 1; continue
            consumed[key] += 1
            if not v6a._intersects(word["bbox"], group_bbox):
                continue
            wx = (word["bbox"][0] + word["bbox"][2]) / 2
            wy = (word["bbox"][1] + word["bbox"][3]) / 2
            candidates = []
            for label in stable_labels:
                lx = (label["bbox"][0] + label["bbox"][2]) / 2
                ly = (label["bbox"][1] + label["bbox"][3]) / 2
                if abs(wx - lx) <= 28 and abs(wy - ly) <= 28:
                    candidates.append((abs(wx - lx) + .25 * abs(wy - ly), label))
            if not candidates:
                continue
            label = min(candidates, key=lambda item: item[0])[1]
            result.append({"field": label["text"], "before": word["text"] if side == "removed" else "отсутствует",
                           "after": word["text"] if side == "added" else "отсутствует", "change": side,
                           "value_bbox": word["bbox"], "label_bbox": label["bbox"], "evidence_level": "exact"})
    return result


def _best_table_block(blocks: list[dict], group_bbox: list[float]) -> dict | None:
    # PreparedDocument исторически называл часть ведомостей ``legend``.
    # Решение о валидности всё равно принимает реальная vector grid ниже.
    tables = [block for block in blocks if block.get("semantic_type") in {"table", "legend"} and block.get("bbox")]
    return max(tables, key=lambda block: block.get("group_overlap_of_block", 0), default=None)


def _table_for_side(page: fitz.Page, blocks: list[dict], all_words: list[dict], group_bbox: list[float], matrix: Any | None = None) -> dict:
    block = _best_table_block(blocks, group_bbox)
    if not block:
        return {"valid": False, "reason": "no_local_table_block", "rows": [], "columns": [], "cells": []}
    bbox = block["bbox"]
    xs, ys = extract_table_boundaries(page, bbox, matrix)
    words = [word for word in all_words if v6a._intersects(word["bbox"], bbox)]
    model = build_table_model(words, xs, ys, block_id=block.get("block_id"))
    model["boundary_counts"] = {"x": len(xs), "y": len(ys)}
    return model


def _result_from_analysis(group: dict, context: dict, table_diff: dict, entity_left: list[dict],
                          entity_right: list[dict], numeric: list[dict], evidence: list[dict],
                          stamp_fields: list[dict] | None = None) -> dict[str, Any]:
    direct_left = [item for item in entity_left if item["entity_location"] == "exact"]
    direct_right = [item for item in entity_right if item["entity_location"] == "exact"]
    table_changes = table_diff.get("changes") or []
    inserted, removed = table_diff.get("inserted_rows") or [], table_diff.get("removed_rows") or []
    stamp_fields = stamp_fields or []
    if table_diff.get("table_replaced"):
        before, after = table_diff["left_title"], table_diff["right_title"]
        summary = f"Локальная таблица «{before}» заменена таблицей «{after}»; строки двух разных таблиц не сопоставлялись искусственно."
        level, kind = "strong", "reconfigured"
    elif stamp_fields:
        before = "; ".join(f"{item['field']}: {item['before']}" for item in stamp_fields)
        after = "; ".join(f"{item['field']}: {item['after']}" for item in stamp_fields)
        summary = f"Точно локализованы изменённые поля штампа: {', '.join(item['field'] for item in stamp_fields)}."
        level, kind = "exact", "changed"
    elif table_changes:
        shown = table_changes[:12]
        before = "; ".join(f"{c.get('row_label') or 'строка ?'} / {c.get('column_label') or 'колонка ?'}: {c['before'] or 'отсутствует'}" for c in shown)
        after = "; ".join(f"{c.get('row_label') or 'строка ?'} / {c.get('column_label') or 'колонка ?'}: {c['after'] or 'отсутствует'}" for c in shown)
        level = "exact" if all(change["evidence_level"] == "exact" for change in table_changes) else "strong"
        summary = f"Изменено ячеек: {len(table_changes)}; строка и колонка определены для {sum(bool(c.get('row_label') and c.get('column_label')) for c in table_changes)}."
        kind = "changed"
    elif inserted or removed:
        before = "; ".join(row.get("label") or " | ".join(c["text"] for c in row["cells"] if c["text"]) for row in removed[:8]) or "отсутствует"
        after = "; ".join(row.get("label") or " | ".join(c["text"] for c in row["cells"] if c["text"]) for row in inserted[:8]) or "отсутствует"
        kind = "added" if inserted and not removed else "removed" if removed and not inserted else "changed"
        level = table_diff.get("evidence_level") or "strong"
        summary = f"Строки таблицы: добавлено {len(inserted)}, удалено {len(removed)}."
    elif numeric:
        before = "; ".join(f"{item['label']}: {item['before']} {item['unit']}".strip() for item in numeric)
        after = "; ".join(f"{item['label']}: {item['after']} {item['unit']}".strip() for item in numeric)
        level, kind = "exact", "changed"
        summary = f"Изменено числовых значений с надёжным локальным контекстом: {len(numeric)}."
    elif group.get("change_types") == ["vector"]:
        before, after = "Геометрия согласно V2", "Геометрия изменена в V3"
        summary = "Обнаружено локальное изменение векторной геометрии без достаточных данных для определения инженерного смысла."
        level, kind = "insufficient", "geometric_change"
    elif group.get("change_types") == ["image"]:
        before, after = "Изображение согласно V2", "Изображение изменено или добавлено в V3"
        summary = "Обнаружено локальное изменение изображения без достаточных данных для определения его содержания."
        level, kind = "insufficient", "image_change"
    else:
        # Односторонний точный local word diff разрешён только как факт текста,
        # но без label остаётся contextual и требует проверки.
        changes = v6a._pair_text_entries(evidence)
        before_values = [change["before"] for change in changes if change["before"]]
        after_values = [change["after"] for change in changes if change["after"]]
        before, after = "; ".join(before_values) or "отсутствует", "; ".join(after_values) or "отсутствует"
        if changes:
            kind = "added" if not before_values else "removed" if not after_values else "changed"
            level = "strong" if len(changes) <= 4 and (direct_left or direct_right) else "contextual"
            summary = f"Локально доказаны текстовые различия ({len(changes)}), но строка/label определены не полностью."
        else:
            kind, level = "uncertain", "insufficient"
            summary = "Недостаточно локальных доказательств для точного «Было → Стало»."
    confidence = {"exact": .97, "strong": .88, "contextual": .65, "insufficient": .35}[level]
    review = level in {"contextual", "insufficient"}
    return {"before": before, "after": after, "change_summary": summary, "change_kind": kind,
            "source": "deterministic_v6a1", "evidence_level": level, "confidence": confidence,
            "requires_human_review": review, "table_changes": table_changes,
            "inserted_table_rows": inserted, "removed_table_rows": removed,
            "numeric_context_changes": numeric, "localized_entities_left": direct_left,
            "localized_entities_right": direct_right,
            "entity_location_uncertain": sum(item["entity_location"] == "uncertain" for item in entity_left + entity_right),
            "stamp_field_changes": stamp_fields}


def analyze_group(left_page: fitz.Page, right_page: fitz.Page, left_document: dict, right_document: dict,
                  pair_item: dict, group: dict, *, padding_pt: float = v6a.DEFAULT_PADDING_PT) -> dict[str, Any]:
    """Применить к одной группе неизменённый deterministic pipeline пилота 6А.1."""
    context = v6a._context_for_group(
        pair_item, group, left_document, right_document,
        left_page.parent, right_page.parent, padding=padding_pt,
    )
    matrix = pair_item["alignment"]["transform"]["matrix"]
    left_all, right_all = v6a._words(left_page), v6a._words(right_page, matrix)
    left_table = _table_for_side(left_page, context["left"]["blocks"], left_all, group["bbox"])
    right_table = _table_for_side(right_page, context["right"]["blocks"], right_all, group["bbox"], matrix)
    table_diff = compare_table_models(left_table, right_table)
    entities_left = localize_entities(context["left"]["blocks"], context["left"]["words"], group["bbox"])
    entities_right = localize_entities(
        context["right"]["blocks"], context["right"]["words_v2_coordinates"], group["bbox"],
    )
    left_numbers = number_contexts(context["left"]["words"], group["bbox"])
    right_numbers = number_contexts(context["right"]["words_v2_coordinates"], group["bbox"])
    numeric_changes = match_numeric_contexts(left_numbers, right_numbers)
    stamp_fields = (
        stamp_field_changes(context["left"]["words"], context["right"]["words_v2_coordinates"], group["bbox"])
        if group["region_role"] == "stamp" else []
    )
    evidence = context["evidence"]
    if not v6a._pair_text_entries(evidence):
        evidence = sorted(
            evidence + v6a._local_word_evidence(
                context["left"]["words"], context["right"]["words_v2_coordinates"], group["bbox"],
            ),
            key=lambda entry: (entry.get("bucket", ""), entry.get("evidence_id", "")),
        )
    result = _result_from_analysis(
        group, context, table_diff, entities_left, entities_right,
        numeric_changes, evidence, stamp_fields,
    )
    return {
        **result,
        "table_model": {"left": left_table, "right": right_table, "comparison": table_diff},
        "entity_localization": {"left": entities_left, "right": entities_right},
        "number_contexts": {"left": left_numbers, "right": right_numbers},
        "evidence": evidence,
        "context": context,
    }


def run_pilot(left_pdf_path: str | Path, right_pdf_path: str | Path, left_document: dict, right_document: dict,
              change_detection: dict, old_semantic: dict, destination: str | Path,
              *, padding_pt: float = v6a.DEFAULT_PADDING_PT,
              selection: tuple[tuple[int, int, str, str], ...] = v6a.PILOT_SELECTION) -> dict[str, Any]:
    destination = Path(destination); diagnostics = destination / "diagnostics"
    by_pair = {(int(item["left_page"]), int(item["right_page"])): item for item in change_detection.get("items") or []}
    old_by = {(int(item["left_page"]), int(item["right_page"]), item["group_id"]): item for item in old_semantic.get("items") or []}
    results = []
    with fitz.open(left_pdf_path) as left_pdf, fitz.open(right_pdf_path) as right_pdf:
        for left_number, right_number, group_id, reason in selection:
            item = by_pair[(left_number, right_number)]
            group = next(value for value in item["change_groups"] if value["group_id"] == group_id)
            context = v6a._context_for_group(item, group, left_document, right_document, left_pdf, right_pdf, padding=padding_pt)
            matrix = item["alignment"]["transform"]["matrix"]
            left_all, right_all = v6a._words(left_pdf[left_number - 1]), v6a._words(right_pdf[right_number - 1], matrix)
            left_table = _table_for_side(left_pdf[left_number - 1], context["left"]["blocks"], left_all, group["bbox"])
            right_table = _table_for_side(right_pdf[right_number - 1], context["right"]["blocks"], right_all, group["bbox"], matrix)
            table_diff = compare_table_models(left_table, right_table)
            entities_left = localize_entities(context["left"]["blocks"], context["left"]["words"], group["bbox"])
            entities_right = localize_entities(context["right"]["blocks"], context["right"]["words_v2_coordinates"], group["bbox"])
            left_numbers = number_contexts(context["left"]["words"], group["bbox"])
            right_numbers = number_contexts(context["right"]["words_v2_coordinates"], group["bbox"])
            numeric_changes = match_numeric_contexts(left_numbers, right_numbers)
            stamp_fields = stamp_field_changes(context["left"]["words"], context["right"]["words_v2_coordinates"], group["bbox"]) if group["region_role"] == "stamp" else []
            evidence = context["evidence"]
            if not v6a._pair_text_entries(evidence):
                evidence = sorted(evidence + v6a._local_word_evidence(context["left"]["words"], context["right"]["words_v2_coordinates"], group["bbox"]),
                                  key=lambda entry: (entry.get("bucket", ""), entry.get("evidence_id", "")))
            result = _result_from_analysis(group, context, table_diff, entities_left, entities_right, numeric_changes, evidence, stamp_fields)
            old = old_by.get((left_number, right_number, group_id), {})
            improvement = []
            if old.get("requires_human_review") and not result["requires_human_review"]: improvement.append("manual_review_removed")
            if result["evidence_level"] in {"exact", "strong"}: improvement.append("explicit_evidence_level")
            if table_diff.get("changes"): improvement.append("table_cells_localized")
            if numeric_changes: improvement.append("numbers_bound_to_labels")
            if result["change_kind"] == "geometric_change": improvement.append("vector_semantics_not_invented")
            stem = f"v2_{left_number:03d}_v3_{right_number:03d}_{group_id}"
            visuals = v6a._render_diagnostics(left_pdf[left_number - 1], right_pdf[right_number - 1], group["bbox"], matrix,
                                               padding_pt, diagnostics, stem)
            output_item = {"group_id": group_id, "left_page": left_number, "right_page": right_number,
                            "bbox": group["bbox"], "change_types": group["change_types"], "region_role": group["region_role"],
                            "selection_reason": reason, "atomic_region_ids": group["atomic_region_ids"], "block_ids": group["block_ids"],
                            **result, "old_result": {key: old.get(key) for key in ("before", "after", "change_summary", "change_kind", "source", "confidence", "requires_human_review")},
                            "comparison_to_v6a": {"improvements": improvement,
                                                  "verdict_changed": old.get("change_kind") != result["change_kind"],
                                                  "confidence_delta": round(result["confidence"] - float(old.get("confidence") or 0), 3),
                                                  "reason": "confidence_now_derived_from_local_evidence"},
                            "table_model": {"left": left_table, "right": right_table, "comparison": table_diff},
                            "entity_localization": {"left": entities_left, "right": entities_right},
                            "number_contexts": {"left": left_numbers, "right": right_numbers},
                            "evidence": evidence, "diagnostics": visuals}
            results.append(output_item)
            _atomic_write(diagnostics / f"{stem}.json", json.dumps({
                "group": {key: output_item[key] for key in ("group_id", "left_page", "right_page", "bbox", "change_types", "region_role")},
                "result": {key: output_item[key] for key in ("before", "after", "change_summary", "change_kind", "evidence_level", "confidence", "requires_human_review")},
                "comparison_to_v6a": output_item["comparison_to_v6a"],
                "table": {"comparison": table_diff, "left_grid": left_table.get("grid"), "right_grid": right_table.get("grid")},
                "entities": output_item["entity_localization"], "number_contexts": output_item["number_contexts"],
                "evidence": evidence, "visuals": visuals,
            }, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    levels = Counter(item["evidence_level"] for item in results)
    summary = {"selected_groups": len(results), "without_human_review": sum(not item["requires_human_review"] for item in results),
               "requires_human_review": sum(item["requires_human_review"] for item in results),
               **{level: levels[level] for level in ("exact", "strong", "contextual", "insufficient")},
               "table_groups_detected": sum(item["table_model"]["comparison"].get("applicable", False) for item in results),
               "tables_replaced": sum(item["table_model"]["comparison"].get("table_replaced", False) for item in results),
               "table_cells_localized": sum(len(item["table_changes"]) for item in results),
               "table_rows_inserted": sum(len(item["inserted_table_rows"]) for item in results if not item["table_model"]["comparison"].get("table_replaced")),
               "table_rows_removed": sum(len(item["removed_table_rows"]) for item in results if not item["table_model"]["comparison"].get("table_replaced")),
               "table_rows_in_replacements": sum(len(item["inserted_table_rows"]) + len(item["removed_table_rows"]) for item in results if item["table_model"]["comparison"].get("table_replaced")),
               "entities_localized_exact": sum(len(item["localized_entities_left"]) + len(item["localized_entities_right"]) for item in results),
               "entities_location_uncertain": sum(item["entity_location_uncertain"] for item in results),
               "numbers_with_reliable_context": sum(len(item["numeric_context_changes"]) for item in results),
               "llm_calls": 0}
    return {"schema_version": SCHEMA_VERSION, "kind": "stage_comparison_semantic_diff_v6a1_pilot",
            "settings": {"padding_pt": padding_pt, "llm_used": False, "findings_created": False,
                         "previous_stages_changed": False, "pilot_selection_changed": False},
            "items": results, "summary": summary}


def write_report(destination: str | Path, report: dict[str, Any]) -> tuple[Path, Path]:
    destination = Path(destination); json_path, md_path = destination / "semantic_diff.json", destination / "semantic_diff.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Этап 6А.1 — улучшенное детерминированное «Было → Стало»", "",
             "Те же 12 групп. LLM не вызывался; старый `semantic_diff_v6a` и этапы 1–5 не изменялись.", "",
             "| V2↔V3 | Group | Evidence | Было | Стало | Review | Изменение относительно 6А |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    for item in report["items"]:
        old = item["old_result"]
        improvement = ", ".join(item["comparison_to_v6a"]["improvements"]) or "уточнён confidence"
        lines.append(f"| {item['left_page']}↔{item['right_page']} | `{item['group_id']}` | {item['evidence_level']} | {item['before'][:180]} | {item['after'][:180]} | {'да' if item['requires_human_review'] else 'нет'} | {improvement} |")
        lines += ["", f"## V2 {item['left_page']} ↔ V3 {item['right_page']} / {item['group_id']}", "",
                  f"- Старый результат: `{old.get('change_kind')}`; confidence {old.get('confidence')}; review {old.get('requires_human_review')}. {old.get('change_summary') or '—'}",
                  f"- Новый результат: `{item['change_kind']}`; evidence `{item['evidence_level']}`; confidence {item['confidence']}; review {item['requires_human_review']}.",
                  f"- Было: {item['before'][:1000]}", f"- Стало: {item['after'][:1000]}", f"- Описание: {item['change_summary']}",
                  f"- Что улучшилось: {improvement}.",
                  f"- Табличные ячейки/добавленные строки/удалённые строки: {len(item['table_changes'])}/{len(item['inserted_table_rows'])}/{len(item['removed_table_rows'])}.",
                  f"- Локализованные Entities V2/V3: {len(item['localized_entities_left'])}/{len(item['localized_entities_right'])}; uncertain: {item['entity_location_uncertain']}.",
                  f"- Числа с надёжным label/context: {len(item['numeric_context_changes'])}.",
                  f"- Crops: `{item['diagnostics']['left_crop']}`, `{item['diagnostics']['right_crop']}`, `{item['diagnostics']['overlay']}`", ""]
    lines += ["## Сводка", "", *[f"- {key}: {value}" for key, value in report["summary"].items()], ""]
    _atomic_write(md_path, "\n".join(lines)); return json_path, md_path


__all__ = ["normalize_number", "build_table_model", "match_table_rows", "compare_table_models",
           "localize_entities", "number_contexts", "match_numeric_contexts", "analyze_group",
           "run_pilot", "write_report"]
