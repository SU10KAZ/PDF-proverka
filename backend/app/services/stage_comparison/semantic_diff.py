"""Этап 6А: локальное смысловое объяснение уже найденных change groups.

Этот модуль принципиально не ищет новые различия. Входом служит неизменяемый
отчёт 5Б.4, а каждый вывод обязан ссылаться на evidence внутри выбранной
группы. LLM является опциональным интерпретатором локального контекста и не
может расширять геометрию или добавлять неподтверждённые факты.
"""
from __future__ import annotations

import difflib
import json
import math
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Callable

import cv2
import fitz
import numpy as np

from .sheet_alignment import transform_bbox


SCHEMA_VERSION = 1
DEFAULT_PADDING_PT = 12.0
LLMRunner = Callable[[str, dict[str, Any]], dict[str, Any]]

# Репрезентативная, заранее фиксированная выборка. Она не зависит от полученного
# смыслового ответа и потому не подгоняет пилот под красивый результат.
PILOT_SELECTION = (
    (7, 6, "group_017", "крупная спецификация/таблица: text + vector"),
    (8, 7, "group_001", "локальное изменение обычного текста или числа"),
    (8, 7, "group_008", "смешанная область text + vector"),
    (10, 9, "group_001", "крупное изменение плана, только vector"),
    (10, 9, "group_003", "изображение в зоне штампа"),
    (12, 11, "group_001", "табличная область с множеством текстовых значений"),
    (12, 11, "group_002", "сложный графический блок text + vector"),
    (13, 12, "group_003", "несколько локальных текстовых значений"),
    (14, 13, "group_001", "спецификация: крупная область text + vector"),
    (14, 13, "group_003", "отдельная текстово-векторная группа штампа"),
    (15, 14, "group_001", "небольшой векторный фрагмент"),
    (18, 17, "group_008", "локальная группа внутри большой таблицы"),
)

_SPACE = re.compile(r"\s+")
_ENTITY_SPLIT = re.compile(r"[,;\n]+")


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _clean(value: Any) -> str:
    return _SPACE.sub(" ", str(value or "")).strip()


def _norm(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-яё.+×xх/%-]+", "", _clean(value).lower())


def _bbox(value: Any) -> list[float] | None:
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    try:
        x0, y0, x1, y1 = map(float, value)
    except (TypeError, ValueError):
        return None
    return [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]


def _intersects(first: Any, second: Any) -> bool:
    a, b = _bbox(first), _bbox(second)
    return bool(a and b and a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3])


def _intersection_ratio(inner: Any, outer: Any) -> float:
    a, b = _bbox(inner), _bbox(outer)
    if not a or not b:
        return 0.0
    area = max(0.0, a[2] - a[0]) * max(0.0, a[3] - a[1])
    overlap = max(0.0, min(a[2], b[2]) - max(a[0], b[0])) * max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    return round(overlap / area, 6) if area else 0.0


def _pad_bbox(value: list[float], padding: float, width: float, height: float) -> list[float]:
    x0, y0, x1, y1 = value
    return [round(max(0.0, x0 - padding), 6), round(max(0.0, y0 - padding), 6),
            round(min(width, x1 + padding), 6), round(min(height, y1 + padding), 6)]


def _page(document: dict, page_number: int) -> dict:
    for item in document.get("pages") or []:
        if int(item.get("pdf_page") or item.get("page") or (int(item.get("page_index", -1)) + 1)) == page_number:
            return item
    raise ValueError(f"prepared_page_missing:{page_number}")


def _words(page: fitz.Page, matrix: Any | None = None) -> list[dict[str, Any]]:
    result = []
    for row in page.get_text("words", sort=True) or []:
        box = [float(value) for value in row[:4]]
        if matrix is not None:
            box = transform_bbox(matrix, box)
        result.append({
            "text": str(row[4]), "bbox": [round(value, 6) for value in box],
            "block": int(row[5]), "line": int(row[6]), "word": int(row[7]),
        })
    return result


def _local_lines(words: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for word in words:
        grouped.setdefault((word["block"], word["line"]), []).append(word)
    result = []
    for key in sorted(grouped):
        items = sorted(grouped[key], key=lambda item: (item["bbox"][0], item["word"]))
        boxes = [item["bbox"] for item in items]
        result.append({
            "text": " ".join(item["text"] for item in items),
            "bbox": [min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes)],
        })
    return result


def _entities(block: dict) -> list[str]:
    raw = block.get("entities") or (block.get("graphic_description") or {}).get("entities") or ""
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict):
        values = [value for value in raw.values() if isinstance(value, (str, int, float))]
    else:
        values = _ENTITY_SPLIT.split(str(raw))
    unique: dict[str, str] = {}
    for value in values:
        cleaned = _clean(value)
        if cleaned and _norm(cleaned):
            unique.setdefault(_norm(cleaned), cleaned)
    return list(unique.values())


def _block_context(block: dict, group_bbox: list[float], *, mapped_bbox: list[float] | None = None,
                   local_text: str = "") -> dict[str, Any]:
    box = mapped_bbox or _bbox(block.get("bbox_pdf_visual")) or [0, 0, 0, 0]
    graphic = block.get("graphic_description") or {}
    entities = _entities(block)
    # Entities из описания большого блока считаются локальными только когда их
    # значение действительно встретилось в текстовом слое данного crop.
    localized = [entity for entity in entities if _norm(entity) and _norm(entity) in _norm(local_text)]
    return {
        "block_id": block.get("block_id"), "type": block.get("type"),
        "semantic_type": block.get("semantic_type"), "bbox": box,
        "group_overlap_of_block": _intersection_ratio(box, group_bbox),
        "text_excerpt": _clean(block.get("text"))[:1000],
        "graphic_summary": _clean(graphic.get("summary"))[:1200],
        "graphic_description": _clean(graphic.get("description"))[:2400],
        "entities": entities, "localized_entities": localized,
        "verification": _clean(block.get("verification") or graphic.get("verification"))[:1000],
        "whole_block_change_not_assumed": True,
    }


def _evidence_for_group(item: dict, group: dict) -> list[dict[str, Any]]:
    atomic_ids = set(group.get("atomic_region_ids") or [])
    evidence_ids: set[str] = set()
    allowed_kinds: set[str] = set()
    for region in item.get("atomic_regions") or []:
        if region.get("region_id") in atomic_ids:
            evidence_ids.update(str(value) for value in region.get("evidence_ids") or [])
            allowed_kinds.update(str(value) for value in region.get("change_types") or [])
    result, seen = [], set()
    for bucket, entries in (item.get("evidence") or {}).items():
        for index, entry in enumerate(entries or []):
            if allowed_kinds and str(entry.get("kind")) not in allowed_kinds:
                continue
            evidence_id = str(entry.get("evidence_id") or entry.get("id") or f"{bucket}_{index}")
            if evidence_ids and evidence_id not in evidence_ids:
                continue
            if not evidence_ids and not _intersects(entry.get("bbox"), group.get("bbox")):
                continue
            key = (bucket, evidence_id)
            if key in seen:
                continue
            seen.add(key)
            copied = dict(entry)
            copied["evidence_id"] = evidence_id
            copied["bucket"] = bucket
            result.append(copied)
    # В старом 5Б.4 часть image/text IDs была перенумерована при rebuild.
    # Без изменения исходного отчёта восстанавливаем evidence только по двум
    # независимым ограничениям: тот же kind и пересечение с bbox группы.
    if not result:
        for bucket, entries in (item.get("evidence") or {}).items():
            for index, entry in enumerate(entries or []):
                if allowed_kinds and str(entry.get("kind")) not in allowed_kinds:
                    continue
                if not _intersects(entry.get("bbox"), group.get("bbox")):
                    continue
                copied = dict(entry)
                copied["evidence_id"] = str(entry.get("evidence_id") or entry.get("id") or f"{bucket}_{index}")
                copied["bucket"] = bucket
                copied["recovered_by"] = "kind_and_group_bbox_due_to_v5b4_id_mismatch"
                result.append(copied)
    return sorted(result, key=lambda entry: (entry["bucket"], entry["evidence_id"]))


def _local_word_evidence(left_words: list[dict[str, Any]], right_words: list[dict[str, Any]],
                         group_bbox: list[float]) -> list[dict[str, Any]]:
    """Точный multiset diff слов только внутри bbox, когда raw IDs ненадёжны."""
    left = [word for word in left_words if _intersects(word["bbox"], group_bbox) and _norm(word["text"])]
    right = [word for word in right_words if _intersects(word["bbox"], group_bbox) and _norm(word["text"])]
    left_by: dict[str, list[dict]] = {}
    right_by: dict[str, list[dict]] = {}
    for word in left:
        left_by.setdefault(_norm(word["text"]), []).append(word)
    for word in right:
        right_by.setdefault(_norm(word["text"]), []).append(word)
    for key in set(left_by) & set(right_by):
        cancel = min(len(left_by[key]), len(right_by[key]))
        del left_by[key][:cancel]
        del right_by[key][:cancel]
    result = []
    for key in sorted(left_by):
        for index, word in enumerate(left_by[key]):
            result.append({"evidence_id": f"local_left_{key}_{index}", "kind": "text", "change": "removed",
                           "bbox": word["bbox"], "left_value": word["text"], "left_ref": f"pdf_word:{word['block']}:{word['line']}:{word['word']}",
                           "bucket": "local_pdf_words", "recovered_by": "local_bbox_word_multiset"})
    for key in sorted(right_by):
        for index, word in enumerate(right_by[key]):
            result.append({"evidence_id": f"local_right_{key}_{index}", "kind": "text", "change": "added",
                           "bbox": word["bbox"], "right_value": word["text"], "right_ref": f"pdf_word:{word['block']}:{word['line']}:{word['word']}",
                           "bucket": "local_pdf_words", "recovered_by": "local_bbox_word_multiset"})
    return result


def _evidence_ref(entry: dict, side: str, page: int) -> dict[str, Any]:
    value = entry.get(f"{side}_value")
    return {
        "evidence_id": entry.get("evidence_id"), "page": page,
        "bbox": entry.get("bbox"), "quote": _clean(value) or None,
        "reference": entry.get(f"{side}_ref") or entry.get("representative_ref"),
        "kind": entry.get("kind"),
    }


def _pair_text_entries(evidence: list[dict]) -> list[dict[str, Any]]:
    text = [entry for entry in evidence if entry.get("kind") == "text"]
    changes = []
    removed, added = [], []
    for entry in text:
        change = str(entry.get("change") or "changed")
        before, after = _clean(entry.get("left_value")), _clean(entry.get("right_value"))
        if before and after:
            # Этап 6 объясняет значение, а не повторяет геометрический diff.
            # Одинаковое слово с немного иной позицией не является
            # доказанным смысловым «Было → Стало».
            if _norm(before) == _norm(after):
                continue
            changes.append({"kind": "changed", "before": before, "after": after,
                            "left": entry, "right": entry})
        elif before:
            removed.append(entry)
        elif after:
            added.append(entry)
    # Сначала взаимно погасить одинаковые removed/added tokens: это перенос,
    # а не изменение значения. Сохраняем только избыточные экземпляры.
    removed_by_norm: dict[str, list[dict]] = {}
    added_by_norm: dict[str, list[dict]] = {}
    for entry in removed:
        removed_by_norm.setdefault(_norm(entry.get("left_value")), []).append(entry)
    for entry in added:
        added_by_norm.setdefault(_norm(entry.get("right_value")), []).append(entry)
    for key in set(removed_by_norm) & set(added_by_norm):
        cancel = min(len(removed_by_norm[key]), len(added_by_norm[key]))
        del removed_by_norm[key][:cancel]
        del added_by_norm[key][:cancel]
    removed = [entry for key in sorted(removed_by_norm) for entry in removed_by_norm[key]]
    added = [entry for key in sorted(added_by_norm) for entry in added_by_norm[key]]

    # Pair nearby removed/added values. The raw detector already proved both
    # sides changed; this step merely turns two local facts into before→after.
    while removed and added:
        best = None
        for li, left in enumerate(removed):
            lb = _bbox(left.get("bbox")) or [0, 0, 0, 0]
            for ri, right in enumerate(added):
                rb = _bbox(right.get("bbox")) or [0, 0, 0, 0]
                distance = math.hypot((lb[0] + lb[2] - rb[0] - rb[2]) / 2, (lb[1] + lb[3] - rb[1] - rb[3]) / 2)
                shape = abs(len(_clean(left.get("left_value"))) - len(_clean(right.get("right_value")))) * 2
                candidate = (distance + shape, li, ri)
                if best is None or candidate < best:
                    best = candidate
        _, li, ri = best
        left, right = removed.pop(li), added.pop(ri)
        changes.append({"kind": "changed", "before": _clean(left.get("left_value")),
                        "after": _clean(right.get("right_value")), "left": left, "right": right})
    changes.extend({"kind": "removed", "before": _clean(entry.get("left_value")), "after": "",
                    "left": entry, "right": None} for entry in removed)
    changes.extend({"kind": "added", "before": "", "after": _clean(entry.get("right_value")),
                    "left": None, "right": entry} for entry in added)
    return [item for item in changes if item["before"] or item["after"]]


def _entity_changes(left_entities: list[str], right_entities: list[str]) -> list[dict[str, str]]:
    left = {_norm(value): value for value in left_entities if _norm(value)}
    right = {_norm(value): value for value in right_entities if _norm(value)}
    result = []
    for key in sorted(set(left) - set(right)):
        choices = difflib.get_close_matches(key, list(set(right) - set(left)), n=1, cutoff=.58)
        if choices:
            other = choices[0]
            result.append({"status": "same_entity_changed", "before": left[key], "after": right[other]})
            right.pop(other, None)
        else:
            result.append({"status": "removed", "before": left[key], "after": ""})
    matched_after = {_norm(item["after"]) for item in result if item["after"]}
    for key in sorted(set(right) - set(left) - matched_after):
        result.append({"status": "added", "before": "", "after": right[key]})
    return result


def analyze_local_evidence(evidence: list[dict[str, Any]], *, left_page: int = 1, right_page: int = 1,
                           left_entities: list[str] | None = None, right_entities: list[str] | None = None,
                           semantic_types: list[str] | None = None, region_role: str = "drawing") -> dict[str, Any]:
    """Чистый deterministic first-level; используется и интеграцией, и тестами."""
    text_changes = _pair_text_entries(evidence)
    entity_changes = _entity_changes(left_entities or [], right_entities or [])
    before_evidence, after_evidence = [], []
    for change in text_changes:
        if change["left"]:
            before_evidence.append(_evidence_ref(change["left"], "left", left_page))
        if change["right"]:
            after_evidence.append(_evidence_ref(change["right"], "right", right_page))
    kinds = {item["kind"] for item in text_changes}
    if text_changes:
        before_values = [item["before"] for item in text_changes if item["before"]]
        after_values = [item["after"] for item in text_changes if item["after"]]
        before = "; ".join(before_values) if before_values else "отсутствует"
        after = "; ".join(after_values) if after_values else "отсутствует"
        if kinds == {"added"}:
            kind, summary = "added", f"Добавлено: {after}"
        elif kinds == {"removed"}:
            kind, summary = "removed", f"Удалено: {before}"
        elif kinds == {"moved"}:
            kind, summary = "reconfigured", f"Текст перемещён без доказанного изменения значения: {before}"
        else:
            kind, summary = "changed", f"Изменено: {before} → {after}"
        if kind == "added":
            requires_review = not after_evidence
        elif kind == "removed":
            requires_review = not before_evidence
        else:
            requires_review = not before_evidence or not after_evidence
        if kind == "changed" and len(text_changes) > 12:
            # Значения доказаны, но для большой таблицы greedy word pairing не
            # доказывает строку/колонку — это обязательно видно оператору.
            requires_review = True
        confidence = (.82 if len(text_changes) > 12 else .96) if not requires_review else .80
        source = "deterministic"
    elif entity_changes:
        before_values = [item["before"] for item in entity_changes if item["before"]]
        after_values = [item["after"] for item in entity_changes if item["after"]]
        before = "; ".join(before_values) or "отсутствует"
        after = "; ".join(after_values) or "отсутствует"
        kind, summary, confidence = "changed", f"Локальные сущности: {before} → {after}", .78
        requires_review, source = True, "graphic_description"
    else:
        vector = [item for item in evidence if item.get("kind") == "vector"]
        images = [item for item in evidence if item.get("kind") == "image"]
        if images:
            image_changes = {str(item.get("change") or "changed") for item in images}
            if image_changes == {"added"}:
                before, after, kind = "отсутствует", "добавлено изображение", "added"
                summary = "Доказано локальное добавление изображения; содержание требует визуальной проверки."
            elif image_changes == {"removed"}:
                before, after, kind = "изображение удалено", "отсутствует", "removed"
                summary = "Доказано локальное удаление изображения; содержание требует визуальной проверки."
            else:
                before, after, kind = "изображение до изменения", "изображение после изменения", "changed"
                summary = "Доказано локальное изменение изображения; содержание требует визуальной проверки."
        elif vector:
            before, after = "геометрия до изменения", "геометрия после изменения"
            summary, kind = "Доказано локальное векторное изменение; смысл не подтверждён текстом или сущностями.", "reconfigured"
        else:
            before = after = "недостаточно локальных данных"
            summary, kind = "Недостаточно evidence для формулировки «Было → Стало».", "uncertain"
        refs = [_evidence_ref(item, "left", left_page) for item in evidence
                if item.get("left_ref") or str(item.get("change")) in {"changed", "removed"}]
        after_refs = [_evidence_ref(item, "right", right_page) for item in evidence
                      if item.get("right_ref") or str(item.get("change")) in {"changed", "added"}]
        before_evidence, after_evidence = refs, after_refs
        confidence, requires_review, source = (.55 if evidence else .0), True, "deterministic"
    return {
        "before": before, "after": after, "change_summary": summary, "change_kind": kind,
        "source": source, "confidence": round(confidence, 2), "requires_human_review": requires_review,
        "before_evidence": before_evidence, "after_evidence": after_evidence,
        "structured_changes": text_changes,
        "entity_matches": entity_changes, "semantic_types": sorted(set(semantic_types or [])),
        "stamp_change": region_role == "stamp",
    }


def build_llm_prompt(group_context: dict[str, Any], deterministic: dict[str, Any]) -> str:
    allowed = sorted({str(item.get("evidence_id")) for item in group_context.get("evidence") or []})
    payload = {"group": group_context, "deterministic_result": deterministic, "allowed_evidence_ids": allowed}
    return (
        "Область изменения уже найдена алгоритмом. Не ищи изменения вне предоставленной области. "
        "Определи только смысл доказанного локального изменения. Не оценивай влияние, стоимость, "
        "критичность или severity. Верни только JSON с before, after, change_summary, change_kind, "
        "confidence, requires_human_review, before_evidence_ids, after_evidence_ids. Любой факт должен "
        "ссылаться только на allowed_evidence_ids; иначе ответ будет отклонён.\n\n" +
        json.dumps(payload, ensure_ascii=False, sort_keys=True)
    )


def validate_llm_result(candidate: dict[str, Any], group_context: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(candidate, dict):
        return False, "response_not_object"
    allowed = {str(item.get("evidence_id")) for item in group_context.get("evidence") or []}
    before_ids = {str(value) for value in candidate.get("before_evidence_ids") or []}
    after_ids = {str(value) for value in candidate.get("after_evidence_ids") or []}
    if not before_ids.issubset(allowed) or not after_ids.issubset(allowed):
        return False, "evidence_outside_change_group"
    kind = str(candidate.get("change_kind") or "uncertain")
    if kind not in {"changed", "added", "removed", "reconfigured", "uncertain"}:
        return False, "invalid_change_kind"
    if kind not in {"added", "uncertain"} and not before_ids:
        return False, "before_has_no_local_evidence"
    if kind not in {"removed", "uncertain"} and not after_ids:
        return False, "after_has_no_local_evidence"
    if not _clean(candidate.get("change_summary")):
        return False, "empty_summary"
    try:
        confidence = float(candidate.get("confidence", 0))
    except (TypeError, ValueError):
        return False, "invalid_confidence"
    if not 0 <= confidence <= 1:
        return False, "invalid_confidence"
    return True, "ok"


def _json_object(raw: str) -> dict[str, Any]:
    """Разобрать Claude CLI envelope и единственный JSON-объект ответа."""
    value: Any = json.loads(raw)
    if isinstance(value, dict) and isinstance(value.get("result"), str):
        body = value["result"].strip()
        if body.startswith("```"):
            body = re.sub(r"^```(?:json)?\s*|\s*```$", "", body, flags=re.IGNORECASE)
        value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("llm_response_not_object")
    return value


def resolve_provider_runner(work_dir: str | Path) -> tuple[LLMRunner | None, dict[str, Any]]:
    """Опциональный существующий Claude Code provider, только при ENV opt-in."""
    from . import text_llm_provider

    provider, config = text_llm_provider.resolve_provider()
    meta = {"enabled": config.enabled, "provider": config.provider, "model": config.model}
    if provider is None:
        meta["status"] = "disabled"
        return None, meta
    available, reason = provider.check_availability()
    if not available:
        meta.update({"status": "provider_not_available", "reason": reason})
        return None, meta

    def runner(prompt: str, _context: dict[str, Any]) -> dict[str, Any]:
        if len(prompt) > config.max_chars:
            raise ValueError("local_prompt_exceeds_configured_limit")
        result = provider.invoke(
            system_prompt=(
                "Ты интерпретируешь только уже доказанное локальное изменение. "
                "Запрещено добавлять факты или evidence вне переданного change group."
            ),
            user_prompt=prompt, model=config.model, timeout_sec=config.timeout_sec,
            work_dir=Path(work_dir),
        )
        if result.status != "done":
            raise RuntimeError(f"llm_provider_{result.status}:{result.error or ''}")
        return _json_object(result.raw_response)

    meta["status"] = "available"
    return runner, meta


def _apply_llm(candidate: dict, context: dict, deterministic: dict) -> dict:
    valid, reason = validate_llm_result(candidate, context)
    if not valid:
        result = dict(deterministic)
        result["llm_validation"] = {"accepted": False, "reason": reason}
        result["requires_human_review"] = True
        return result
    evidence_by_id = {str(item.get("evidence_id")): item for item in context.get("evidence") or []}
    result = dict(deterministic)
    for field in ("before", "after", "change_summary", "change_kind", "requires_human_review"):
        if field in candidate:
            result[field] = candidate[field]
    result["confidence"] = round(float(candidate.get("confidence", 0)), 2)
    result["source"] = "llm"
    result["before_evidence"] = [evidence_by_id[value] for value in map(str, candidate.get("before_evidence_ids") or [])]
    result["after_evidence"] = [evidence_by_id[value] for value in map(str, candidate.get("after_evidence_ids") or [])]
    result["llm_validation"] = {"accepted": True, "reason": "ok"}
    return result


def _crop(page: fitz.Page, bbox: list[float], path: Path, *, dpi: int = 96) -> np.ndarray:
    rect = fitz.Rect(*bbox) & page.rect
    if rect.is_empty or rect.width < 1 or rect.height < 1:
        raise ValueError("empty_crop")
    pix = page.get_pixmap(clip=rect, matrix=fitz.Matrix(dpi / 72, dpi / 72), alpha=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    pix.save(str(path))
    image = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    return image[:, :, :3]


def _render_diagnostics(left_page: fitz.Page, right_page: fitz.Page, group_bbox: list[float], matrix: Any,
                        padding: float, output: Path, stem: str) -> dict[str, str]:
    left_box = _pad_bbox(group_bbox, padding, left_page.rect.width, left_page.rect.height)
    inverse = np.linalg.inv(np.asarray(matrix, dtype=float))
    right_box = transform_bbox(inverse, left_box)
    right_box = _pad_bbox(right_box, 0, right_page.rect.width, right_page.rect.height)
    left_path, right_path, overlay_path = output / f"{stem}_v2.png", output / f"{stem}_v3.png", output / f"{stem}_overlay.png"
    left_image = _crop(left_page, left_box, left_path)
    right_image = _crop(right_page, right_box, right_path)
    right_image = cv2.resize(right_image, (left_image.shape[1], left_image.shape[0]), interpolation=cv2.INTER_AREA)
    overlay = cv2.addWeighted(left_image, .5, right_image, .5, 0)
    cv2.imwrite(str(overlay_path), cv2.cvtColor(overlay, cv2.COLOR_RGB2BGR))
    return {"left_crop": str(left_path), "right_crop": str(right_path), "overlay": str(overlay_path)}


def _context_for_group(item: dict, group: dict, left_document: dict, right_document: dict,
                       left_pdf: fitz.Document, right_pdf: fitz.Document, *, padding: float) -> dict[str, Any]:
    left_number, right_number = int(item["left_page"]), int(item["right_page"])
    left_page, right_page = left_pdf[left_number - 1], right_pdf[right_number - 1]
    matrix = (item.get("alignment") or {}).get("transform", {}).get("matrix")
    if not matrix:
        raise ValueError(f"alignment_matrix_missing:{left_number}:{right_number}")
    group_box = _bbox(group.get("bbox"))
    if not group_box:
        raise ValueError(f"group_bbox_missing:{group.get('group_id')}")
    padded = _pad_bbox(group_box, padding, left_page.rect.width, left_page.rect.height)
    left_words = [word for word in _words(left_page) if _intersects(word["bbox"], padded)]
    right_words = [word for word in _words(right_page, matrix) if _intersects(word["bbox"], padded)]
    left_text, right_text = " ".join(word["text"] for word in left_words), " ".join(word["text"] for word in right_words)
    left_prepared, right_prepared = _page(left_document, left_number), _page(right_document, right_number)
    left_blocks = [_block_context(block, group_box, local_text=left_text) for block in left_prepared.get("blocks") or [] if _intersects(block.get("bbox_pdf_visual"), padded)]
    right_blocks = []
    for block in right_prepared.get("blocks") or []:
        raw = _bbox(block.get("bbox_pdf_visual"))
        mapped = transform_bbox(matrix, raw) if raw else None
        if mapped and _intersects(mapped, padded):
            right_blocks.append(_block_context(block, group_box, mapped_bbox=mapped, local_text=right_text))
    atomic_ids = set(group.get("atomic_region_ids") or [])
    atomic = [region for region in item.get("atomic_regions") or [] if region.get("region_id") in atomic_ids]
    evidence = _evidence_for_group(item, group)
    left_graphic = [block for block in left_blocks if block.get("graphic_description") or block.get("entities")]
    right_graphic = [block for block in right_blocks if block.get("graphic_description") or block.get("entities")]
    return {
        "left_page": left_number, "right_page": right_number, "bbox_v2": group_box,
        "padded_bbox_v2": padded, "padding_pt": padding, "atomic_regions": atomic,
        "evidence": evidence,
        "left": {"page": left_number, "words": left_words, "lines": _local_lines(left_words), "text": left_text, "blocks": left_blocks,
                 "vector_differences": [entry for entry in evidence if entry.get("kind") == "vector" and (entry.get("left_ref") or entry.get("change") in {"changed", "removed"})],
                 "images": [entry for entry in evidence if entry.get("kind") == "image" and (entry.get("left_ref") or entry.get("change") in {"changed", "removed"})],
                 "graphic_descriptions": left_graphic,
                 "entities": [entity for block in left_blocks for entity in block.get("localized_entities") or []],
                 "verification": [block["verification"] for block in left_blocks if block.get("verification")]},
        "right": {"page": right_number, "words_v2_coordinates": right_words, "lines": _local_lines(right_words), "text": right_text, "blocks": right_blocks,
                  "vector_differences": [entry for entry in evidence if entry.get("kind") == "vector" and (entry.get("right_ref") or entry.get("change") in {"changed", "added"})],
                  "images": [entry for entry in evidence if entry.get("kind") == "image" and (entry.get("right_ref") or entry.get("change") in {"changed", "added"})],
                  "graphic_descriptions": right_graphic,
                  "entities": [entity for block in right_blocks for entity in block.get("localized_entities") or []],
                  "verification": [block["verification"] for block in right_blocks if block.get("verification")]},
    }


def run_pilot(left_pdf_path: str | Path, right_pdf_path: str | Path, left_document: dict,
              right_document: dict, change_detection: dict, destination: str | Path, *,
              padding_pt: float = DEFAULT_PADDING_PT, llm_runner: LLMRunner | None = None,
              max_llm_calls: int = 1,
              selection: tuple[tuple[int, int, str, str], ...] = PILOT_SELECTION) -> dict[str, Any]:
    destination = Path(destination)
    diagnostics = destination / "diagnostics"
    by_pair = {(int(item["left_page"]), int(item["right_page"])): item for item in change_detection.get("items") or []}
    results = []
    llm_calls = 0
    with fitz.open(left_pdf_path) as left_pdf, fitz.open(right_pdf_path) as right_pdf:
        for left_number, right_number, group_id, reason in selection:
            item = by_pair.get((left_number, right_number))
            if item is None:
                raise ValueError(f"pilot_pair_missing:{left_number}:{right_number}")
            group = next((value for value in item.get("change_groups") or [] if value.get("group_id") == group_id), None)
            if group is None:
                raise ValueError(f"pilot_group_missing:{left_number}:{right_number}:{group_id}")
            context = _context_for_group(item, group, left_document, right_document, left_pdf, right_pdf, padding=padding_pt)
            if not _pair_text_entries(context["evidence"]):
                local_evidence = _local_word_evidence(
                    context["left"]["words"], context["right"]["words_v2_coordinates"], context["bbox_v2"],
                )
                if local_evidence:
                    context["evidence"].extend(local_evidence)
                    context["evidence"] = sorted(context["evidence"], key=lambda entry: (entry.get("bucket", ""), entry.get("evidence_id", "")))
            left_entities = [entity for block in context["left"]["blocks"] for entity in block["localized_entities"]]
            right_entities = [entity for block in context["right"]["blocks"] for entity in block["localized_entities"]]
            semantic_types = [block.get("semantic_type") for side in ("left", "right") for block in context[side]["blocks"] if block.get("semantic_type")]
            result = analyze_local_evidence(context["evidence"], left_page=left_number, right_page=right_number,
                                            left_entities=left_entities, right_entities=right_entities,
                                            semantic_types=semantic_types, region_role=group.get("region_role") or "drawing")
            # Evidence каждой стороны получает конкретные PreparedDocument
            # blocks. Для entities создаём отдельные block-bound доказательства.
            for evidence_ref in result["before_evidence"]:
                evidence_ref.setdefault("block_ids", [block["block_id"] for block in context["left"]["blocks"] if block.get("block_id")])
            for evidence_ref in result["after_evidence"]:
                evidence_ref.setdefault("block_ids", [block["block_id"] for block in context["right"]["blocks"] if block.get("block_id")])
            if result["source"] == "graphic_description":
                result["before_evidence"] = [{
                    "block_id": block["block_id"], "page": left_number, "bbox": block["bbox"],
                    "entity": entity, "kind": "prepared_entity", "reference": "PreparedDocument.entities",
                } for block in context["left"]["blocks"] for entity in block.get("localized_entities") or []]
                result["after_evidence"] = [{
                    "block_id": block["block_id"], "page": right_number, "bbox": block["bbox"],
                    "entity": entity, "kind": "prepared_entity", "reference": "PreparedDocument.entities",
                } for block in context["right"]["blocks"] for entity in block.get("localized_entities") or []]
            llm_status = "not_needed"
            if result["requires_human_review"] and group.get("region_role") != "stamp":
                if llm_runner is None:
                    llm_status = "skipped_no_runner"
                elif llm_calls >= max(0, int(max_llm_calls)):
                    llm_status = "skipped_pilot_limit"
                else:
                    try:
                        llm_calls += 1
                        candidate = llm_runner(build_llm_prompt(context, result), context)
                        result = _apply_llm(candidate, context, result)
                        llm_status = "accepted" if result.get("source") == "llm" else "rejected"
                    except Exception as exc:  # fail-soft: deterministic result stays authoritative
                        llm_status = f"error:{type(exc).__name__}"
                        result["requires_human_review"] = True
            stem = f"v2_{left_number:03d}_v3_{right_number:03d}_{group_id}"
            visual = _render_diagnostics(left_pdf[left_number - 1], right_pdf[right_number - 1], group["bbox"],
                                         item["alignment"]["transform"]["matrix"], padding_pt, diagnostics, stem)
            results.append({
                "group_id": group_id, "left_page": left_number, "right_page": right_number,
                "bbox": group["bbox"], "region_role": group.get("region_role"),
                "change_types": group.get("change_types") or [], "selection_reason": reason,
                "atomic_region_ids": group.get("atomic_region_ids") or [], "block_ids": group.get("block_ids") or [],
                **result, "llm_status": llm_status, "context": context, "diagnostics": visual,
            })
    counts = Counter(item["source"] for item in results)
    llm_candidates = [item for item in results if item["region_role"] != "stamp" and item["llm_status"] != "not_needed"]
    summary = {
        "selected_groups": len(results), "deterministic": counts["deterministic"],
        "deterministic_resolved": sum(item["source"] == "deterministic" and not item["requires_human_review"] for item in results),
        "graphic_description_entities": counts["graphic_description"], "llm": counts["llm"],
        "llm_candidates": len(llm_candidates), "llm_failed_or_rejected": sum(item["llm_status"].startswith(("error:", "rejected")) for item in results),
        "uncertain": sum(item["requires_human_review"] for item in results),
        "stamp_groups": sum(item["region_role"] == "stamp" for item in results),
    }
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_semantic_diff_v6a_pilot",
        "settings": {"padding_pt": padding_pt, "full_document_sent_to_llm": False,
                     "new_changes_search": False, "findings_created": False,
                     "influence_or_severity_computed": False, "llm_available": llm_runner is not None,
                     "max_llm_calls": max_llm_calls, "llm_calls_attempted": llm_calls},
        "pilot_selection": [{"left_page": a, "right_page": b, "group_id": c, "reason": d} for a, b, c, d in selection],
        "items": results, "summary": summary,
    }


def write_report(destination: str | Path, report: dict[str, Any]) -> tuple[Path, Path]:
    destination = Path(destination)
    json_path, md_path = destination / "semantic_diff.json", destination / "semantic_diff.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Этап 6А — локальное смысловое сравнение change groups", "",
             "Пилот не меняет PreparedDocument, карту листов, change groups, findings или вкладку «Расхождения».", "",
             "## Выборка", ""]
    for item in report.get("items") or []:
        lines.append(f"- V2 {item['left_page']} ↔ V3 {item['right_page']} / `{item['group_id']}` — {item['selection_reason']}")
    for item in report.get("items") or []:
        lines += ["", f"## {item['group_id']} — V2 {item['left_page']} ↔ V3 {item['right_page']}", "",
                  f"- BBox V2: `{item['bbox']}`", f"- Atomic regions: {', '.join(item['atomic_region_ids']) or '—'}",
                  f"- Blocks: {', '.join(item['block_ids']) or '—'}", f"- Роль: {item['region_role']} (штамп анализируется отдельно)",
                  f"- Детерминированно найдено: {len(item.get('structured_changes') or [])} текстовых изменений; {len(item.get('entity_matches') or [])} локальных изменений сущностей.",
                  f"- Было: {item['before'][:900]}{'…' if len(item['before']) > 900 else ''}",
                  f"- Стало: {item['after'][:900]}{'…' if len(item['after']) > 900 else ''}",
                  f"- Описание: {item['change_summary'][:1200]}{'…' if len(item['change_summary']) > 1200 else ''}",
                  f"- Источник: {item['source']}; LLM: {item['llm_status']}", f"- Confidence: {item['confidence']:.2f}",
                  f"- Human review: {'да' if item['requires_human_review'] else 'нет'}",
                  f"- Evidence V2/V3: {len(item['before_evidence'])}/{len(item['after_evidence'])}",
                  f"- Crops: `{item['diagnostics']['left_crop']}`, `{item['diagnostics']['right_crop']}`, `{item['diagnostics']['overlay']}`"]
    lines += ["", "## Сводка", "", *[f"- {key}: {value}" for key, value in report.get("summary", {}).items()], ""]
    _atomic_write(md_path, "\n".join(lines))
    return json_path, md_path


__all__ = [
    "PILOT_SELECTION", "analyze_local_evidence", "build_llm_prompt", "validate_llm_result",
    "resolve_provider_runner", "run_pilot", "write_report",
]
