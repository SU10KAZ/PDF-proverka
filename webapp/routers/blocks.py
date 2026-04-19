"""
REST API для OCR-блоков чертежей.
"""
import json
import re
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from webapp.services.project_service import resolve_project_dir

router = APIRouter(prefix="/api/tiles", tags=["blocks"])


# ─── V4: Человекочитаемая конвертация typed_facts → block summary ───

_TYPE_LABELS = {
    "breaker": "Автомат",
    "line": "Кабельная линия",
    "cable": "Кабельная линия",
    "panel": "Щит",
    "current_transformer": "Трансформатор тока",
    "note": "Примечание",
    "spec_row": "Спецификация",
    "room": "Помещение",
    "other": "Прочее",
}

_ATTR_LABELS = {
    "breaker_model": "модель",
    "breaker_nominal_a": "номинал",
    "cable_mark": "марка",
    "phase_section_mm2": "сечение",
    "phase_count": "жилы",
    "pe_or_n_section_mm2": "PE/N сечение",
    "pe_or_n_count": "PE/N жил",
    "source_panel": "от",
    "destination_panel": "к",
    "note_text": "текст",
    "designation": "обозначение",
    "description": "описание",
    "ct_ratio_primary_a": "первичный ток",
    "ct_accuracy_class": "класс точности",
    "position": "позиция",
    "room_no": "номер",
    "room_name": "наименование помещения",
    "purpose": "назначение",
    "storeys": "этажность",
    "count": "количество",
    "grid_lines": "оси",
    "location": "расположение",
    "requirement_type": "тип ссылки",
    "page": "страница",
    "sheet": "лист",
    "area_m2": "площадь",
    "length_mm": "длина",
    "width_mm": "ширина",
    "height_mm": "высота",
    "depth_mm": "глубина",
    "level": "отметка",
    "section": "сечение",
    "material": "материал",
    "mark": "марка",
    "floor": "этаж",
    "type": "тип",
}

_ATTR_UNITS = {
    "breaker_nominal_a": "А",
    "phase_section_mm2": "мм²",
    "pe_or_n_section_mm2": "мм²",
    "ct_ratio_primary_a": "А",
    "area_m2": " м²",
    "length_mm": " мм",
    "width_mm": " мм",
    "height_mm": " мм",
    "depth_mm": " мм",
    "storeys": " эт.",
}


_TOKEN_LABELS = {
    "grid": "оси",
    "lines": "линии",
    "location": "расположение",
    "requirement": "требование",
    "type": "тип",
    "room": "помещение",
    "name": "наименование",
    "purpose": "назначение",
    "count": "количество",
    "page": "страница",
    "sheet": "лист",
}

_INLINE_ATTR_ORDER = (
    "designation",
    "room_name",
    "room_no",
    "purpose",
    "storeys",
    "description",
    "count",
    "grid_lines",
    "requirement_type",
)

_NOTE_ONLY_VIEW_TYPES = {
    "general_notes",
}


def _normalize_spaces(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _try_parse_json_like(value):
    if not isinstance(value, str):
        return value
    raw = value.strip()
    if not raw or raw[0] not in "{[":
        return value
    try:
        return json.loads(raw)
    except Exception:
        return value


def _humanize_key(key: str) -> str:
    raw = _normalize_spaces(key)
    if not raw:
        return ""
    lower = raw.lower()
    label = _ATTR_LABELS.get(lower)
    if label:
        return label

    tokens = [token for token in re.split(r"[_\-.]+", lower) if token]
    if not tokens:
        return raw

    translated = [_TOKEN_LABELS.get(token, token) for token in tokens]
    label = " ".join(translated)
    return label[0].upper() + label[1:] if label else raw


def _replace_embedded_field_labels(text: str) -> str:
    result = _normalize_spaces(text)
    if not result:
        return ""
    result = re.sub(r"^Прочее\s+", "", result, flags=re.IGNORECASE)
    for key, label in _ATTR_LABELS.items():
        result = re.sub(rf"\b{re.escape(key)}\b(?=\s*:)", label, result, flags=re.IGNORECASE)
    return result


def _format_scalar_value(key: str, value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "да" if value else "нет"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and value.is_integer():
            text = str(int(value))
        else:
            text = str(value)
        unit = _ATTR_UNITS.get(str(key or "").lower(), "")
        return f"{text}{unit}" if unit else text

    text = _replace_embedded_field_labels(str(value))
    if not text:
        return ""
    unit = _ATTR_UNITS.get(str(key or "").lower(), "")
    if unit and not text.endswith(unit):
        return f"{text}{unit}"
    return text


def _flatten_value_pairs(value, path=()):
    value = _try_parse_json_like(value)
    if value is None:
        return []

    if isinstance(value, dict):
        pairs = []
        for child_key, child_value in value.items():
            pairs.extend(_flatten_value_pairs(child_value, path + (str(child_key),)))
        return pairs

    if isinstance(value, list):
        if not value:
            return []
        pairs = []
        scalars = []
        for item in value[:10]:
            parsed_item = _try_parse_json_like(item)
            if isinstance(parsed_item, (dict, list)):
                pairs.extend(_flatten_value_pairs(parsed_item, path))
            else:
                text = _format_scalar_value(path[-1] if path else "", parsed_item)
                if text:
                    scalars.append(text)
        if scalars:
            pairs.insert(0, (path, ", ".join(scalars)))
        return pairs

    text = _format_scalar_value(path[-1] if path else "", value)
    return [(path, text)] if text else []


def _label_from_path(path) -> str:
    parts = []
    for part in path:
        part_text = _normalize_spaces(part)
        if not part_text or part_text.isdigit():
            continue
        parts.append(_humanize_key(part_text))
    if not parts:
        return ""
    head = parts[0].capitalize()
    if len(parts) == 1:
        return head
    return f"{head}: {' / '.join(parts[1:])}"


def _flatten_to_lines(value) -> list[str]:
    lines = []
    for path, text in _flatten_value_pairs(value):
        if not text:
            continue
        label = _label_from_path(path)
        lines.append(f"{label}: {text}" if label else text)
    return lines


def _format_inline_value(value, key: str = "") -> str:
    parsed = _try_parse_json_like(value)
    if isinstance(parsed, (dict, list)):
        return "; ".join(_flatten_to_lines(parsed))
    if isinstance(parsed, str):
        return "; ".join(
            cleaned
            for cleaned in (_normalize_spaces(line) for line in parsed.splitlines())
            if cleaned
        )
    return _format_scalar_value(key, parsed)


def _normalize_entity_caption(caption: str) -> str:
    text = _normalize_spaces(caption)
    if text.startswith("Прочее "):
        return text.split(" ", 1)[1]
    return text


def _entity_title(etype: str, label: str) -> str:
    clean_label = _normalize_spaces(label)
    if etype == "other":
        return clean_label or "Объект"
    type_label = _TYPE_LABELS.get(etype, _humanize_key(etype)).strip()
    title = f"{type_label} {clean_label}".strip()
    return title or type_label or clean_label or "Объект"


def _format_inline_attributes(attrs: dict, limit: int = 3) -> str:
    if not isinstance(attrs, dict):
        return ""

    ordered_keys = []
    seen = set()
    for key in _INLINE_ATTR_ORDER:
        if key in attrs and key not in seen:
            ordered_keys.append(key)
            seen.add(key)
    for key in attrs.keys():
        if key not in seen:
            ordered_keys.append(key)
            seen.add(key)

    parts = []
    for key in ordered_keys:
        value_text = _format_inline_value(attrs.get(key), key)
        if not value_text:
            continue
        parts.append(f"{_humanize_key(key)}: {value_text}")
        if len(parts) >= limit:
            break
    return ", ".join(parts)


def _normalize_summary(summary) -> str:
    parsed = _try_parse_json_like(summary)
    if isinstance(parsed, (dict, list)):
        return "\n".join(_flatten_to_lines(parsed))
    if isinstance(parsed, str):
        lines = [
            cleaned
            for cleaned in (_replace_embedded_field_labels(line) for line in parsed.splitlines())
            if cleaned
        ]
        return "\n".join(lines)
    return _format_scalar_value("", parsed)


def _pairs_to_kv_items(pairs) -> list:
    items = []
    for path, text in pairs:
        if not text:
            continue
        label = _label_from_path(path)
        if label:
            items.append({"key": label, "value": text})
        else:
            items.append(text)
    return items


def _normalize_key_values(items) -> list:
    parsed = _try_parse_json_like(items)
    if parsed is None:
        return []

    if isinstance(parsed, dict):
        return _pairs_to_kv_items(_flatten_value_pairs(parsed))

    if not isinstance(parsed, list):
        text = _format_inline_value(parsed)
        return [text] if text else []

    normalized = []
    for item in parsed:
        parsed_item = _try_parse_json_like(item)
        if parsed_item is None:
            continue

        if isinstance(parsed_item, dict):
            raw_key = parsed_item.get("key") or parsed_item.get("name") or ""
            if "value" in parsed_item or "val" in parsed_item or raw_key:
                key = _normalize_entity_caption(raw_key)
                value = parsed_item.get("value") if "value" in parsed_item else parsed_item.get("val")
                value_text = _format_inline_value(value)
                if key and value_text:
                    normalized.append({"key": key, "value": value_text})
                elif key:
                    normalized.append(key)
                elif value_text:
                    normalized.append(value_text)
                continue

            normalized.extend(_pairs_to_kv_items(_flatten_value_pairs(parsed_item)))
            continue

        if isinstance(parsed_item, list):
            normalized.extend(_pairs_to_kv_items(_flatten_value_pairs(parsed_item)))
            continue

        text = _format_inline_value(parsed_item)
        if text:
            normalized.append(text)

    return normalized


def _normalize_block_info(block_info: dict) -> dict:
    if not isinstance(block_info, dict):
        return block_info
    block_info["summary"] = _normalize_summary(block_info.get("summary"))
    block_info["key_values_read"] = _normalize_key_values(block_info.get("key_values_read"))
    if isinstance(block_info.get("label"), str):
        block_info["label"] = _normalize_spaces(block_info["label"])
    return block_info


def _filter_entities_for_display(entities: list[dict], sheet_type: str = "") -> list[dict]:
    if not entities:
        return []

    normalized_sheet_type = _normalize_spaces(sheet_type).lower()
    if normalized_sheet_type in _NOTE_ONLY_VIEW_TYPES:
        note_entities = [entity for entity in entities if entity.get("type") == "note"]
        return note_entities or entities

    non_note_entities = [entity for entity in entities if entity.get("type") != "note"]
    if non_note_entities:
        return non_note_entities

    return entities


def _format_entity_line(e: dict) -> str:
    """Одна строка описания entity на русском."""
    etype = e.get("type", "other")
    label = e.get("label", "")
    attrs = e.get("attributes", {})
    type_label = _TYPE_LABELS.get(etype, etype)

    if etype == "note":
        text = str(attrs.get("note_text", ""))[:200]
        return f"{type_label}: {text}"

    if etype == "breaker":
        model = attrs.get("breaker_model", "")
        nom = attrs.get("breaker_nominal_a", "")
        pos = attrs.get("position", "")
        parts = [f"{type_label} {label}"]
        if model:
            parts.append(model)
        if nom:
            parts.append(f"{nom}А")
        if pos:
            parts.append(f"({pos})")
        return ": ".join(parts[:2]) + (" " + " ".join(parts[2:]) if len(parts) > 2 else "")

    if etype in ("line", "cable"):
        mark = attrs.get("cable_mark", "")
        section = attrs.get("phase_section_mm2", "")
        src = attrs.get("source_panel", "")
        dst = attrs.get("destination_panel", "")
        parts = [f"{type_label} {label}"]
        if mark:
            parts.append(mark)
        if section:
            parts.append(f"{section} мм²")
        route = ""
        if src and dst:
            route = f" ({src} → {dst})"
        elif dst:
            route = f" (→ {dst})"
        return ": ".join(parts[:2]) + (" " + " ".join(parts[2:]) if len(parts) > 2 else "") + route

    if etype == "panel":
        desc = attrs.get("description", "")
        return f"{type_label} {label}" + (f": {desc}" if desc else "")

    if etype == "current_transformer":
        ratio = attrs.get("ct_ratio_primary_a", "")
        acc = attrs.get("ct_accuracy_class", "")
        parts = [f"ТТ {label}"]
        if ratio:
            parts.append(f"{ratio}А")
        if acc:
            parts.append(f"кл.точн. {acc}")
        return ", ".join(parts)

    if etype == "room":
        room_name = attrs.get("room_name") or attrs.get("room_no") or label
        base = f"Помещение {_normalize_spaces(room_name)}".strip()
        purpose = _format_inline_attributes({"purpose": attrs.get("purpose")}, limit=1)
        return f"{base}: {purpose}" if purpose else base

    base = _entity_title(etype, label)
    details = _format_inline_attributes(attrs)
    return f"{base}: {details}" if details else base


def _v4_block_summary(entities: list[dict]) -> str:
    """Человекочитаемый summary блока из entity_mentions."""
    lines = []
    for e in entities[:15]:
        lines.append(_format_entity_line(e))
    return "\n".join(lines)


def _v4_key_values(entities: list[dict]) -> list[dict]:
    """key_values_read для совместимости с UI — label → атрибуты на русском."""
    result = []
    for e in entities[:20]:
        label = e.get("label", "?")
        etype = e.get("type", "other")
        attrs = e.get("attributes", {})

        # Красивый value
        parts = []
        for attr_name, attr_val in attrs.items():
            if attr_val is None:
                continue
            ru_name = _humanize_key(attr_name)
            val_str = _format_inline_value(attr_val, attr_name)
            if not val_str:
                continue
            parts.append(f"{ru_name}: {val_str}")

        result.append({
            "key": _entity_title(etype, label),
            "value": ", ".join(parts) if parts else "—",
        })
    return result


# ─── OCR-блоки ───

@router.get("/{project_id:path}/blocks")
async def get_blocks(project_id: str):
    """Список image-блоков, сгруппированных по страницам."""
    blocks_dir = resolve_project_dir(project_id) / "_output" / "blocks"
    index_path = blocks_dir / "index.json"
    if not index_path.exists():
        raise HTTPException(404, f"Блоки не найдены для '{project_id}'")

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    # Группируем по страницам
    pages_map: dict[int, list] = {}
    for block in index_data.get("blocks", []):
        page = block.get("page", 0)
        pages_map.setdefault(page, []).append(block)

    pages = []
    for page_num in sorted(pages_map.keys()):
        blocks = pages_map[page_num]
        pages.append({
            "page_num": page_num,
            "block_count": len(blocks),
            "blocks": blocks,
        })

    return {
        "project_id": project_id,
        "total_blocks": index_data.get("total_blocks", 0),
        "total_expected": index_data.get("total_expected", 0),
        "errors": index_data.get("errors", 0),
        "pages": pages,
    }


@router.get("/{project_id:path}/blocks/analysis")
async def get_blocks_analysis(project_id: str):
    """Агрегированные данные анализа блоков из block_batch_*.json или typed_facts_batch_*.json (v4)."""
    output_dir = resolve_project_dir(project_id) / "_output"

    # Legacy: block_batch_*.json
    batch_files = sorted(output_dir.glob("block_batch_*.json"))
    blocks_map = {}
    for bf in batch_files:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                data = json.load(f)
            block_list = data.get("blocks_reviewed") or data.get("block_analyses") or []
            for block_info in block_list:
                bid = block_info.get("block_id", "")
                if bid:
                    blocks_map[bid] = block_info
        except Exception:
            continue

    # V4 fallback: typed_facts_batch_*.json → конвертируем в совместимый формат
    if not blocks_map:
        typed_files = sorted(output_dir.glob("typed_facts_batch_*.json"))
        for tf in typed_files:
            try:
                with open(tf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for mention in data.get("entity_mentions", []):
                    src = mention.get("source_context", {}) or {}
                    bid = src.get("block_id")
                    if not bid:
                        continue
                    if bid not in blocks_map:
                        blocks_map[bid] = {
                            "block_id": bid,
                            "page": src.get("page"),
                            "sheet": src.get("sheet"),
                            "sheet_type": src.get("view_type", ""),
                            "summary": "",
                            "key_values_read": [],
                            "findings": [],
                            "_v4_entities": [],
                        }
                    entry = blocks_map[bid]
                    # Собираем entities
                    entity_type = mention.get("entity_type", "")
                    label = mention.get("normalized_label", "")
                    attrs = {
                        a["name"]: a.get("value_norm") if a.get("value_norm") is not None else a.get("value_raw")
                        for a in mention.get("attributes", [])
                    }
                    entry["_v4_entities"].append({
                        "type": entity_type,
                        "label": label,
                        "attributes": attrs,
                    })
            except Exception:
                continue

        # Генерируем человекочитаемый summary для каждого блока
        for bid, block in blocks_map.items():
            entities = block.pop("_v4_entities", [])
            if entities:
                display_entities = _filter_entities_for_display(entities, block.get("sheet_type", ""))
                block["summary"] = _v4_block_summary(display_entities)
                block["key_values_read"] = _v4_key_values(display_entities)

    # ═══════════════════════════════════════════════════════════════════
    # Классификация всех блоков из index.json для UI
    # ═══════════════════════════════════════════════════════════════════
    # Статусы:
    #   has_findings  — блок проанализирован индивидуально, есть замечания
    #   no_findings   — проанализирован индивидуально, замечаний не выявлено
    #   merged_into   — свёрнут в родительский page/quadrant PNG
    #                    (parent_block_id указывает на родителя)
    #   skipped       — алгоритм решил не включать в анализ
    #                    (не попал ни в batch, ни в чей-то merged_block_ids)
    #
    # Для merged_into блока:
    #   - summary наследуется от parent (одинаковый для всех детей одной страницы)
    #   - original_ocr_label содержит собственный label этого конкретного фрагмента
    #
    # Bridge index.json → classification:
    #   1. Проанализированные (A) уже в blocks_map из block_batch_*.json
    #   2. Merged (B) — из block_batches.json (поле merged_block_ids у parent-блоков)
    #   3. Skipped (C) — всё остальное из index.json
    # ═══════════════════════════════════════════════════════════════════

    # Собираем map: child_block_id → parent_block_id (для статуса merged_into)
    merged_parent_map: dict[str, str] = {}
    batches_path = output_dir / "block_batches.json"
    if batches_path.exists():
        try:
            batches_data = json.loads(batches_path.read_text(encoding="utf-8"))
            for batch in batches_data.get("batches", []):
                for blk in batch.get("blocks", []):
                    parent_bid = blk.get("block_id", "")
                    for child_bid in (blk.get("merged_block_ids") or []):
                        if child_bid:
                            merged_parent_map[child_bid] = parent_bid
        except Exception:
            pass

    # Собираем set блоков, упомянутых в финальных findings (03_findings.json).
    # Блок считается "с замечаниями" если он упомянут в любом поле finding:
    # source_block_ids, related_block_ids или evidence[*].block_id.
    # Это устраняет противоречие: нельзя ставить "Замечаний не выявлено"
    # когда в сплит-обзоре рядом показываются финальные замечания на этот блок.
    blocks_in_findings: set[str] = set()
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        try:
            findings_data = json.loads(findings_path.read_text(encoding="utf-8"))
            for f in findings_data.get("findings", []):
                for bid in (f.get("source_block_ids") or []):
                    if bid: blocks_in_findings.add(bid)
                for bid in (f.get("related_block_ids") or []):
                    if bid: blocks_in_findings.add(bid)
                for ev in (f.get("evidence") or []):
                    bid = ev.get("block_id")
                    if bid: blocks_in_findings.add(bid)
        except Exception:
            pass

    # Классификация A-блоков (проанализированы индивидуально)
    for bid, block in blocks_map.items():
        findings = block.get("findings") or []
        if findings or bid in blocks_in_findings:
            block["status"] = "has_findings"
        else:
            block["status"] = "no_findings"

    # Добавляем B (merged) и C (skipped) из index.json
    index_path = output_dir / "blocks" / "index.json"
    if index_path.exists():
        try:
            index_data = json.loads(index_path.read_text(encoding="utf-8"))
            for ib in index_data.get("blocks", []):
                bid = ib.get("block_id", "")
                if not bid or bid in blocks_map:
                    continue  # уже классифицирован как A

                parent_bid = merged_parent_map.get(bid)
                if parent_bid:
                    # B — свёрнут в родителя
                    parent = blocks_map.get(parent_bid, {})
                    blocks_map[bid] = {
                        "block_id": bid,
                        "page": ib.get("page"),
                        "sheet": parent.get("sheet"),
                        "sheet_type": parent.get("sheet_type", "other"),
                        "summary": parent.get("summary") or "Разобран в составе родительского листа",
                        "key_values_read": [],
                        "findings": [],
                        "status": "merged_into",
                        "parent_block_id": parent_bid,
                        "original_ocr_label": ib.get("ocr_label", ""),
                    }
                else:
                    # C — ни в batch, ни в merged
                    blocks_map[bid] = {
                        "block_id": bid,
                        "page": ib.get("page"),
                        "sheet": None,
                        "sheet_type": "other",
                        "summary": "Без значимого содержимого",
                        "key_values_read": [],
                        "findings": [],
                        "status": "skipped",
                        "is_empty_scope": True,
                        "original_ocr_label": ib.get("ocr_label", ""),
                    }
        except Exception:
            pass

    for block in blocks_map.values():
        _normalize_block_info(block)

    # Сводные счётчики по статусам
    counts = {"has_findings": 0, "no_findings": 0, "merged_into": 0, "skipped": 0}
    for block in blocks_map.values():
        s = block.get("status")
        if s in counts:
            counts[s] += 1

    return {
        "project_id": project_id,
        "total_analyzed": len(blocks_map),
        "counts": counts,
        "blocks": blocks_map,
    }


@router.get("/{project_id:path}/blocks/image/{block_id}")
async def get_block_image(project_id: str, block_id: str):
    """PNG-файл кропнутого блока."""
    block_path = resolve_project_dir(project_id) / "_output" / "blocks" / f"block_{block_id}.png"
    if not block_path.exists():
        raise HTTPException(404, f"Блок {block_id} не найден")
    return FileResponse(str(block_path), media_type="image/png")
