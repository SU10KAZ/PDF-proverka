"""
REST API для OCR-блоков чертежей.
"""
import json
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
}

_ATTR_UNITS = {
    "breaker_nominal_a": "А",
    "phase_section_mm2": "мм²",
    "pe_or_n_section_mm2": "мм²",
    "ct_ratio_primary_a": "А",
}


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
        room_no = attrs.get("room_no", label)
        return f"Помещение {room_no}"

    # Fallback
    return f"{type_label} {label}"


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
        type_label = _TYPE_LABELS.get(etype, etype)

        # Красивый value
        parts = []
        for attr_name, attr_val in attrs.items():
            if attr_val is None:
                continue
            ru_name = _ATTR_LABELS.get(attr_name, attr_name)
            unit = _ATTR_UNITS.get(attr_name, "")
            val_str = f"{attr_val}{unit}" if unit else str(attr_val)
            parts.append(f"{ru_name}: {val_str}")

        result.append({
            "key": f"{type_label} {label}",
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
                    attrs = {a["name"]: a.get("value_norm") or a.get("value_raw")
                             for a in mention.get("attributes", [])}
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
                block["summary"] = _v4_block_summary(entities)
                block["key_values_read"] = _v4_key_values(entities)

    # Для блоков из index.json без анализа — добавить пустую запись
    # чтобы UI не показывал "Данные анализа отсутствуют"
    index_path = output_dir / "blocks" / "index.json"
    if index_path.exists() and blocks_map:
        try:
            index_data = json.loads(index_path.read_text(encoding="utf-8"))
            for ib in index_data.get("blocks", []):
                bid = ib.get("block_id", "")
                if bid and bid not in blocks_map:
                    blocks_map[bid] = {
                        "block_id": bid,
                        "page": ib.get("page"),
                        "sheet": None,
                        "sheet_type": "other",
                        "summary": "Блок не содержит сущностей в scope аудита (v4)",
                        "key_values_read": [],
                        "findings": [],
                    }
        except Exception:
            pass

    return {
        "project_id": project_id,
        "total_analyzed": len(blocks_map),
        "blocks": blocks_map,
    }


@router.get("/{project_id:path}/blocks/image/{block_id}")
async def get_block_image(project_id: str, block_id: str):
    """PNG-файл кропнутого блока."""
    block_path = resolve_project_dir(project_id) / "_output" / "blocks" / f"block_{block_id}.png"
    if not block_path.exists():
        raise HTTPException(404, f"Блок {block_id} не найден")
    return FileResponse(str(block_path), media_type="image/png")
