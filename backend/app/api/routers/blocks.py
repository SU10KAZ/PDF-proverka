"""
REST API для OCR-блоков чертежей.
"""
import asyncio
import json
import re
from pathlib import Path

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    resolve_existing,
)
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response

from backend.app.services.common import block_crop_store, version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.pipeline.stages.block_context.contract import (
    VECTOR_GRAPH_MISSING_MESSAGE,
    decorate_blocks_vector_state,
    load_block_context_summary,
    resolve_blocks_dir,
    resolve_blocks_index,
    source_has_vector_text,
)

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

def _version_output(project_id: str, version_id: Optional[str]):
    try:
        return version_service.resolve_version_output_dir(project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except FileNotFoundError:
        raise HTTPException(404, f"Проект '{project_id}' не найден")


@router.get("/{project_id:path}/blocks")
async def get_blocks(
    project_id: str,
    request: Request,
    version_id: Optional[str] = Query(None, description="Конкретная версия, по умолчанию latest"),
):
    """Список image-блоков, сгруппированных по страницам.

    opt-in/default read canary: `?storage=projects_v2`/header или default-флаг →
    список блоков из projects_v2 (read-only). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_blocks(request, project_id)
    output_dir = _version_output(project_id, version_id)
    index_path = resolve_blocks_index(output_dir)
    if not index_path.exists():
        raise HTTPException(404, f"Блоки не найдены для '{project_id}'")

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    # Этот загрузчик также переводит старый Gemma-summary в единый контракт:
    # OCR/vision-описание не ошибочно считается векторным текстом.
    context_summary = load_block_context_summary(output_dir)
    decorate_blocks_vector_state(index_data.get("blocks") or [], context_summary)

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
        # #10: упавшие при кропе блоки видны в покрытии, а не теряются молча
        "failed_block_ids": index_data.get("failed_block_ids", []),
        "failed_details": index_data.get("failed_details", []),
        "pages": pages,
    }


def _lookup_block_page(output_dir: Path, block_id: str) -> Optional[int]:
    """Найти страницу блока: 01_blocks_analysis.json (v2+legacy) → gemma index."""
    ba = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    if ba.exists():
        try:
            d = json.loads(ba.read_text(encoding="utf-8"))
            for b in (d.get("block_analyses") or d.get("blocks_reviewed") or []):
                if str(b.get("block_id")) == block_id and b.get("page") is not None:
                    return b.get("page")
        except (OSError, json.JSONDecodeError):
            pass
    idx_path = resolve_blocks_index(output_dir)
    if idx_path.exists():
        try:
            idx = json.loads(idx_path.read_text(encoding="utf-8"))
            for b in idx.get("blocks", []):
                if str(b.get("block_id")) == block_id:
                    return b.get("page")
        except (OSError, json.JSONDecodeError):
            pass
    return None


def _block_from_result_json(version_dir: Path, block_id: str) -> dict:
    """Запись блока из 02_work/result.json (Chandra): pdfplumber_text (вектор) + ocr_text (gemma)."""
    rp = version_dir / "02_work" / "result.json"
    if not rp.exists():
        return {}
    try:
        rj = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    def _match(b):
        return isinstance(b, dict) and (str(b.get("id")) == block_id or str(b.get("block_id")) == block_id)

    for pg in rj.get("pages", []):
        for b in (pg.get("blocks") or []):
            if _match(b):
                return b
    for b in (rj.get("blocks") or []):
        if _match(b):
            return b
    return {}


def _neighbor_text_blocks_from_result_json(version_dir: Path, block_id: str) -> list[dict]:
    """Соседние `text`-блоки ТОЙ ЖЕ страницы, что и block_id (без самого блока).

    Скоуп по странице обязателен: одноимённые примечания есть на многих листах, и без
    ограничения страницей дедуп ловил бы дубли с чужих листов.
    """
    rp = version_dir / "02_work" / "result.json"
    if not rp.exists():
        return []
    try:
        rj = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    def _id(b) -> str:
        return str(b.get("id") or b.get("block_id") or "")

    for pg in rj.get("pages", []):
        blocks = pg.get("blocks") or []
        if not any(_id(b) == block_id for b in blocks):
            continue
        out = []
        for b in blocks:
            if b.get("block_type") == "text" and _id(b) != block_id:
                out.append({
                    "block_id": _id(b),
                    "label": b.get("ocr_label") or b.get("label") or "",
                    "text": b.get("pdfplumber_text") or b.get("ocr_text") or "",
                })
        return out
    return []


@router.get("/{project_id:path}/blocks/llm-text/{block_id}")
async def get_block_llm_text(
    project_id: str,
    block_id: str,
    request: Request,
    version_id: Optional[str] = Query(None),
    page: Optional[int] = Query(None, description="Страница блока (если известна на клиенте)"),
):
    """Текст, реально уходящий в LLM для блока на Stage 02 (без изображения).

    Для визуальной сверки «что мы отправляем в нейронку»: тот же `system_prompt` +
    `user_text` (enrichment JSON + текст страницы), что собирает реальный анализ блока
    (`call_gpt_for_block` через общий `build_block_user_text`). Работает на v2 и legacy.
    """
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        build_block_user_text,
        build_system_prompt,
        get_enrichment,
        load_page_text,
    )

    try:
        ctx = version_service.resolve_project_version_context(project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except FileNotFoundError:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    output_dir: Path = ctx["output_dir"]
    version_dir: Path = ctx.get("version_dir") or output_dir.parent

    # 1) Страница блока: с клиента (query) или из 01_blocks_analysis.json / gemma index
    if page is None:
        page = _lookup_block_page(output_dir, block_id)
    if page is None:
        raise HTTPException(404, f"Блок '{block_id}' не найден для '{project_id}'")

    # 2) project_info (источник enrichment + секция). v2: 01_input/, legacy: корень версии
    project_info: dict = {}
    for cand in (version_dir / "01_input" / "project_info.json", version_dir / "project_info.json"):
        if cand.exists():
            try:
                project_info = json.loads(cand.read_text(encoding="utf-8"))
                break
            except (OSError, json.JSONDecodeError):
                continue

    # 3) enrichment — ТОТ ЖЕ источник, что у Stage 02 (MD [ENRICHED] → gemma experiments)
    enrichment, enr_source = get_enrichment(version_dir, {}, project_info, block_id)

    # 4) текст страницы + 0-based page_index блока из document_graph.json (авторитетный источник
    #    страницы для fitz-рендера: result.json.page_index бывает +1 → рендерит не ту страницу).
    page_text = ""
    dg_page_index = None
    graph_path = output_dir / "document_graph.json"
    if graph_path.exists():
        try:
            _dg = json.loads(graph_path.read_text(encoding="utf-8"))
            page_text = load_page_text(_dg, page)
            for _p in _dg.get("pages", []):
                if any(str(_b.get("id") or _b.get("block_id")) == str(block_id)
                       for _b in _p.get("image_blocks", [])):
                    dg_page_index = _p.get("page_index", _p.get("page"))
                    break
        except (OSError, json.JSONDecodeError):
            page_text = ""

    section = str(project_info.get("section") or "")
    user_text = build_block_user_text(block_id, page, enrichment, page_text)
    try:
        system_prompt = build_system_prompt(section, extended=True)
    except Exception:
        system_prompt = build_system_prompt(section, extended=False)

    # 5) Сырьё блока из result.json: вектор-слой PDF + сырой gemma-ocr (для полного UI-просмотра).
    #    Вектор СЕЙЧАС в промпт Stage 02 НЕ попадает — показываем, чтобы видеть «что доступно».
    rblock = _block_from_result_json(version_dir, block_id)
    vector_text = rblock.get("pdfplumber_text") or ""
    gemma_ocr_text = rblock.get("ocr_text") or ""
    try:
        from backend.app.pipeline.stages.block_grounding.grounding import vector_usable
        v_usable = vector_usable(vector_text)
    except Exception:
        v_usable = bool(vector_text and len(vector_text) >= 30)

    # 6) Структурированный граф однолинейной схемы (ввод→секции→линии). None, если не схема.
    #    Часть механизма «Вектограф» (vectograf) — разбор текста-формул. См. docs/vectograf.md.
    structured_graph = None
    try:
        if v_usable and vector_text:
            from backend.app.pipeline.stages.block_grounding.singleline_structurer import (
                structure_singleline_text,
            )
            panel_name = ""
            if isinstance(enrichment, dict):
                mk = enrichment.get("marks")
                if isinstance(mk, list) and mk:
                    panel_name = str(mk[0])
                elif enrichment.get("subject"):
                    panel_name = str(enrichment["subject"])[:48]
            structured_graph = structure_singleline_text(vector_text, panel=panel_name or "схема")
    except Exception:
        structured_graph = None

    # 7) Полный граф схемы из ГЕОМЕТРИИ PDF (топология QF↔линия↔панель РПn, управление АСУД/ПС).
    #    Ядро механизма «Вектограф» (vectograf) — топология по координатам. См. docs/vectograf.md.
    singleline_graph = None
    try:
        if v_usable and vector_text and structured_graph:
            from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
                build_singleline_graph,
            )
            pdf = version_dir / "02_work" / "document.pdf"
            if not pdf.exists() and (version_dir / "document.pdf").exists():
                pdf = version_dir / "document.pdf"
            if pdf.exists():
                ph = (structured_graph or {}).get("panel") or "ВРУ"
                singleline_graph = build_singleline_graph(
                    pdf, vector_text, panel_hint=ph, bbox_norm=rblock.get("coords_norm"),
                    polygon_norm=rblock.get("polygon_points_norm"))
    except Exception:
        singleline_graph = None

    # 7b) Пространственные группы текста блока (для оверлея «области»): чисто геометрия вектор-слоя,
    #     bbox нормирован к coords_norm → совпадает с рендером /blocks/region-image. Работает на
    #     ЛЮБОМ блоке с вектор-слоем (не только однолинейки). См. block_text_clustering. fail-soft.
    text_groups: list = []
    try:
        from backend.app.pipeline.stages.block_grounding.block_text_clustering import (
            compute_text_groups,
        )
        pdf_tg = version_dir / "02_work" / "document.pdf"
        if not pdf_tg.exists() and (version_dir / "document.pdf").exists():
            pdf_tg = version_dir / "document.pdf"
        text_groups = compute_text_groups(
            pdf_tg, rblock.get("coords_norm"), vector_text,
            rblock.get("polygon_points_norm"),
            page_index=dg_page_index,
            page_index_fallback=int(rblock.get("page_index") or 0))
    except Exception:
        text_groups = []

    # user_text превью = РЕАЛЬНЫЙ user_text Stage 02 (правдиво):
    #  - SINGLELINE_RICH_PROMPT ON  → полная rich-разметка графа (совпадает с call_gpt_for_block);
    #  - OFF (default) → базовый build_block_user_text (enrichment+page_text), как в проде.
    # singleline_graph_markdown отдаётся ВСЕГДА (для UI-отображения, независимо от флага).
    singleline_graph_markdown = None
    stage02_prompt_mode = "base"
    block_graph_package = None
    profiled_graph_display = None
    vector_text_available = None

    # Канонический роутер Stage 01: structured graph / raw vector / image-only.
    # Он безусловно совпадает с реальным payload анализа блоков.
    _router_applied = False
    try:
        from backend.app.pipeline.stages.block_grounding.block_source_router import (
            resolve_block_package as _resolve_block_package,
        )
        block_graph_package = _resolve_block_package(output_dir, block_id, page)
        from backend.app.pipeline.stages.block_grounding.profiled_graph_localization import (
            package_display,
        )
        profiled_graph_display = package_display(block_graph_package)
        _rtext = block_graph_package.get("user_text")
        _rkind = str(block_graph_package.get("source_kind") or "error")
        vector_text_available = source_has_vector_text(_rkind)
        stage02_prompt_mode = "image_only" if _rkind == "gemma_fallback" else _rkind
        if not vector_text_available:
            # У image-only блока нет TXT-представления. Не протаскиваем сюда
            # legacy enrichment/page_text, подготовленный до вызова роутера.
            user_text = None
        elif _rtext:
            user_text = _rtext
        _router_applied = True
    except Exception:
        pass

    if singleline_graph:
        try:
            from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
                render_graph_etalon_markdown,
            )
            singleline_graph_markdown = render_graph_etalon_markdown(singleline_graph)
            from backend.app.core import config as _slcfg
            if not _router_applied and getattr(_slcfg, "SINGLELINE_RICH_PROMPT_ENABLED", False):
                _task = ("## Задача:\nПосмотри на изображение блока и верни findings[]. "
                         "Только проблемы. Не описывай что видишь. Если всё корректно — пустой массив.")
                user_text = (f"# Блок {block_id} | страница PDF {page}\n\n"
                             f"{singleline_graph_markdown}\n\n{_task}")
                stage02_prompt_mode = "singleline_rich"
        except Exception:
            singleline_graph_markdown = singleline_graph_markdown or None

        # bbox каждой линии (page-norm) → координаты БЛОКА (для полупрозрачных областей в UI)
        cn = rblock.get("coords_norm")
        if cn and len(cn) == 4:
            bw = (cn[2] - cn[0]) or 1.0
            bh = (cn[3] - cn[1]) or 1.0
            for pan in singleline_graph.get("panels", []):
                for f in pan.get("feeders", []):
                    bp = f.get("bbox_page")
                    if bp and len(bp) == 4:
                        f["bbox"] = [round((bp[0] - cn[0]) / bw, 5), round((bp[1] - cn[1]) / bh, 5),
                                     round((bp[2] - cn[0]) / bw, 5), round((bp[3] - cn[1]) / bh, 5)]
                    pp = f.get("polygon_page")
                    if pp:
                        f["polygon"] = [[round((px - cn[0]) / bw, 5), round((py - cn[1]) / bh, 5)]
                                        for px, py in pp]
                    pps = f.get("polygons_page")
                    if pps:
                        f["polygons"] = [[[round((px - cn[0]) / bw, 5), round((py - cn[1]) / bh, 5)]
                                          for px, py in poly] for poly in pps]

    # OCR-подмена («зеркало»): добавить чистый вектор-текст блока в промпт как приоритетный
    # источник ЧИСЕЛ. Флаг MIRROR_OCR_ENABLED (default OFF). Тем же inject_mirror_text, что
    # call_gpt_for_block — чтобы превью совпадало с реальным Stage 02. fail-soft.
    try:
        from backend.app.core import config as _mcfg
        if (not _router_applied and getattr(_mcfg, "MIRROR_OCR_ENABLED", False)
                and vector_text and len(vector_text.strip()) >= 40):
            from backend.app.pipeline.stages.block_grounding.mirror_block_text import (
                inject_mirror_text as _inject_mirror,
            )
            user_text = _inject_mirror(user_text, vector_text.strip())
            if stage02_prompt_mode == "base":
                stage02_prompt_mode = "mirror_ocr"
    except Exception:
        pass

    # Дедуп соседних текст-блоков: какие text-блоки той же страницы УЖЕ есть в текст-слое блока
    # (не слать повторно в LLM) и какие уникальны. Аддитивно, разметку/промпт не меняет. fail-soft.
    neighbor_text_blocks = None
    try:
        from backend.app.core import config as _cfg
        if getattr(_cfg, "NEIGHBOR_TEXT_BLOCKS_ENABLED", True) and vector_text:
            from backend.app.services.common.neighbor_block_dedup import (
                DEFAULT_THRESHOLD,
                filter_neighbor_blocks,
            )
            _neighbors = _neighbor_text_blocks_from_result_json(version_dir, block_id)
            if _neighbors:
                _send, _dropped = filter_neighbor_blocks(vector_text, _neighbors)
                neighbor_text_blocks = {
                    "send": _send,  # уникальные соседи (с полем bigram_in_text_layer)
                    "dropped": [{k: v for k, v in d.items() if k != "text"} for d in _dropped],
                    "threshold": DEFAULT_THRESHOLD,
                }
    except Exception:
        neighbor_text_blocks = None

    return {
        "project_id": project_id,
        "block_id": block_id,
        "page": page,
        "enrichment": enrichment,
        "enrichment_source": enr_source,
        "has_enrichment": enrichment is not None,
        "page_text": page_text,
        "system_prompt": system_prompt,
        "user_text": user_text,
        # режим промпта Stage 02: "base" (enrichment+page_text) | "singleline_rich" (флаг ON)
        "stage01_prompt_mode": stage02_prompt_mode,
        "stage02_prompt_mode": stage02_prompt_mode,
        # доступное сырьё блока (НЕ в промпте сейчас)
        "gemma_ocr_text": gemma_ocr_text,
        "vector_text": vector_text,
        "vector_len": len(vector_text),
        "vector_usable": v_usable,
        # структурированный граф схемы (демо метода) — None, если блок не однолинейная схема
        "structured_graph": structured_graph,
        # полный граф из геометрии PDF (QF↔панель РПn, автомат, управление) — None, если недоступно
        "singleline_graph": singleline_graph,
        # полный Markdown графа в формате эталона (8 разделов) — None, если блок не схема
        "singleline_graph_markdown": singleline_graph_markdown,
        # Единый пакет любого профильного графа: тот же сохранённый артефакт читает
        # Stage 01 при формировании фактического запроса к модели.
        "block_graph_package": block_graph_package,
        "profiled_graph": (block_graph_package or {}).get("graph"),
        # Человекочитаемая русская проекция. Машинные profile_id/node_type/
        # edge_state остаются неизменными в profiled_graph для алгоритмов.
        "profiled_graph_display": profiled_graph_display,
        "vector_text_available": vector_text_available,
        "vector_graph_message": (
            VECTOR_GRAPH_MISSING_MESSAGE if vector_text_available is False else None
        ),
        # пространственные группы текста блока (оверлей «области»): bbox в [0,1] региона блока
        "text_groups": text_groups,
        # соседние text-блоки страницы: send=уникальные (слать), dropped=дубли текст-слоя (не слать)
        "neighbor_text_blocks": neighbor_text_blocks,
    }


@router.get("/{project_id:path}/blocks/analysis")
async def get_blocks_analysis(
    project_id: str,
    request: Request,
    version_id: Optional[str] = Query(None),
):
    """Агрегированные данные анализа блоков из 01_blocks_analysis.json
    (текущий production-pipeline) с fallback на legacy block_batch_*.json /
    typed_facts_batch_*.json (v4).

    opt-in read canary: `?storage=projects_v2` (или header) + флаг → block-анализ
    из projects_v2 (read-only). Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_blocks_analysis(request, project_id)
    output_dir = _version_output(project_id, version_id)

    blocks_map = {}

    # Primary: 01_blocks_analysis.json — единый merged-результат Stage 02 текущего
    # production-режима (findings_only_block_context / single_block). Он использует
    # тот же ключ block_analyses, что и legacy-парсер ниже. Без этого источника
    # все блоки уходили в "skipped" (Без значимого содержимого), даже когда аудит
    # завершён, потому что новый режим не пишет per-batch block_batch_*.json.
    merged_path = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    if merged_path.exists():
        try:
            data = json.loads(merged_path.read_text(encoding="utf-8"))
            block_list = data.get("blocks_reviewed") or data.get("block_analyses") or []
            for block_info in block_list:
                bid = block_info.get("block_id", "")
                if bid:
                    blocks_map[bid] = block_info
        except Exception:
            pass

    # Legacy fallback: block_batch_*.json (старые batched-проекты без merged-файла)
    if not blocks_map:
        batch_files = sorted(output_dir.glob("block_batch_*.json"))
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
    index_path = resolve_blocks_index(output_dir)
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


_BLOCK_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}


def _block_media_type(path: Path) -> str:
    """MIME по расширению: галерея векторных графов пишет .webp, не только .png."""
    return _BLOCK_MEDIA_TYPES.get(Path(path).suffix.lower(), "image/png")


@router.get("/{project_id:path}/blocks/image/{block_id}")
async def get_block_image(
    project_id: str,
    block_id: str,
    request: Request,
    version_id: Optional[str] = Query(None),
):
    """PNG-файл кропнутого блока.

    opt-in/default read canary: при v2-backend кроп берётся из projects_v2
    (path-safe). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_block_image(request, project_id, block_id)
    output_dir = _version_output(project_id, version_id)
    blocks_dir = resolve_blocks_dir(output_dir)
    # Имя берём из index.json: галерея векторных графов пишет block_<id>.webp,
    # и прежний хардкод "block_{id}.png" отдавал на таких блоках 404.
    # resolve_block_image при включённом флаге дотягивает эвакуированный кроп.
    block_path = await asyncio.to_thread(
        block_crop_store.resolve_block_image, blocks_dir, block_id
    )
    if block_path is None:
        raise HTTPException(404, f"Блок {block_id} не найден")
    return FileResponse(str(block_path), media_type=_block_media_type(block_path))


@router.get("/{project_id:path}/blocks/region-image/{block_id}")
async def get_block_region_image(
    project_id: str,
    block_id: str,
    version_id: Optional[str] = Query(None),
):
    """PNG блока, отрендеренный ИЗ FITZ (та же страница/координаты, что и геометрия графа).

    Картинка `/blocks/image` — кроп Chandra-страницы с иной нормировкой → SVG-области линий
    на ней сдвинуты на колонку. Эта база рендерится из той же fitz-страницы (найденной по
    контенту) и того же coords_norm, что и bbox линий → области совпадают точно.
    """
    import fitz
    from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
        _find_page_index,
    )
    try:
        ctx = version_service.resolve_project_version_context(project_id, version_id)
    except (version_service.VersionNotFoundError, FileNotFoundError):
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    version_dir: Path = ctx.get("version_dir") or ctx["output_dir"].parent
    rblock = _block_from_result_json(version_dir, block_id)
    cn = rblock.get("coords_norm")
    vector_text = rblock.get("pdfplumber_text") or ""
    pdf = version_dir / "02_work" / "document.pdf"
    if not pdf.exists() and (version_dir / "document.pdf").exists():
        pdf = version_dir / "document.pdf"
    if not (cn and len(cn) == 4 and pdf.exists()):
        raise HTTPException(404, "Нет данных для рендера области блока")
    doc = fitz.open(str(pdf))
    try:
        pidx = _find_page_index(doc, vector_text)
        if pidx is None:
            pidx = int(rblock.get("page_index") or 0)
        pg = doc[pidx]
        W, H = pg.rect.width, pg.rect.height
        clip = fitz.Rect(cn[0] * W, cn[1] * H, cn[2] * W, cn[3] * H)
        pix = pg.get_pixmap(clip=clip, matrix=fitz.Matrix(2.0, 2.0))
        data = pix.tobytes("png")
    finally:
        doc.close()
    return Response(content=data, media_type="image/png")
