"""Роутер источника блока для Stage 02 — «вместо Gemma подаём точный текст чертежа».

Решение Андрея 2026-07-07: «вместо Gemma везде делаем сырые данные, а если это
однолинейки — делаем структурированные вектографом; в дальнейшем — свой
структурированный профиль для каждого типа графического блока».

Развилка на блок (4 ветки):
  1) однолинейная расчётная схема И гейт Вектографа пройден → ПОЛНЫЙ структурированный
     рендер (`render_graph_etalon_markdown`): связи QF↔код↔кабель↔потребитель привязаны
     геометрией колонок (мис-привязки соседства нет);
  2) известная схема ЭОМ/ОВ/ВК/ALIA И профильный гейт пройден → структурированный дисциплинарный
     Markdown: контейнеры, узлы, сети и честное состояние доказательности связей;
  3) есть содержательный вектор-слой (иначе) → СЫРОЙ вектор-текст блока (полигон-клип):
     100% полнота, 0 галлюцинаций OCR; связи домысливает LLM по соседству;
  4) вектор-слоя нет (скан/растр — клип тоньше порога) → image-only: Stage 01
     анализирует приложенный PNG без OCR-описания.

Источник вектор-текста — полигон-клип из PDF по `document_graph.json` (НЕ `pdfplumber_text`
из result.json, который у многих проектов пуст — см. память проекта).

Роутер является штатным источником Stage 01. Ошибка извлечения деградирует в image-only,
если PNG блока доступен.

Для ЭОМ, ГП, АР, КЖ, КМ, ТХ, ОВ, ВК и СС подключены собственные
структурированные профили уровня Вектографа.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Optional, Tuple

from .singleline_graph_geometry import (
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    build_singleline_graph,
    evaluate_vectograf_gate,
    render_graph_etalon_markdown,
)
from .block_profile_registry import load_prepared_package, make_package
from backend.app.pipeline.stages.block_context.reference_catalog import load_reference_rules

# Тоньше → считаем, что вектор-слоя нет (скан/растр) → fallback на Gemma.
_MIN_VECTOR_CHARS = 40
_REFERENCE_RULES = load_reference_rules()

_PER_BLOCK_PROFILE_ROUTING_FLAG = "STAGE01_PER_BLOCK_PROFILE_ROUTING_ENABLED"
_PER_BLOCK_PROFILE_ROUTING_POLICY = "stage01_per_block_profile_v1"

# Пер-блочный роутинг пока нужен только смешанным комплектам АИ.  АР остаётся
# на прежнем дисциплинарном маршруте: включение A/B-флага для АИ не должно
# незаметно менять уже проверенные архитектурные корпуса.
_AI_DISCIPLINE_CODES = frozenset({"AI", "АИ"})

_PARKING_GEOMETRY_MARKERS = (
    "рамп", "проезд", "машино-мест", "машиномест", "уклон",
)
_PARKING_PLACE_MARKERS = ("автостоян", "паркинг")
_PARKING_LAYOUT_MARKERS = (
    "план", "разметк",
)
_REFERENCE_SHEET_MARKERS = (
    "ведомость ссылочных документов", "ссылочные документы",
    "перечень ссылочных документов", "титульный лист",
)
_INTERIOR_MARKERS = (
    "развертк", "развёртк", "ведомость отделки", "спецификация отделки",
    "отделк", "напольн", "потолк", "чистовая", "мебел",
)
_DOOR_INTERIOR_MARKERS = (
    "двер", "д20", "д22", "д16", "д14", "д13.2", "антипаник",
)
_DOOR_CONTEXT_MARKERS = (
    "эскиз", "узел", "спецификац", "ведомост", "маркировоч", "ручк",
    "высот", "размер",
)


def per_block_profile_routing_enabled() -> bool:
    """Default-OFF A/B-флаг пер-блочного профиль-роутинга Stage 01.

    Читается на каждый вызов, чтобы per-run env override и monkeypatch в тестах
    не зависели от порядка импорта worker-процесса.
    """
    raw = os.environ.get(_PER_BLOCK_PROFILE_ROUTING_FLAG)
    if raw is None or raw == "":
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}

_PATH_DISCIPLINES = {
    "EOM": "ЭОМ", "ЭОМ": "ЭОМ", "GP": "ГП", "ГП": "ГП",
    "AR": "АР", "АР": "АР", "KJ": "КЖ", "КЖ": "КЖ",
    "KM": "КМ", "КМ": "КМ", "TX": "ТХ", "ТХ": "ТХ",
    "OV": "ОВ", "ОВ": "ОВ", "VK": "ВК", "ВК": "ВК",
    "SS": "СС", "СС": "СС",
    # АИ (интерьеры) использует те же типы графических блоков, что АР:
    # планы, развёртки, ведомости отделки, потолки, дверные узлы.
    "AI": "АР", "АИ": "АР",
}

_TASK = (
    "## Задача:\n"
    "Выше — ТОЧНЫЙ текст блока (встроенный текст чертежа / структурный разбор вектор-слоя, "
    "это приоритетный источник ЧИСЕЛ, марок, сечений и связей). Найди проблемы и верни "
    "findings[]. Только проблемы. Не описывай что видишь. Если всё корректно — пустой массив."
)


def _locate(output_dir) -> Tuple[Optional[Path], Optional[Path]]:
    """(pdf_path, document_graph_path) по output_dir.

    PDF ищем вверх по родителям (до 4 уровней): legacy (project/_output → PDF в project/),
    V2 (<version_dir>/_output → 02_work/document.pdf) и v2-primary, где output_dir =
    <version_dir>/03_analysis/runs/<run_id> или .../latest — PDF лежит в
    <version_dir>/02_work на 2-3 уровня выше (один od.parent его НЕ находил: роутер и
    предикат пропуска Gemma молча выключались на всех v2-primary прогонах)."""
    od = Path(output_dir)
    dg = od / "document_graph.json"
    dgp = dg if dg.exists() else None
    parents = list(od.parents)[:4]
    for vd in parents:
        for cand in (vd / "02_work" / "document.pdf", vd / "document.pdf"):
            if cand.exists():
                return cand, dgp
    for vd in parents:
        work = vd / "02_work"
        if work.is_dir():
            cands = sorted(work.glob("*.pdf"))
            if cands:
                return cands[0], dgp
    cands = sorted(od.parent.glob("*.pdf"))
    if cands:
        return cands[0], dgp
    return None, dgp


def _locate_chandra_markdown(pdf_path: Path) -> Optional[Path]:
    """Найти исходный Markdown Chandra рядом с нормализованным PDF версии."""
    pdf_path = Path(pdf_path)
    candidates = [
        pdf_path.with_suffix(".md"),
        pdf_path.parent / "document.md",
        pdf_path.parent / f"{pdf_path.stem}_document.md",
    ]
    candidates.extend(sorted(pdf_path.parent.glob("*_document.md")))
    if pdf_path.parent.name == "02_work":
        input_dir = pdf_path.parent.parent / "01_input"
        candidates.extend(sorted(input_dir.glob("*_document.md")))
        candidates.extend(sorted(input_dir.glob("*.md")))
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_file():
            return candidate
    return None


def _load_chandra_description(pdf_path: Path, block_id: str):
    """Fail-soft загрузка разрешённых полей Chandra конкретного блока."""
    try:
        from ..crop_blocks.block_markdown import extract_chandra_block_description

        md_path = _locate_chandra_markdown(pdf_path)
        if md_path is None:
            return None
        return extract_chandra_block_description(
            md_path.read_text(encoding="utf-8"), str(block_id)
        )
    except (OSError, UnicodeError):
        return None


def _classification_context(chandra, block_text: str, page_text: str) -> str:
    """Контекст выбора профиля; текст чертежа Chandra сюда не попадает."""
    if chandra is not None and chandra.classification_text:
        # Описание Chandra является самостоятельным семантическим источником.
        # Даже точный текст полигона может содержать примечания и ссылки на другие
        # чертежи (например, «см. кладочные планы») и не должен менять тип блока.
        return chandra.classification_text
    return (block_text or "") + "\n" + (page_text or "")


def _classification_metadata(
    chandra, profile_id: Optional[str], source_override: Optional[str] = None
) -> dict:
    """Аудируемое объяснение того, откуда взят тип блока."""
    if chandra is None:
        return {
            "profile_id": profile_id,
            "source": source_override or "vector_pdf_fallback",
            "confidence": "medium" if profile_id else "unclassified",
            "block_title": None,
            "chandra_drawing_text_used": False,
        }
    block_title = (
        chandra.short_description or chandra.description or chandra.block_type
    )
    return {
        "profile_id": profile_id,
        "source": source_override or "chandra_md",
        "confidence": (
            "high" if profile_id and (source_override in (None, "chandra_md"))
            else "medium" if profile_id else "needs_profile"
        ),
        "block_title": block_title,
        "block_type": chandra.block_type,
        "short_description": chandra.short_description,
        "description": chandra.description,
        "chandra_drawing_text_used": False,
        "page_context_fallback_used": source_override == "vector_page_fallback",
        "ignored_chandra_fields": list(
            (_REFERENCE_RULES.get("classification") or {}).get(
                "ignored_chandra_fields", ["Текст на чертеже", "Сущности", "ENRICHED"]
            )
        ),
    }


def _discipline_hint(output_dir) -> Optional[str]:
    """Дисциплина версии из пути хранения; защищает от междисциплинарных ложных профилей."""
    output_path = Path(output_dir)
    parts = output_path.parts
    if "disciplines" in parts:
        pos = parts.index("disciplines") + 1
        if pos < len(parts):
            raw = str(parts[pos]).upper()
            # Незнакомая дисциплина — тоже значимый hint: она не должна
            # проваливаться в свободный перебор чужих профильных построителей.
            return _PATH_DISCIPLINES.get(raw, raw)
    # Изолированные эксперименты/legacy-копии могут не повторять полный путь
    # ``disciplines/<code>``, но сохраняют канонический project_info. Без этого
    # AI-копия свободно перебирала ГП/ЭОМ и ошибочно структурировала 36/73 блоков
    # чужими профилями. Метаданные версии надёжнее имени временного каталога.
    for parent in list(output_path.parents)[:5]:
        for candidate in (
            parent / "01_input" / "project_info.json",
            parent / "project_info.json",
        ):
            if not candidate.is_file():
                continue
            try:
                info = json.loads(candidate.read_text(encoding="utf-8"))
                raw = str(info.get("section") or "").strip().upper()
                if raw:
                    return _PATH_DISCIPLINES.get(raw, raw)
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
    # Legacy: .../<объект>/<discipline>/<document>/_output.
    for part in reversed(parts):
        value = _PATH_DISCIPLINES.get(str(part).upper())
        if value:
            return value
    return None


def _storage_discipline_code(output_dir) -> Optional[str]:
    """Исходный код дисциплины без AI→АР alias.

    Нужен только для ограничения A/B-флага смешанными комплектами АИ; штатный
    _discipline_hint продолжает определять допустимые профильные построители
    как раньше.
    """
    output_path = Path(output_dir)
    parts = output_path.parts
    if "disciplines" in parts:
        pos = parts.index("disciplines") + 1
        if pos < len(parts):
            raw = str(parts[pos]).strip().upper()
            if raw:
                return raw
    for parent in list(output_path.parents)[:5]:
        for candidate in (
            parent / "01_input" / "project_info.json",
            parent / "project_info.json",
        ):
            if not candidate.is_file():
                continue
            try:
                info = json.loads(candidate.read_text(encoding="utf-8"))
                raw = str(info.get("section") or "").strip().upper()
                if raw:
                    return raw
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
    for part in reversed(parts):
        raw = str(part).strip().upper()
        if raw in _PATH_DISCIPLINES:
            return raw
    return None


def _per_block_profile_routing_applies(output_dir) -> bool:
    """Флаг ON и документ относится именно к АИ (не ко всему АР-корпусу)."""
    return (
        per_block_profile_routing_enabled()
        and _storage_discipline_code(output_dir) in _AI_DISCIPLINE_CODES
    )


def _document_graph_block_context(dg: dict, block_id: str) -> tuple[dict, dict]:
    """(page, image_block) из document_graph; пустые dict при неполных данных."""
    try:
        for page in dg.get("pages", []) or []:
            if not isinstance(page, dict):
                continue
            for block in page.get("image_blocks", []) or []:
                if not isinstance(block, dict):
                    continue
                bid = block.get("id") or block.get("block_id")
                if str(bid) == str(block_id):
                    return page, block
    except Exception:
        pass
    return {}, {}


def _sheet_title(page_record: dict) -> str:
    """Наименование листа из известных вариантов CTX/document_graph.json."""
    values = []
    for key in (
        "sheet_name", "sheet_title", "title", "name",
        "Наименование листа", "наименование листа",
    ):
        value = page_record.get(key) if isinstance(page_record, dict) else None
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    stamp = page_record.get("stamp_data") if isinstance(page_record, dict) else None
    if isinstance(stamp, dict):
        value = stamp.get("sheet_name") or stamp.get("title")
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    return " | ".join(dict.fromkeys(values))


def _canonical_graphic_block_type(block_type: str) -> str:
    """Привести Chandra/document_graph Type к литералам graphic_profiles.py."""
    value = str(block_type or "").strip().lower().replace("ё", "е")
    canonical = {
        "dense_grsh_singleline", "scheme", "dense_scheme", "table_legend",
        "stamp", "plan", "photo_or_general",
    }
    if value in canonical:
        return value
    if any(marker in value for marker in (
        "ведомост", "спецификац", "экспликац", "таблиц", "перечень",
    )):
        return "table_legend"
    if any(marker in value for marker in (
        "штамп", "титульн", "основная надпись",
    )):
        return "stamp"
    if "план" in value:
        return "plan"
    if "схем" in value:
        return "scheme"
    return "photo_or_general"


def _route_from_semantic_text(text: str) -> Optional[tuple[str, str, list[str]]]:
    """Сильные смысловые сигналы; raw-предикаты намеренно проверяются первыми."""
    value = str(text or "").lower()
    if not value.strip():
        return None

    reference_hits = [
        marker for marker in _REFERENCE_SHEET_MARKERS if marker in value
    ]
    if reference_hits:
        return "raw_vector", "reference_or_title_sheet", reference_hits

    geometry_hits = [
        marker for marker in _PARKING_GEOMETRY_MARKERS if marker in value
    ]
    if geometry_hits:
        return "raw_vector", "parking_geometry", geometry_hits

    parking_place_hits = [
        marker for marker in _PARKING_PLACE_MARKERS if marker in value
    ]
    parking_layout_hits = [
        marker for marker in _PARKING_LAYOUT_MARKERS if marker in value
    ]
    if parking_place_hits and parking_layout_hits:
        return (
            "raw_vector",
            "parking_plan_or_section",
            (parking_place_hits + parking_layout_hits)[:8],
        )

    interior_hits = [marker for marker in _INTERIOR_MARKERS if marker in value]
    door_hits = [marker for marker in _DOOR_INTERIOR_MARKERS if marker in value]
    door_context_hits = [
        marker for marker in _DOOR_CONTEXT_MARKERS if marker in value
    ]
    if interior_hits:
        return "structured_architecture", "interior_finish_or_elevation", interior_hits
    if door_hits and door_context_hits:
        return (
            "structured_architecture",
            "door_drawing_or_schedule",
            (door_hits + door_context_hits)[:6],
        )
    if re.search(r"\bплан\b[^\n]{0,80}-\s*\d+\s*(?:-?го\s+)?этаж", value):
        return "raw_vector", "negative_storey_plan", ["план -N этажа"]
    return None


def _per_block_profile_route(
    *,
    sheet_name: str = "",
    sheet_no=None,
    block_type: str = "",
    classification_text: str = "",
    block_text: str = "",
) -> dict:
    """Выбрать профиль блока АИ по приоритету лист → block_type → контент.

    Возвращаемое решение является аудируемым и само по себе fail-soft: target
    None означает «сохранить прежний дисциплинарный роутинг».
    """
    canonical_type = _canonical_graphic_block_type(block_type)
    graphic_profile_id = None
    try:
        from backend.app.services.stage_comparison.graphic_profiles import (
            classify_graphic_profile,
        )

        graphic_profile_id, _ = classify_graphic_profile(canonical_type)
    except Exception:
        graphic_profile_id = None

    base = {
        "policy": _PER_BLOCK_PROFILE_ROUTING_POLICY,
        "flag": _PER_BLOCK_PROFILE_ROUTING_FLAG,
        "enabled": True,
        "sheet_name": str(sheet_name or "").strip() or None,
        "sheet_no": sheet_no,
        "block_type": str(block_type or "").strip() or None,
        "graphic_block_type": canonical_type,
        "graphic_profile_id": graphic_profile_id,
    }

    def choice(
        target: Optional[str],
        signal_source: str,
        reason: str,
        markers: list[str],
        confidence: str,
    ) -> dict:
        return {
            **base,
            "selected_source_kind": target,
            "selected_profile": target,
            "signal_source": signal_source,
            "reason": reason,
            "matched_markers": list(dict.fromkeys(markers))[:8],
            "confidence": confidence,
        }

    # 1. Штамп/наименование листа — самый сильный источник.
    sheet_decision = _route_from_semantic_text(sheet_name)
    if sheet_decision:
        target, reason, markers = sheet_decision
        return choice(target, "sheet_name", reason, markers, "high")
    normalized_sheet_no = str(sheet_no or "").strip().lower().replace(",", ".")
    if normalized_sheet_no == "0.1":
        return choice(
            "raw_vector", "sheet_no", "title_or_reference_sheet_0_1",
            ["лист 0.1"], "high",
        )

    # 2. Специализированный block_type. Generic «План» остаётся слабым:
    # он одинаков для отделки и паркинга, поэтому контент ниже может его уточнить.
    block_type_decision = _route_from_semantic_text(block_type)
    if block_type_decision:
        target, reason, markers = block_type_decision
        return choice(target, "block_type", reason, markers, "high")
    if graphic_profile_id == "title_stamp_notes":
        return choice(
            "raw_vector", "block_type_classifier", "title_stamp_profile",
            [canonical_type], "high",
        )
    weak_graphic_target = (
        "structured_architecture"
        if graphic_profile_id == "architectural_plan_or_facade"
        else None
    )

    # 3. Chandra-классификация + точный text-layer полигона.
    content = (str(classification_text or "") + "\n" + str(block_text or "")).strip()
    content_decision = _route_from_semantic_text(content)
    if content_decision:
        target, reason, markers = content_decision
        return choice(target, "block_content", reason, markers, "high")

    # Угловые значения сами по себе важны: два и более десятичных угла
    # характерны для трассировки криволинейной рампы, а не для дверной дуги 90°.
    degree_hits = re.findall(r"(?<!\w)\d{1,3}(?:[.,]\d+)?\s*°", content)
    if len(degree_hits) >= 2 and any(
        "." in hit or "," in hit for hit in degree_hits
    ):
        return choice(
            "raw_vector", "block_text_geometry", "multiple_decimal_angles",
            degree_hits[:8], "high",
        )

    if weak_graphic_target:
        return choice(
            weak_graphic_target, "block_type_classifier",
            "generic_architectural_plan", [canonical_type], "medium",
        )
    return choice(
        None, "discipline_fallback", "no_decisive_per_block_signal", [],
        "low",
    )


def _extract_block(pdf_path: Path, dg: dict, block_id: str):
    """(page_text, block_text, bbox_norm, polygon_norm, page_pdf) для image-блока. Один open PDF."""
    import fitz  # локально, как в остальных block_grounding

    from .md_mirror_reconcile import _block_text

    doc = fitz.open(str(pdf_path))
    try:
        for p in dg.get("pages", []):
            pi = p.get("page_index", p.get("page"))
            if pi is None or pi >= doc.page_count:
                continue
            for b in p.get("image_blocks", []):
                bid = b.get("id") or b.get("block_id")
                if str(bid) != str(block_id):
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                poly = b.get("polygon_points_norm")
                bbox = b.get("coords_norm")
                clipped = (
                    _clip_words_to_polygon(words, poly, pw, ph)
                    if poly
                    else _clip_words_to_bbox(words, bbox, pw, ph)
                )
                return page.get_text(), _block_text(clipped), bbox, poly, (pi or 0) + 1
        return None
    finally:
        doc.close()


def vector_text_block_index(
    output_dir,
    *,
    include_text_blocks: bool = True,
    include_image_blocks: bool = True,
) -> dict:
    """Точный PDF-векторный текст image- и text-блоков за один проход.

    В отличие от :func:`vector_covered_block_ids`, индекс включает короткие
    подписи и ``text_blocks``.  Это нужно детерминированным evidence-проверкам:
    значимый токен может состоять из одного обозначения, а текст в
    ``document_graph`` для text-блока является OCR-производным и потому не
    может служить источником истины.

    Формат записи::

        {block_id: {
            "text": str,
            "page": int | str | None,
            "page_index": int,
            "block_kind": "image" | "text",
            "source": "pdf_vector_text",
            "source_file": str,
            "router_eligible": bool,
        }}

    Смещения внутри ``text`` остаются смещениями Python Unicode-строки.
    Любая ошибка даёт пустой индекс (fail-soft).
    """
    try:
        import fitz  # локально, как в остальных block_grounding

        from .md_mirror_reconcile import _block_text

        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return {}
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        doc = fitz.open(str(pdf))
        try:
            out: dict = {}
            for p in dg.get("pages", []):
                pi = p.get("page_index", p.get("page"))
                if pi is None or pi >= doc.page_count:
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                page_number = p.get("page")
                if page_number is None:
                    page_number = (pi or 0) + 1
                block_fields = []
                if include_text_blocks:
                    block_fields.append(("text", "text_blocks"))
                if include_image_blocks:
                    block_fields.append(("image", "image_blocks"))
                for block_kind, field in block_fields:
                    for b in p.get(field, []) or []:
                        bid = b.get("id") or b.get("block_id")
                        if not bid:
                            continue
                        poly = b.get("polygon_points_norm")
                        clipped = (
                            _clip_words_to_polygon(words, poly, pw, ph)
                            if poly
                            else _clip_words_to_bbox(
                                words, b.get("coords_norm"), pw, ph
                            )
                        )
                        txt = _block_text(clipped)
                        if not str(txt or "").strip():
                            continue
                        out[str(bid)] = {
                            "text": txt,
                            "page": page_number,
                            "page_index": pi,
                            "block_kind": block_kind,
                            "source": "pdf_vector_text",
                            "source_file": str(pdf),
                            "router_eligible": (
                                block_kind == "image"
                                and len(str(txt).strip()) >= _MIN_VECTOR_CHARS
                            ),
                        }
            return out
        finally:
            doc.close()
    except Exception:
        return {}


def vector_covered_block_ids(output_dir) -> dict:
    """{block_id: вектор-текст} для image-блоков с ГОДНЫМ вектор-слоем (≥ _MIN_VECTOR_CHARS).

    ОБЩИЙ предикат с `resolve_block_source`: тот же полигон-клип, тот же порог. Блок попадает
    сюда ТОГДА И ТОЛЬКО ТОГДА, когда роутер на Stage 02 отдаст ему вектор-текст (не gemma_fallback).
    Используется гейтом пропуска стадии Gemma: эти блоки можно НЕ гонять через Gemma — роутер и так
    подаст их точный текст. Один open PDF (батч). fail-soft → {}.

    ВАЖНО: значение (вектор-текст) кладётся в placeholder-enrichment пропущенного блока —
    страховка на случай, если роутер на Stage 02 всё же fail-soft'нётся: блок не станет слепым.
    """
    try:
        return {
            block_id: record["text"]
            for block_id, record in vector_text_block_index(
                output_dir,
                include_text_blocks=False,
            ).items()
            if isinstance(record, dict) and record.get("router_eligible") is True
        }
    except Exception:
        return {}


def resolve_block_package(
    output_dir, block_id: str, page, *, panel_hint: str = "ВРУ",
    prefer_prepared: bool = True,
) -> dict:
    """Канонический пакет Stage 01: эталон + профиль + граф + Markdown + LLM-текст.

    source_kind:
      structured_singleline | structured_electrical | structured_general_plan | structured_architecture | structured_structure | structured_hvac | structured_water | structured_alia_scheme | raw_vector | image_only |
      no_sources | block_not_found | error.
    fail-soft: при любой проблеме возвращается пакет без текста и прод-путь сохраняется.
    """
    ai_output = _storage_discipline_code(output_dir) in _AI_DISCIPLINE_CODES
    routing_applies = _per_block_profile_routing_applies(output_dir)
    if prefer_prepared:
        prepared = load_prepared_package(Path(output_dir), str(block_id))
        if prepared is not None:
            routing_meta = (
                prepared.get("classification") or {}
            ).get("profile_routing") or {}
            cached_routing_applies = (
                routing_meta.get("policy") == _PER_BLOCK_PROFILE_ROUTING_POLICY
                and routing_meta.get("enabled") is True
            )
            # Не смешиваем prepared-пакеты A/B: ON не читает старый mono-profile,
            # OFF не читает пакет, ранее построенный с пер-блочным флагом.
            if not ai_output or cached_routing_applies == routing_applies:
                return prepared
    try:
        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return make_package(block_id=block_id, page=page, source_kind="no_sources", user_text=None)
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        ex = _extract_block(pdf, dg, str(block_id))
        if ex is None:
            return make_package(block_id=block_id, page=page, source_kind="block_not_found", user_text=None)
        page_text, block_text, bbox, poly, page_pdf = ex
        head = f"# Блок {block_id} | страница PDF {page or page_pdf}\n\n"
        discipline_hint = _discipline_hint(output_dir)
        allows = lambda code: discipline_hint in (None, code)
        chandra = _load_chandra_description(pdf, str(block_id))
        profile_context = _classification_context(chandra, block_text, page_text)
        block_profile_context = _classification_context(chandra, block_text, "")
        legacy_page_context = (block_text or "") + "\n" + (page_text or "")
        legacy_block_context = block_text or ""
        page_record, block_record = _document_graph_block_context(dg, str(block_id))
        route_sheet_name = _sheet_title(page_record)
        route_sheet_no = (
            page_record.get("sheet_no_raw")
            or page_record.get("sheet_no_normalized")
            or page_record.get("sheet_no")
        )
        route_block_type = (
            getattr(chandra, "block_type", None)
            or block_record.get("block_type")
            or block_record.get("type")
            or ""
        )
        routing_decision = None
        if routing_applies:
            try:
                routing_decision = _per_block_profile_route(
                    sheet_name=route_sheet_name,
                    sheet_no=route_sheet_no,
                    block_type=route_block_type,
                    classification_text=profile_context,
                    block_text=block_text,
                )
            except Exception as exc:
                routing_decision = {
                    "policy": _PER_BLOCK_PROFILE_ROUTING_POLICY, "enabled": True,
                    "selected_source_kind": None, "signal_source": "discipline_fallback",
                    "reason": f"classifier_error:{type(exc).__name__}",
                    "confidence": "low",
                }
        profile_sources: dict[str, str] = {}

        def remember_profile_source(profile_id, source: str) -> None:
            if profile_id:
                profile_sources[str(profile_id)] = source

        def package(**kwargs):
            graph = kwargs.get("graph") if isinstance(kwargs.get("graph"), dict) else None
            profile_id = str(
                kwargs.get("profile_id") or (graph or {}).get("profile_id") or ""
            ).strip() or None
            source = profile_sources.get(str(profile_id or ""))
            if source is None and profile_id == "raw_vector":
                if (
                    routing_decision
                    and routing_decision.get("selected_source_kind") == "raw_vector"
                ):
                    source = "per_block_" + str(
                        routing_decision.get("signal_source") or "content"
                    )
                else:
                    source = "vector_pdf_fallback"
            classification = _classification_metadata(chandra, profile_id, source)
            if routing_decision:
                routing_meta = dict(routing_decision)
                actual_source_kind = str(kwargs.get("source_kind") or "")
                selected_source_kind = routing_meta.get("selected_source_kind")
                routing_meta.update({
                    "actual_source_kind": actual_source_kind,
                    "actual_profile_id": profile_id,
                    "applied": bool(
                        selected_source_kind
                        and selected_source_kind == actual_source_kind
                    ),
                })
                classification["profile_routing"] = routing_meta
            if graph is not None:
                graph["classification"] = classification
            # Структурный профиль — индекс, а не замена первичного источника.
            # Для АИ сохраняем рядом полный точный текст полигона: профиль может
            # ошибиться в типе или не извлечь марку/размер, а LLM не должен
            # принимать отсутствие поля в производном графе за отсутствие в РД.
            if (
                kwargs.get("source_kind") == "structured_architecture"
                and block_text
                and kwargs.get("user_text")
            ):
                kwargs["user_text"] += (
                    "\n\n## Полный точный текст полигона из PDF\n"
                    "Это первичный проектный источник; служебное название профиля выше "
                    "не является данными проекта.\n```\n"
                    + block_text
                    + "\n```\n"
                )
            kwargs["classification"] = classification
            return make_package(**kwargs)

        # (0) «Условные обозначения» — универсальный профиль поверх дисциплин.
        # Легенда встречается в любом разделе, и до появления профиля она
        # наследовала тип листа: легенда на листе плана потолка становилась
        # «планом потолка», а расшифровка марок («СН-1.2 → стена из газобетона
        # D600 → 250 мм») терялась целиком. Проверка стоит первой: легенда не
        # должна перехватываться дисциплинарными каскадами по словам «план»,
        # «стена», «трубопровод», которых в ней всегда много.
        try:
            from .legend_geometry import (PROFILE_LEGEND,
                add_legend_secondary_description, build_legend_graph_from_source,
                classify_legend_profile, evaluate_legend_gate, render_legend_markdown)
            legend_description = str(getattr(chandra, "description", "") or "")
            if classify_legend_profile(block_text or "", description=legend_description):
                legend_graph = build_legend_graph_from_source(
                    pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                    polygon_norm=poly, block_id=str(block_id),
                )
                add_legend_secondary_description(legend_graph, legend_description)
                legend_gate = evaluate_legend_gate(legend_graph)
                if legend_graph is not None and legend_gate.get("use"):
                    legend_md = render_legend_markdown(legend_graph)
                    if legend_md and len(legend_md) > 150:
                        remember_profile_source(PROFILE_LEGEND, "legend_rows_pdf")
                        return package(
                            block_id=block_id, page=page or page_pdf,
                            source_kind="structured_legend", discipline=discipline_hint,
                            profile_id=PROFILE_LEGEND, graph=legend_graph,
                            gate=legend_gate, markdown=legend_md,
                            user_text=head + legend_md + "\n\n" + _TASK,
                        )
        except Exception:
            pass

        # (1) однолинейка + гейт → полный структурированный рендер
        if allows("ЭОМ") and len((block_text or "").strip()) >= _MIN_VECTOR_CHARS:
            graph = None
            try:
                graph = build_singleline_graph(
                    pdf, page_text, panel_hint=panel_hint, bbox_norm=bbox, polygon_norm=poly
                )
            except Exception:
                graph = None
            if graph and evaluate_vectograf_gate(graph).get("use"):
                etalon = render_graph_etalon_markdown(graph)
                if etalon and len(etalon) > 200:
                    gate = evaluate_vectograf_gate(graph)
                    remember_profile_source("electrical_singleline", "vectograph_pdf")
                    return package(
                        block_id=block_id, page=page or page_pdf,
                        source_kind="structured_singleline", discipline="ЭОМ",
                        profile_id="electrical_singleline", graph=graph, gate=gate,
                        markdown=etalon, user_text=head + etalon + "\n\n" + _TASK,
                    )

        # (2) остальные профили ЭОМ. Они идут до общих инженерных профилей:
        # план электрощитовой содержит слова «план» и «оборудование», из-за чего
        # без дисциплинарного приоритета мог ошибочно попасть в ВК.
        try:
            import re
            from .electrical_geometry import (
                PROFILE_PANEL,PROFILE_SINGLELINE,build_electrical_graph_from_source,
                classify_electrical_profile,evaluate_electrical_gate,render_electrical_markdown,
            )
            electrical_source="chandra_md" if chandra is not None else "vector_block_pdf"
            electrical_profile=classify_electrical_profile(block_profile_context) if allows("ЭОМ") else None
            if electrical_profile is None and chandra is not None and allows("ЭОМ"):
                electrical_profile=classify_electrical_profile(legacy_block_context)
                electrical_source="vector_block_fallback"
            if electrical_profile==PROFILE_SINGLELINE:
                electrical_profile=PROFILE_PANEL if len(re.findall(r"\bQF\s*\d",block_text,re.I))>=3 else None
                electrical_source="vector_block_fallback"
            remember_profile_source(electrical_profile,electrical_source)
            electrical_graph=build_electrical_graph_from_source(
                pdf,page_index=page_pdf-1,bbox_norm=bbox,polygon_norm=poly,
                block_id=str(block_id),profile_hint=electrical_profile,
                subtype_hint="многоуровневая схема распределения" if electrical_profile==PROFILE_PANEL else "графический блок ЭОМ",
            ) if electrical_profile else None
        except Exception:
            electrical_graph=None
        if electrical_graph and evaluate_electrical_gate(electrical_graph).get("use"):
            structured=render_electrical_markdown(electrical_graph)
            if structured and len(structured)>150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_electrical",discipline="ЭОМ",graph=electrical_graph,
                  gate=evaluate_electrical_gate(electrical_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (3) генеральный план, разбивка, рельеф, покрытия, МАФ и водоотвод ГП.
        try:
            from .general_plan_geometry import (build_gp_graph_from_source,classify_gp_profile,
                evaluate_gp_gate,render_gp_markdown)
            # Chandra описывает конкретный полигон. Штамп листа используется только
            # как fallback, когда исходной секции Chandra нет.
            gp_source="chandra_md" if chandra is not None else "vector_page_fallback"
            gp_profile=classify_gp_profile(profile_context) if allows("ГП") else None
            if gp_profile is None and chandra is not None and allows("ГП"):
                gp_profile=classify_gp_profile(legacy_block_context)
                gp_source="vector_block_fallback"
                if gp_profile is None:
                    gp_profile=classify_gp_profile(legacy_page_context)
                    gp_source="vector_page_fallback"
            remember_profile_source(gp_profile,gp_source)
            gp_graph=build_gp_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=gp_profile) if gp_profile else None
        except Exception:
            gp_graph=None
        if gp_graph and evaluate_gp_gate(gp_graph).get("use"):
            structured=render_gp_markdown(gp_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_general_plan",discipline="ГП",graph=gp_graph,
              gate=evaluate_gp_gate(gp_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (4) архитектурные планы, фасады, разрезы, развёртки и узлы АР.
        try:
            from .architecture_geometry import (build_ar_graph_from_source,classify_ar_profile,
                evaluate_ar_gate,render_ar_markdown)
            selected_block_source = (
                routing_decision.get("selected_source_kind")
                if routing_decision else None
            )
            architecture_allowed = (
                allows("АР") and selected_block_source != "raw_vector"
            )
            ar_context = profile_context + (
                "\n" + route_sheet_name
                if selected_block_source == "structured_architecture" and route_sheet_name else ""
            )
            ar_upper=ar_context.upper()
            ar_marker=(
              selected_block_source == "structured_architecture"
              or chandra is not None
              or any(x in ar_upper for x in ("АРХИТЕКТУРНЫЕ РЕШЕНИЯ","КЛАДОЧ","МАРКИРОВОЧ",
                "РАЗВЕРТК","ФАСАД","ОТДЕЛКА КВАРТИР","ЧИСТОВАЯ ОТДЕЛКА","УЗЛЫ КРОВЛИ",
                "ЛЕСТНИЦ","ОГРАЖДЕНИЯ ЛЕСТНИЧНЫХ"))
            )
            ar_source="chandra_md" if chandra is not None else "vector_page_fallback"
            ar_profile=classify_ar_profile(ar_context) if ar_marker and architecture_allowed else None
            if ar_profile is None and chandra is not None and architecture_allowed:
                ar_profile=classify_ar_profile(legacy_block_context)
                ar_source="vector_block_fallback"
                if ar_profile is None:
                    ar_profile=classify_ar_profile(legacy_page_context)
                    ar_source="vector_page_fallback"
            remember_profile_source(ar_profile,ar_source)
            ar_graph=build_ar_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=ar_profile) if ar_profile else None
        except Exception:
            ar_graph=None
        if ar_graph and evaluate_ar_gate(ar_graph).get("use"):
            structured=render_ar_markdown(ar_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_architecture",discipline="АР",graph=ar_graph,
              gate=evaluate_ar_gate(ar_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (5) железобетонные планы, армирование, сечения и закладные детали КЖ.
        try:
            from .structural_geometry import (build_kj_graph_from_source,classify_kj_profile,
                evaluate_structural_gate,render_structural_markdown)
            kj_context=profile_context;kj_upper=kj_context.upper()
            kj_marker=chandra is not None or any(x in kj_upper for x in ("АРМИРОВАН","ОПАЛУБ","ВЕРТИКАЛЬНЫЕ КОНСТРУКЦ",
              "ПЛИТА ПЕРЕКРЫТИЯ","СЕЧЕНИЯ АРМИРОВАНИЯ","ЗАКЛАДНАЯ ДЕТАЛЬ"))
            kj_source="chandra_md" if chandra is not None else "vector_page_fallback"
            kj_profile=classify_kj_profile(kj_context) if kj_marker and allows("КЖ") else None
            if kj_profile is None and chandra is not None and allows("КЖ"):
                kj_profile=classify_kj_profile(legacy_block_context)
                kj_source="vector_block_fallback"
                if kj_profile is None:
                    kj_profile=classify_kj_profile(legacy_page_context)
                    kj_source="vector_page_fallback"
            remember_profile_source(kj_profile,kj_source)
            kj_graph=build_kj_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=kj_profile) if kj_profile else None
        except Exception:
            kj_graph=None
        if kj_graph and evaluate_structural_gate(kj_graph).get("use"):
            structured=render_structural_markdown(kj_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_structure",discipline="КЖ",graph=kj_graph,
              gate=evaluate_structural_gate(kj_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # Металлоконструкции и фасадные подсистемы КМ используют ту же
        # доказательную основу, но собственные профили элементов и соединений.
        try:
            from .structural_geometry import (build_kj_graph_from_source,classify_km_profile,
                evaluate_structural_gate,render_structural_markdown)
            km_context=profile_context;km_upper=km_context.upper()
            km_marker=chandra is not None or any(x in km_upper for x in ("КОНСТРУКЦИИ МЕТАЛЛИЧЕСКИЕ","МЕТАЛЛИЧЕСКИХ КОНСТРУКЦИЙ",
              "НАВЕСНОЙ ФАСАДНОЙ СИСТЕМЫ","НВФ","MOCKUP","СТРЕМЯНКА"))
            km_source="chandra_md" if chandra is not None else "vector_page_fallback"
            km_profile=classify_km_profile(km_context) if km_marker and allows("КМ") else None
            if km_profile is None and chandra is not None and allows("КМ"):
                km_profile=classify_km_profile(legacy_block_context)
                km_source="vector_block_fallback"
                if km_profile is None:
                    km_profile=classify_km_profile(legacy_page_context)
                    km_source="vector_page_fallback"
            remember_profile_source(km_profile,km_source)
            km_graph=build_kj_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=km_profile,subtype_hint="металлическая конструкция") if km_profile else None
        except Exception:
            km_graph=None
        if km_graph and evaluate_structural_gate(km_graph).get("use"):
            structured=render_structural_markdown(km_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_structure",discipline="КМ",graph=km_graph,
              gate=evaluate_structural_gate(km_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (6) автостоянки, лифты и мусороудаление ТХ.
        try:
            from .technology_geometry import (build_tx_graph_from_source,classify_tx_profile,
                evaluate_tx_gate,render_tx_markdown)
            tx_context=profile_context;tx_upper=tx_context.upper()
            tx_marker=chandra is not None or any(x in tx_upper for x in ("ТЕХНОЛОГИЧЕСКИЕ РЕШЕНИЯ","ВЕРТИКАЛЬНЫЙ ТРАНСПОРТ",
              "МАШИНОМЕСТ","ЛИФТОВЫХ ШАХТ","МУСОРОУДАЛЕНИЕ"))
            tx_source="chandra_md" if chandra is not None else "vector_page_fallback"
            tx_profile=classify_tx_profile(tx_context) if tx_marker and allows("ТХ") else None
            if tx_profile is None and chandra is not None and allows("ТХ"):
                tx_profile=classify_tx_profile(legacy_block_context)
                tx_source="vector_block_fallback"
                if tx_profile is None:
                    tx_profile=classify_tx_profile(legacy_page_context)
                    tx_source="vector_page_fallback"
            remember_profile_source(tx_profile,tx_source)
            tx_graph=build_tx_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=tx_profile) if tx_profile else None
        except Exception:
            tx_graph=None
        if tx_graph and evaluate_tx_gate(tx_graph).get("use"):
            structured=render_tx_markdown(tx_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_technology",discipline="ТХ",graph=tx_graph,
              gate=evaluate_tx_gate(tx_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (6) планы, аксонометрии, узлы, профили и оборудование ВК.
        # ВК идёт раньше ОВ: водомерный/насосный узел по общей лексике похож на
        # гидравлическую схему ОВ, но известный block_id дисциплины точнее текста.
        try:
            from .water_geometry import (
                build_water_graph_from_source,
                classify_water_profile,
                evaluate_water_gate,
                render_water_markdown,
            )
            water_source="chandra_md" if chandra is not None else "vector_block_pdf"
            water_profile, water_subtype = classify_water_profile(
                block_profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("ВК") else (None, None)
            if water_profile is None and chandra is not None and allows("ВК"):
                water_profile, water_subtype = classify_water_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                water_source="vector_block_fallback"
            remember_profile_source(water_profile,water_source)
            water_graph = build_water_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=water_profile, subtype_hint=water_subtype,
            ) if water_profile else None
        except Exception:
            water_graph = None
        if water_graph and evaluate_water_gate(water_graph).get("use"):
            structured = render_water_markdown(water_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_water",discipline="ВК",graph=water_graph,
                  gate=evaluate_water_gate(water_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (5) планы, аксонометрии, гидравлика, узлы, разрезы и оборудование ОВ
        try:
            from .hvac_geometry import (
                build_hvac_graph_from_source,
                classify_hvac_profile,
                evaluate_hvac_gate,
                render_hvac_markdown,
            )
            hvac_source="chandra_md" if chandra is not None else "vector_block_pdf"
            hvac_profile, hvac_subtype = classify_hvac_profile(
                block_profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("ОВ") else (None, None)
            if hvac_profile is None and chandra is not None and allows("ОВ"):
                hvac_profile, hvac_subtype = classify_hvac_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                hvac_source="vector_block_fallback"
            remember_profile_source(hvac_profile,hvac_source)
            hvac_graph = build_hvac_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=hvac_profile, subtype_hint=hvac_subtype,
            ) if hvac_profile else None
        except Exception:
            hvac_graph = None
        if hvac_graph and evaluate_hvac_gate(hvac_graph).get("use"):
            structured = render_hvac_markdown(hvac_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_hvac",discipline="ОВ",graph=hvac_graph,
                  gate=evaluate_hvac_gate(hvac_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # Специализированные СС-грамматики: структурная схема АПС/АППЗ,
        # аксонометрия кабельных лотков и клеммные подключения. Этот построитель
        # точнее общих ALIA-профилей и поэтому должен срабатывать первым.
        low_voltage_text = block_text
        low_voltage_bbox = bbox
        low_voltage_poly = poly
        if (
            not poly
            and isinstance(bbox, (list, tuple))
            and len(bbox) == 4
            and float(bbox[0]) <= 0.001
            and float(bbox[1]) <= 0.001
            and float(bbox[2]) >= 0.999
            and float(bbox[3]) >= 0.999
        ):
            # У самостоятельного whole-page блока исходный page text сохраняет
            # порядок многострочных выносок лучше реконструкции из words.
            # Для реальных полигонов по-прежнему используем только block_text,
            # чтобы соседняя схема не протекала в текущий CTX-граф.
            low_voltage_text = page_text or block_text
            low_voltage_bbox = None
        try:
            from .low_voltage_geometry import (
                build_low_voltage_graph,
                evaluate_low_voltage_gate,
                normalize_low_voltage_graph,
                profile_id_for_subtype,
                render_low_voltage_graph_markdown,
            )
            low_voltage_graph = (
                build_low_voltage_graph(
                    pdf,
                    low_voltage_text,
                    bbox_norm=low_voltage_bbox,
                    polygon_norm=low_voltage_poly,
                )
                if allows("СС") and len((low_voltage_text or "").strip()) >= _MIN_VECTOR_CHARS
                else None
            )
            low_voltage_gate = evaluate_low_voltage_gate(low_voltage_graph)
        except Exception:
            low_voltage_graph = None
            low_voltage_gate = {"use": False}
        if low_voltage_graph and low_voltage_gate.get("use"):
            low_voltage_graph = normalize_low_voltage_graph(low_voltage_graph)
            low_voltage_profile = profile_id_for_subtype(low_voltage_graph.get("subtype"))
            remember_profile_source(low_voltage_profile, "vector_block_pdf")
            structured = render_low_voltage_graph_markdown(low_voltage_graph)
            if structured and len(structured) > 150:
                return package(
                    block_id=block_id,
                    page=page or page_pdf,
                    source_kind="structured_alia_scheme",
                    discipline="СС",
                    profile_id=low_voltage_profile,
                    graph=low_voltage_graph,
                    gate=low_voltage_gate,
                    markdown=structured,
                    user_text=head+structured+"\n\n"+_TASK,
                )

        if len((block_text or "").strip()) < _MIN_VECTOR_CHARS:
            return package(block_id=block_id,page=page or page_pdf,
              source_kind="image_only",user_text=None)  # Stage 01 анализирует PNG

        # (5) профили графических схем ALIA → дисциплинарная структура вместо сырого текста
        try:
            from .alia_scheme_geometry import (
                build_alia_scheme_graph_from_source,
                classify_alia_scheme_profile,
                evaluate_alia_scheme_gate,
                render_alia_scheme_markdown,
            )
            alia_profile = classify_alia_scheme_profile(block_profile_context) if allows("СС") else None
            remember_profile_source(
                alia_profile,
                "chandra_md" if chandra is not None else "vector_block_pdf",
            )
            alia_graph = build_alia_scheme_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=alia_profile,
            ) if allows("СС") else None
            if alia_graph and alia_profile is None:
                remember_profile_source(alia_graph.get("profile_id"),"vector_block_fallback")
        except Exception:
            alia_graph = None
        if alia_graph and evaluate_alia_scheme_gate(alia_graph).get("use"):
            structured = render_alia_scheme_markdown(alia_graph)
            if structured and len(structured) > 200:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_alia_scheme",discipline="СС",graph=alia_graph,
                  gate=evaluate_alia_scheme_gate(alia_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (6) планы, подключения, принципиальные схемы, узлы и физические сборки ALIA
        try:
            from .alia_remaining_geometry import (
                build_remaining_graph_from_source,
                classify_remaining_profile,
                evaluate_remaining_gate,
                render_remaining_markdown,
            )
            remaining_profile, remaining_subtype = classify_remaining_profile(
                profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("СС") else (None, None)
            remaining_context = profile_context
            remaining_source="chandra_md" if chandra is not None else "vector_page_fallback"
            if remaining_profile is None and chandra is not None and allows("СС"):
                remaining_profile, remaining_subtype = classify_remaining_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                remaining_context = legacy_block_context
                remaining_source="vector_block_fallback"
                if remaining_profile is None:
                    remaining_profile, remaining_subtype = classify_remaining_profile(
                        legacy_page_context, block_id=str(block_id)
                    )
                    remaining_context = legacy_page_context
                    remaining_source="vector_page_fallback"
            remember_profile_source(remaining_profile,remaining_source)
            remaining_graph = build_remaining_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox, polygon_norm=poly,
                block_id=str(block_id), context_text=remaining_context,
                profile_hint=remaining_profile, subtype_hint=remaining_subtype,
            ) if allows("СС") else None
            if remaining_graph and remaining_profile is None:
                remember_profile_source(remaining_graph.get("profile_id"),"vector_page_fallback")
        except Exception:
            remaining_graph = None
        if remaining_graph and evaluate_remaining_gate(remaining_graph).get("use"):
            structured = render_remaining_markdown(remaining_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_alia_scheme",discipline="СС",graph=remaining_graph,
                  gate=evaluate_remaining_gate(remaining_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (7) иначе → сырой вектор-текст блока (полный, без потерь)
        body = (
            "## Точный текст блока из вектор-слоя PDF (встроенный текст чертежа, без ошибок OCR):\n"
            f"```\n{block_text}\n```\n"
        )
        return package(block_id=block_id,page=page or page_pdf,
          source_kind="raw_vector",profile_id="raw_vector",markdown=body,
          user_text=head+body+"\n"+_TASK)
    except Exception as exc:
        return make_package(block_id=block_id,page=page,source_kind="error",
          user_text=None,error=f"{type(exc).__name__}: {exc}")


def resolve_block_source(
    output_dir, block_id: str, page, *, panel_hint: str = "ВРУ"
) -> Tuple[Optional[str], str]:
    """Совместимый интерфейс: вернуть только LLM-текст и вид источника."""
    package = resolve_block_package(output_dir, block_id, page, panel_hint=panel_hint)
    return package.get("user_text"), str(package.get("source_kind") or "error")
