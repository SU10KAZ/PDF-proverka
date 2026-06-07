# -*- coding: utf-8 -*-
"""Graphic Structured Extraction — универсальный слой профилей для image/imagine
блоков проектной документации.

Цель: НЕ алгоритм «только под ГРЩ», а общая основа, в которую профили
добавляются по дисциплинам. GRSH — первый доказанный профиль
(`electrical_singleline / grsh`), остальные — схемы-заглушки (schema готова,
extractor пока не production-ready).

Общий конвейер (для всех image-блоков):

    block crop_url PDF → text layer (OCR vocabulary) → high-res render
      → block type classifier → extraction profile → Qwen structured JSON
      → deterministic validation/merge (field_state, anti-hallucination, recall)
      → enriched MD

Принцип:
    text layer / Chandra OCR = буквальные значения (маркировки, кабели, токи, …);
    Qwen = структура / связи / группировка / что важно для сравнения;
    backend = validation / field_state / anti-hallucination / merge / recall.

Модуль не делает сетевых вызовов и не зависит от Qwen напрямую — он описывает
профили, классифицирует блок в профиль и нормализует структурированный вывод
(включая field_state). Сам запуск Qwen — в profile-extractor'ах (для
electrical_singleline это grsh_feeder_extraction).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ─── env ──────────────────────────────────────────────────────────────────


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def graphic_structured_extraction_enabled() -> bool:
    """Главный включатель универсального слоя (default OFF).

    Backward-compat: исторический `STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED`
    тоже включает слой (GRSH — это профиль electrical_singleline/grsh внутри него).

    Учитывает per-run analysis_profile override (rich_grsh) поверх env-флагов.
    """
    from . import analysis_profile as _ap
    return (
        _ap.flag_enabled(_ap.GRAPHIC_STRUCTURED_FLAG, False)
        or _ap.flag_enabled(_ap.GRSH_FEEDER_FLAG, False)
    )


# ─── field_state ──────────────────────────────────────────────────────────


class FieldState:
    """Состояние поля в structured output. Универсально для всех профилей."""

    PRESENT = "present"                       # извлечено и подтверждено
    NOT_EXTRACTED = "not_extracted"           # не прочитано (НЕ «удалено»)
    NOT_SPECIFIED = "not_specified"           # на чертеже явно отсутствует
    VISUAL_UNVERIFIED = "visual_unverified"   # Qwen увидел, в text layer нет — не evidence
    OCR_ONLY = "ocr_only"                     # есть в text layer, визуально не подтверждено
    REQUIRES_HUMAN_REVIEW = "requires_human_review"


FIELD_STATES = frozenset({
    FieldState.PRESENT, FieldState.NOT_EXTRACTED, FieldState.NOT_SPECIFIED,
    FieldState.VISUAL_UNVERIFIED, FieldState.OCR_ONLY, FieldState.REQUIRES_HUMAN_REVIEW,
})

# field_state, которые НЕ должны превращаться в «removed» на стадии сравнения.
NON_REMOVAL_STATES = frozenset({
    FieldState.NOT_EXTRACTED, FieldState.VISUAL_UNVERIFIED, FieldState.OCR_ONLY,
    FieldState.NOT_SPECIFIED, FieldState.REQUIRES_HUMAN_REVIEW,
})


def field_cell(value, state: str) -> dict:
    """Унифицированная ячейка значения с состоянием."""
    if state not in FIELD_STATES:
        state = FieldState.NOT_EXTRACTED
    return {"value": value, "field_state": state}


def derive_field_state(value, *, in_text_layer: Optional[bool] = None,
                       low_confidence: bool = False) -> str:
    """Определить field_state по наличию значения, подтверждению text-слоем и
    уверенности. Универсальная анти-галлюцинационная логика:

    - значение есть и подтверждено text layer → present;
    - значение есть, но в text layer его нет → visual_unverified (не evidence);
    - text layer содержит, визуально не подтверждено → ocr_only;
    - low confidence → requires_human_review;
    - значения нет → not_extracted (НЕ «удалено»).
    """
    has_value = value not in (None, "", "null", "unknown", [])
    if not has_value:
        return FieldState.NOT_EXTRACTED
    if low_confidence:
        return FieldState.REQUIRES_HUMAN_REVIEW
    if in_text_layer is True:
        return FieldState.PRESENT
    if in_text_layer is False:
        return FieldState.VISUAL_UNVERIFIED
    return FieldState.PRESENT


# ─── profile descriptors ──────────────────────────────────────────────────


@dataclass
class GraphicProfile:
    """Описание профиля извлечения: id, набор групп полей, готовность к продакшену."""

    profile_id: str
    title: str
    disciplines: tuple[str, ...]
    field_groups: tuple[str, ...]
    production_ready: bool = False           # есть рабочий extractor
    subtypes: tuple[str, ...] = field(default_factory=tuple)


# Profile ids
ELECTRICAL_SINGLELINE = "electrical_singleline"
HVAC_SCHEME = "hvac_scheme"
WATER_SUPPLY_SCHEME = "water_supply_scheme"
LOW_VOLTAGE_SCHEME = "low_voltage_scheme"
STRUCTURAL_SCHEME = "structural_scheme"
ARCHITECTURAL_PLAN_OR_FACADE = "architectural_plan_or_facade"
TABLE_OR_SCHEDULE = "table_or_schedule"
TITLE_STAMP_NOTES = "title_stamp_notes"
GENERAL = "general"


PROFILE_REGISTRY: dict[str, GraphicProfile] = {
    ELECTRICAL_SINGLELINE: GraphicProfile(
        ELECTRICAL_SINGLELINE, "Электрические однолинейные схемы",
        ("ЭОМ", "ИОС1", "ЭО", "ЭС", "ЭМ"),
        ("feeders", "breakers", "cables", "loads", "metering",
         "compensation", "earthing", "connections"),
        production_ready=True, subtypes=("grsh",)),  # grsh — первый рабочий subtype
    HVAC_SCHEME: GraphicProfile(
        HVAC_SCHEME, "Схемы ОВ / холодоснабжения / вентиляции",
        ("ОВ", "ОВиК", "ХС", "ИОС4"),
        ("systems", "equipment", "airflows", "ducts", "valves",
         "pumps_fans_chillers", "parameters", "connections")),
    WATER_SUPPLY_SCHEME: GraphicProfile(
        WATER_SUPPLY_SCHEME, "Схемы ВК / ВПВ / ХПВ / насосных / ИТП",
        ("ВК", "ИОС2", "ИОС3", "ВПВ", "ХПВ"),
        ("zones", "pumps", "pipes", "diameters", "flows",
         "pressures", "tanks", "connections")),
    LOW_VOLTAGE_SCHEME: GraphicProfile(
        LOW_VOLTAGE_SCHEME, "Слаботочные схемы (СС/СПС/СОУЭ/СКУД/АСКУЭ)",
        ("СС", "ИОС5", "СПС", "СОУЭ", "СКУД", "АСКУЭ"),
        ("systems", "devices", "loops", "cables", "panels",
         "addresses_zones", "connections")),
    STRUCTURAL_SCHEME: GraphicProfile(
        STRUCTURAL_SCHEME, "Конструктивные схемы (КР/КЖ/узлы/разрезы)",
        ("КР", "КЖ", "КМ"),
        ("elements", "concrete_class", "reinforcement", "thicknesses",
         "marks", "sections", "notes")),
    ARCHITECTURAL_PLAN_OR_FACADE: GraphicProfile(
        ARCHITECTURAL_PLAN_OR_FACADE, "Архитектурные планы / фасады",
        ("АР", "АС"),
        ("zones", "materials", "facade_systems", "doors_windows",
         "dimensions", "levels", "notes")),
    TABLE_OR_SCHEDULE: GraphicProfile(
        TABLE_OR_SCHEDULE, "Таблицы / ведомости / спецификации",
        (),
        ("rows", "columns", "units", "quantities", "materials_equipment", "notes")),
    TITLE_STAMP_NOTES: GraphicProfile(
        TITLE_STAMP_NOTES, "Штампы / общие данные / примечания",
        (),
        ("document_code", "sheet_name", "stage", "revision",
         "organization", "date", "notes")),
    GENERAL: GraphicProfile(
        GENERAL, "Общий графический блок (fallback)",
        (),
        ("visible_text", "equipment", "numeric_parameters", "notes")),
}


def get_profile(profile_id: str) -> GraphicProfile:
    return PROFILE_REGISTRY.get(profile_id) or PROFILE_REGISTRY[GENERAL]


def list_profiles() -> list[str]:
    return list(PROFILE_REGISTRY.keys())


# ─── classifier: block_type → profile ─────────────────────────────────────

# block_type строки (из md_image_enrichment.classify_image_block). Держим
# здесь как стабильные литералы, чтобы не тянуть тяжёлый импорт.
_BT_DENSE_GRSH = "dense_grsh_singleline"
_BT_SCHEME = "scheme"
_BT_DENSE_SCHEME = "dense_scheme"
_BT_TABLE_LEGEND = "table_legend"
_BT_STAMP = "stamp"
_BT_PLAN = "plan"
_BT_GENERAL = "photo_or_general"


def classify_graphic_profile(block_type: str) -> tuple[str, Optional[str]]:
    """block_type → (profile_id, subtype).

    Сейчас рабочий extractor есть ТОЛЬКО у electrical_singleline/grsh
    (dense_grsh_singleline). Прочие схемы пока идут general/fallback —
    дисциплинарная маршрутизация (scheme → hvac/water/lv по дисциплине листа)
    добавится отдельным detector'ом. Таблицы/штампы/планы получают свой
    профиль (schema готова), но без production-extractor'а.
    """
    if block_type == _BT_DENSE_GRSH:
        return (ELECTRICAL_SINGLELINE, "grsh")
    if block_type == _BT_TABLE_LEGEND:
        return (TABLE_OR_SCHEDULE, None)
    if block_type == _BT_STAMP:
        return (TITLE_STAMP_NOTES, None)
    if block_type == _BT_PLAN:
        return (ARCHITECTURAL_PLAN_OR_FACADE, None)
    # обычные схемы неизвестной дисциплины + фото/прочее → fallback
    return (GENERAL, None)


def profile_production_ready(profile_id: str, subtype: Optional[str]) -> bool:
    """Есть ли рабочий extractor для (profile, subtype). Сейчас True только
    для electrical_singleline/grsh."""
    if profile_id == ELECTRICAL_SINGLELINE and subtype == "grsh":
        return True
    return False


# ─── electrical_singleline structured builder ─────────────────────────────


def build_electrical_singleline_structured(merged: dict, *, subtype: str = "grsh") -> dict:
    """Собрать универсальный structured JSON профиля electrical_singleline из
    результата feeder-merge (grsh_feeder_extraction.merge_tile_feeders).

    Группы (feeders/breakers/cables/loads/metering/compensation/earthing/
    connections) деривируются из feeders[] + equipment[], каждое значимое поле
    несёт field_state. anchor_status (verified/visual_unverified) маппится в
    field_state по NON_REMOVAL_STATES.
    """
    feeders_in = merged.get("feeders") or []
    equipment = merged.get("equipment") or []
    connections = merged.get("connections") or []

    def cell(v, verified: bool):
        if v in (None, "", "null", "unknown"):
            return field_cell(None, FieldState.NOT_EXTRACTED)
        return field_cell(v, FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED)

    feeders, breakers, cables, loads, metering = [], [], [], [], []
    compensation = []
    for f in feeders_in:
        verified = (f.get("anchor_status") == "verified")
        consumer = f.get("consumer")
        feeders.append({
            "consumer": consumer,
            "designation": f.get("designation"),
            "anchor_status": f.get("anchor_status"),
            "source_panel": f.get("source_panel"),
            "fields": {
                "breaker": cell(f.get("breaker"), verified),
                "breaker_rating": cell(f.get("breaker_rating"), verified),
                "breaking_capacity": cell(f.get("breaking_capacity"), verified),
                "cable_mark": cell(f.get("cable_mark"), verified),
                "cable_section": cell(f.get("cable_section"), verified),
                "p_calc_kw": cell(f.get("p_calc_kw"), verified),
                "i_calc_a": cell(f.get("i_calc_a"), verified),
                "ct_ratio": cell(f.get("ct_ratio"), verified),
                "metering": cell(f.get("metering"), verified),
            },
        })
        if f.get("breaker") or f.get("breaker_rating"):
            breakers.append({"ref": f.get("breaker"), "rating": f.get("breaker_rating"),
                             "breaking_capacity": f.get("breaking_capacity"),
                             "feeder": consumer, "field_state": FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED})
        if f.get("cable_mark") or f.get("cable_section"):
            cables.append({"designation": f.get("designation"), "mark": f.get("cable_mark"),
                           "section": f.get("cable_section"), "feeder": consumer,
                           "field_state": FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED})
        if f.get("p_calc_kw") or f.get("i_calc_a"):
            loads.append({"consumer": consumer, "p_calc_kw": f.get("p_calc_kw"),
                          "i_calc_a": f.get("i_calc_a"),
                          "field_state": FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED})
        if f.get("ct_ratio") or f.get("metering"):
            metering.append({"consumer": consumer, "ct_ratio": f.get("ct_ratio"),
                             "device": f.get("metering"),
                             "field_state": FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED})
        if consumer and "АУКРМ" in str(consumer).upper():
            compensation.append({"ref": consumer, "rating": f.get("p_calc_kw"),
                                 "field_state": FieldState.PRESENT if verified else FieldState.VISUAL_UNVERIFIED})

    earthing = []
    for e in equipment:
        kind = (e.get("kind") or "").lower()
        if kind == "earthing":
            earthing.append({"name": e.get("name"), "detail": e.get("detail"),
                             "field_state": FieldState.PRESENT})
        elif kind == "compensation":
            compensation.append({"ref": e.get("name"), "detail": e.get("detail"),
                                 "field_state": FieldState.PRESENT})
        elif kind == "metering":
            metering.append({"device": e.get("name"), "detail": e.get("detail"),
                             "field_state": FieldState.PRESENT})

    return {
        "profile": ELECTRICAL_SINGLELINE,
        "subtype": subtype,
        "feeders": feeders,
        "breakers": breakers,
        "cables": cables,
        "loads": loads,
        "metering": metering,
        "compensation": compensation,
        "earthing": earthing,
        "connections": connections,
        "diagnostics": merged.get("diagnostics", {}),
    }


def structured_field_state_audit(structured: dict) -> dict:
    """Сводка по field_state в structured output (для diagnostics/тестов)."""
    counts: dict[str, int] = {}
    def walk(o):
        if isinstance(o, dict):
            fs = o.get("field_state")
            if isinstance(fs, str):
                counts[fs] = counts.get(fs, 0) + 1
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(structured)
    return counts
