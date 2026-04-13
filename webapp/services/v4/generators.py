"""
candidate_generators_v2.py
---------------------------
V2 генераторы под typed_facts schema консультанта.

Реализация основана на pseudocode консультанта с моими practical fixes:
- Регексы для парсинга описаний (сч[её]?тчик)
- Нормализация line_id (латиница→кириллица)
- Умный OCR-check (исключение маленьких чисел)
- Breaker knowledge base

Генераторы:
- generate_class2_identity_candidates → #5, F-016, F-021
- generate_class3_cross_view_candidates → #2, #7, #18
- generate_class4_requirement_conflict_candidates → #55, F-008, F-023, F-030, F-031
"""

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


# ─── Load knowledge base & policy ─────────────────────────────────────────


_kb_path = Path(__file__).parent / "schemas" / "breaker_kb.json"
_policy_path = Path(__file__).parent / "schemas" / "guardrails.json"
_disciplines_dir = Path(__file__).parent.parent.parent.parent / "prompts" / "disciplines"

try:
    BREAKER_KB = json.loads(_kb_path.read_text(encoding="utf-8"))
except FileNotFoundError:
    BREAKER_KB = {"breakers": {}, "selectivity_requirements": {}}

try:
    EXPERT_POLICY = json.loads(_policy_path.read_text(encoding="utf-8"))
except FileNotFoundError:
    EXPERT_POLICY = {"expert_policy": {}, "guardrails": []}

# Кэш KB по дисциплинам
_kb_cache: dict[str, dict] = {}


def _get_knowledge_base(config: dict | None) -> dict:
    """Загрузить KB из config или fallback на EOM breaker_kb."""
    if not config:
        return BREAKER_KB
    kb_file = config.get("generators", {}).get("class4_requirement_conflict", {}).get("knowledge_base_file")
    if not kb_file:
        return {"breakers": {}}
    # Resolve relative to discipline dir
    disc = config.get("discipline", "")
    cache_key = f"{disc}/{kb_file}"
    if cache_key in _kb_cache:
        return _kb_cache[cache_key]
    kb_path = _disciplines_dir / disc / kb_file
    if not kb_path.exists():
        # Fallback: schemas/ in v4 module
        kb_path = Path(__file__).parent / kb_file
    if kb_path.exists():
        try:
            kb = json.loads(kb_path.read_text(encoding="utf-8"))
            _kb_cache[cache_key] = kb
            return kb
        except Exception:
            pass
    return BREAKER_KB  # fallback


# ─── Helpers ──────────────────────────────────────────────────────────────


def get_attr(mention: dict, attr_name: str) -> Any:
    """Извлечь value_norm атрибута по имени."""
    for attr in mention.get("attributes", []):
        if attr.get("name") == attr_name:
            return attr.get("value_norm")
    return None


def get_attr_raw(mention: dict, attr_name: str) -> Any:
    for attr in mention.get("attributes", []):
        if attr.get("name") == attr_name:
            return attr.get("value_raw")
    return None


def make_id(*parts) -> str:
    return "-".join(str(p) for p in parts if p)


# ─── Человекочитаемые имена полей (fallback — EOM) ────────────────────────

_FIELD_LABELS_DEFAULT = {
    # EOM
    "phase_section_mm2": "сечение фазной жилы",
    "phase_count": "количество фаз",
    "neutral_section_mm2": "сечение нейтрали",
    "neutral_count": "количество нейтралей",
    "pe_count": "количество PE жил",
    "pe_section_mm2": "сечение PE жилы",
    "pe_or_n_count": "количество PE/N жил",
    "pe_or_n_section_mm2": "сечение PE/N жилы",
    "cable_mark": "марка кабеля",
    "source_panel": "источник питания",
    "destination_panel": "потребитель",
    "breaker_model": "модель автомата",
    "breaker_nominal_a": "номинал автомата",
    "designation": "обозначение",
    "description": "описание",
    "trip_unit_type": "тип расцепителя",
    "identifier_uniqueness": "уникальность маркера",
    # AR / общие
    "room_name": "наименование помещения",
    "area_m2": "площадь",
    "purpose": "назначение",
    "width_mm": "ширина",
    "height_mm": "высота",
    "length_mm": "длина",
    "depth_mm": "глубина",
    "thickness_mm": "толщина",
    "count": "количество",
    "section": "сечение",
    "diameter": "диаметр",
    "material": "материал",
    "mark": "марка",
    "type": "тип",
    "category": "категория",
    "fire_rating": "огнестойкость",
    "floor": "этаж",
    "level": "отметка",
    "volume_m3": "объём",
    "weight_kg": "масса",
    "power_kw": "мощность",
    "current_a": "ток",
    "voltage_v": "напряжение",
    "slope_pct": "уклон",
    "temperature_c": "температура",
    "pressure_pa": "давление",
    "flow_m3h": "расход",
    "insulation": "утеплитель",
    "finish": "отделка",
    "coating": "покрытие",
    "color": "цвет",
}

_FIELD_UNITS_DEFAULT = {
    "phase_section_mm2": "мм²",
    "neutral_section_mm2": "мм²",
    "pe_section_mm2": "мм²",
    "pe_or_n_section_mm2": "мм²",
    "breaker_nominal_a": "А",
}


def _get_field_labels(config: dict | None) -> dict:
    if config and config.get("field_labels"):
        merged = dict(_FIELD_LABELS_DEFAULT)
        merged.update(config["field_labels"])
        return merged
    return _FIELD_LABELS_DEFAULT


def _get_field_units(config: dict | None) -> dict:
    if config and config.get("field_units"):
        merged = dict(_FIELD_UNITS_DEFAULT)
        merged.update(config["field_units"])
        return merged
    return _FIELD_UNITS_DEFAULT


def _human_field(field: str, config: dict | None = None) -> str:
    return _get_field_labels(config).get(field, field)


def _human_values(values: list, field: str, config: dict | None = None) -> str:
    """Форматирование списка значений: [120, 150] → '120 и 150 мм²'."""
    unit = _get_field_units(config).get(field, "")
    parts = [str(v) for v in values]
    joined = " и ".join(parts) if len(parts) <= 2 else ", ".join(parts[:-1]) + f" и {parts[-1]}"
    return f"{joined} {unit}".strip() if unit else joined


def looks_like_line_id(key: str) -> bool:
    """Проверить что key похож на идентификатор кабельной линии."""
    if not key:
        return False
    return bool(re.match(r"^М-?\d+[.\d]*$", key.upper()))


def build_evidence(mentions: list[dict]) -> list[dict]:
    """Собрать evidence список из mentions."""
    evidence = []
    for m in mentions:
        ctx = m.get("source_context", {})
        evidence.append({
            "mention_id": m.get("mention_id"),
            "page": ctx.get("page"),
            "sheet": ctx.get("sheet"),
            "block_id": ctx.get("block_id"),
            "view_type": ctx.get("view_type"),
            "raw_excerpt": m.get("raw_text_excerpt"),
        })
    return evidence


def value_diff_looks_like_ocr(values: list) -> bool:
    """Проверить на OCR artifact — только для строковых/больших числовых."""
    if len(values) != 2:
        return False
    a, b = str(values[0]), str(values[1])

    # Числа меньше 100 — не OCR (реальные данные)
    try:
        na, nb = int(a), int(b)
        if na < 100 and nb < 100:
            return False
    except ValueError:
        pass

    # Латиница vs кириллица в марках
    latin = set("ABCEHKMOPTXY")
    cyr = set("АВСЕНКМОРТХУ")
    if any(c in latin for c in a) and any(c in cyr for c in b):
        return True
    if any(c in cyr for c in a) and any(c in latin for c in b):
        return True

    # Одинаковая длина ≥ 3, одна цифра разная
    if len(a) == len(b) and len(a) >= 3 and a != b:
        diffs = sum(1 for x, y in zip(a, b) if x != y)
        if diffs == 1:
            return True

    return False


def mention_has_ocr_hints(mention: dict) -> bool:
    """Проверить есть ли у mention OCR-подозрения."""
    flags = mention.get("flags", {})
    if flags.get("likely_ocr_artifact"):
        return True
    for attr in mention.get("attributes", []):
        if attr.get("likely_ocr_artifact"):
            return True
    return False


def _extract_count_from_description(text: str) -> int | None:
    """Извлечь количество счётчиков из описания щита учёта."""
    if not text:
        return None
    t = text.lower()

    # Числовые: "на 12 счетчиков", "12 счётчиков"
    m = re.search(r"(\d+)\s*сч[её]?тчик", t)
    if m:
        return int(m.group(1))

    # Словарные
    word_to_num = {
        "один": 1, "одного": 1,
        "два": 2, "двух": 2, "двум": 2,
        "три": 3, "четыре": 4, "пять": 5,
        "шесть": 6, "семь": 7, "восемь": 8,
        "девять": 9, "десять": 10, "двенадцать": 12,
    }
    for word, num in word_to_num.items():
        if re.search(rf"\b{word}\s*сч[её]?тчик", t):
            return num

    return None


def _extract_count_from_panel_id(panel_id: str) -> int | None:
    """ЩУ-2/Т → 2, ЩУ-12/Т → 12."""
    if not panel_id:
        return None
    m = re.search(r"-(\d+)", panel_id)
    if m:
        return int(m.group(1))
    return None


# ─── Generator Class 2: Identity / Non-unique identifier ──────────────────


def generate_class2_identity_candidates(memory: dict, config: dict | None = None) -> list[dict]:
    """
    Класс 2: идентичность и адресация.

    Покрывает:
    - #5 M-1.5 дубль (разные destinations у одного line_id на одной схеме)
    - F-016 дубль room_no
    - F-021 дубль TA1.x
    """
    candidates = []
    mentions_by_key = memory.get("mentions_by_key", {})

    # Проверяем сущности где идентификатор должен быть уникальным
    eligible_types = ["line", "panel", "current_transformer", "room"]

    for entity_type in eligible_types:
        entities = mentions_by_key.get(entity_type, {})

        for canonical_key, mentions in entities.items():
            if len(mentions) < 2:
                continue

            # Для линий и ТТ — проверяем на одной ли схеме дубль
            if entity_type in ("line", "current_transformer"):
                # Группируем по sheet
                by_sheet = defaultdict(list)
                for m in mentions:
                    ctx = m.get("source_context", {})
                    sheet = ctx.get("sheet", "?")
                    by_sheet[sheet].append(m)

                for sheet, sheet_mentions in by_sheet.items():
                    if len(sheet_mentions) < 2:
                        continue

                    # Проверяем что у упоминаний разные destinations/характеристики
                    destinations = set()
                    signatures = set()
                    for m in sheet_mentions:
                        dest = get_attr(m, "destination_panel")
                        if dest:
                            destinations.add(dest)
                        # Signature = (dest, source, section)
                        sig = (
                            dest,
                            get_attr(m, "source_panel"),
                            get_attr(m, "phase_section_mm2"),
                        )
                        signatures.add(sig)

                    if len(destinations) > 1 or len(signatures) > 1:
                        likely_ocr = any(mention_has_ocr_hints(m) for m in sheet_mentions)
                        candidates.append({
                            "candidate_id": make_id("C2", canonical_key, sheet),
                            "issue_class_id": 2,
                            "issue_class": "identity_addressing",
                            "subtype": "non_unique_identifier",
                            "entity_key": f"{entity_type}:{canonical_key}",
                            "field": "identifier_uniqueness",
                            "matching_policy": "exact_line_id_only" if entity_type == "line" else "exact_breaker_id_only",
                            "generator": {"name": "gen_class2_identity", "version": "v2"},
                            "candidate_claim": {
                                "kind": "non_unique_identifier",
                                "summary": f"Дублирующийся маркер {canonical_key} на {sheet}: одно обозначение ведёт к разным потребителям ({', '.join(sorted(destinations))})",
                                "proposed_severity": "EXPLOITATION" if looks_like_line_id(canonical_key) else "RECOMMENDED",
                            },
                            "values": [
                                {
                                    "label": f"mention_{i+1}",
                                    "value_raw": get_attr_raw(m, "destination_panel"),
                                    "value_norm": get_attr(m, "destination_panel"),
                                    "mention_id": m.get("mention_id"),
                                }
                                for i, m in enumerate(sheet_mentions)
                            ],
                            "evidence": build_evidence(sheet_mentions),
                            "flags": {
                                "likely_ocr_artifact": likely_ocr,
                                "needs_adjacent_sections": False,
                                "out_of_scope": False,
                            },
                            "source_mention_ids": [m.get("mention_id") for m in sheet_mentions],
                            "suggested_judge_question": f"Относится ли идентификатор {canonical_key} к одной сущности или это действительно дубликат на одном листе?",
                        })

            # Для помещений и щитов — просто дубль в любом контексте
            elif entity_type in ("room", "panel"):
                # Только явные дубли в разных helpers (чтобы не ложно срабатывать на тех же самых)
                unique_contexts = set()
                for m in mentions:
                    ctx = m.get("source_context", {})
                    unique_contexts.add((ctx.get("page"), ctx.get("block_id")))

                if len(unique_contexts) >= 2 and entity_type == "room":
                    # Для помещений — проверяем что это РАЗНЫЕ помещения (разные areas/descriptions)
                    descriptions = set()
                    for m in mentions:
                        desc = get_attr(m, "description")
                        if desc:
                            descriptions.add(desc)

                    if len(descriptions) > 1:
                        candidates.append({
                            "candidate_id": make_id("C2_ROOM", canonical_key),
                            "issue_class_id": 2,
                            "issue_class": "identity_addressing",
                            "subtype": "non_unique_identifier.room_number_duplicate",
                            "entity_key": f"room:{canonical_key}",
                            "field": "room_no",
                            "matching_policy": "exact_room_no_only",
                            "candidate_claim": {
                                "kind": "non_unique_identifier",
                                "summary": f"Помещение {canonical_key} описывает несколько разных помещений",
                                "proposed_severity": "RECOMMENDED",
                            },
                            "values": [
                                {"label": f"desc_{i+1}", "value_raw": d, "value_norm": d}
                                for i, d in enumerate(descriptions)
                            ],
                            "evidence": build_evidence(mentions),
                            "flags": {
                                "likely_ocr_artifact": False,
                                "needs_adjacent_sections": False,
                                "out_of_scope": False,
                            },
                            "source_mention_ids": [m.get("mention_id") for m in mentions],
                        })

    # Также проверяем duplicate_identifier_with relations (если extractor их создал)
    for relation in memory.get("relations", []):
        if relation.get("relation_type") == "duplicate_identifier_with":
            # Это сильный сигнал от extractor-а что он явно видел дубль
            # Проверим что такого кандидата ещё нет
            from_id = relation.get("from_mention_id")
            to_id = relation.get("to_mention_id")

            # Найдём mention-ы
            from_mention = None
            to_mention = None
            for entities in memory.get("mentions_by_key", {}).values():
                for mlist in entities.values():
                    for m in mlist:
                        if m.get("mention_id") == from_id:
                            from_mention = m
                        if m.get("mention_id") == to_id:
                            to_mention = m

            if from_mention and to_mention:
                canonical_key = from_mention.get("_canonical_key", "?")
                # Если уже есть кандидат на этот key — не дублируем
                existing = [c for c in candidates if c.get("entity_key", "").endswith(canonical_key)]
                if not existing:
                    candidates.append({
                        "candidate_id": make_id("C2_REL", canonical_key),
                        "issue_class_id": 2,
                        "issue_class": "identity_addressing",
                        "subtype": "non_unique_identifier",
                        "entity_key": f"line:{canonical_key}",
                        "field": "identifier_uniqueness",
                        "matching_policy": "exact_line_id_only",
                        "generator": {"name": "gen_class2_identity_from_relation", "version": "v2"},
                        "candidate_claim": {
                            "kind": "non_unique_identifier",
                            "summary": f"Дублирующийся маркер {canonical_key}: одно обозначение используется для разных направлений на одном чертеже",
                            "proposed_severity": "EXPLOITATION",
                        },
                        "values": [
                            {"label": "mention_A", "mention_id": from_id},
                            {"label": "mention_B", "mention_id": to_id},
                        ],
                        "evidence": build_evidence([from_mention, to_mention]),
                        "flags": {
                            "likely_ocr_artifact": False,
                            "needs_adjacent_sections": False,
                            "out_of_scope": False,
                        },
                        "source_mention_ids": [from_id, to_id],
                    })

    return candidates


# ─── Generator Class 3: Cross-view consistency ────────────────────────────


_WATCH_FIELDS_DEFAULT = {
    "line": [
        "phase_section_mm2", "phase_count",
        "neutral_section_mm2", "neutral_count",
        "pe_count", "pe_section_mm2",
        "pe_or_n_section_mm2", "pe_or_n_count",
        "cable_mark", "source_panel", "destination_panel",
    ],
    "panel": ["designation", "description"],
}

_SAFETY_CRITICAL_DEFAULT = {
    "phase_section_mm2", "phase_count",
    "neutral_section_mm2", "neutral_count",
    "pe_count", "pe_section_mm2",
    "pe_or_n_count", "pe_or_n_section_mm2",
}

_TOPOLOGY_DEFAULT = {"source_panel", "destination_panel"}


def _get_watch_fields(config: dict | None) -> dict:
    if config:
        wf = config.get("generators", {}).get("class3_cross_view", {}).get("watch_fields")
        if wf:
            return wf
    return _WATCH_FIELDS_DEFAULT


def _get_safety_critical(config: dict | None) -> set:
    if config:
        sc = config.get("generators", {}).get("class3_cross_view", {}).get("safety_critical_fields")
        if sc is not None:
            return set(sc)
    return _SAFETY_CRITICAL_DEFAULT


def _get_topology_fields(config: dict | None) -> set:
    if config:
        tf = config.get("generators", {}).get("class3_cross_view", {}).get("topology_fields")
        if tf is not None:
            return set(tf)
    return _TOPOLOGY_DEFAULT


def generate_class3_cross_view_candidates(memory: dict, config: dict | None = None) -> list[dict]:
    """
    Класс 3: межлистовая консистентность.

    Покрывает:
    - #7 нейтраль 150 vs 120 → neutral_section_mm2 mismatch
    - #18 ВРУ-1 1 PE vs 2 PE → pe_count mismatch
    - #2 ЩУ-2/Т vs ЩУ-12/Т swap → pairwise swap check
    - F-002/F-003/F-004 cable mismatches
    """
    candidates = []

    WATCH_FIELDS_BY_ENTITY = _get_watch_fields(config)
    SAFETY_CRITICAL_FIELDS = _get_safety_critical(config)
    TOPOLOGY_FIELDS = _get_topology_fields(config)

    # 3A: exact attribute mismatch для line и panel
    for entity_type, fields in WATCH_FIELDS_BY_ENTITY.items():
        entities = memory.get("mentions_by_key", {}).get(entity_type, {})

        for canonical_key, mentions in entities.items():
            if len(mentions) < 2:
                continue

            for field in fields:
                # Собираем значения из разных mentions
                values_with_source = []
                for m in mentions:
                    val = get_attr(m, field)
                    if val is not None:
                        values_with_source.append((val, m))

                if len(values_with_source) < 2:
                    continue

                # Topology fields: group by view_type, only compare within same view
                if field in TOPOLOGY_FIELDS:
                    by_view: dict[str, list] = {}
                    for val, m in values_with_source:
                        view = (m.get("source_context", {}) or {}).get("view_type") or "?"
                        by_view.setdefault(view, []).append((val, m))
                    # Берём группу с наибольшим числом mentions и её же mismatch
                    # Если mismatch только между разными views — пропускаем
                    found_mismatch = False
                    for view_mentions in by_view.values():
                        if len(view_mentions) < 2:
                            continue
                        view_unique = list(set(v[0] for v in view_mentions))
                        if len(view_unique) > 1:
                            values_with_source = view_mentions
                            found_mismatch = True
                            break
                    if not found_mismatch:
                        continue

                unique_values = list(set(v[0] for v in values_with_source))
                if len(unique_values) <= 1:
                    continue

                # OCR artifact detection — для текстовых полей (cable_mark, breaker_model)
                check_ocr = field in ("cable_mark", "breaker_model", "designation")
                likely_ocr = value_diff_looks_like_ocr(unique_values) if check_ocr else False

                # OCR-нормализация уже применена в canonical_memory для cable_mark.
                # Если после normalization значения совпали — этот блок не выполнится
                # (проверка `len(unique_values) <= 1` выше).
                if likely_ocr:
                    # Всё ещё есть расхождение после нормализации — мягкая severity
                    proposed_severity = "RECOMMENDED"
                elif field in SAFETY_CRITICAL_FIELDS:
                    # Реальное расхождение в безопасностно-критичных параметрах
                    proposed_severity = "CRITICAL"
                else:
                    # Всё остальное (cable_mark после OCR, topology) — операционное
                    proposed_severity = "EXPLOITATION"

                # Человекочитаемый summary
                field_label = _human_field(field, config)
                values_str = _human_values(unique_values, field, config)
                # Генитив для "Расхождение [чего]"
                _GENITIVE = {
                    "сечение фазной жилы": "сечения фазной жилы",
                    "сечение PE/N жилы": "сечения PE/N жилы",
                    "сечение нейтрали": "сечения нейтрали",
                    "сечение PE жилы": "сечения PE жилы",
                    "количество PE/N жил": "количества PE/N жил",
                    "количество PE жил": "количества PE жил",
                    "количество фаз": "количества фаз",
                    "количество нейтралей": "количества нейтралей",
                    "марка кабеля": "марки кабеля",
                    "источник питания": "источника питания",
                    "потребитель": "потребителя",
                    # AR / общие
                    "наименование помещения": "наименования помещения",
                    "площадь": "площади",
                    "назначение": "назначения",
                    "ширина": "ширины",
                    "высота": "высоты",
                    "длина": "длины",
                    "глубина": "глубины",
                    "толщина": "толщины",
                    "количество": "количества",
                    "сечение": "сечения",
                    "диаметр": "диаметра",
                    "материал": "материала",
                    "марка": "марки",
                    "тип": "типа",
                    "категория": "категории",
                    "огнестойкость": "огнестойкости",
                    "этаж": "этажа",
                    "отметка": "отметки",
                    "объём": "объёма",
                    "масса": "массы",
                    "мощность": "мощности",
                    "ток": "тока",
                    "напряжение": "напряжения",
                    "уклон": "уклона",
                    "температура": "температуры",
                    "давление": "давления",
                    "расход": "расхода",
                    "утеплитель": "утеплителя",
                    "отделка": "отделки",
                    "покрытие": "покрытия",
                    "цвет": "цвета",
                    "обозначение": "обозначения",
                    "описание": "описания",
                }
                label_gen = _GENITIVE.get(field_label, field_label)
                _ENTITY_LABEL = {
                    "line": "кабеля",
                    "panel": "щита",
                    "breaker": "автомата",
                    "room": "помещения",
                    "door": "двери",
                    "wall": "стены",
                    "floor_type": "типа пола",
                    "window": "окна",
                    "opening": "проёма",
                    "stair": "лестницы",
                    "ramp": "рампы",
                    "elevator": "лифта",
                    "duct": "воздуховода",
                    "pipe": "трубопровода",
                    "valve": "клапана",
                    "device": "устройства",
                    "equipment": "оборудования",
                }
                ent_label = _ENTITY_LABEL.get(entity_type, entity_type)
                summary = f"Расхождение {label_gen} {ent_label} {canonical_key}: {values_str} (на разных листах проекта)"

                candidates.append({
                    "candidate_id": make_id("C3", entity_type, canonical_key, field),
                    "issue_class_id": 3,
                    "issue_class": "cross_view_consistency",
                    "subtype": "cross_view_attribute_mismatch",
                    "entity_key": f"{entity_type}:{canonical_key}",
                    "field": field,
                    "matching_policy": (
                        "exact_line_id_only" if entity_type == "line"
                        else "exact_panel_id_only"
                    ),
                    "generator": {"name": "gen_class3_cross_view", "version": "v2"},
                    "candidate_claim": {
                        "kind": "attribute_mismatch",
                        "summary": summary,
                        "proposed_severity": proposed_severity,
                    },
                    "values": [
                        {
                            "label": f"value_{i+1}",
                            "value_raw": get_attr_raw(m, field),
                            "value_norm": val,
                            "mention_id": m.get("mention_id"),
                        }
                        for i, (val, m) in enumerate(values_with_source)
                    ],
                    "evidence": build_evidence([vs[1] for vs in values_with_source]),
                    "flags": {
                        "likely_ocr_artifact": likely_ocr,
                        "needs_adjacent_sections": False,
                        "out_of_scope": False,
                    },
                    "source_mention_ids": [vs[1].get("mention_id") for vs in values_with_source],
                })

    # 3B: pairwise swap detection для ЩУ пар (class 2 для #2 ЩУ-2/Т vs ЩУ-12/Т)
    # Эвристика: ищем spec_row щитов с цифрой в id и описанием с цифрой
    candidates.extend(_detect_panel_designation_swap(memory))

    return candidates


def _detect_panel_designation_swap(memory: dict) -> list[dict]:
    """Найти pairwise swaps ЩУ-N/Т vs ЩУ-M/Т где описание и обозначение поменяны."""
    candidates = []

    spec_rows = memory.get("mentions_by_key", {}).get("spec_row", {})
    panels = memory.get("mentions_by_key", {}).get("panel", {})

    # Объединяем — и spec_rows, и panels могут содержать описания щитов
    parsed = []
    for key, mentions in {**spec_rows, **panels}.items():
        for m in mentions:
            designation = key
            description = get_attr(m, "description") or get_attr(m, "note_text")
            if not description:
                continue

            desig_count = _extract_count_from_panel_id(designation)
            desc_count = _extract_count_from_description(description)

            if desig_count is not None and desc_count is not None and desig_count != desc_count:
                parsed.append({
                    "mention": m,
                    "designation": designation,
                    "designation_count": desig_count,
                    "description": description,
                    "description_count": desc_count,
                })

    # Ищем пары где A.desig = B.desc и A.desc = B.desig
    for i in range(len(parsed)):
        for j in range(i + 1, len(parsed)):
            a, b = parsed[i], parsed[j]
            if (a["designation_count"] == b["description_count"]
                    and a["description_count"] == b["designation_count"]):
                candidates.append({
                    "candidate_id": make_id("C3_SWAP", a["designation"], b["designation"]),
                    "issue_class_id": 3,
                    "issue_class": "cross_view_consistency",
                    "subtype": "panel_designation_description_swap",
                    "entity_key": f"panel_pair:{a['designation']}|{b['designation']}",
                    "secondary_entity_key": f"panel:{b['designation']}",
                    "field": "designation_description_mapping",
                    "matching_policy": "pairwise_swap_check",
                    "generator": {"name": "gen_class3_swap_detect", "version": "v2"},
                    "candidate_claim": {
                        "kind": "designation_description_swap",
                        "summary": f"Описания щитов {a['designation']} и {b['designation']} вероятно перепутаны: первый описан как {a['description_count']}-счётчиковый, второй как {b['description_count']}-счётчиковый",
                        "proposed_severity": "CRITICAL",
                    },
                    "values": [
                        {
                            "label": a["designation"],
                            "value_raw": a["description"],
                            "value_norm": a["description_count"],
                            "mention_id": a["mention"].get("mention_id"),
                        },
                        {
                            "label": b["designation"],
                            "value_raw": b["description"],
                            "value_norm": b["description_count"],
                            "mention_id": b["mention"].get("mention_id"),
                        },
                    ],
                    "evidence": build_evidence([a["mention"], b["mention"]]),
                    "flags": {
                        "likely_ocr_artifact": False,
                        "needs_adjacent_sections": False,
                        "out_of_scope": False,
                    },
                    "source_mention_ids": [
                        a["mention"].get("mention_id"),
                        b["mention"].get("mention_id"),
                    ],
                })

    return candidates


# ─── Generator Class 4: Requirement vs selected part conflict ─────────────


def _parse_note_requirements(memory: dict) -> list[dict]:
    """Извлечь правила из текстов примечаний."""
    rules = []
    notes_dict = memory.get("mentions_by_key", {}).get("note", {})

    all_notes = []
    for mentions in notes_dict.values():
        all_notes.extend(mentions)

    for n in all_notes:
        text = get_attr(n, "note_text") or n.get("raw_text_excerpt") or ""
        text_lower = text.lower()

        # Правило 1: электронные расцепители
        if "электронн" in text_lower and "расцепител" in text_lower:
            rules.append({
                "rule_id": make_id("R", "electronic_trip"),
                "applies_to": "outgoing_breakers_grsh",
                "required_field": "trip_unit_type",
                "required_value": "электронный",
                "source_mention": n,
                "source_text": text,
            })

        # Правило 2: резерв 10% АВ
        if re.search(r"не\s*менее\s*10\s*%?\s*резервн", text_lower):
            rules.append({
                "rule_id": make_id("R", "reserve_10pct"),
                "applies_to": "grsh_configuration",
                "required_field": "reserve_breaker_count_pct",
                "required_value": 10,
                "source_mention": n,
                "source_text": text,
            })

        # Правило 3: резерв 20% места
        if re.search(r"не\s*менее\s*20\s*%?\s*резерв\w*\s*мест", text_lower):
            rules.append({
                "rule_id": make_id("R", "reserve_space_20pct"),
                "applies_to": "grsh_configuration",
                "required_field": "reserve_space_pct",
                "required_value": 20,
                "source_mention": n,
                "source_text": text,
            })

        # Правило 4: SA/SF контакты
        if ("SA" in text and "SF" in text) or re.search(r"контакт\w*\s*состояни", text_lower):
            rules.append({
                "rule_id": make_id("R", "sa_sf_contacts"),
                "applies_to": "input_and_section_breakers",
                "required_field": "aux_contacts_shown",
                "required_value": True,
                "source_mention": n,
                "source_text": text,
            })

        # Правило 5: раздельная прокладка взаиморезервируемых
        if "разн" in text_lower and ("лотк" in text_lower or "трасс" in text_lower):
            if "взаиморезервируем" in text_lower or "резервн" in text_lower:
                rules.append({
                    "rule_id": make_id("R", "redundant_separated"),
                    "applies_to": "redundant_cable_routes",
                    "required_field": "separate_trays_confirmed",
                    "required_value": True,
                    "source_mention": n,
                    "source_text": text,
                })

    return rules


def _breaker_model_trip_type(model: str, config: dict | None = None) -> str | None:
    """Получить trip_type из KB по марке АВ."""
    if not model:
        return None
    kb = _get_knowledge_base(config)
    breakers = kb.get("breakers", {})
    info = breakers.get(model.strip().upper())
    if info:
        return info.get("trip_unit_type")
    return None


def _breaker_in_scope(breaker: dict, scope_hint: str) -> bool:
    """Проверить что breaker попадает в scope (outgoing_breakers_grsh)."""
    if "outgoing" not in scope_hint.lower() and "отходящ" not in scope_hint.lower():
        return True

    # Отходящие АВ ГРЩ — в РП1/РП2 или с location ГРЩ
    location = get_attr(breaker, "position") or get_attr(breaker, "source_panel")
    if not location:
        return True  # неизвестно — не исключаем

    loc_upper = str(location).upper()
    return any(kw in loc_upper for kw in ["ГРЩ", "РП1", "РП2", "ОТХОД"])


def generate_class4_requirement_conflict_candidates(memory: dict, config: dict | None = None) -> list[dict]:
    """
    Класс 4: требование / примечание / выбранный элемент.

    Покрывает:
    - #55 ВА-335А vs электронные расцепители
    - F-008 резервные АВ
    - F-023 модульные vs электронные
    - F-030 SA/SF контакты
    - F-031 раздельная прокладка
    """
    # Собираем violations, агрегируем по (rule_id, breaker_model).
    # Результат: одно замечание на "семейство модели" вместо N штук по QF-ID.
    # Структура: groups[(rule_id, model_norm)] = {
    #   "rule": rule,
    #   "model": str,
    #   "actual_trip": str,
    #   "breakers": [br_mention, ...],
    #   "breaker_keys": [...],
    # }
    groups: dict[tuple, dict] = {}
    rules = _parse_note_requirements(memory)

    breakers_dict = memory.get("mentions_by_key", {}).get("breaker", {})

    for rule in rules:
        if rule["applies_to"] != "outgoing_breakers_grsh":
            continue

        for breaker_key, breaker_mentions in breakers_dict.items():
            for br in breaker_mentions:
                if not _breaker_in_scope(br, rule["applies_to"]):
                    continue

                model = get_attr(br, "breaker_model")
                if not model:
                    continue

                # Определяем actual trip_type: сначала из атрибутов, потом из KB
                actual = get_attr(br, "trip_unit_type")
                if not actual:
                    actual = _breaker_model_trip_type(model, config)
                if not actual:
                    continue

                required = rule["required_value"]
                actual_norm = str(actual).lower()
                required_norm = str(required).lower()

                if actual_norm == required_norm or "термомагнит" not in actual_norm:
                    continue

                # Есть violation — агрегируем
                model_key = str(model).strip().upper()
                gkey = (rule["rule_id"], model_key)
                if gkey not in groups:
                    groups[gkey] = {
                        "rule": rule,
                        "model": model,
                        "actual_trip": actual,
                        "required": required,
                        "breakers": [],
                        "breaker_keys": [],
                    }
                groups[gkey]["breakers"].append(br)
                if breaker_key not in groups[gkey]["breaker_keys"]:
                    groups[gkey]["breaker_keys"].append(breaker_key)

    candidates = []
    for (rule_id, model_key), g in groups.items():
        rule = g["rule"]
        breakers = g["breakers"]
        breaker_keys = g["breaker_keys"]
        model = g["model"]
        actual = g["actual_trip"]
        required = g["required"]

        # Entity key: breaker_family вместо конкретного QF-ID
        entity_key_val = f"breaker_family:{model_key}"

        # Summary: перечисляем затронутые QF (первые 10), указываем общее число
        qf_list = breaker_keys[:10]
        qf_str = ", ".join(qf_list)
        suffix = f" +{len(breaker_keys) - 10}" if len(breaker_keys) > 10 else ""
        summary = (
            f"Автоматы серии {model} ({actual}) — {len(breaker_keys)} шт. "
            f"[{qf_str}{suffix}] не соответствуют требованию примечания: "
            f"'{rule['source_text'][:120]}'"
        )

        # Evidence: все breakers + 1 note. build_evidence сам дедуплицирует по block_id
        evidence_mentions = list(breakers) + [rule["source_mention"]]

        candidates.append({
            "candidate_id": make_id("C4", "family", model_key, rule["rule_id"]),
            "issue_class_id": 4,
            "issue_class": "requirement_compatibility",
            "subtype": "requirement_selected_part_conflict",
            "entity_key": entity_key_val,
            "field": "trip_unit_type",
            "matching_policy": "rule_applicability_check",
            "generator": {"name": "gen_class4_req_conflict", "version": "v2_aggregated"},
            "candidate_claim": {
                "kind": "requirement_conflict",
                "summary": summary,
                "proposed_severity": "CRITICAL",
            },
            "values": [
                {
                    "label": "required",
                    "value_raw": rule["source_text"],
                    "value_norm": required,
                },
                {
                    "label": "actual",
                    "value_raw": model,
                    "value_norm": actual,
                },
                {
                    "label": "affected_breakers",
                    "value_raw": breaker_keys,
                    "value_norm": len(breaker_keys),
                },
            ],
            "evidence": build_evidence(evidence_mentions),
            "flags": {
                "likely_ocr_artifact": False,
                "needs_adjacent_sections": False,
                "out_of_scope": False,
                "aggregated_count": len(breaker_keys),
            },
            "source_mention_ids": [br.get("mention_id") for br in breakers]
                + [rule["source_mention"].get("mention_id")],
        })

    return candidates


# ─── Aggregator ───────────────────────────────────────────────────────────


def generate_all_candidates(memory: dict, config: dict | None = None) -> dict:
    """Запустить все генераторы."""
    generators = [
        ("class2_identity", generate_class2_identity_candidates),
        ("class3_cross_view", generate_class3_cross_view_candidates),
        ("class4_requirement_conflict", generate_class4_requirement_conflict_candidates),
    ]

    results = {
        "by_generator": {},
        "all_candidates": [],
        "stats": {},
    }

    for name, gen in generators:
        try:
            candidates = gen(memory, config=config)
        except TypeError:
            candidates = gen(memory)
        except Exception as e:
            print(f"[ERROR] Generator {name} failed: {e}", file=sys.stderr)
            candidates = []
        results["by_generator"][name] = candidates
        results["all_candidates"].extend(candidates)
        results["stats"][name] = len(candidates)

    results["stats"]["total"] = len(results["all_candidates"])
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("project")
    parser.add_argument("--memory", help="Путь к canonical_memory_v2.json")
    parser.add_argument("--output", help="Путь к output candidates.json")
    args = parser.parse_args()

    project_dir = Path(args.project)
    mem_path = Path(args.memory) if args.memory else project_dir / "_output" / "canonical_memory_v2.json"

    if not mem_path.exists():
        print(f"ERROR: {mem_path} не найден. Сначала build_canonical_memory_v2.py", file=sys.stderr)
        sys.exit(1)

    memory = json.loads(mem_path.read_text(encoding="utf-8"))
    results = generate_all_candidates(memory)

    print(f"\n=== Candidates v2 ===")
    for name, count in results["stats"].items():
        print(f"  {name}: {count}")

    out_path = Path(args.output) if args.output else project_dir / "_output" / "candidates_v2.json"
    out_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
