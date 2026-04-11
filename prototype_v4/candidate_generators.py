"""
candidate_generators.py
------------------------
Генераторы кандидатов противоречий по canonical memory.

4 генератора покрывают 5 definite KEEPs:
- gen_duplicate_marker    → #5 (дубль М-1.5)
- gen_identity_swap       → #2 (ЩУ-2/Т vs ЩУ-12/Т)
- gen_cable_mismatch      → #7 (нейтраль 150/120), #18 (ВРУ-1 1/2 PE)
- gen_note_equipment_conflict → #55 (ВА-335А vs "электронные расцепители")

Каждый генератор возвращает список candidates.
Candidates НЕ являются финальными findings — они передаются в judge stage.

Использование:
    from candidate_generators import generate_all_candidates
    from build_canonical_memory import CanonicalMemory

    memory = build_from_extractions(project_dir)
    candidates = generate_all_candidates(memory)
"""

import json
import re
import sys
from pathlib import Path
from typing import Any


# ─── Utilities ────────────────────────────────────────────────────────────


def _unique_values(attr_values: list[dict]) -> list:
    """Извлечь уникальные значения из списка атрибутов (игнорируя alternate)."""
    return list(set(v["value"] for v in attr_values if not v.get("is_alternate")))


def _group_by_source_type(mentions: list[dict]) -> dict[str, list[dict]]:
    """Группировать mentions по типу источника (sheet_type)."""
    result = {}
    for m in mentions:
        stype = (m.get("source") or {}).get("sheet_type", "unknown")
        result.setdefault(stype, []).append(m)
    return result


def _extract_number_from_description(text: str) -> int | None:
    """Извлечь число счётчиков/позиций из описания.

    Примеры:
      "Шкаф учета на 12 счетчиков" → 12
      "на два счетчика" → 2
      "ЩУ-2/Т" → None (не описание)
    """
    if not text:
        return None

    # Словарные числительные
    word_to_num = {
        "один": 1, "одного": 1, "одному": 1,
        "два": 2, "двух": 2, "двум": 2,
        "три": 3, "трёх": 3,
        "четыре": 4, "пять": 5, "шесть": 6,
        "семь": 7, "восемь": 8, "девять": 9,
        "десять": 10, "двенадцать": 12,
    }
    text_lower = text.lower()

    # Сначала числовые упоминания рядом со "счёт"/"счет"
    m = re.search(r"(\d+)\s*сч[её]?тчик", text_lower)
    if m:
        return int(m.group(1))

    # Словарные
    for word, num in word_to_num.items():
        if re.search(rf"\b{word}\s*сч[её]?тчик", text_lower):
            return num

    return None


def _extract_number_from_id(panel_id: str) -> int | None:
    """Извлечь число из идентификатора щита.

    Примеры:
      "ЩУ-2/Т"   → 2
      "ЩУ-12/Т"  → 12
      "ЩУ-2"     → 2
      "ВРУ-1"    → 1
    """
    if not panel_id:
        return None
    m = re.search(r"-(\d+)", panel_id)
    if m:
        return int(m.group(1))
    return None


def _make_candidate(
    class_: int,
    subtype: str,
    entity_id: str,
    field: str | None = None,
    severity: str = "КРИТИЧЕСКОЕ",
    category: str = "documentation",
    evidence: list[dict] | None = None,
    explanation: str = "",
    filter_flags: dict | None = None,
) -> dict:
    """Создать candidate в стандартном формате."""
    return {
        "class": class_,
        "subtype": subtype,
        "entity_id": entity_id,
        "field": field,
        "auto_severity": severity,
        "auto_category": category,
        "evidence": evidence or [],
        "explanation": explanation,
        "filter_flags": filter_flags or {
            "likely_ocr_artifact": False,
            "scope_boundary_ok": True,
        },
        "needs_llm_judge": True,
    }


# ─── Filter Rules (from v4 taxonomy) ──────────────────────────────────────


def is_likely_ocr_artifact(values: list) -> bool:
    """Проверить не является ли расхождение OCR-артефактом.

    Rule F2 из v4:
    - Латиница vs кириллица в одинаковых марках
    - Одна цифра в большом числе (648 vs 448), минимум 3 цифры — маленькие
      числа (1 vs 2) это реальные проектные значения
    - Потеря символа в шифре
    """
    if len(values) != 2:
        return False

    a, b = str(values[0]), str(values[1])

    # Если оба значения — числа, они должны быть достаточно большими
    # чтобы «одна цифра разная» считалась OCR-артефактом.
    # 1 vs 2 — это реальные значения (pe_count), не OCR.
    # 648 vs 448 — очень похоже на OCR (большие числа).
    try:
        na, nb = int(a), int(b)
        # Если оба меньше 100 — это не OCR артефакт
        if na < 100 and nb < 100:
            return False
    except ValueError:
        pass  # нечисловые значения

    # Латиница/кириллица confusion в марке
    latin_chars = set("ABCEHKMOPTXY")
    cyr_chars = set("АВСЕНКМОРТХУ")
    if any(c in latin_chars for c in a) and any(c in cyr_chars for c in b):
        return True
    if any(c in cyr_chars for c in a) and any(c in latin_chars for c in b):
        return True

    # Одинаковая длина ≥ 3, одна цифра разная
    if len(a) == len(b) and len(a) >= 3 and a != b:
        diffs = sum(1 for x, y in zip(a, b) if x != y)
        if diffs == 1:
            # 648 vs 448 — одна цифра, большие числа
            return True

    return False


# ─── Generator 1: Duplicate Marker (Class 2, покрывает #5) ────────────────


def gen_duplicate_marker(memory: dict) -> list[dict]:
    """Generator для класса 2.duplicate_marker.

    Ищет случаи когда один и тот же маркер линии (М-1.5) используется
    для разных назначений на ОДНОЙ схеме.

    Покрывает: #5 (дубль М-1.5 для ВРУ-ИТП и ВРУ-НС)
    """
    candidates = []

    for line_id, cable in memory.get("cables", {}).items():
        destinations_attr = cable.get("attributes", {}).get("destination", [])
        if len(destinations_attr) < 2:
            continue

        # Группируем по sheet — дубль должен быть на ОДНОЙ схеме
        by_sheet = {}
        for dv in destinations_attr:
            sheet = (dv.get("source") or {}).get("sheet", "?")
            by_sheet.setdefault(sheet, []).append(dv)

        for sheet, values_on_sheet in by_sheet.items():
            destinations = list(set(v["value"] for v in values_on_sheet if v.get("value")))
            if len(destinations) > 1:
                # Нашли: на одном листе маркер М-1.5 ведёт к разным destinations
                evidence = [
                    {
                        "value": v["value"],
                        "source": v.get("source"),
                        "raw_quote": v.get("raw_quote"),
                    }
                    for v in values_on_sheet
                ]
                candidates.append(_make_candidate(
                    class_=2,
                    subtype="duplicate_marker",
                    entity_id=line_id,
                    field="destination",
                    severity="КРИТИЧЕСКОЕ",
                    category="documentation",
                    evidence=evidence,
                    explanation=(
                        f"Маркер линии {line_id} использован для разных назначений "
                        f"на {sheet}: {', '.join(destinations)}. "
                        f"Согласно ГОСТ 2.710-81 позиционные обозначения должны "
                        f"быть уникальными."
                    ),
                ))

    return candidates


# ─── Generator 2: Identity Swap (Class 2, покрывает #2) ───────────────────


def gen_identity_swap(memory: dict) -> list[dict]:
    """Generator для класса 2.swapped_descriptions.

    Ищет случаи когда description щита содержит число (количество счётчиков),
    которое не соответствует идентификатору.

    Пример:
      ЩУ-2/Т описан как "Шкаф на 12 счётчиков" → mismatch (ожидалось 2)
      ЩУ-12/Т описан как "Шкаф на 2 счётчика" → mismatch (ожидалось 12)

    Покрывает: #2 (ЩУ-2/Т vs ЩУ-12/Т перепутаны)
    """
    candidates = []
    # Только для panel_id в которых есть число в идентификаторе (ЩУ-N, ЩУ-12)
    panel_mismatches = []

    for panel_id, panel in memory.get("panels", {}).items():
        expected_count = _extract_number_from_id(panel_id)
        if expected_count is None:
            continue
        # Только для ЩУ-*/Т (шкафы учёта) — для них number = кол-во счётчиков
        if not panel_id.startswith("ЩУ"):
            continue

        descriptions = panel.get("attributes", {}).get("description", [])
        for d in descriptions:
            desc_text = d.get("value", "")
            actual_count = _extract_number_from_description(desc_text)
            if actual_count is None:
                continue
            if actual_count != expected_count:
                panel_mismatches.append({
                    "panel_id": panel_id,
                    "expected": expected_count,
                    "actual": actual_count,
                    "description": desc_text,
                    "source": d.get("source"),
                    "raw_quote": d.get("raw_quote"),
                })

    # Если нашли 2+ mismatch — проверяем может ли это быть swap
    if len(panel_mismatches) >= 2:
        # Ищем пары где expected_A = actual_B и actual_A = expected_B
        for i, m_a in enumerate(panel_mismatches):
            for m_b in panel_mismatches[i + 1:]:
                if (m_a["expected"] == m_b["actual"] and
                        m_a["actual"] == m_b["expected"]):
                    # Swap detected!
                    evidence = [
                        {
                            "entity": m_a["panel_id"],
                            "expected_count": m_a["expected"],
                            "actual_count": m_a["actual"],
                            "description": m_a["description"],
                            "source": m_a["source"],
                            "raw_quote": m_a["raw_quote"],
                        },
                        {
                            "entity": m_b["panel_id"],
                            "expected_count": m_b["expected"],
                            "actual_count": m_b["actual"],
                            "description": m_b["description"],
                            "source": m_b["source"],
                            "raw_quote": m_b["raw_quote"],
                        },
                    ]
                    candidates.append(_make_candidate(
                        class_=2,
                        subtype="swapped_descriptions",
                        entity_id=f"{m_a['panel_id']} ↔ {m_b['panel_id']}",
                        field="description",
                        severity="РЕКОМЕНДАТЕЛЬНОЕ",
                        category="documentation",
                        evidence=evidence,
                        explanation=(
                            f"Описания щитов {m_a['panel_id']} и {m_b['panel_id']} "
                            f"перепутаны в спецификации: {m_a['panel_id']} описан как "
                            f"{m_a['actual']}-счётчиковый (должен быть {m_a['expected']}), "
                            f"и наоборот."
                        ),
                    ))
    else:
        # Одиночный mismatch — просто поднимем как низкий confidence
        for m in panel_mismatches:
            candidates.append(_make_candidate(
                class_=2,
                subtype="description_count_mismatch",
                entity_id=m["panel_id"],
                field="description",
                severity="РЕКОМЕНДАТЕЛЬНОЕ",
                category="documentation",
                evidence=[{
                    "entity": m["panel_id"],
                    "expected_count": m["expected"],
                    "actual_count": m["actual"],
                    "description": m["description"],
                    "source": m["source"],
                    "raw_quote": m["raw_quote"],
                }],
                explanation=(
                    f"Описание щита {m['panel_id']} упоминает {m['actual']} счётчиков, "
                    f"но ID указывает на {m['expected']}."
                ),
            ))

    return candidates


# ─── Generator 3: Cable Mismatch (Class 3, покрывает #7, #18) ─────────────


def gen_cable_mismatch(memory: dict) -> list[dict]:
    """Generator для класса 3.cable_composition_mismatch.

    Ищет случаи когда атрибуты одного и того же кабеля (по точному line_id)
    различаются между источниками.

    Покрывает:
      #7 нейтраль 150 vs 120 → pe_or_n_section_mm2 mismatch
      #18 ВРУ-1 1 PE vs 2 PE → pe_or_n_count mismatch
    """
    candidates = []

    # Критичные поля для сравнения
    critical_fields = [
        ("mark", "cable"),
        ("phase_count", "cable"),
        ("phase_section_mm2", "cable"),
        ("pe_or_n_count", "cable"),
        ("pe_or_n_section_mm2", "cable"),
    ]

    for line_id, cable in memory.get("cables", {}).items():
        attributes = cable.get("attributes", {})

        # Правило F6 — проверяем что у нас минимум 2 разных источника
        if len(cable.get("mentions", [])) < 2:
            continue

        for field, category in critical_fields:
            field_values = attributes.get(field, [])
            if len(field_values) < 2:
                continue

            unique_values = _unique_values(field_values)
            if len(unique_values) <= 1:
                continue

            # Правило F2 — проверка на OCR артефакт
            # Для cable_composition_mismatch применяем ТОЛЬКО к mark (марки кабелей),
            # не к числовым полям. Реальные расхождения сечений 150 vs 120 нельзя
            # спутать с OCR, а в марках ПО-Latin vs ПО-Cyrillic — частый случай.
            ocr_artifact = False
            if field == "mark":
                ocr_artifact = is_likely_ocr_artifact(unique_values)

            # Формируем evidence из всех источников значений
            evidence = []
            for v in field_values:
                if v.get("is_alternate"):
                    continue
                evidence.append({
                    "value": v["value"],
                    "source": v.get("source"),
                    "raw_quote": v.get("raw_quote"),
                    "confidence": v.get("confidence"),
                })

            candidates.append(_make_candidate(
                class_=3,
                subtype="cable_composition_mismatch",
                entity_id=line_id,
                field=field,
                severity="КРИТИЧЕСКОЕ" if not ocr_artifact else "РЕКОМЕНДАТЕЛЬНОЕ",
                category=category,
                evidence=evidence,
                explanation=(
                    f"Кабель {line_id} имеет разные значения поля {field}: "
                    f"{unique_values}. "
                    f"{'ВОЗМОЖНЫЙ OCR-АРТЕФАКТ (латиница/кириллица).' if ocr_artifact else ''}"
                ),
                filter_flags={
                    "likely_ocr_artifact": ocr_artifact,
                    "scope_boundary_ok": True,
                },
            ))

    return candidates


# ─── Generator 4: Note vs Equipment Conflict (Class 4, покрывает #55) ────


# Паттерны для парсинга требований из примечаний
NOTE_RULE_PATTERNS = [
    {
        "pattern": r"электронн\w*\s*расцепител",
        "requirement": {"field": "trip_type", "required_value": "электронный"},
        "scope_hint": "отходящие АВ",
    },
    {
        "pattern": r"с\s*FR|индекс\w*\s*FR|с\s*огнестойкост",
        "requirement": {"field": "fire_index", "required_value": "FR"},
        "scope_hint": "кабели СПЗ",
    },
    {
        "pattern": r"класс\w*\s*точност\w*\s*не\s*хуже\s*0[.,]\s*5\s*S",
        "requirement": {"field": "accuracy_class", "required_value": "0.5S"},
        "scope_hint": "трансформаторы тока учёта",
    },
]


def _parse_note_rule(note_text: str) -> dict | None:
    """Распарсить правило из текста примечания.

    Возвращает {requirement, scope_hint} или None если правило не распознано.

    В production это должен делать LLM. Здесь regex-based MVP.
    """
    if not note_text:
        return None
    text_lower = note_text.lower()
    for pattern_def in NOTE_RULE_PATTERNS:
        if re.search(pattern_def["pattern"], text_lower):
            return {
                "requirement": pattern_def["requirement"],
                "scope_hint": pattern_def["scope_hint"],
                "matched_pattern": pattern_def["pattern"],
            }
    return None


def _breaker_matches_trip_requirement(breaker_attrs: dict, required_type: str) -> bool:
    """Проверить соответствует ли АВ требованию по типу расцепителя."""
    trip_values = breaker_attrs.get("trip_type", [])
    if not trip_values:
        return True  # неизвестно — не нарушение
    actual_types = set(v["value"] for v in trip_values)
    if required_type in actual_types:
        return True
    # Если явно указан другой тип — нарушение
    if "термомагнитный" in actual_types and required_type == "электронный":
        return False
    return True  # неопределённость → в judge


def _breaker_in_scope(breaker_attrs: dict, scope_hint: str) -> bool:
    """Проверить что АВ попадает в scope примечания."""
    locations = set(
        v["value"] for v in breaker_attrs.get("location", [])
    )
    # "отходящие АВ" → location: ГРЩ, РП1, РП2
    if "отходящие" in scope_hint.lower():
        outgoing_locations = {"ГРЩ", "РП1", "РП2"}
        return bool(locations & outgoing_locations)
    return True


def gen_note_equipment_conflict(memory: dict) -> list[dict]:
    """Generator для класса 4.note_vs_equipment_type.

    Для каждого примечания:
    1. Парсит требование из текста (regex MVP)
    2. Находит сущности в scope примечания
    3. Проверяет соответствуют ли они требованию

    Покрывает: #55 (ВА-335А термомагнитный vs "электронные расцепители")
    """
    candidates = []

    for note in memory.get("notes", []):
        rule = _parse_note_rule(note.get("text", ""))
        if not rule:
            continue

        required_field = rule["requirement"]["field"]
        required_value = rule["requirement"]["required_value"]
        scope_hint = rule["scope_hint"]

        # Только для trip_type пока реализовано
        if required_field != "trip_type":
            continue

        # Ищем АВ в scope
        for breaker_id, breaker in memory.get("breakers", {}).items():
            breaker_attrs = breaker.get("attributes", {})

            if not _breaker_in_scope(breaker_attrs, scope_hint):
                continue

            if _breaker_matches_trip_requirement(breaker_attrs, required_value):
                continue

            # Нашли нарушение
            evidence = [
                {
                    "source": "note",
                    "note_text": note.get("text", ""),
                    "note_source": note.get("source"),
                },
                {
                    "source": "breaker",
                    "breaker_id": breaker_id,
                    "models": [v["value"] for v in breaker_attrs.get("model", [])],
                    "actual_trip_types": [v["value"] for v in breaker_attrs.get("trip_type", [])],
                    "breaker_source": breaker.get("mentions", [{}])[0].get("source"),
                },
            ]

            candidates.append(_make_candidate(
                class_=4,
                subtype="note_vs_equipment_type",
                entity_id=breaker_id,
                field="trip_type",
                severity="ЭКСПЛУАТАЦИОННОЕ",
                category="protection",
                evidence=evidence,
                explanation=(
                    f"Автомат {breaker_id} (тип: {evidence[1]['actual_trip_types']}) "
                    f"не соответствует требованию из примечания "
                    f"'{note.get('text', '')[:100]}...'"
                ),
            ))

    return candidates


# ─── Main aggregator ──────────────────────────────────────────────────────


def generate_all_candidates(memory: dict) -> dict:
    """Запустить все 4 генератора и вернуть объединённый результат."""
    generators = [
        ("duplicate_marker", gen_duplicate_marker),
        ("identity_swap", gen_identity_swap),
        ("cable_mismatch", gen_cable_mismatch),
        ("note_equipment_conflict", gen_note_equipment_conflict),
    ]

    results = {
        "by_generator": {},
        "all_candidates": [],
        "stats": {},
    }

    for name, gen in generators:
        candidates = gen(memory)
        results["by_generator"][name] = candidates
        results["all_candidates"].extend(candidates)
        results["stats"][name] = len(candidates)

    results["stats"]["total"] = len(results["all_candidates"])
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate candidates from canonical memory")
    parser.add_argument("project", help="Путь к проекту")
    parser.add_argument("--output", help="Куда сохранить (default: _output/candidates.json)")
    args = parser.parse_args()

    project_dir = Path(args.project)
    memory_path = project_dir / "_output" / "canonical_memory.json"
    if not memory_path.exists():
        print(f"ERROR: {memory_path} не найден. Сначала запусти build_canonical_memory.py",
              file=sys.stderr)
        sys.exit(1)

    memory = json.loads(memory_path.read_text(encoding="utf-8"))
    results = generate_all_candidates(memory)

    print(f"\n=== Candidates generated ===")
    for name, count in results["stats"].items():
        print(f"  {name}: {count}")

    output_path = Path(args.output) if args.output else project_dir / "_output" / "candidates.json"
    output_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
