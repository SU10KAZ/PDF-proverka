"""
test_generators_v2.py
----------------------
Синтетический тест v2 генераторов под typed_facts schema.

Проверяет что генераторы ловят 4 из 5 definite KEEPs:
- #2 ЩУ-2/Т ↔ ЩУ-12/Т swap
- #5 М-1.5 duplicate marker
- #7 нейтраль 150 vs 120
- #18 ВРУ-1 1 PE vs 2 PE
- #55 ВА-335А vs note

(#55 покрывается через breaker_kb lookup)
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_canonical_memory_v2 import CanonicalMemory
from candidate_generators_v2 import generate_all_candidates


def make_source_context(block_id, page, sheet, view_type):
    return {
        "page": page,
        "sheet": sheet,
        "block_id": block_id,
        "view_type": view_type,
        "bbox": {"x1": 0.1, "y1": 0.1, "x2": 0.5, "y2": 0.5},
    }


def make_attribute(name, value_norm, value_raw=None, value_type="string"):
    return {
        "name": name,
        "value_type": value_type,
        "value_raw": value_raw if value_raw is not None else str(value_norm),
        "value_norm": value_norm,
        "unit": None,
        "value_source": "vision",
        "parse_confidence": 0.9,
        "ambiguous_values": [],
        "likely_ocr_artifact": False,
    }


def make_mention(mention_id, entity_type, normalized_label, exact_keys, source_context, attributes):
    return {
        "mention_id": mention_id,
        "entity_type": entity_type,
        "normalized_label": normalized_label,
        "raw_label": normalized_label,
        "exact_keys": exact_keys,
        "source_context": source_context,
        "attributes": attributes,
        "raw_text_excerpt": None,
        "flags": {
            "likely_ocr_artifact": False,
            "needs_adjacent_sections": False,
            "out_of_scope_ref": False,
        },
        "confidence": 0.9,
    }


def build_mock_typed_facts():
    """Построить mock typed_facts имитирующий правильное извлечение из ГРЩ."""
    memory = CanonicalMemory()

    source_schema = make_source_context("block_schema", 7, "Лист 3", "single_line_scheme")
    source_node1 = make_source_context("block_node1", 14, "Лист 10", "cable_layout_node")
    source_node2 = make_source_context("block_node2", 14, "Лист 10", "cable_layout_node")
    source_spec = make_source_context("block_spec", 15, "Лист 11", "specification")

    # ── #18 ВРУ-1 1 PE vs 2 PE (cross-view mismatch pe_count) ──
    m001 = make_mention(
        "M-001", "line", "М-1.1",
        {"line_id": "М-1.1"},
        source_schema,
        [
            make_attribute("cable_mark", "ППГнг(А)-HF"),
            make_attribute("phase_count", 8, value_type="integer"),
            make_attribute("phase_section_mm2", 185, value_type="integer"),
            make_attribute("pe_count", 1, value_type="integer"),  # ← schema: 1 PE
            make_attribute("pe_section_mm2", 185, value_type="integer"),
        ],
    )
    m002 = make_mention(
        "M-002", "line", "М-1.1",
        {"line_id": "М-1.1"},
        source_node1,
        [
            make_attribute("cable_mark", "ППГнг(А)-HF"),
            make_attribute("phase_count", 8, value_type="integer"),
            make_attribute("phase_section_mm2", 185, value_type="integer"),
            make_attribute("pe_count", 2, value_type="integer"),  # ← node: 2 PE
            make_attribute("pe_section_mm2", 185, value_type="integer"),
        ],
    )
    memory.add_mention(m001, "mock")
    memory.add_mention(m002, "mock")

    # ── #7 нейтраль ВРУ-4 150 vs 120 (cross-view mismatch pe_section_mm2) ──
    m003 = make_mention(
        "M-003", "line", "М-1.4",
        {"line_id": "М-1.4"},
        source_schema,
        [
            make_attribute("cable_mark", "ППГнг(А)-HF"),
            make_attribute("phase_count", 4, value_type="integer"),
            make_attribute("phase_section_mm2", 240, value_type="integer"),
            make_attribute("pe_count", 1, value_type="integer"),
            make_attribute("pe_section_mm2", 150, value_type="integer"),  # ← schema: 150
        ],
    )
    m004 = make_mention(
        "M-004", "line", "М-1.4",
        {"line_id": "М-1.4"},
        source_node1,
        [
            make_attribute("cable_mark", "ППГнг(А)-HF"),
            make_attribute("phase_count", 4, value_type="integer"),
            make_attribute("phase_section_mm2", 240, value_type="integer"),
            make_attribute("pe_count", 1, value_type="integer"),
            make_attribute("pe_section_mm2", 120, value_type="integer"),  # ← node: 120
        ],
    )
    memory.add_mention(m003, "mock")
    memory.add_mention(m004, "mock")

    # ── #5 М-1.5 duplicate marker (на ОДНОЙ схеме) ──
    m005 = make_mention(
        "M-005", "line", "М-1.5",
        {"line_id": "М-1.5"},
        source_schema,
        [
            make_attribute("destination_panel", "ВРУ-ИТП"),
            make_attribute("phase_section_mm2", 120, value_type="integer"),
        ],
    )
    m006 = make_mention(
        "M-006", "line", "М-1.5",
        {"line_id": "М-1.5"},
        source_schema,  # та же схема
        [
            make_attribute("destination_panel", "ВРУ-НС"),  # ← другой destination!
            make_attribute("phase_section_mm2", 16, value_type="integer"),
        ],
    )
    memory.add_mention(m005, "mock")
    memory.add_mention(m006, "mock")

    # relation duplicate_identifier_with (extractor явно заметил дубль)
    memory.add_relation({
        "relation_id": "R-001",
        "relation_type": "duplicate_identifier_with",
        "from_mention_id": "M-005",
        "to_mention_id": "M-006",
        "confidence": 0.95,
        "evidence_refs": [source_schema],
    })

    # ── Нормальный кабель (не должен давать кандидатов) ──
    m007 = make_mention(
        "M-007", "line", "М-2.2",
        {"line_id": "М-2.2"},
        source_schema,
        [make_attribute("phase_section_mm2", 150, value_type="integer")],
    )
    m008 = make_mention(
        "M-008", "line", "М-2.2",
        {"line_id": "М-2.2"},
        source_node2,
        [make_attribute("phase_section_mm2", 150, value_type="integer")],
    )
    memory.add_mention(m007, "mock")
    memory.add_mention(m008, "mock")

    # ── #55 ВА-335А vs "электронные расцепители" ──
    m009 = make_mention(
        "M-009", "breaker", "QF1.1",
        {"breaker_id": "QF1.1"},
        source_schema,
        [
            make_attribute("breaker_model", "ВА-335А"),  # термомагнитный по KB
            make_attribute("breaker_nominal_a", 630, value_type="integer"),
            make_attribute("position", "ГРЩ"),
            # trip_unit_type НЕ указан напрямую — будет lookup в KB
        ],
    )
    memory.add_mention(m009, "mock")

    m010 = make_mention(
        "M-010", "note", "note_trip_unit",
        {},
        source_schema,
        [
            make_attribute(
                "note_text",
                "Автоматические выключатели отходящих линий ГРЩ применить с электронными расцепителями, обладающими селективной задержкой срабатывания от КЗ.",
            ),
        ],
    )
    # Note используется через mentions_by_key, но у него нет entity_type canonical_key
    # Переопределим вручную чтобы note попал в memory
    memory.mentions_by_key["note"]["note_trip_unit"].append({
        **m010,
        "_canonical_key": "note_trip_unit",
    })
    memory._all_mentions_by_id["M-010"] = m010

    # ── #2 ЩУ-2/Т vs ЩУ-12/Т swap ──
    m011 = make_mention(
        "M-011", "spec_row", "ЩУ-2/Т",
        {"spec_position": "ЩУ-2/Т"},
        source_spec,
        [
            make_attribute("designation", "ЩУ-2/Т"),
            make_attribute("description", "Шкаф учета на 12 счетчиков трансф.вкл."),  # должно быть 2, а 12
        ],
    )
    m012 = make_mention(
        "M-012", "spec_row", "ЩУ-12/Т",
        {"spec_position": "ЩУ-12/Т"},
        source_spec,
        [
            make_attribute("designation", "ЩУ-12/Т"),
            make_attribute("description", "Шкаф учета на два счетчика трансф.вкл."),  # должно быть 12, а 2
        ],
    )
    memory.add_mention(m011, "mock")
    memory.add_mention(m012, "mock")

    return memory


def run_test():
    memory = build_mock_typed_facts()

    print("=" * 70)
    print("ТЕСТ V2 ГЕНЕРАТОРОВ (typed_facts schema)")
    print("=" * 70)

    stats = memory.stats()
    print(f"\nMemory stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    results = generate_all_candidates(memory.to_dict())

    print(f"\n=== Generated candidates ===")
    for gen_name, count in results["stats"].items():
        print(f"  {gen_name}: {count}")

    print(f"\n=== Details ===")
    for gen_name, candidates in results["by_generator"].items():
        print(f"\n-- {gen_name} --")
        for c in candidates:
            print(f"  [class={c['issue_class_id']}] {c['subtype']}: {c['entity_key']}")
            print(f"    field: {c.get('field')}")
            print(f"    severity: {c['candidate_claim']['proposed_severity']}")
            print(f"    summary: {c['candidate_claim']['summary'][:140]}")

    # KEEP coverage check
    print(f"\n=== KEEP coverage ===")
    keep_checks = [
        ("#2 ЩУ-2/Т swap",
         lambda c: c["subtype"] == "panel_designation_description_swap"
                   and "ЩУ-2/Т" in c["entity_key"]
                   and "ЩУ-12/Т" in c["entity_key"]),
        ("#5 М-1.5 duplicate",
         lambda c: c["subtype"] == "non_unique_identifier"
                   and "М-1.5" in c["entity_key"]),
        ("#7 нейтраль 150/120",
         lambda c: c["subtype"] == "cross_view_attribute_mismatch"
                   and c["entity_key"] == "line:М-1.4"
                   and c.get("field") == "pe_section_mm2"),
        ("#18 ВРУ-1 pe_count",
         lambda c: c["subtype"] == "cross_view_attribute_mismatch"
                   and c["entity_key"] == "line:М-1.1"
                   and c.get("field") == "pe_count"),
        ("#55 ВА-335А vs note",
         lambda c: c["subtype"] == "requirement_selected_part_conflict"
                   and "QF1.1" in c["entity_key"]),
    ]

    all_candidates = results["all_candidates"]
    passed = 0
    failed = []
    for name, check in keep_checks:
        matches = [c for c in all_candidates if check(c)]
        if matches:
            print(f"  ✅ {name}: POINYAN")
            passed += 1
        else:
            print(f"  ❌ {name}: ПРОПУЩЕН")
            failed.append(name)

    print(f"\n=== RESULT ===")
    print(f"Recall: {passed}/5 = {passed/5*100:.0f}%")
    if failed:
        print(f"Failed: {failed}")
        return 1
    else:
        print("ALL KEEPs COVERED!")
        return 0


if __name__ == "__main__":
    sys.exit(run_test())
