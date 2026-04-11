"""
test_generators.py
------------------
Синтетический тест candidate generators на mock canonical memory.

Проверяет что генераторы ловят все 5 definite KEEPs эталона когда им
подают идеально извлечённые данные.

Если этот тест проходит — значит логика генераторов правильная, и
качество recall зависит только от extractor-stage.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from candidate_generators import generate_all_candidates


def make_source(block_id, page, sheet, sheet_type):
    return {
        "block_id": block_id,
        "page": page,
        "sheet": sheet,
        "sheet_type": sheet_type,
    }


def build_mock_memory() -> dict:
    """Синтетический canonical memory имитирующий правильное извлечение из ГРЩ."""

    source_schema = make_source("block_schema", 7, "Лист 3", "schema")
    source_node1 = make_source("block_node1", 14, "Лист 10", "node")
    source_node2 = make_source("block_node2", 14, "Лист 10", "node")
    source_spec = make_source("block_spec", 15, "Лист 11", "spec_table")

    return {
        "cables": {
            # #18 ВРУ-1 1 PE vs 2 PE — cross-view mismatch на pe_or_n_count
            "М-1.1": {
                "mentions": [
                    {"source": source_schema, "raw_quote": "ППГнг(А)-HF 2×(4×(1×185))+1×(1×185)"},
                    {"source": source_node1, "raw_quote": "ППГнг(А)-HF 2×4×(1×185)+2×(1×185)"},
                ],
                "attributes": {
                    "mark": [
                        {"value": "ППГнг(А)-HF", "source": source_schema,
                         "raw_quote": "ППГнг(А)-HF"},
                        {"value": "ППГнг(А)-HF", "source": source_node1,
                         "raw_quote": "ППГнг(А)-HF"},
                    ],
                    "phase_count": [
                        {"value": 8, "source": source_schema},
                        {"value": 8, "source": source_node1},
                    ],
                    "phase_section_mm2": [
                        {"value": 185, "source": source_schema},
                        {"value": 185, "source": source_node1},
                    ],
                    "pe_or_n_count": [  # ← конфликт
                        {"value": 1, "source": source_schema,
                         "raw_quote": "ППГнг(А)-HF 2×(4×(1×185))+1×(1×185)"},
                        {"value": 2, "source": source_node1,
                         "raw_quote": "ППГнг(А)-HF 2×4×(1×185)+2×(1×185)"},
                    ],
                },
            },

            # #7 нейтраль ВРУ-4 150 vs 120 — pe_or_n_section_mm2 mismatch
            "М-1.4": {
                "mentions": [
                    {"source": source_schema, "raw_quote": "ППГнг(А)-HF 4×(1×240)+(1×150)"},
                    {"source": source_node1, "raw_quote": "ППГнг(А)-HF 4×(1×240)+(1×120)"},
                ],
                "attributes": {
                    "mark": [
                        {"value": "ППГнг(А)-HF", "source": source_schema},
                        {"value": "ППГнг(А)-HF", "source": source_node1},
                    ],
                    "phase_count": [
                        {"value": 4, "source": source_schema},
                        {"value": 4, "source": source_node1},
                    ],
                    "phase_section_mm2": [
                        {"value": 240, "source": source_schema},
                        {"value": 240, "source": source_node1},
                    ],
                    "pe_or_n_count": [
                        {"value": 1, "source": source_schema},
                        {"value": 1, "source": source_node1},
                    ],
                    "pe_or_n_section_mm2": [  # ← конфликт
                        {"value": 150, "source": source_schema,
                         "raw_quote": "ППГнг(А)-HF 4×(1×240)+(1×150)"},
                        {"value": 120, "source": source_node1,
                         "raw_quote": "ППГнг(А)-HF 4×(1×240)+(1×120)"},
                    ],
                },
            },

            # #5 дубль М-1.5 — два разных destination на одной схеме
            "М-1.5": {
                "mentions": [
                    {"source": source_schema, "raw_quote": "М-1.5 → ВРУ-ИТП"},
                    {"source": source_schema, "raw_quote": "М-1.5 → ВРУ-НС"},
                ],
                "attributes": {
                    "destination": [  # ← ДВА РАЗНЫХ на ОДНОМ листе
                        {"value": "ВРУ-ИТП", "source": source_schema,
                         "raw_quote": "М-1.5 → ВРУ-ИТП"},
                        {"value": "ВРУ-НС", "source": source_schema,
                         "raw_quote": "М-1.5 → ВРУ-НС"},
                    ],
                },
            },

            # Нормальный кабель — не должен давать candidate
            "М-2.2": {
                "mentions": [
                    {"source": source_schema},
                    {"source": source_node2},
                ],
                "attributes": {
                    "mark": [
                        {"value": "ППГнг(А)-HF", "source": source_schema},
                        {"value": "ППГнг(А)-HF", "source": source_node2},
                    ],
                    "phase_section_mm2": [
                        {"value": 150, "source": source_schema},
                        {"value": 150, "source": source_node2},
                    ],
                },
            },
        },

        "breakers": {
            # #55 ВА-335А — термомагнитный тип vs требование электронных
            "QF1.1": {
                "mentions": [
                    {"source": source_schema},
                ],
                "attributes": {
                    "model": [
                        {"value": "ВА-335А", "source": source_schema},
                    ],
                    "current_rating_a": [
                        {"value": 630, "source": source_schema},
                    ],
                    "trip_type": [
                        {"value": "термомагнитный", "source": source_schema},  # ← нарушает примечание
                    ],
                    "location": [
                        {"value": "ГРЩ", "source": source_schema},
                    ],
                    "protects_line": [
                        {"value": "ВРУ-1", "source": source_schema},
                    ],
                },
            },

            # Нормальный АВ — электронный, не должен давать candidate
            "QF1.2": {
                "mentions": [
                    {"source": source_schema},
                ],
                "attributes": {
                    "model": [
                        {"value": "ВА-ЭЛЕКТР", "source": source_schema},
                    ],
                    "trip_type": [
                        {"value": "электронный", "source": source_schema},
                    ],
                    "location": [
                        {"value": "ГРЩ", "source": source_schema},
                    ],
                },
            },
        },

        "panels": {
            # #2 ЩУ-2/Т vs ЩУ-12/Т — swapped descriptions
            "ЩУ-2/Т": {
                "mentions": [
                    {"source": source_spec, "context": "spec_row"},
                ],
                "attributes": {
                    "description": [  # описание намекает на 12 счётчиков
                        {"value": "Шкаф учета на 12 счетчиков трансф.вкл.",
                         "source": source_spec,
                         "raw_quote": "ЩУ-2/Т — Шкаф учета на 12 счетчиков трансф.вкл."},
                    ],
                },
            },
            "ЩУ-12/Т": {
                "mentions": [
                    {"source": source_spec, "context": "spec_row"},
                ],
                "attributes": {
                    "description": [  # описание намекает на 2 счётчика
                        {"value": "Шкаф учета на два счетчика трансф.вкл.",
                         "source": source_spec,
                         "raw_quote": "ЩУ-12/Т — Шкаф учета на два счетчика трансф.вкл."},
                    ],
                },
            },

            # Нормальный щит — не должен давать candidate
            "ВРУ-1": {
                "mentions": [
                    {"source": source_schema, "context": "schema_label"},
                ],
                "attributes": {
                    "description": [
                        {"value": "Вводно-распределительное устройство корпуса 1",
                         "source": source_schema},
                    ],
                },
            },
        },

        "notes": [
            # Правило о электронных расцепителях — должно сработать на QF1.1
            {
                "text": "Автоматические выключатели отходящих линий ГРЩ применить с "
                        "электронными расцепителями, обладающими селективной задержкой "
                        "срабатывания от КЗ.",
                "scope_hint": "отходящие АВ ГРЩ",
                "category": "requirement",
                "source": source_schema,
                "raw_quote": "АВ отходящих линий ГРЩ — с электронными расцепителями",
            },
        ],

        "current_transformers": {},
    }


def run_test():
    """Запустить генераторы на mock memory и проверить что все 5 KEEPs пойманы."""
    memory = build_mock_memory()

    print("=" * 70)
    print("ТЕСТ ГЕНЕРАТОРОВ НА СИНТЕТИЧЕСКИХ ДАННЫХ")
    print("=" * 70)
    print(f"\nMock memory stats:")
    print(f"  cables: {len(memory['cables'])}")
    print(f"  breakers: {len(memory['breakers'])}")
    print(f"  panels: {len(memory['panels'])}")
    print(f"  notes: {len(memory['notes'])}")

    results = generate_all_candidates(memory)

    print(f"\n=== Generated candidates ===")
    for gen_name, count in results["stats"].items():
        print(f"  {gen_name}: {count}")

    print(f"\n=== Details by candidate ===")
    for gen_name, candidates in results["by_generator"].items():
        print(f"\n-- {gen_name} ({len(candidates)}) --")
        for c in candidates:
            print(f"  [class={c['class']}] {c['subtype']}: {c['entity_id']}")
            print(f"    field: {c.get('field')}")
            print(f"    severity: {c['auto_severity']}")
            print(f"    explanation: {c['explanation'][:150]}")

    # Проверка покрытия KEEPs
    print(f"\n=== KEEP coverage check ===")
    keep_checks = [
        ("#2 ЩУ-2/Т перепутаны",
         lambda c: c["subtype"] == "swapped_descriptions"
                   and "ЩУ-2/Т" in c["entity_id"]
                   and "ЩУ-12/Т" in c["entity_id"]),
        ("#5 дубль М-1.5",
         lambda c: c["subtype"] == "duplicate_marker"
                   and c["entity_id"] == "М-1.5"),
        ("#7 нейтраль 150/120",
         lambda c: c["subtype"] == "cable_composition_mismatch"
                   and c["entity_id"] == "М-1.4"
                   and c.get("field") == "pe_or_n_section_mm2"),
        ("#18 ВРУ-1 PE 1/2",
         lambda c: c["subtype"] == "cable_composition_mismatch"
                   and c["entity_id"] == "М-1.1"
                   and c.get("field") == "pe_or_n_count"),
        ("#55 ВА-335А vs note",
         lambda c: c["subtype"] == "note_vs_equipment_type"
                   and "QF" in c["entity_id"]),
    ]

    all_candidates = results["all_candidates"]
    passed = 0
    failed = []
    for keep_name, check in keep_checks:
        matches = [c for c in all_candidates if check(c)]
        if matches:
            print(f"  ✅ {keep_name}: POINYAN ({len(matches)} candidate)")
            passed += 1
        else:
            print(f"  ❌ {keep_name}: ПРОПУЩЕН")
            failed.append(keep_name)

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
