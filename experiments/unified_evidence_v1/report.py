"""The report, drawn from the artifacts and never from the measurement in flight."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(out)


def _counter(title: str, counter: Mapping[str, Any]) -> str:
    return _table([title, "Число"], [[key, value] for key, value in counter.items()])


def render(output: Path) -> Path:
    census = _read(output / "unified_evidence_census.json")
    functions = _read(output / "function_facts.json")["census"]
    coverage = _read(output / "certified_coverage.json")
    reassessment = _read(output / "lineage_reassessment_no_ai.json")
    determinism = _read(output / "determinism.json")
    verdict = _read(output / "verdict.json")

    parts: list[str] = []
    parts.append("# UNIFIED ENGINEERING EVIDENCE V1 — отчёт (автогенерация)\n")
    parts.append("Обращений к модели: 0. Выкатки, теневого режима, материализации и пуша нет.\n")
    parts.append("## Итог\n")
    parts.append(_table(["Показатель", "Значение"], [[key, value] for key, value in verdict.items()
                                                      if key not in {"schema_version", "kind", "coverage"}]))
    parts.append("\n## Единая модель: что выпущено\n")
    parts.append(_counter("Производитель", census["by_producer"]))
    parts.append("")
    parts.append(_counter("Представление", census["by_source_representation"]))
    parts.append("")
    parts.append(_counter("Поле", census["by_field"]))
    parts.append("")
    parts.append(_counter("Область действия", census["by_applicability"]))
    parts.append("")
    parts.append(_counter("Заявление", census["by_claim_semantics"]))
    parts.append("")
    parts.append(_counter("Класс провенанса", census["by_provenance_grade"]))
    parts.append(f"\nФактов, привязанных к сертифицированной функции: {census['facts_attached_to_a_certified_function']}; "
                 f"заявленных только паспортом: {census['facts_declared_by_a_passport_only']}; "
                 f"строк OCR: {census['ocr_evidence_rows']}, из них подтверждено нативным слоем: "
                 f"{census['ocr_evidence_rows_promoted_by_the_native_layer']}; "
                 f"JSONL: {census['jsonl_bytes']} байт (~{census['bytes_per_page']} байт/стр.).\n")
    parts.append(_table(["Документ", "Фактов", "POSITIVE_PRESENCE", "SUPPORT_ONLY"], [
        [doc, value.get("facts", 0), value.get("POSITIVE_PRESENCE", 0), value.get("SUPPORT_ONLY", 0)]
        for doc, value in census["by_document"].items()
    ]))
    parts.append("\n## Факты функций (Фаза 3)\n")
    parts.append(_counter("Основание", functions["facts_by_basis"]))
    parts.append(f"\nФункций с хотя бы одним сертифицированным фактом: {functions['functions_with_a_certified_fact']} из {functions['functions']}\n")
    parts.append(_table(["Поле", "Функций", "CERTIFIED", "DECLARED", "SHEET_SHARED", "REFERENCED", "заявлено / напечатано нативно"], [
        [field, value.get("functions", 0), value.get("CERTIFIED", 0), value.get("DECLARED", 0),
         value.get("SHEET_SHARED", 0), value.get("REFERENCED", 0),
         f"{value.get('declared_values', 0)} / {value.get('declared_values_printed_natively_on_the_page', 0)}" if value.get("declared_values") else ""]
        for field, value in functions["by_field"].items()
    ]))
    parts.append("\n## Покрытие задач (Фазы 4–5)\n")
    parts.append(f"Факты сборки на обеих сторонах (мост, до): {coverage['assembly_facts_both_sides_before_this_track']}\n")
    parts.append(_counter("Класс сертифицированных фактов функции", coverage["certified_function_facts"]))
    parts.append("\n## Переоценка lineage без ИИ (Фаза 6)\n")
    parts.append(_counter("Итоги", reassessment["totals"]))
    parts.append("")
    parts.append(_counter("Идентичность (марка на обеих сторонах)", reassessment["identity"]))
    parts.append("")
    parts.append(_counter("Исследовательские эталоны как гипотезы", reassessment["references"]))
    parts.append("")
    parts.append(_counter("Ничьи и подавление", reassessment["tie_and_suppression"]))
    parts.append("")
    parts.append(_table(["Отношение", "задач", "с поддержанным кандидатом", "с новым доказательством", "с явным противоречием"], [
        [key, value.get("tasks", 0), value.get("with_a_supported_candidate", 0), value.get("with_new_evidence", 0), value.get("with_an_explicit_contradiction", 0)]
        for key, value in reassessment["by_relation_type"].items()
    ]))
    parts.append("")
    parts.append(_table(["Производность группы", "задач", "с поддержанным кандидатом", "с новым доказательством"], [
        [key, value.get("tasks", 0), value.get("with_a_supported_candidate", 0), value.get("with_new_evidence", 0)]
        for key, value in reassessment["by_group_derivability"].items()
    ]))
    parts.append("\n**Ответ:** " + json.dumps(reassessment["answer"], ensure_ascii=False, indent=1) + "\n")
    parts.append("## Детерминизм\n")
    parts.append(_table(["Показатель", "Значение"], [[key, value] for key, value in determinism.items() if key not in {"schema_version", "kind"}]))
    parts.append("")
    path = output / "report.md"
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


__all__ = ["render"]
