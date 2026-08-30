#!/usr/bin/env python3
"""Числа одного прогона сравнения — все, которыми его можно принимать.

Скрипт ничего не запускает и ничего не чинит: он читает артефакты уже
состоявшегося прогона и печатает то, что нужно для приёмки, одним набором.
Главная метрика здесь — не «ИИ закрыл N %»: она ничего не говорит о том,
правильные ли это N %. Поэтому рядом всегда стоят область сравнения, доля
прошедших верификатор, покрытие доказательствами и остаток человеку.

Использование:
    python scripts/stage_comparison_acceptance_metrics.py <production_dir>
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path
from typing import Any, Mapping


def _load(directory: Path, name: str) -> dict[str, Any]:
    path = directory / name
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def _counter(values) -> dict[str, int]:
    return dict(sorted(collections.Counter(values).items()))


def metrics(directory: Path) -> dict[str, Any]:
    state = _load(directory, "state.json")
    sheets = _load(directory, "sheet_relations.json")
    synthesis = _load(directory, "unified_synthesis.json")
    questions = _load(directory, "review_questions.json")
    ai = _load(directory, "ai_resolutions.json")
    report = _load(directory, "final_report.json")
    decisions = _load(directory, "engineer_decisions.json")
    facts = _load(directory, "text_fact_production.json")
    table_changes = _load(directory, "electrical_table_changes.json")
    inconsistencies = _load(directory, "document_inconsistencies.json")
    preliminary = _load(directory, "preliminary_report.json")
    direct_page = _load(directory, "direct_page_mode2.json")

    relations = [
        item for item in sheets.get("relations") or [] if isinstance(item, Mapping)
    ]
    stages = state.get("stages") or {}
    scope = stages.get("sheet_scope") or {}
    review_items = synthesis.get("review_items") or []
    changes = synthesis.get("changes") or []
    ai_rows = [
        item for item in ai.get("resolutions") or [] if isinstance(item, Mapping)
    ]
    resolved = [item for item in ai_rows if item.get("status") == "AI_RESOLVED"]
    ai_diagnostics = ai.get("diagnostics") or {}

    # Покрытие доказательствами: у разрешения обязана быть дословная цитата, и
    # для двустороннего изменения — с обеих сторон.
    both_sides = 0
    any_quote = 0
    for item in resolved:
        quotes = item.get("evidence_quotes") or []
        sides = {str(value.get("side") or "") for value in quotes if isinstance(value, Mapping)}
        if quotes:
            any_quote += 1
        if {"LEFT", "RIGHT"} <= sides:
            both_sides += 1

    load_tables = (direct_page.get("diagnostics") or {}).get(
        "electrical_load_tables"
    ) or {}
    matching_metrics = (
        (direct_page.get("comparison_result") or {}).get("matching") or {}
    ).get("metrics") or {}
    table_counts = table_changes.get("counts") or {}
    preliminary_counts = (preliminary.get("summary") or {}).get("counts") or {}
    graphic_changes = [
        item
        for item in changes
        if (((item.get("provenance") or {}).get("source_atoms") or [{}])[0].get(
            "provenance"
        ) or {}).get("producer")
        != "electrical-table-diff-v1"
    ]

    return {
        "прогон": {
            "статус": state.get("status"),
            "длительность_мс": state.get("duration_ms"),
            "режим_входа": state.get("input_mode"),
        },
        "листы": {
            "отношений_всего": len(relations),
            "по_статусу": _counter(str(item.get("status")) for item in relations),
            "в_области_сравнения": scope.get("groups"),
            "ждут_подтверждения": scope.get("pending_confirmation"),
        },
        "текст": {
            "фактов": (facts.get("diagnostics") or {}).get("facts"),
            "по_правилу": (facts.get("diagnostics") or {}).get("facts_by_rule"),
            "не_является_фактом": (facts.get("diagnostics") or {}).get(
                "not_applicable_source_evidence"
            ),
            "не_разобрано": (facts.get("diagnostics") or {}).get(
                "unresolved_source_evidence"
            ),
        },
        "таблицы_нагрузок": {
            "строк_слева": (load_tables.get("LEFT") or {}).get("counts", {}).get("rows"),
            "строк_справа": (load_tables.get("RIGHT") or {}).get("counts", {}).get("rows"),
            "связано_слева": (load_tables.get("LEFT") or {}).get("counts", {}).get("bound"),
            "связано_справа": (load_tables.get("RIGHT") or {}).get("counts", {}).get("bound"),
            "неоднозначно_слева": (load_tables.get("LEFT") or {}).get("counts", {}).get("ambiguous"),
            "неоднозначно_справа": (load_tables.get("RIGHT") or {}).get("counts", {}).get("ambiguous"),
            "не_связано_слева": (load_tables.get("LEFT") or {}).get("counts", {}).get("unbound"),
            "не_связано_справа": (load_tables.get("RIGHT") or {}).get("counts", {}).get("unbound"),
            "сопоставлено_пар": table_counts.get("matches"),
            "изменений": table_counts.get("changes"),
            "без_изменений": table_counts.get("unchanged"),
            "не_сравнивается": table_counts.get("blocked"),
            "без_пары": table_counts.get("unproven"),
            "по_свойству": _counter(
                str(item.get("facet_title"))
                for item in table_changes.get("changes") or []
            ),
            "по_строгости_сопоставления": _counter(
                str(item.get("match_method"))
                for item in table_changes.get("changes") or []
            ),
        },
        "сопоставление_объектов": {
            "узлов_слева": matching_metrics.get("left_nodes"),
            "узлов_справа": matching_metrics.get("right_nodes"),
            "надёжных_пар": matching_metrics.get("matched_pairs"),
            "неоднозначных_слева": matching_metrics.get("ambiguous_left_nodes"),
            "неоднозначных_справа": matching_metrics.get("ambiguous_right_nodes"),
        },
        "противоречия_документа": {
            "всего": (inconsistencies.get("counts") or {}).get("total"),
            "по_видам": _counter(
                str(item.get("kind")) for item in inconsistencies.get("items") or []
            ),
        },
        "предварительный_отчёт": {
            "найдено_автоматически": preliminary_counts.get("automatic"),
            "требует_проверки": preliminary_counts.get("review"),
            "противоречий_документа": preliminary_counts.get("inconsistency"),
            "недостаточно_доказательств": preliminary_counts.get("unproven"),
            "групп_по_оборудованию": preliminary_counts.get("equipment_groups"),
        },
        "синтез": {
            "изменений": len(changes),
            "из_графа": len(graphic_changes),
            "из_таблиц": len(changes) - len(graphic_changes),
            "групп_отображения": len(synthesis.get("presentation_groups") or []),
            "требуют_разбора": len(review_items),
            "измерения_у_требующих": _counter(
                str(item.get("dimension")) for item in review_items
            ),
        },
        "ии": {
            "режим": ai.get("mode"),
            "на_входе": ai_diagnostics.get("input_items", 0),
            "разрешено": len(resolved),
            "осталось_человеку": ai_diagnostics.get("human_required", 0),
            "почему_осталось": ai_diagnostics.get("human_reasons") or {},
            "отклонено_верификатором": ai_diagnostics.get("verifier_rejected", 0),
            "не_прошло_верификатор_с_первого_раза": ai_diagnostics.get(
                "verifier_failed_first_pass", 0
            ),
            "повторов_на_высоком_уровне": ai_diagnostics.get("retries_used", 0),
            "отклонено_критиком": ai_diagnostics.get("critic_rejected", 0),
            "отказов_модели": ai_diagnostics.get("model_failed", 0),
            "таймаутов": ai_diagnostics.get("model_timeout", 0),
            "вызовов_модели": ai_diagnostics.get("model_calls", 0),
            "партий": ai_diagnostics.get("batches", 0),
            "проходов_критика": ai_diagnostics.get("critic_passes", 0),
            "вызовов_по_чертежу": ai_diagnostics.get("vision_calls", 0),
            "кэш": ai_diagnostics.get("cache") or {},
            "пределы_исчерпаны": ai_diagnostics.get("budgets_hit") or [],
            "длительность_мс": ai_diagnostics.get("duration_ms", 0),
            "покрытие_цитатами": {
                "хотя_бы_одна": any_quote,
                "с_обеих_сторон": both_sides,
                "из_разрешённых": len(resolved),
            },
            "уверенность_разрешённых": _counter(
                str(item.get("confidence")) for item in resolved
            ),
        },
        "вопросы_инженеру": {
            "всего": len(questions.get("questions") or []),
            "по_категориям": _counter(
                str(item.get("category")) for item in questions.get("questions") or []
            ),
            "по_типам": _counter(
                str(item.get("question_type"))
                for item in questions.get("questions") or []
            ),
            "подавлено": (questions.get("diagnostics") or {}).get(
                "suppressed_change_question_reasons"
            ) or {},
        },
        "находки_инженеру": {
            "строк_всего": len(changes) + len(review_items),
            "изменений": len(changes),
            "требуют_разбора": len(review_items),
            # Этап 7 — единственное место, где находка подтверждается: сколько
            # строк реально легло инженеру на стол.
            "решений_на_столе": len(decisions.get("decisions") or []),
            "по_состоянию": decisions.get("counts") or {},
            "показываются": sum(
                1
                for item in decisions.get("decisions") or []
                if isinstance(item, Mapping) and item.get("presentable")
            ),
        },
        "итоговый_отчёт": {
            "подтверждено_инженером": len(
                report.get("approved_atomic_changes") or []
            ),
            "только_подтверждённые": (report.get("constraints") or {}).get(
                "approved_only"
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("production_dir")
    args = parser.parse_args()
    print(json.dumps(
        metrics(Path(args.production_dir)), ensure_ascii=False, indent=1
    ))


if __name__ == "__main__":
    main()
