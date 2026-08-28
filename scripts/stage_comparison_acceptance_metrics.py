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
    facts = _load(directory, "text_fact_production.json")

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
        "синтез": {
            "изменений": len(changes),
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
