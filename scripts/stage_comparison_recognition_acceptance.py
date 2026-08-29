#!/usr/bin/env python3
"""Приёмка полноты распознавания на реальной паре документов.

Что делает. Прогоняет НАСТОЯЩУЮ текстовую ветку конвейера сравнения —
сопоставление листов, подготовку, Stage 3, производителя фактов — и отвечает на
два вопроса:

1. Сколько расхождений система объявила бы изменением проекта, если бы полноту
   распознавания никто не проверял, и сколько из них не проходят проверку.
2. Какие из «удалено»/«добавлено» ДОКАЗАННО ложные: значение, которого якобы
   нет на встречной стороне, лежит в нативном текстовом слое её же PDF.

Второй список — это корпус ложных отсутствий. Он не размечается вручную и не
объявляет истину там, где она не доказана: в него попадает только то, где
исходный документ прямо противоречит выводу.

Ни одного вызова модели, ни одного токена, ни одной записи в артефакты прогона.

    python scripts/stage_comparison_recognition_acceptance.py \\
        comparison/sessions/<sid>/pairs/<pid>/pair.json [--json отчёт.json]
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison import (  # noqa: E402
    production_orchestrator as production,
    recognition_coverage as rc,
)
from backend.app.services.stage_comparison.production_text_flow import (  # noqa: E402
    build_text_differences_from_preparation,
    prepare_text_scope,
)
from backend.app.services.stage_comparison.text_fact_producer import (  # noqa: E402
    produce_text_facts,
)
from backend.app.services.stage_comparison.text_semantic_validation import (  # noqa: E402
    iter_stage3_evidence,
)

#: Причины, названные проверкой полноты. Отделены от причин соседних гейтов,
#: чтобы в отчёте не смешивались «лист не подтверждён» и «лист не прочитан».
_RECOGNITION_PREFIXES = (
    "recognition_coverage", "opposite_side_native", "opposite_side_not",
    "own_side", "page_recognition", "native_text", "side_recognized",
    "value_has",
)


def _load_pair(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {"id": payload["id"], "left": payload["left"], "right": payload["right"]}


def _outcomes(production_facts: dict) -> Counter:
    return Counter(fact["outcome"] for fact in production_facts["facts"])


def run(pair_path: Path) -> dict:
    import fitz

    pair = _load_pair(pair_path)
    relations, indexes = production._run_sheet_matcher(pair)
    groups = production._sheet_comparison_groups(relations)
    if not groups:
        raise SystemExit(
            "сопоставитель не подтвердил ни одной пары листов: сравнивать нечего"
        )
    preparation = prepare_text_scope(pair, groups, sheet_indexes=indexes, fitz=fitz)
    differences = build_text_differences_from_preparation(preparation)
    coverage = differences["recognition_coverage"]

    after = produce_text_facts(differences, preparation)
    # «Как было бы без проверки»: тот же конвейер, но каждый вердикт полноты
    # объявлен достаточным. Разница между двумя прогонами и есть цена гейта.
    without = copy.deepcopy(differences)
    for value in without["recognition_coverage"]["by_evidence"].values():
        value["status"] = rc.SUFFICIENT
        value["reason_codes"] = []
    before = produce_text_facts(without, preparation)

    by_id = {fact["fact_id"]: fact for fact in before["facts"]}
    downgraded = [
        fact for fact in after["facts"]
        if fact["outcome"] == "REVIEW_REQUIRED"
        and by_id.get(fact["fact_id"], {}).get("outcome") == "MATERIAL_CHANGE"
    ]

    false_absences = []
    for source_ref, group, bucket, item in iter_stage3_evidence(differences):
        verdict = coverage["by_evidence"].get(source_ref) or {}
        codes = set(verdict.get("reason_codes") or ())
        if rc.REASON_OPPOSITE_CONTAINS_VALUE not in codes:
            continue
        opposite = (verdict.get("signals") or {}).get("opposite") or {}
        false_absences.append({
            "source_evidence_ref": source_ref,
            "bucket": bucket,
            "group_id": group.get("id"),
            "value": str(item.get("before") or item.get("after") or "")[:200],
            "found_on_opposite_side": opposite.get("present_tokens") or [],
            "opposite_side": opposite.get("side"),
        })

    return {
        "pair_id": pair["id"],
        "sheet_pairs": {"total": len(relations.get("relations") or []),
                        "proven": len(groups)},
        "stage3": differences["summary"],
        "coverage": {
            "documents": {
                side: value["status"] for side, value in coverage["documents"].items()
            },
            "pages": {
                side: dict(Counter(v["status"] for v in pages.values()))
                for side, pages in coverage["pages"].items()
            },
            "evidence": coverage["diagnostics"]["evidence_status_counts"],
            "reasons": dict(Counter(
                code
                for value in coverage["by_evidence"].values()
                for code in value["reason_codes"]
            ).most_common()),
        },
        "facts": {
            "total": len(after["facts"]),
            "without_the_check": dict(_outcomes(before)),
            "with_the_check": dict(_outcomes(after)),
            "downgraded_from_material": len(downgraded),
            "downgraded_by_direction": dict(Counter(
                fact["direction"] for fact in downgraded
            )),
            "downgraded_by_reason": dict(Counter(
                code
                for fact in downgraded
                for code in fact["provenance"]["review_requirement"]["reason_codes"]
                if code.startswith(_RECOGNITION_PREFIXES)
            ).most_common()),
        },
        "false_absence_corpus": false_absences,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pair", type=Path, help="путь к pair.json")
    parser.add_argument("--json", type=Path, help="куда записать полный отчёт")
    parser.add_argument(
        "--examples", type=int, default=10, help="сколько примеров печатать",
    )
    args = parser.parse_args()

    report = run(args.pair)
    print(f"── Пара {report['pair_id']} ──")
    print("пар листов: {proven} доказанных из {total}".format(**report["sheet_pairs"]))
    print("Stage 3:", json.dumps(report["stage3"], ensure_ascii=False))
    print("полнота по документам:",
          json.dumps(report["coverage"]["documents"], ensure_ascii=False))
    print("полнота по расхождениям:",
          json.dumps(report["coverage"]["evidence"], ensure_ascii=False))
    facts = report["facts"]
    print(f"\nфактов: {facts['total']}")
    print("без проверки полноты:",
          json.dumps(facts["without_the_check"], ensure_ascii=False))
    print("с проверкой полноты:",
          json.dumps(facts["with_the_check"], ensure_ascii=False))
    print("переведено из «существенное» в «нужна проверка»:",
          facts["downgraded_from_material"])
    print("по направлению:",
          json.dumps(facts["downgraded_by_direction"], ensure_ascii=False))
    print("по причине:",
          json.dumps(facts["downgraded_by_reason"], ensure_ascii=False, indent=1))

    corpus = report["false_absence_corpus"]
    print(f"\n── Доказанные ложные отсутствия: {len(corpus)} ──")
    print("(значение есть в тексте PDF встречной стороны — разошлось "
          "распознавание, а не проект)")
    for value in corpus[: args.examples]:
        print(f"  [{value['bucket']}] {value['value'][:90]!r}"
              f"  ← найдено на стороне {value['opposite_side']}:"
              f" {value['found_on_opposite_side']}")

    if args.json:
        args.json.write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8",
        )
        print(f"\nполный отчёт: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
