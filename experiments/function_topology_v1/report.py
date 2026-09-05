"""The verdict and the report, both computed from the artifacts and not typed in."""
from __future__ import annotations

from typing import Any, Mapping

from .contract import (
    AMBIGUOUS_BINDING,
    BOTH_SIDES_ON_TOPOLOGY,
    NEITHER_SIDE_ON_TOPOLOGY,
    NO_BINDING,
    PARTIAL_BINDING,
    PROVEN,
    PROVEN_BINDING,
    SCHEMA_VERSION,
    SHAPE_AND_DEVICES,
    SHAPE_ONLY,
    UNKNOWN,
)


def verdict(**artifacts: Mapping[str, Any]) -> dict[str, Any]:
    controls = artifacts["controls"]
    safety = controls["safety"]
    side = artifacts["side"]
    cross = artifacts["cross"]
    subgraphs = artifacts["subgraphs"]
    replay = artifacts["replay"]

    leak_free = all(value == 0 for value in safety.values())
    proven = side["totals"]["PROVEN_BOUND"]
    partial = side["totals"]["PARTIAL_BOUND"]
    functions = side["totals"]["functions"]
    both = cross["by_representation_class"].get(BOTH_SIDES_ON_TOPOLOGY, 0)
    neither = cross["by_representation_class"].get(NEITHER_SIDE_ON_TOPOLOGY, 0)
    aggregates = subgraphs["corpus_total"]
    proven_extent = subgraphs["by_boundary_status"].get(PROVEN, 0)

    if not leak_free:
        letter = "C"
        reason = "контроль, который обязан был остаться нулём, им не остался"
    elif proven_extent == 0:
        letter = "C"
        reason = "ни один агрегат не получил доказанной границы"
    elif proven == 0 and partial == 0:
        letter = "D"
        reason = (
            "агрегаты построены и доказаны, и ни один FunctionScope до них не "
            "дотягивается: гранулярность паспорта и гранулярность чертежа не "
            "встречаются"
        )
    elif both == 0:
        letter = "B"
        reason = (
            "агрегация работает и привязывает функции на подмножестве схем, и ни у "
            "одной задачи lineage обе стороны не лежат на нарисованном агрегате, "
            "поэтому межверсионная польза на этом корпусе не показывается"
        )
    elif both * 4 >= cross["tasks"]:
        letter = "A"
        reason = "агрегаты привязывают scope, и обе стороны задач до них дотягиваются"
    else:
        letter = "B"
        reason = "агрегация привязывает на подмножестве схем; покрытие между версиями остаётся ограниченным"

    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_verdict",
        "model_calls": 0,
        "deploy": False,
        "shadow": False,
        "materialization": False,
        "verdict": letter,
        "reason": reason,
        "aggregates": aggregates,
        "aggregates_with_a_proven_extent": proven_extent,
        "functions": functions,
        "functions_proven_bound": proven,
        "functions_partially_bound": partial,
        "tasks": cross["tasks"],
        "tasks_with_both_sides_on_an_aggregate": both,
        "tasks_with_neither_side_on_an_aggregate": neither,
        "controls_all_zero": leak_free,
        "replay_byte_identical": bool(replay["byte_identical"]),
    }


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    line = "| " + " | ".join(headers) + " |"
    rule = "|" + "|".join("---:" if index else "---" for index in range(len(headers))) + "|"
    body = ["| " + " | ".join(str(value) for value in row) + " |" for row in rows]
    return "\n".join([line, rule, *body])


def render(**artifacts: Mapping[str, Any]) -> str:
    subgraphs = artifacts["subgraphs"]
    signatures = artifacts["signatures"]
    bindings = artifacts["bindings"]
    side = artifacts["side"]
    cross = artifacts["cross"]
    passport = artifacts["passport"]
    lineage = artifacts["lineage"]
    controls = artifacts["controls"]
    replay = artifacts["replay"]
    walk = artifacts["walk"]
    result = artifacts["verdict"]
    written = artifacts["written"]

    parts: list[str] = []
    parts.append("# FUNCTION TOPOLOGY V1 — агрегация топологии до уровня функции\n")
    parts.append(
        "Исследование. Модель не вызывалась ни разу; выкатки, теневого режима и\n"
        "материализации нет, ни один боевой модуль не изменён, низкоуровневый граф\n"
        f"V2 не расширялся. Артефакты — в `{list(written)[0].rsplit('/', 1)[0] if '/' in list(written)[0] else '20260904_function_topology_v1'}`.\n"
    )
    parts.append(f"## Вердикт\n\n**{result['verdict']}.** {result['reason']}.\n")

    parts.append("## 1. Что построено\n")
    parts.append(_table(
        ["Документ", "Агрегатов", "PROVEN", "PARTIAL", "UNKNOWN", "Узлов", "Шин", "Фидеров"],
        [
            [
                document,
                values.get("subgraphs", 0),
                values.get(PROVEN, 0),
                values.get("PARTIAL", 0),
                values.get(UNKNOWN, 0),
                values.get("members", 0),
                values.get("buses", 0),
                values.get("feeders", 0),
            ]
            for document, values in subgraphs["by_document"].items()
        ],
    ))
    parts.append(
        f"\nАгрегатов всего: **{subgraphs['corpus_total']}**; с доказанной границей: "
        f"**{subgraphs['by_boundary_status'].get(PROVEN, 0)}**; названы напечатанной "
        f"маркой: **{subgraphs['aggregates_named_by_a_printed_mark']}**.\n"
    )

    parts.append("\n### Статус границы\n")
    parts.append(_table(
        ["Статус", "Агрегатов"],
        [[key, value] for key, value in subgraphs["by_boundary_status"].items()],
    ))

    parts.append("\n## 2. Контрольный лист\n")
    board = walk["board_aggregate"]
    parts.append(_table(
        ["", ""],
        [
            ["Электрических узлов на листе", walk["electrical_nodes"]],
            ["Агрегатов", walk["aggregates_on_the_sheet"]],
            ["из них с доказанной границей", walk["aggregates_with_a_proven_extent"]],
            ["Фидеров, названных маркой кабеля", walk["cable_marked_feeders"]],
            ["из них внутри агрегата щита", walk["cable_marked_feeders_inside_the_board_aggregate"]],
            ["Узлов в агрегате щита", board["members"]],
            ["Шин", board["buses"]],
            ["Фидеров", board["feeders"]],
            ["Аппаратов", board["equipment"]],
            ["Подписей принадлежит агрегату", board["labels_belonging_to_it"]],
        ],
    ))
    parts.append(f"\nМарки-владельцы агрегата щита: `{'`, `'.join(board['owner_marks'])}`.\n")
    parts.append(f"\nСигнатура: `{board['topology_signature']}`.\n")

    parts.append("## 3. Привязка к FunctionScope\n")
    parts.append(_table(
        ["Корпус", "Сторона", "Функций", "PROVEN", "PARTIAL", "AMBIGUOUS", "UNBOUND"],
        [
            [
                row["project"], row["side"], row["functions"],
                row["PROVEN_BOUND"], row["PARTIAL_BOUND"],
                row["AMBIGUOUS_BOUND"], row["UNBOUND"],
            ]
            for row in side["documents"]
        ],
    ))
    parts.append("\n### Механизмы, останавливающие привязку\n")
    parts.append(_table(
        ["Механизм", "Функций"],
        [[key, value] for key, value in bindings["cause_when_not_proven"].items()],
    ))

    parts.append("\n## 4. Много ветвей — одна функция\n")
    granularity = bindings["granularity"]
    parts.append(_table(
        ["Функций на один агрегат", "Агрегатов"],
        [[key, value] for key, value in granularity["functions_per_bound_subgraph"].items()],
    ))
    single = granularity["functions_per_bound_subgraph"].get("1", 0)
    parts.append(
        f"\nАгрегатов, несущих ровно одну функцию паспорта: **{single}**; "
        f"агрегатов, несущих несколько: **{granularity['subgraphs_carrying_several_functions']}**. "
        "Это §8 в измеренном виде. Это **не** межверсионное слияние: несколько "
        "функций паспорта на одном нарисованном агрегате — внутренняя структура "
        "одного листа, а слияние версий это утверждение о двух документах и "
        "решается в другом месте.\n"
    )

    parts.append("\n## 5. Сигнатура\n")
    parts.append(_table(
        ["Тир", "Агрегатов", "Различных сигнатур", "Крупнейшая группа", "Одиночек"],
        [
            [tier, values["subgraphs"], values["distinct_signatures"],
             values["largest_group"], values["singletons"]]
            for tier, values in signatures["distinguishing_power"].items()
        ],
    ))

    separation = signatures["same_class_separation"]
    # every series, not the largest few: the fully separated ones are small, and
    # a table sorted by size would hide the only positive answer §16 has
    interesting = sorted(
        separation["groups"],
        key=lambda row: (-row["distinct_by_tier"][SHAPE_ONLY] / max(row["subgraphs"], 1),
                         -row["subgraphs"]),
    )
    parts.append("\n### Различает ли структура два экземпляра одного класса (§16)\n")
    parts.append(_table(
        ["Серия марки", "Агрегатов", "SHAPE_ONLY", "SHAPE_AND_DEVICES", "SHAPE_AND_CONSUMERS"],
        [
            [row["owner_series"], row["subgraphs"],
             row["distinct_by_tier"]["SHAPE_ONLY"],
             row["distinct_by_tier"]["SHAPE_AND_DEVICES"],
             row["distinct_by_tier"]["SHAPE_AND_CONSUMERS"]]
            for row in interesting
        ],
    ))

    parts.append("\n## 6. Представления сторон\n")
    parts.append(_table(
        ["Класс", "Задач"],
        [[key, value] for key, value in cross["by_representation_class"].items()],
    ))
    left_only = cross["by_representation_class"].get("LEFT_ONLY_ON_TOPOLOGY", 0)
    right_only = cross["by_representation_class"].get("RIGHT_ONLY_ON_TOPOLOGY", 0)
    both = cross["by_representation_class"].get(BOTH_SIDES_ON_TOPOLOGY, 0)
    parts.append(
        f"\nСлева до агрегата дотягиваются **{left_only}** задач, справа — "
        f"**{right_only}**, и пересечение этих множеств — **{both}**. Топология "
        "здесь — необязательное положительное доказательство: сторона, у которой "
        "её нет, ничему не противоречит.\n"
    )

    parts.append("\n## 7. Отрицательные контроли\n")
    parts.append(_table(
        ["Контроль", "Наблюдение"],
        [[key, value] for key, value in controls["safety"].items()],
    ))
    parts.append("\n" + _table(
        ["Замороженный слой", "Значение"],
        [[key, value] for key, value in controls["frozen_layers"].items()
         if not isinstance(value, dict)],
    ))

    parts.append("\n## 8. Паспорт и lineage\n")
    parts.append(_table(
        ["", ""],
        [
            ["Функций обогащено", passport["functions_enriched"]],
            ["Фактов на функцию", passport["facts_added_per_function"]],
            ["Фактов отсутствия", passport["facts_asserting_a_gap_added"]],
            ["Задач", lineage["tasks"]],
            ["Обе стороны на агрегате", lineage["tasks_with_both_sides_on_an_aggregate"]],
            ["Обе стороны доказаны", lineage["tasks_with_both_sides_proven_on_an_aggregate"]],
            ["AUTO_ONE_TO_ONE_CERTIFIED", f"{lineage['tiers']['AUTO_ONE_TO_ONE_CERTIFIED']['before']} → {lineage['tiers']['AUTO_ONE_TO_ONE_CERTIFIED']['after']}"],
            ["AUTO_MERGED_CERTIFIED", f"{lineage['tiers']['AUTO_MERGED_CERTIFIED']['before']} → {lineage['tiers']['AUTO_MERGED_CERTIFIED']['after']}"],
        ],
    ))

    parts.append("\n## 9. Воспроизводимость\n")
    parts.append(_table(
        ["", ""],
        [
            ["Страниц с нарисованным графом", replay["pages_with_a_drawn_graph"]],
            ["Пересобрано из PDF", replay["pages_rebuilt_from_the_pdf"]],
            ["Совпало байт-в-байт", replay["pages_identical_on_replay"]],
            ["Обращений к модели", 0],
        ],
    ))
    parts.append("\n## 10. Артефакты\n")
    parts.append(_table(
        ["Файл", "SHA-256"],
        [[name, digest[:16]] for name, digest in written.items()],
    ))
    return "\n".join(parts) + "\n"


__all__ = ["render", "verdict"]
