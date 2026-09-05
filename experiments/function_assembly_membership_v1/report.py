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


def _counter_table(title: str, counter: Mapping[str, Any]) -> str:
    return _table([title, "Число"], [[key, value] for key, value in counter.items()])


def render(output: Path) -> Path:
    certificates = _read(output / "membership_certificates.json")
    scopes = _read(output / "scope_certificates.json")
    composition = _read(output / "assembly_scope_composition.json")
    controls = _read(output / "certificate_negative_controls.json")
    gate = _read(output / "lineage_gate.json")
    determinism = _read(output / "determinism.json")
    verdict = _read(output / "verdict.json")
    census = certificates["census"]

    parts: list[str] = []
    parts.append("# FUNCTION ASSEMBLY MEMBERSHIP CERTIFICATE V1 — отчёт (автогенерация)\n")
    parts.append(
        "Обращений к модели: 0. Выкатки, теневого режима, материализации и пуша нет. "
        "Замороженные слои (V1, V2, Topology V1, Bridge V1, генератор кандидатов) не изменены.\n"
    )
    parts.append("## Ворота Фазы 1\n")
    parts.append(_table(["Показатель", "Значение"], [
        ["PROVEN до (мост)", verdict["proven_before"]],
        ["CERTIFIED после", verdict["certified_after"]],
        ["CERTIFIED по каналам", json.dumps(verdict["certified_after_by_channel"], ensure_ascii=False)],
        ["CERTIFIED_BOTH_SIDES из 26 двусторонних задач моста", verdict["certified_both_sides_of_bridge_26"]],
        ["CERTIFIED_BOTH_SIDES из всех 213 задач", verdict["certified_both_sides_of_all_tasks"]],
        ["Ложных сертификатов на приманках", verdict["false_certificates_on_decoys"]],
        ["AMBIGUOUS", verdict["ambiguous"]],
        ["CONTRADICTORY", verdict["contradictory"]],
        ["Сработавших контролей безопасности", verdict["safety_controls_fired"]],
        ["Значимое сертифицированное покрытие", verdict["meaningful_certified_coverage"]],
    ]))
    parts.append("\n## Сертификаты функций\n")
    parts.append(_counter_table("Статус", census["by_status"]))
    parts.append("")
    parts.append(_counter_table("Сертифицирующий канал", census["certified_by_channel"]))
    parts.append("")
    parts.append(_counter_table("Вид отношения", census["certified_by_relation"]))
    parts.append(f"\nСертифицировано на нескольких контейнерах: {census['certified_on_several_containers']}\n")
    parts.append(_table(["Документ", "CERTIFIED", "PARTIAL", "AMBIGUOUS", "CONTRADICTORY", "UNKNOWN"], [
        [doc, *[value.get(key, 0) for key in ("CERTIFIED", "PARTIAL", "AMBIGUOUS", "CONTRADICTORY", "UNKNOWN")]]
        for doc, value in census["by_document"].items()
    ]))
    parts.append("\n### Механизмы несертификации\n")
    parts.append(_counter_table("Механизм", census["by_cause"]))
    parts.append("\n### Исходы по каналам\n")
    for channel, outcomes in census["channel_outcomes"].items():
        parts.append(f"**{channel}**\n")
        parts.append(_counter_table("Исход", outcomes))
        parts.append("")
    parts.append("### Кривая чувствительности\n")
    sens = certificates["sensitivity"]
    parts.append(f"Рабочая точка: {json.dumps(sens['operating_point'])}\n")
    parts.append(_table(
        ["мин. символов", "мин. строк", "CERTIFIED", "из них канал фрагмента", "PARTIAL", "AMBIGUOUS", "CONTRADICTORY", "UNKNOWN"],
        [[r["minimum_segment_chars"], r["minimum_located_segments"], r["CERTIFIED"],
          r["certified_on_the_fragment_channel"], r["PARTIAL"], r["AMBIGUOUS"], r["CONTRADICTORY"], r["UNKNOWN"]]
         for r in sens["curve"]]))
    parts.append("\n## Сертификаты FunctionScope\n")
    parts.append(_counter_table("Статус", scopes["census"]["by_status"]))
    parts.append("")
    parts.append(_counter_table("Причина", scopes["census"]["by_cause"]))
    parts.append("")
    parts.append(_counter_table("Контейнеров у сертифицированного скоупа", scopes["census"]["certified_scopes_by_container_count"]))
    parts.append("\n## Сборка ← скоупы\n")
    parts.append(_counter_table("Состав", composition["by_composition"]))
    parts.append("")
    parts.append(_counter_table("Сертифицированных функций на сборку", composition["certified_functions_per_assembly"]))
    parts.append("\n## Покрытие задач lineage\n")
    parts.append(_counter_table("Класс", gate["by_coverage_class"]))
    parts.append("")
    parts.append(_table(["Отношение", *gate_keys()], [
        [relation, *[value.get(key, 0) for key in gate_keys()]]
        for relation, value in gate["by_relation_type"].items()
    ]))
    parts.append(f"\nСтрогое двустороннее (весь левый скоуп + все правые функции кандидата): {gate['strict_both_sides_tasks']}\n")
    parts.append("## Контроли\n")
    parts.append(_table(["Контроль", "Значение"], [[key, value] for key, value in controls["safety"].items()]))
    parts.append("\n### Приманки\n")
    decoys = controls["decoys"]
    parts.append(f"Функций проверено: {decoys['attempted_functions']}\n")
    parts.append(_table(["Форма", "Исходы", "Ложных сертификатов по каналам"], [
        [form, json.dumps(decoys["by_form"][form], ensure_ascii=False),
         json.dumps(decoys["false_certificates_by_form_and_channel"][form], ensure_ascii=False)]
        for form in decoys["by_form"]
    ]))
    demoted = controls.get("bridge_proven_memberships_demoted_by_the_owner_rule", [])
    parts.append(f"\nPROVEN моста, пониженных правилом владельца (≥2 связанных члена): {len(demoted)}\n")
    distance = controls["controls"]["distance_rule"]
    parts.append(_table(["Правило расстояния (отвергнуто)", "Значение"], [[key, value] for key, value in distance.items()]))
    parts.append("\n## Детерминизм\n")
    parts.append(_table(["Показатель", "Значение"], [[key, value] for key, value in determinism.items()
                                                      if key not in {"schema_version", "kind"}]))
    parts.append("")
    path = output / "report.md"
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


def gate_keys() -> list[str]:
    return ["CERTIFIED_FUNCTION_FACTS_BOTH_SIDES", "CERTIFIED_LEFT_ONLY", "CERTIFIED_RIGHT_ONLY", "NO_CERTIFIED_FACTS"]


__all__ = ["render"]
