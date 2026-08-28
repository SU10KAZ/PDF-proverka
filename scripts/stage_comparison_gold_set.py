#!/usr/bin/env python3
"""Предложение эталонной выборки для сравнения документации.

Без эталона любая цифра точности — самооценка. Этот скрипт не измеряет
точность и не притворяется, что может: он отбирает те 50-100 случаев, разметка
которых один раз даёт право говорить о точности всех остальных.

Выборка расслоена по решению системы, а не случайна: равномерная случайная
выборка из 1400 находок почти целиком состоит из того, в чём система и так
уверена, и почти не содержит спорного. Здесь наоборот — берём по квоте из
каждой группы, где ошибка означала бы разное:

  * ИИ разрешил и не сомневался  → ловим ложную уверенность;
  * ИИ разрешил, критик принял   → ловим ошибку, пережившую две проверки;
  * верификатор отклонил         → ловим ЛОЖНЫЙ отказ (ответ был верен);
  * модель отказалась сама       → проверяем, что отказ был обоснован;
  * детерминированная находка    → проверяем базу, а не только ИИ;
  * вопрос инженеру              → проверяем, что вопрос вообще осмыслен.

Использование:
    python scripts/stage_comparison_gold_set.py <production_dir> [--size 80]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

#: Доля выборки на каждую группу. Сумма — единица.
STRATA = (
    ("ai_resolved_high", "ИИ разрешил, уверенность высокая", 0.15),
    ("ai_resolved_low", "ИИ разрешил, уверенность средняя или низкая", 0.10),
    ("ai_critic_accepted", "ИИ разрешил, критик принял", 0.10),
    # Самая информативная группа: верификатор вывод принял, а критик
    # отклонил. Кто из них прав, без разметки не скажет никто.
    ("ai_critic_rejected", "Верификатор принял, критик отклонил", 0.17),
    # Чертёж прямо спорит с текстовым разбором. Кто прав — вопрос к инженеру,
    # и ответ на него настраивает доверие сразу к обоим слоям.
    ("vision_contradicts", "Чертёж противоречит текстовому разбору", 0.13),
    ("verifier_rejected", "Верификатор отклонил вывод ИИ", 0.08),
    ("model_declined", "ИИ отказался сам", 0.13),
    ("deterministic_change", "Находка без участия ИИ", 0.12),
    ("engineer_question", "Вопрос инженеру", 0.07),
)

#: Что инженер отвечает по каждому случаю. Три вопроса, не больше.
LABEL_FORM = {
    "verdict": ["ВЕРНО", "НЕВЕРНО", "НЕ ЗНАЮ"],
    "what_is_wrong": (
        "если НЕВЕРНО — что именно: не тот объект / не то значение / "
        "перепутаны стороны / это не изменение / другое"
    ),
    "correct_answer": "если знаете правильный ответ — одной строкой",
}


def _load(directory: Path, name: str) -> dict[str, Any]:
    path = directory / name
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _case(kind: str, identifier: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {"stratum": kind, "id": identifier, **dict(payload)}


def collect(directory: Path) -> dict[str, list[dict[str, Any]]]:
    ai = _load(directory, "ai_resolutions.json")
    synthesis = _load(directory, "unified_synthesis.json")
    questions = _load(directory, "review_questions.json")
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key, _t, _s in STRATA}

    for item in ai.get("resolutions") or []:
        if not isinstance(item, Mapping):
            continue
        identifier = str(item.get("review_evidence_id") or "")
        if item.get("status") == "AI_RESOLVED":
            typed = item.get("typed_resolution") or {}
            payload = {
                "object": typed.get("object_label"),
                "change": f"{typed.get('dimension')} · {typed.get('direction')}",
                "before": typed.get("before_value"),
                "after": typed.get("after_value"),
                "summary": item.get("engineering_summary"),
                "confidence": item.get("confidence"),
            }
            if item.get("critic"):
                buckets["ai_critic_accepted"].append(
                    _case("ai_critic_accepted", identifier, payload)
                )
            elif item.get("confidence") == "HIGH":
                buckets["ai_resolved_high"].append(
                    _case("ai_resolved_high", identifier, payload)
                )
            else:
                buckets["ai_resolved_low"].append(
                    _case("ai_resolved_low", identifier, payload)
                )
            continue
        reason = str(item.get("reason_code") or "")
        payload = {
            "reason": reason,
            "detail": item.get("reason_detail"),
            "question": item.get("human_question"),
        }
        if reason == "CRITIC_REJECTED":
            critic = item.get("critic") or {}
            buckets["ai_critic_rejected"].append(_case(
                "ai_critic_rejected", identifier, {
                    **payload,
                    "critic_verdict": critic.get("verdict"),
                    "critic_explanation": critic.get("explanation"),
                    "critic_problems": ", ".join(
                        str(problem.get("code"))
                        for problem in critic.get("problems") or []
                        if isinstance(problem, Mapping)
                    ),
                },
            ))
        elif reason == "VISION_CONTRADICTS_TEXT":
            seen = item.get("vision") or {}
            buckets["vision_contradicts"].append(_case(
                "vision_contradicts", identifier, {
                    **payload,
                    "видно_слева": seen.get("observed_left"),
                    "видно_справа": seen.get("observed_right"),
                },
            ))
        elif reason == "VERIFIER_REJECTED":
            buckets["verifier_rejected"].append(
                _case("verifier_rejected", identifier, payload)
            )
        elif reason == "MODEL_DECLINED":
            buckets["model_declined"].append(
                _case("model_declined", identifier, payload)
            )

    resolved_ids = {
        str(item.get("review_evidence_id") or "")
        for item in ai.get("resolutions") or []
        if isinstance(item, Mapping) and item.get("status") == "AI_RESOLVED"
    }
    for change in synthesis.get("changes") or []:
        if not isinstance(change, Mapping):
            continue
        provenance = change.get("provenance") or {}
        atoms = provenance.get("source_atoms") or []
        touched_by_ai = any(
            isinstance(atom, Mapping)
            and isinstance(atom.get("provenance"), Mapping)
            and atom["provenance"].get("ai_change_resolution")
            for atom in atoms
        )
        if touched_by_ai or str(change.get("change_id") or "") in resolved_ids:
            continue
        buckets["deterministic_change"].append(_case(
            "deterministic_change", str(change.get("change_id") or ""), {
                "object": change.get("subject_ref"),
                "change": f"{change.get('dimension')} · {change.get('direction')}",
                "before": change.get("before_value"),
                "after": change.get("after_value"),
            },
        ))

    for question in questions.get("questions") or []:
        if not isinstance(question, Mapping):
            continue
        buckets["engineer_question"].append(_case(
            "engineer_question", str(question.get("question_id") or ""), {
                "category": question.get("category"),
                "prompt": question.get("prompt"),
            },
        ))
    return buckets


def propose(buckets: Mapping[str, list[dict[str, Any]]], size: int) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    shortfall: list[str] = []
    for key, title, share in STRATA:
        quota = max(1, round(size * share))
        available = buckets.get(key) or []
        # Детерминированный отбор: каждый n-й, а не случайный. Одна и та же
        # выборка на одном и том же прогоне — иначе разметку не повторить.
        if len(available) <= quota:
            taken = list(available)
            if len(available) < quota:
                shortfall.append(f"{title}: доступно {len(available)} из {quota}")
        else:
            step = len(available) / quota
            taken = [available[int(index * step)] for index in range(quota)]
        for case in taken:
            case["stratum_title"] = title
        selected.extend(taken)
    return {
        "kind": "stage_comparison_gold_set_proposal",
        "schema_version": "gold-set-proposal.v1",
        "requested_size": size,
        "selected_size": len(selected),
        "label_form": LABEL_FORM,
        "strata": [
            {
                "key": key,
                "title": title,
                "share": share,
                "available": len(buckets.get(key) or []),
                "selected": sum(1 for case in selected if case["stratum"] == key),
            }
            for key, title, share in STRATA
        ],
        "shortfall": shortfall,
        "cases": selected,
    }


def render(proposal: Mapping[str, Any]) -> str:
    lines = [
        "# Эталонная выборка: что разметить один раз",
        "",
        f"Отобрано случаев: {proposal['selected_size']} из "
        f"{proposal['requested_size']} запрошенных.",
        "",
        "По каждому случаю нужны три ответа: ВЕРНО / НЕВЕРНО / НЕ ЗНАЮ;",
        "если НЕВЕРНО — что именно не так; если знаете — правильный ответ.",
        "",
        "| Группа | Доступно | В выборке |",
        "|---|---:|---:|",
    ]
    for stratum in proposal["strata"]:
        lines.append(
            f"| {stratum['title']} | {stratum['available']} | {stratum['selected']} |"
        )
    if proposal["shortfall"]:
        lines += ["", "**Не хватило случаев:**", ""]
        lines += [f"* {item}" for item in proposal["shortfall"]]
    lines += ["", "## Случаи", ""]
    for index, case in enumerate(proposal["cases"], start=1):
        lines.append(f"### {index}. {case['stratum_title']}")
        lines.append("")
        for key, value in case.items():
            if key in {"stratum", "stratum_title"} or value in (None, ""):
                continue
            lines.append(f"* **{key}**: {value}")
        lines += ["", "Ответ: ______  Что не так: ______  Как правильно: ______", ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("production_dir")
    parser.add_argument("--size", type=int, default=80)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    directory = Path(args.production_dir)
    proposal = propose(collect(directory), args.size)
    target = Path(args.out) if args.out else directory / "gold_set_proposal"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.with_suffix(".json").write_text(
        json.dumps(proposal, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    target.with_suffix(".md").write_text(render(proposal), encoding="utf-8")
    print(json.dumps({
        "selected": proposal["selected_size"],
        "strata": {item["key"]: item["selected"] for item in proposal["strata"]},
        "shortfall": proposal["shortfall"],
        "json": str(target.with_suffix(".json")),
        "markdown": str(target.with_suffix(".md")),
    }, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
