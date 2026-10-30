"""Сводные пакеты оптимизации для решения инженера.

Сигналы остаются доказательными деталями, но инженер работает с пакетом:
одна идея, весь найденный объём раздела, технические исключения и один
экономический вердикт.  Сервис детерминированный и не вызывает внешние модели.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import hashlib
import re
from typing import Any


_VERDICT_RANK = {
    "applicable": 1,
    "applicable_with_conditions": 2,
    "reject": 3,
    "needs_graphics": 4,
    "needs_data": 5,
    "": 5,
}


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _norm(value: Any) -> str:
    text = _text(value).lower().replace("ё", "е")
    return re.sub(r"\s+", " ", re.sub(r"[^0-9a-zа-я]+", " ", text)).strip()


def _tokens(value: Any) -> set[str]:
    return {
        token for token in re.findall(r"[0-9a-zа-я][0-9a-zа-я-]{2,}", _norm(value))
        if token not in {"для", "при", "без", "шт", "компл", "проект", "проекте"}
    }


def _family(text: str) -> tuple[str, str]:
    """Return a stable material family and user-facing label."""
    value = _norm(text)
    rules = (
        (("лоток", "лотков", "кабеленесущ", "трасс"), "cable_tray", "Кабельные лотки и трассы"),
        (("кабел", "провод"), "cable", "Кабельная продукция"),
        (("гофр", "труб", "труба", "пн"), "conduit", "Трубы и гофрированные изделия"),
        (("крепеж", "креплен", "хомут", "анкер", "держател"), "fastening", "Крепёж и монтаж"),
        (("розет", "выключател", "переключател", "коробк", "рамк"), "electrical_fittings", "Розетки, выключатели и коробки"),
        (("светиль", "указател", "освещен"), "lighting", "Освещение и указатели"),
        (("датчик", "сенсор"), "sensors", "Датчики"),
        (("нагрев", "обогрев", "теплолюкс", "freezstop"), "heating", "Электрообогрев"),
        (("трансформ", "ятп"), "transformer", "Трансформаторы"),
        (("шкаф", "щит", "автомат"), "panels", "Щиты и шкафы"),
        (("огнетуш", "очки", "указатель напряж"), "safety", "Средства безопасности"),
    )
    for needles, key, label in rules:
        if any(needle in value for needle in needles):
            return key, label
    words = sorted(_tokens(value))[:3]
    key = "other_" + hashlib.sha1(" ".join(words).encode("utf-8")).hexdigest()[:8]
    return key, "Прочие материалы"


def _action(text: str, kind: str) -> tuple[str, str]:
    value = _norm(text)
    if kind == "type_size_reduction_opportunity" or any(word in value for word in ("унифиц", "типоразмер", "сечен", "размер")):
        return "standardization", "Унифицировать исполнения"
    if any(word in value for word in ("производител", "завод", "вендор", "бренд", "марку")):
        return "manufacturer", "Унифицировать производителя"
    if any(word in value for word in ("огнестой", "frhf", "пофидер", "пожар")):
        return "fire_class", "Оптимизировать пожарное исполнение"
    if any(word in value for word in ("креплен", "анкер", "держател", "шаг монтажа")):
        return "installation", "Упростить монтаж"
    if any(word in value for word in ("контроллер", "автоматик", "эксплуатац", "энергопотреб")):
        return "lifecycle", "Снизить стоимость эксплуатации"
    return "alternative", "Проверить функциональный аналог"


def _group_key(signal: dict) -> tuple[str, str, str, str]:
    summary = signal.get("package_summary") or {}
    source = summary.get("source_reference") or {}
    text = " ".join(
        [
            _text(signal.get("title")),
            _text(signal.get("reason")),
            _text(signal.get("representative_proposal")),
            _text(source.get("current")),
            _text(source.get("proposed")),
            " ".join(_text(item.get("label")) for item in (summary.get("scope") or {}).get("material_groups") or []),
        ]
    )
    family_key, _ = _family(text)
    action_key, _ = _action(text, _text(signal.get("kind")))
    proposal = " | ".join(filter(None, [
        _text(source.get("current")),
        _text(source.get("proposed")),
        _text(signal.get("representative_proposal")),
        _text(signal.get("title")),
        _text(signal.get("signal_id")),
    ]))
    # Keep technically different candidates separate.  Family/action alone
    # would merge every cable or panel idea into one unreviewable package.
    proposal_key = hashlib.sha1(_norm(proposal).encode("utf-8")).hexdigest()[:12]
    return _text(signal.get("kind")), family_key, action_key, proposal_key


def _num(value: Any) -> float | None:
    try:
        if value is None or _text(value) == "":
            return None
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _scope(rows: list[dict]) -> dict:
    projects: dict[str, dict] = {}
    quantities: dict[str, float] = defaultdict(float)
    unknown: list[str] = []
    untraceable: list[str] = []
    row_ids: list[str] = []
    for row in rows:
        row_id = _text(row.get("row_id"))
        if not row_id:
            continue
        row_ids.append(row_id)
        project_id = _text(row.get("project_id")) or "unknown"
        if not _text(row.get("project_id")) or not _text(row.get("version_id")):
            untraceable.append(row_id)
        project = projects.setdefault(project_id, {
            "project_id": project_id,
            "project_name": _text(row.get("project_name")) or project_id,
            "version_id": _text(row.get("version_id")),
            "row_count": 0,
        })
        project["row_count"] += 1
        quantity = _num(row.get("quantity"))
        unit = _text(row.get("unit"))
        if quantity is None or not unit:
            unknown.append(row_id)
        else:
            quantities[unit] += quantity
    return {
        "row_count": len(set(row_ids)),
        "row_ids": sorted(set(row_ids)),
        "project_count": len(projects),
        "projects": sorted(projects.values(), key=lambda value: value["project_id"]),
        "quantity_by_unit": {key: round(value, 6) for key, value in sorted(quantities.items())},
        "unknown_quantity_row_ids": sorted(set(unknown)),
        "untraceable_row_ids": sorted(set(untraceable)),
    }


def _latest_replications(replications: list[dict]) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for job in replications:
        if job.get("input_stale"):
            continue
        signal_id = _text(job.get("signal_id"))
        if not signal_id:
            continue
        old = latest.get(signal_id)
        if old is None or _text(job.get("updated_at")) > _text(old.get("updated_at")):
            latest[signal_id] = job
    return latest


def _assessment_rows(signal: dict, job: dict | None, rows_by_id: dict[str, dict]) -> dict[str, str]:
    result: dict[str, str] = {}
    if not job:
        return result
    for assessment in job.get("agent_assessments") or []:
        verdict = _text(assessment.get("resolved_verdict") or assessment.get("verdict"))
        for row_id in assessment.get("target_row_ids") or []:
            key = _text(row_id)
            if key not in rows_by_id:
                continue
            current = result.get(key)
            if current is None or _VERDICT_RANK.get(verdict, 5) > _VERDICT_RANK.get(current, 5):
                result[key] = verdict
    # A signal without an assessment is unresolved, even if its rows were found.
    for row_id in signal.get("target_row_ids") or []:
        key = _text(row_id)
        if key in rows_by_id:
            result.setdefault(key, "")
    return result


def _technical_partition(rows: list[dict], signals: list[dict], latest: dict[str, dict], rows_by_id: dict[str, dict]) -> dict:
    statuses: dict[str, str] = {}
    reasons: dict[str, list[str]] = defaultdict(list)
    for signal in signals:
        assessment_rows = _assessment_rows(signal, latest.get(_text(signal.get("signal_id"))), rows_by_id)
        job = latest.get(_text(signal.get("signal_id"))) or {}
        for row_id, verdict in assessment_rows.items():
            if verdict in {"", "needs_data", "needs_graphics"}:
                normalized = "needs_data"
            elif verdict == "reject":
                normalized = "excluded"
            elif verdict == "applicable_with_conditions":
                normalized = "conditional"
            else:
                normalized = "eligible"
            current = statuses.get(row_id)
            rank = {"eligible": 1, "conditional": 2, "excluded": 3, "needs_data": 4}
            if current is None or rank[normalized] > rank[current]:
                statuses[row_id] = normalized
            for assessment in job.get("agent_assessments") or []:
                if row_id in [str(value) for value in assessment.get("target_row_ids") or []]:
                    reasons[row_id].extend(_text(value) for value in (assessment.get("conditions") or []) if _text(value))
    buckets = {"eligible": [], "conditional": [], "excluded": [], "needs_data": []}
    for row in rows:
        row_id = _text(row.get("row_id"))
        buckets[statuses.get(row_id, "needs_data")].append(row)
    exceptions = []
    for bucket, bucket_rows in buckets.items():
        if not bucket_rows or bucket == "eligible":
            continue
        by_project: dict[str, dict] = {}
        for row in bucket_rows:
            project_id = _text(row.get("project_id")) or "unknown"
            item = by_project.setdefault(project_id, {
                "project_id": project_id,
                "project_name": _text(row.get("project_name")) or project_id,
                "row_ids": [],
                "status": bucket,
                "reasons": [],
            })
            item["row_ids"].append(_text(row.get("row_id")))
            item["reasons"].extend(reasons.get(_text(row.get("row_id")), []))
        for item in by_project.values():
            item["row_ids"] = sorted(set(item["row_ids"]))
            item["reasons"] = sorted(set(item["reasons"]))[:8]
            exceptions.append(item)
    return {
        "eligible": _scope(buckets["eligible"]),
        "conditional": _scope(buckets["conditional"]),
        "excluded": _scope(buckets["excluded"]),
        "needs_data": _scope(buckets["needs_data"]),
        "exceptions": sorted(exceptions, key=lambda item: (item["status"], item["project_id"])),
        "status": (
            "needs_data" if buckets["needs_data"]
            else "exceptions" if buckets["conditional"] or buckets["excluded"]
            else "pass"
        ),
    }


def _economic_status(signals: list[dict], latest: dict[str, dict]) -> tuple[str, dict]:
    monetary: list[dict] = []
    for signal in signals:
        job = latest.get(_text(signal.get("signal_id"))) or {}
        evaluation = job.get("alternative_evaluation") or {}
        value = (evaluation.get("effect") or {}).get("monetary")
        if value:
            monetary.append(value)
    currencies = {_text(item.get("currency")).upper() for item in monetary if _text(item.get("currency"))}
    # A package is only economically comparable when every member has a
    # calculation.  Partial sums would make a section-wide decision look
    # cheaper simply because unpriced candidates were silently omitted.
    if len(monetary) != len(signals) or not monetary or len(currencies) != 1:
        return "needs_data", {"status": "needs_data", "sources": len(monetary), "currency": ""}
    baseline = sum(_num(item.get("baseline")) or 0 for item in monetary)
    proposal = sum(_num(item.get("proposal")) or 0 for item in monetary)
    delta = proposal - baseline
    worst = sum(_num((item.get("delta_range") or {}).get("worst")) or (_num(item.get("delta")) or 0) for item in monetary)
    status = "positive" if delta < 0 and worst < 0 else "negative" if delta >= 0 else "needs_data"
    return status, {
        "status": "calculated" if status != "needs_data" else "uncertain",
        "baseline": baseline,
        "proposal": proposal,
        "delta": delta,
        "worst_delta": worst,
        "currency": next(iter(currencies)),
        "sources": len(monetary),
    }


def build_section_optimization_portfolio(snapshot: dict, replications: list[dict] | None = None) -> list[dict]:
    """Build compact section-wide packages from current signals."""
    signals = [item for item in snapshot.get("signals") or [] if _text(item.get("signal_id"))]
    grouped: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    for signal in signals:
        grouped[_group_key(signal)].append(signal)
    rows_by_id = {
        _text(row.get("row_id")): row
        for row in snapshot.get("specification_rows") or []
        if _text(row.get("row_id"))
    }
    latest = _latest_replications(list(replications or []))
    packages: list[dict] = []
    for (kind_key, family_key, action_key, _proposal_key), members in grouped.items():
        row_ids = sorted({
            _text(row_id)
            for signal in members
            for row_id in signal.get("target_row_ids") or []
            if _text(row_id) in rows_by_id
        })
        rows = [rows_by_id[row_id] for row_id in row_ids]
        referenced_row_ids = sorted({
            _text(row_id)
            for signal in members
            for row_id in (signal.get("target_row_ids") or [])
            if _text(row_id)
        })
        missing_row_ids = sorted(set(referenced_row_ids) - set(row_ids))
        source_row_ids = sorted({
            _text(row_id)
            for signal in members
            for row_id in signal.get("source_row_ids") or []
            if _text(row_id) in rows_by_id
        })
        source_rows = [rows_by_id[row_id] for row_id in source_row_ids]
        family_label = _family(" ".join(_text(item.get("title")) for item in members))[1]
        action_label = _action(" ".join(_text(item.get("title")) for item in members), _text(members[0].get("kind")))[1]
        seed = "|".join(sorted(_text(item.get("signal_id")) for item in members))
        package_id = "PKG-" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
        dossier_revision = "|".join(
            f"{_text(item.get('signal_id'))}:{_text((latest.get(_text(item.get('signal_id'))) or {}).get('input_fingerprint'))}:{_text((latest.get(_text(item.get('signal_id'))) or {}).get('updated_at'))}"
            for item in members
        )
        fingerprint = hashlib.sha256((
            _text(snapshot.get("meta", {}).get("generated_at")) + "|" +
            package_id + "|" + "|".join(row_ids) + "|" + dossier_revision
        ).encode("utf-8")).hexdigest()
        partition = _technical_partition(rows, members, latest, rows_by_id)
        dossier_blockers = []
        for member in members:
            job = latest.get(_text(member.get("signal_id"))) or {}
            if job and (job.get("critic") or {}).get("status") not in {"pass", ""}:
                dossier_blockers.append(f"Critic: {(job.get('critic') or {}).get('status')}")
            if job.get("graphics_required") and job.get("graphics_status") not in {"done", "completed", "supports_replication", "not_required"}:
                dossier_blockers.append("Обязательная графическая проверка не завершена")
        if dossier_blockers:
            partition["status"] = "needs_data"
            partition["exceptions"].append({"status": "needs_data", "project_id": "досье", "row_ids": [], "reasons": sorted(set(dossier_blockers))})
        economic_status, economic = _economic_status(members, latest)
        source_refs = sorted({
            _text(ref)
            for signal in members
            for ref in signal.get("evidence_refs") or []
            if _text(ref)
        })
        proposals = sorted({
            _text(((signal.get("package_summary") or {}).get("source_reference") or {}).get("proposed"))
            for signal in members
            if _text(((signal.get("package_summary") or {}).get("source_reference") or {}).get("proposed"))
        }, key=len, reverse=True)
        blockers = []
        if partition["status"] != "pass":
            blockers.append(f"Техническая проверка: {partition['status']}")
        if partition["needs_data"]["row_count"]:
            blockers.append(f"Не подтверждено строк: {partition['needs_data']['row_count']}")
        if partition["excluded"]["row_count"]:
            blockers.append(f"Исключено из общего решения: {partition['excluded']['row_count']} строк")
        if economic_status == "needs_data":
            blockers.append("Нужны сопоставимые затраты по всему применимому объёму")
        if missing_row_ids:
            blockers.append(f"Не найдены строки спецификации: {len(missing_row_ids)}")
        packages.append({
            "package_id": package_id,
            "fingerprint": fingerprint,
            "family_key": family_key,
            "action_key": action_key,
            "title": f"{family_label}: {action_label}",
            "question": "Можно ли применить это решение ко всему применимому объёму раздела и получить подтверждённую экономию?",
            "kind_counts": dict(sorted({kind: sum(1 for item in members if item.get("kind") == kind) for kind in {item.get("kind") for item in members}}.items())),
            "signal_ids": sorted(_text(item.get("signal_id")) for item in members),
            "signal_titles": [_text(item.get("title")) for item in members],
            "source_refs": source_refs,
            "proposed_actions": proposals[:5],
            "scope": _scope(rows),
            "missing_row_ids": missing_row_ids,
            "source_scope": _scope(source_rows),
            "applicability": partition,
            "technical_review": {
                "status": partition["status"],
                "message": {
                    "pass": "Все строки пакета имеют подтверждённую применимость.",
                    "exceptions": "Есть условные или исключённые строки; они показаны отдельно.",
                    "needs_data": "Нельзя считать экономию по неподтверждённым строкам.",
                }[partition["status"]],
                "exceptions": partition["exceptions"],
            },
            "economic_status": economic_status,
            "effect": economic,
            "checklist": [
                "Сверить обязательные параметры, пожарное исполнение, IP, размеры/сечения и монтажные интерфейсы.",
                "Отделить исключения и не включать их в общий расчёт.",
                "Сложить стоимость материала, монтажа, переделки и эксплуатации по применимому объёму.",
                "Зафиксировать источник и дату цен в одной валюте.",
            ],
            "blockers": blockers,
            "decision_gate": {
                "economic_yes": {
                    "allowed": partition["status"] == "pass" and economic_status == "positive",
                    "reason": "Нужны технически подтверждённые строки и отрицательная дельта в худшем сценарии.",
                },
                "economic_no": {
                    "allowed": economic_status == "negative" or partition["status"] == "exceptions",
                    "reason": "Экономия отсутствует либо общий вариант технически неприменим.",
                },
                "needs_data": {"allowed": True, "reason": "; ".join(blockers) or "Недостаточно данных."},
            },
            "decision": None,
            "decision_note": "",
        })
    return sorted(packages, key=lambda item: (-item["scope"]["row_count"], item["title"]))


__all__ = ["build_section_optimization_portfolio"]
