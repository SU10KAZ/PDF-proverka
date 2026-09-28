"""Детерминированная проверка досье перед решением эксперта.

Critic не придумывает инженерные факты и не вызывает модель. Он проверяет
полноту адресации, согласованность вердикта, графики, условий и недостающих
данных. Результат объясняет, какие действия эксперт вправе выполнить.
"""
from __future__ import annotations

from collections import Counter
from typing import Any


CRITIC_VERSION = 1


def _clean(value: Any, limit: int = 2000) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def review_replication_dossier(dossier: dict, assessments: list[dict]) -> dict:
    targets = {
        str(target.get("project_id") or ""): target
        for target in (dossier.get("targets") or [])
        if target.get("project_id")
    }
    source_decisions = list(dossier.get("source_decisions") or [])
    by_project = {
        str(item.get("project_id") or ""): item
        for item in assessments
        if item.get("project_id")
    }
    reviews: list[dict] = []

    for project_id, target in targets.items():
        assessment = by_project.get(project_id) or {}
        allowed_rows = {
            str(row.get("row_id") or "")
            for row in (target.get("rows") or [])
            if row.get("row_id")
        }
        cited_rows = {
            str(row_id) for row_id in (assessment.get("target_row_ids") or [])
            if str(row_id) in allowed_rows
        }
        verdict = str(assessment.get("resolved_verdict") or assessment.get("verdict") or "needs_data")
        missing_data = [_clean(value, 1500) for value in (assessment.get("missing_data") or []) if value]
        conditions = [_clean(value, 1500) for value in (assessment.get("conditions") or []) if value]
        findings: list[dict] = []

        def add(code: str, severity: str, message: str) -> None:
            findings.append({"code": code, "severity": severity, "message": message})

        if not source_decisions:
            add("source_decision_missing", "blocking", "В досье отсутствует исходное принятое решение.")
        if not cited_rows:
            add("target_evidence_missing", "blocking", "Нет допустимой ссылки на целевую строку спецификации.")
        if not _clean(assessment.get("reason")):
            add("reason_missing", "blocking", "Не приведено инженерное основание вердикта.")
        if missing_data:
            add("missing_data", "blocking", "Остались незакрытые исходные данные: " + "; ".join(missing_data))
        if verdict in {"needs_data", "needs_graphics"}:
            add("assessment_incomplete", "blocking", "Заключение ещё не доведено до инженерного решения.")
        if verdict == "applicable_with_conditions" and not conditions:
            add("conditions_missing", "blocking", "Условная применимость указана без перечня условий.")

        graphics_required = bool(assessment.get("graphics_required"))
        graphics_review = assessment.get("graphics_review") or {}
        if graphics_required:
            conclusion = str(graphics_review.get("conclusion") or "")
            if conclusion not in {"supports_replication", "contradicts_replication"}:
                add("graphics_inconclusive", "blocking", "Запрошенная графическая проверка не дала определённого результата.")
            elif not graphics_review.get("evidence"):
                add("graphics_evidence_missing", "blocking", "Графический вывод не содержит адресуемого доказательства целевого проекта.")

        status = "blocked" if any(item["severity"] == "blocking" for item in findings) else "pass"
        allowed_decisions = (
            ["rejected", "returned"]
            if status == "blocked"
            else ["accepted", "accepted_with_conditions", "rejected", "returned"]
        )
        reviews.append({
            "project_id": project_id,
            "status": status,
            "verdict": verdict,
            "findings": findings,
            "allowed_expert_decisions": allowed_decisions,
        })

    counts = Counter(item["status"] for item in reviews)
    return {
        "critic_version": CRITIC_VERSION,
        "status": "blocked" if counts.get("blocked") else "pass",
        "target_reviews": reviews,
        "counts": {"pass": counts.get("pass", 0), "blocked": counts.get("blocked", 0)},
    }


__all__ = ["CRITIC_VERSION", "review_replication_dossier"]
