"""Детерминированный инженерный паспорт кандидата уровня раздела."""
from __future__ import annotations

from typing import Any


PASSPORT_VERSION = 1
_PARAMETER_FIELDS = (
    "designation", "type_mark", "code", "manufacturer", "unit", "quantity",
    "mass", "total_mass", "note",
)


def _clean(value: Any, limit: int = 3000) -> str:
    text = " ".join(str(value or "").split())
    return text[:limit]


def build_engineering_passport(
    candidate: dict,
    source_decisions: list[dict],
    targets: list[dict],
) -> dict:
    subjects: list[dict] = []
    seen_subjects: set[tuple[str, str, str]] = set()
    target_summaries: list[dict] = []
    for target in targets:
        rows = list(target.get("rows") or [])
        row_summaries: list[dict] = []
        for row in rows:
            key = (
                _clean(row.get("name"), 1000),
                _clean(row.get("type_mark") or row.get("designation"), 1000),
                _clean(row.get("code"), 500),
            )
            if key not in seen_subjects:
                seen_subjects.add(key)
                subjects.append({"name": key[0], "type_mark": key[1], "code": key[2]})
            parameters = {
                field: _clean(row.get(field), 1000)
                for field in _PARAMETER_FIELDS
                if _clean(row.get(field), 1000)
            }
            missing = [
                field for field in ("name", "type_mark", "designation", "code", "unit", "quantity")
                if not _clean(row.get(field), 1000)
            ]
            row_summaries.append({
                "row_id": row.get("row_id"),
                "page": row.get("page"),
                "sheet": row.get("sheet") or "",
                "name": row.get("name") or "",
                "parameters": parameters,
                "missing_parameters": missing,
            })
        target_summaries.append({
            "project_id": target.get("project_id"),
            "project_name": target.get("project_name") or target.get("project_id"),
            "version_id": target.get("version_id") or "",
            "rows": row_summaries,
        })

    constraints: list[dict] = []
    for source in source_decisions:
        for field, kind in (("risks", "risk"), ("norm", "norm")):
            value = _clean(source.get(field))
            if value:
                constraints.append({
                    "kind": kind,
                    "text": value,
                    "source_ref": source.get("source_ref") or "",
                })

    source_actions = [{
        "source_ref": source.get("source_ref") or "",
        "project_id": source.get("project_id") or "",
        "version_id": source.get("version_id") or "",
        "current": _clean(source.get("current")),
        "proposed": _clean(source.get("proposed")),
    } for source in source_decisions]

    return {
        "passport_version": PASSPORT_VERSION,
        "title": _clean(candidate.get("title"), 1000),
        "action": _clean(candidate.get("representative_proposal")),
        "subjects": subjects,
        "source_actions": source_actions,
        "constraints": constraints,
        "targets": target_summaries,
        "scope": {
            "source_projects": sorted({item["project_id"] for item in source_actions if item["project_id"]}),
            "target_projects": [str(item.get("project_id") or "") for item in target_summaries],
        },
    }


__all__ = ["PASSPORT_VERSION", "build_engineering_passport"]
