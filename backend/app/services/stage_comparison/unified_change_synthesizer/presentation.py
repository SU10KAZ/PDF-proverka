"""Deterministic presentation grouping over immutable atomic changes."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping

from .identity import stable_group_id


PRESENTATION_VERSION = "unified-change-presentation-v1"


def build_presentation_groups(
    changes: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Group only compatible PARAMETER siblings without creating a new fact."""
    buckets: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for change in changes:
        if (
            change.get("dimension") == "PARAMETER"
            and change.get("relation_status") != "CONTRADICTORY"
        ):
            buckets[
                (
                    str(change.get("scope_ref") or ""),
                    str(change.get("subject_ref") or ""),
                    "PARAMETER",
                )
            ].append(change)

    groups: list[dict[str, Any]] = []
    for (scope_ref, subject_ref, family), related in sorted(buckets.items()):
        if len(related) < 2:
            continue
        change_ids = sorted(str(change["change_id"]) for change in related)
        group_identity = {
            "presentation_version": PRESENTATION_VERSION,
            "scope_ref": scope_ref,
            "subject_ref": subject_ref,
            "family": family,
        }
        groups.append(
            {
                "group_id": stable_group_id("pgroup_", group_identity),
                "scope_ref": scope_ref,
                "subject_ref": subject_ref,
                "family": family,
                "change_ids": change_ids,
                "title": "Изменены параметры объекта",
                "provenance": {
                    "presentation_version": PRESENTATION_VERSION,
                    "group_identity": group_identity,
                    "creates_engineering_fact": False,
                },
            }
        )
    return sorted(groups, key=lambda group: group["group_id"])


__all__ = ["PRESENTATION_VERSION", "build_presentation_groups"]
