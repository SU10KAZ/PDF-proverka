"""Deterministic apply_dedupe ? no model calls."""
from __future__ import annotations

from typing import Any


def compact_change(change: dict[str, Any]) -> dict[str, Any]:
    return {
        k: change[k]
        for k in (
            "projectchange_id",
            "engineering_subject",
            "scope",
            "locations",
            "change_summary",
            "old_state",
            "new_state",
            "changed_parameters",
            "old_pages",
            "new_pages",
        )
    }


def apply_dedupe(
    pair: str,
    changes: list[dict[str, Any]],
    raw: dict[str, Any],
) -> list[dict[str, Any]]:
    """Apply a frozen dedupe decision payload to mined ProjectChanges.

    ``raw`` must already be model output (or a fixture). This function is
    purely deterministic and never calls a model.
    """
    by_id = {c["projectchange_id"]: c for c in changes}
    seen: list[str] = []
    output: list[dict[str, Any]] = []
    for decision in raw["decisions"]:
        ids = decision["projectchange_ids"]
        if (
            not ids
            or any(i not in by_id for i in ids)
            or (decision["decision"] == "KEEP_SEPARATE" and len(ids) != 1)
            or (decision["decision"] == "MERGE_DUPLICATES" and len(ids) < 2)
        ):
            raise RuntimeError(f"Pair {pair} invalid dedupe decision")
        seen.extend(ids)
        members = [by_id[i] for i in ids]
        canonical = dict(members[0])
        canonical["dedupe_lineage"] = ids
        canonical["dedupe_reason"] = decision["reason"]
        if len(members) > 1:
            for field in ("locations", "modalities"):
                canonical[field] = list(
                    dict.fromkeys(x for m in members for x in m[field])
                )
            for field in ("old_pages", "new_pages"):
                canonical[field] = sorted({x for m in members for x in m[field]})
            for field in ("evidence_items", "changed_parameters"):
                canonical[field] = [x for m in members for x in m[field]]
        output.append(canonical)
    if len(seen) != len(set(seen)) or set(seen) != set(by_id):
        raise RuntimeError(f"Pair {pair} dedupe is not exact partition")
    return output
