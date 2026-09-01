"""Closed, budgeted evidence expansion for v2."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from . import schemas
from .context import ContextBundle


@dataclass
class ExpansionBudget:
    maximum: int = 2
    used: int = 0

    def take(self) -> bool:
        if self.used >= max(0, min(2, int(self.maximum))):
            return False
        self.used += 1
        return True


def _tokens(value: Any) -> set[str]:
    return {
        token for token in str(value or "").lower().replace("ё", "е").split()
        if len(token) > 2
    }


def expand_focus(
    bundle: ContextBundle,
    task_id: str,
    requested: Sequence[str],
    *,
    limit: int = 12,
) -> list[str]:
    """Add only evidence described by the closed expansion vocabulary.

    The function never reads the filesystem.  It selects records already in
    the frozen catalog; image crops therefore remain unavailable unless a
    caller explicitly froze such a record beforehand.
    """
    if any(value not in schemas.EXPANSION_ALLOWLIST for value in requested):
        return []
    focus = bundle.focused_by_task.get(task_id)
    if not isinstance(focus, dict):
        return []
    candidates = [
        bundle.evidence_catalog[ref]
        for ref in focus.get("candidate_refs") or ()
        if ref in bundle.evidence_catalog
    ]
    existing = set(focus.get("context_refs") or ()) | set(
        focus.get("candidate_refs") or ()
    )
    added: list[str] = []

    for kind in requested:
        if kind in {"graph_neighbors", "neighboring_entities"}:
            node_ids = {
                str(value.get("entity_id") or "") for value in candidates
                if value.get("entity_id")
            }
            edge_refs = [
                ref for ref, value in bundle.evidence_catalog.items()
                if value.get("relation")
                and node_ids & {
                    str(value.get("from_entity") or ""),
                    str(value.get("to_entity") or ""),
                }
            ]
            for ref in edge_refs:
                added.append(ref)
                edge = bundle.evidence_catalog[ref]
                side = str(edge.get("side") or "")
                for node_id in (edge.get("from_entity"), edge.get("to_entity")):
                    node_ref = f"{side}:NODE:{node_id}"
                    if node_ref in bundle.evidence_catalog:
                        added.append(node_ref)
        elif kind in {"neighboring_rows", "summary_row"}:
            sections = {str(value.get("section") or "") for value in candidates}
            words = set().union(*(
                _tokens(value.get("label")) for value in candidates
            )) if candidates else set()
            for ref, value in bundle.evidence_catalog.items():
                if ":ROW:" not in ref:
                    continue
                same_section = str(value.get("section") or "") in sections - {""}
                same_family = bool(words & _tokens(value.get("label")))
                is_summary = value.get("row_kind") == "CONSUMER_TOTAL"
                if kind == "summary_row" and is_summary and same_family:
                    added.append(ref)
                elif kind == "neighboring_rows" and same_section:
                    added.append(ref)
        elif kind == "opposite_section_peer":
            identities = {
                str(value.get("canonical_identity") or "") for value in candidates
                if value.get("canonical_identity")
            }
            sections = {str(value.get("section") or "") for value in candidates}
            for ref, value in bundle.evidence_catalog.items():
                if (
                    value.get("canonical_identity") in identities
                    and value.get("section") not in sections
                ):
                    added.append(ref)
        elif kind == "larger_text_window":
            # Legacy text evidence already freezes the configured window in
            # one FOCUS record.  Other text atoms are not silently borrowed.
            added.extend(
                ref for ref in bundle.evidence_catalog
                if ref.startswith("FOCUS:TEXT:") and ref.endswith(task_id)
            )
        elif kind == "bounded_image_crop":
            added.extend(
                ref for ref, value in bundle.evidence_catalog.items()
                if value.get("kind") == "BOUNDED_IMAGE_CROP"
            )

    unique = [
        ref for ref in dict.fromkeys(added)
        if ref not in existing and ref in bundle.evidence_catalog
    ][: max(0, int(limit))]
    focus["context_refs"] = [*list(focus.get("context_refs") or ()), *unique]
    return unique


__all__ = ["ExpansionBudget", "expand_focus"]
