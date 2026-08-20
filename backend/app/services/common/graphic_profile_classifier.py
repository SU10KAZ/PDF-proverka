"""Small shared classifier used by block-grounding profile routing."""
from __future__ import annotations


ELECTRICAL_SINGLELINE = "electrical_singleline"
TABLE_OR_SCHEDULE = "table_or_schedule"
TITLE_STAMP_NOTES = "title_stamp_notes"
ARCHITECTURAL_PLAN_OR_FACADE = "architectural_plan_or_facade"
GENERAL = "general"


def classify_graphic_profile(block_type: str) -> tuple[str, str | None]:
    """Map a normalized graphic block type to a routing profile."""
    if block_type == "dense_grsh_singleline":
        return ELECTRICAL_SINGLELINE, "grsh"
    if block_type == "table_legend":
        return TABLE_OR_SCHEDULE, None
    if block_type == "stamp":
        return TITLE_STAMP_NOTES, None
    if block_type == "plan":
        return ARCHITECTURAL_PLAN_OR_FACADE, None
    return GENERAL, None


__all__ = ["classify_graphic_profile"]
