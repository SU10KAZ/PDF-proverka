"""Route findings to graphic / text / weak verification paths."""
from __future__ import annotations

from .classifier import _has_image_evidence
from .context_loader import FindingContext

PATH_GRAPHIC = "graphic"
PATH_TEXT = "text"
PATH_MIXED = "mixed"
PATH_WEAK = "weak"


def route_verification_path(ctx: FindingContext) -> str:
    finding = ctx.finding
    has_image = bool(ctx.graphic_block_ids) or _has_image_evidence(finding)
    has_text = bool(ctx.text_block_ids) or bool(ctx.md_excerpt)

    if has_image and has_text:
        return PATH_MIXED
    if has_image:
        return PATH_GRAPHIC
    if has_text:
        return PATH_TEXT
    if ctx.grounding_level == "ungrounded":
        return PATH_WEAK
    return PATH_WEAK
