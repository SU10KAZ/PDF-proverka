"""BlockLink validation ? V1.2.4 REASSIGN by previous_link_id only."""
from __future__ import annotations

from typing import Any


class BlockLinkValidationError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def base_proposed_links(region: dict[str, Any]) -> list[dict[str, Any]]:
    olds = [b["id"] for b in region.get("old_blocks") or []]
    news = [b["id"] for b in region.get("new_blocks") or []]
    rid = region["id"]
    if len(olds) == 1 and len(news) == 1:
        return [{
            "link_id": f"ai:{rid}:{olds[0]}:{news[0]}",
            "old_block_id": olds[0],
            "new_block_id": news[0],
        }]
    if len(olds) == 1:
        return [{
            "link_id": f"ai:{rid}:{olds[0]}:{n}",
            "old_block_id": olds[0],
            "new_block_id": n,
        } for n in news]
    if len(news) == 1:
        return [{
            "link_id": f"ai:{rid}:{o}:{news[0]}",
            "old_block_id": o,
            "new_block_id": news[0],
        } for o in olds]
    return []


def effective_links(region: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replay BlockLink events; REASSIGN matches previous_link_id only."""
    links = [dict(x) for x in base_proposed_links(region)]
    for e in events:
        if e.get("region_id") != region["id"]:
            continue
        et = e.get("event_type")
        if et == "ADD_BLOCK_LINK":
            links.append({
                "link_id": e["link_id"],
                "old_block_id": e["old_block_id"],
                "new_block_id": e["new_block_id"],
            })
        elif et == "DELETE_BLOCK_LINK":
            links = [
                l for l in links
                if l["link_id"] != e["link_id"]
                and not (
                    l["old_block_id"] == e["old_block_id"]
                    and l["new_block_id"] == e["new_block_id"]
                )
            ]
        elif et == "REASSIGN_BLOCK_LINK":
            prev = e.get("previous_link_id")
            links = [
                {
                    "link_id": e["link_id"],
                    "old_block_id": e["old_block_id"],
                    "new_block_id": e["new_block_id"],
                }
                if l["link_id"] == prev
                else l
                for l in links
            ]
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for l in links:
        key = (l["old_block_id"], l["new_block_id"])
        if key in seen:
            continue
        seen.add(key)
        out.append(l)
    return out


def allowed_block_ids(region: dict[str, Any], side: str) -> set[str]:
    """BlockLink endpoints a human may use on one side of a region.

    V1.2.4: the region's semantic membership.  A side whose V3 membership is
    explicitly EMPTY (``membership_state``) has no members to link, so its
    page context is offered instead — never silently turned into membership.
    """
    key = "old_blocks" if side == "OLD" else "new_blocks"
    members = {b["id"] for b in region.get(key) or []}
    if members:
        return members
    if (region.get("membership_state") or {}).get(side) == "EMPTY":
        return {
            b["id"]
            for page in (region.get("pages") or {}).get(side) or []
            for b in page.get("blocks") or []
        }
    return set()


def validate_block_link_event(
    *,
    event_type: str,
    region: dict[str, Any],
    events: list[dict[str, Any]],
    link_id: str,
    old_block_id: str,
    new_block_id: str,
    previous_link_id: str | None = None,
) -> dict[str, Any]:
    """Validate and resolve authoritative previous endpoints (V1.2.4).

    Returns dict with keys used for persistence:
    previous_old_block_id, previous_new_block_id, previous_link_id
    or raises BlockLinkValidationError with code.
    Special: returns {"noop": True} for REASSIGN NO_CHANGE.
    """
    if event_type not in {"ADD_BLOCK_LINK", "DELETE_BLOCK_LINK", "REASSIGN_BLOCK_LINK"}:
        raise BlockLinkValidationError("BAD_BLOCK_LINK_EVENT")

    old_ids = allowed_block_ids(region, "OLD")
    new_ids = allowed_block_ids(region, "NEW")
    region_events = [e for e in events if e.get("region_id") == region["id"]]
    effective = effective_links(region, region_events)
    effective_pairs = {(l["old_block_id"], l["new_block_id"]) for l in effective}

    prev_old_persist = None
    prev_new_persist = None
    prev_link_persist = previous_link_id

    if event_type in {"ADD_BLOCK_LINK", "REASSIGN_BLOCK_LINK"}:
        if old_block_id in new_ids and old_block_id not in old_ids:
            raise BlockLinkValidationError("WRONG_BLOCK_SIDE")
        if new_block_id in old_ids and new_block_id not in new_ids:
            raise BlockLinkValidationError("WRONG_BLOCK_SIDE")
        if old_block_id not in old_ids:
            raise BlockLinkValidationError("OLD_BLOCK_NOT_IN_REGION")
        if new_block_id not in new_ids:
            raise BlockLinkValidationError("NEW_BLOCK_NOT_IN_REGION")

    if event_type == "REASSIGN_BLOCK_LINK":
        # Exact link_id lookup only ? no fallback to client previous OLD/NEW.
        source_link = next(
            (l for l in effective if previous_link_id and l["link_id"] == previous_link_id),
            None,
        )
        if source_link is None:
            raise BlockLinkValidationError("BLOCK_LINK_NOT_FOUND")
        prev_o = source_link["old_block_id"]
        prev_n = source_link["new_block_id"]
        if prev_o == old_block_id and prev_n == new_block_id:
            return {"noop": True, "error": "NO_CHANGE", "ok": True}
        if (old_block_id, new_block_id) in effective_pairs:
            raise BlockLinkValidationError("BLOCK_LINK_ALREADY_EXISTS")
        prev_old_persist = prev_o
        prev_new_persist = prev_n
        prev_link_persist = source_link["link_id"]

    if event_type == "ADD_BLOCK_LINK":
        if (old_block_id, new_block_id) in effective_pairs:
            raise BlockLinkValidationError("BLOCK_LINK_ALREADY_EXISTS")

    if event_type == "DELETE_BLOCK_LINK":
        exists = any(
            (l["link_id"] == link_id)
            or (l["old_block_id"] == old_block_id and l["new_block_id"] == new_block_id)
            for l in effective
        )
        if not exists:
            raise BlockLinkValidationError("BLOCK_LINK_NOT_FOUND")

    return {
        "previous_old_block_id": prev_old_persist,
        "previous_new_block_id": prev_new_persist,
        "previous_link_id": prev_link_persist,
    }
