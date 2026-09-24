"""Model payload of ONE Consolidator call (``projectchange_consolidator_runtime_input/1``).

Only the cards of the cluster, the hints attached to them (possibly from other
Mapper regions), the involved regions of the semantic map and the pair
features go out.  Locations are referenced by ``location_id`` from a per-call
catalog built from the members' own location texts and the involved Mapper
regions, so the model can never introduce a place.  Every payload is checked
against the input schema, the label-leak guard and the character budget
BEFORE anything is sent; a failed check means the cluster is not sent at all.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .contracts import (
    FRAGMENT_CHARS, HINT_TEXT_CHARS, INPUT_CONTRACT, INPUT_SCHEMA, LABEL_LEAK_RE, MODE_CLUSTER,
    PAYLOAD_BUDGET_CHARS, ROLE_CHARS, sha256_json,
)
from .features import location_units
from .identity import HintEntry

# Location-unit keywords by level: a building-level keyword wins over a part of
# a building, so "all units covered" means all buildings when buildings exist.
UNIT_PRECEDENCE = ("корпус", "здани", "строени", "блок", "секци")
MIN_UNITS = 2


class PayloadError(ValueError):
    def __init__(self, code: str, message: str = ""):
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _clean(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def cluster_id(member_ids: list[str], mode: str) -> str:
    prefix = "CL" if mode == MODE_CLUSTER else "SR"
    return f"{prefix}-{sha256_json(sorted(member_ids))[:12]}"


def location_registry(semantic_map: dict[str, Any]) -> dict[str, Any]:
    """Syntactic registry of location units of the WHOLE Mapper map (keyword + number).

    The unit kind is the highest-level keyword with at least two distinct units;
    with fewer than two units anywhere the registry is unavailable (fail-closed:
    ALL_LOCATION_UNITS_COVERED can then never be claimed).
    """
    found: dict[str, set[str]] = {kw: set() for kw in UNIT_PRECEDENCE}
    for region in semantic_map.get("regions") or []:
        for text in region.get("locations") or []:
            for unit in location_units(str(text)):
                kw = unit.split(" ", 1)[0]
                if kw in found:
                    found[kw].add(unit)
    for kw in UNIT_PRECEDENCE:
        if len(found[kw]) >= MIN_UNITS:
            return {"available": True, "kind": kw, "units": sorted(found[kw], key=_unit_order),
                    "unavailable_reason": ""}
    return {"available": False, "kind": "", "units": [],
            "unavailable_reason": f"fewer than {MIN_UNITS} location units in the Mapper map"}


def _unit_order(label: str) -> tuple:
    kw, num = label.split(" ", 1)
    return (kw, [float(x) for x in num.split(".")] if re.fullmatch(r"\d+(?:\.\d+)*", num) else [], num)


class _Catalog:
    def __init__(self) -> None:
        self.texts: list[str] = []
        self.origins: dict[str, list[dict[str, str]]] = {}

    def add(self, text: Any, kind: str, ref: str) -> str | None:
        t = _clean(text)
        if not t:
            return None
        if t not in self.origins:
            self.texts.append(t)
            self.origins[t] = []
        origin = {"kind": kind, "ref": ref}
        if origin not in self.origins[t]:
            self.origins[t].append(origin)
        return t

    def ids(self) -> dict[str, str]:
        return {t: f"L{i:02d}" for i, t in enumerate(self.texts, 1)}


@dataclass
class ClusterInput:
    """Everything one call needs, already resolved to canonical identities."""
    mode: str
    pair_id: str
    source_run_id: str
    cards: list[tuple[str, str, dict[str, Any]]]          # (card_id, region_id, card)
    hints: list[HintEntry]                                  # union attached to the members
    attached: dict[str, list[HintEntry]]                    # card_id → its hints (best first)
    pair_features: list[dict[str, Any]]


def build_payload(ci: ClusterInput, semantic_map: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    member_ids = [cid for cid, _, _ in ci.cards]
    catalog = _Catalog()
    for cid, _, card in ci.cards:
        for loc in card.get("locations") or []:
            catalog.add(loc, "CARD_LOCATION", cid)
        for i, p in enumerate(card.get("changed_parameters") or [], 1):
            catalog.add(p.get("location"), "PARAMETER_LOCATION", f"{cid}#p{i}")
    involved = list(dict.fromkeys(rid for _, rid, _ in ci.cards))
    map_regions = {str(r.get("region_id")): r for r in semantic_map.get("regions") or []}
    regions = []
    for rid in involved:
        region = map_regions.get(rid) or {}
        for loc in region.get("locations") or []:
            catalog.add(loc, "MAPPER_REGION", rid)
        regions.append({"region_id": rid, "engineering_domain": _clean(region.get("engineering_domain")),
                        "locations": [_clean(x) for x in region.get("locations") or [] if _clean(x)]})
    loc_id = catalog.ids()
    units = []
    if registry["available"]:
        for i, label in enumerate(registry["units"], 1):
            units.append({"unit_id": f"U{i:02d}", "label": label,
                          "location_ids": [loc_id[t] for t in catalog.texts if label in location_units(t)]})
    cards = []
    for cid, rid, card in ci.cards:
        cards.append({
            "card_ref": cid, "region_id": rid,
            "dedupe_lineage": [str(x) for x in card.get("dedupe_lineage") or [cid]],
            "subject": _clean(card.get("engineering_subject")), "region_scope_claim": _clean(card.get("scope")),
            "locations": [_clean(x) for x in card.get("locations") or [] if _clean(x)],
            "location_ids": [loc_id[_clean(x)] for x in card.get("locations") or [] if _clean(x)],
            "summary": _clean(card.get("change_summary")), "old_state": _clean(card.get("old_state")),
            "new_state": _clean(card.get("new_state")), "why_one_event": _clean(card.get("why_one_event")),
            "parameters": [{"p": f"p{i}", "name": _clean(p.get("name")), "old_value": _clean(p.get("old_value")),
                            "new_value": _clean(p.get("new_value")), "unit": _clean(p.get("unit")),
                            "location": _clean(p.get("location")),
                            "location_ids": [loc_id[_clean(p.get("location"))]] if _clean(p.get("location")) else []}
                           for i, p in enumerate(card.get("changed_parameters") or [], 1)],
            "evidence": [{"e": f"e{i}", "side": e["side"], "page": int(e["physical_page"]), "block": str(e["block_id"]),
                          "type": e["block_type"], "fragment": _clean(e.get("relevant_fragment"))[:FRAGMENT_CHARS],
                          "role": _clean(e.get("evidence_role"))[:ROLE_CHARS]}
                         for i, e in enumerate(card.get("evidence_items") or [], 1)],
            "attached_hint_keys": [h.key_obj() for h in ci.attached.get(cid, [])],
        })
    hints = []
    for h in ci.hints:
        raw = h.hint
        hints.append({
            "hint_key": h.key_obj(), "hint_ref": h.ref, "kind": raw.get("kind"),
            "subject": _clean(raw.get("engineering_subject")),
            "suspected": _clean(raw.get("suspected_change"))[:HINT_TEXT_CHARS],
            "conflict_or_missing": _clean(raw.get("missing_proof_or_conflict"))[:HINT_TEXT_CHARS],
            "evidence": [{"h": f"h{i}", "side": e["side"], "page": int(e["physical_page"]), "block": str(e["block_id"]),
                          "fragment": _clean(e.get("relevant_fragment"))[:FRAGMENT_CHARS]}
                         for i, e in enumerate(raw.get("evidence_items") or [], 1)],
            "attached_to_cards": [cid for cid in member_ids if any(x.key == h.key for x in ci.attached.get(cid, []))],
        })
    return {
        "contract": INPUT_CONTRACT, "mode": ci.mode, "pair_id": ci.pair_id, "source_run_id": ci.source_run_id,
        "cluster_id": cluster_id(member_ids, ci.mode),
        "location_catalog": [{"location_id": loc_id[t], "text": t, "origins": catalog.origins[t]} for t in catalog.texts],
        "location_units": {"available": bool(registry["available"]), "units": units,
                           "unavailable_reason": registry.get("unavailable_reason") or ""},
        "regions": regions, "cards": cards, "hints": hints, "pair_features": ci.pair_features,
    }


def model_visible_text(prompt: str, payload: dict[str, Any]) -> str:
    """Exactly what the V3 transport shows the model (``build_codex_payload`` with no images)."""
    return prompt + "\nIMAGES:\n" + json.dumps([], ensure_ascii=False) + "\nSOURCE DATA:\n" + json.dumps(
        payload, ensure_ascii=False)


def guard(prompt: str, payload: dict[str, Any]) -> str:
    """Schema, label-leak and budget checks; returns the model-visible text or raises PayloadError."""
    import jsonschema

    try:
        jsonschema.Draft202012Validator(INPUT_SCHEMA).validate(payload)
    except jsonschema.ValidationError as exc:
        raise PayloadError("INPUT_SCHEMA_INVALID", exc.message) from exc
    text = model_visible_text(prompt, payload)
    leak = LABEL_LEAK_RE.search(text)
    if leak:
        raise PayloadError("LABEL_LEAK_GUARD", f"evaluation vocabulary {leak.group(0)!r} in the payload")
    if len(text) > PAYLOAD_BUDGET_CHARS:
        raise PayloadError("PAYLOAD_OVER_BUDGET", f"{len(text)} > {PAYLOAD_BUDGET_CHARS} characters")
    return text
