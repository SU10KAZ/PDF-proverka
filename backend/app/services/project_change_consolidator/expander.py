"""Deterministic expansion of a validated decision into the shadow result.

The model only names references (``<card>#p<n>``, ``<card>#e<n>``,
``{region_id, hint_id}``, ``L<nn>``) and writes prose with ``{{Pn.old|new|unit}}``
placeholders.  Everything factual in a consolidated card — parameter values,
evidence items, locations, conflict provenance — is copied here from the
source result and the canonical hint table; nothing is taken from the model.
Pass-through cards are copied unchanged.
"""
from __future__ import annotations

import copy
import hashlib
from typing import Any

from .contracts import CONSOLIDATED_SCHEMA_VERSION, PARAM_REF_RE, PLACEHOLDER_RE
from .identity import HintEntry

NOT_ASSESSED = "NOT_ASSESSED"


def consolidated_id(member_ids: list[str]) -> str:
    return "PCC-" + hashlib.sha256("|".join(sorted(member_ids)).encode("utf-8")).hexdigest()[:12]


def param_of(cards: dict[str, Any], ref: str) -> dict[str, Any]:
    m = PARAM_REF_RE.fullmatch(ref)
    return cards[m.group(1)].card["changed_parameters"][int(m.group(2)) - 1]


def substitute(text: str, values: dict[str, dict[str, str]]) -> str:
    return PLACEHOLDER_RE.sub(lambda m: str(values[m.group(1)][m.group(2)]), text or "")


def hint_provenance(entry: HintEntry) -> dict[str, Any]:
    h = entry.hint
    return {"region_id": entry.region_id, "hint_id": entry.hint_id, "hint_ref": entry.ref, "kind": h.get("kind"),
            "engineering_subject": h.get("engineering_subject"), "suspected_change": h.get("suspected_change"),
            "missing_proof_or_conflict": h.get("missing_proof_or_conflict"),
            "old_pages": list(h.get("old_pages") or []), "new_pages": list(h.get("new_pages") or []),
            "evidence_items": [{"ref": f"{entry.ref}#h{i}", **copy.deepcopy(e)}
                               for i, e in enumerate(h.get("evidence_items") or [], 1)]}


def expand_merge(*, cards: dict[str, Any], hints: dict[tuple[str, str], HintEntry], catalog: dict[str, dict[str, Any]],
                 group: dict[str, Any], rec: dict[str, Any], dispositions: list[dict[str, Any]],
                 lineage: dict[str, Any]) -> dict[str, Any]:
    members = sorted(group["member_refs"], key=lambda cid: cards[cid].ordinal)
    values: dict[str, dict[str, str]] = {}
    params = []
    duplicates = {d["duplicate_of"]: [] for d in rec["duplicate_parameter_refs"]}
    for d in rec["duplicate_parameter_refs"]:
        duplicates[d["duplicate_of"]].append(d["ref"])
    for p in rec["parameters"]:
        first = param_of(cards, p["sources"][0])
        values[p["param_key"]] = {"old": first.get("old_value") or "", "new": first.get("new_value") or "",
                                  "unit": first.get("unit") or ""}
        refs = list(p["sources"]) + [r for s in p["sources"] for r in duplicates.get(s, [])]
        params.append({
            "param_key": p["param_key"], "name": p["name"], "status": p["status"],
            "old_value": values[p["param_key"]]["old"] if p["status"] == "CONSISTENT" else "",
            "new_value": values[p["param_key"]]["new"] if p["status"] == "CONSISTENT" else "",
            "unit": values[p["param_key"]]["unit"],
            "applies_to_locations": [catalog[x] for x in p["applies_to_location_ids"]],
            "source_values": [{"ref": r, "card_id": PARAM_REF_RE.fullmatch(r).group(1),
                               **{k: param_of(cards, r).get(k) for k in ("name", "old_value", "new_value", "unit",
                                                                         "location")},
                               "duplicate": r not in p["sources"]} for r in refs],
        })
    evidence = []
    for cid in members:
        for i, e in enumerate(cards[cid].card.get("evidence_items") or [], 1):
            evidence.append({"ref": f"{cid}#e{i}", "card_id": cid, **copy.deepcopy(e)})
    by_ref = {e["ref"]: e for e in evidence}
    groups: dict[str, list[str]] = {}
    for e in evidence:
        groups.setdefault(e["block_type"], []).append(e["ref"])
    reasons = {(d["hint_key"]["region_id"], d["hint_key"]["hint_id"]): d["reason"] for d in dispositions}
    conflicts = []
    for c in rec["open_conflicts"]:
        key = (c["hint_key"]["region_id"], c["hint_key"]["hint_id"])
        conflicts.append({
            "source": c["source"], "statement": substitute(c["statement"], values),
            "affected_param_keys": list(c["affected_param_keys"]), "member_param_refs": list(c["member_param_refs"]),
            "hint": hint_provenance(hints[key]) if c["source"] == "HINT" else None,
            "disposition_reason": reasons.get(key, "") if c["source"] == "HINT" else "",
        })
    scope = rec["scope"]
    return {
        "schema": CONSOLIDATED_SCHEMA_VERSION, "origin": "CONSOLIDATED",
        "consolidated_id": consolidated_id(members), "channel": group["channel"], "flags": list(group["flags"]),
        "related_engineering_card_ref": group["related_engineering_card_ref"],
        "engineering_subject": rec["engineering_subject"], "system_designations": list(rec["system_designations"]),
        "scope": {"scope_type": scope["scope_type"], "scope_basis": scope["scope_basis"],
                  "affected_locations": [catalog[x] for x in scope["affected_location_ids"]],
                  "basis_evidence": [by_ref[r] for r in scope["basis_evidence_refs"]]},
        "change_summary": substitute(rec["change_summary"], values),
        "old_state": substitute(rec["old_state"], values), "new_state": substitute(rec["new_state"], values),
        "why_one_event": substitute(rec["why_one_event"], values),
        "changed_parameters": params,
        "manifestations": [{"locations": [catalog[x] for x in m["location_ids"]],
                            "member_ids": sorted(m["member_refs"], key=lambda cid: cards[cid].ordinal),
                            "local_note": substitute(m["local_note"], values), "param_keys": list(m["local_param_keys"])}
                           for m in rec["manifestations"]],
        "evidence_items": evidence,
        "evidence_groups": [{"role": role, "refs": refs} for role, refs in sorted(groups.items())],
        "open_conflicts": conflicts, "claim_status": rec["claim_status"],
        "old_pages": sorted({p for cid in members for p in cards[cid].card.get("old_pages") or []}),
        "new_pages": sorted({p for cid in members for p in cards[cid].card.get("new_pages") or []}),
        "modalities": sorted({m for cid in members for m in cards[cid].card.get("modalities") or []}),
        "lineage": {"member_ids": members,
                    "member_regions": {cid: cards[cid].region_id for cid in members},
                    "member_dedupe_lineage": {cid: list(cards[cid].card.get("dedupe_lineage") or [cid])
                                              for cid in members},
                    "relations": [dict(r) for r in group["relations"]], "merge_basis": list(group["merge_basis"]),
                    "distinguishing_check": group["distinguishing_check"], **lineage},
    }


def pass_through(entry: Any, *, decision: str, channel: str = NOT_ASSESSED, flags: list[str] | None = None,
                 related: str = "", annotations: list[dict[str, Any]] | None = None,
                 possible_same_event: list[str] | None = None, cluster_id: str = "", group_id: str = "",
                 fallback: dict[str, str] | None = None) -> dict[str, Any]:
    card = entry.card
    return {"origin": "PASS_THROUGH", "projectchange_id": entry.card_id, "region_id": entry.region_id,
            "card": copy.deepcopy(card), "decision": decision, "channel": channel, "flags": list(flags or []),
            "related_engineering_card_ref": related, "scope_type": NOT_ASSESSED,
            "region_scope_claim": card.get("scope"), "conflict_annotations": list(annotations or []),
            "possible_same_event": list(possible_same_event or []), "cluster_id": cluster_id, "group_id": group_id,
            "fallback": fallback}
