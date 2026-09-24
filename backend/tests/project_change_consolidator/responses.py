"""Scripted Consolidator answers built from a payload (fake provider; no model)."""
from __future__ import annotations

import copy
from typing import Any

ENG = "ENGINEERING_CHANGE"


def group(gid: str, decision: str, members: list[str], *, basis=None, check: str = "", uncertainty: str = "",
          flags=(), channel: str = ENG, related: str = "", relations=None) -> dict[str, Any]:
    if relations is None:
        relations = [{"card_ref": m, "relation": "MANIFESTATION_OF"} for m in members] if decision == "MERGE" else []
    return {"group_id": gid, "decision": decision, "member_refs": list(members), "relations": relations,
            "merge_basis": list(basis if basis is not None else (["SAME_DECISION_DIFFERENT_LOCATION"]
                                                                 if decision == "MERGE" else [])),
            "distinguishing_check": check or ("другие системы или другой переход сделали бы их разными"
                                              if decision == "MERGE" else "самостоятельная карточка"),
            "uncertainty_reason": uncertainty, "flags": list(flags), "channel": channel,
            "related_engineering_card_ref": related}


def _dispositions(payload: dict[str, Any], groups: list[dict[str, Any]], material=()) -> list[dict[str, Any]]:
    out = []
    for g in groups:
        for h in payload["hints"]:
            if set(h["attached_to_cards"]) & set(g["member_refs"]):
                key = (h["hint_key"]["region_id"], h["hint_key"]["hint_id"])
                is_material = key in material
                out.append({"hint_key": dict(h["hint_key"]), "group_id": g["group_id"],
                            "disposition": "MATERIAL_REPRESENTED" if is_material else "NOT_MATERIAL",
                            "reason": "оспаривает утверждение" if is_material else "к событию не относится"})
    return out


def keep_all(payload: dict[str, Any], *, channel: str = ENG, flags=(), material=()) -> dict[str, Any]:
    groups = [group(f"G{i}", "KEEP_SEPARATE", [c["card_ref"]], channel=channel, flags=flags)
              for i, c in enumerate(payload["cards"], 1)]
    return {"cluster_id": payload["cluster_id"], "groups": groups, "recompositions": [],
            "conflict_dispositions": _dispositions(payload, groups, material)}


def recomposition(payload: dict[str, Any], members: list[str], gid: str = "G1") -> dict[str, Any]:
    cards = {c["card_ref"]: c for c in payload["cards"]}
    params: dict[tuple, list[str]] = {}
    for m in members:
        for p in cards[m]["parameters"]:
            params.setdefault((p["old_value"], p["new_value"], p["unit"]), []).append(f"{m}#{p['p']}")
    parameters = [{"param_key": f"P{i}", "name": "Параметр решения", "sources": refs, "status": "CONSISTENT",
                   "applies_to_location_ids": []} for i, refs in enumerate(params.values(), 1)]
    manifestations = [{"location_ids": cards[m]["location_ids"][:1], "member_refs": [m], "local_note": "",
                       "local_param_keys": [p["param_key"] for p in parameters]} for m in members]
    affected = [lid for mf in manifestations for lid in mf["location_ids"]]
    summary = "Одно решение на нескольких листах" + (": {{P1.old}} → {{P1.new}}." if parameters else ".")
    return {"group_id": gid, "engineering_subject": "Сводное решение", "system_designations": [],
            "scope": {"scope_type": "MULTI_BUILDING", "scope_basis": "UNION_OF_MANIFESTATIONS",
                      "affected_location_ids": affected, "basis_evidence_refs": []},
            "change_summary": summary, "old_state": "Было прежнее решение.", "new_state": "Стало новое решение.",
            "why_one_event": "Одно проектное решение повторено на листах разных зданий.",
            "parameters": parameters, "duplicate_parameter_refs": [], "manifestations": manifestations,
            "open_conflicts": [], "claim_status": "SUPPORTED_BY_MEMBERS"}


def merge(payload: dict[str, Any], members: list[str] | None = None, *, material=(), channel: str = ENG,
          flags=()) -> dict[str, Any]:
    members = members or [c["card_ref"] for c in payload["cards"]]
    groups = [group("G1", "MERGE", members, channel=channel, flags=flags)]
    rest = [c["card_ref"] for c in payload["cards"] if c["card_ref"] not in members]
    groups += [group(f"G{i}", "KEEP_SEPARATE", [m]) for i, m in enumerate(rest, 2)]
    rec = recomposition(payload, members)
    disp = _dispositions(payload, groups, material)
    for d in disp:
        if d["group_id"] == "G1" and d["disposition"] == "MATERIAL_REPRESENTED":
            rec["open_conflicts"].append({"source": "HINT", "hint_key": dict(d["hint_key"]), "member_param_refs": [],
                                          "statement": "Подсказка оспаривает решение; сторона не выбрана.",
                                          "affected_param_keys": [], })
    return {"cluster_id": payload["cluster_id"], "groups": groups, "recompositions": [rec],
            "conflict_dispositions": disp}


def edit(resp: dict[str, Any], fn) -> dict[str, Any]:
    out = copy.deepcopy(resp)
    fn(out)
    return out
