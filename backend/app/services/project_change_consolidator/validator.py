"""Deterministic validator of one Consolidator answer (``VALIDATOR_CONTRACT``: S, R, C, K, P, M).

The validator makes no semantic judgement.  It checks structure, exact
partition, that every reference exists and belongs to the right card or
cluster, that every number, designation and place in new prose can be traced
to the inputs, and that the fields implied by the model's own decisions are
filled.  Any failure closes the decision: a cluster-level failure (S) sends
every card of the cluster through unchanged, a group-level failure sends the
members of that group through unchanged.  Nothing is retried and nothing is
repaired.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .contracts import (
    COMPOSITE_CARD, DECISION_SCHEMA, DOCUMENTARY, ENGINEERING, EVIDENCE_REF_RE, KEEP_SEPARATE, LOCATION_ID_RE,
    MERGE, MODE_SINGLETON_REVIEW, PARAM_REF_RE, PLACEHOLDER_RE, REVIEW, UNCERTAIN, sha256_json,
)
from .expander import expand_merge, hint_provenance, pass_through
from .features import HOMO, NUM_RE, designations, norm

PROSE_LIMITS = {"engineering_subject": 200, "change_summary": 700, "old_state": 1000, "new_state": 1000,
                "why_one_event": 600}
LOCAL_NOTE_LIMIT, STATEMENT_LIMIT = 240, 500
BLOCK_ID_RE = re.compile(r"blk_[0-9a-fA-F]{6,}")


class GroupInvalid(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass
class CallOutcome:
    call_id: str
    cluster_id: str
    mode: str
    status: str                       # ACCEPTED | CLUSTER_FALLBACK | NOT_SENT | PROVIDER_FAILED
    reason: str
    checks: list[dict[str, Any]] = field(default_factory=list)
    groups: list[dict[str, Any]] = field(default_factory=list)
    consolidated: list[dict[str, Any]] = field(default_factory=list)
    pass_through: list[dict[str, Any]] = field(default_factory=list)
    rollbacks: list[dict[str, Any]] = field(default_factory=list)


def nvalue(text: Any) -> str:
    t = str(text or "").replace("−", "-").replace("–", "-").replace("—", "-").translate(HOMO).lower()
    t = re.sub(r"(\d),(\d)", r"\1.\2", t.replace("ё", "е"))
    return re.sub(r"\s+", " ", t).strip()


def _number_values(text: str) -> set[float]:
    out = set()
    for m in NUM_RE.finditer(text or ""):
        try:
            out.add(float(m.group(0).replace(",", ".")))
        except ValueError:
            pass
    return out


def _strip_placeholders(text: str) -> str:
    return PLACEHOLDER_RE.sub(" ", text or "")


class _Ctx:
    """What one group may be traced to (exactly what the model saw for these members)."""

    def __init__(self, payload: dict[str, Any], members: list[str]):
        self.cards = {c["card_ref"]: c for c in payload["cards"]}
        self.members = set(members)
        self.catalog = {x["location_id"]: x for x in payload["location_catalog"]}
        self.hints = {(h["hint_key"]["region_id"], h["hint_key"]["hint_id"]): h for h in payload["hints"]}
        self.attached = {k for k, h in self.hints.items() if self.members & set(h["attached_to_cards"])}
        texts: list[str] = []
        member_texts: list[str] = []
        for cid in members:
            c = self.cards[cid]
            parts = [c["subject"], c["region_scope_claim"], c["summary"], c["old_state"], c["new_state"],
                     c["why_one_event"], *c["locations"]]
            for p in c["parameters"]:
                parts += [p["name"], p["old_value"], p["new_value"], p["unit"], p["location"]]
            for e in c["evidence"]:
                parts += [e["fragment"], e["role"]]
            member_texts += parts
        texts += member_texts
        for key in self.attached:
            h = self.hints[key]
            texts += [h["subject"], h["suspected"], h["conflict_or_missing"], *[e["fragment"] for e in h["evidence"]]]
        texts += [x["text"] for x in payload["location_catalog"]]
        for r in payload["regions"]:
            texts += [r["engineering_domain"], *r["locations"]]
        self.numbers = set().union(*(_number_values(t) for t in texts)) if texts else set()
        self.designations = set().union(*(designations(t) for t in texts)) if texts else set()
        self.member_designations = set().union(*(designations(t) for t in member_texts)) if member_texts else set()
        self.member_norm = " ".join(norm(t) for t in member_texts)

    def param(self, ref: str) -> dict[str, Any]:
        m = PARAM_REF_RE.fullmatch(ref or "")
        if not m or m.group(1) not in self.members:
            raise GroupInvalid("R2", f"parameter ref {ref!r} is not a parameter of a member")
        params = self.cards[m.group(1)]["parameters"]
        n = int(m.group(2))
        if not 1 <= n <= len(params):
            raise GroupInvalid("R2", f"parameter ref {ref!r} does not exist")
        return params[n - 1]

    def evidence(self, ref: str) -> dict[str, Any]:
        m = EVIDENCE_REF_RE.fullmatch(ref or "")
        if not m or m.group(1) not in self.members:
            raise GroupInvalid("R3", f"evidence ref {ref!r} is not evidence of a member")
        ev = self.cards[m.group(1)]["evidence"]
        n = int(m.group(2))
        if not 1 <= n <= len(ev):
            raise GroupInvalid("R3", f"evidence ref {ref!r} does not exist")
        return ev[n - 1]

    def locations(self, ids: list[str], code: str = "R7") -> None:
        for x in ids:
            if not LOCATION_ID_RE.fullmatch(x or "") or x not in self.catalog:
                raise GroupInvalid(code, f"location {x!r} is not in the location catalog of the cluster")


# ----------------------------------------------------------------------------- cluster level (S)
def _structure(payload: dict[str, Any], resp: dict[str, Any], mode: str) -> None:
    import jsonschema

    try:
        jsonschema.Draft202012Validator(DECISION_SCHEMA).validate(resp)
    except jsonschema.ValidationError as exc:
        raise GroupInvalid("S1", exc.message[:300]) from exc
    if resp["cluster_id"] != payload["cluster_id"]:
        raise GroupInvalid("S2", f"cluster_id {resp['cluster_id']!r} != {payload['cluster_id']!r}")
    cards = [c["card_ref"] for c in payload["cards"]]
    seen: list[str] = [m for g in resp["groups"] for m in g["member_refs"]]
    if sorted(seen) != sorted(cards) or len(set(seen)) != len(seen):
        dup = sorted({m for m in seen if seen.count(m) > 1})
        raise GroupInvalid("S3", f"not an exact partition: duplicates {dup}, foreign "
                                 f"{sorted(set(seen) - set(cards))}, missing {sorted(set(cards) - set(seen))}")
    ids = [g["group_id"] for g in resp["groups"]]
    if len(set(ids)) != len(ids):
        raise GroupInvalid("S4", "group_id is not unique")
    rec_ids = [r["group_id"] for r in resp["recompositions"]]
    merges = [g["group_id"] for g in resp["groups"] if g["decision"] == MERGE]
    if sorted(rec_ids) != sorted(merges):
        raise GroupInvalid("S4", f"recompositions {sorted(rec_ids)} do not match MERGE groups {sorted(merges)}")
    if any(d["group_id"] not in ids for d in resp["conflict_dispositions"]):
        raise GroupInvalid("S4", "a conflict disposition names an unknown group")
    for g in resp["groups"]:
        n = len(g["member_refs"])
        if (g["decision"] == MERGE and n < 2) or (g["decision"] == KEEP_SEPARATE and n != 1) or (
                g["decision"] == UNCERTAIN and n < 2):
            raise GroupInvalid("S4", f"{g['group_id']}: {g['decision']} with {n} member(s)")
        if mode == MODE_SINGLETON_REVIEW and g["decision"] != KEEP_SEPARATE:
            raise GroupInvalid("S5", f"{g['group_id']}: {g['decision']} in SINGLETON_REVIEW")
        if g["decision"] == MERGE:
            rel = [r["card_ref"] for r in g["relations"]]
            if sorted(rel) != sorted(g["member_refs"]):
                raise GroupInvalid("S6", f"{g['group_id']}: relations do not cover exactly the members")
            if sum(r["relation"] == "ANCHOR" for r in g["relations"]) > 1:
                raise GroupInvalid("S6", f"{g['group_id']}: more than one ANCHOR")


# ----------------------------------------------------------------------------- group level
def _group_common(g: dict[str, Any], resp: dict[str, Any], ctx: _Ctx, cluster_cards: set[str]) -> None:
    flags = set(g["flags"])
    related = g["related_engineering_card_ref"] or ""
    if related and related not in cluster_cards:
        raise GroupInvalid("R1", f"related_engineering_card_ref {related!r} is not a card of the cluster")
    if COMPOSITE_CARD in flags and not (g["decision"] == KEEP_SEPARATE and len(g["member_refs"]) == 1
                                        and g["channel"] == REVIEW):
        raise GroupInvalid("M1", "COMPOSITE_CARD must be a single KEEP_SEPARATE card routed to REVIEW")
    if related:
        if g["channel"] != DOCUMENTARY:
            raise GroupInvalid("M2", "related_engineering_card_ref is only for DOCUMENTARY_CHANGE")
        if related in g["member_refs"]:
            raise GroupInvalid("M2", "a documentary group cannot relate to its own member")
        other = next(x for x in resp["groups"] if related in x["member_refs"])
        if other["channel"] != ENGINEERING:
            raise GroupInvalid("M2", "the related card does not remain ENGINEERING_CHANGE")
    if g["decision"] == UNCERTAIN and not g["uncertainty_reason"].strip():
        raise GroupInvalid("M3", "UNCERTAIN without uncertainty_reason")
    if g["decision"] == MERGE and (not g["merge_basis"] or not g["distinguishing_check"].strip()):
        raise GroupInvalid("M5", "MERGE without merge_basis or distinguishing_check")
    mine = [d for d in resp["conflict_dispositions"] if d["group_id"] == g["group_id"]]
    keys = [(d["hint_key"]["region_id"], d["hint_key"]["hint_id"]) for d in mine]
    for k in keys:
        if k not in ctx.attached:
            raise GroupInvalid("R4", f"disposition for {k[0]}/{k[1]}: hint not attached to a member of the group")
    if sorted(keys) != sorted(ctx.attached) or len(set(keys)) != len(keys):
        raise GroupInvalid("K1", f"dispositions {sorted(keys)} != attached hints {sorted(ctx.attached)}")
    for d in mine:
        if d["disposition"] == "NOT_MATERIAL" and not d["reason"].strip():
            raise GroupInvalid("K5", "NOT_MATERIAL without a reason")


def _trace(text: str, ctx: _Ctx, where: str, ids: dict[str, set[str]]) -> None:
    raw = _strip_placeholders(text)
    if BLOCK_ID_RE.search(raw):
        raise GroupInvalid("R6", f"{where}: block identifier in prose")
    for kind, values in ids.items():
        for token in values:
            if token and re.search(rf"(?<![0-9A-Za-zА-Яа-яЁё]){re.escape(token)}(?![0-9A-Za-zА-Яа-яЁё])", raw):
                raise GroupInvalid("R6", f"{where}: {kind} identifier {token!r} in prose")
    extra = _number_values(raw.replace("−", "-")) - ctx.numbers
    if extra:
        raise GroupInvalid("C4", f"{where}: numbers {sorted(extra)} are not in the members, hints or places")
    new_desig = designations(raw) - ctx.designations
    if new_desig:
        raise GroupInvalid("C4", f"{where}: designations {sorted(new_desig)} are not in the members or hints")


def _recomposition(g: dict[str, Any], rec: dict[str, Any], ctx: _Ctx, payload: dict[str, Any],
                   resp: dict[str, Any], ids: dict[str, set[str]]) -> None:
    members = g["member_refs"]
    # C7 — lengths and non-empty prose
    for key, limit in PROSE_LIMITS.items():
        if not rec[key].strip() or len(rec[key]) > limit:
            raise GroupInvalid("C7", f"{key} empty or longer than {limit}")
    # parameters: R2 / C1 / C2 / C4b
    keys = [p["param_key"] for p in rec["parameters"]]
    if len(set(keys)) != len(keys) or any(not re.fullmatch(r"P[0-9]+", k) for k in keys):
        raise GroupInvalid("C1", "param_key must be unique P<n>")
    covered: list[str] = []
    status = {}
    for p in rec["parameters"]:
        vals = []
        for ref in p["sources"]:
            q = ctx.param(ref)
            covered.append(ref)
            vals.append((nvalue(q["old_value"]), nvalue(q["new_value"]), nvalue(q["unit"]), nvalue(q["location"]),
                         PARAM_REF_RE.fullmatch(ref).group(1)))
        ctx.locations(p["applies_to_location_ids"])
        same = len({v[:3] for v in vals}) == 1
        if p["status"] == "CONSISTENT" and not same:
            raise GroupInvalid("C2", f"{p['param_key']}: CONSISTENT with different values")
        if p["status"] != "CONSISTENT" and same:
            raise GroupInvalid("C2", f"{p['param_key']}: {p['status']} with identical values")
        if p["status"] == "LOCATION_VARIANT":
            places = {(v[3] or f"@{ctx.cards[v[4]]['region_id']}") for v in vals}
            if len(places) < 2:
                raise GroupInvalid("C2", f"{p['param_key']}: LOCATION_VARIANT without different places")
        if p["status"] == "CONFLICT" and not any(
                c["source"] == "MEMBER_DISAGREEMENT" and set(c["member_param_refs"]) & set(p["sources"])
                for c in rec["open_conflicts"]):
            raise GroupInvalid("C2", f"{p['param_key']}: CONFLICT without a MEMBER_DISAGREEMENT open conflict")
        status[p["param_key"]] = p["status"]
    sources = set(covered)
    for d in rec["duplicate_parameter_refs"]:
        target = ctx.param(d["duplicate_of"])
        dup = ctx.param(d["ref"])
        if d["duplicate_of"] not in sources:
            raise GroupInvalid("C1", f"duplicate_of {d['duplicate_of']} is not a parameter source")
        if tuple(nvalue(dup[k]) for k in ("old_value", "new_value", "unit")) != tuple(
                nvalue(target[k]) for k in ("old_value", "new_value", "unit")):
            raise GroupInvalid("C1", f"{d['ref']} is not a duplicate of {d['duplicate_of']}")
        covered.append(d["ref"])
    expected = [f"{cid}#p{i}" for cid in members for i in range(1, len(ctx.cards[cid]["parameters"]) + 1)]
    if sorted(covered) != sorted(expected):
        missing = sorted(set(expected) - set(covered))
        twice = sorted({r for r in covered if covered.count(r) > 1})
        raise GroupInvalid("C1", f"parameter coverage: missing {missing}, more than once {twice}")
    # placeholders (C4b) — every prose field
    prose = {k: rec[k] for k in PROSE_LIMITS}
    for i, m in enumerate(rec["manifestations"]):
        prose[f"manifestations[{i}].local_note"] = m["local_note"]
    for i, c in enumerate(rec["open_conflicts"]):
        prose[f"open_conflicts[{i}].statement"] = c["statement"]
    for i, p in enumerate(rec["parameters"]):
        prose[f"parameters[{i}].name"] = p["name"]
    for where, text in prose.items():
        for m in PLACEHOLDER_RE.finditer(text):
            if m.group(1) not in status:
                raise GroupInvalid("C4b", f"{where}: placeholder of unknown parameter {m.group(1)}")
            if status[m.group(1)] != "CONSISTENT":
                raise GroupInvalid("C4b", f"{where}: placeholder of {status[m.group(1)]} parameter {m.group(1)}")
        if "{{" in PLACEHOLDER_RE.sub("", text) or "}}" in PLACEHOLDER_RE.sub("", text):
            raise GroupInvalid("C4b", f"{where}: malformed placeholder")
        _trace(text, ctx, where, ids)
    # C5 — system designations
    for s in rec["system_designations"]:
        found = designations(s)
        if found and not found <= ctx.member_designations:
            raise GroupInvalid("C5", f"system designation {s!r} is not in the member texts")
        if not found and norm(s) not in ctx.member_norm:
            raise GroupInvalid("C5", f"system designation {s!r} is not in the member texts")
    # C6 / R7 — manifestations
    seen: set[str] = set()
    for m in rec["manifestations"]:
        ctx.locations(m["location_ids"])
        if not set(m["member_refs"]) <= set(members):
            raise GroupInvalid("R1", "a manifestation names a card outside the group")
        if not set(m["local_param_keys"]) <= set(status):
            raise GroupInvalid("C6", "a manifestation names an unknown parameter")
        if len(m["local_note"]) > LOCAL_NOTE_LIMIT:
            raise GroupInvalid("C7", "local_note too long")
        seen |= set(m["member_refs"])
    if seen != set(members):
        raise GroupInvalid("C6", f"members without a manifestation: {sorted(set(members) - seen)}")
    # P — scope
    scope = rec["scope"]
    ctx.locations(scope["affected_location_ids"])
    basis_types = [ctx.evidence(r)["type"] for r in scope["basis_evidence_refs"]]
    st, sb = scope["scope_type"], scope["scope_basis"]
    if st != "DESIGN_BASIS" and not scope["affected_location_ids"]:
        raise GroupInvalid("P1", f"{st} without affected locations")
    if st == "SYSTEM_WIDE":
        if sb == "EXPLICIT_GENERAL_STATEMENT":
            if not any(t in ("TEXT", "TABLE") for t in basis_types):
                raise GroupInvalid("P2", "SYSTEM_WIDE general statement without a TEXT/TABLE member evidence")
        elif sb == "ALL_LOCATION_UNITS_COVERED":
            units = payload["location_units"]
            if not units["available"] or not units["units"]:
                raise GroupInvalid("P2", "location unit registry unavailable")
            affected = set(scope["affected_location_ids"])
            uncovered = [u["label"] for u in units["units"] if not affected & set(u["location_ids"])]
            if uncovered:
                raise GroupInvalid("P2", f"units not covered: {uncovered}")
        else:
            raise GroupInvalid("P2", f"SYSTEM_WIDE with basis {sb}")
    if sb == "SINGLE_MANIFESTATION" and len(rec["manifestations"]) != 1:
        raise GroupInvalid("P3", "SINGLE_MANIFESTATION with several manifestations")
    if sb == "UNION_OF_MANIFESTATIONS" and len(rec["manifestations"]) < 2:
        raise GroupInvalid("P3", "UNION_OF_MANIFESTATIONS with fewer than two manifestations")
    if (st == "DESIGN_BASIS") != (sb == "CALCULATION_BASIS"):
        raise GroupInvalid("P4", "DESIGN_BASIS and CALCULATION_BASIS go together")
    if st == "DESIGN_BASIS" and "TABLE" not in basis_types:
        raise GroupInvalid("P4", "DESIGN_BASIS without a TABLE member evidence")
    # K2 / K3 / R4 — conflicts
    dispositions = {(d["hint_key"]["region_id"], d["hint_key"]["hint_id"]): d["disposition"]
                    for d in resp["conflict_dispositions"] if d["group_id"] == g["group_id"]}
    represented = set()
    for c in rec["open_conflicts"]:
        key = (c["hint_key"]["region_id"], c["hint_key"]["hint_id"])
        if len(c["statement"].strip()) == 0 or len(c["statement"]) > STATEMENT_LIMIT:
            raise GroupInvalid("C7", "open conflict statement empty or too long")
        if not set(c["affected_param_keys"]) <= set(status):
            raise GroupInvalid("R2", "open conflict names an unknown parameter")
        for ref in c["member_param_refs"]:
            ctx.param(ref)
        if c["source"] == "HINT":
            if key not in ctx.attached:
                raise GroupInvalid("R4", f"open conflict hint {key[0]}/{key[1]} is not attached to the group")
            if dispositions.get(key) != "MATERIAL_REPRESENTED":
                raise GroupInvalid("K2", f"open conflict {key[0]}/{key[1]} is not MATERIAL_REPRESENTED")
            represented.add(key)
        else:
            if key != ("", ""):
                raise GroupInvalid("R4", "a non-HINT open conflict carries a hint_key")
            if c["source"] == "MEMBER_DISAGREEMENT" and len(c["member_param_refs"]) < 2:
                raise GroupInvalid("C2", "MEMBER_DISAGREEMENT needs the disagreeing parameter refs")
        affects = bool(c["affected_param_keys"]) or c["source"] == "MEMBER_DISAGREEMENT"
        if affects and rec["claim_status"] != "CONTAINS_UNRESOLVED_CONFLICT":
            raise GroupInvalid("K3", "a conflict on a parameter requires CONTAINS_UNRESOLVED_CONFLICT")
    missing = {k for k, v in dispositions.items() if v == "MATERIAL_REPRESENTED"} - represented
    if missing:
        raise GroupInvalid("K2", f"material hints not in open_conflicts: {sorted(missing)}")


def _r8(bundle: Any, cards: dict[str, Any], members: list[str]) -> None:
    for cid in members:
        for i, e in enumerate(cards[cid].card.get("evidence_items") or [], 1):
            try:
                block = bundle.block(e["side"], int(e["physical_page"]), str(e["block_id"]))
            except Exception as exc:  # hash mismatch of the source package
                raise GroupInvalid("R8", f"{cid}#e{i}: {exc}") from exc
            if block is None:
                raise GroupInvalid("R8", f"{cid}#e{i}: block not found in the source package")
            if block.get("modality") != e.get("block_type"):
                raise GroupInvalid("R8", f"{cid}#e{i}: block type differs from the source package")
            bb = [round(float(x), 9) for x in block.get("bbox") or []]
            if bb != [round(float(x), 9) for x in e.get("bbox") or []]:
                raise GroupInvalid("R8", f"{cid}#e{i}: bbox differs from the source package")


# ----------------------------------------------------------------------------- one call
def validate_call(prepared: Any, call: Any, record: dict[str, Any] | None, *, consolidator_run_id: str
                  ) -> CallOutcome:
    cards = prepared.cards
    out = CallOutcome(call.call_id, call.cluster_id, call.mode, "ACCEPTED", "")

    def add(group_id: str, code: str, ok: bool, detail: str = "") -> None:
        out.checks.append({"cluster_id": call.cluster_id, "group_id": group_id, "code": code,
                           "result": "PASS" if ok else "FAIL", "detail": detail})

    def all_through(status: str, reason: str) -> CallOutcome:
        out.status, out.reason = status, reason
        for cid in call.members:
            out.pass_through.append(pass_through(cards[cid], decision=status, cluster_id=call.cluster_id,
                                                 fallback={"code": reason, "level": "CLUSTER"}))
        out.rollbacks.append({"cluster_id": call.cluster_id, "group_id": "", "level": "CLUSTER", "code": reason,
                              "members": list(call.members)})
        return out

    if call.status != "READY":
        return all_through("NOT_SENT", call.reason)
    if record is None or record.get("status") != "RESPONDED":
        return all_through("PROVIDER_FAILED", f"PROVIDER:{(record or {}).get('error_code') or 'no_record'}")
    resp = record["raw_response"]
    payload = call.payload
    try:
        _structure(payload, resp if isinstance(resp, dict) else {}, call.mode)
        add("", "S", True)
    except GroupInvalid as exc:
        add("", exc.code, False, exc.detail)
        return all_through("CLUSTER_FALLBACK", f"CLUSTER_INVALID:{exc.code}")

    cluster_cards = {c["card_ref"] for c in payload["cards"]}
    recs = {r["group_id"]: r for r in resp["recompositions"]}
    ids = {"card": set(prepared.cards), "region": {c.region_id for c in prepared.cards.values()},
           "hint": {e.hint_id for e in prepared.hints.entries} | {e.ref for e in prepared.hints.entries}}
    catalog = {x["location_id"]: x for x in payload["location_catalog"]}
    response_sha = record.get("raw_response_sha256") or sha256_json(resp)
    for g in resp["groups"]:
        members = sorted(g["member_refs"], key=lambda cid: cards[cid].ordinal)
        ctx = _Ctx(payload, members)
        decision = g["decision"]
        mine = [d for d in resp["conflict_dispositions"] if d["group_id"] == g["group_id"]]
        try:
            _group_common(g, resp, ctx, cluster_cards)
            if decision == MERGE and "NEEDS_SOURCE" in g["flags"]:
                add(g["group_id"], "M4", True, "NEEDS_SOURCE → UNCERTAIN (no second call in V1)")
                decision = UNCERTAIN
            card = None
            if decision == MERGE:
                _recomposition(g, recs[g["group_id"]], ctx, payload, resp, ids)
                _r8(prepared.bundle, cards, members)
                card = expand_merge(cards=cards, hints=prepared.hints.by_key, catalog=catalog, group=g,
                                    rec=recs[g["group_id"]], dispositions=mine,
                                    lineage={"consolidator_run_id": consolidator_run_id,
                                             "cluster_id": call.cluster_id, "group_id": g["group_id"],
                                             "input_cluster_sha256": call.payload_sha256,
                                             "response_sha256": response_sha})
                union = [e["ref"] for e in card["evidence_items"]]
                expected = [f"{cid}#e{i}" for cid in members
                            for i in range(1, len(cards[cid].card.get("evidence_items") or []) + 1)]
                if union != expected:
                    raise GroupInvalid("C3", "consolidated evidence is not the union of the members")
            add(g["group_id"], "GROUP", True)
        except GroupInvalid as exc:
            add(g["group_id"], exc.code, False, exc.detail)
            out.rollbacks.append({"cluster_id": call.cluster_id, "group_id": g["group_id"], "level": "GROUP",
                                  "code": exc.code, "detail": exc.detail, "members": members,
                                  "model_decision": g["decision"]})
            out.groups.append({"group_id": g["group_id"], "model_decision": g["decision"], "final": "FALLBACK",
                               "members": members, "code": exc.code})
            for cid in members:
                out.pass_through.append(pass_through(cards[cid], decision="FALLBACK", cluster_id=call.cluster_id,
                                                     group_id=g["group_id"],
                                                     fallback={"code": f"GROUP_INVALID:{exc.code}", "level": "GROUP"}))
            continue
        out.groups.append({"group_id": g["group_id"], "model_decision": g["decision"], "final": decision,
                           "members": members, "channel": g["channel"], "flags": list(g["flags"])})
        if card is not None:
            out.consolidated.append(card)
            continue
        for cid in members:
            notes = []
            for d in mine:
                key = (d["hint_key"]["region_id"], d["hint_key"]["hint_id"])
                if d["disposition"] == "MATERIAL_REPRESENTED" and cid in ctx.hints[key]["attached_to_cards"]:
                    notes.append({"hint": hint_provenance(prepared.hints.by_key[key]),
                                  "text": prepared.hints.by_key[key].hint.get("missing_proof_or_conflict") or "",
                                  "disposition_reason": d["reason"]})
            out.pass_through.append(pass_through(
                cards[cid], decision=decision, channel=g["channel"], flags=g["flags"],
                related=g["related_engineering_card_ref"] or "", annotations=notes,
                possible_same_event=[m for m in members if m != cid] if decision == UNCERTAIN else [],
                cluster_id=call.cluster_id, group_id=g["group_id"]))
    return out
