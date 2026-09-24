"""Provider-neutral Consolidator run: prepare (0 calls) → execute (one call per cluster).

``prepare`` freezes everything the model will see before any call: canonical
card and hint identities, the prefilter plan, every payload, its sha256 and
the exact model-visible text hash.  ``execute`` sends each READY payload once
through an INJECTED provider (``V3Provider.complete``; ``get_provider`` is
never used here), with ``images=[]`` and zero automatic retries, and hands
every raw answer to ``on_record`` before anything validates it.  A provider
error leaves its cluster unmerged; nothing is invented to fill the gap.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .contracts import (
    CONSOLIDATOR_PROMPT, CONSOLIDATOR_PROMPT_SHA256, DECISION_CONTRACT, DECISION_SCHEMA, DECISION_SCHEMA_SHA256,
    ENGINE_NAME, ENGINE_VERSION, INPUT_CONTRACT, INPUT_SCHEMA_SHA256, MAX_CLUSTER_CARDS, MODE_CLUSTER,
    MODE_SINGLETON_REVIEW, RUNTIME_CONTRACT, STAGE, sha256_json,
)
from .identity import CardEntry, HintEntry, HintTable, card_table
from .payload import ClusterInput, PayloadError, build_payload, guard, location_registry
from .prefilter import PARAMS as PREFILTER_PARAMS
from .prefilter import PrefilterPlan, build_plan
from .source_view import SourceBundle

READY = "READY"
NOT_SENT = "NOT_SENT"


class CallPlanExceeded(RuntimeError):
    """More calls are planned than the caller allowed: nothing was sent."""

    def __init__(self, planned: int, allowed: int):
        super().__init__(f"{planned} model calls planned, {allowed} allowed")
        self.planned = planned
        self.allowed = allowed


def _sha_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class PlannedCall:
    call_id: str
    mode: str
    cluster_id: str
    members: list[str]
    hint_refs: list[str]
    payload: dict[str, Any]
    payload_sha256: str
    model_visible_sha256: str
    model_visible_chars: int
    status: str
    reason: str = ""

    def summary(self) -> dict[str, Any]:
        return {"call_id": self.call_id, "mode": self.mode, "cluster_id": self.cluster_id, "members": self.members,
                "hint_refs": self.hint_refs, "status": self.status, "reason": self.reason,
                "payload_sha256": self.payload_sha256, "model_visible_sha256": self.model_visible_sha256,
                "model_visible_chars": self.model_visible_chars}


@dataclass
class PreparedRun:
    bundle: SourceBundle
    cards: dict[str, CardEntry]
    hints: HintTable
    plan: PrefilterPlan
    registry: dict[str, Any]
    calls: list[PlannedCall]
    prompt: str = CONSOLIDATOR_PROMPT
    prompt_sha256: str = CONSOLIDATOR_PROMPT_SHA256
    attached: dict[str, list[HintEntry]] = field(default_factory=dict)

    @property
    def ready_calls(self) -> list[PlannedCall]:
        return [c for c in self.calls if c.status == READY]

    def contracts(self) -> dict[str, Any]:
        return {"engine": ENGINE_NAME, "engine_version": ENGINE_VERSION, "runtime_contract": RUNTIME_CONTRACT,
                "input_contract": INPUT_CONTRACT, "input_schema_sha256": INPUT_SCHEMA_SHA256,
                "decision_contract": DECISION_CONTRACT, "decision_schema_sha256": DECISION_SCHEMA_SHA256,
                "prompt_sha256": self.prompt_sha256, "prefilter": PREFILTER_PARAMS, "stage": STAGE}

    def input_freeze(self) -> dict[str, Any]:
        """Everything the model will see, hashed, before the first call."""
        prefilter = self.plan.to_json([e.ref for e in self.hints.entries])
        body = {
            "source": self.bundle.identity(),
            "contracts": self.contracts(),
            "hint_identity": {k: v for k, v in self.hints.receipt.items() if k != "table"},
            "hint_mapping_sha256": self.hints.receipt["mapping_sha256"],
            "cards": len(self.cards), "hints": len(self.hints.entries),
            "location_registry": self.registry,
            "prefilter": prefilter,
            "calls": [c.summary() for c in self.calls],
        }
        body["input_freeze_sha256"] = sha256_json(body)
        return body

    def call_plan(self, allowed: int) -> dict[str, Any]:
        ready = self.ready_calls
        return {
            "planned_model_calls": len(ready), "allowed": allowed, "within_limit": len(ready) <= allowed,
            "cluster_calls": sum(1 for c in ready if c.mode == MODE_CLUSTER),
            "singleton_review_calls": sum(1 for c in ready if c.mode == MODE_SINGLETON_REVIEW),
            "not_sent": [c.summary() for c in self.calls if c.status != READY],
            "automatic_retries": 0, "max_generations_per_cluster": 1, "images_per_call": 0,
            "calls": [c.summary() for c in ready],
        }


def _pair_rows(plan: PrefilterPlan, members: list[int]) -> list[dict[str, Any]]:
    rows = []
    for x in range(len(members)):
        for y in range(x + 1, len(members)):
            a, b = sorted((members[x], members[y]))
            p = plan.pair_features[(a, b)]
            rows.append({"a": plan.card_ids[a], "b": plan.card_ids[b], "same_region": plan.regions[a] == plan.regions[b],
                         "shared_blocks": p["shared_blocks"], "frag_sim": p["frag_sim"],
                         "shared_transitions": p["shared_transitions"],
                         "shared_designations": list(p["shared_designations"]), "lex_cos": p["lex_cos"]})
    return rows


def prepare(bundle: SourceBundle, *, prompt: str = CONSOLIDATOR_PROMPT) -> PreparedRun:
    """Canonical identities → prefilter → one frozen payload per call (0 model calls)."""
    cards = card_table(bundle.result)
    hints = bundle.hint_table()
    ordered = sorted(cards.values(), key=lambda c: c.ordinal)
    plan = build_plan([c.card for c in ordered], [c.region_id for c in ordered], [e.hint for e in hints.entries])
    attached: dict[str, list[HintEntry]] = {}
    for a in sorted(plan.attachments, key=lambda a: (a.card_index, a.rank_in_card)):
        attached.setdefault(plan.card_ids[a.card_index], []).append(hints.entries[a.hint_index])
    registry = location_registry(bundle.semantic_map)
    groups: list[tuple[str, list[int]]] = [(MODE_CLUSTER, c) for c in plan.call_clusters()]
    review = list(plan.singleton_review)
    for start in range(0, len(review), MAX_CLUSTER_CARDS):
        groups.append((MODE_SINGLETON_REVIEW, review[start:start + MAX_CLUSTER_CARDS]))
    calls: list[PlannedCall] = []
    for mode, members in groups:
        member_ids = [plan.card_ids[i] for i in members]
        union = sorted({h.key: h for cid in member_ids for h in attached.get(cid, [])}.values(), key=lambda h: h.ordinal)
        ci = ClusterInput(mode=mode, pair_id=bundle.pair_id, source_run_id=bundle.source_run_id,
                          cards=[(cid, cards[cid].region_id, cards[cid].card) for cid in member_ids],
                          hints=union, attached={cid: attached.get(cid, []) for cid in member_ids},
                          pair_features=_pair_rows(plan, members) if mode == MODE_CLUSTER else [])
        payload = build_payload(ci, bundle.semantic_map, registry)
        status, reason, visible_sha, visible_chars = READY, "", "", 0
        try:
            text = guard(prompt, payload)
            visible_sha, visible_chars = _sha_text(text), len(text)
        except PayloadError as exc:
            status, reason = NOT_SENT, exc.code
        calls.append(PlannedCall(
            call_id=f"consolidate:{payload['cluster_id']}", mode=mode, cluster_id=payload["cluster_id"],
            members=member_ids, hint_refs=[h.ref for h in union], payload=payload,
            payload_sha256=sha256_json(payload), model_visible_sha256=visible_sha,
            model_visible_chars=visible_chars, status=status, reason=reason))
    return PreparedRun(bundle=bundle, cards=cards, hints=hints, plan=plan, registry=registry, calls=calls,
                       prompt=prompt, prompt_sha256=_sha_text(prompt), attached=attached)


def provider_config(provider: Any) -> dict[str, str]:
    return {"provider": str(getattr(provider, "provider", type(provider).__name__)),
            "model": str(getattr(provider, "model", "")), "reasoning": str(getattr(provider, "reasoning", ""))}


def execute(prepared: PreparedRun, provider: Any, *, max_calls: int,
            on_record: Callable[[dict[str, Any]], None],
            should_stop: Callable[[], bool] = lambda: False) -> list[dict[str, Any]]:
    """One generation per READY call, 0 retries; every record reaches ``on_record`` before validation."""
    from backend.app.services.project_change_v3.provider import ProviderError

    ready = prepared.ready_calls
    if len(ready) > max_calls:
        raise CallPlanExceeded(len(ready), max_calls)
    config = provider_config(provider)
    records: list[dict[str, Any]] = []
    for call in ready:
        record: dict[str, Any] = {
            "call_id": call.call_id, "cluster_id": call.cluster_id, "mode": call.mode, "members": call.members,
            "payload_sha256": call.payload_sha256, "model_visible_sha256": call.model_visible_sha256,
            "provider_config": config, "attempt": 1, "automatic_retries": 0, "started_at": _now(),
        }
        if should_stop():
            record.update(status="CANCELLED_BEFORE_SEND", error_code="cancelled", raw_response=None)
        else:
            t0 = time.monotonic()
            try:
                response = provider.complete(stage=STAGE, call_id=call.call_id, pair_id=prepared.bundle.pair_id,
                                             prompt=prepared.prompt, data=call.payload, schema=DECISION_SCHEMA,
                                             images=[])
                record.update(status="RESPONDED", error_code="", raw_response=response)
            except ProviderError as exc:
                record.update(status="PROVIDER_ERROR", error_code=exc.code, error=exc.message,
                              raw_response=getattr(provider, "last_response", None))
            except Exception as exc:  # a broken provider must not break the source run
                record.update(status="PROVIDER_EXCEPTION", error_code=type(exc).__name__, error=str(exc)[:2000],
                              raw_response=getattr(provider, "last_response", None))
            record["duration_s"] = round(time.monotonic() - t0, 3)
        transport = getattr(provider, "last_transport", None)
        record["transport"] = transport
        record["model_visible_sha256_verified"] = (
            None if not isinstance(transport, dict) or "model_visible_payload_sha256" not in transport
            else transport["model_visible_payload_sha256"] == call.model_visible_sha256)
        record["raw_response_sha256"] = None if record["raw_response"] is None else sha256_json(record["raw_response"])
        record["finished_at"] = _now()
        on_record(record)
        records.append(record)
    return records
