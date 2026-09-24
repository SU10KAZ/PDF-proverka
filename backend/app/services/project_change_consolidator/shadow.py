"""One shadow Consolidator run: prepare → execute → validate/expand → assemble → X-checks → freeze.

The source (a completed V3 run or a frozen bundle) is only read.  The output
is a new immutable run of the shadow store; the source result, the Dedupe
result, the run and ``current_run.json`` are never written.  Nothing here
promotes a shadow result: readers must ask for a COMPLETED run bound to the
exact source result sha256.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .contracts import (
    DOCUMENTARY, ENGINE_NAME, ENGINE_VERSION, REVIEW, SHADOW_RESULT_SCHEMA, UNCERTAIN, sha256_json,
)
from .engine import CallPlanExceeded, PreparedRun, execute, prepare, provider_config
from .expander import pass_through
from .payload import guard
from .storage import RESULT, ShadowStore
from .validator import CallOutcome, validate_call

FLAG = "PROJECTCHANGE_CONSOLIDATOR_SHADOW"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def snapshot(roots: Iterable[Path]) -> dict[str, str]:
    """sha256 of every file under the watched roots (X6: the source is byte-identical afterwards)."""
    out: dict[str, str] = {}
    for root in roots:
        root = Path(root)
        paths = [root] if root.is_file() else sorted(p for p in root.rglob("*") if p.is_file()) if root.is_dir() else []
        for p in paths:
            out[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def assemble(prepared: PreparedRun, outcomes: list[CallOutcome], consolidator_run_id: str,
             config: dict[str, str]) -> dict[str, Any]:
    cards = prepared.cards
    rows: list[tuple[int, dict[str, Any]]] = []
    called = {cid for o in outcomes for cid in [*[p["projectchange_id"] for p in o.pass_through],
                                                *[m for c in o.consolidated for m in c["lineage"]["member_ids"]]]}
    for o in outcomes:
        for c in o.consolidated:
            rows.append((min(cards[m].ordinal for m in c["lineage"]["member_ids"]), c))
        for p in o.pass_through:
            rows.append((cards[p["projectchange_id"]].ordinal, p))
    for cid, entry in cards.items():
        if cid not in called:
            rows.append((entry.ordinal, pass_through(entry, decision="NOT_CLUSTERED")))
    rows.sort(key=lambda r: r[0])
    items = [r for _, r in rows]
    engineering = [x for x in items if x["channel"] not in (REVIEW, DOCUMENTARY)]
    review = [x for x in items if x["channel"] == REVIEW]
    documentary = [x for x in items if x["channel"] == DOCUMENTARY]
    relations = []
    for o in outcomes:
        for g in o.groups:
            if g["final"] == UNCERTAIN:
                relations.append({"relation": "possible_same_event", "members": g["members"],
                                  "cluster_id": o.cluster_id, "group_id": g["group_id"]})
    decisions: dict[str, int] = {}
    for o in outcomes:
        for g in o.groups:
            decisions[g["final"]] = decisions.get(g["final"], 0) + 1
    bundle = prepared.bundle
    return {
        "schema": SHADOW_RESULT_SCHEMA, "consolidator_run_id": consolidator_run_id,
        "source": {"session_id": bundle.session_id, "pair_id": bundle.pair_id, "source_run_id": bundle.source_run_id,
                   "source_result_sha256": bundle.result_sha256,
                   "hint_mapping_sha256": prepared.hints.receipt["mapping_sha256"]},
        "engine": ENGINE_NAME, "engine_version": ENGINE_VERSION, "model": config,
        "engineering_changes": engineering, "review_items": review, "documentary_changes": documentary,
        "relations": relations,
        "rollbacks": [r for o in outcomes for r in o.rollbacks],
        "unresolved_hints": [{"hint_key": e.key_obj(), "hint_ref": e.ref, "hint": e.hint}
                             for e in prepared.hints.entries],
        "stats": {
            "source_cards": len(cards), "source_hints": len(prepared.hints.entries),
            "output_items": len(items),
            "consolidated_cards": sum(1 for x in items if x["origin"] == "CONSOLIDATED"),
            "cards_absorbed": sum(len(x["lineage"]["member_ids"]) for x in items if x["origin"] == "CONSOLIDATED"),
            "pass_through_cards": sum(1 for x in items if x["origin"] == "PASS_THROUGH"),
            "engineering_changes": len(engineering), "review_items": len(review),
            "documentary_changes": len(documentary), "possible_same_event_relations": len(relations),
            "group_decisions": decisions, "rollbacks": sum(len(o.rollbacks) for o in outcomes),
            "calls": {s: sum(1 for o in outcomes if o.status == s)
                      for s in ("ACCEPTED", "CLUSTER_FALLBACK", "NOT_SENT", "PROVIDER_FAILED")},
        },
    }


def run_checks(prepared: PreparedRun, outcomes: list[CallOutcome], result: dict[str, Any],
               records: list[dict[str, Any]], recheck: dict[str, Any], before: dict[str, str],
               after: dict[str, str]) -> list[dict[str, Any]]:
    checks = []

    def add(code: str, ok: bool, detail: str = "") -> None:
        checks.append({"cluster_id": "", "group_id": "", "code": code, "result": "PASS" if ok else "FAIL",
                       "detail": detail})

    ids = [m for key in ("engineering_changes", "review_items", "documentary_changes") for x in result[key]
           for m in (x["lineage"]["member_ids"] if x["origin"] == "CONSOLIDATED" else [x["projectchange_id"]])]
    add("X1", sorted(ids) == sorted(prepared.cards) and len(ids) == len(set(ids)),
        f"{len(ids)} output members for {len(prepared.cards)} source cards")
    called = [m for c in prepared.calls for m in c.members]
    add("X2", len(called) == len(set(called)), "every card in at most one call")
    bad = []
    for c in prepared.ready_calls:
        try:
            text = guard(prepared.prompt, c.payload)
            if hashlib.sha256(text.encode("utf-8")).hexdigest() != c.model_visible_sha256:
                bad.append(c.cluster_id)
        except Exception:  # noqa: BLE001 — any guard failure of a sent payload is an X3 failure
            bad.append(c.cluster_id)
    add("X3", not bad, f"guard failures {bad}" if bad else "label-leak guard passed for every sent payload")
    configs = {sha256_json(r["provider_config"]) for r in records}
    mixed = [r["call_id"] for r in records if isinstance(r.get("transport"), dict)
             and r["transport"].get("model") not in (None, r["provider_config"]["model"])]
    mixed += [r["call_id"] for r in records if isinstance(r.get("transport"), dict)
              and r["transport"].get("reasoning") not in (None, r["provider_config"]["reasoning"])]
    unverified = [r["call_id"] for r in records if r.get("model_visible_sha256_verified") is False]
    add("X4", len(configs) <= 1 and not mixed and not unverified,
        f"configurations {len(configs)}, transport mismatches {mixed}, payload mismatches {unverified}")
    add("X5", recheck["files_unchanged"] and recheck["hint_mapping_sha256"] == prepared.hints.receipt["mapping_sha256"],
        "source files and hint mapping unchanged")
    changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    add("X6", not changed, f"changed source files: {changed[:10]}" if changed else f"{len(before)} watched files unchanged")
    return checks


def run_shadow(bundle: Any, provider: Any, store_root: Path | str, *, max_calls: int,
               watch_roots: Iterable[Path] = (), flags: dict[str, str] | None = None,
               prepared: PreparedRun | None = None, should_stop: Callable[[], bool] = lambda: False,
               on_event: Callable[[str, dict[str, Any]], None] = lambda kind, data: None) -> dict[str, Any]:
    """Run one shadow consolidation; returns the frozen manifest.  Raises CallPlanExceeded before any write."""
    prepared = prepared or prepare(bundle)
    if len(prepared.ready_calls) > max_calls:
        raise CallPlanExceeded(len(prepared.ready_calls), max_calls)
    watch = list(watch_roots)
    before = snapshot(watch)
    freeze = prepared.input_freeze()
    config = provider_config(provider)
    store = ShadowStore(store_root)
    run = store.create_run(bundle.source_run_id)
    started = _now()
    try:
        source_doc = {"identity": bundle.identity(), "files": bundle.files}
        run.write("SOURCE_BUNDLE.json", source_doc)
        run.write("HINT_IDENTITY.json", prepared.hints.receipt)
        run.write("PREFILTER.json", freeze["prefilter"])
        run.write("INPUT_FREEZE.json", freeze)
        dirs = {}
        for i, c in enumerate(prepared.calls, 1):
            dirs[c.call_id] = f"calls/{i:03d}_{c.cluster_id}"
            run.write(f"{dirs[c.call_id]}/INPUT.json", c.payload)
            run.write(f"{dirs[c.call_id]}/MODEL_VISIBLE.json", {
                **c.summary(), "prompt_sha256": prepared.prompt_sha256, **prepared.contracts(), **config})

        def on_record(record: dict[str, Any]) -> None:
            d = dirs[record["call_id"]]
            run.write(f"{d}/RESPONSE.json", {"call_id": record["call_id"], "status": record["status"],
                                             "raw_response": record["raw_response"],
                                             "raw_response_sha256": record["raw_response_sha256"]})
            run.write(f"{d}/RECEIPT.json", {k: v for k, v in record.items() if k != "raw_response"})
            on_event("call", {"call_id": record["call_id"], "status": record["status"]})

        records = execute(prepared, provider, max_calls=max_calls, on_record=on_record, should_stop=should_stop)
        by_call = {r["call_id"]: r for r in records}
        outcomes = []
        for c in prepared.calls:
            o = validate_call(prepared, c, by_call.get(c.call_id), consolidator_run_id=run.consolidator_run_id)
            run.write(f"{dirs[c.call_id]}/VALIDATION.json", {"status": o.status, "reason": o.reason,
                                                              "groups": o.groups, "checks": o.checks,
                                                              "rollbacks": o.rollbacks})
            outcomes.append(o)
        result = assemble(prepared, outcomes, run.consolidator_run_id, config)
        x = run_checks(prepared, outcomes, result, records, bundle.recheck(), before, snapshot(watch))
        failed = [c["code"] for c in x if c["result"] == "FAIL"]
        report = {"consolidator_run_id": run.consolidator_run_id, "run_checks": x,
                  "calls": [{"call_id": o.call_id, "cluster_id": o.cluster_id, "status": o.status, "reason": o.reason,
                             "checks": o.checks} for o in outcomes]}
        run.write("VALIDATION_REPORT.json", report)
        result_sha = None
        if not failed:
            result_sha = run.write(RESULT, result)
        usage = [r.get("transport", {}).get("usage") for r in records if isinstance(r.get("transport"), dict)]
        return run.finalize({
            "session_id": bundle.session_id, "pair_id": bundle.pair_id,
            "source_result_sha256": bundle.result_sha256, "source_bundle_sha256": sha256_json(source_doc),
            "hint_identity_sha256": prepared.hints.receipt["mapping_sha256"],
            "prefilter_sha256": freeze["prefilter"]["prefilter_sha256"],
            "input_freeze_sha256": freeze["input_freeze_sha256"],
            **prepared.contracts(), **config,
            "flags": {FLAG: (flags or {}).get(FLAG, "")},
            "state": "FAILED" if failed else "COMPLETED", "reason_code": ",".join(failed),
            "created_at": started, "completed_at": _now(),
            "calls_planned": len(prepared.ready_calls),
            "calls_made": sum(1 for r in records if r["status"] not in ("CANCELLED_BEFORE_SEND",)),
            "automatic_retries": 0,
            "usage_total": {k: sum(int((u or {}).get(k) or 0) for u in usage)
                            for k in ("input_tokens", "cached_input_tokens", "output_tokens",
                                      "reasoning_output_tokens")},
            "shadow_result_sha256": result_sha, "stats": result["stats"],
        })
    except BaseException:
        store.release(bundle.source_run_id)
        raise
