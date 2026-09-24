#!/usr/bin/env python3
"""Shadow Consolidator over a FROZEN source bundle (research runner; never touches production).

    # 0 model calls: freeze inputs and the call plan
    python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR --plan-only --max-calls 12
    # 0 model calls: full run with a scripted fake provider (keep | broken)
    python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR --provider fake:keep --max-calls 12
    # live: only with an explicit provider, --allow-live and the frozen input sha of the plan
    python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR \
        --provider claude_code_cli:claude-opus-5:xhigh --allow-live --expect-freeze <sha> --max-calls 12

Exit codes: 0 done, 2 refused, 3 call plan above --max-calls (CONSOLIDATOR_CALL_PLAN_REQUIRES_APPROVAL).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise SystemExit(f"refusing to overwrite {path}")
    path.write_text(json.dumps(value, ensure_ascii=False, indent=1), encoding="utf-8")


def fake_provider(kind: str):
    from backend.app.services.project_change_consolidator.contracts import STAGE
    from backend.app.services.project_change_v3.provider import FakeProvider

    def keep(call_id, pair_id, data, schema, images):
        groups = [{"group_id": f"G{i}", "decision": "KEEP_SEPARATE", "member_refs": [c["card_ref"]], "relations": [],
                   "merge_basis": [], "distinguishing_check": "fake", "uncertainty_reason": "", "flags": [],
                   "channel": "ENGINEERING_CHANGE", "related_engineering_card_ref": ""}
                  for i, c in enumerate(data["cards"], 1)]
        disp = [{"hint_key": h["hint_key"], "group_id": g["group_id"], "disposition": "NOT_MATERIAL", "reason": "fake"}
                for g in groups for h in data["hints"] if g["member_refs"][0] in h["attached_to_cards"]]
        return {"cluster_id": data["cluster_id"], "groups": groups, "recompositions": [],
                "conflict_dispositions": disp}

    def broken(call_id, pair_id, data, schema, images):
        return {"cluster_id": data["cluster_id"], "groups": []}

    handlers = {"keep": keep, "broken": broken}
    if kind not in handlers:
        raise SystemExit(f"unknown fake provider {kind!r}")
    return FakeProvider(handlers={STAGE: handlers[kind]})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True, help="frozen bundle spec (explicit files + sha256)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-calls", type=int, required=True)
    ap.add_argument("--plan-only", action="store_true")
    ap.add_argument("--provider", default="")
    ap.add_argument("--allow-live", action="store_true")
    ap.add_argument("--expect-freeze", default="")
    args = ap.parse_args(argv)

    from backend.app.services.project_change_consolidator import engine as E
    from backend.app.services.project_change_consolidator.shadow import run_shadow
    from backend.app.services.project_change_consolidator.source_view import load_frozen_bundle

    spec = json.loads(Path(args.bundle).read_text(encoding="utf-8"))
    bundle = load_frozen_bundle(spec)
    prepared = E.prepare(bundle)
    freeze = prepared.input_freeze()
    plan = prepared.call_plan(args.max_calls)
    out = Path(args.out)
    if args.plan_only:
        _write(out / "INPUT_FREEZE.json", freeze)
        _write(out / "CALL_PLAN.json", {**plan, "input_freeze_sha256": freeze["input_freeze_sha256"]})
        print(json.dumps({"planned_model_calls": plan["planned_model_calls"], "within_limit": plan["within_limit"],
                          "input_freeze_sha256": freeze["input_freeze_sha256"]}, ensure_ascii=False))
        return 0 if plan["within_limit"] else 3
    if not plan["within_limit"]:
        print("CONSOLIDATOR_CALL_PLAN_REQUIRES_APPROVAL", plan["planned_model_calls"], ">", args.max_calls)
        return 3
    if args.provider.startswith("fake:"):
        provider = fake_provider(args.provider.split(":", 1)[1])
    else:
        if not args.allow_live:
            print("refused: a live provider needs --allow-live")
            return 2
        if not args.expect_freeze or args.expect_freeze != freeze["input_freeze_sha256"]:
            print("refused: --expect-freeze must equal the frozen input sha", freeze["input_freeze_sha256"])
            return 2
        from backend.app.services.project_change_consolidator.hook import provider_from_spec

        provider = provider_from_spec(args.provider)
    watch = [Path(f["path"]) for f in bundle.files]
    manifest = run_shadow(bundle, provider, out / "shadow_store", max_calls=args.max_calls, prepared=prepared,
                          watch_roots=watch, on_event=lambda kind, data: print(kind, json.dumps(data), flush=True))
    print(json.dumps({k: manifest.get(k) for k in ("consolidator_run_id", "state", "reason_code", "calls_planned",
                                                   "calls_made", "shadow_result_sha256", "usage_total")},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
