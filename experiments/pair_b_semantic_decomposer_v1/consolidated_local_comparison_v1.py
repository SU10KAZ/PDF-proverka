"""Frozen 80-group Pair B local comparison and post-freeze evaluation gate.

The inference half reads only the semantic-consolidator freeze and the source
payloads from which its atomic candidates were produced.  Truth artifacts are
named only in ``evaluate`` and cannot be opened before RESULT_FREEZE exists.
"""
from __future__ import annotations

import asyncio
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from .preflight import OUT as PARENT, read, sha, write
from .recovery import command_for
from .transport import decode
from experiments.project_change_semantic_codex_272.provider import safe_env, runtime_identity
from experiments.project_change_semantic_codex_272.schema import validate

REPO = Path(__file__).resolve().parents[2]
SOURCE = PARENT / "semantic_consolidator_v1"
STAGE = PARENT / "consolidated_local_comparison_v1"
INPUTS = PARENT / "decomposition_inputs"
RASTER_BASE = PARENT.parent / "fresh_dev_sample_f5_pipeline_v6"
READY = PARENT / "candidate_condensation_v2/READY_FOR_COMPARISON.json"
GROUPS = SOURCE / "FINAL_CONSOLIDATED_GROUPS.json"
LINEAGE = SOURCE / "CANDIDATE_LINEAGE.json"
MODEL, REASONING = "gpt-6-astra", "xhigh"
MAX_GROUPS, PARALLELISM = 100, 2
MISSING = "NOT_READY_DUE_MISSING_RASTER"


def now(): return datetime.now(timezone.utc).isoformat()
def commit(): return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
def compact(v): return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


PROMPT_ADDENDUM = """

Это сравнение одной frozen consolidated group. Consolidation — только гипотеза,
а не доказательство. Проверь, подтверждают ли доставленные OLD и NEW evidence
заявленное изменение именно для supplied engineering_subject/scope. Разные
modalities допустимы. Для GRAPHIC используй приложенный растр как primary source,
а description/текст — лишь как metadata. Не расширяй scope.

Вердикт только ACCEPT, REVIEW или NOT_CHANGE. Для КАЖДОГО atomic member верни
atomic_member_results со строго тем же candidate_id и одним из SUPPORTED,
PARTIALLY_SUPPORTED, NOT_SUPPORTED, INSUFFICIENT. System-wide claim принимай
целиком лишь когда одна OLD→NEW логика подтверждена во всех перечисленных
locations; иначе REVIEW и частичное покрытие members. В old_state/new_state
engineering_subject используй точный source subject того evidence, которое
цитируешь; consolidated subject остаётся проверяемой гипотезой верхнего уровня.
Не добавляй evidence IDs. JSON only.
"""


def output_schema():
    base = read(PARENT.parent / "controlled_inference_f1_f4_f2_v3/OUTPUT_SCHEMA.json")
    base = deepcopy(base)
    base["properties"]["atomic_member_results"] = {
        "type": "array", "items": {"type": "object", "additionalProperties": False,
        "required": ["candidate_id", "status", "reason"], "properties": {
            "candidate_id": {"type": "string"},
            "status": {"type": "string", "enum": ["SUPPORTED", "PARTIALLY_SUPPORTED", "NOT_SUPPORTED", "INSUFFICIENT"]},
            "reason": {"type": "string"}}}}
    base["required"].append("atomic_member_results")
    return base


def decoded_bundles(candidates):
    result = {}
    for row in candidates.values():
        key = row["source_bundle"]
        if key not in result:
            result[key] = decode(read(INPUTS / key / "MODEL_INPUT.json"))
    return result


def evidence_for(row, side, refs, bundle):
    registry = bundle["reference_registry"]
    selected = []
    for ref in refs:
        meta = registry[ref]
        matches = [e for e in bundle["source_evidence"][side]
                   if e["evidence_id"] == ref or any(b.get("region_id") == ref for b in e.get("region_bindings", []))]
        # A TABLE region can share its page-level source evidence without a
        # dedicated row. Graphic evidence is never allowed this fallback.
        if not matches and meta["route"] != "GRAPHIC":
            matches = [e for e in bundle["source_evidence"][side] if e["page"] == meta["page"]]
        for item in matches:
            value = deepcopy(item)
            value["requested_reference"] = ref
            value["requested_route"] = meta["route"]
            selected.append(value)
    out = {}
    for item in selected: out.setdefault(item["evidence_id"], item)
    return list(out.values())


def package_group(group, candidates, bundles):
    evidence = {"old": [], "new": []}
    atomic = []
    for cid in group["atomic_candidate_ids"]:
        row = candidates[cid]; bundle = bundles[row["source_bundle"]]
        atomic.append(row)
        evidence["old"] += evidence_for(row, "old", row["old_evidence_refs"], bundle)
        evidence["new"] += evidence_for(row, "new", row["new_evidence_refs"], bundle)
    for side in evidence:
        unique = {}
        for item in evidence[side]: unique.setdefault(item["evidence_id"], item)
        evidence[side] = list(unique.values())
    versions = {side: sorted({e["document_version"] for e in evidence[side]} or
                             {bundles[row["source_bundle"]]["frozen_bundle"][side]["document_version"] for row in atomic})
                for side in evidence}
    if any(len(v) != 1 for v in versions.values()):
        raise ValueError(group["consolidated_change_id"] + ": non-unique side version")
    requirements = []
    for side in ("old", "new"):
        for index, e in enumerate(evidence[side]):
            rid = next((b.get("requirement_id") for b in e.get("region_bindings", []) if b.get("requirement_id")),
                       f"lc_{group['consolidated_change_id']}_{side}_{index:02d}")
            requirements.append({"requirement": {"requirement_id": rid, "side": side.upper(),
                "page": e["page"], "document_version": e["document_version"], "subject": e["subject"],
                "provenance": {"candidate_id": group["consolidated_change_id"], "discovery_subject_ids": []}},
                "evidence_ids": [e["evidence_id"]], "completeness": "COMPLETE",
                "delivery": {"payload_delivered": bool(e.get("quote") or e.get("raster"))}})
    packet = {"pair_key": "caea6d2810c334ec0368de8e", "proposal_query": group["engineering_subject"],
              "source_versions": {s: versions[s][0] for s in versions},
              "source_package_hash": sha(GROUPS), "evidence": evidence,
              "evidence_coverage": {"requirements": requirements}}
    data = {"consolidated_group": group, "atomic_members": atomic,
            "evidence": evidence, "source_pages": {
                s: sorted({e["page"] for e in evidence[s]}) for s in evidence},
            "modalities": group["modalities"],
            "missing_evidence_markers": group["blocking_reasons"],
            "provenance": {"final_groups_sha256": sha(GROUPS), "lineage_sha256": sha(LINEAGE)}}
    images = []
    for side in ("old", "new"):
        for e in evidence[side]:
            if e.get("requested_route") != "GRAPHIC": continue
            raster = e.get("raster")
            if not raster: continue
            path = RASTER_BASE / raster["path"]
            if not path.is_file() or sha(path) != raster["sha256"]: raise ValueError("Raster drift: " + str(path))
            images.append({"path": str(path), "sha256": raster["sha256"], "evidence_id": e["evidence_id"],
                           "side": side, "page": e["page"], "bbox": e["bbox"]})
    dedup = {}; [dedup.setdefault(x["sha256"], x) for x in images]
    return data, packet, list(dedup.values())


def request_bytes(prompt, data, images):
    manifest = [{"image_index": i + 1, "file": f"image_{i:02d}.png", **{k: v for k, v in x.items() if k != "path"}}
                for i, x in enumerate(images)]
    return (prompt + "\n\nATTACHED GRAPHIC RASTERS:\n" + compact(manifest)
            + "\n\nUNTRUSTED GROUP DATA:\n" + compact(data) + "\n").encode()


def prepare(stage=STAGE):
    if stage.exists(): raise FileExistsError("Immutable stage exists: " + str(stage))
    frozen = read(SOURCE / "CONSOLIDATION_RESULT_FREEZE.json")
    if sha(GROUPS) != frozen["final_groups_sha256"] or sha(LINEAGE) != frozen["lineage_sha256"]:
        raise ValueError("Consolidator hash drift")
    groups = read(GROUPS)["groups"]
    if len(groups) != 80 or len(groups) > MAX_GROUPS: raise ValueError("Expected exactly 80 groups")
    rows = read(READY)["candidates"]; candidates = {x["local_candidate_id"]: x for x in rows}
    bundles = decoded_bundles(candidates)
    lineage = read(LINEAGE)["lineage"]
    missing_groups = {x["consolidated_change_id"] for x in lineage if x["member_status"] == MISSING}
    prompt = (REPO / "experiments/project_change_contracts_v3_272/prompt.txt").read_text() + PROMPT_ADDENDUM
    schema = output_schema()
    for name in ("comparison_inputs", "comparison_raw"): (stage / name).mkdir(parents=True, exist_ok=True)
    write(stage / "PROMPT.txt", prompt); write(stage / "OUTPUT_SCHEMA.json", schema)
    plan = []; preflight = []
    for group in groups:
        gid = group["consolidated_change_id"]
        data, packet, images = package_group(group, candidates, bundles)
        missing_side = not data["evidence"]["old"] or not data["evidence"]["new"]
        action = ("NO_CALL_MISSING_REQUIRED_GRAPHIC" if gid in missing_groups else
                  "NO_CALL_INSUFFICIENT_EVIDENCE" if missing_side else "MODEL_CALL")
        directory = stage / "comparison_inputs" / gid; directory.mkdir()
        write(directory / "MODEL_INPUT.json", data); write(directory / "PACKET.json", packet); write(directory / "IMAGES.json", images)
        exact = request_bytes(prompt, data, images); (directory / "EXACT_PROMPT.txt").write_bytes(exact)
        evidence_hashes = {e["evidence_id"]: hashlib.sha256(compact(e).encode()).hexdigest()
                           for side in ("old", "new") for e in packet["evidence"][side]}
        record = {"group_id": gid, "engineering_subject": group["engineering_subject"], "scope": group["scope"],
                  "old_refs": group["old_refs"], "new_refs": group["new_refs"],
                  "old_pages": data["source_pages"]["old"], "new_pages": data["source_pages"]["new"],
                  "modalities": group["modalities"], "lineage": group["atomic_candidate_ids"],
                  "comparison_readiness": group["comparison_readiness"], "blockers": group["blocking_reasons"],
                  "action": action, "evidence_hashes": evidence_hashes, "images": len(images)}
        preflight.append(record)
        plan.append({"group_id": gid, "action": action, "exact_request_sha256": hashlib.sha256(exact).hexdigest(),
                     "input_sha256": sha(directory / "MODEL_INPUT.json"), "packet_sha256": sha(directory / "PACKET.json"),
                     "images_sha256": sha(directory / "IMAGES.json")})
    ready = [p["group_id"] for p in plan if p["action"] == "MODEL_CALL"]
    no_call = [p["group_id"] for p in plan if p["action"] != "MODEL_CALL"]
    if len(ready) > 80: raise RuntimeError("STOP: MODEL_READY_GROUPS > 80")
    write(stage / "LOCAL_COMPARISON_PREFLIGHT.json", {"status": "PASS", "pair": "ИОС4.2", "groups": preflight,
          "total": 80, "model_ready_groups": len(ready), "no_call_groups": len(no_call), "truth_opened": False})
    write(stage / "CALL_PLAN.json", {"maximum_groups": MAX_GROUPS, "automatic_retries": 0, "parallelism": PARALLELISM,
          "model_ready_ids": ready, "no_call_ids": no_call, "groups": plan})
    print(f"MODEL_READY_GROUPS = {len(ready)}; NO_CALL = {len(no_call)}", flush=True)


def freeze(stage=STAGE):
    if (stage / "CONSOLIDATED_COMPARISON_INFERENCE_FREEZE.json").exists(): raise FileExistsError("Already frozen")
    if subprocess.check_output(["git", "status", "--porcelain", "--", str(Path(__file__).relative_to(REPO))], cwd=REPO, text=True):
        raise RuntimeError("Commit orchestrator before freeze")
    plan = read(stage / "CALL_PLAN.json"); runtime = runtime_identity()
    if (runtime["model"], runtime["reasoning"], runtime["provider"]) != (MODEL, REASONING, "codex_chatgpt"):
        raise ValueError("Model config drift")
    inputs = {}
    for p in (stage / "comparison_inputs").rglob("*"):
        if p.is_file(): inputs[str(p)] = sha(p)
    code = {str(p.relative_to(REPO)): sha(p) for folder in (
        "project_change_f2_binding_v4_272", "project_change_post_inference_repair_v4_272",
        "project_change_post_inference_repair_v2_272", "project_change_post_inference_repair_272")
        for p in (REPO / "experiments" / folder).glob("*.py")}
    code[str(Path(__file__).relative_to(REPO))] = sha(__file__)
    value = {"status": "FROZEN_READY", "frozen_at": now(), "pair": "ИОС4.2", "pair_key": "caea6d2810c334ec0368de8e",
             "exact_group_ids": [p["group_id"] for p in plan["groups"]], "model_ready_ids": plan["model_ready_ids"],
             "no_call_ids": plan["no_call_ids"], "input_hashes": inputs, "prompt_hash": sha(stage / "PROMPT.txt"),
             "schema_hash": sha(stage / "OUTPUT_SCHEMA.json"), "model_config": runtime, "code_commit": commit(),
             "code_hashes": code, "final_groups_hash": sha(GROUPS), "lineage_hash": sha(LINEAGE),
             "openrouter_calls": 0, "claude_calls": 0, "source_truth_opened": False, "automatic_retries": 0}
    write(stage / "CONSOLIDATED_COMPARISON_INFERENCE_FREEZE.json", value)
    verify(stage); print(f"FREEZE PASS; MODEL_READY_GROUPS = {len(plan['model_ready_ids'])}", flush=True)


def verify(stage=STAGE):
    f = read(stage / "CONSOLIDATED_COMPARISON_INFERENCE_FREEZE.json")
    if commit() != f["code_commit"]: raise ValueError("Code commit drift")
    if runtime_identity() != f["model_config"]: raise ValueError("Runtime drift")
    if sha(GROUPS) != f["final_groups_hash"] or sha(LINEAGE) != f["lineage_hash"]: raise ValueError("Source group drift")
    for p, digest in {**f["input_hashes"], **{str(REPO/k): v for k, v in f["code_hashes"].items()}}.items():
        if sha(p) != digest: raise ValueError("Frozen file drift: " + p)
    return f


async def call_one(stage, item):
    gid = item["group_id"]; source = stage / "comparison_inputs" / gid; target = stage / "comparison_raw" / gid
    if target.exists(): raise FileExistsError("No retry or overwrite: " + gid)
    target.mkdir(); payload = (source / "EXACT_PROMPT.txt").read_bytes()
    if hashlib.sha256(payload).hexdigest() != item["exact_request_sha256"]: raise ValueError("Request drift")
    shutil.copyfile(source / "EXACT_PROMPT.txt", target / "prompt.txt")
    shutil.copyfile(stage / "OUTPUT_SCHEMA.json", target / "schema.json")
    names = []
    for i, image in enumerate(read(source / "IMAGES.json")):
        name = f"image_{i:02d}.png"; shutil.copyfile(image["path"], target / name)
        if sha(target / name) != image["sha256"]: raise ValueError("Raster drift")
        names.append(name)
    hashes = {p.name: sha(p) for p in target.iterdir() if p.is_file()}; command = command_for(target, names)
    write(target / "INVOCATION.json", {"group_id": gid, "model": MODEL, "reasoning": REASONING,
          "provider": "codex_chatgpt", "fresh_context": True, "openrouter": False, "retries": 0,
          "input_hashes": hashes, "command": command})
    started = time.monotonic(); proc = None; error = None
    try:
        with (target / "raw.jsonl").open("wb") as raw, (target / "stderr.txt").open("wb") as err:
            proc = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE, stdout=raw, stderr=err,
                                                        env=safe_env(), start_new_session=True)
            await asyncio.wait_for(proc.communicate(payload), timeout=3600)
    except BaseException as exc:
        error = exc
        if proc is not None and proc.returncode is None: os.killpg(proc.pid, signal.SIGKILL); await proc.wait()
    records = []
    for line in (target / "raw.jsonl").read_text().splitlines():
        try: records.append(json.loads(line))
        except ValueError: pass
    usage = [r["usage"] for r in records if r.get("type") == "turn.completed" and r.get("usage")]
    tools = [r for r in records if r.get("type", "").startswith("item.") and r.get("item", {}).get("type") not in
             {None, "agent_message", "reasoning", "error"}]
    receipt = {"seconds": time.monotonic() - started, "usage": usage, "tool_items": len(tools),
               "input_integrity": all(sha(target / n) == d for n, d in hashes.items()),
               "exit_code": proc.returncode if proc else None, "model": MODEL, "reasoning": REASONING}
    try:
        if error: raise error
        if proc.returncode or not usage or tools or not receipt["input_integrity"]: raise RuntimeError("Runtime/schema/tool failure")
        value = read(target / "final.txt"); validate(value, read(target / "schema.json"))
        if value["case_token"] != gid: raise ValueError("Group token mismatch")
        expected = set(read(source / "MODEL_INPUT.json")["consolidated_group"]["atomic_candidate_ids"])
        got = [x["candidate_id"] for x in value["atomic_member_results"]]
        if len(got) != len(set(got)) or set(got) != expected: raise ValueError("Atomic member coverage mismatch")
        write(target / "parsed.json", value); receipt.update(status="SUCCESS", output_sha256=sha(target / "parsed.json"))
        write(target / "SUCCESS.json", receipt); return value
    except BaseException as exc:
        receipt.update(status="FAILED_CLOSED", error_type=type(exc).__name__, error=str(exc)); write(target / "FAILURE.json", receipt); raise


async def run(stage=STAGE):
    frozen = verify(stage); plan = read(stage / "CALL_PLAN.json")["groups"]
    if list((stage / "comparison_raw").glob("*/INVOCATION.json")): raise RuntimeError("Run already started; no automatic resume")
    ready = [p for p in plan if p["action"] == "MODEL_CALL"]
    print(f"MODEL_READY_GROUPS = {len(ready)}", flush=True)
    try:
        for start in range(0, len(ready), PARALLELISM):
            verify(stage); batch = ready[start:start + PARALLELISM]
            results = await asyncio.gather(*(call_one(stage, x) for x in batch), return_exceptions=True)
            errors = [x for x in results if isinstance(x, BaseException)]
            if errors: raise errors[0]
            done = len(list((stage / "comparison_raw").glob("*/SUCCESS.json")))
            print(f"COMPARISON COMPLETE {done}/{len(ready)}", flush=True)
        write(stage / "INFERENCE_COMPLETE.json", {"at": now(), "successful_calls": len(ready), "technical_failures": 0,
              "openrouter_calls": 0, "claude_calls": 0, "source_truth_opened": False})
    except BaseException as exc:
        write(stage / "STOP_RECEIPT.json", {"status": "STOPPED_NO_REPAIR", "at": now(), "error_type": type(exc).__name__,
              "error": str(exc), "attempts": len(list((stage / "comparison_raw").glob("*/INVOCATION.json"))),
              "successful": len(list((stage / "comparison_raw").glob("*/SUCCESS.json"))), "source_truth_opened": False})
        raise


def downstream(stage=STAGE):
    frozen = verify(stage); plan = read(stage / "CALL_PLAN.json")["groups"]
    if len(list((stage / "comparison_raw").glob("*/SUCCESS.json"))) != len(frozen["model_ready_ids"]):
        raise RuntimeError("All model-ready calls must succeed before downstream")
    from experiments.project_change_post_inference_repair_272.replay import profile
    from experiments.project_change_post_inference_repair_v4_272.repair import repair
    groups = {x["consolidated_change_id"]: x for x in read(GROUPS)["groups"]}
    local = []; atoms = []; post = []
    for item in plan:
        gid = item["group_id"]; group = groups[gid]
        if item["action"] == "MODEL_CALL":
            raw = read(stage / "comparison_raw" / gid / "parsed.json")
            packet = read(stage / "comparison_inputs" / gid / "PACKET.json")
            normalized = repair(raw, packet, None, ())
            final = normalized.get("effective_verdict", "REVIEW")
            local.append({"group_id": gid, "raw_verdict": raw["verdict"], "reasoning": raw["reasoning_ru"],
                          "old_state": raw["old_state"], "new_state": raw["new_state"], "atomic_member_results": raw["atomic_member_results"]})
            atoms += [{"group_id": gid, **x} for x in raw["atomic_member_results"]]
            post.append({"group_id": gid, "call_status": "SUCCESS", "raw_verdict": raw["verdict"],
                         "final_verdict": final, "normalized": normalized})
        else:
            local.append({"group_id": gid, "raw_verdict": "NO_CALL", "reasoning": item["action"],
                          "old_state": None, "new_state": None, "atomic_member_results": []})
            atoms += [{"group_id": gid, "candidate_id": cid, "status": "INSUFFICIENT", "reason": item["action"]}
                      for cid in group["atomic_candidate_ids"]]
            post.append({"group_id": gid, "call_status": item["action"], "raw_verdict": "NO_CALL",
                         "final_verdict": "NO_CALL", "normalized": None})
    write(stage / "ATOMIC_MEMBER_RESULTS.json", {"count": len(atoms), "results": atoms})
    write(stage / "LOCAL_COMPARISON_RESULTS.json", {"count": len(local), "results": local})
    write(stage / "POST_INFERENCE_RESULTS.json", {"count": len(post), "results": post})
    files = {str(p.relative_to(stage)): sha(p) for folder in (stage / "comparison_inputs", stage / "comparison_raw")
             for p in folder.rglob("*") if p.is_file()}
    for name in ("ATOMIC_MEMBER_RESULTS.json", "LOCAL_COMPARISON_RESULTS.json", "POST_INFERENCE_RESULTS.json"):
        files[name] = sha(stage / name)
    counts = Counter(x["final_verdict"] for x in post)
    write(stage / "CONSOLIDATED_COMPARISON_RESULT_FREEZE.json", {"status": "FROZEN", "frozen_at": now(),
          "inference_freeze_sha256": sha(stage / "CONSOLIDATED_COMPARISON_INFERENCE_FREEZE.json"),
          "code_commit": commit(), "raw_responses": len(frozen["model_ready_ids"]), "files": files,
          "final_verdict_counts": dict(counts), "model": MODEL, "reasoning": REASONING,
          "source_truth_opened": False, "openrouter_calls": 0, "claude_calls": 0,
          "production": "UNCHANGED", "validation": "NOT OPENED", "final_holdout": "NOT OPENED"})
    print("RESULT FREEZE CREATED; source truth may now be opened", flush=True)


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "prepare": prepare()
    elif command == "freeze": freeze()
    elif command == "verify": verify(); print("FREEZE VERIFIED")
    elif command == "run": asyncio.run(run())
    elif command == "downstream": downstream()
    else: raise SystemExit("usage: prepare|freeze|verify|run|downstream")


if __name__ == "__main__": main()
