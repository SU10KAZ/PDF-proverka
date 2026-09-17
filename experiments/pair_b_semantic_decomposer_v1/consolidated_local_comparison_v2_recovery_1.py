"""Additive transport recovery for frozen consolidated local comparison V2.

No V1 comparison artifacts or semantic builders are read.  Exact request bytes,
schema, packets, alias adapters and images come only from the V2 inference
freeze.  The 50 original successes and two quota failures remain immutable.
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

from .preflight import read, sha, write
from .recovery import command_for
from experiments.project_change_semantic_codex_272.provider import safe_env, runtime_identity
from experiments.project_change_semantic_codex_272.schema import validate

REPO = Path(__file__).resolve().parents[2]
ROOT = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272")
STAGE = ROOT / "pair_b_semantic_decomposer_v1/consolidated_local_comparison_v2"
RECOVERY = STAGE / "recovery_1"
FREEZE = STAGE / "CONSOLIDATED_COMPARISON_V2_INFERENCE_FREEZE.json"
MODEL, REASONING, PARALLELISM = "gpt-6-astra", "xhigh", 2


def now(): return datetime.now(timezone.utc).isoformat()
def commit(): return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def validate_and_expand(raw, entries):
    aliases = [x["atomic_alias"] for x in entries]; ids = [x["atomic_candidate_id"] for x in entries]
    if len(aliases) != len(set(aliases)) or len(ids) != len(set(ids)): raise ValueError("Frozen alias map not one-to-one")
    mapping = dict(zip(aliases, ids, strict=True)); returned = [x["atomic_alias"] for x in raw["atomic_results"]]
    unknown = [x for x in returned if x not in mapping]
    duplicates = [x for x, n in Counter(returned).items() if n > 1]
    missing = [x for x in aliases if x not in returned]
    if unknown: raise ValueError("UNKNOWN_ALIASES:" + ",".join(unknown))
    if duplicates: raise ValueError("DUPLICATE_ALIASES:" + ",".join(duplicates))
    if missing: raise ValueError("MISSING_ALIASES:" + ",".join(missing))
    if len(returned) != len(mapping): raise ValueError("ALIAS_CARDINALITY_MISMATCH")
    expanded = deepcopy(raw)
    expanded["atomic_member_results"] = [{"candidate_id": mapping[x["atomic_alias"]], "status": x["verdict"],
        "reason": x["reason"], "atomic_alias": x["atomic_alias"]} for x in raw["atomic_results"]]
    return expanded


def verify_frozen_inputs():
    frozen = read(FREEZE)
    if frozen["model_config"] != runtime_identity(): raise ValueError("Frozen model config drift")
    if (frozen["model_config"]["model"], frozen["model_config"]["reasoning"], frozen["model_config"]["provider"]) != (MODEL, REASONING, "codex_chatgpt"):
        raise ValueError("Wrong frozen model")
    if sha(STAGE / "ATOMIC_ALIAS_MAP.json") != frozen["alias_map_sha256"]: raise ValueError("Alias map drift")
    if sha(STAGE / "PROMPT.txt") != frozen["prompt_hash"] or sha(STAGE / "OUTPUT_SCHEMA.json") != frozen["schema_hash"]:
        raise ValueError("Prompt/schema drift")
    for path, digest in frozen["input_hashes"].items():
        if sha(path) != digest: raise ValueError("Frozen input drift: " + path)
    for relative, digest in frozen["code_hashes"].items():
        if sha(REPO / relative) != digest: raise ValueError("Frozen V2 code drift: " + relative)
    plan = read(STAGE / "CALL_PLAN.json")
    if plan["model_ready_ids"] != frozen["model_ready_ids"] or plan["no_call_ids"] != frozen["no_call_ids"]:
        raise ValueError("Group membership drift")
    if (len(frozen["exact_group_ids"]), len(frozen["model_ready_ids"]), len(frozen["no_call_ids"])) != (80, 74, 6):
        raise ValueError("Frozen denominator drift")
    return frozen, plan


def original_state(frozen):
    successes = []; failures = []
    for gid in frozen["model_ready_ids"]:
        directory = STAGE / "comparison_raw" / gid
        success, failure = directory / "SUCCESS.json", directory / "FAILURE.json"
        if success.exists():
            receipt = read(success)
            if sha(directory / "parsed.json") != receipt["raw_output_sha256"] or sha(directory / "parsed_expanded.json") != receipt["expanded_output_sha256"]:
                raise ValueError("Original success drift: " + gid)
            successes.append(gid)
        elif failure.exists():
            receipt = read(failure)
            if receipt.get("input_integrity") is not True or receipt.get("usage"):
                raise ValueError("Historical failure is not zero-usage transport failure: " + gid)
            records = (directory / "raw.jsonl").read_text()
            if "usage limit" not in records: raise ValueError("Historical failure premise drift: " + gid)
            failures.append(gid)
    if successes != frozen["model_ready_ids"][:50]: raise ValueError("Expected exact first 50 original successes")
    if failures != ["PB_053", "PB_054"]: raise ValueError("Expected exact historical failures PB_053/PB_054")
    return successes, failures


def prepare(recovery=RECOVERY, require_clean=True):
    if recovery.exists(): raise FileExistsError("Recovery generation exists: " + str(recovery))
    frozen, plan = verify_frozen_inputs(); successes, failures = original_state(frozen)
    remaining = [x for x in frozen["model_ready_ids"] if x not in successes]
    if remaining[:2] != failures or len(remaining) != 24: raise ValueError("Recovery plan drift")
    if require_clean and subprocess.check_output(["git", "status", "--porcelain", "--", str(Path(__file__).relative_to(REPO))], cwd=REPO, text=True):
        raise RuntimeError("Commit recovery implementation before freeze")
    (recovery / "comparison_raw").mkdir(parents=True)
    inputs = {str(p): sha(p) for gid in remaining for p in (STAGE / "comparison_inputs" / gid).iterdir() if p.is_file()}
    historical = {gid: {str(p.name): sha(p) for p in (STAGE / "comparison_raw" / gid).iterdir() if p.is_file()}
                  for gid in successes + failures}
    call_by_id = {x["group_id"]: x for x in plan["groups"]}
    value = {"status": "FROZEN_READY", "frozen_at": now(), "authorization": "User explicitly authorized V2 recovery_1",
        "source_v2_inference_freeze_sha256": sha(FREEZE), "source_v2_code_commit": frozen["code_commit"],
        "recovery_code_commit": commit(), "recovery_code_sha256": sha(__file__), "model_config": frozen["model_config"],
        "model": MODEL, "reasoning": REASONING, "original_success_ids": successes,
        "historical_failed_ids": failures, "recovery_call_ids": remaining,
        "no_call_ids": frozen["no_call_ids"], "exact_group_ids": frozen["exact_group_ids"],
        "request_hashes": {gid: call_by_id[gid]["exact_request_sha256"] for gid in remaining},
        "frozen_input_hashes": inputs, "historical_artifact_hashes": historical,
        "prompt_hash": frozen["prompt_hash"], "schema_hash": frozen["schema_hash"],
        "alias_map_sha256": frozen["alias_map_sha256"], "evidence_hashes": frozen["evidence_hashes"],
        "group_hashes": frozen["group_hashes"], "parallelism": PARALLELISM, "automatic_retries": 0,
        "source_truth_opened": False, "openrouter_calls": 0, "claude_calls": 0}
    write(recovery / "RECOVERY_1_FREEZE.json", value); verify_recovery(recovery)
    write(recovery / "RECOVERY_1_PREFLIGHT.json", {"status": "PASS", "original_successes": 50,
        "historical_failures_preserved": failures, "recovery_calls": 24, "same_frozen_inputs": True,
        "same_prompt_schema_alias_evidence_groups_model": True, "source_truth_opened": False, "model_calls": 0})
    print("RECOVERY_1 PREFLIGHT PASS; preserved=50; historical failures=2; planned calls=24", flush=True)


def verify_recovery(recovery=RECOVERY):
    f = read(recovery / "RECOVERY_1_FREEZE.json"); frozen, _ = verify_frozen_inputs()
    if commit() != f["recovery_code_commit"] or sha(__file__) != f["recovery_code_sha256"]: raise ValueError("Recovery code drift")
    if sha(FREEZE) != f["source_v2_inference_freeze_sha256"] or runtime_identity() != f["model_config"]: raise ValueError("V2 freeze/runtime drift")
    for path, digest in f["frozen_input_hashes"].items():
        if sha(path) != digest: raise ValueError("Recovery input drift: " + path)
    for gid, files in f["historical_artifact_hashes"].items():
        for name, digest in files.items():
            if sha(STAGE / "comparison_raw" / gid / name) != digest: raise ValueError("Historical artifact drift: " + gid + "/" + name)
    if frozen["model_ready_ids"] != f["original_success_ids"] + f["recovery_call_ids"]: raise ValueError("Recovery coverage drift")
    return f


async def call_one(recovery, gid, expected_hash, entries):
    source = STAGE / "comparison_inputs" / gid; target = recovery / "comparison_raw" / gid
    if target.exists(): raise FileExistsError("No retry/overwrite inside recovery: " + gid)
    target.mkdir(); payload = (source / "EXACT_PROMPT.txt").read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_hash: raise ValueError("Exact request drift: " + gid)
    shutil.copyfile(source / "EXACT_PROMPT.txt", target / "prompt.txt"); shutil.copyfile(STAGE / "OUTPUT_SCHEMA.json", target / "schema.json")
    names = []
    for i, image in enumerate(read(source / "IMAGES.json")):
        name=f"image_{i:02d}.png"; shutil.copyfile(image["path"],target/name)
        if sha(target/name)!=image["sha256"]:raise ValueError("Raster drift")
        names.append(name)
    hashes={p.name:sha(p) for p in target.iterdir() if p.is_file()};command=command_for(target,names)
    write(target/"INVOCATION.json",{"group_id":gid,"model":MODEL,"reasoning":REASONING,"provider":"codex_chatgpt",
        "fresh_context":True,"recovery_generation":"recovery_1","historical_attempt_preserved":gid in {"PB_053","PB_054"},
        "openrouter":False,"retries_within_recovery":0,"input_hashes":hashes,"command":command})
    started=time.monotonic();proc=None;error=None
    try:
        with (target/"raw.jsonl").open("wb") as raw,(target/"stderr.txt").open("wb") as err:
            proc=await asyncio.create_subprocess_exec(*command,stdin=asyncio.subprocess.PIPE,stdout=raw,stderr=err,env=safe_env(),start_new_session=True)
            await asyncio.wait_for(proc.communicate(payload),timeout=3600)
    except BaseException as exc:
        error=exc
        if proc is not None and proc.returncode is None:os.killpg(proc.pid,signal.SIGKILL);await proc.wait()
    records=[]
    for line in (target/"raw.jsonl").read_text().splitlines():
        try:records.append(json.loads(line))
        except ValueError:pass
    usage=[r["usage"] for r in records if r.get("type")=="turn.completed" and r.get("usage")]
    tools=[r for r in records if r.get("type","").startswith("item.") and r.get("item",{}).get("type") not in {None,"agent_message","reasoning","error"}]
    receipt={"seconds":time.monotonic()-started,"usage":usage,"tool_items":len(tools),"input_integrity":all(sha(target/n)==d for n,d in hashes.items()),"exit_code":proc.returncode if proc else None}
    try:
        if error:raise error
        if proc.returncode or not usage or tools or not receipt["input_integrity"]:raise RuntimeError("Runtime/schema/tool failure")
        raw=read(target/"final.txt");validate(raw,read(target/"schema.json"))
        if raw["case_token"]!=gid:raise ValueError("Group token mismatch")
        expanded=validate_and_expand(raw,entries);write(target/"parsed.json",raw);write(target/"parsed_expanded.json",expanded)
        receipt.update(status="SUCCESS",raw_output_sha256=sha(target/"parsed.json"),expanded_output_sha256=sha(target/"parsed_expanded.json"));write(target/"SUCCESS.json",receipt)
    except BaseException as exc:
        receipt.update(status="FAILED_CLOSED",error_type=type(exc).__name__,error=str(exc));write(target/"FAILURE.json",receipt);raise


async def run(recovery=RECOVERY):
    f=verify_recovery(recovery)
    if list((recovery/"comparison_raw").glob("*/INVOCATION.json")):raise RuntimeError("Recovery already started; no automatic resume")
    aliases=read(STAGE/"ATOMIC_ALIAS_MAP.json")["groups"]
    try:
        for start in range(0,len(f["recovery_call_ids"]),PARALLELISM):
            verify_recovery(recovery);batch=f["recovery_call_ids"][start:start+PARALLELISM]
            results=await asyncio.gather(*(call_one(recovery,gid,f["request_hashes"][gid],aliases[gid]) for gid in batch),return_exceptions=True)
            errors=[x for x in results if isinstance(x,BaseException)]
            if errors:raise errors[0]
            print(f"RECOVERY_1 COMPLETE {len(list((recovery/'comparison_raw').glob('*/SUCCESS.json')))}/24; unified={50+len(list((recovery/'comparison_raw').glob('*/SUCCESS.json')))}/74",flush=True)
        write(recovery/"RECOVERY_1_INFERENCE_COMPLETE.json",{"at":now(),"successful_calls":24,"technical_failures":0,"source_truth_opened":False})
    except BaseException as exc:
        write(recovery/"RECOVERY_1_STOP.json",{"status":"STOPPED_NO_REPAIR","at":now(),"error_type":type(exc).__name__,"error":str(exc),
            "attempts":len(list((recovery/'comparison_raw').glob('*/INVOCATION.json'))),"successful":len(list((recovery/'comparison_raw').glob('*/SUCCESS.json'))),"source_truth_opened":False})
        raise


def finalize(recovery=RECOVERY):
    f=verify_recovery(recovery)
    if (STAGE/"CONSOLIDATED_COMPARISON_V2_RESULT_FREEZE.json").exists():raise FileExistsError("Unified V2 result already frozen")
    if len(list((recovery/"comparison_raw").glob("*/SUCCESS.json")))!=24:raise RuntimeError("Need 24 successful recovery calls")
    from experiments.project_change_post_inference_repair_v4_272.repair import repair
    plan=read(STAGE/"CALL_PLAN.json");original=set(f["original_success_ids"]);local=[];atoms=[];post=[];expansion=[];sources={}
    for item in plan["groups"]:
        gid=item["group_id"]
        if item["action"]=="MODEL_CALL":
            directory=(STAGE/"comparison_raw"/gid) if gid in original else (recovery/"comparison_raw"/gid)
            raw=read(directory/"parsed.json");expanded=read(directory/"parsed_expanded.json")
            normalized=repair(expanded,read(STAGE/"comparison_inputs"/gid/"PACKET.json"),None,())
            rows=expanded["atomic_member_results"];atoms += [{"group_id":gid,**x} for x in rows]
            local.append({"group_id":gid,"raw_verdict":raw["verdict"],"reasoning":raw["reasoning_ru"],"old_state":raw["old_state"],"new_state":raw["new_state"],"atomic_member_results":rows})
            post.append({"group_id":gid,"call_status":"SUCCESS","raw_verdict":raw["verdict"],"final_verdict":normalized.get("effective_verdict","REVIEW"),"normalized":normalized})
            expansion.append({"group_id":gid,"status":"PASS","source":"original_v2" if gid in original else "recovery_1","aliases":len(raw["atomic_results"]),"unknown":0,"duplicate":0,"missing":0})
            sources[gid]="original_v2" if gid in original else "recovery_1"
        else:
            entries=read(STAGE/"comparison_inputs"/gid/"ADAPTER.json")["aliases"]
            atoms += [{"group_id":gid,"candidate_id":x["atomic_candidate_id"],"status":"INSUFFICIENT","reason":item["action"]} for x in entries]
            local.append({"group_id":gid,"raw_verdict":"NO_CALL","reasoning":item["action"],"old_state":None,"new_state":None,"atomic_member_results":[]})
            post.append({"group_id":gid,"call_status":item["action"],"raw_verdict":"NO_CALL","final_verdict":"NO_CALL","normalized":None})
            sources[gid]="no_call"
    write(STAGE/"ALIAS_EXPANSION_AUDIT.json",{"status":"PASS","groups":expansion,"unknown_aliases":0,"duplicate_aliases":0,"missing_aliases":0,"exact_id_expansion":True})
    write(STAGE/"ATOMIC_MEMBER_RESULTS.json",{"count":len(atoms),"results":atoms});write(STAGE/"LOCAL_COMPARISON_RESULTS.json",{"count":len(local),"results":local});write(STAGE/"POST_INFERENCE_RESULTS.json",{"count":len(post),"results":post})
    write(recovery/"RECOVERY_1_RECEIPT.json",{"status":"COMPLETE","at":now(),"original_v2_success_ids":f["original_success_ids"],
        "historical_technical_failures":f["historical_failed_ids"],"recovery_1_success_ids":f["recovery_call_ids"],"group_result_sources":sources,
        "original_successes":50,"recovery_successes":24,"unified_successes":74,"historical_failures_preserved":2,
        "same_frozen_inputs":True,"source_truth_opened":False,"openrouter_calls":0,"claude_calls":0})
    files={}
    for directory in (STAGE/"comparison_inputs",STAGE/"comparison_raw",recovery):
        for p in directory.rglob("*"):
            if p.is_file():files[str(p.relative_to(STAGE))]=sha(p)
    for n in ("ALIAS_EXPANSION_AUDIT.json","ATOMIC_MEMBER_RESULTS.json","LOCAL_COMPARISON_RESULTS.json","POST_INFERENCE_RESULTS.json"):files[n]=sha(STAGE/n)
    write(STAGE/"CONSOLIDATED_COMPARISON_V2_RESULT_FREEZE.json",{"status":"FROZEN","frozen_at":now(),"files":files,
        "inference_freeze_sha256":sha(FREEZE),"recovery_freeze_sha256":sha(recovery/"RECOVERY_1_FREEZE.json"),"recovery_receipt_sha256":sha(recovery/"RECOVERY_1_RECEIPT.json"),
        "original_successful_calls":50,"recovery_successful_calls":24,"successful_results":74,"historical_technical_failures":2,
        "final_verdict_counts":dict(Counter(x["final_verdict"] for x in post)),"model":MODEL,"reasoning":REASONING,"alias_contract":"PASS",
        "source_truth_opened":False,"openrouter_calls":0,"claude_calls":0,"production":"UNCHANGED","ui":"UNCHANGED","validation":"NOT OPENED","final_holdout":"NOT OPENED"})
    print("UNIFIED V2 RESULT FREEZE CREATED; 74/74 successful results; truth may now be opened",flush=True)


def main():
    command=sys.argv[1] if len(sys.argv)>1 else ""
    if command=="prepare":prepare()
    elif command=="verify":verify_recovery();print("RECOVERY_1 FREEZE VERIFIED")
    elif command=="run":asyncio.run(run())
    elif command=="finalize":finalize()
    else:raise SystemExit("usage: prepare|verify|run|finalize")


if __name__=="__main__":main()
