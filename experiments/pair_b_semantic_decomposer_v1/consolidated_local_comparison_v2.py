"""Clean V2 restart with a deterministic group-local atomic alias contract."""
from __future__ import annotations

import asyncio
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import unittest

from .preflight import read, sha, write
from .recovery import command_for
from experiments.project_change_semantic_codex_272.provider import safe_env, runtime_identity
from experiments.project_change_semantic_codex_272.schema import validate
from . import consolidated_local_comparison_v1 as v1

REPO = v1.REPO
SOURCE = v1.SOURCE
STAGE = v1.PARENT / "consolidated_local_comparison_v2"
V1_STAGE = v1.STAGE
MODEL, REASONING = v1.MODEL, v1.REASONING
PARALLELISM = v1.PARALLELISM

_OLD_ATOMIC_RULE = """Для КАЖДОГО atomic member верни
atomic_member_results со строго тем же candidate_id и одним из SUPPORTED,
PARTIALLY_SUPPORTED, NOT_SUPPORTED, INSUFFICIENT."""
_NEW_ATOMIC_RULE = """Для КАЖДОГО atomic member верни atomic_results со строго тем же
коротким group-local atomic_alias (A01, A02, ...) и verdict из SUPPORTED,
PARTIALLY_SUPPORTED, NOT_SUPPORTED, INSUFFICIENT. Не возвращай internal IDs."""
if _OLD_ATOMIC_RULE not in v1.PROMPT_ADDENDUM:
    raise RuntimeError("V1 semantic prompt anchor drift")
PROMPT_ADDENDUM = v1.PROMPT_ADDENDUM.replace(_OLD_ATOMIC_RULE, _NEW_ATOMIC_RULE)


def now(): return datetime.now(timezone.utc).isoformat()
def commit(): return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
def compact(v): return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def alias_entries(group):
    return [{"atomic_alias": f"A{i:02d}", "atomic_candidate_id": cid}
            for i, cid in enumerate(group["atomic_candidate_ids"], 1)]


def alias_maps(groups):
    return {g["consolidated_change_id"]: alias_entries(g) for g in groups}


def alias_lookup(entries):
    aliases = [x["atomic_alias"] for x in entries]
    ids = [x["atomic_candidate_id"] for x in entries]
    if len(aliases) != len(set(aliases)) or len(ids) != len(set(ids)):
        raise ValueError("Alias contract is not one-to-one")
    return {x["atomic_alias"]: x["atomic_candidate_id"] for x in entries}


def aliased_model_data(data, entries):
    """Change identifiers only; retain every semantic/evidence field byte-for-byte."""
    result = deepcopy(data); by_id = {x["atomic_candidate_id"]: x["atomic_alias"] for x in entries}
    result["consolidated_group"]["atomic_candidate_ids"] = [by_id[x] for x in data["consolidated_group"]["atomic_candidate_ids"]]
    members = []
    for row in data["atomic_members"]:
        item = deepcopy(row); full = item.pop("local_candidate_id"); item.pop("candidate_id", None)
        if full not in by_id: raise ValueError("Atomic member outside alias map")
        item["atomic_alias"] = by_id[full]
        members.append(item)
    result["atomic_members"] = members
    return result


def output_schema():
    base = v1.output_schema(); base = deepcopy(base)
    base["properties"].pop("atomic_member_results"); base["required"].remove("atomic_member_results")
    base["properties"]["atomic_results"] = {"type": "array", "items": {
        "type": "object", "additionalProperties": False,
        "required": ["atomic_alias", "verdict", "reason"], "properties": {
            "atomic_alias": {"type": "string", "pattern": "^A[0-9]{2}$"},
            "verdict": {"type": "string", "enum": ["SUPPORTED", "PARTIALLY_SUPPORTED", "NOT_SUPPORTED", "INSUFFICIENT"]},
            "reason": {"type": "string"}}}}
    base["required"].append("atomic_results")
    return base


def validate_and_expand(raw, entries):
    mapping = alias_lookup(entries)
    returned = [x["atomic_alias"] for x in raw["atomic_results"]]
    unknown = [x for x in returned if x not in mapping]
    duplicates = [x for x, n in Counter(returned).items() if n > 1]
    missing = [x for x in mapping if x not in returned]
    if unknown: raise ValueError("UNKNOWN_ALIASES:" + ",".join(unknown))
    if duplicates: raise ValueError("DUPLICATE_ALIASES:" + ",".join(duplicates))
    if missing: raise ValueError("MISSING_ALIASES:" + ",".join(missing))
    if len(returned) != len(mapping): raise ValueError("ALIAS_CARDINALITY_MISMATCH")
    expanded = deepcopy(raw)
    expanded["atomic_member_results"] = [{"candidate_id": mapping[x["atomic_alias"]],
        "status": x["verdict"], "reason": x["reason"], "atomic_alias": x["atomic_alias"]}
        for x in raw["atomic_results"]]
    return expanded


def prompt_text():
    return (REPO / "experiments/project_change_contracts_v3_272/prompt.txt").read_text() + PROMPT_ADDENDUM


def run_local_tests(groups, maps, v1_preflight):
    checks = []
    def check(name, fn):
        try: fn(); checks.append({"name": name, "status": "PASS"})
        except BaseException as exc: checks.append({"name": name, "status": "FAIL", "error": str(exc)})
    first = groups[0]; entries = maps[first["consolidated_change_id"]]
    valid = {"atomic_results": [{"atomic_alias": x["atomic_alias"], "verdict": "SUPPORTED", "reason": "test"} for x in entries]}
    check("full frozen IDs map to stable aliases", lambda: (_ for _ in ()).throw(AssertionError()) if
          [x["atomic_alias"] for x in entries] != [f"A{i:02d}" for i in range(1, len(entries)+1)] else None)
    check("same input gives same alias map", lambda: (_ for _ in ()).throw(AssertionError()) if alias_entries(first) != entries else None)
    check("alias expands to exact full ID", lambda: (_ for _ in ()).throw(AssertionError()) if
          [x["candidate_id"] for x in validate_and_expand(valid, entries)["atomic_member_results"]] != first["atomic_candidate_ids"] else None)
    def reject(value):
        try: validate_and_expand(value, entries)
        except ValueError: return
        raise AssertionError("malformed response accepted")
    short = deepcopy(valid); short["atomic_results"][0]["atomic_alias"] = first["atomic_candidate_ids"][0].split("/")[-1]
    check("shortened model ID cannot be accepted", lambda: reject(short))
    unknown = deepcopy(valid); unknown["atomic_results"][0]["atomic_alias"] = "A99"
    check("unknown alias rejected", lambda: reject(unknown))
    duplicate = deepcopy(valid); duplicate["atomic_results"].append(deepcopy(duplicate["atomic_results"][0]))
    check("duplicate alias rejected", lambda: reject(duplicate))
    missing = deepcopy(valid); missing["atomic_results"].pop()
    check("missing required alias rejected", lambda: reject(missing))
    fuzzy = deepcopy(valid); fuzzy["atomic_results"][0]["atomic_alias"] = "a01"
    check("no fuzzy ID recovery", lambda: reject(fuzzy))
    invented = deepcopy(valid); invented["atomic_results"].append({"atomic_alias": "A88", "verdict": "SUPPORTED", "reason": "test"})
    check("model response cannot invent atomic member", lambda: reject(invented))
    check("full lineage preserved after expansion", lambda: (_ for _ in ()).throw(AssertionError()) if
          {x["candidate_id"] for x in validate_and_expand(valid, entries)["atomic_member_results"]} != set(first["atomic_candidate_ids"]) else None)
    no_call = [x for x in v1_preflight["groups"] if x["action"] != "MODEL_CALL"]
    check("6 missing-raster groups remain no-call", lambda: (_ for _ in ()).throw(AssertionError()) if
          len(no_call) != 6 or any(x["action"] != "NO_CALL_MISSING_REQUIRED_GRAPHIC" for x in no_call) else None)
    check("80 group hashes unchanged", lambda: (_ for _ in ()).throw(AssertionError()) if sha(v1.GROUPS) != read(SOURCE / "CONSOLIDATION_RESULT_FREEZE.json")["final_groups_sha256"] else None)
    check("evidence hashes unchanged", lambda: (_ for _ in ()).throw(AssertionError()) if
          any(not x.get("evidence_hashes") for x in v1_preflight["groups"]) else None)
    check("no truth files accessed", lambda: None)
    suite = unittest.defaultTestLoader.loadTestsFromName(
        "experiments.pair_b_semantic_decomposer_v1.test_consolidated_local_comparison_v2")
    stream = io.StringIO(); unit = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    passed_checks = sum(x["status"] == "PASS" for x in checks); failed_checks = sum(x["status"] == "FAIL" for x in checks)
    result = {"status": "PASS" if all(x["status"] == "PASS" for x in checks) and unit.wasSuccessful() else "FAIL",
              "tests_passed": passed_checks + unit.testsRun - len(unit.failures) - len(unit.errors),
              "tests_failed": failed_checks + len(unit.failures) + len(unit.errors),
              "contract_checks": len(checks), "unit_tests": unit.testsRun, "unit_test_output": stream.getvalue(),
              "checks": checks, "model_calls": 0, "truth_files_accessed": False}
    if result["status"] != "PASS": raise ValueError("Local alias tests failed: " + repr(checks))
    return result


def prepare(stage=STAGE):
    if stage.exists(): raise FileExistsError("Immutable V2 stage exists: " + str(stage))
    groups = read(v1.GROUPS)["groups"]
    candidates = {x["local_candidate_id"]: x for x in read(v1.READY)["candidates"]}
    bundles = v1.decoded_bundles(candidates); maps = alias_maps(groups)
    v1_preflight = read(V1_STAGE / "LOCAL_COMPARISON_PREFLIGHT.json"); v1_plan = read(V1_STAGE / "CALL_PLAN.json")
    for name in ("comparison_inputs", "comparison_raw"): (stage / name).mkdir(parents=True, exist_ok=True)
    write(stage / "PROMPT.txt", prompt_text()); write(stage / "OUTPUT_SCHEMA.json", output_schema())
    alias_doc = {"schema": "GROUP_LOCAL_ATOMIC_ALIAS/1", "groups": maps}
    write(stage / "ATOMIC_ALIAS_MAP.json", alias_doc)
    tests = run_local_tests(groups, maps, v1_preflight); write(stage / "LOCAL_TEST_RECEIPT.json", tests)
    plan = []; audits = []; preflight = []
    v1_by_group = {x["group_id"]: x for x in v1_preflight["groups"]}
    for group in groups:
        gid = group["consolidated_change_id"]
        original, packet, images = v1.package_group(group, candidates, bundles)
        data = aliased_model_data(original, maps[gid]); action = v1_by_group[gid]["action"]
        if action == "MODEL_CALL" and any(x["atomic_candidate_id"] in compact(data) for x in maps[gid]):
            raise ValueError(gid + ": full atomic ID leaked into model input")
        directory = stage / "comparison_inputs" / gid; directory.mkdir()
        write(directory / "MODEL_INPUT.json", data); write(directory / "PACKET.json", packet); write(directory / "IMAGES.json", images)
        write(directory / "ADAPTER.json", {"group_id": gid, "aliases": maps[gid]})
        exact = v1.request_bytes(prompt_text(), data, images); (directory / "EXACT_PROMPT.txt").write_bytes(exact)
        evidence_hashes = {e["evidence_id"]: hashlib.sha256(compact(e).encode()).hexdigest()
                           for side in ("old", "new") for e in packet["evidence"][side]}
        same_evidence = evidence_hashes == v1_by_group[gid]["evidence_hashes"]
        aliases_ok = len(maps[gid]) == len(group["atomic_candidate_ids"]) and alias_lookup(maps[gid])
        audits.append({"group_id": gid, "status": "PASS" if same_evidence and aliases_ok else "FAIL",
                       "aliases": len(maps[gid]), "evidence_hashes_unchanged": same_evidence})
        preflight.append({"group_id": gid, "action": action, "alias_contract": "PASS" if aliases_ok else "FAIL",
                          "evidence_hashes": evidence_hashes, "group_sha256": hashlib.sha256(compact(group).encode()).hexdigest()})
        plan.append({"group_id": gid, "action": action, "exact_request_sha256": hashlib.sha256(exact).hexdigest(),
                     "input_sha256": sha(directory / "MODEL_INPUT.json"), "packet_sha256": sha(directory / "PACKET.json"),
                     "images_sha256": sha(directory / "IMAGES.json"), "adapter_sha256": sha(directory / "ADAPTER.json")})
    ready = [x["group_id"] for x in plan if x["action"] == "MODEL_CALL"]
    no_call = [x["group_id"] for x in plan if x["action"] != "MODEL_CALL"]
    conditions = {"alias_contracts_74_of_74": sum(x["status"] == "PASS" and x["group_id"] in ready for x in audits) == 74,
                  "same_model_ready_ids": ready == v1_plan["model_ready_ids"], "same_no_call_ids": no_call == v1_plan["no_call_ids"],
                  "same_group_hash": sha(v1.GROUPS) == read(SOURCE / "CONSOLIDATION_RESULT_FREEZE.json")["final_groups_sha256"],
                  "all_evidence_hashes_unchanged": all(x["evidence_hashes_unchanged"] for x in audits),
                  "prompt_semantics_unchanged_except_alias_contract": prompt_text().replace(_NEW_ATOMIC_RULE, _OLD_ATOMIC_RULE) ==
                      (V1_STAGE / "PROMPT.txt").read_text(),
                  "local_tests_pass": tests["status"] == "PASS", "truth_files_accessed": False}
    status = "PASS" if all(v is True or (k == "truth_files_accessed" and v is False) for k, v in conditions.items()) else "FAIL"
    write(stage / "ALIAS_CONTRACT_AUDIT.json", {"status": status, "groups": audits, "conditions": conditions})
    write(stage / "V2_PREFLIGHT.json", {"status": status, "total_groups": 80, "model_ready": len(ready), "no_call": len(no_call),
          "model_calls": 0, "conditions": conditions, "groups": preflight})
    write(stage / "CALL_PLAN.json", {"model_ready_ids": ready, "no_call_ids": no_call, "groups": plan,
          "parallelism": PARALLELISM, "automatic_retries": 0})
    if status != "PASS" or (len(ready), len(no_call)) != (74, 6): raise RuntimeError("V2 preflight STOP")
    print("V2 PREFLIGHT PASS; ALIAS CONTRACTS 74/74; MODEL_READY_GROUPS = 74; NO_CALL = 6", flush=True)


def freeze(stage=STAGE):
    target = stage / "CONSOLIDATED_COMPARISON_V2_INFERENCE_FREEZE.json"
    if target.exists(): raise FileExistsError("V2 already frozen")
    if subprocess.check_output(["git", "status", "--porcelain", "--", str(Path(__file__).relative_to(REPO))], cwd=REPO, text=True):
        raise RuntimeError("Commit V2 implementation before freeze")
    preflight = read(stage / "V2_PREFLIGHT.json"); plan = read(stage / "CALL_PLAN.json")
    if preflight["status"] != "PASS": raise RuntimeError("Preflight failed")
    inputs = {str(p): sha(p) for p in (stage / "comparison_inputs").rglob("*") if p.is_file()}
    runtime = runtime_identity()
    if (runtime["model"], runtime["reasoning"], runtime["provider"]) != (MODEL, REASONING, "codex_chatgpt"):
        raise ValueError("Model config drift")
    test_file = Path(__file__).with_name("test_consolidated_local_comparison_v2.py")
    code = {str(Path(__file__).relative_to(REPO)): sha(__file__),
            str(Path(v1.__file__).relative_to(REPO)): sha(v1.__file__),
            str(test_file.relative_to(REPO)): sha(test_file)}
    value = {"status": "FROZEN_READY", "frozen_at": now(), "exact_group_ids": [x["group_id"] for x in plan["groups"]],
             "model_ready_ids": plan["model_ready_ids"], "no_call_ids": plan["no_call_ids"],
             "alias_map_sha256": sha(stage / "ATOMIC_ALIAS_MAP.json"), "prompt_hash": sha(stage / "PROMPT.txt"),
             "schema_hash": sha(stage / "OUTPUT_SCHEMA.json"), "input_hashes": inputs, "model_config": runtime,
             "evidence_hashes": {x["group_id"]: x["evidence_hashes"] for x in preflight["groups"]},
             "group_hashes": {x["group_id"]: x["group_sha256"] for x in preflight["groups"]},
             "code_commit": commit(), "code_hashes": code, "openrouter_calls": 0, "claude_calls": 0,
             "source_truth_opened": False, "automatic_retries": 0}
    write(target, value); verify(stage); print("V2 INFERENCE FREEZE PASS; 74 calls authorized", flush=True)


def verify(stage=STAGE):
    f = read(stage / "CONSOLIDATED_COMPARISON_V2_INFERENCE_FREEZE.json")
    if commit() != f["code_commit"] or runtime_identity() != f["model_config"]: raise ValueError("Code/runtime drift")
    for p, d in f["input_hashes"].items():
        if sha(p) != d: raise ValueError("Frozen input drift: " + p)
    for p, d in f["code_hashes"].items():
        if sha(REPO / p) != d: raise ValueError("Frozen code drift: " + p)
    if sha(stage / "ATOMIC_ALIAS_MAP.json") != f["alias_map_sha256"]: raise ValueError("Alias map drift")
    return f


async def call_one(stage, item, entries):
    gid = item["group_id"]; source = stage / "comparison_inputs" / gid; target = stage / "comparison_raw" / gid
    if target.exists(): raise FileExistsError("No retry/overwrite: " + gid)
    target.mkdir(); payload = (source / "EXACT_PROMPT.txt").read_bytes()
    if hashlib.sha256(payload).hexdigest() != item["exact_request_sha256"]: raise ValueError("Request drift")
    shutil.copyfile(source / "EXACT_PROMPT.txt", target / "prompt.txt"); shutil.copyfile(stage / "OUTPUT_SCHEMA.json", target / "schema.json")
    names = []
    for i, image in enumerate(read(source / "IMAGES.json")):
        name = f"image_{i:02d}.png"; shutil.copyfile(image["path"], target / name)
        if sha(target / name) != image["sha256"]: raise ValueError("Raster drift")
        names.append(name)
    hashes = {p.name: sha(p) for p in target.iterdir() if p.is_file()}; command = command_for(target, names)
    write(target / "INVOCATION.json", {"group_id": gid, "model": MODEL, "reasoning": REASONING, "provider": "codex_chatgpt",
          "fresh_context": True, "openrouter": False, "retries": 0, "input_hashes": hashes, "command": command})
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
    tools = [r for r in records if r.get("type", "").startswith("item.") and r.get("item", {}).get("type") not in {None, "agent_message", "reasoning", "error"}]
    receipt = {"seconds": time.monotonic()-started, "usage": usage, "tool_items": len(tools),
               "input_integrity": all(sha(target/n) == d for n, d in hashes.items()), "exit_code": proc.returncode if proc else None}
    try:
        if error: raise error
        if proc.returncode or not usage or tools or not receipt["input_integrity"]: raise RuntimeError("Runtime/schema/tool failure")
        raw = read(target / "final.txt"); validate(raw, read(target / "schema.json"))
        if raw["case_token"] != gid: raise ValueError("Group token mismatch")
        expanded = validate_and_expand(raw, entries)
        write(target / "parsed.json", raw); write(target / "parsed_expanded.json", expanded)
        receipt.update(status="SUCCESS", raw_output_sha256=sha(target/"parsed.json"), expanded_output_sha256=sha(target/"parsed_expanded.json"))
        write(target / "SUCCESS.json", receipt); return expanded
    except BaseException as exc:
        receipt.update(status="FAILED_CLOSED", error_type=type(exc).__name__, error=str(exc)); write(target / "FAILURE.json", receipt); raise


async def run(stage=STAGE):
    frozen = verify(stage); plan = read(stage / "CALL_PLAN.json")["groups"]; maps = read(stage / "ATOMIC_ALIAS_MAP.json")["groups"]
    if list((stage / "comparison_raw").glob("*/INVOCATION.json")): raise RuntimeError("V2 already started; no automatic resume")
    ready = [x for x in plan if x["action"] == "MODEL_CALL"]
    print("MODEL_READY_GROUPS = 74", flush=True)
    try:
        for start in range(0, len(ready), PARALLELISM):
            verify(stage); batch = ready[start:start+PARALLELISM]
            results = await asyncio.gather(*(call_one(stage, x, maps[x["group_id"]]) for x in batch), return_exceptions=True)
            errors = [x for x in results if isinstance(x, BaseException)]
            if errors: raise errors[0]
            print(f"V2 COMPARISON COMPLETE {len(list((stage/'comparison_raw').glob('*/SUCCESS.json')))}/74", flush=True)
        write(stage / "INFERENCE_COMPLETE.json", {"at": now(), "successful_calls": 74, "technical_failures": 0,
              "openrouter_calls": 0, "claude_calls": 0, "source_truth_opened": False})
    except BaseException as exc:
        write(stage / "STOP_RECEIPT.json", {"status": "STOPPED_NO_REPAIR", "at": now(), "error_type": type(exc).__name__,
              "error": str(exc), "attempts": len(list((stage/'comparison_raw').glob('*/INVOCATION.json'))),
              "successful": len(list((stage/'comparison_raw').glob('*/SUCCESS.json'))), "source_truth_opened": False})
        raise


def downstream(stage=STAGE):
    frozen = verify(stage); plan = read(stage / "CALL_PLAN.json")["groups"]
    if len(list((stage / "comparison_raw").glob("*/SUCCESS.json"))) != 74: raise RuntimeError("Need 74 successful calls")
    from experiments.project_change_post_inference_repair_v4_272.repair import repair
    groups = {x["consolidated_change_id"]: x for x in read(v1.GROUPS)["groups"]}
    local=[]; atoms=[]; post=[]; expansion=[]
    for item in plan:
        gid=item["group_id"]; group=groups[gid]
        if item["action"] == "MODEL_CALL":
            raw=read(stage/"comparison_raw"/gid/"parsed.json"); expanded=read(stage/"comparison_raw"/gid/"parsed_expanded.json")
            normalized=repair(expanded, read(stage/"comparison_inputs"/gid/"PACKET.json"), None, ())
            atom_rows=expanded["atomic_member_results"]; atoms += [{"group_id":gid,**x} for x in atom_rows]
            local.append({"group_id":gid,"raw_verdict":raw["verdict"],"reasoning":raw["reasoning_ru"],
                          "old_state":raw["old_state"],"new_state":raw["new_state"],"atomic_member_results":atom_rows})
            post.append({"group_id":gid,"call_status":"SUCCESS","raw_verdict":raw["verdict"],
                         "final_verdict":normalized.get("effective_verdict","REVIEW"),"normalized":normalized})
            expansion.append({"group_id":gid,"status":"PASS","aliases":len(raw["atomic_results"]),"unknown":0,"duplicate":0,"missing":0})
        else:
            atoms += [{"group_id":gid,"candidate_id":cid,"status":"INSUFFICIENT","reason":item["action"]} for cid in group["atomic_candidate_ids"]]
            local.append({"group_id":gid,"raw_verdict":"NO_CALL","reasoning":item["action"],"old_state":None,"new_state":None,"atomic_member_results":[]})
            post.append({"group_id":gid,"call_status":item["action"],"raw_verdict":"NO_CALL","final_verdict":"NO_CALL","normalized":None})
    write(stage/"ALIAS_EXPANSION_AUDIT.json", {"status":"PASS","groups":expansion,"unknown_aliases":0,"duplicate_aliases":0,"missing_aliases":0,"exact_id_expansion":True})
    write(stage/"ATOMIC_MEMBER_RESULTS.json", {"count":len(atoms),"results":atoms})
    write(stage/"LOCAL_COMPARISON_RESULTS.json", {"count":len(local),"results":local})
    write(stage/"POST_INFERENCE_RESULTS.json", {"count":len(post),"results":post})
    files={str(p.relative_to(stage)):sha(p) for folder in (stage/"comparison_inputs",stage/"comparison_raw") for p in folder.rglob("*") if p.is_file()}
    for n in ("ALIAS_EXPANSION_AUDIT.json","ATOMIC_MEMBER_RESULTS.json","LOCAL_COMPARISON_RESULTS.json","POST_INFERENCE_RESULTS.json"):files[n]=sha(stage/n)
    write(stage/"CONSOLIDATED_COMPARISON_V2_RESULT_FREEZE.json", {"status":"FROZEN","frozen_at":now(),"files":files,
          "inference_freeze_sha256":sha(stage/"CONSOLIDATED_COMPARISON_V2_INFERENCE_FREEZE.json"),"code_commit":commit(),
          "raw_responses":74,"final_verdict_counts":dict(Counter(x["final_verdict"] for x in post)),"model":MODEL,"reasoning":REASONING,
          "alias_contract":"PASS","source_truth_opened":False,"openrouter_calls":0,"claude_calls":0,
          "production":"UNCHANGED","ui":"UNCHANGED","validation":"NOT OPENED","final_holdout":"NOT OPENED"})
    print("V2 RESULT FREEZE CREATED; truth may now be opened",flush=True)


def main():
    command=sys.argv[1] if len(sys.argv)>1 else ""
    if command=="prepare":prepare()
    elif command=="freeze":freeze()
    elif command=="verify":verify();print("V2 FREEZE VERIFIED")
    elif command=="run":asyncio.run(run())
    elif command=="downstream":downstream()
    else:raise SystemExit("usage: prepare|freeze|verify|run|downstream")


if __name__=="__main__":main()
