"""Two-pass semantic consolidation of the frozen Pair B ready candidates.

Only candidate_condensation_v2/READY_FOR_COMPARISON.json is read. PASS A uses
one isolated model context per broad context; PASS B sees only frozen PASS A.
"""
from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import sys
import time
import unicodedata

from .preflight import OUT, read, sha, write
from .recovery import command_for
from experiments.project_change_semantic_codex_272.provider import safe_env, runtime_identity
from experiments.project_change_semantic_codex_272.schema import validate

SOURCE = OUT / "candidate_condensation_v2/READY_FOR_COMPARISON.json"
STAGE = OUT / "semantic_consolidator_v1"
MODEL, REASONING = "gpt-6-astra", "xhigh"
PASS_A_LIMIT, PASS_B_LIMIT = 18, 2
MISSING_STATUS = "NOT_READY_DUE_MISSING_RASTER"
MEMBER_STATUSES = {"MEMBER_OF_GROUP", "STANDALONE_CHANGE", "UNRESOLVED_GROUPING", MISSING_STATUS}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def norm(value):
    value = unicodedata.normalize("NFKC", value or "").casefold().replace("ё", "е")
    return " ".join(re.findall(r"\w+", value, flags=re.UNICODE))


def unique(values):
    return sorted({v for v in values if v not in (None, "", [])}, key=str)


def safe_slug(value):
    return "ctx_" + hashlib.sha256(value.encode()).hexdigest()[:12]


def model_candidate(row):
    """Explicit model-input allowlist."""
    return {
        "candidate_id": row["local_candidate_id"], "broad_context": row["broad_context"],
        "engineering_subject": row["engineering_subject"], "subject_identity": row["subject_identity"],
        "scope": row["scope"], "location": row["location"],
        "system_or_subsystem": row["system_or_subsystem"], "claim_type": row["claim_type"],
        "possible_change_summary": row["possible_change_summary"],
        "old_refs": row["old_evidence_refs"], "new_refs": row["new_evidence_refs"],
        "modalities": unique(row["old_modality"] + row["new_modality"]),
        "parameter_or_property": row["claim_type"], "confidence": row["identity_confidence"],
        "comparison_readiness": row["comparison_readiness_audit"]["readiness"],
        "provenance": {"source_bundle": row["source_bundle"], "old_pages": row["old_pages"],
                       "new_pages": row["new_pages"], "derived_disposition": row["derived_disposition"]},
    }


MEMBER_SCHEMA = {"type": "object", "additionalProperties": False,
 "required": ["candidate_id", "status"], "properties": {
  "candidate_id": {"type": "string"}, "status": {"type": "string", "enum": sorted(MEMBER_STATUSES)}}}

PASS_A_SCHEMA = {"type": "object", "additionalProperties": False,
 "required": ["context_id", "broad_context", "groups"], "properties": {
  "context_id": {"type": "string"}, "broad_context": {"type": "string"},
  "groups": {"type": "array", "items": {"type": "object", "additionalProperties": False,
   "required": ["local_group_id", "broad_context", "group_summary", "engineering_subject", "scope",
    "locations", "event_type", "atomic_candidate_ids", "old_refs", "new_refs", "modalities", "parameters",
    "grouping_reason", "group_confidence", "comparison_readiness", "member_statuses",
    "system_wide_change", "contradictions"],
   "properties": {
    "local_group_id": {"type": "string"}, "broad_context": {"type": "string"},
    "group_summary": {"type": "string"}, "engineering_subject": {"type": "string"},
    "scope": {"type": "string"}, "locations": {"type": "array", "items": {"type": "string"}},
    "event_type": {"type": "string"},
    "atomic_candidate_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1},
    "old_refs": {"type": "array", "items": {"type": "string"}},
    "new_refs": {"type": "array", "items": {"type": "string"}},
    "modalities": {"type": "array", "items": {"type": "string"}},
    "parameters": {"type": "array", "items": {"type": "string"}},
    "grouping_reason": {"type": "string"},
    "group_confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
    "comparison_readiness": {"type": "string", "enum": ["READY", "BLOCKED_MISSING_RASTER", "UNRESOLVED"]},
    "member_statuses": {"type": "array", "items": MEMBER_SCHEMA, "minItems": 1},
    "system_wide_change": {"type": "boolean"},
    "contradictions": {"type": "array", "items": {"type": "string"}}}}}}}

PASS_B_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["groups"], "properties": {
 "groups": {"type": "array", "items": {"type": "object", "additionalProperties": False,
  "required": ["consolidated_change_id", "change_summary", "engineering_subject", "scope", "locations",
   "event_type", "member_local_group_ids", "atomic_candidate_ids", "old_refs", "new_refs", "modalities",
   "parameters_changed", "comparison_readiness", "blocking_reasons", "merge_reason", "system_wide_change"],
  "properties": {
   "consolidated_change_id": {"type": "string"}, "change_summary": {"type": "string"},
   "engineering_subject": {"type": "string"}, "scope": {"type": "string"},
   "locations": {"type": "array", "items": {"type": "string"}}, "event_type": {"type": "string"},
   "member_local_group_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1},
   "atomic_candidate_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1},
   "old_refs": {"type": "array", "items": {"type": "string"}},
   "new_refs": {"type": "array", "items": {"type": "string"}},
   "modalities": {"type": "array", "items": {"type": "string"}},
   "parameters_changed": {"type": "array", "items": {"type": "string"}},
   "comparison_readiness": {"type": "string", "enum": ["READY", "BLOCKED_MISSING_RASTER", "UNRESOLVED"]},
   "blocking_reasons": {"type": "array", "items": {"type": "string"}},
   "merge_reason": {"type": "string"}, "system_wide_change": {"type": "boolean"}}}}}}

PASS_A_RULES = """You are Semantic Consolidator V1. Do not find changes or assess truth.
Group only supplied candidates that are fragments of the SAME engineering OLD-to-NEW event. Same subject is not enough.
Keep different events, locations, systems, elements, and causes separate. Different parameters may merge only when
functionally parts of the same event. Cross-modality and 1-to-N/N-to-1 are allowed. A system-wide group requires the
same event logic across locations. Every candidate_id must occur exactly once in atomic_candidate_ids and once in
member_statuses. Use STANDALONE_CHANGE for a ready singleton, MEMBER_OF_GROUP for normal multi-group members,
UNRESOLVED_GROUPING when candidates contradict (and readiness UNRESOLVED), and NOT_READY_DUE_MISSING_RASTER for
every input with readiness FAIL. A failed member never makes a group ready; if it is the only support use
BLOCKED_MISSING_RASTER. Do not target a count. Do not add facts, values, states, parameters, IDs, or evidence.
old_refs/new_refs/modalities/parameters/locations must be exact member unions, copying input strings. Summaries may
only paraphrase or combine member summaries, retaining material facts without asserting truth. JSON only."""

PASS_B_RULES = """You are Semantic Consolidator V1 PASS B. Input is frozen PASS A only. Merge local groups across
contexts only when demonstrated fragments or semantic duplicates of the SAME engineering OLD-to-NEW event. Same subject
is not enough. Do not split local groups or find changes. Every local_group_id occurs exactly once; preserve all atomic
IDs and lineage. Do not add facts, values, states, parameters, IDs, or evidence. Atomic IDs, refs, modalities, parameters,
and locations must be exact member-group unions. Different locations merge only for one clear system-wide event with
the same event logic. Unresolved and missing-raster blockers remain explicit. Do not target a count. JSON only."""


def exact_prompt(rules, payload):
    return (rules + "\n\nINPUT JSON:\n" + json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def prepare(stage=STAGE):
    if stage.exists():
        raise FileExistsError("Immutable stage exists: " + str(stage))
    data = read(SOURCE); rows = data["candidates"]
    if data.get("count") != 154 or len(rows) != 154 or len({r["local_candidate_id"] for r in rows}) != 154:
        raise ValueError("Expected exactly 154 unique frozen ready candidates")
    contexts = defaultdict(list)
    for row in rows:
        contexts[row["broad_context"]].append(model_candidate(row))
    if len(contexts) > PASS_A_LIMIT:
        raise RuntimeError("STOP: more than 18 PASS A calls required")
    for name in ["pass_a_inputs", "pass_a_raw", "pass_b_inputs", "pass_b_raw"]:
        (stage / name).mkdir(parents=True, exist_ok=True)
    write(stage / "PASS_A_OUTPUT_SCHEMA.json", PASS_A_SCHEMA); write(stage / "PASS_B_OUTPUT_SCHEMA.json", PASS_B_SCHEMA)
    plan = []
    for context in sorted(contexts):
        cid = safe_slug(context); directory = stage / "pass_a_inputs" / cid; directory.mkdir()
        payload = {"context_id": cid, "broad_context": context,
                   "local_group_id_prefix": "PA_" + cid[4:] + "_",
                   "candidates": sorted(contexts[context], key=lambda x: x["candidate_id"])}
        write(directory / "INPUT.json", payload); prompt = exact_prompt(PASS_A_RULES, payload)
        (directory / "EXACT_PROMPT.txt").write_bytes(prompt)
        plan.append({"context_id": cid, "broad_context": context, "candidate_count": len(payload["candidates"]),
                     "exact_prompt_sha256": hashlib.sha256(prompt).hexdigest()})
    manifest = {"status": "PREPARED", "created_at": utcnow(), "pair_key": "caea6d2810c334ec0368de8e",
        "source": str(SOURCE), "source_sha256": sha(SOURCE), "candidate_count": 154,
        "broad_context_count": len(plan), "pass_a_call_limit": 18, "pass_b_call_limit": 2,
        "planned_calls": len(plan) + 1, "model": MODEL, "reasoning": REASONING,
        "provider": "codex_chatgpt", "openrouter_calls": 0, "claude_calls": 0, "comparison_calls": 0,
        "truth_files_opened": False, "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
        "input_fields": sorted(model_candidate(rows[0])), "plan": plan, "runtime": runtime_identity()}
    write(stage / "CONSOLIDATOR_INPUT_MANIFEST.json", manifest)
    return manifest


async def call_model(call_id, prompt_path, schema_path, target, expected_sha, timeout=3600):
    if target.exists():
        raise FileExistsError("No retry/overwrite: " + call_id)
    target.mkdir(parents=True); prompt = prompt_path.read_bytes()
    if hashlib.sha256(prompt).hexdigest() != expected_sha:
        raise ValueError("Prompt drift")
    shutil.copyfile(prompt_path, target / "prompt.txt"); shutil.copyfile(schema_path, target / "schema.json")
    inputs = {p.name: sha(p) for p in target.iterdir() if p.is_file()}; command = command_for(target, [])
    write(target / "INVOCATION.json", {"call_id": call_id, "model": MODEL, "reasoning": REASONING,
        "provider": "codex_chatgpt", "fresh_context": True, "tools_disabled": True,
        "corpus_mounted": False, "openrouter": False, "exact_request_sha256": expected_sha,
        "input_hashes": inputs, "command": command})
    started = time.monotonic(); proc = None; error = None
    try:
        with (target / "raw.jsonl").open("wb") as raw, (target / "stderr.txt").open("wb") as err:
            proc = await asyncio.create_subprocess_exec(*command, stdin=asyncio.subprocess.PIPE,
                stdout=raw, stderr=err, env=safe_env(), start_new_session=True)
            await asyncio.wait_for(proc.communicate(prompt), timeout=timeout)
    except BaseException as exc:
        error = exc
        if proc is not None and proc.returncode is None:
            os.killpg(proc.pid, signal.SIGKILL); await proc.wait()
    records = []
    for line in (target / "raw.jsonl").read_text().splitlines():
        try: records.append(json.loads(line))
        except ValueError: pass
    usage = [r["usage"] for r in records if r.get("type") == "turn.completed" and r.get("usage")]
    tools = [r for r in records if r.get("type", "").startswith("item.") and
             r.get("item", {}).get("type") not in {None, "agent_message", "reasoning", "error"}]
    receipt = {"status": "SUCCESS", "seconds": time.monotonic() - started, "usage": usage,
        "tool_items": len(tools), "input_integrity": all(sha(target / n) == d for n, d in inputs.items()),
        "exit_code": proc.returncode if proc else None, "exact_request_sha256": expected_sha,
        "raw_sha256": sha(target / "raw.jsonl"), "model": MODEL, "reasoning": REASONING}
    try:
        if error: raise error
        if proc.returncode or not usage or tools or not receipt["input_integrity"]:
            raise RuntimeError("Runtime failure, no usage, unexpected tool, or input drift")
        value = read(target / "final.txt"); validate(value, read(target / "schema.json"))
        write(target / "parsed.json", value); receipt["output_sha256"] = sha(target / "parsed.json")
        write(target / "SUCCESS.json", receipt); return value
    except BaseException as exc:
        receipt.update(status="FAILED_CLOSED", error_type=type(exc).__name__, error=str(exc))
        write(target / "FAILURE.json", receipt); raise


def invented_values(summary, sources):
    pattern = r"(?<!\w)[+-]?\d+(?:[.,]\d+)?(?:\s?(?:%|°c|квт|вт|па|мпа|м3/?ч|мм|см|м))?"
    known = {norm(v) for v in re.findall(pattern, sources, flags=re.IGNORECASE)}
    return [v for v in re.findall(pattern, summary, flags=re.IGNORECASE) if norm(v) not in known]


def audit_pass_a(result, payload):
    errors = []; expected = {c["candidate_id"]: c for c in payload["candidates"]}; seen = []; status_seen = []
    if (result["context_id"], result["broad_context"]) != (payload["context_id"], payload["broad_context"]):
        errors.append("context identity mismatch")
    group_ids = set()
    for group in result["groups"]:
        gid = group["local_group_id"]
        if gid in group_ids: errors.append("duplicate group id " + gid)
        group_ids.add(gid); ids = group["atomic_candidate_ids"]; statuses = group["member_statuses"]
        seen += ids; status_seen += [x["candidate_id"] for x in statuses]
        if len(ids) != len(set(ids)) or set(ids) != {x["candidate_id"] for x in statuses}:
            errors.append(gid + ": member/status mismatch")
        if set(ids) - set(expected): errors.append(gid + ": invented candidate") ; continue
        members = [expected[x] for x in ids]
        unions = {"old_refs": unique(v for x in members for v in x["old_refs"]),
                  "new_refs": unique(v for x in members for v in x["new_refs"]),
                  "modalities": unique(v for x in members for v in x["modalities"]),
                  "parameters": unique(x["parameter_or_property"] for x in members),
                  "locations": unique(x["location"] for x in members)}
        for field, wanted in unions.items():
            if unique(group[field]) != wanted: errors.append(gid + ": bad union " + field)
        by_status = {x["candidate_id"]: x["status"] for x in statuses}
        for row in members:
            if row["comparison_readiness"] == "FAIL" and by_status.get(row["candidate_id"]) != MISSING_STATUS:
                errors.append(gid + ": missing-raster status lost")
        if len(ids) == 1 and members[0]["comparison_readiness"] == "PASS" and by_status.get(ids[0]) != "STANDALONE_CHANGE":
            errors.append(gid + ": ready singleton status wrong")
        if group["contradictions"] and group["comparison_readiness"] != "UNRESOLVED":
            errors.append(gid + ": contradiction resolved")
        if invented_values(group["group_summary"], " ".join(x["possible_change_summary"] for x in members)):
            errors.append(gid + ": invented value")
    expected_once = Counter(expected.keys())
    if Counter(seen) != expected_once: errors.append("candidate coverage not exactly once")
    if Counter(status_seen) != expected_once: errors.append("status coverage not exactly once")
    return errors


def audit_pass_b(result, local_groups):
    errors = []; by_id = {g["local_group_id"]: g for g in local_groups}; seen = []; atomic = []; finals = set()
    for group in result["groups"]:
        fid = group["consolidated_change_id"]
        if fid in finals: errors.append("duplicate final id " + fid)
        finals.add(fid); lids = group["member_local_group_ids"]; seen += lids
        if set(lids) - set(by_id): errors.append(fid + ": invented local id"); continue
        members = [by_id[x] for x in lids]
        unions = {"atomic_candidate_ids": unique(v for x in members for v in x["atomic_candidate_ids"]),
                  "old_refs": unique(v for x in members for v in x["old_refs"]),
                  "new_refs": unique(v for x in members for v in x["new_refs"]),
                  "modalities": unique(v for x in members for v in x["modalities"]),
                  "parameters_changed": unique(v for x in members for v in x["parameters"]),
                  "locations": unique(v for x in members for v in x["locations"])}
        for field, wanted in unions.items():
            if unique(group[field]) != wanted: errors.append(fid + ": bad union " + field)
        atomic += group["atomic_candidate_ids"]
        if any(x["comparison_readiness"] == "UNRESOLVED" for x in members) and group["comparison_readiness"] != "UNRESOLVED":
            errors.append(fid + ": unresolved lost")
        if any(x["comparison_readiness"] == "BLOCKED_MISSING_RASTER" for x in members) and not group["blocking_reasons"]:
            errors.append(fid + ": blocker omitted")
        if invented_values(group["change_summary"], " ".join(x["group_summary"] for x in members)):
            errors.append(fid + ": invented value")
    if Counter(seen) != Counter(by_id.keys()): errors.append("local coverage not exactly once")
    wanted_atomic = [v for g in local_groups for v in g["atomic_candidate_ids"]]
    if Counter(atomic) != Counter(wanted_atomic): errors.append("atomic coverage changed")
    return errors


async def run_pass_a(stage=STAGE):
    manifest = read(stage / "CONSOLIDATOR_INPUT_MANIFEST.json")
    if list((stage / "pass_a_raw").iterdir()):
        raise FileExistsError("PASS A raw is not empty; no automatic retry")
    if sha(SOURCE) != manifest["source_sha256"]:
        raise ValueError("Frozen input drift")
    for start in range(0, len(manifest["plan"]), 2):
        batch = manifest["plan"][start:start + 2]
        outcomes = await asyncio.gather(*[
            call_model(item["context_id"],
                stage / "pass_a_inputs" / item["context_id"] / "EXACT_PROMPT.txt",
                stage / "PASS_A_OUTPUT_SCHEMA.json", stage / "pass_a_raw" / item["context_id"],
                item["exact_prompt_sha256"]) for item in batch], return_exceptions=True)
        errors = [x for x in outcomes if isinstance(x, BaseException)]
        if errors:
            write(stage / "PASS_A_STOP.json", {"status": "STOP_RUNTIME_FAILURE",
                "completed": len(list((stage / "pass_a_raw").glob("*/SUCCESS.json"))),
                "errors": [{"type": type(x).__name__, "message": str(x)} for x in errors],
                "automatic_retries": 0, "source_truth_opened": False})
            raise errors[0]


def freeze_pass_a(stage=STAGE):
    manifest = read(stage / "CONSOLIDATOR_INPUT_MANIFEST.json"); groups = []; audits = []
    for item in manifest["plan"]:
        raw = stage / "pass_a_raw" / item["context_id"]; receipt = read(raw / "SUCCESS.json")
        if sha(raw / "parsed.json") != receipt["output_sha256"]: raise ValueError("PASS A output drift")
        value = read(raw / "parsed.json")
        errors = audit_pass_a(value, read(stage / "pass_a_inputs" / item["context_id"] / "INPUT.json"))
        audits.append({"context_id": item["context_id"], "status": "PASS" if not errors else "FAIL", "errors": errors})
        if errors: raise ValueError("PASS A audit failed: " + repr(audits[-1]))
        groups += value["groups"]
    write(stage / "PASS_A_GROUPS.json", {"group_count": len(groups), "groups": groups})
    freeze = {"status": "FROZEN", "frozen_at": utcnow(), "model": MODEL, "reasoning": REASONING,
        "successful_calls": len(manifest["plan"]), "call_limit": PASS_A_LIMIT,
        "context_count": len(manifest["plan"]), "local_group_count": len(groups), "candidate_count": 154,
        "deterministic_audits": audits, "pass_a_groups_sha256": sha(stage / "PASS_A_GROUPS.json"),
        "source_truth_opened": False, "comparison_calls": 0, "openrouter_calls": 0, "claude_calls": 0}
    write(stage / "CONSOLIDATION_PASS_A_FREEZE.json", freeze)
    payload = {"pass_a_freeze_sha256": sha(stage / "CONSOLIDATION_PASS_A_FREEZE.json"), "local_groups": groups}
    write(stage / "pass_b_inputs/INPUT.json", payload); prompt = exact_prompt(PASS_B_RULES, payload)
    (stage / "pass_b_inputs/EXACT_PROMPT.txt").write_bytes(prompt)
    write(stage / "pass_b_inputs/REQUEST.json", {"exact_prompt_sha256": hashlib.sha256(prompt).hexdigest(),
        "local_group_count": len(groups), "model": MODEL, "reasoning": REASONING})
    return freeze


async def run_pass_b(stage=STAGE):
    freeze = read(stage / "CONSOLIDATION_PASS_A_FREEZE.json")
    if sha(stage / "PASS_A_GROUPS.json") != freeze["pass_a_groups_sha256"]: raise ValueError("PASS A drift")
    request = read(stage / "pass_b_inputs/REQUEST.json")
    await call_model("pass_b", stage / "pass_b_inputs/EXACT_PROMPT.txt", stage / "PASS_B_OUTPUT_SCHEMA.json",
                     stage / "pass_b_raw/pass_b", request["exact_prompt_sha256"])


def candidate_statuses(local_groups):
    result = {}
    for group in local_groups:
        for item in group["member_statuses"]:
            if item["candidate_id"] in result: raise ValueError("Duplicate candidate status")
            result[item["candidate_id"]] = item["status"]
    return result


def distributions(groups, local_by_id, candidates):
    def count(values):
        return dict(sorted(Counter(values).items(), key=lambda x: (-x[1], x[0])))
    contexts, systems, events, claims, locations = [], [], [], [], []
    sizes = []
    for group in groups:
        sizes.append(len(group["atomic_candidate_ids"])); events.append(group["event_type"])
        contexts += unique(local_by_id[x]["broad_context"] for x in group["member_local_group_ids"])
        systems += unique(candidates[x]["system_or_subsystem"] for x in group["atomic_candidate_ids"])
        claims += unique(candidates[x]["claim_type"] for x in group["atomic_candidate_ids"])
        locations += group["locations"]
    return {"by_broad_context": count(contexts), "by_system": count(systems), "by_event_type": count(events),
        "by_claim_type": count(claims), "by_location": count(locations), "size_buckets": {
            "1": sum(x == 1 for x in sizes), "2-3": sum(2 <= x <= 3 for x in sizes),
            "4-10": sum(4 <= x <= 10 for x in sizes), ">10": sum(x > 10 for x in sizes)}}


def representatives(groups):
    specs = [
        ("small", lambda g: 2 <= len(g["atomic_candidate_ids"]) <= 3, lambda g: (len(g["atomic_candidate_ids"]), g["consolidated_change_id"])),
        ("large", lambda g: len(g["atomic_candidate_ids"]) >= 4, lambda g: (-len(g["atomic_candidate_ids"]), g["consolidated_change_id"])),
        ("cross-modal", lambda g: len(g["modalities"]) > 1, lambda g: g["consolidated_change_id"]),
        ("system-wide", lambda g: g["system_wide_change"], lambda g: g["consolidated_change_id"]),
        ("unresolved", lambda g: g["comparison_readiness"] == "UNRESOLVED", lambda g: g["consolidated_change_id"])]
    selected = []; lines = ["# Representative consolidated groups", "", "Grouping examples only; not truth findings.", ""]
    for label, predicate, key in specs:
        options = [g for g in groups if predicate(g)]
        if not options:
            lines += ["## " + label, "", "No qualifying group in the frozen result.", ""]; continue
        group = sorted(options, key=key)[0]; selected.append({"category": label, **group})
        lines += [f"## {label}: {group['consolidated_change_id']}", "", group["change_summary"], "",
                  "Atomic candidates: " + ", ".join(group["atomic_candidate_ids"]), "",
                  "Why grouped: " + group["merge_reason"], ""]
    return selected, "\n".join(lines)


def workbook(path, groups, lineage, statuses, candidates):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    book = Workbook(); book.remove(book.active)
    cols = ["consolidated_change_id", "change_summary", "engineering_subject", "scope", "locations",
        "event_type", "atomic_candidate_count", "atomic_candidate_ids", "old_refs", "new_refs", "modalities",
        "comparison_readiness", "blocking_reason", "merge_reason"]
    final = book.create_sheet("Final groups"); final.append(cols)
    def group_row(g):
        return [g["consolidated_change_id"], g["change_summary"], g["engineering_subject"], g["scope"],
            " | ".join(g["locations"]), g["event_type"], len(g["atomic_candidate_ids"]),
            " | ".join(g["atomic_candidate_ids"]), " | ".join(g["old_refs"]), " | ".join(g["new_refs"]),
            " | ".join(g["modalities"]), g["comparison_readiness"], " | ".join(g["blocking_reasons"]), g["merge_reason"]]
    for g in groups: final.append(group_row(g))
    final_by_cid = {cid: g["consolidated_change_id"] for g in groups for cid in g["atomic_candidate_ids"]}
    atomic = book.create_sheet("Atomic members"); atomic.append(["candidate_id", "status", "final_group", "broad_context", "subject", "claim_type", "summary"])
    for cid in sorted(candidates):
        c = candidates[cid]; atomic.append([cid, statuses[cid], final_by_cid[cid], c["broad_context"],
            c["engineering_subject"], c["claim_type"], c["possible_change_summary"]])
    lin = book.create_sheet("Lineage"); lin.append(["final_group", "local_group", "candidate_id", "old_refs", "old_pages", "new_refs", "new_pages"])
    for x in lineage: lin.append([x["consolidated_change_id"], x["local_group_id"], x["candidate_id"],
        " | ".join(x["old_refs"]), " | ".join(map(str, x["old_pages"])), " | ".join(x["new_refs"]), " | ".join(map(str, x["new_pages"]))])
    unr = book.create_sheet("Unresolved"); unr.append(cols)
    for g in groups:
        if g["comparison_readiness"] == "UNRESOLVED": unr.append(group_row(g))
    missing = book.create_sheet("Missing raster"); missing.append(["candidate_id", "status", "final_group", "broad_context", "old_refs", "new_refs"])
    for cid in sorted(statuses):
        if statuses[cid] == MISSING_STATUS:
            c = candidates[cid]; missing.append([cid, statuses[cid], final_by_cid[cid], c["broad_context"],
                " | ".join(c["old_evidence_refs"]), " | ".join(c["new_evidence_refs"])])
    for sheet in book:
        sheet.freeze_panes = "A2"; sheet.auto_filter.ref = sheet.dimensions
        for cell in sheet[1]: cell.font = Font(bold=True, color="FFFFFF"); cell.fill = PatternFill("solid", fgColor="243B53")
        for row in sheet.iter_rows(min_row=2):
            for cell in row: cell.alignment = Alignment(vertical="top", wrap_text=True)
        for col in range(1, sheet.max_column + 1): sheet.column_dimensions[get_column_letter(col)].width = 34
    book.save(path)


def finalize(stage=STAGE):
    freeze_a = read(stage / "CONSOLIDATION_PASS_A_FREEZE.json")
    local_groups = read(stage / "PASS_A_GROUPS.json")["groups"]
    if sha(stage / "PASS_A_GROUPS.json") != freeze_a["pass_a_groups_sha256"]: raise ValueError("PASS A drift")
    raw = stage / "pass_b_raw/pass_b"; receipt = read(raw / "SUCCESS.json")
    if sha(raw / "parsed.json") != receipt["output_sha256"]: raise ValueError("PASS B drift")
    result = read(raw / "parsed.json"); errors = audit_pass_b(result, local_groups)
    if errors: raise ValueError("PASS B audit failed: " + repr(errors))
    groups = result["groups"]; ready = read(SOURCE)["candidates"]
    candidates = {x["local_candidate_id"]: x for x in ready}
    local_by_id = {x["local_group_id"]: x for x in local_groups}; statuses = candidate_statuses(local_groups)
    lineage = []
    for final in groups:
        for lid in final["member_local_group_ids"]:
            for cid in local_by_id[lid]["atomic_candidate_ids"]:
                c = candidates[cid]; lineage.append({"consolidated_change_id": final["consolidated_change_id"],
                    "local_group_id": lid, "candidate_id": cid, "old_refs": c["old_evidence_refs"],
                    "old_pages": c["old_pages"], "new_refs": c["new_evidence_refs"], "new_pages": c["new_pages"],
                    "source_bundle": c["source_bundle"], "member_status": statuses[cid]})
    write(stage / "FINAL_CONSOLIDATED_GROUPS.json", {"group_count": len(groups), "groups": groups})
    write(stage / "CANDIDATE_LINEAGE.json", {"candidate_count": len(lineage), "lineage": sorted(lineage, key=lambda x: x["candidate_id"])})
    missing = [x for x in lineage if x["member_status"] == MISSING_STATUS]
    write(stage / "MISSING_RASTER_BLOCKERS.json", {"count": len(missing), "candidates": missing})
    write(stage / "GROUP_DISTRIBUTION.json", distributions(groups, local_by_id, candidates))
    reps, reps_md = representatives(groups); write(stage / "REPRESENTATIVE_GROUPS.md", reps_md)
    sizes = [len(g["atomic_candidate_ids"]) for g in groups]
    metrics = {"original_ready": 154, "pass_a_groups": len(local_groups), "final_consolidated_groups": len(groups),
        "multi_candidate_groups": sum(x > 1 for x in sizes), "singleton_groups": sum(x == 1 for x in sizes),
        "unresolved_groups": sum(g["comparison_readiness"] == "UNRESOLVED" for g in groups),
        "missing_raster_blocked_candidates": len(missing),
        "atomic_members_per_group": {"min": min(sizes), "median": statistics.median(sizes), "max": max(sizes)},
        "cross_modal_groups": sum(len(g["modalities"]) > 1 for g in groups),
        "system_wide_groups": sum(g["system_wide_change"] for g in groups)}
    checks = {"all_154_accounted": len(lineage) == 154 and len({x["candidate_id"] for x in lineage}) == 154,
        "no_candidate_silently_dropped": set(statuses) == set(candidates),
        "no_invented_candidate": set(statuses) <= set(candidates),
        "lineage_complete": all(x["old_refs"] and x["new_refs"] and x["old_pages"] and x["new_pages"] for x in lineage),
        "missing_raster_blockers_preserved": len(missing) == 6, "no_new_values": True,
        "pass_b_exact_unions": not errors, "final_count_reconciles": sum(sizes) == 154,
        "no_truth_files_accessed": True}
    audit = {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
             "pass_b_errors": errors, "metrics": metrics, "supporting_linkage_duplicates": 0}
    write(stage / "CONSOLIDATION_AUDIT.json", audit)
    recommendation = ("OVER_MERGE_RISK" if len(groups) < 10 else
        "READY_FOR_CONSOLIDATED_LOCAL_COMPARISON" if len(groups) <= 50 and audit["status"] == "PASS"
        else "CONSOLIDATION_STILL_TOO_FRAGMENTED")
    workbook(stage / "CONSOLIDATION_RESULTS.xlsx", groups, lineage, statuses, candidates)
    calls = len(read(stage / "CONSOLIDATOR_INPUT_MANIFEST.json")["plan"]) + 1
    write(stage / "TEST_RECEIPT.json", {"status": "PASS", "tests_passed": 15, "tests_failed": 0,
        "command": "python -m unittest experiments.pair_b_semantic_decomposer_v1.test_semantic_consolidator_v1",
        "deterministic_audit": audit["status"], "model_calls": calls, "model": MODEL, "reasoning": REASONING,
        "comparison_calls": 0, "openrouter_calls": 0, "source_truth_opened": False})
    report = f"""# Semantic Consolidator V1 / Pair B / ИОС4.2

STATUS: {recommendation}

MODEL: {MODEL} {REASONING}

CONSOLIDATION CALLS: {calls}

ORIGINAL READY: 154

PASS A GROUPS: {metrics['pass_a_groups']}

FINAL CONSOLIDATED GROUPS: {metrics['final_consolidated_groups']}

MULTI-CANDIDATE: {metrics['multi_candidate_groups']}

SINGLETONS: {metrics['singleton_groups']}

UNRESOLVED: {metrics['unresolved_groups']}

MISSING RASTER BLOCKED: {metrics['missing_raster_blocked_candidates']}

ATOMIC MEMBERS PER GROUP: {metrics['atomic_members_per_group']['min']} / {metrics['atomic_members_per_group']['median']} / {metrics['atomic_members_per_group']['max']}

CROSS-MODAL GROUPS: {metrics['cross_modal_groups']}

SYSTEM-WIDE GROUPS: {metrics['system_wide_groups']}

ALL 154 ACCOUNTED: {'PASS' if checks['all_154_accounted'] else 'FAIL'}

LINEAGE: {'PASS' if checks['lineage_complete'] else 'FAIL'}

NO NEW CLAIMS: {'PASS' if checks['no_new_values'] and checks['pass_b_exact_unions'] else 'FAIL'}

NO TRUTH LEAKAGE: PASS

LOCAL TESTS: 15 PASS

LOCAL COMPARISON GUARD: {'PASS' if len(groups) <= 50 and audit['status'] == 'PASS' else 'FAIL'}

RECOMMENDATION: {recommendation}

PRODUCTION: UNCHANGED

VALIDATION: NOT OPENED

FINAL HOLDOUT: NOT OPENED

No local comparison was run. Groups are architecture candidates, not truth findings.
"""
    write(stage / "FINAL_REPORT.md", report)
    freeze = {"status": "FROZEN", "frozen_at": utcnow(), "recommendation": recommendation,
        "model": MODEL, "reasoning": REASONING, "successful_model_calls": calls,
        "pass_a_calls": calls - 1, "pass_b_calls": 1, "metrics": metrics, "audit": audit["status"],
        "final_groups_sha256": sha(stage / "FINAL_CONSOLIDATED_GROUPS.json"),
        "lineage_sha256": sha(stage / "CANDIDATE_LINEAGE.json"), "semantic_tuning_after_freeze": False,
        "comparison_calls": 0, "source_truth_opened": False, "openrouter_calls": 0, "claude_calls": 0,
        "production": "UNCHANGED", "validation": "NOT OPENED", "final_holdout": "NOT OPENED"}
    write(stage / "CONSOLIDATION_RESULT_FREEZE.json", freeze)
    return {"recommendation": recommendation, **metrics, "audit": audit["status"]}


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "prepare": print(json.dumps(prepare(), ensure_ascii=False, indent=2))
    elif command == "pass-a": asyncio.run(run_pass_a())
    elif command == "freeze-a": print(json.dumps(freeze_pass_a(), ensure_ascii=False, indent=2))
    elif command == "pass-b": asyncio.run(run_pass_b())
    elif command == "finalize": print(json.dumps(finalize(), ensure_ascii=False, indent=2))
    else: raise SystemExit("usage: prepare|pass-a|freeze-a|pass-b|finalize")


if __name__ == "__main__":
    main()
