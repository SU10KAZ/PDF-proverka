#!/usr/bin/env python3
"""Oversize V3 transport: research audit, model-free test, Pair A preflight.

Zero model calls.  The research side is read only, from the frozen V3
experiment (recover script source + ``miner_raw`` receipts); evaluation,
truth, validation and holdout artifacts are never opened.  The Codex CLI is
touched only for its offline protocol-schema generator; every provider path is
exercised against ``backend/tests/project_change_v3/fake_codex.py``.

    python scripts/audit_projectchange_v3_oversize_transport.py --out-dir DIR [--work SCRATCH]

Writes OVERSIZE_TRANSPORT_AUDIT.json, OVERSIZE_TRANSPORT_TEST.json and
PAIR_A_OVERSIZE_PREFLIGHT.json.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from backend.app.services.project_change_v3 import contracts, source_prep, transport  # noqa: E402
from backend.app.services.project_change_v3.provider import CodexProvider, ProviderError, build_codex_payload  # noqa: E402
from scripts.audit_projectchange_v3_source_parity import FROZEN, _norm_text, _read  # noqa: E402

RECOVER = REPO / "experiments/project_change_272/ai_first_semantic_mapping_projectchange_v3_pair_a_recover.py"
FAKE = REPO / "backend/tests/project_change_v3/fake_codex.py"
REGIONS = ("A-R003", "A-R011", "A-R022")
LIMIT = transport.CODEX_TURN_MAX_CHARS


def sha_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> Any:
    return _read(path) if path.exists() else None


def codex_binary() -> str:
    """The installed CLI, found WITHOUT PATH (PATH may hold a tripwire stand-in).

    Used only for ``--version`` and the offline protocol-schema generator.
    """
    candidates = sorted(Path("/home/coder/.vscode-server/extensions").glob("openai.chatgpt-*/bin/*/codex"), reverse=True)
    if not candidates:
        raise SystemExit("installed codex CLI not found")
    return str(candidates[0])


# ── 1. Research audit ───────────────────────────────────────────────────────

def research_code_facts() -> dict[str, Any]:
    source = RECOVER.read_text(encoding="utf-8")
    tree = ast.parse(source)
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in {"CHUNK_CHARS", "STREAM_LIMIT", "OVERSIZED", "STANDARD_APP_SERVER"}:
                constants[name] = ast.literal_eval(node.value) if not isinstance(node.value, ast.BinOp) else eval(  # noqa: S307
                    compile(ast.Expression(node.value), "c", "eval"))
    func = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "injected_call")
    sequence, params = [], {}
    for node in ast.walk(func):
        if isinstance(node, ast.Dict):
            keys = [k.value for k in node.keys if isinstance(k, ast.Constant)]
            if "method" in keys:
                method = node.values[keys.index("method")]
                if isinstance(method, ast.Constant):
                    sequence.append((node.lineno, method.value))
                    if "params" in keys:
                        value = node.values[keys.index("params")]
                        if isinstance(value, ast.Dict):
                            params[method.value] = sorted(k.value for k in value.keys if isinstance(k, ast.Constant))
    return {
        "file": str(RECOVER.relative_to(REPO)),
        "git_blob": subprocess.run(["git", "-C", str(REPO), "rev-parse", f"HEAD:{RECOVER.relative_to(REPO)}"],
                                   capture_output=True, text=True).stdout.strip(),
        "sha256": sha_file(RECOVER),
        "constants": {k: sorted(v) if isinstance(v, set) else v for k, v in constants.items()},
        "protocol_sequence": [m for _l, m in sorted(sequence)],
        "request_params": params,
        "split_rule": "payload[start:start+CHUNK_CHARS] for start in range(0, len(payload), CHUNK_CHARS); "
                      "''.join(chunks) == payload checked before any call",
        "history_item_shape": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<chunk>"}]},
        "final_turn_input": "[{type: text, text: <last chunk>}] + [{type: localImage, path: /work/image_NNN.png} per sha-unique image]",
        "isolation": "bwrap --unshare-all --share-net; read-only codex binary and auth.json; empty CODEX_HOME otherwise",
    }


def receipts_for(region: str) -> list[dict[str, Any]]:
    rows = []
    for target in sorted((FROZEN / "miner_raw").glob(f"PAIR_A_{region}*")):
        row: dict[str, Any] = {"call_dir": target.name}
        invocation = load_json(target / "INVOCATION.json") or {}
        command = invocation.get("command") or []
        row["cli_mode"] = next((c for c in command if c in {"exec", "app-server"}), None)
        prompt = target / "prompt.txt"
        if prompt.exists():
            text = prompt.read_text(encoding="utf-8")
            row.update(prompt_chars=len(text), prompt_sha256=sha_text(text))
        transport_receipt = load_json(target / "TRANSPORT.json")
        if transport_receipt:
            row["transport"] = {k: transport_receipt[k] for k in (
                "transport", "reason", "chunk_chars", "chunk_sha256", "concatenation_sha256", "frozen_prompt_sha256",
                "lossless_concatenation_equal", "prompt_changed", "evidence_changed", "schema_changed")}
        receipt = load_json(target / "RECEIPT.json")
        if receipt:
            row["receipt"] = {k: receipt.get(k) for k in ("exit_code", "turn_status", "transport", "usage", "tool_items")}
        failure = load_json(target / "TECHNICAL_FAILURE.json")
        if failure:
            row["technical_failure"] = {k: failure.get(k) for k in ("status", "reason", "result_accepted")}
        stderr = target / "stderr.txt"
        if stderr.exists() and "input_too_large" in stderr.read_text(encoding="utf-8", errors="replace"):
            line = next(l for l in stderr.read_text(encoding="utf-8", errors="replace").splitlines() if "input_too_large" in l)
            row["provider_rejection"] = line.strip()
        raw = target / "raw.jsonl"
        if raw.exists():
            errors = []
            for line in raw.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if "id" in message and "error" in message:
                    errors.append({"id": message["id"], "error": message["error"]})
                if message.get("id") == 1 and "result" in message:
                    row["cli_user_agent"] = message["result"].get("userAgent")
            if errors:
                row["jsonrpc_errors"] = errors
        rows.append(row)
    return rows


def protocol_schema(binary: str) -> dict[str, Any]:
    out = {}
    for experimental in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            command = [binary, "app-server", "generate-json-schema", "--out", tmp] + (["--experimental"] if experimental else [])
            subprocess.run(command, check=True, capture_output=True, timeout=120)
            path = Path(tmp) / "ClientRequest.json"
            schema = json.loads(path.read_text(encoding="utf-8"))
            definitions = schema["definitions"]
            methods = [m for option in schema.get("oneOf", [])
                       for m in option.get("properties", {}).get("method", {}).get("enum", [])]
            out["experimental" if experimental else "stable"] = {
                "client_request_schema_sha256": sha_file(path),
                "has_thread_inject_items": "thread/inject_items" in methods,
                "inject_items_description": definitions["ThreadInjectItemsParams"]["properties"]["items"].get("description"),
                "turn_start_has_cyberAccessProgram": "cyberAccessProgram" in definitions["TurnStartParams"]["properties"],
                "turn_start_has_outputSchema": "outputSchema" in definitions["TurnStartParams"]["properties"],
            }
    version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=30).stdout.strip()
    return {"cli": binary, "cli_version": version, "offline_schema_generator": "codex app-server generate-json-schema",
            **out}


def schema_validator_runtime() -> list[dict[str, Any]]:
    """The provider re-validates every answer with jsonschema; is it importable in production?"""
    python = Path("/home/coder/auditmanager/current/venv/bin/python")
    probe = subprocess.run([str(python), "-c", "import jsonschema"], capture_output=True, text=True, timeout=60)
    requirement = next((line.strip() for line in (REPO / "requirements.txt").read_text(encoding="utf-8").splitlines()
                        if line.strip().lower().startswith("jsonschema")), None)
    if probe.returncode == 0:
        return []
    return [{
        "id": "JSONSCHEMA_MISSING_IN_PRODUCTION_VENV",
        "evidence": {"interpreter": str(python), "import_error": probe.stderr.strip().splitlines()[-1],
                     "requirements_txt": requirement,
                     "research_source": "controlled_inference_f1_f4_f2_v3/runtime_deps (jsonschema 4.23.0 vendored)"},
        "effect_before_fix": "every live V3 call would fail AFTER a paid model call (schema_invalid: No module named "
                             "'jsonschema')",
        "fix_in_this_change": "CodexProvider refuses with schema_validator_unavailable BEFORE contacting the model",
        "remaining_action": "install jsonschema (requirements.txt) into the release venv in a reviewed release build; "
                            "the release builder clones the venv without pip install",
    }]


def audit(binary: str) -> dict[str, Any]:
    code = research_code_facts()
    regions = {r: receipts_for(r) for r in (*REGIONS, "A-R017")}
    evidence = {}
    for region in REGIONS:
        accepted = [r for r in regions[region] if (r.get("receipt") or {}).get("turn_status") == "completed"]
        success = accepted[-1] if accepted else None
        if not success:
            evidence[region] = {"accepted_call": None}
            continue
        chunks = success["transport"]["chunk_chars"]
        usage = success["receipt"]["usage"][0]
        evidence[region] = {
            "accepted_call": success["call_dir"],
            "prompt_chars": success["prompt_chars"],
            "chunks": chunks,
            "lossless": success["transport"]["concatenation_sha256"] == success["transport"]["frozen_prompt_sha256"]
            == success["prompt_sha256"],
            "input_tokens": usage["input_tokens"],
            "final_chunk_chars": chunks[-1],
            # Billed input tokens are consistent only with the WHOLE payload
            # being model-visible: ~7 characters per token for all three
            # regions, versus an implausible 0.2–2.4 if only the final chunk
            # had reached the model.
            "chars_per_input_token_if_full_payload": round(success["prompt_chars"] / usage["input_tokens"], 2),
            "chars_per_input_token_if_final_chunk_only": round(chunks[-1] / usage["input_tokens"], 2),
        }
    base = next((r for r in regions["A-R003"] if r["call_dir"] == "PAIR_A_A-R003"), {})
    split = next((r for r in regions["A-R003"] if r["call_dir"].endswith("_SPLIT")), {})
    return {
        "schema": "projectchange-v3-oversize-transport-audit/1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_calls": 0,
        "research_code": code,
        "research_receipts": regions,
        "findings": {
            "limit": {
                "value_chars": LIMIT,
                "unit": "Unicode code points of user text in one turn (actual_chars equals Python len of the prompt)",
                "evidence": base.get("provider_rejection"),
                "prompt_chars_python_len": base.get("prompt_chars"),
            },
            "rejected_variants": [
                {"variant": "codex exec, single stdin prompt", "call": "PAIR_A_A-R003", "result": "input_too_large"},
                {"variant": "app-server, all chunks as ordered text items of ONE turn/start", "call": split.get("call_dir"),
                 "result": (split.get("jsonrpc_errors") or [{}])[0].get("error")},
            ],
            "accepted_variant": "app-server thread/inject_items (leading chunks as user messages in model-visible "
                                "history) + turn/start (last chunk + images + outputSchema)",
            "accepted_calls": evidence,
            "research_transport_side_issues": [
                "asyncio StreamReader 64 KiB line limit broke the first A-R011/A-R017 attempts (fixed with a 16 MiB limit)",
                "A-R017 is not oversize: codex exec automatic cyber treatment was rejected; app-server single item with "
                "cyberAccessProgram=standard was used",
                "provenance-validator rejections were retried with the same input (_RETRY_n / _INJECT_n)",
            ],
        },
        "protocol": protocol_schema(binary),
        "pre_inference_blockers_found": schema_validator_runtime(),
        "production_equivalent": {
            "module": "backend/app/services/project_change_v3/transport.py",
            "gateway": "backend/app/services/stage_comparison/ai/gateway.py:call_codex_app_server",
            "provider": "backend/app/services/project_change_v3/provider.py:CodexProvider.complete",
            "provider_transport_version": transport.PROVIDER_TRANSPORT_VERSION,
            "same_as_research": [
                f"threshold {LIMIT} characters; above it the split transport, never truncation",
                f"chunk size {transport.CHUNK_CHARS}, exact slicing, join == payload verified before sending",
                "thread/inject_items with user input_text messages, then turn/start with the last chunk + localImage",
                "ephemeral thread, sandbox read-only, approvalPolicy never, model/effort, outputSchema",
                f"cyberAccessProgram={transport.OVERSIZE_CYBER_ACCESS_PROGRAM}",
                "any tool item or failed turn fails the call",
            ],
            "differences": [
                "no bubblewrap: the same isolation as the production codex exec path (clean env allow-list, empty temp cwd, "
                "read-only sandbox, production CODEX_DISABLED_FEATURES, mcp_servers={}, web_search disabled, "
                "project_doc_max_bytes=0)",
                "service tier not forced to priority (production codex exec does not force it either)",
                "JSON lines read without a line-length limit (the research 64 KiB failure cannot occur)",
                "server-initiated requests are answered with an error (nothing can be approved)",
                "the text is rebuilt from the serialized JSON-RPC lines and compared by sha256 BEFORE the process starts "
                "and again with the real thread id before sending",
                "no automatic retry (the enablement contract keeps retries=0; unchanged)",
            ],
            "provenance_fields": ["provider_transport_version", "transport", "oversize_transport_used",
                                  "model_visible_payload_sha256", "model_visible_payload_size",
                                  "model_visible_payload_bytes", "chunk_chars", "chunk_sha256", "image_sha256",
                                  "wire.wire_text_sha256"],
        },
    }


# ── 2. Model-free synthetic test ────────────────────────────────────────────

class FakeCodex:
    def __init__(self, root: Path):
        self.root = root
        self.log = root / "fake_codex.log"
        self.answer = root / "answer.json"
        self.answer.write_text(json.dumps({"pair": "P", "ok": True}), encoding="utf-8")
        self.saved: dict[str, str | None] = {}

    def __enter__(self):
        values = {"STAGE_COMPARISON_AI_CODEX_BIN": str(FAKE),
                  "STAGE_COMPARISON_AI_ENV_ALLOWLIST": "FAKE_CODEX_LOG,FAKE_CODEX_RESPONSE,FAKE_CODEX_MODE",
                  "FAKE_CODEX_LOG": str(self.log), "FAKE_CODEX_RESPONSE": str(self.answer), "FAKE_CODEX_MODE": None}
        for key, value in values.items():
            self.saved[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return self

    def __exit__(self, *exc):
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def mode(self, value: str | None) -> None:
        if value:
            os.environ["FAKE_CODEX_MODE"] = value
        else:
            os.environ.pop("FAKE_CODEX_MODE", None)

    def records(self) -> list[dict[str, Any]]:
        if not self.log.exists():
            return []
        rows = [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]
        self.log.unlink()
        return rows


def ensure_schema_validator() -> str:
    try:
        import jsonschema  # noqa: F401
        return "jsonschema"
    except ImportError:
        def validate(value, schema):
            missing = [k for k in schema.get("required", []) if k not in value]
            if missing:
                raise ValueError(f"missing {missing}")

        sys.modules["jsonschema"] = types.SimpleNamespace(validate=validate)
        return "STUB (jsonschema is not installed in this interpreter; see BLOCKERS)"


SCHEMA = {"type": "object", "required": ["pair", "ok"], "properties": {"pair": {"type": "string"}, "ok": {"type": "boolean"}}}


def synthetic(chars: int) -> str:
    unit = "Щит ВРУ-1 → 400 В; 配电; 𝛑=3,14159 | "
    return (unit * (chars // len(unit) + 1))[:chars]


def run_case(fake: FakeCodex, name: str, prompt: str, images: list[dict[str, Any]], mode: str | None = None) -> dict[str, Any]:
    fake.mode(mode)
    payload = build_codex_payload(prompt, {"pair": "P"}, images)[0]
    provider = CodexProvider()
    row: dict[str, Any] = {"case": name, "payload_chars": len(payload), "payload_sha256": sha_text(payload),
                           "over_limit": len(payload) > LIMIT, "fake_mode": mode}
    try:
        provider.complete(stage="MINING", call_id=name, pair_id="P", prompt=prompt, data={"pair": "P"},
                          schema=SCHEMA, images=images)
        row["outcome"] = "ANSWER_ACCEPTED"
    except ProviderError as exc:
        row["outcome"] = f"FAILED_CLOSED:{exc.code}"
    records = fake.records()
    receipt = provider.last_transport or {}
    row["receipt"] = {k: receipt.get(k) for k in ("provider_transport_version", "transport", "oversize_transport_used",
                                                   "model_visible_payload_sha256", "model_visible_payload_size",
                                                   "chunk_chars", "injected_history_items", "images")}
    if records:
        record = records[-1]
        row["cli_mode"] = record["mode"]
        restored = record.get("stdin_sha256") or record.get("model_visible_sha256")
        row["restored_sha256"] = restored
        row["restored_equals_original"] = restored == row["payload_sha256"]
        row["max_turn_chars_sent"] = record.get("stdin_chars") or record.get("turn_text_chars")
        row["turn_limit_respected"] = (row["max_turn_chars_sent"] or 0) <= LIMIT or record["mode"] == "exec"
        row["history_items"] = record.get("history_items", 0)
        row["images_sent"] = len(record.get("images") or [])
        row["provider_rejected_turn"] = "rejected_turn_chars" in record
    else:
        row["cli_mode"] = None
    return row


def synthetic_test(work: Path) -> dict[str, Any]:
    validator = ensure_schema_validator()
    images = []
    for i in range(4):
        path = work / f"img_{i}.png"
        path.write_bytes(b"\x89PNG synthetic %d" % (i % 3))
        images.append({"path": str(path), "label": {"block": f"B{i}"}})
    overhead = len(build_codex_payload("", {"pair": "P"}, images)[0])
    cases = []
    with FakeCodex(work) as fake:
        cases.append(run_case(fake, "standard_200k", synthetic(200_000), images))
        cases.append(run_case(fake, "boundary_exactly_limit", synthetic(LIMIT - overhead), images))
        cases.append(run_case(fake, "boundary_limit_plus_1", synthetic(LIMIT - overhead + 1), images))
        cases.append(run_case(fake, "oversize_1_7M", synthetic(1_700_000), images))
        cases.append(run_case(fake, "oversize_3_5M", synthetic(3_500_000), images))
        for mode in ("reject_inject", "turn_failed", "tool_item"):
            cases.append(run_case(fake, f"oversize_fail_{mode}", synthetic(1_300_000), images, mode))
        from backend.app.services.stage_comparison.ai.gateway import call_codex

        fake.mode(None)
        big = build_codex_payload(synthetic(1_300_000), {"pair": "P"}, images)[0]
        exec_only = call_codex(big, model="fake", schema=SCHEMA, retries=0)
        fake.records()
        cases.append({"case": "control_exec_without_split", "payload_chars": len(big), "outcome":
                      "REJECTED_input_too_large" if "input_too_large" in exec_only.raw_excerpt else exec_only.error_kind})
    ok_cases = [c for c in cases if c["case"] in {"standard_200k", "boundary_exactly_limit", "boundary_limit_plus_1",
                                                  "oversize_1_7M", "oversize_3_5M"}]
    checks = {
        "all_answers_accepted": all(c["outcome"] == "ANSWER_ACCEPTED" for c in ok_cases),
        "restored_sha_equals_original_sha": all(c.get("restored_equals_original") for c in ok_cases),
        "standard_path_for_payload_le_limit": all(c["cli_mode"] == "exec" for c in ok_cases if not c["over_limit"]),
        "oversize_path_for_payload_gt_limit": all(c["cli_mode"] == "app-server" and c["receipt"]["oversize_transport_used"]
                                                  for c in ok_cases if c["over_limit"]),
        "no_turn_over_limit": all(not c.get("provider_rejected_turn") for c in ok_cases),
        "failures_fail_closed": all(c["outcome"].startswith("FAILED_CLOSED") and c["receipt"]["oversize_transport_used"]
                                    for c in cases if c["case"].startswith("oversize_fail_")),
        "control_shows_the_limit": cases[-1]["outcome"] == "REJECTED_input_too_large",
    }
    return {"schema": "projectchange-v3-oversize-transport-test/1", "generated_at": datetime.now(timezone.utc).isoformat(),
            "model_calls": 0, "cli": "backend/tests/project_change_v3/fake_codex.py (enforces the real 1 048 576 turn limit)",
            "schema_validator": validator, "provider": "CodexProvider (production adapter, unmodified path selection)",
            "cases": cases, "checks": checks, "status": "PASS" if all(checks.values()) else "FAIL"}


# ── 3. Pair A preflight ─────────────────────────────────────────────────────

def preflight(work: Path) -> dict[str, Any]:
    freeze = _read(FROZEN / "EXPERIMENT_FREEZE.json")["pairs"]["A"]
    admission = _read(Path(freeze["source_admission"]))
    artifacts = {side: admission[side]["artifacts"] for side in ("old", "new")}
    pair_work = work / "pair_a"
    prepared = source_prep.prepare_comparison_sources(
        pair_id="A",
        old_paths={"pdf": Path(artifacts["old"]["pdf"]["path"]), "blocks": Path(artifacts["old"]["blocks"]["path"]),
                   "markdown": Path(artifacts["old"]["work_md"]["path"])},
        new_paths={"pdf": Path(artifacts["new"]["pdf"]["path"]), "blocks": Path(artifacts["new"]["blocks"]["path"]),
                   "markdown": Path(artifacts["new"]["work_md"]["path"])},
        work_dir=pair_work, object_id="oversize_preflight",
    )
    prefixes = [str(FROZEN / "source" / "pair_a"), str(pair_work / "source")]
    semantic_map = _read(FROZEN / "PAIR_A_SEMANTIC_MAP.json")
    rows = []
    with FakeCodex(work) as fake:
        for region in semantic_map["regions"]:
            if region["region_id"] not in REGIONS:
                continue
            rid = region["region_id"]
            frozen_prompt_path = FROZEN / "miner_inputs_optimized" / f"PAIR_A_{rid}" / "EXACT_PROMPT.txt"
            frozen_prompt = frozen_prompt_path.read_text(encoding="utf-8")
            data, images = source_prep.optimized_region_bundle(pair_id="A", region=region, work_dir=pair_work)
            payload, image_paths, _labels = build_codex_payload(contracts.MINER_PROMPT, data, images)
            prod_plan = transport.plan(payload)
            frozen_plan = transport.plan(frozen_prompt)
            accepted = [r for r in receipts_for(rid) if (r.get("receipt") or {}).get("turn_status") == "completed"]
            research = accepted[-1]["transport"] if accepted else {}
            delivery = run_case_real(fake, rid, data, images)
            rows.append({
                "region_id": rid,
                "frozen_exact_prompt_sha256": sha_file(frozen_prompt_path),
                "frozen_chars": len(frozen_prompt),
                "production_payload_chars": len(payload),
                "production_payload_sha256": prod_plan.payload_sha256,
                "production_equals_frozen_after_source_root_normalization":
                    _norm_text(payload, prefixes) == _norm_text(frozen_prompt, prefixes),
                "images": {"production": len(image_paths), "frozen_labels": len(json.loads(
                    frozen_prompt.split("\nIMAGES:\n", 1)[1].split("\nSOURCE DATA:\n", 1)[0]))},
                "production_plan": {"oversize": prod_plan.oversize, "chunk_chars": [len(c) for c in prod_plan.chunks],
                                    "every_turn_within_limit": all(len(c) <= LIMIT for c in prod_plan.chunks)},
                "split_of_frozen_prompt_equals_research": {
                    "chunk_chars": [len(c) for c in frozen_plan.chunks] == research.get("chunk_chars"),
                    "chunk_sha256": [transport.sha256_text(c) for c in frozen_plan.chunks] == research.get("chunk_sha256"),
                    "concatenation_sha256": frozen_plan.payload_sha256 == research.get("concatenation_sha256"),
                    "research_call": accepted[-1]["call_dir"] if accepted else None,
                },
                "dry_run_delivery_through_production_provider": delivery,
                "model_calls": 0,
            })
    checks = {
        "three_regions": [r["region_id"] for r in rows] == list(REGIONS),
        "all_oversize": all(r["production_plan"]["oversize"] for r in rows),
        "production_payload_equals_frozen": all(r["production_equals_frozen_after_source_root_normalization"] for r in rows),
        "image_counts_equal": all(r["images"]["production"] == r["images"]["frozen_labels"] for r in rows),
        "split_identical_to_research": all(all(r["split_of_frozen_prompt_equals_research"][k] for k in
                                               ("chunk_chars", "chunk_sha256", "concatenation_sha256")) for r in rows),
        "delivered_lossless": all(r["dry_run_delivery_through_production_provider"]["restored_equals_original"] for r in rows),
        "no_turn_over_limit": all(not r["dry_run_delivery_through_production_provider"]["provider_rejected_turn"] for r in rows),
    }
    return {"schema": "projectchange-v3-pair-a-oversize-preflight/1", "generated_at": datetime.now(timezone.utc).isoformat(),
            "model_calls": 0, "inference": "DISABLED — dry run against fake_codex.py only",
            "forbidden_artifacts_opened": False, "source_structure_sha256": prepared["structure_sha256"],
            "regions": rows, "checks": checks, "status": "PASS" if all(checks.values()) else "FAIL"}


def run_case_real(fake: FakeCodex, rid: str, data: Any, images: list[dict[str, Any]]) -> dict[str, Any]:
    fake.mode(None)
    fake.answer.write_text(json.dumps({"pair": "A", "region_id": rid}), encoding="utf-8")
    schema = {"type": "object", "required": ["pair", "region_id"]}
    payload = build_codex_payload(contracts.MINER_PROMPT, data, images)[0]
    provider = CodexProvider()
    provider.complete(stage="MINING", call_id=f"preflight_{rid}", pair_id="A", prompt=contracts.MINER_PROMPT,
                      data=data, schema=schema, images=images)
    (record,) = fake.records()
    receipt = provider.last_transport
    return {"cli_mode": record["mode"], "history_items": record["history_items"],
            "turn_text_chars": record["turn_text_chars"], "images_sent": len(record["images"]),
            "restored_sha256": record["model_visible_sha256"],
            "restored_equals_original": record["model_visible_sha256"] == sha_text(payload),
            "provider_rejected_turn": "rejected_turn_chars" in record,
            "receipt": {k: receipt[k] for k in ("provider_transport_version", "transport", "oversize_transport_used",
                                                "model_visible_payload_sha256", "model_visible_payload_size", "chunk_chars")}}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--work", type=Path)
    args = parser.parse_args()
    work = args.work or Path(tempfile.mkdtemp(prefix="v3_oversize_"))
    work.mkdir(parents=True, exist_ok=True)
    binary = codex_binary()
    outputs = {
        "OVERSIZE_TRANSPORT_AUDIT.json": audit(binary),
        "OVERSIZE_TRANSPORT_TEST.json": synthetic_test(work),
        "PAIR_A_OVERSIZE_PREFLIGHT.json": preflight(work),
    }
    for name, value in outputs.items():
        (args.out_dir / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(name, value.get("status", "WRITTEN"), json.dumps(value.get("checks", {}), ensure_ascii=False))
    return 0 if all(v.get("status", "PASS") == "PASS" for v in outputs.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
