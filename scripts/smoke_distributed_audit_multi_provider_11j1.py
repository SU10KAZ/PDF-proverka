#!/usr/bin/env python3
"""11J.1: два multi-provider job через настоящий HTTPS, только на заглушках.

Центр запускается на текущей машине в изолированных каталогах, worker — на
удалённом VPS. Agent/claim/download/Executor/upload/ACK/import настоящие;
Claude/Codex указывают на исполняемые заглушки, OpenRouter — на localhost HTTP
stub воркера. Ни один настоящий provider endpoint этим сценарием не достижим.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import smoke_distributed_audit_real_vps as base  # noqa: E402

from backend.app.services.audit_routing import (              # noqa: E402
    budget as routing_budget,
    presets,
    registry,
)
from backend.app.services.audit_routing.plan import RoutingPlan  # noqa: E402
from backend.app.core.config import CODEX_STAGE_MODEL_ID         # noqa: E402


OPENROUTER_STUB_PORT = 18765
FULL_DOC_CODE = "ТЕСТ-11J1-РД-АР-К1"
CLAUDE_DOC_CODE = "ТЕСТ-11J1-РД-АР-К2"
ROUTING_FLAGS = {
    "STAGE01_THIRD_LEG_ENABLED": "true",
    "STAGE01_DUAL_REVIEW_ENABLED": "true",
    "OPTIMIZATION_CRITIC_DETERMINISTIC": "true",
    "AUDIT_CODEX_TARGETED_FINDINGS": "true",
    "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED": "true",
    "AUDIT_CODEX_OPTIMIZATION_IMAGES": "true",
    "PIPELINE_VERIFIER_ENABLED": "true",
    "PIPELINE_NORMS_AFTER_MERGE_ENABLED": "true",
    "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED": "true",
}


def _paths(root: str) -> dict[str, str]:
    name = Path(root).name
    secret_dir = str(Path(root).parent / ".config" / f"{name}-test-secrets")
    stub_dir = f"{root}/provider_stub"
    return {
        "policy": f"{root}/data/provider_policy.json",
        "grant": f"{root}/data/config/allow_synthetic_inference",
        "stub_dir": stub_dir,
        "claude_binary": f"{stub_dir}/claude",
        "codex_binary": f"{stub_dir}/codex",
        "claude_wrapper": f"{stub_dir}/claude-with-call-log",
        "codex_wrapper": f"{stub_dir}/codex-with-call-log",
        "claude_log": f"{root}/logs/provider_stub_claude_calls.jsonl",
        "codex_log": f"{root}/logs/provider_stub_codex_calls.jsonl",
        "openrouter_log": f"{root}/logs/provider_stub_openrouter_calls.jsonl",
        "secret_dir": secret_dir,
        "secret": f"{secret_dir}/openrouter-credential.json",
        "openrouter_unit": f"{name}-openrouter-stub.service",
    }


def _worker_env(
    *, root: str, central_url: str, revision: str, display_name: str,
    max_inferences: int, paths: dict[str, str],
) -> str:
    """Взять проверенный 11G env и раскрыть его на три локальных маршрута."""
    text = base.worker_env_file(
        root=root,
        central_url=central_url,
        revision=revision,
        display_name=display_name,
        provider_mode=base.PROVIDER_ENV_BRIDGE,
        claude_executable=paths["claude_wrapper"],
        provider=base.PROVIDER_CLAUDE,
        max_inferences=max_inferences,
        stub_call_log=paths["claude_log"],
    )
    text = text.replace(
        "AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE=ambient_user",
        "AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE=isolated_provider_home",
    )
    text = text.replace(
        "AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE=unavailable\n", ""
    )
    extra = [
        "",
        "# 11J.1 multi-provider fake transport: все пути задаёт admin plane.",
        "AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE=isolated_provider_home",
        "AUDIT_WORKER_PROVIDER_OPENROUTER_AUTH_MODE=isolated_provider_home",
        f"AUDIT_WORKER_PROVIDER_CODEX_EXECUTABLE={paths['codex_wrapper']}",
        f"AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL={paths['secret']}",
        (
            "AUDIT_WORKER_PROVIDER_OPENROUTER_BASE_URL="
            f"http://127.0.0.1:{OPENROUTER_STUB_PORT}"
        ),
        "AUDIT_WORKER_PROVIDER_ENDPOINTS_STUBBED=true",
        "AUDIT_WORKER_PROVIDER_CLAUDE_MAX_CONCURRENCY=1",
        "AUDIT_WORKER_PROVIDER_CODEX_MAX_CONCURRENCY=2",
        "AUDIT_WORKER_PROVIDER_OPENROUTER_MAX_CONCURRENCY=1",
        "",
    ]
    return text.rstrip() + "\n" + "\n".join(extra)


_MULTI_SETUP = r'''
import json
import os
import secrets
from pathlib import Path

from audit_worker.providers import model_policy, openrouter_secret
from backend.app.pipeline.execution import provider_bridge_stub

policy_path = Path(os.environ["POLICY_PATH"])
stub_dir = Path(os.environ["STUB_DIR"])
secret_path = Path(os.environ["SECRET_PATH"])

def cap(model, *, unsupported=False):
    row = {"model": model}
    if unsupported:
        row["model_report"] = model_policy.MODEL_REPORT_UNSUPPORTED
    return row

policy = {
    "policy_version": model_policy.POLICY_SCHEMA_VERSION,
    "claude": {
        "auth_mode": "isolated_provider_home",
        "capabilities": {
            "strong_audit": cap("claude-opus-5"),
            "cheap_review": cap("claude-sonnet-5"),
        },
    },
    "codex": {
        "auth_mode": "isolated_provider_home",
        "capabilities": {
            name: cap("gpt-5.6-sol", unsupported=True)
            for name in (
                "strong_audit", "cheap_review", "block_detector",
                "block_detector_strong", "block_judge", "visual_reasoning",
            )
        },
    },
    "openrouter": {
        "auth_mode": "isolated_provider_home",
        "capabilities": {
            "block_detector": cap("openai/gpt-5.4"),
        },
    },
}
policy_path.parent.mkdir(parents=True, exist_ok=True)
policy_path.write_text(json.dumps(policy, ensure_ascii=False, indent=2), encoding="utf-8")
os.chmod(policy_path, 0o600)
parsed = model_policy.parse_policy(
    json.loads(policy_path.read_text(encoding="utf-8")), source_path=policy_path
)
for provider, capability in (
    ("claude", "strong_audit"), ("claude", "cheap_review"),
    ("codex", "block_detector"), ("codex", "block_detector_strong"),
    ("codex", "block_judge"), ("codex", "visual_reasoning"),
    ("codex", "strong_audit"), ("openrouter", "block_detector"),
):
    parsed.resolve(provider, capability)

for provider in ("claude", "codex"):
    binary = provider_bridge_stub.materialize(stub_dir, provider=provider)
    if not provider_bridge_stub.looks_like_stub(binary):
        raise SystemExit("provider stub marker invalid")

openrouter_secret.write_secret_for_tests(
    secret_path, "sk-or-test-11j1-" + secrets.token_hex(24)
)
status = openrouter_secret.probe(secret_path, env={})
if not status.configured:
    raise SystemExit("test OpenRouter secret not configured safely")
print("POLICY_OK providers=claude,codex,openrouter mode=%o" % (
    policy_path.stat().st_mode & 0o777,
))
print("TEST_SECRET_OK configured=true mode=%s" % status.mode)
'''


def configure_worker(
    worker: base.Worker, *, central_url: str, revision: str,
    display_name: str, max_inferences: int,
) -> dict[str, str]:
    print("\n── 11J.1: три fake provider transport на worker ───────────────")
    paths = _paths(worker.root)
    env_body = _worker_env(
        root=worker.root,
        central_url=central_url,
        revision=revision,
        display_name=display_name,
        max_inferences=max_inferences,
        paths=paths,
    )
    agent_unit = base.systemd_unit(kind="agent", root=worker.root)
    executor_unit = base.systemd_unit(kind="executor", root=worker.root)
    openrouter_unit = f"""[Unit]
Description=11J.1 OpenRouter localhost stub (zero real inference)

[Service]
Type=simple
WorkingDirectory={worker.root}/current
Environment=PYTHONPATH={worker.root}/current
ExecStart={worker.root}/venv/bin/python -m tests.distributed_audit_e2e.openrouter_stub --port {OPENROUTER_STUB_PORT} --behaviour ok --log {paths['openrouter_log']}
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
ProtectSystem=full

[Install]
WantedBy=default.target
"""
    script = f"""set -euo pipefail
root={shlex.quote(worker.root)}
mkdir -p "$root/config" "$root/logs" "$root/data" "$root/provider_stub"
umask 077
cat > "$root/config/worker.env" <<'WORKER_ENV_EOF'
{env_body}
WORKER_ENV_EOF
chmod 600 "$root/config/worker.env"
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/{base.AGENT_UNIT} <<'AGENT_EOF'
{agent_unit}
AGENT_EOF
cat > ~/.config/systemd/user/{base.EXECUTOR_UNIT} <<'EXECUTOR_EOF'
{executor_unit}
EXECUTOR_EOF
cat > ~/.config/systemd/user/{paths['openrouter_unit']} <<'OPENROUTER_UNIT_EOF'
{openrouter_unit}
OPENROUTER_UNIT_EOF
export PYTHONPATH="$root/current"
export POLICY_PATH={shlex.quote(paths['policy'])}
export STUB_DIR={shlex.quote(paths['stub_dir'])}
export SECRET_PATH={shlex.quote(paths['secret'])}
"$root/venv/bin/python" - <<'MULTI_SETUP_PY'
{_MULTI_SETUP}
MULTI_SETUP_PY
cat > {shlex.quote(paths['claude_wrapper'])} <<'CLAUDE_WRAPPER_EOF'
#!/bin/sh
AUDIT_PROVIDER_STUB_CALL_LOG={shlex.quote(paths['claude_log'])}
AUDIT_PROVIDER_STUB_MODEL=claude-opus-5
export AUDIT_PROVIDER_STUB_CALL_LOG AUDIT_PROVIDER_STUB_MODEL
exec {shlex.quote(paths['claude_binary'])} "$@"
CLAUDE_WRAPPER_EOF
cat > {shlex.quote(paths['codex_wrapper'])} <<'CODEX_WRAPPER_EOF'
#!/bin/sh
AUDIT_PROVIDER_STUB_CALL_LOG={shlex.quote(paths['codex_log'])}
AUDIT_PROVIDER_STUB_MODEL=gpt-5.6-sol
export AUDIT_PROVIDER_STUB_CALL_LOG AUDIT_PROVIDER_STUB_MODEL
exec {shlex.quote(paths['codex_binary'])} "$@"
CODEX_WRAPPER_EOF
chmod 700 {shlex.quote(paths['claude_wrapper'])} {shlex.quote(paths['codex_wrapper'])}
rm -f {shlex.quote(paths['claude_log'])} {shlex.quote(paths['codex_log'])} {shlex.quote(paths['openrouter_log'])}
export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user daemon-reload
systemctl --user enable {paths['openrouter_unit']} >/dev/null
systemctl --user restart {paths['openrouter_unit']}
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:{OPENROUTER_STUB_PORT}/ >/dev/null; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:{OPENROUTER_STUB_PORT}/ >/dev/null
echo "ENV_MODE=$(stat -c '%a' "$root/config/worker.env")"
echo "WRAPPER_MODES=$(stat -c '%a' {shlex.quote(paths['claude_wrapper'])}),$(stat -c '%a' {shlex.quote(paths['codex_wrapper'])})"
echo "OPENROUTER_STUB=ready host=127.0.0.1"
echo CONFIG_OK
"""
    result = worker.act(script, timeout=240)
    base.check("CONFIG_OK" in result.stdout, "multi-provider worker.env установлен",
               result.stderr[-500:] if result.returncode else "")
    base.check("POLICY_OK providers=claude,codex,openrouter" in result.stdout,
               "локальная policy покрывает три провайдера")
    base.check("TEST_SECRET_OK configured=true mode=0600" in result.stdout,
               "тестовый OpenRouter secret локален и имеет 0600")
    base.check("WRAPPER_MODES=700,700" in result.stdout,
               "Claude/Codex wrappers закрыты правами 0700")
    base.check("OPENROUTER_STUB=ready host=127.0.0.1" in result.stdout,
               "OpenRouter stub слушает только localhost worker")
    return paths


def _read_jsonl(worker: base.Worker, path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in base.worker_read_file(worker, path, timeout=180).splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _result_package_manifest(
    worker: base.Worker, *, attempt_id: str, job_id: str,
) -> dict[str, Any]:
    """Извлечь только safe manifest из result archive на удалённом worker."""
    probe = r'''
import json
import os
import tarfile
from pathlib import Path

root = Path(os.environ["ROOT"]) / "data" / "jobs"
attempt = os.environ["ATTEMPT"]
job_id = os.environ["JOB_ID"]
for archive in sorted(root.rglob("*.tar*"), reverse=True):
    if attempt not in archive.parts:
        continue
    try:
        with tarfile.open(archive, "r:*") as tf:
            member = next((m for m in tf.getmembers() if m.name.endswith("package_manifest.json")), None)
            if member is None:
                continue
            handle = tf.extractfile(member)
            data = json.load(handle) if handle is not None else {}
    except Exception:
        continue
    if str(data.get("job_id") or "") != job_id or data.get("package_type") != "result":
        continue
    safe = {
        "job_id": data.get("job_id"),
        "attempt_id": data.get("attempt_id"),
        "routing_plan_id": data.get("routing_plan_id"),
        "routing_plan_hash": data.get("routing_plan_hash"),
        "provider_action_provenance": data.get("provider_action_provenance") or [],
        "provider_mode": data.get("provider_mode"),
        "completed_stages": data.get("completed_stages") or [],
        "forbidden_stages_not_run": data.get("forbidden_stages_not_run") or [],
        "source_package_hash": data.get("source_package_hash"),
    }
    print(json.dumps(safe, ensure_ascii=False))
    break
'''
    result = worker.read(
        f"""set -euo pipefail
export ROOT={shlex.quote(worker.root)}
export ATTEMPT={shlex.quote(attempt_id)}
export JOB_ID={shlex.quote(job_id)}
{shlex.quote(worker.root)}/venv/bin/python - <<'RESULT_MANIFEST_PY'
{probe}
RESULT_MANIFEST_PY
""",
        timeout=180,
    )
    return base._first_json_object(result.stdout)


def _safe_grants(worker: base.Worker, path: str, *, job_id: str) -> list[dict[str, Any]]:
    payload = base._first_json_object(base.worker_read_file(worker, path))
    return [
        {
            "grant_id": row.get("grant_id"),
            "provider": row.get("provider"),
            "used": row.get("used"),
            "max_uses": row.get("max_uses"),
        }
        for row in (payload.get("grants") or [])
        if str(row.get("task_id") or "") == job_id
    ]


def _assert_trace(
    *, label: str, run: base.RemoteAuditRun, package: dict[str, Any],
    expected_preset: str,
) -> dict[str, Any]:
    payload = base._job_payload(run.row)
    plan_payload = payload.get("routing_plan") or {}
    plan = RoutingPlan.from_dict(plan_payload)
    plan.assert_hash(str(plan_payload.get("routing_plan_hash") or ""))
    rows = list(package.get("provider_action_provenance") or [])
    completed = [row for row in rows if str(row.get("status")) == "success"]
    providers = Counter(str(row.get("provider") or "") for row in completed)
    actions = Counter(str(row.get("action_id") or "") for row in completed)
    expected = routing_budget.estimate(
        plan,
        routing_budget.DocumentShape(graphic_blocks=2),
        scope=registry.SCOPE_WORKER,
    )

    base.check(plan.preset_id == expected_preset,
               f"{label}: frozen plan имеет ожидаемый preset", plan.preset_id)
    base.check(package.get("routing_plan_hash") == plan.plan_hash(),
               f"{label}: routing hash совпал job → result", plan.plan_hash())
    base.check(package.get("routing_plan_id") == plan.routing_plan_id,
               f"{label}: routing_plan_id совпал job → result")
    base.check(len(completed) == int(expected["natural_calls"]),
               f"{label}: budget равен executed logical actions",
               f"predicted={expected['natural_calls']} executed={len(completed)}")
    base.check(len({str(row.get('ledger_key')) for row in rows}) == len(rows),
               f"{label}: per-action ledger keys уникальны")
    base.check(len(completed) == len(rows),
               f"{label}: все logical actions завершены без indeterminate")

    blocks: dict[str, set[str]] = {}
    for row in completed:
        block = str(row.get("block_identity") or "")
        if block:
            blocks.setdefault(block, set()).add(str(row.get("action_id") or ""))
    exact_block_actions = {
        "detector_openrouter", "detector_codex_standard",
        "detector_codex_strong", "judge_gap_search",
    }
    base.check(len(blocks) == 2 and all(v == exact_block_actions for v in blocks.values()),
               f"{label}: каждый block дал ровно четыре model actions",
               json.dumps({k: sorted(v) for k, v in blocks.items()}, ensure_ascii=False))
    base.check(actions["detector_openrouter"] == 2,
               f"{label}: OpenRouter detector выполнен worker-side")
    base.check(actions["detector_codex_standard"] == 2
               and actions["detector_codex_strong"] == 2,
               f"{label}: две Codex detector legs выполнены worker-side")
    base.check(actions["judge_gap_search"] == 2,
               f"{label}: judge/gap выполнен после detector group")
    base.check(actions["absence_guard"] == 1
               and next((r.get("provider") for r in completed
                         if r.get("action_id") == "absence_guard"), None) == "claude",
               f"{label}: absence guard реально выполнен через Claude")
    base.check(actions["optimization_primary"] == 1
               and actions["optimization_visual"] == 1,
               f"{label}: optimization dual-provider выполнена")
    deterministic = {
        "combine_detectors", "structural_checks", "apply_verdicts",
        "optimization_merge", "deterministic_fix",
    }
    base.check(not (deterministic & set(actions)),
               f"{label}: deterministic actions не создали model calls")

    targeted = {
        "targeted_discipline", "targeted_docnorm", "targeted_mark_system",
    }
    if expected_preset == presets.PRESET_FULL_CODEX:
        base.check(all(actions[name] == 1 for name in targeted),
                   f"{label}: все Full Codex targeted passes реально executed",
                   json.dumps({name: actions[name] for name in sorted(targeted)}))
    else:
        base.check(not (targeted & set(actions)),
                   f"{label}: Claude preset не получил Full-Codex targeted passes")

    return {
        "label": label,
        "job_id": str(run.row.get("job_id") or ""),
        "attempt_id": run.attempt_id,
        "preset_id": plan.preset_id,
        "routing_plan_id": plan.routing_plan_id,
        "routing_plan_hash": plan.plan_hash(),
        "predicted": expected,
        "executed": {
            "total": len(completed),
            "per_provider": dict(sorted(providers.items())),
            "per_action": dict(sorted(actions.items())),
            "rows": completed,
        },
        "result_package": package,
        "center_handoff_state": run.row.get("central_handoff_state"),
        "result_import_state": run.row.get("result_import_state"),
    }


def _scan_test_secret(worker: base.Worker, paths: dict[str, str]) -> dict[str, Any]:
    """Искать значение локально на worker; наружу вернуть только пути/счёт."""
    probe = r'''
import json
import os
from pathlib import Path
from audit_worker.providers import openrouter_secret

root = Path(os.environ["ROOT"])
secret_path = Path(os.environ["SECRET"])
secret = openrouter_secret.read_secret(secret_path, env={})
hits = []
for base in (root / "data", root / "logs", root / "config"):
    if not base.exists():
        continue
    for path in base.rglob("*"):
        if not path.is_file() or path == secret_path:
            continue
        try:
            if secret.encode("utf-8") in path.read_bytes():
                hits.append(str(path.relative_to(root)))
        except OSError:
            pass
print(json.dumps({"leak_count": len(hits), "paths": hits}, ensure_ascii=False))
'''
    result = worker.read(
        f"""set -euo pipefail
export ROOT={shlex.quote(worker.root)}
export SECRET={shlex.quote(paths['secret'])}
{shlex.quote(worker.root)}/venv/bin/python - <<'SECRET_SCAN_PY'
{probe}
SECRET_SCAN_PY
""",
        timeout=240,
    )
    return base._first_json_object(result.stdout)


def _launch_pair(
    stand: base.Stand, operator: base.Operator, worker: base.Worker, *,
    worker_id: str, paths: dict[str, str], timeout: float,
) -> dict[str, Any]:
    codex_model = CODEX_STAGE_MODEL_ID
    switched: dict[str, Any] = {}
    before_calls = {
        name: len(_read_jsonl(worker, paths[f"{name}_log"]))
        for name in ("claude", "codex", "openrouter")
    }

    def switch_after_claim(attempt_id: str, _manifest: dict[str, Any]) -> None:
        cfg = presets.reference_config(
            presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=codex_model
        )
        response = operator.post("/api/audit/model/stages", json=cfg)
        body = response.json() if response.status_code < 400 else {}
        switched.update({
            "after_attempt_id": attempt_id,
            "http_status": response.status_code,
            "rejected": body.get("rejected") or {},
            "target_preset": presets.PRESET_CLAUDE_GPT_CODEX,
        })
        base.check(bool(attempt_id), "freeze: Job A уже claimed до переключения")
        base.check(response.status_code < 400 and not switched["rejected"],
                   "freeze: global preset переключён после claim",
                   f"HTTP {response.status_code}")

    base.DOCUMENT_CODE = FULL_DOC_CODE
    base.EXTERNAL_ID = "ТЕСТ 11J.1 Full Codex"
    full_run = base.launch_remote_audit(
        stand, operator, worker, worker_id=worker_id, timeout=timeout,
        clone_local=False, stop_at_boundary=False,
        after_claim=switch_after_claim,
    )
    if full_run is None:
        raise SystemExit("Full Codex network job не создан")
    full_package = _result_package_manifest(
        worker,
        attempt_id=full_run.attempt_id,
        job_id=str(full_run.row.get("job_id") or ""),
    )
    full = _assert_trace(
        label="Full Codex Job A", run=full_run, package=full_package,
        expected_preset=presets.PRESET_FULL_CODEX,
    )
    full["automatic_grants"] = _safe_grants(
        worker, paths["grant"], job_id=full["job_id"]
    )
    base.check(
        {g.get("provider") for g in full["automatic_grants"]}
        == {"claude", "codex", "openrouter"}
        and all(str(g.get("grant_id") or "").startswith("auto-")
                for g in full["automatic_grants"]),
        "Full Codex Job A: grants для трёх провайдеров выписаны автоматически",
    )

    middle_calls = {
        name: len(_read_jsonl(worker, paths[f"{name}_log"]))
        for name in ("claude", "codex", "openrouter")
    }

    base.DOCUMENT_CODE = CLAUDE_DOC_CODE
    base.EXTERNAL_ID = "ТЕСТ 11J.1 Claude+GPT+Codex"
    claude_run = base.launch_remote_audit(
        stand, operator, worker, worker_id=worker_id, timeout=timeout,
        clone_local=False, stop_at_boundary=False,
    )
    if claude_run is None:
        raise SystemExit("Claude+GPT+Codex network job не создан")
    claude_package = _result_package_manifest(
        worker,
        attempt_id=claude_run.attempt_id,
        job_id=str(claude_run.row.get("job_id") or ""),
    )
    claude = _assert_trace(
        label="Claude+GPT+Codex Job B", run=claude_run, package=claude_package,
        expected_preset=presets.PRESET_CLAUDE_GPT_CODEX,
    )
    claude["automatic_grants"] = _safe_grants(
        worker, paths["grant"], job_id=claude["job_id"]
    )
    base.check(
        {g.get("provider") for g in claude["automatic_grants"]}
        == {"claude", "codex", "openrouter"}
        and all(str(g.get("grant_id") or "").startswith("auto-")
                for g in claude["automatic_grants"]),
        "Claude Job B: grants для трёх провайдеров выписаны автоматически",
    )

    after_calls = {
        name: len(_read_jsonl(worker, paths[f"{name}_log"]))
        for name in ("claude", "codex", "openrouter")
    }
    call_deltas = {
        "full_codex": {
            name: middle_calls[name] - before_calls[name]
            for name in before_calls
        },
        "claude_gpt_codex": {
            name: after_calls[name] - middle_calls[name]
            for name in before_calls
        },
    }
    base.check(full["routing_plan_hash"] != claude["routing_plan_hash"],
               "freeze: hash_A != hash_B после переключения global preset")
    base.check(full["preset_id"] == presets.PRESET_FULL_CODEX,
               "freeze: worker A продолжил Full Codex snapshot")
    base.check(full["center_handoff_state"] == "completed",
               "freeze: center tail A завершил тот же frozen plan")
    base.check(claude["preset_id"] == presets.PRESET_CLAUDE_GPT_CODEX,
               "freeze: Job B получил новый global preset")

    secret_scan = _scan_test_secret(worker, paths)
    base.check(secret_scan.get("leak_count") == 0,
               "тестовый OpenRouter key не попал в job/outbox/log/result",
               json.dumps(secret_scan, ensure_ascii=False))

    return {
        "full_codex": full,
        "claude_gpt_codex": claude,
        "freeze_switch": {
            **switched,
            "routing_plan_hash_A": full["routing_plan_hash"],
            "routing_plan_hash_B": claude["routing_plan_hash"],
            "running_job_changed": False,
            "center_tail_A_preset": full["preset_id"],
        },
        "stub_call_log_deltas": call_deltas,
        "test_secret_scan": secret_scan,
    }


def _write_preset(stand: base.Stand, preset_id: str) -> dict[str, str]:
    models = presets.reference_config(
        preset_id, codex_model_id=CODEX_STAGE_MODEL_ID
    )
    path = stand.central_app_data / "stage_models.json"
    path.write_text(json.dumps(models, ensure_ascii=False, indent=2), encoding="utf-8")
    return models


def _cleanup_worker(worker: base.Worker, paths: dict[str, str]) -> None:
    """Остановить только изолированные 11J.1 units и удалить тестовый secret."""
    try:
        worker.act(
            f"""set +e
export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user stop {base.AGENT_UNIT} {base.EXECUTOR_UNIT} {paths['openrouter_unit']}
rm -f {shlex.quote(paths['secret'])}
rmdir {shlex.quote(paths['secret_dir'])} 2>/dev/null || true
echo CLEANUP_OK
""",
            timeout=120,
        )
    except Exception:
        pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-host", required=True)
    parser.add_argument("--worker-user", required=True)
    parser.add_argument("--worker-root", default="/home/coder/audit-worker-11j1")
    parser.add_argument("--central-url", default="")
    parser.add_argument("--central-port", type=int, default=0)
    parser.add_argument("--tunnel", choices=("none", "cloudflared"), default="cloudflared")
    parser.add_argument("--tunnel-binary", default="cloudflared")
    parser.add_argument("--pipeline-revision", default="")
    parser.add_argument("--display-name", default="11j1-multiprovider-fake")
    parser.add_argument("--bootstrap-secret", default="")
    parser.add_argument("--root", default="")
    parser.add_argument("--audit-timeout-sec", type=float, default=3600.0)
    parser.add_argument("--allow-remote-actions", action="store_true")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--keep-runtime", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.allow_remote_actions:
        raise SystemExit("сетевой прогон требует --allow-remote-actions")
    revision = args.pipeline_revision or (
        "git:" + subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT,
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    )
    root = Path(args.root) if args.root else Path(
        tempfile.mkdtemp(prefix="audit_11j1_multi_")
    )
    stand = base.Stand(root=root, port=args.central_port or base._free_port())
    worker = base.Worker(
        host=args.worker_host,
        user=args.worker_user,
        root=args.worker_root,
        allow_actions=True,
        # На .128 системный include ssh сломан; /dev/null оставляет только
        # явные безопасные параметры этого административного канала.
        ssh_opts=("-F", "/dev/null", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"),
    )
    base.AGENT_UNIT, base.EXECUTOR_UNIT = base.unit_names(args.worker_root)
    base.DISCIPLINE_SECTION = "AR"
    base.DISCIPLINE_FOLDER = "АР"
    paths = _paths(args.worker_root)
    bootstrap = args.bootstrap_secret or ("11j1-" + uuid.uuid4().hex)
    operator: Optional[base.Operator] = None

    print("═" * 72)
    print("11J.1 MULTI-PROVIDER HTTPS — FAKE PROVIDERS ONLY")
    print(f"  center host: {os.uname().nodename} (.128), isolated={root}")
    print(f"  worker host: {worker.target} (.31), isolated={worker.root}")
    print(f"  revision:    {revision}")
    print("  real Claude/Codex/OpenRouter inference: FORBIDDEN (0)")
    print("═" * 72)

    try:
        base.phase_preflight_central(stand, revision=revision)
        inventory = base.phase_preflight_worker(worker, revision=revision, require_real_cli=False)
        base.prepare_central_assets(stand)
        _write_preset(stand, presets.PRESET_FULL_CODEX)
        env = stand.central_env(
            revision=revision, bootstrap_secret=bootstrap,
            stop_at_boundary=False, central_tail_cli="",
        )
        env.update(ROUTING_FLAGS)
        # Центральный хвост тоже только на materialized fake_providers.
        env["PAID_API_ENABLED"] = "false"
        base.start_backend(stand, env=env, tag="11j1")
        base.check(base._wait_for(lambda: base.backend_ready(stand), timeout=180),
                   "изолированный test-center поднялся", stand.local_url)

        central_url = args.central_url
        if args.tunnel == "cloudflared" and not central_url:
            central_url = base.start_tunnel(stand, binary=args.tunnel_binary)
            base.check(
                base._wait_for(
                    lambda: base.backend_ready(stand, url=central_url), timeout=120
                ),
                "test-center доступен по валидному публичному HTTPS",
                central_url,
            )
        if not central_url:
            raise SystemExit("нужен --central-url либо --tunnel cloudflared")
        stand.central_url = central_url
        base.phase_transport(worker, stand, central_url=central_url)

        operator = base.Operator(central_url)
        stand.cleanup.append(operator.close)
        base.check(operator.login(), "оператор вошёл в изолированный center")
        configure_worker(
            worker,
            central_url=central_url,
            revision=revision,
            display_name=args.display_name,
            max_inferences=base.center_max_inferences(),
        )
        base.phase_reset_registration(worker)
        worker_id = base.phase_register(
            worker, operator, bootstrap_secret=bootstrap,
            display_name=args.display_name,
        )
        base.phase_start_units(worker)
        heartbeat = base.phase_heartbeat(operator, worker_id=worker_id, revision=revision)
        base.phase_targets(operator, worker_id=worker_id)
        test_job = base.phase_test_job(operator, worker, worker_id=worker_id)

        pair = _launch_pair(
            stand, operator, worker, worker_id=worker_id, paths=paths,
            timeout=args.audit_timeout_sec,
        )
        backend_log = (stand.evidence / "backend_11j1.log").read_text(
            encoding="utf-8", errors="replace"
        )
        frozen_lines = [
            line[-500:] for line in backend_log.splitlines()
            if "FROZEN_ROUTING_PLAN" in line
        ]
        base.check(any("FROZEN_ROUTING_PLAN FOUND" in line for line in frozen_lines),
                   "center log различает FROZEN_ROUTING_PLAN FOUND")
        base.check(not any("FROZEN_ROUTING_PLAN INVALID" in line for line in frozen_lines),
                   "новые job не использовали INVALID/fallback frozen plan")

        report = {
            "schema_version": 1,
            "architecture_test": "11J.1 multi-provider fake HTTPS",
            "revision": revision,
            "worker_id": worker_id,
            "center_endpoint": {
                "scheme": "https",
                "host": central_url.split("//", 1)[-1].split("/", 1)[0],
                "tls_verified": True,
            },
            "worker_host": args.worker_host,
            "worker_root": args.worker_root,
            "worker_inbound_runtime_port_opened": False,
            "transport": {
                "agent_poll_claim_download_upload_ack_import": True,
                "test_job": test_job,
            },
            "heartbeat_provider_snapshot": heartbeat.get("capabilities") or [],
            "jobs": pair,
            "frozen_routing_log": frozen_lines,
            "real_runtime_calls": {
                "claude": 0, "codex": 0, "openrouter": 0,
            },
            "production_changed": False,
            "worker_listen_before": inventory.get("listen", ""),
        }
        report_path = stand.evidence / "11j1_multi_provider_report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\n11J.1 evidence: {report_path}")
        return base._finish()
    finally:
        if not args.keep_runtime:
            _cleanup_worker(worker, paths)
        stand.run_cleanup()
        stand.stop_all()
        if not args.keep and not args.root:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"\nстенд сохранён: {root}")


if __name__ == "__main__":
    sys.exit(main())
