#!/usr/bin/env python3
"""11H — собрать доказательную базу боевого прогона с воркера и из стенда.

Собирает то, что нельзя восстановить задним числом: журнал вызовов попытки,
провенанс провайдера, временную шкалу этапов, состав результата. Клиентских
данных наружу не выносит — только счётчики, хэши и коды (§38 задания).
"""
from __future__ import annotations

import argparse
import collections
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]


def ssh(host: str, user: str, script: str, *, timeout: int = 300) -> str:
    result = subprocess.run(                                   # noqa: S603
        ["ssh", "-o", "BatchMode=yes", f"{user}@{host}", script],
        capture_output=True, text=True, timeout=timeout,
    )
    return result.stdout


def collect_ledger(host: str, user: str, job: str, attempt: str) -> dict[str, Any]:
    """Журнал вызовов ПОПЫТКИ — источник истины о числе обращений к модели."""
    raw = ssh(host, user, f'''J=/home/coder/audit-worker-11h/data/jobs/{job}/{attempt}
python3 - <<'PYEOF'
import json, glob, os
base = "/home/coder/audit-worker-11h/data/jobs/{job}/{attempt}/inference"
calls = []
for path in sorted(glob.glob(os.path.join(base, "*.result.json"))):
    data = json.load(open(path))
    pr = data.get("provider_result") or {{}}
    usage = pr.get("usage") or {{}}
    calls.append({{
        "key": data.get("key"),
        "purpose": (data.get("key") or "").split("-")[0],
        "status": pr.get("status"),
        "error_code": pr.get("error_code"),
        "model_reported": pr.get("model"),
        "duration_ms": pr.get("duration_ms"),
        "exit_code": pr.get("exit_code"),
        "input_tokens": usage.get("input_tokens"),
        "cached_input_tokens": usage.get("cached_input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "reasoning_output_tokens": usage.get("reasoning_output_tokens"),
        "findings": len(((pr.get("result") or {{}}).get("findings")) or []),
        "raw_sha256": pr.get("raw_sha256"),
    }})
claims = len(glob.glob(os.path.join(base, "*.claim.json")))
print(json.dumps({{"calls": calls, "claims": claims}}, ensure_ascii=False))
PYEOF''')
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    return {"calls": [], "claims": 0}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="176.12.77.31")
    parser.add_argument("--user", default="coder")
    parser.add_argument("--job", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--stand", required=True, help="каталог стенда центра")
    parser.add_argument("--out-dir", default="docs/distributed_audit_workers/11h")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ledger = collect_ledger(args.host, args.user, args.job, args.attempt)
    calls = ledger["calls"]
    by_stage: dict[str, list] = collections.defaultdict(list)
    for call in calls:
        stage = str(call.get("key") or "").split(":")[0] or "?"
        by_stage[stage].append(call)

    summary = {
        "stage": "11H",
        "question": "сколько обращений к Codex сделал боевой прогон и с каким исходом",
        "job_id": args.job,
        "attempt_id": args.attempt,
        "source": "журнал вызовов ПОПЫТКИ на воркере (inference/*.result.json)",
        "claims_started": ledger["claims"],
        "calls_completed": len(calls),
        "indeterminate": ledger["claims"] - len(calls),
        "by_stage": {
            stage: {
                "calls": len(rows),
                "success": sum(1 for r in rows if r["status"] == "success"),
                "error": sum(1 for r in rows if r["status"] != "success"),
                "findings": sum(r["findings"] for r in rows),
                "input_tokens": sum(r["input_tokens"] or 0 for r in rows),
                "cached_input_tokens": sum(r["cached_input_tokens"] or 0 for r in rows),
                "output_tokens": sum(r["output_tokens"] or 0 for r in rows),
                "reasoning_output_tokens": sum(r["reasoning_output_tokens"] or 0 for r in rows),
                "duration_sec": round(sum(r["duration_ms"] or 0 for r in rows) / 1000),
            }
            for stage, rows in sorted(by_stage.items())
        },
        "totals": {
            "input_tokens": sum(r["input_tokens"] or 0 for r in calls),
            "cached_input_tokens": sum(r["cached_input_tokens"] or 0 for r in calls),
            "output_tokens": sum(r["output_tokens"] or 0 for r in calls),
            "reasoning_output_tokens": sum(r["reasoning_output_tokens"] or 0 for r in calls),
            "duration_sec": round(sum(r["duration_ms"] or 0 for r in calls) / 1000),
            "findings": sum(r["findings"] for r in calls),
        },
        "errors": dict(collections.Counter(
            r["error_code"] for r in calls if r["error_code"]
        )),
        "model_reported_by_cli": sorted({str(r["model_reported"]) for r in calls}),
        "model_reporting_note": (
            "None у всех вызовов — Codex 0.147.0 не сообщает применённую модель "
            "(см. 11H_CODEX_PROVIDER_CONTRACT.json). Назначенная модель — в привязке"
        ),
        "calls": calls,
    }
    (out_dir / "11H_CODEX_CALL_LEDGER.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"вызовов: {len(calls)}, заявок: {ledger['claims']}")
    print(f"по этапам: { {k: v['calls'] for k, v in summary['by_stage'].items()} }")
    print(f"токены out: {summary['totals']['output_tokens']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
