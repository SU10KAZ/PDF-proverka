#!/usr/bin/env python3
"""Bounded, local CPU load check; synthetic work only, no document or AI calls."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from backend.app.services.version_comparison import resources, work_queue as q


def cpu_sample():
    return {parts[0]: list(map(int, parts[1:])) for line in Path("/proc/stat").read_text().splitlines()
            if (parts := line.split()) and parts[0].startswith("cpu") and parts[0] != "cpu"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="/home/coder/auditmanager/local-version-comparison-recovery/worker-queue")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.environ["VERSION_COMPARISON_QUEUE_DIR"] = args.queue
    status = q.heartbeat()
    if not status.get("online"):
        parser.error("Compute service is not online; no load was started")
    cgroup = Path(f"/proc/{status['pid']}/cgroup").read_text()
    if "/system.slice/auditmanager-comparison-worker.service" not in cgroup:
        parser.error("Worker is not isolated in its system service; no load was started")
    with q.database() as db:
        busy = db.execute("SELECT COUNT(*) FROM work WHERE state IN ('queued','running')").fetchone()[0]
    if busy:
        parser.error("Existing computation is active; no load was started")
    prefix = "load-check-" + uuid.uuid4().hex
    identifiers = []
    samples = []
    failure = None
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    previous_cpu = cpu_sample()
    try:
        identifiers = [q.submit("probe", [{"seconds": 5, "mib": 4}], key=f"{prefix}:{i}") for i in range(24)]
        while time.monotonic() - started < 60:
            sample = {"elapsed": round(time.monotonic() - started, 2), "http": {}, "worker": q.heartbeat()}
            # Retain the partial sample and failing port even when a request
            # times out; otherwise the report contains only successful probes.
            samples.append(sample)
            for port in (8081, 8095):
                before = time.monotonic()
                measurement = sample["http"][str(port)] = {}
                try:
                    with urlopen(f"http://127.0.0.1:{port}/login", timeout=2) as response:
                        measurement["status"] = response.status
                except Exception as exc:
                    measurement["error"] = f"{type(exc).__name__}: {exc}"
                    raise RuntimeError(f"Port {port} /login: {measurement['error']}") from exc
                finally:
                    measurement["seconds"] = round(time.monotonic() - before, 4)
            current_cpu = cpu_sample()
            usage = {}
            for name, values in current_cpu.items():
                old = previous_cpu.get(name, values)
                delta = [a - b for a, b in zip(values[:8], old[:8])]
                total = sum(delta)
                usage[name] = round(100 * (total - delta[3] - delta[4]) / total, 1) if total else 0
            previous_cpu = current_cpu
            sample["cpu_busy_percent"] = usage
            sample["host_memory_pressure"] = resources.pressure(Path("/proc/pressure/memory").read_text())
            if sample["host_memory_pressure"] > 5:
                raise RuntimeError("Memory pressure increased; synthetic load cancelled")
            if any(v["status"] != 200 or v["seconds"] > 1 for v in sample["http"].values()):
                raise RuntimeError("Portal latency exceeded one second; synthetic load cancelled")
            states = [q.get(i)["state"] for i in identifiers]
            if any(state in ("failed", "cancelled") for state in states):
                raise RuntimeError("Synthetic task failed")
            if all(state == "done" for state in states):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Load check exceeded 60 seconds")
    except Exception as exc:
        failure = str(exc)
    finally:
        for identifier in identifiers:
            q.cancel(identifier)
    results = [q.get(i) for i in identifiers]
    report = {"ok": failure is None, "error": failure, "kind": "synthetic CPU load; not a PDF throughput benchmark",
              "started_at": started_at,
              "elapsed_seconds": round(time.monotonic() - started, 2), "tasks": len(results),
              "done": sum(r["state"] == "done" for r in results),
              "peak_active_workers": max((s["worker"].get("active", 0) for s in samples), default=0),
              "peak_busy_cores": max((sum(v >= 50 for v in s.get("cpu_busy_percent", {}).values()) for s in samples), default=0),
              "native_threads": sorted({json.loads(r["result"])["native_threads"] for r in results if r["state"] == "done"}),
              "samples": samples}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "samples"}, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
