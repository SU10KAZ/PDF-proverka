#!/usr/bin/env python3
"""Bounded comparison benchmark using generated PDFs only; no AI or user documents."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.app.services.common.cpu_pool import configure_worker_threads
from backend.app.services.version_comparison import paths, profile, work_queue as q


def make_sources(root: Path) -> dict:
    import fitz

    os.environ["VERSION_COMPARISON_ROOT"] = str(root / "synthetic-data")
    sources = {}
    for size in ("a4", "a1", "a0"):
        pair = []
        for revision in (0, 1):
            document = fitz.open()
            rect = fitz.paper_rect(size)
            page = document.new_page(width=rect.width, height=rect.height)
            # Anchors span the page; colored geometry exercises full-size masks,
            # connected components and the Lab color path at 200 dpi.
            for row in range(12):
                for col in range(8):
                    x = 24 + col * (rect.width - 48) / 8
                    y = 30 + row * (rect.height - 60) / 12
                    page.insert_text((x, y), f"anchor{row:02d}{col:02d}", fontsize=6)
                    box = fitz.Rect(x, y + 5, x + (rect.width - 48) / 12, y + 20)
                    color = (0.2, 0.3, 0.6) if (col + row) % 3 else (0.3, 0.3, 0.3)
                    if revision and row == 5:
                        box.x1 += 5
                        color = (0.7, 0.2, 0.2)
                    page.draw_rect(box, color=color, fill=color if row % 2 else None, width=1)
            data = document.tobytes(garbage=4, deflate=True)
            document.close()
            sha = hashlib.sha256(data).hexdigest()
            pdf = paths.source_pdf("synthetic-memory", sha)
            pdf.parent.mkdir(parents=True, exist_ok=True)
            pdf.write_bytes(data)
            record = profile.build("synthetic-memory", sha)["pages"][0]
            pair.append({"version_id": f"v{revision}", "sha": sha, "page": 1, "pdf": str(pdf),
                         "profile_dir": str(paths.profile_dir("synthetic-memory", sha)), "profile": record})
        sources[size] = pair
    return sources


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--queue", default="/home/coder/auditmanager/local-version-comparison-recovery/worker-queue")
    args = parser.parse_args()
    configure_worker_threads()
    os.environ["VERSION_COMPARISON_QUEUE_DIR"] = args.queue
    heartbeat = q.heartbeat()
    if not heartbeat.get("online"):
        parser.error("Compute service is offline")
    if "/system.slice/auditmanager-comparison-worker.service" not in Path(f"/proc/{heartbeat['pid']}/cgroup").read_text():
        parser.error("Refusing load outside the isolated system compute service")
    with q.database() as db:
        if db.execute("SELECT COUNT(*) FROM work WHERE state IN ('queued','running')").fetchone()[0]:
            parser.error("Existing work is active")
    output = args.directory / args.label
    output.mkdir(parents=True, exist_ok=False)
    manifest = args.directory / "sources.json"
    if not manifest.exists():
        manifest.write_text(json.dumps(make_sources(args.directory)))
    sources = json.loads(manifest.read_text())
    identifiers, tasks, samples, error = [], [], [], None
    prefix = "synthetic-memory-" + uuid.uuid4().hex
    started = time.monotonic()
    try:
        for size, count in (("a4", 12), ("a1", 6), ("a0", 3)):
            for index in range(count):
                name = f"{size}-{index}"
                task = {"sheet_pair_id": name, "out_dir": str(output / name), "reasons": [],
                        "params": {"dpi": 200, "tol_pt": 0.5}, "a": sources[size][0], "b": sources[size][1]}
                identifier = q.submit("sheet", [task], key=f"{prefix}:{name}")
                identifiers.append(identifier)
                tasks.append({"id": identifier, "name": name, "reservation_bytes": q.get(identifier)["cost"]})
        last_print = 0
        while time.monotonic() - started < 240:
            status = q.heartbeat()
            sample = {"elapsed": round(time.monotonic() - started, 2), "worker": status}
            before = time.monotonic()
            with urlopen("http://127.0.0.1:8095/login", timeout=2) as response:
                sample["http_status"] = response.status
            sample["http_ms"] = round((time.monotonic() - before) * 1000, 1)
            samples.append(sample)
            if sample["http_ms"] > 1000 or status.get("resources", {}).get("pressure_avg10", 0) > 5:
                raise RuntimeError("Responsiveness or memory pressure guard triggered")
            states = [q.get(identifier)["state"] for identifier in identifiers]
            if "failed" in states or "cancelled" in states:
                raise RuntimeError("A synthetic computation failed")
            if time.monotonic() - last_print > 20:
                print(json.dumps({"done": states.count("done"), "total": len(states),
                                  "active": status.get("active"), "elapsed": sample["elapsed"]}), flush=True)
                last_print = time.monotonic()
            if all(state == "done" for state in states):
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("Synthetic benchmark exceeded 240 seconds")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        for identifier in identifiers:
            q.cancel(identifier)
    for task in tasks:
        row = q.get(task["id"])
        task.update(state=row["state"], attempts=row["attempts"], error=row["error"])
        log = Path(args.queue) / f"{task['id']}.log"
        if log.exists():
            for line in log.read_text(errors="replace").splitlines():
                try:
                    metric = json.loads(line)
                    if isinstance(metric, dict) and metric.get("event") == "compute_finished":
                        task["metrics"] = metric
                except ValueError:
                    pass
        result_path = output / task["name"] / "result.json"
        if result_path.exists():
            result = json.loads(result_path.read_text())
            result.pop("elapsed_s", None)
            task["result_sha256"] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    report = {"kind": "synthetic PDF benchmark, 200 dpi", "error": error,
              "elapsed_seconds": round(time.monotonic() - started, 2), "tasks": tasks,
              "peak_active": max((s["worker"].get("active", 0) for s in samples), default=0),
              "samples": samples}
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k not in ("tasks", "samples")}), flush=True)
    return 1 if error else 0


if __name__ == "__main__":
    raise SystemExit(main())
