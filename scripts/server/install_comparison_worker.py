#!/usr/bin/env python3
"""Install the local comparison worker outside coder's interactive user slice.

This manages the existing development portal on 8095, not auditmanager-backend
or the public release. Run --dry-run as coder; --apply requires root.
"""
from __future__ import annotations

import argparse
import datetime
import os
import pwd
import shutil
import subprocess
from pathlib import Path

REPO = Path("/home/coder/projects/PDF-proverka")
BASE = Path("/home/coder/auditmanager/local-version-comparison-recovery")
PYTHON = "/home/coder/auditmanager/current/venv/bin/python"
QUEUE = BASE / "worker-queue"
UNIT = Path("/etc/systemd/system/auditmanager-comparison-worker.service")

WORKER_UNIT = f"""[Unit]
Description=AuditManager deterministic comparison worker
After=local-fs.target
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Type=simple
User=coder
Group=coder
Slice=system.slice
WorkingDirectory={REPO}
ExecStart={PYTHON} -m backend.app.services.version_comparison.worker
Environment=VERSION_COMPARISON_QUEUE_DIR={QUEUE}
Environment=VERSION_COMPARISON_WORKERS=12
Environment=VERSION_COMPARISON_MEMORY_GB=18
Environment=VERSION_COMPARISON_HOST_RESERVE_GB=6
Environment=VERSION_COMPARISON_TASK_TIMEOUT=900
Environment=OPENCV_FOR_THREADS_NUM=1
Environment=OMP_NUM_THREADS=1
Environment=OPENBLAS_NUM_THREADS=1
Environment=MKL_NUM_THREADS=1
Environment=AUDIT_DATA_DIR=/srv/auditmanager/data
Environment=AUDIT_APP_DATA_DIR=/srv/auditmanager/data/backend/app/data
Environment=VERSION_COMPARISON_ROOT=/srv/auditmanager/data/version_comparison
CPUAccounting=yes
CPUWeight=25
Nice=5
IOAccounting=yes
IOWeight=25
MemoryAccounting=yes
MemoryHigh=20G
MemoryMax=20G
MemorySwapMax=0
OOMScoreAdjust=500
OOMPolicy=continue
TasksMax=256
KillMode=control-group
TimeoutStopSec=15
Restart=on-failure
RestartSec=5
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=full
RestrictAddressFamilies=AF_UNIX
IPAddressDeny=any
UMask=0077

[Install]
WantedBy=multi-user.target
"""

LAUNCHER = f"""#!/bin/bash
set -eu
cd {REPO}
export AUDIT_DATA_DIR=/srv/auditmanager/data
export AUDIT_APP_DATA_DIR=/srv/auditmanager/data/backend/app/data
export AUDIT_PROJECTS_V2_DIR=/srv/auditmanager/data/projects_v2
export COMPARISON_ROOT=/srv/auditmanager/data/comparison
export VERSION_COMPARISON_ROOT=/srv/auditmanager/data/version_comparison
export AUDIT_STAGE_COMPARISON_ROOTS=/srv/auditmanager/data/comparison_sources
export VERSION_COMPARISON_EXECUTOR=worker
export VERSION_COMPARISON_QUEUE_DIR={QUEUE}
export VERSION_COMPARISON_WORKERS=12
export CPU_POOL_WORKERS=2
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENCV_FOR_THREADS_NUM=1
exec {PYTHON} -m uvicorn scripts.server.version_comparison_local:app --host 127.0.0.1 --port 8095
"""

LOCAL_OVERRIDE = """[Service]
CPUQuota=200%
MemoryHigh=3G
MemoryMax=4G
MemorySwapMax=0
TimeoutStopSec=20
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    destinations = {
        UNIT: WORKER_UNIT,
        BASE / "run-8095.sh": LAUNCHER,
        Path("/home/coder/.config/systemd/user/auditmanager-local-8095.service.d/20-isolated-worker.conf"): LOCAL_OVERRIDE,
    }
    if not args.apply or args.dry_run:
        for path, content in destinations.items():
            print(f"\n{path}\n{content}")
        print("Apply: sudo python3 scripts/server/install_comparison_worker.py --apply")
        return
    if os.geteuid() != 0:
        parser.error("--apply requires root; no changes were made")
    account = pwd.getpwnam("coder")
    for relative in ("backend/app/services/version_comparison/worker.py", "scripts/server/version_comparison_local.py"):
        if not (REPO / relative).is_file():
            parser.error(f"Missing {relative}")
    user_ctl = ["runuser", "-u", "coder", "--", "env", f"XDG_RUNTIME_DIR=/run/user/{account.pw_uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{account.pw_uid}/bus", "systemctl", "--user"]
    # Existing 8095 work is deterministic; it is preserved by the worker queue.
    subprocess.run(user_ctl + ["stop", "auditmanager-local-8095.service"], check=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = BASE / "diagnostics" / f"worker-install-{stamp}"
    backup.mkdir(parents=True)
    os.chown(backup, account.pw_uid, account.pw_gid)
    for path, content in destinations.items():
        if path.exists():
            shutil.copy2(path, backup / (path.name + ".before"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if str(path).startswith("/home/coder/"):
            os.chown(path.parent, account.pw_uid, account.pw_gid)
            os.chown(path, account.pw_uid, account.pw_gid)
        path.chmod(0o755 if path.suffix == ".sh" else 0o644)
    QUEUE.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chown(QUEUE, account.pw_uid, account.pw_gid)
    subprocess.run(["systemd-analyze", "verify", str(UNIT)], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)
    subprocess.run(["systemctl", "enable", "auditmanager-comparison-worker.service"], check=True)
    subprocess.run(["systemctl", "restart", "auditmanager-comparison-worker.service"], check=True)
    subprocess.run(user_ctl + ["daemon-reload"], check=True)
    subprocess.run(user_ctl + ["enable", "--now", "auditmanager-local-8095.service"], check=True)
    print(f"Installed. Previous configuration: {backup}")


if __name__ == "__main__":
    main()
