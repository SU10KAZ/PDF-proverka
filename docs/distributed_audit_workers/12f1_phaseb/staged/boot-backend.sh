#!/usr/bin/env bash
set -euo pipefail

export XDG_RUNTIME_DIR="/run/user/$(id -u)"
if ! systemctl --user is-enabled --quiet auditmanager-backend.service 2>/dev/null; then
    echo "[auditmanager-boot] immutable auditmanager-backend.service is not enabled" >&2
    exit 1
fi

exec systemctl --user start auditmanager-backend.service
