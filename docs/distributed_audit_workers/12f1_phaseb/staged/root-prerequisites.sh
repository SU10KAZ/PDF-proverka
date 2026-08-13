#!/usr/bin/env bash
set -euo pipefail

install -d -o coder -g coder -m 0700 /var/lib/auditmanager
install -d -o coder -g coder -m 0700 /var/lib/auditmanager/distributed_workers
loginctl enable-linger coder

stat -c '%n %U:%G %a' \
  /var/lib/auditmanager \
  /var/lib/auditmanager/distributed_workers
loginctl show-user coder -p Linger -p State -p RuntimePath
