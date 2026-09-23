# OpenRouter kill switch V1

Production service: `systemctl --user ... auditmanager-backend.service`, user `coder`.
Unit: `/home/coder/.config/systemd/user/auditmanager-backend.service`.
Effective application path: `/home/coder/auditmanager/current/app`.
Environment files, in load order:

1. `/home/coder/.config/auditmanager/backend.secrets.env`
2. `/home/coder/.config/auditmanager/backend.phaseb.env` (put the switch here).

`AUDIT_OPENROUTER_ENABLED` accepts `1/true/yes/on` or `0/false/no/off`, ignoring case.
Unset means enabled. All other values, including empty or whitespace-padded values,
fail closed with `openrouter_invalid_enable_flag`. Disabled returns `OPENROUTER_DISABLED`.
The flag does not authorize inference and does not replace existing paid API or
per-request permission gates. Do not send a canary or quota request to verify it.

## Safe operational procedure

First inspect the authenticated `/api/audit/batch/status` and `/api/audit/live-status`,
preparation queues, Project Comparison V3 runtime status, distributed job states,
and the backend systemd cgroup/child processes. Stop the operation if there are
active or queued jobs, inference child processes, or critical work; do not kill
provider calls. Exclude concurrent job submission during the maintenance window.

The environment is process-scoped. Editing an EnvironmentFile alone does **not**
change the running backend. Every new request/retry reads the current process
environment; changing that environment in-process takes effect on the next call.
For operational file changes, restart the idle service as below.

To ENABLE, run the following with `AUDIT_GATE_VALUE=1`; to DISABLE, use
`AUDIT_GATE_VALUE=0`. This backs up the complete prior file privately, then atomically
changes only the switch. Do not attach the backup to a report: it may contain secrets.

```bash
AUDIT_GATE_VALUE=0 python3 - <<'PY'
import os, re, shutil, time
from pathlib import Path
value = os.environ['AUDIT_GATE_VALUE']
assert value in {'0', '1'}
p = Path('/home/coder/.config/auditmanager/backend.phaseb.env')
backup_dir = Path('/home/coder/.config/auditmanager/openrouter-killswitch-backups')
backup_dir.mkdir(mode=0o700, exist_ok=True)
backup = backup_dir / (p.name + '.' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
assert not backup.exists()
shutil.copy2(p, backup)
backup.chmod(0o600)
content = re.sub(r'^AUDIT_OPENROUTER_ENABLED=.*\n?', '', p.read_text(), flags=re.M)
tmp = p.with_name(p.name + '.killswitch.tmp')
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as f:
    f.write(content.rstrip('\n') + '\nAUDIT_OPENROUTER_ENABLED=' + value + '\n')
    f.flush()
    os.fsync(f.fileno())
os.replace(tmp, p)
print('Backup:', backup)
print('Configured AUDIT_OPENROUTER_ENABLED=' + value)
PY
systemctl --user restart auditmanager-backend.service
systemctl --user is-active auditmanager-backend.service
curl --fail --silent http://127.0.0.1:8081/api/info >/dev/null
```

Verify using an authenticated same-origin browser console (no model request):

```javascript
(await (await fetch('/api/audit/model', {credentials: 'same-origin'})).json()).openrouter
```

Expected for disabled: `{"openrouter_enabled":false,"source":"environment"}`.
Expected for enabled: `{"openrouter_enabled":true,"source":"environment"}`.
Only those two fields are exposed. SSE uses an explicit error event; REST uses
HTTP 503 with code/message. A denied audit ends FAILED and retains earlier results.
Provider fallback from that job is stopped; independent direct-provider jobs continue.

## Independently deployed processes

Workers and Agent Gateway have their own OS environments and immutable releases.
Before opening their intake, deploy the same gate code and set the same flag in
their service environments; restart only when idle. Worker pipeline children inherit
the flag through the explicit allowlist. The center rejects new/frozen remote
OpenRouter plans while disabled. V1 is not a remote runtime configuration bus:
editing the center's EnvironmentFile cannot mutate an already running remote worker.

At this deployment the only registered worker is offline with intake disabled,
zero active local jobs/processes, and only terminal attempts. Its intake is left
unchanged. The existing gateway/certificate issuer restart failures are unrelated
and are not repaired or restarted by this task.

## Offline verification

```bash
AUDIT_DISABLE_DOTENV=1 PYTEST_PLUGINS=tests.openrouter_killswitch_safety \
  /home/coder/auditmanager/current/venv/bin/python -m pytest \
  tests/test_openrouter_kill_switch.py tests/test_stage01_openrouter_retry.py -q
```

Run from the release's `app` directory or canonical repository. All ON probes use
fake transports; do not test ON against OpenRouter. Research/evaluation files are
not needed. The production state at the end of this task must stay **DISABLED**.
