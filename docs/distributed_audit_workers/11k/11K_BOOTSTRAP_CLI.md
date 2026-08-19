# Bootstrap CLI

One command starts the whole process:

```bash
python3 scripts/audit_worker_bootstrap.py install --spec new-worker.json --idempotency-key vps-2026-001
```

`new-worker.json` contains host, port, non-root SSH user, opaque `ssh_auth_ref`, out-of-band `expected_host_fingerprint`, install root, center HTTPS URL, display name and provider profile. It contains no password/key/token.

Continuation:

```bash
python3 scripts/audit_worker_bootstrap.py provider-auth wbs_... codex
python3 scripts/audit_worker_bootstrap.py resume wbs_...
python3 scripts/audit_worker_bootstrap.py session-status wbs_...
```

`provider-auth` reuses the pinned host key, exposes the provider login only on the attached terminal, and automatically runs `resume` after a successful action. Explicit `resume` is for browser/device completion, interruption or API-driven operation.

The same form exists for `update`, `repair`, `validate`, `status`, `rollback`, `uninstall` and `deregister`.
