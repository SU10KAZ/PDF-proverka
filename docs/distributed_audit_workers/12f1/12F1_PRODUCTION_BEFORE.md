# 12F.1 production BEFORE

The initial snapshot was read-only. Production backend PID `1931160` was
healthy on `127.0.0.1:8081` and was not signalled or restarted by 12F.1.

## Process and release identity

The backend has no dedicated service unit. It is an orphaned child in
`session-10184.scope`, launched as:

`/opt/py312/bin/python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8081`

Its cwd is the mutable main worktree at commit `9168c393...`. That worktree
has 23 tracked and 20 untracked runtime changes. Four backend modules changed
after PID `1931160` started, including the stage-comparison router and store.
The exact initial in-memory release therefore could not be reproduced from
either the Git commit or the then-current disk snapshot.

During Phase A the backend independently changed PID to `1959493`, started at
`17:16:15`. 12F.1 did not signal or restart production. No runtime file changed
after the new PID started, so it loaded the current dirty disk snapshot; that
snapshot remains uncommitted, unpackaged and mutable, not an immutable release.

## Configuration and data

The application loads the live root `.env`. It is owned by `coder:coder` and
has mode `0664`; no secret values were printed or changed. Portal auth is on,
but no distributed-worker flag, DB path, bootstrap secret, role map, remote
execution flag, or pipeline revision is configured.

AuditManager currently has no SQLite project DB. Its existing production
state is filesystem/JSON-based under the reported audit/app/project roots.
There is no `workers.db` and no open SQLite fd.

## Infrastructure and Worker

UFW, nginx, Caddy, cloudflared and listeners match the 12F snapshot. The
source-scoped `.31 -> 8443` rule remains; `:8443` and `:9443` do not listen.
Pre-existing cloudflared PID `1263127` remains unchanged and targets `:8081`.

Worker `.31` Agent PID `1575036` and Executor PID `1384880` remain active and
untouched. The Agent is configured for an expired/unresolvable Quick Tunnel
hostname `conviction-medicaid-coupled-tricks.trycloudflare.com`. Current
cloudflared advertises `sas-nam-nurses-produce.trycloudflare.com`, which does
resolve and returns production health 200. Center deployment alone cannot
make the existing Agent reconnect while its dispatcher host remains stale.

Provider inference by 12F.1 is `0/0/0`.
