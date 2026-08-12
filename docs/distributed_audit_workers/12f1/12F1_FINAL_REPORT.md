# 12F.1 Phase A final report

## Verdict

- `12F.1 PHASE A = PARTIAL / BLOCKED`
- `READY_FOR_OPERATOR_RESTART = NO`
- `PRODUCTION RESTART = NOT_DONE`
- `PRODUCTION DEPLOY = NOT_DONE`
- `WORKER CUTOVER = NOT_DONE`
- `REAL AUDIT = NOT_RUN`
- `PROVIDER INFERENCE = 0/0/0`

## What is proven

The 12F commit `dc2cfb92...` is docs-only; its runtime is final 12E
`73486e7a...`. That reference runtime builds/imports, starts on an isolated
loopback port in `1.992732 s`, initializes a fresh separate schema-v11 WAL DB
in `0.006007 s`, serves existing core and distributed routes, enforces portal
auth, supports the legacy polling contract and leaves remote execution and
automatic dispatch disabled. Relevant suites passed `658`, with `1` expected
skip and no inference.

## Why restart is not ready

Production is not actually an immutable `9168c393...` release. Initial PID
`1931160` ran from a mutable worktree with 23 tracked and 20 untracked runtime
changes; four backend modules changed after it started. The backend then
independently restarted as PID `1959493` at `17:16:15`; 12F.1 did not signal or
restart it. No runtime file changed after that time, but the loaded disk
snapshot remains uncommitted and unpackaged. The 12E line also lacks 11
committed production-only product commits. There is no immutable production
baseline to integrate or roll back to without risking existing AuditManager
behavior.

Production config is also incomplete: distributed roles and bootstrap secret
are absent, `.env` is mode `0664`, and the process umask is `0002`. Finally,
the physical polling Agent uses an unresolvable old Quick Tunnel hostname;
current unchanged cloudflared advertises a different working URL. A Center-only
restart cannot meet the required polling reconnect acceptance.

## Required next authorization/input before a new Phase A candidate

1. Resolve the operator-owned dirty production runtime into an approved clean
   immutable baseline, explicitly deciding which uncommitted comparison/UI
   changes are production requirements.
2. Approve the portal subject-to-role mapping for viewer/operator/admin.
3. Approve a stable polling dispatcher endpoint and the separate Worker Agent
   config/restart needed to leave polling transport unchanged but reachable.
4. Establish an immutable previous release and supervised launcher for exact
   rollback.

Only then can the baseline be integrated with 12A–12E, rebuilt, rerun and
reviewed into a candidate whose restart gate is all true except the separate
operator authorization. Push: `NO`. Merge: `NO`.
