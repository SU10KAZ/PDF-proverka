# 12F final report

## Verdict

- `12F PRODUCTION CANARY = FAIL (BLOCKED_PRE_CUTOVER)`
- `FIRST PRODUCTION CANARY = FAIL`
- `ONE WORKER CUTOVER = NOT_STARTED`
- `WORKER .31 TRANSPORT = POLLING_UNCHANGED`
- `CONTROL CHANNEL = NOT_STARTED`
- `REAL PROJECT = NOT_STARTED`
- `RESULT ACK = NOT_STARTED`
- `EXACT ROUTING = NOT_EVALUATED`
- `EVENTOUTBOX = NOT_PROVEN_AGAINST_CENTER`
- `NORMAL SCHEDULING = DISABLED / NO 12F JOB CREATED`
- `GENERAL ROLLOUT = NOT_DONE`

Base commit is `73486e7af4f193d2f6efd73c5bf069c14bfbf764`, whose
`12E_12F_ENTRY_GATE.json` correctly allows beginning a separately authorized
12F preflight. It does not assert that the unrelated running production
backend already contains or enables that release.

## Fail-closed hard gate

The healthy production backend on `127.0.0.1:8081` runs PID `1931160` from
commit `9168c3930c5e87e918f62c3812e19a8f924a3ade`. Its environment does not set
`DISTRIBUTED_WORKERS_ENABLED`; the default is false; its loaded `main.py` does
not register distributed-worker routes; and no production distributed data
directory or `workers.db` exists.

The physical Worker is unambiguous (`wrk_19c87718`, instance
`inst_boot_e129036dddf5c59049080ddd15624e72`) and locally idle, but Center-side
identity, queue, ownership, lease, pending result/cancel and ACK cursor cannot
be checked. Its existing polling Agent is active but reports
`center_unreachable` because its old Quick Tunnel hostname no longer resolves.

Activating the exact 12F Center in the production backend requires a planned
production deployment/restart. That would contradict the explicit rule not to
stop or intentionally restart `:8081`. A separate isolated Center would not be
the requested production queue/data/services path. The session therefore
stopped before candidate deploy, Gateway start, certificate work, polling
Agent stop, provider live probe, document selection, or job creation.

## Production integrity

Production backend PID `1931160`, polling Agent PID `1575036`, Executor PID
`1384880`, nginx and pre-existing cloudflared PID `1263127` remain active and
were not signalled or reconfigured by 12F. No database, UFW, nginx, Caddy or
Cloudflare change occurred. The existing UFW allow remains source-scoped from
`.31` to `8443`; no `:8443` or `:9443` listener exists and no `9443` rule was
added. No tunnel, SSH forwarding or overlay path was used by 12F.

Jobs/attempts/Executor executions created by 12F are `0/0/0`. Provider
inference is Claude/Codex/OpenRouter `0/0/0`. Rollback was unnecessary because
ownership and transport never changed. Push: `NO`. Merge: `NO`.
