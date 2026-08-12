# 12F production BEFORE snapshot

Captured read-only on 2026-08-12 before any 12F production mutation.

## Center `.128`

Production backend PID `1931160` is healthy on `127.0.0.1:8081`. It runs from
`/home/coder/projects/PDF-proverka` at commit
`9168c3930c5e87e918f62c3812e19a8f924a3ade`, branch
`feature/block-vector-graphs`. 12F did not signal or restart it.

The running process has no `DISTRIBUTED_WORKERS_ENABLED` or
`DISTRIBUTED_WORKERS_DATA_DIR` environment entries. Its configuration default
is disabled, and the loaded production `backend/app/main.py` does not register
the distributed-worker router. The default distributed data directory and
`workers.db` do not exist. Consequently no production Center worker record,
queue, transport ownership, pending result, pending cancel, lease, or ACK
cursor can be verified.

Only `127.0.0.1:8081` listens. `:8443` and `:9443` are absent. UFW remains
active with the existing source-scoped `176.12.77.31 -> TCP/8443` allow; there
is no worldwide `8443` rule and no `9443` rule. nginx is active and Caddy is
inactive. Pre-existing cloudflared PID `1263127` still targets
`http://127.0.0.1:8081`; 12F did not modify it.

## Production Worker `.31`

The target is unambiguous:

- worker: `wrk_19c87718`;
- instance: `inst_boot_e129036dddf5c59049080ddd15624e72`;
- user: `auditworker_11l`;
- install root: `/home/auditworker_11l/audit-worker-11l`;
- polling Agent PID `1575036`, active;
- Executor PID `1384880`, active;
- release `20260811T055247-743dfe7fa00e`;
- pipeline revision `e05f44ef4a0f732d8c3fb3b30e7dc8ed11939f4e`;
- configured `max_slots=1` and real-audit slots `1`.

Local SQLite is idle: zero active execution attempts, zero running process
rows, zero pending local commands, and zero unresolved local leases. The old
schema has 24 durable `event_journal` rows but no 12D/12E EventOutbox/ACK
cursor surface, so Center reconciliation cannot be proven.

The polling process is alive but its state is `center_unreachable`; its
pre-existing Quick Tunnel dispatcher hostname currently fails DNS resolution.
No Worker inbound listener was created. Provider live readiness and production
mTLS were not probed after the upstream Center hard gate failed.

## Fail-closed conclusion

The Worker-local idle condition alone is insufficient. The required
simultaneous Center proof is unavailable, and the production Center cannot
serve a 12F job in its current runtime. Enabling that subsystem would require
redeploying/restarting production `:8081`, which task 12F explicitly forbids.
Starting a separate isolated Center would not constitute the requested
production queue/data/services path. Cutover therefore stopped before deploy,
Gateway start, polling stop, provider access, job creation, or inference.
