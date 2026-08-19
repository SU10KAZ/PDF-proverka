# 12F.1 enablement map

`DISTRIBUTED_WORKERS_ENABLED=true` is necessary but not sufficient.

The 12E reference runtime conditionally registers the machine polling router,
portal-protected admin/bootstrap routers and status surface. The first DB
operation calls `database.ensure_ready()`, which creates a separate
`workers.db`, applies transactional migrations 0→11 and enables WAL,
`synchronous=NORMAL`, `busy_timeout=5000` and foreign keys. Registry, events,
ACK cursors, transport sessions and Gateway repositories share that DB.

Remote audit scheduling is a separate gate:
`DISTRIBUTED_AUDIT_EXECUTION_ENABLED=false` leaves the existing local
AuditManager path unchanged. There is no automatic worker selection or
background dispatch; assignments require an authenticated operator action.

The remaining exact changes are blocked by production facts:

1. The production release is not immutable or reproducible. It combines
   commit `9168c393...` with uncommitted runtime files, some changed after the
   process started. The 12E line also diverges from production by 11 committed
   product commits.
2. Distributed portal roles have no approved subject mapping.
3. The live `.env` is mode `0664` and the process umask is `0002`; adding a
   bootstrap credential or creating the DB under those permissions is unsafe.
4. The old polling Agent uses a dead Quick Tunnel hostname. Current
   cloudflared has a different URL, so a Center-only restart cannot satisfy the
   required physical polling reconnect gate.

No Phase A workaround copies isolated DB/PKI state or edits production.
