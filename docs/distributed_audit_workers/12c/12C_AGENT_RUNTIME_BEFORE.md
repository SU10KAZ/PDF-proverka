# 12C Agent runtime before changes

The real network Agent is `audit_worker.agent.WorkerAgent`; the real Executor is the
separate `audit_worker.executor` process. The durable seam between them is
`worker.db`. Agent restart observes surviving Executor work and never launches a
second pipeline process.

Before 12C all control-plane calls and all package bytes used `CenterClient` over
HTTPS polling. `EventOutbox`, `LocalJobStore`, `LocalDB`, resumable upload and the
result package were already durable and are shared runtime components. There was
no worker-side gRPC client, durable connection epoch, stream reconnect manager, or
transport selector.

The 12B Proto deliberately excludes execution tokens. Source offers already create
an opaque `center_to_agent` transfer authorization, so 12C must consume that
authorization on HTTPS without putting secrets into Proto. Result bytes continue
over HTTPS and the authoritative retention acknowledgement comes from ResultAck.

Production defaults before the change: polling, verified HTTPS, real provider
inference disabled. Those defaults must remain unchanged.
