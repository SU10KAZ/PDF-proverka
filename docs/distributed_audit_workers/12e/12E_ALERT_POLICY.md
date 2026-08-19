# Operator alert policy

These are operational thresholds for the first canary, not invented product
SLOs. The read-only `audit-worker doctor` surface provides the named fields.

| Condition | Severity | Operator action |
| --- | --- | --- |
| `grpc_connection_state != connected` longer than the configured reconnect ceiling | warning | inspect typed disconnect reason and direct path; do not enable polling automatically |
| outbox pending count grows across two observations | high | stop new offers, preserve local data, inspect ACK cursor |
| certificate near expiry or `CERT_EXPIRED`/`CERT_REVOKED` | high | stop new offers and use documented renewal/replacement procedure |
| reconnect rate exceeds expected backoff | high | stop canary and inspect Gateway/DB evidence |
| pending result has no ResultAck | high | reconcile; do not delete result |
| offer/cancel is stuck past its bounded timeout | high | inspect ownership and durable command state |
| `worker_accepting_jobs=false` from disk/resource/provider state | warning | do not force a lease; correct the reported condition |

No alert handler is allowed to switch the transport to polling automatically.
