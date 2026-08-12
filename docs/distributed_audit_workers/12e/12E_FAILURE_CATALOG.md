# 12E failure catalog

This catalog is intentionally evidence-first. `PASS_LOCAL` means an isolated
regression has passed; it is not a claim of physical `.31` completion.
`PENDING` is an explicit remaining acceptance gate.

| Category | Failure / recovery contract | Current evidence |
| --- | --- | --- |
| CONTROL | Gateway loss causes bounded reconnect, higher durable epoch and replay; it never fails the job. | C01/C02 `PASS_LOCAL_PROCESS` |
| CENTER | Offers, events, results and commands survive Gateway restart from isolated DB persistence. | C03/C04/C06 partial local coverage; exact windows pending |
| AGENT | Restart discovers a prior attempt/Executor and does not duplicate it. | graceful `PASS_LOCAL`; hard-kill windows pending |
| EVENTOUTBOX | Durable tail survives disconnect/restart, gaps fail safely, duplicate batches are idempotent. | local C11–C13 coverage; physical growth pending |
| DATA_PLANE | Resumable source/result transfer remains identity-bound and corruption fails closed. | validation/security regressions pass; interruption resume pending |
| RESULT | Validation precedes ResultAck; lost ACK is replayable and retention cannot start early. | local lost-ACK coverage passes; crash-after-ACK window pending |
| CANCEL | Online/offline/replayed cancellation is command-idempotent. | local command replay coverage; physical crash window pending |
| CERTIFICATE | Rotation, renewal outage and revocation never weaken authorization or duplicate an attempt. | 12D lifecycle suite passed; active-job variants pending |
| MULTI_SLOT | A fault/cancel in A must not disturb B; last-slot offer is atomic. | basic multi-slot local coverage; fault variants pending |
| BACKPRESSURE | Critical queue is bounded; durable outbox is the pressure buffer, not RAM. | local bounded-queue coverage passes; physical observation pending |
| RESOURCE | Low disk/health state stops new offers without deleting unacked data. | unit coverage exists; final relevant suite execution pending |
| OWNERSHIP | Polling and gRPC leases are mutually fenced; rollback is manual and zero-active only. | local ownership/fallback coverage passes |
| ROLLBACK | First canary rollback is allowed only after zero active attempts and reconciled outbox. | runbook drafted; not executed against production |

The authoritative per-scenario state is
[`12E_CHAOS_MATRIX.json`](12E_CHAOS_MATRIX.json).
