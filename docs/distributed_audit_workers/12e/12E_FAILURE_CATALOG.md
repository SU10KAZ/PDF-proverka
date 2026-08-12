# 12E failure catalog

All 41 mandatory critical scenarios pass. Process-scoped/physical tests are
used where process ownership or networking is the subject; deterministic
regression seams are used for sub-request crash windows where an arbitrary
sleep would make the result weaker.

| Category | Fault / recovery contract | Evidence | Result |
| --- | --- | --- | --- |
| CONTROL | Gateway loss, offer/event/result ACK loss; recover only from persistence | C01–C06 | PASS |
| CENTER | DB unavailable/locked; no unsafe ACK and bounded recovery | C28–C29 | PASS |
| AGENT / EXECUTOR | graceful/hard Agent loss, accept/launch windows; one Executor | C07–C10 | PASS |
| EVENTOUTBOX | offline durable tail, gap and duplicate replay | C11–C13 | PASS |
| DATA / RESULT | resumable source/result, corrupt reject, validation/ACK recovery | C14–C20 | PASS |
| CANCEL | online/offline/restart/terminal idempotency | C21–C24 | PASS |
| CERTIFICATE | rotation, issuer outage, active revocation | C25–C27 | PASS |
| MULTI_SLOT | two-attempt isolation and 20-way last-slot race | C30–C32 | PASS |
| OWNERSHIP | polling/gRPC fence and no automatic fallback | C33–C34 | PASS |
| BACKPRESSURE | bounded queues and jittered reconnect herd | C35–C36 | PASS |
| RESOURCE | simulated disk/swap/capability degradation | C37–C38 | PASS |
| DUPLICATES | JobOffer, ResultReady and ResultAck replay | C39–C41 | PASS |
| ROLLBACK | explicit gRPC→polling only after zero-active reconciliation | runbook | READY |

Hard outcomes: job loss 0; duplicate Executor 0; premature deletion 0;
unrecoverable outbox loss 0; unauthorized/cross-worker acceptance 0; silent
polling fallback 0.
