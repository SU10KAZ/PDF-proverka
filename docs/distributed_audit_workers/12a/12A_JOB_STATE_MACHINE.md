# 12A — job state machine

Proto `JobState` — точное отображение текущего domain enum, не новый lifecycle.

```text
CREATED → ASSIGNED → SOURCE_UPLOADING → SOURCE_READY
                    (atomic /jobs/next claim; future JobOffer)
SOURCE_READY → ACCEPTED_BY_WORKER → RUNNING → COMPLETED_LOCALLY
             (JobAccept after source verification)
COMPLETED_LOCALLY → RESULT_UPLOADING → RESULT_RECEIVED → VALIDATING
VALIDATING → COMPLETED  (validated ResultAck; retention starts)
VALIDATING → FAILED     (typed ResultRejected / terminal validation failure)

nonterminal → CANCEL_REQUESTED → CANCELLED (only after worker stop ACK)
active attempt superseded → SUPERSEDED_RESULT_RECEIVED (stored, never published)
```

Полный enum: `created`, `assigned`, `source_uploading`, `source_ready`, `accepted_by_worker`, `running`, `completed_locally`, `result_uploading`, `result_received`, `validating`, `completed`, `failed`, `cancel_requested`, `cancelled`, `superseded_result_received`.

`JobOffer` отправляется только после persisted/atomic central claim. Это устраняет race между offer и claim: accept не соревнуется за queue item, а подтверждает уже назначенный attempt. Offer lease expiry и потеря worker решаются центральным attempt/lease recovery; тишина connection не меняет execution state в failed.

Progress observational. EventOutbox durable. Connectivity (`online/stale/offline/reconnecting`) и retention — отдельные оси. Один worker имеет несколько независимых attempts до `max_slots`; connection state никогда не является job state.
