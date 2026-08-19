# Client crash matrix

| Window | Durable fact before network | Recovery |
|---|---|---|
| JobOffer before JobAccept | Gateway offer; optional local assignment metadata | re-offer; `worker.db` attempt enqueue is idempotent |
| JobAccept before Executor launch | durable `verified`/`accepted` metadata precedes local enqueue | startup recovery resends idempotent JobAccept, then enqueues the same attempt once |
| Executor running when Agent dies | process registry + queue owned by Executor | new Agent adopts observer; never launches process |
| Event sent before ACK | EventOutbox cursor not advanced | replay same sequence; Center deduplicates |
| ResultReady before ResultAck | archive and upload session durable | resend ResultReady after reconnect |
| ResultAck before local retention save | Center accepted state is authoritative | resumable upload/ResultReady returns idempotent ACK |

Connection epoch is always persisted before network. Result retention is persisted
only after ACK. These orderings close the “network said it, local state forgot”
windows without a second business path.

Late re-offers also consult durable metadata and `worker.db`, not only in-memory
Agent threads. Terminal attempts are never recreated; a pre-dispatch attempt is
resumed without re-downloading or overwriting its metadata.
