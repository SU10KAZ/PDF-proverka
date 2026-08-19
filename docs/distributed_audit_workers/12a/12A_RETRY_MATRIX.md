# 12A — retry matrix

| Condition | Actor / retry | State invariant |
|---|---|---|
| Stream disconnected | agent reconnect with backoff/new epoch/hello | local job continues; outbox appends |
| Center restart | agent reconnect/resume | no Executor restart or failed transition |
| Agent restart | agent discovers processes/attempts and replays disk outbox | central attempt remains authoritative |
| Retryable application error | sender repeats same domain identity after advised delay | idempotency key unchanged |
| Backpressure | agent pauses stream sends, keeps durable append | no event loss; obey negotiated window |
| Revision/policy mismatch | fail closed; update/admin workflow outside stream | job does not start |
| Unsupported major/protocol violation | close/reject; no best effort | no business mutation |
| Auth failure (future mTLS) | do not retry blindly; bootstrap/admin repair | no application credential in payload |
| Non-retryable job/validation error | typed decline/reject/failed path | transport reconnect cannot hide it |
| Provider transient/model retry | worker ProviderAdapter policy only | center does not command a second model call |

Cancel repeats the same `command_id`. Events restart from `highest_contiguous_sequence + 1`. Upload continues only missing HTTPS chunks. Result deletion is never a retry response; it remains forbidden until validated ResultAck.
