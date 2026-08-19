# EventOutbox flow

`EventBatch` is identity checked, count/size bounded, semantically validated, and converted by the v1 adapter. `event_service.ingest_batch` remains the single persistence implementation. It stores events idempotently and advances the highest contiguous cursor in its established transaction.

`EventAck` is built only from the persisted result. Replayed events are counted as duplicates and return the same durable cursor. A gap produces a retryable typed error with the expected next sequence. On reconnect, `CenterHello.resume_cursors` comes from `repositories.cursors_for_worker`; gateway memory is not authoritative.

If the gateway crashes after persistence and before ACK, worker replay is safe and does not create duplicate domain events.
