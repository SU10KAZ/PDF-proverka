# EventOutbox flow

Executor and Agent write the existing disk EventOutbox. The transport converts
only a pending contiguous batch to EventBatch and waits for the correlated
EventAck. Only that ACK advances the existing cursor. Loss after send leaves the
cursor unchanged; replay is deduplicated at Center. CenterHello resume cursors
raise the local acknowledged floor atomically. Queue pressure cannot drop a
durable event because the source of truth remains the outbox file/SQLite sequence.
