# EventOutbox failure evidence

Existing executed regressions cover a fail-safe resume cursor (including an
impossible ACK that must not delete local data), typed gap handling, duplicate
event idempotency, bounded queueing, reconnect-event persistence and the
connection-epoch stale-sender defect discovered during C02.

The final physical run must produce a bounded offline tail, record its final
contiguous ACK and show zero unexplained local unacked events after recovery.
