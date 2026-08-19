# EventOutbox failure evidence

Verdict: `PASS` for C11–C13.

The real Agent network-loss regression preserves a durable offline tail across
reconnect and process state, then drains it to the Center cursor before result
acknowledgement. Gap handling requests the missing sequence instead of resetting
the outbox. Replayed batches remain idempotent by `(attempt_id, sequence)`.

Final physical reconciliation found 3314/3314 local event-journal rows marked
written, zero duplicate Center sequences, zero live process rows and zero
pending commands. No unexplained unacknowledged tail remained.
