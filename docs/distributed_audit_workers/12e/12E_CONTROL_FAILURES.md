# Control and Center failure evidence

Verdict: `PASS` for C01–C06, C28, C29 and Center restart recovery.

- C01/C02 ran as real separate grpcio Gateway processes. Graceful SIGTERM and
  SIGKILL both preserved the Executor, advanced the durable connection epoch,
  replayed events, completed the same attempt and delivered ResultAck.
- C03/C04 use deterministic persisted-offer fault seams: a claim/offer survives
  lost delivery, and a delivered-but-unaccepted offer is reconciled without a
  conflicting attempt.
- C05 replays an event persisted before its ACK. `(attempt_id, sequence)` stays
  unique and the cursor remains contiguous. C06 replays ResultAck from durable
  accepted-result state after reconnect.
- C28 exposed and fixed a real defect: DB failures could previously escape the
  stream handler. The Gateway now returns typed `CENTER_DB_UNAVAILABLE`, sends
  no unsafe ACK, drops transient ownership, and accepts the event once after
  recovery (`8e206a00`).
- C29 physically held `BEGIN IMMEDIATE` on only the isolated DB for 8.000175 s.
  The Gateway stayed alive, reported one bounded `database is locked`, and the
  same attempt/Executor completed with sequence 216 and ResultAck. SQLite
  `integrity_check=ok`.

Production `:8081`, its DB and production processes were never fault targets.
