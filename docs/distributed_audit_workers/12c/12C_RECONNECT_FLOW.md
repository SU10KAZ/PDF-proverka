# Reconnect flow

`START → reserve epoch → connect loopback Gateway → AgentHello → CenterHello →
READY`. A stream loss clears readiness and the HTTPS stream binding, wakes
correlated waiters with a retryable local error, waits exponential backoff with
typed min/max/jitter bounds, reserves a new epoch and reconnects. Executor
ownership is not in this loop. Durable events/results remain on disk and replay
after READY. The client exposes low-cardinality attempts/success/disconnect/
reconnect counters. A real one-minute idle-stream test proved one connection
epoch, periodic heartbeat, zero disconnect and no reconnect/busy loop.

There is no busy loop and no polling fallback.
