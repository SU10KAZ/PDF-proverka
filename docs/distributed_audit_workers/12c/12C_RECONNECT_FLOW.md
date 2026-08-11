# Reconnect flow

`START → reserve epoch → connect loopback Gateway → AgentHello → CenterHello →
READY`. A stream loss clears readiness and the HTTPS stream binding, wakes
correlated waiters with a retryable local error, waits exponential backoff with
bounded jitter, reserves a new epoch and reconnects. Executor ownership is not
in this loop. Durable events/results remain on disk and replay after READY.

There is no busy loop and no polling fallback.
