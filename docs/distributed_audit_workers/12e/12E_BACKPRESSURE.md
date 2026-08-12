# Backpressure and reconnect herd

Verdict: `PASS` for C35/C36.

The client critical queue is bounded; heartbeats and capability snapshots
coalesce. Fifty ordered event batches converge durably under a slow consumer.
Twenty reconnect candidates use exponential backoff with ±20% jitter: every
sample remained inside the 1/2/4-second bounds and at least 15/20 distinct
millisecond buckets appeared in each round. The 12-failure transport test also
records 12 gRPC attempts, 11 reconnects and exactly zero polling calls.
