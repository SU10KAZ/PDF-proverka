# JobOffer flow

Gateway leases a job and records `center_to_agent` opaque transfer authorization.
The client validates/converts Proto through the shared adapters, deduplicates an
attempt within a stream runtime, and hands the ordinary assignment to
`WorkerAgent`. Local metadata is durable before download/accept. Source bytes use
HTTPS with bearer worker auth, active connection id and transfer id. JobAccept is
gRPC; the existing `worker.db` enqueue remains idempotent by attempt id, so a
re-offer cannot launch a second Executor.
