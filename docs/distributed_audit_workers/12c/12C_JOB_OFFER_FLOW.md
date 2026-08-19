# JobOffer flow

Gateway leases a job and records `center_to_agent` opaque transfer authorization.
The client validates/converts Proto through the shared adapters and hands the
ordinary assignment, including repeated offers, to `WorkerAgent`. Agent core
owns durable idempotency and can resend an accept/decline after a lost ACK
without launching again. Local metadata is durable before download/accept.
Source bytes use HTTPS with bearer worker auth, active connection id and the
transfer id selected by exact attempt id. JobAccept is gRPC; the existing
`worker.db` enqueue remains idempotent by attempt id, so a re-offer cannot launch
a second Executor.
