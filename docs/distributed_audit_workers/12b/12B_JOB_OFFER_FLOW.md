# Job offer flow

The gateway invokes `repositories.claim_next_job_for_worker`, the same atomic claim used by polling. In one database transaction it verifies `grpc_stream` ownership and slot constraints, transitions `assigned` to `source_uploading`, journals the transition, persists `gateway_job_offers`, and records the source transfer identity. Only after commit is `JobOffer` queued on the stream.

The offer contains metadata and an HTTPS package descriptor; it never contains package bytes. `JobAccept` verifies worker/attempt ownership, source hash confirmation, routing hash, and execution revision, then uses `job_service.transition`. Duplicate accept of an already accepted attempt is idempotent. Temporary decline returns an unaccepted attempt to `assigned`; permanent decline follows the existing failure transition.

Free capacity is `max_slots - active_slots - sent_offers`, so a connection cannot be over-offered while accepts are pending.
