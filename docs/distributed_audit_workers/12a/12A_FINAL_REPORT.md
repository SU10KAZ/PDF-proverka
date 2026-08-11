# 12A — final report

Status before immutable review: **candidate complete**. Base: `0fc81cdbcc8f59a2925949b024338c29afbb5d13` from finalized 11L branch.

## Result

The repository now has one strict versioned language for a future Agent ↔ Center control stream: package `auditmanager.agent_stream.v1`, service `AgentStreamService`, bidi RPC `Connect`. Generated message bindings and descriptor compile reproducibly; pure adapters preserve the existing HTTP/domain semantics without importing protobuf into business services.

Agent→Center payloads: AgentHello, Heartbeat, CapabilitiesChanged, JobAccept, typed JobDecline, ProgressUpdate, EventBatch, JobStatusUpdate, ResultReady, CancelAck, ErrorStatus. Center→Agent: CenterHello, JobOffer, CancelCommand, EventAck, ResultAck, typed ResultRejected, ErrorStatus.

JobOffer is emitted only for a centrally persisted atomic claim and semantically replaces successful `/jobs/next`, not the scheduler/domain model. JobAccept follows source verification. EventOutbox remains durable; reconnect replays only after center's highest contiguous ACK. Disconnect never marks a job failed. Cancel is command-idempotent and requires worker stop acknowledgement. ResultAck means central package validation succeeded/stored with the stated outcome; only then does retention begin, and before it automatic deletion is forbidden.

Large packages remain resumable HTTPS via opaque transfer descriptors. The gRPC control stream carries no package bytes. SSH/bootstrap remains admin plane, not runtime. No credential or arbitrary shell/admin field exists. Multi-slot, explicit major negotiation and strictly-greater connection-epoch supersession are represented.

Current HTTP polling still works and was not edited. No live gRPC server/client, listener, mTLS, :8443, production change, audit or model call was performed. Real inference: **0**.

Tests: 45 protocol/compatibility cases and 103 separately reproducible existing regression cases pass; see `12A_TEST_REPORT.md`. Immutable six-lens review and exact final commit are recorded after candidate commit in the final revision.

12B may proceed only after implementing and proving the unresolved gateway/mTLS/fencing/backpressure/data-plane-authorization/cutover issues listed in `12A_KNOWN_ISSUES.md`.
