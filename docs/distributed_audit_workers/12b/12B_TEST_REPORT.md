# Test report

The 12B suite uses real `grpc.aio` TCP sockets bound to `127.0.0.1:0` and only fake agents. It covers the required A–BO behaviors: config/bind guards, hello/version/identity, restart-persistent epoch fencing, heartbeat/capabilities, shared scheduler and atomic offers, accept/decline/multi-slot, progress/state validation, event dedupe/gap/cursors, cancel replay/ACK, HTTPS result descriptor/validation/ACK/reject/replay/retention, graceful shutdown/health, metrics/log safety, polling ownership, no 8443, and no inference.

Additional malformed cases cover wrong job routing, unknown enum, oversized safe text, duplicate stream sequence, wrong worker/attempt, invalid transition, transfer theft, and transport-level oversized messages.

Stress evidence: 20 concurrent streams each complete hello, offer, heartbeat, event persistence, and ACK. Backpressure evidence: 50 EventBatch writes occur before ACK reads; the durable cursor reaches 50. Outbound queues, event count, and message bytes are bounded.

Exact final command counts and commit hash are reported in the task handoff after immutable review.
