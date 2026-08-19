# Test report

The 12B suite uses real `grpc.aio` TCP sockets bound to `127.0.0.1:0` and only fake agents. It covers the required A–BO behaviors: config/bind guards, hello/version/identity, restart-persistent epoch fencing, heartbeat/capabilities, shared scheduler and atomic offers, accept/decline/multi-slot, progress/state validation, event dedupe/gap/cursors, cancel replay/ACK, HTTPS result descriptor/validation/ACK/reject/replay/retention, graceful shutdown/health, metrics/log safety, polling ownership, no 8443, and no inference.

Additional malformed cases cover wrong job routing, unknown enum, oversized safe text, duplicate stream sequence, wrong worker/attempt, invalid transition, transfer theft, and transport-level oversized messages.

Stress evidence: 20 concurrent streams each complete hello, offer, heartbeat, event persistence, and ACK. Backpressure evidence: 50 EventBatch writes occur before ACK reads; the durable cursor reaches 50. Outbound queues, event count, and message bytes are bounded.

Final protocol/gateway command: `92 passed` across `test_agent_stream_protocol_v1.py` and `test_agent_gateway_12b.py`. Existing polling/e2e/hardening/feature-off/11L regression selection also exited successfully. Proto regeneration retained descriptor SHA-256 `b17f857c47e5ce904a6b6283d8b1e1c2d74b31ce4fbd6bc728823b58c0f9e324`; Python compilation, JSON parsing, and `git diff --check` passed. The final commit hash is reported in the task handoff.
