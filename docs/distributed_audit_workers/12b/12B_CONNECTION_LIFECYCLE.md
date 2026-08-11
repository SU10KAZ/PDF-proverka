# Connection lifecycle

1. The server requires `AgentHello` as the first application message.
2. It validates the envelope, worker/instance identity, protocol major, and positive durable epoch.
3. A strictly greater epoch is committed and any older connection is fenced.
4. Capabilities are projected through existing registration/provider services.
5. `CenterHello` returns negotiated limits, duplicate policy, and persisted event cursors.
6. A reader handles inbound control messages while a bounded outbound loop delivers offers, commands, and persisted result outcomes.
7. Heartbeat and idle timeouts close only the connection. They never mark an attempt failed.
8. Drain stops new offers, closes streams gracefully, marks health not-serving, and stops the separate gRPC server.

An orderly or abrupt disconnect clears only the matching active connection id. The epoch and `grpc_stream` transport ownership remain durable for safe reconnect/cutover policy.
