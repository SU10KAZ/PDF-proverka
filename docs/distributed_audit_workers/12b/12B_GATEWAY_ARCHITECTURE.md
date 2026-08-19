# 12B gateway architecture

`python -m backend.app.agent_gateway` starts a separate `grpc.aio` process. It registers generated `AgentStreamService` bindings and the standard gRPC health service, then binds only a validated loopback IP and a dynamic test port.

The control plane is the bidirectional `Connect` stream: hello, heartbeat, capabilities, offers, accept/decline, progress, event metadata/ACK, cancel, and result metadata/ACK. Package bytes remain on the existing HTTPS data plane. Administration remains in the existing center APIs. These planes are deliberately not collapsed.

`AgentStreamService` performs stream validation and bounded queuing. `GatewayDomainAdapter` maps protobuf messages to existing domain functions. `GatewayConnectionRegistry` holds ephemeral live stream objects only. `gateway_repository` stores the minimum durable network facts needed for restart recovery. SQLite job/event/command/result tables remain authoritative.

No Worker Agent transport was changed, no real worker was connected, and no production service definition, proxy, firewall, or port was created.
