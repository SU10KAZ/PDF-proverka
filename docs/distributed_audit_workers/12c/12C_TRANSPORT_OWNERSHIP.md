# Transport ownership

The durable Center row `worker_transport_sessions.transport_mode` fences job
leasing. Gateway Hello claims `grpc_stream`; polling `/jobs/next` then conflicts
instead of creating a dual lease. In the Agent process exactly one object is
assigned to `WorkerAgent.client`. gRPC failure never instantiates `CenterClient`
as a polling controller. Switching mode requires Agent restart; the Executor and
its active process survive independently. The minimal controlled gRPC→polling
transition is permitted transactionally only after the Gateway has cleared the
active connection id; a live gRPC session still rejects polling and there is no
automatic fallback.
