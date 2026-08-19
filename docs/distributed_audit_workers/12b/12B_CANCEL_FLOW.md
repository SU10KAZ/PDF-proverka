# Cancel flow

The existing center action persists a command first. Gateway reads `repositories.pending_commands`, emits the existing `command_id`, and records delivery using the established command repository. A gateway crash before or after delivery leaves the command repeatable after reconnect.

`CancelAck` validates command/worker identity and maps the protobuf stage to the existing outcome vocabulary. `repositories.ack_command` provides idempotency/conflict detection; `attempt_service.apply_cancel_ack` owns the attempt transition. Duplicate delivery is safe because command identity is stable.

Connection loss by itself never fabricates a cancel or failure.
