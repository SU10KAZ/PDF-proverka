# Transport ownership

Workers without a transport session retain the existing polling behavior. An accepted gateway hello durably selects `grpc_stream`. `claim_next_job_for_worker` checks the selected mode inside the claim transaction, so polling cannot lease a second job to that worker. Gateway uses the same repository with explicit `grpc_stream` ownership.

When a gRPC stream is lost, 12B does not silently fall back to polling. The durable mode remains gRPC until a future explicit cutover policy is implemented. This protects against dual delivery while leaving all un-migrated polling workers unchanged.

The real Audit Worker remains a polling client in 12B; only fake agents select the new ownership mode in isolated tests.
