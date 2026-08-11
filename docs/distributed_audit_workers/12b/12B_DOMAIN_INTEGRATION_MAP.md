# 12B domain integration map

Gateway is a protobuf transport adapter. It does not call FastAPI over localhost and does not own a second scheduler, state machine, event store, command store, result validator, or retention policy.

| Operation | Existing HTTP entry point | Reused domain seam | Authoritative persistence |
|---|---|---|---|
| worker lookup/registration | register and status routes | `repositories.get_worker`, `registration_service.update_registration` | `workers` |
| heartbeat | heartbeat route | `worker_registry.record_heartbeat` | existing worker/runtime records |
| capabilities/providers | register/heartbeat | `registration_service`, `provider_accounts` | existing capability/provider records |
| job assignment | `/jobs/next` | `repositories.claim_next_job_for_worker` | `job_attempts`, transition journal |
| lifecycle | job status routes | `job_service.transition` | authoritative attempts/transitions |
| events | event batch route | `event_service.ingest_batch` | events and durable contiguous cursor |
| cancel | commands routes | `repositories.pending_commands`, `ack_command`, `attempt_service.apply_cancel_ack` | durable command/attempt state |
| result | HTTPS upload/finalize routes | existing upload and result validation flow | upload session and attempt result fields |
| reconnect | event resume data | `repositories.cursors_for_worker` | durable cursors |

The only new persistence is transport metadata: connection epochs, delivery leases, transfer identity authorization, and pending result notification identity.
