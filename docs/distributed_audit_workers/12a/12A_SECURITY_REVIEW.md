# 12A — security review

## Contract boundary

Descriptor traversal test confirms no password/API/OAuth/worker/execution/registration token, claim secret, private key, auth URL or device code field. Provider credentials and exact model resolution remain worker-local. Future mTLS certificate worker identity must equal application `worker_id`; certificate data/private key is not application payload.

No arbitrary shell, exec, eval, script, argv, filesystem path, install, edit-file or restart-service message exists. Commands are bounded audit business messages (`JobOffer`, `CancelCommand`). Admin remains SSH/bootstrap.

## Parser/DoS controls

Adapters enforce 1 MiB control messages, 256 KiB canonical JSON, 4 KiB flexible strings, 256 events/batch, contiguous sequence, recursive secret-bearing key rejection and exact rejection of executable/admin JSON shapes (`command`, shell/exec/eval/argv/script/env/hook/install/edit/restart). Typed identities such as `command_id` remain valid. Downstream authoritative domain validation remains mandatory. `CenterHello` negotiates lower/equal production limits and unacked window. Package descriptor cannot hold bytes/URL; large content mapping fails.

All actionable choices are closed enums with `UNSPECIFIED=0`; critical unknown action fails closed. `ErrorStatus` contains only code, safe message, retryable flag and correlation ID—no traceback/raw exception. Correlation logs may use connection/worker/job/attempt/message type, never prompt/client text/credentials.

12A created no CA/certificate/CSR/key, listener, socket, firewall or TLS runtime. TLS/mTLS identity, revocation and gateway fencing require a separate 12B threat model and implementation.
