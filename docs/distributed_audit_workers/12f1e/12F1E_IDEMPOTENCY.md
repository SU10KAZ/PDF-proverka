# Idempotency and lost response

ADMIN creation is keyed by `(authenticated actor, Idempotency-Key)` plus a
canonical request SHA-256. The same body returns the same authorization metadata
but never returns the raw token twice; a changed body is rejected.

Worker completion stores the key and canonical request SHA-256 on the consumed
authorization. An exact retry verifies the still-secret one-time token, exact
fingerprint, Worker row and active runtime-token row, then returns a deterministic
`IDEMPOTENT_COMPLETED` state. It does not create a second Worker, consume a
second authorization or mint a second runtime token.

Because raw runtime tokens are intentionally unrecoverable Center-side, the
retry response sets `credential_issued=false` and `recovery_required=true`.
The documented recovery operation is existing ADMIN-only `rotate-token`; it
preserves identity and also has mandatory idempotency semantics.
