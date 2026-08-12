# Renewal flow

Worker creates a new local key and CSR. `RenewCertificate` travels over the
currently valid mTLS channel. Gateway derives peer identity from verified TLS
auth context; the protected issuer rechecks ACTIVE status and requires the CSR
SAN to name the same worker. Revoked or expired leaves cannot self-renew.

Request ID + CSR hash returns the same issuance after a lost response. Renewal
is a separate RPC service and is not an audit-job message. The Agent owns a
separate bounded scheduler: it derives stable per-leaf jitter, enters the
configured renew-before window, retries without blocking heartbeat/job work,
and schedules the next cycle from the newly installed leaf. No periodic SSH or
manual certificate command is required.
