# Certificate registry

SQLite schema v11 adds `worker_certificates` with serial, SHA-256 fingerprint,
worker/optional instance, CSR hash, idempotency request, public chain, issuer,
profile version, validity, status, revocation and predecessor/replacement links.
Typed statuses are `ACTIVE`, `REPLACED`, `REVOKED`, `EXPIRED`; typed revocation
reasons are `COMPROMISED`, `DECOMMISSIONED`, `REPLACED`, `ADMIN_REVOKED`.

No private-key column exists. Separate central security events record issued,
renewed, replaced, revoked, rejected and identity-mismatch events.
