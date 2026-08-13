# Security audit events

The additive append-only table records:

- `IDENTITY_REENROLLMENT_AUTH_CREATED`;
- `IDENTITY_REENROLLMENT_AUTH_REVOKED`;
- `IDENTITY_REENROLLMENT_COMPLETED`;
- `IDENTITY_REENROLLMENT_REJECTED`.

Rows contain authorization ID, safe Worker/instance IDs, typed reason, actor,
request ID and timestamp. They contain no raw authorization token, runtime
credential, token digest, request digest or runtime-token ID. ADMIN API calls
also use the existing portal action log and `worker_admin_actions`, with only
safe requested/result metadata. Machine log entries contain authorization ID
and low-cardinality typed reason, never request authorization headers.
