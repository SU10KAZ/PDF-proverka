# Authorization lifecycle

`PENDING` is created only by a portal ADMIN and is usable only before its
`expires_at`. Successful completion changes it to `CONSUMED` in the same
transaction that creates the Worker and runtime-token hash. A failed attempt
does not consume it; an expired attempt persists `EXPIRED` and a safe rejection
event. ADMIN revocation persists `REVOKED` and is idempotent. Neither expired,
revoked nor consumed states transition back to pending.

Authorization token material exists in plaintext only during generation, the
first ADMIN response, secure operator delivery and Worker request handling.
The persistence model has only `token_sha256`. A repeated create response cannot
recover the token; the operator revokes/abandons it and creates a new
authorization with a new idempotency key.
