# 12F.1E API contract

## Create authorization

`POST /api/workers/identity-reenrollment/authorizations`

- authentication: signed portal session;
- authorization: `distributed_workers.admin` only;
- CSRF intent: `X-Requested-With: audit-workers`;
- idempotency: mandatory `Idempotency-Key`;
- body: exact expected Worker ID, exact expected instance ID, optional TTL;
- first response: safe authorization metadata plus raw token once;
- exact retry: metadata, `authorization_token=null`, recovery required;
- viewer/operator/machine principal: denied.

## Revoke authorization

`POST /api/workers/identity-reenrollment/authorizations/{id}/revoke` is subject
to the same ADMIN, intent and idempotency gates.

## Complete re-enrollment

`POST /api/v1/worker/identity-reenrollment`

- authentication capability: `Authorization: Bearer <one-time-token>`;
- mandatory `Idempotency-Key`;
- body: authorization ID, exact repeated pair and bounded machine metadata;
- success: exact IDs, approved status, polling ownership, new runtime token;
- exact committed retry: `IDEMPOTENT_COMPLETED`, no token, recovery required;
- security rejection: uniform HTTP 401 payload;
- abuse limit: durable per-IP and per-IP/instance registration limiter;
- protocol mismatch: HTTP 426; rate limit: HTTP 429 with `Retry-After`.

Generic `/register` and `/claim` contracts are unchanged.
