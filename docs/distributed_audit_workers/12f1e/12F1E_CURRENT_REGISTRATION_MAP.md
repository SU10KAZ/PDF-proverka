# 12F.1E current registration map

## Generic new Worker

`POST /api/v1/worker/register` accepts `instance_id` and machine metadata. It
consumes the existing instance-scoped one-time bootstrap token and calls
`registration_service.register_worker` → `repositories.create_worker`.
`create_worker` generates `wrk_<random hex>` at the Center. The request model
has neither `worker_id` nor `requested_worker_id`; extra fields are ignored and
cannot reach the repository. This path remains unchanged.

## Identity-preserving re-enrollment

1. Portal ADMIN calls
   `POST /api/workers/identity-reenrollment/authorizations` with one exact
   `expected_worker_id + expected_instance_id` pair.
2. Center stores only the authorization-token SHA-256 and returns the raw token
   once.
3. The installation calls `POST /api/v1/worker/identity-reenrollment`, proving
   that token and repeating the exact authorized pair.
4. One SQLite write transaction creates the exact approved identity, binds the
   instance, creates polling ownership, hashes a new runtime token, consumes
   the authorization, and appends the completion security event.
5. The Worker command `identity-reenroll --authorization-id <non-secret-id>`
   reads the authorization token from stdin. It has no Worker-ID option and
   takes the historical pair only from local state.

Credential-loss recovery is the existing ADMIN-only
`POST /api/workers/{worker_id}/rotate-token` path. No registry cloning, generic
identity override, production SQL, or old-token import exists.
