# 12F.1E domain contract

`CreateIdentityReenrollmentAuthorization` validates both identifier formats,
validates a configurable short TTL, scopes idempotency to the authenticated
admin actor, generates a random one-time token, stores its digest, and appends
`IDENTITY_REENROLLMENT_AUTH_CREATED` atomically. A repeat with the same actor,
key and body returns safe metadata without the raw token. Reuse with a changed
body is rejected.

`CompleteIdentityReenrollment` requires authorization ID, raw one-time token,
the exact Worker/instance pair, machine metadata and an idempotency key. It
validates state, expiry, token and pair before registry mutation. Registry
conflicts never rebind data. Success returns a new runtime credential once.

`RevokeIdentityReenrollmentAuthorization` is an ADMIN-only transition from
pending/expired to revoked. Consumed authorization cannot be revoked into a
reusable state.

Public APIs never expose the free-form internal exception. The domain uses the
typed `ReasonCode` enum; only the generic rejection class is returned through
the machine contour.
