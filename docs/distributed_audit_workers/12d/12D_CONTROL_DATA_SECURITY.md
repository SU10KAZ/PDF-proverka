# Control and data security

Control plane: gRPC bidi over mutual TLS. Machine identity is the Worker leaf.

Data plane: existing resumable HTTPS. Source/result bytes never enter Agent
Stream. Existing opaque transfer authorization binds transfer, worker, job,
attempt, direction and expiry. A 12B regression proves Worker B cannot use
Worker A's transfer. It is intentionally not claimed that all bytes use mTLS.

The Worker derives package routes from `AUDIT_WORKER_DATA_PLANE_BASE_URL` when
that optional typed setting is present. Its production-compatible default is
the existing `AUDIT_WORKER_DISPATCHER_URL`, so deployments with a single HTTPS
origin are unchanged. Both origins independently enforce HTTPS (the existing
explicit localhost-only development exception remains the only HTTP path).
An isolated origin may use a private trust root through
`AUDIT_WORKER_DATA_PLANE_CA_BUNDLE`; if omitted, the normal system trust store
is unchanged. A configured but unreadable/invalid bundle fails startup.
Transfer descriptors remain URL- and credential-free; no Proto change is
required.
