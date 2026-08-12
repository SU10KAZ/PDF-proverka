# Data-plane failures

The data plane is independently configured through the typed
`AUDIT_WORKER_DATA_PLANE_BASE_URL` and optional
`AUDIT_WORKER_DATA_PLANE_CA_BUNDLE`; the production-compatible default remains
the Dispatcher URL. It is HTTPS-only outside the explicit localhost test
exception.

Executed local regressions reject cross-worker transfer use, unsafe transfer
configuration, source hash mismatch, failed chunk writes and corrupt result
validation. C14 and C16 still require a physical interruption of a separate
isolated `:9443` HTTPS endpoint. If that endpoint is used, its UFW rule must
be source-scoped to `176.12.77.31` and removed after evidence capture.

No `:9443` rule or listener has been created by 12E at this point.
