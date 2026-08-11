# Control and data security

Control plane: gRPC bidi over mutual TLS. Machine identity is the Worker leaf.

Data plane: existing resumable HTTPS. Source/result bytes never enter Agent
Stream. Existing opaque transfer authorization binds transfer, worker, job,
attempt, direction and expiry. A 12B regression proves Worker B cannot use
Worker A's transfer. It is intentionally not claimed that all bytes use mTLS.
