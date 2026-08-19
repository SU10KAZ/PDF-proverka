# Observability

The standard gRPC health service reports `SERVING` only after startup and is set to `NOT_SERVING` before drain. Metrics are bounded in-process counters/gauges for connections, rejects, heartbeat, offers, events, cancel, results, protocol errors, queue rejection, and active connection count.

Only `protocol_version`, bounded reason category, and bounded result category are legal labels. `worker_id`, `job_id`, attempt id, and connection id are rejected as metric labels. They may appear in structured logs for correlation. Logs never serialize full protobuf messages, payload bodies, document content, URLs with secrets, credentials, tokens, or future private key material.

Request-correlated replies preserve `correlation_id`; asynchronous offers and persisted redelivery do not pretend to be request/response calls.
