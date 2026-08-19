# Known issues / deferred scope

- Public/physical gRPC and mTLS are deliberately deferred to 12D.
- No remote `.31` proof was attempted because 12C has no safe public transport.
- `ProgressUpdate` remains a protocol adapter capability; the real pipeline's
  authoritative progress continues through the shared durable EventOutbox.
- The optional gRPC dependency overlay is installed only when bootstrap
  explicitly selects `grpc_stream`; existing polling installations do not gain it.
