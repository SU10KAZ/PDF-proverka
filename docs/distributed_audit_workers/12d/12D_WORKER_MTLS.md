# Worker mTLS

Explicit `grpc` + `mtls` mode requires target, exact target-host server
identity, CA bundle, client chain and KeyStore. The channel uses
`grpc.ssl_channel_credentials`. There is no `verify=false`, system-store-only
fallback, `ssl_target_name_override`, or grpc-to-polling fallback. Polling
remains the default.
