# Gateway mTLS

`security_mode=mtls` loads a strict server profile, mode-protected key and
explicit client CA bundle, then calls `grpc.ssl_server_credentials` with
`require_client_auth=True`. Public `test_insecure` is impossible; it remains
loopback-only and cannot bind 8443. Verified `x509_pem_cert` and
`x509_subject_alternative_name` create one authenticated peer context before
AgentHello/domain dispatch.
