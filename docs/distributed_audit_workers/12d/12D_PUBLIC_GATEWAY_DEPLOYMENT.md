# Public Gateway deployment

`deploy/systemd/web-ocr-agent-gateway.service` runs a dedicated non-root user,
directly binds 8443, terminates TLS/HTTP2 itself and has no Caddy/nginx route.
The issuer is a separate hardened unit with the CA key; Gateway talks to its
Unix socket. The production profile fails without mTLS material, registry DB or
issuer socket. Units/artifacts may be installed but remain disabled/stopped
until explicit cutover approval.
