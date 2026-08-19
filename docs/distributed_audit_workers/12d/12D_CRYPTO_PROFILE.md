# Crypto profile

- Keys: ECDSA P-256, chosen for OpenSSL 3.x and grpcio interoperability.
- Signatures: ECDSA with SHA-256.
- Transport: grpcio TLS with HTTP/2 and mandatory client certificates.
- TLS: grpcio/OpenSSL modern defaults; TLS 1.0/1.1 are not enabled by project
  configuration. Negotiated physical version is evidence, not inferred.
- Worker lifetime default: 30 days; renew before default: 7 days with jitter.
- Gateway leaf default operational target: 90 days.
- Root/intermediate lifetimes are operator-configurable and not leaf defaults.

Center uses OpenSSL 3.5.5; `.31` uses OpenSSL 3.0.13.
