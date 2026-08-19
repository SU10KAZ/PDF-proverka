# Security

Worker bearer authentication remains mandatory for HTTPS. Polling keeps verified
HTTPS and execution-token authorization. gRPC mode carries no execution token in
Proto: source download additionally requires the opaque JobOffer transfer id;
all package mutations require the currently active Gateway connection id and the
assigned worker identity. The id is cleared on disconnect.

`test_insecure` is rejected for every non-loopback target in both WorkerConfig and
bootstrap models. Port 8443, public listeners, mTLS, certificates, firewall and
reverse proxies are untouched. Proto contains no update/restart/shell admin plane.
