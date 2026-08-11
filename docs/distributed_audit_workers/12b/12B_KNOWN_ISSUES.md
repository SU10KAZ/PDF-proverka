# Known issues and deliberate limits

- mTLS credentials and secure server startup are not implemented; production startup is therefore refused.
- There is no real Audit Worker gRPC client; current workers continue HTTPS polling.
- There is no automatic gRPC-to-polling fallback after disconnect; an explicit future cutover policy is required.
- Metrics are bounded in-process instrumentation with no production exporter in 12B.
- The gateway has no production service unit, stable external port, proxy/firewall rule, or deployment configuration.
- Data-plane bytes and central result validation remain HTTPS/domain responsibilities by design.

These are 12C/12D deployment prerequisites, not hidden functional claims for 12B.
