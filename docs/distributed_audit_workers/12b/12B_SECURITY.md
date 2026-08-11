# Security status

Functional test transport is intentionally insecure and local-only. Typed startup validation requires an IP literal, permits `test_insecure` only on loopback, rejects `0.0.0.0` and public IPs, rejects insecure port 8443, rejects production without mTLS, and keeps reflection disabled. The server also rejects an accidental dynamic selection of port 8443.

Application validation enforces first-message hello, identity consistency, strictly increasing sequence, bounded messages/queues/events/safe strings, valid identifiers and hashes, required enums, reasonable timestamps, and legal domain transitions. Violations produce bounded typed errors or gRPC `RESOURCE_EXHAUSTED`; they do not crash the process.

No shell/admin command exists in the protocol, no provider inference is invoked, and no credential field is logged. mTLS, CA material, production listener, firewall/proxy changes, and deployment are explicitly out of scope.

Verdict: `PRODUCTION SECURITY = NOT_READY_MTLS_PENDING`.
