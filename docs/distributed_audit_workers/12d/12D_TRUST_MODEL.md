# Trust model

An offline ECDSA P-256 root signs an issuing intermediate. The intermediate
signs Gateway server leaves and per-Worker client leaves. Workers receive an
explicit CA bundle and do not use the system trust store as the sole trust
source. Gateway requires client authentication against the Worker CA bundle.

TLS verified SAN is the security identity. `AgentHello.worker_id` is only the
application identity and must equal it before domain dispatch. A valid leaf
does not bypass scheduler, capability, revision, slot, attempt or transfer
authorization.
