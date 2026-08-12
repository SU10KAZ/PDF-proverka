# Certificate failure evidence

Verdict: `PASS` for C25–C27.

All three scenarios ran on the physical direct `.31→.128:8443` path with a
synthetic job active. C25 performed new-key rotation and reconnected the same
worker/attempt at a higher epoch without replacing Executor. C26 stopped only
the isolated issuer during the renewal window: active work continued, retry
was bounded, no polling fallback occurred, and renewal succeeded after issuer
recovery. C27 revoked the active certificate: the Gateway closed/denied the
stream, Executor was not killed, and an operator-issued replacement certificate
restored reconciliation.

The final active isolated serial was
`1dc462986cc2b69349be46a8a2f7a8c62b323abf`; isolated processes are now
stopped. The production trust/configuration was not modified.
