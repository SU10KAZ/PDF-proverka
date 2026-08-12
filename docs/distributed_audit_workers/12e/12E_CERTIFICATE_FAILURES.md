# Certificate failures

12D already proved direct mTLS, new-key rotation and revocation for the
isolated path. The 12E addition is business continuity: C25–C27 must run while
a synthetic Executor is active and prove that certificate lifecycle changes
neither attempt identity nor process ownership.

The target behavior is deliberately asymmetric: a revoked certificate blocks a
new authenticated stream and new work, but does not automatically kill a
running Executor. There is no automatic polling fallback. These active-job
variants are still pending.
