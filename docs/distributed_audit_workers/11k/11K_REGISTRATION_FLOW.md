# Registration flow

1. Manager creates `inst_boot_*` and a 300-second token scoped to session + instance.
2. DB stores only SHA-256, TTL and used timestamp.
3. Token goes to `audit_worker register --bootstrap-secret-stdin` over strict SSH stdin.
4. Public register endpoint atomically consumes it and creates the center-assigned worker ID + one-time claim.
5. Trusted admin bootstrap manager approves the exact returned worker and slot limit.
6. Worker claims its permanent runtime token into local mode-0600 storage; center stores only its hash.
7. Manager invalidates any token left live and starts services.

Legacy global-secret manual registration remains backward compatible but is never used by one-click.
