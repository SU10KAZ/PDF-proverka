# Known issues and blockers

- Windows DPAPI is implemented and platform-guard tested, but no Windows host
  was available: `IMPLEMENTED_NOT_PHYSICALLY_PROVEN`.
- Direct `.31 → 176.12.77.128:8443` timed out while `.31 → :22` succeeded.
  Center UFW is active and its persisted policy has no 8443 allow rule.
- Applying the authorized source-scoped temporary rule required interactive
  sudo authentication unavailable to this run; no firewall state was changed.
- Therefore physical grpcio stream, zero-inference E2E, reconnect, rotation and
  old-certificate rejection were not run. Their local tests pass, but physical
  verdicts remain `NOT_TESTED` rather than inferred.
- Production cutover is explicitly out of scope and was not done.
