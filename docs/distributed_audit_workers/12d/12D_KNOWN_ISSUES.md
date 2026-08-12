# Known issues and blockers

- Windows DPAPI is implemented and platform-guard tested, but no Windows host
  was available: `IMPLEMENTED_NOT_PHYSICALLY_PROVEN`.
- The first physical attempt remains historical `PORT_BLOCKED`: Center UFW did
  not yet allow `.31 -> TCP/8443`. After the operator added the source-scoped
  rule, direct TCP/TLS/mTLS/grpcio, heartbeat, HTTPS E2E, live reconnect,
  rotation, revocation and identity rejection all passed physically.
- Final firewall cleanup is complete: the temporary source-scoped
  `.31 -> TCP/9443` rule and both temporary listeners are absent. The persisted
  IPv4 rules match the captured post-8443/pre-9443 baseline exactly.
- A pre-existing `cloudflared` process targeting production
  `127.0.0.1:8081` is currently present. It was not modified or used by 12D,
  but its presence means the explicit final-state predicate
  `cloudflared absent` cannot be certified.
- Production cutover is explicitly out of scope and was not done.
