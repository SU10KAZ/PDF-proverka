# Known issues and blockers

- Windows DPAPI is implemented and platform-guard tested, but no Windows host
  was available: `IMPLEMENTED_NOT_PHYSICALLY_PROVEN`.
- The first physical attempt remains historical `PORT_BLOCKED`: Center UFW did
  not yet allow `.31 -> TCP/8443`. After the operator added the source-scoped
  rule, direct TCP/TLS/mTLS/grpcio, heartbeat, HTTPS E2E, live reconnect,
  rotation, revocation and identity rejection all passed physically.
- The temporary source-scoped `.31 -> TCP/9443` data-plane rule is still
  present because non-interactive removal failed with interactive sudo
  authentication required. Its listener is stopped, so it is inert; the
  operator must delete that exact rule to close the final cleanup item.
- Production cutover is explicitly out of scope and was not done.
