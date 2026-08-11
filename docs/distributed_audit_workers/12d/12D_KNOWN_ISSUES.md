# Known issues before physical proof

- Windows DPAPI is implemented/static-tested but not physically proven.
- Center firewall rule listing requires interactive root; exact rule diff is
  unavailable unless the direct route proves existing policy already permits 8443.
- Physical `.31` direct route, public mTLS E2E, physical rotation/revocation and
  negotiated TLS version remain pending until immutable local review completes.
- Production cutover is explicitly out of scope.
