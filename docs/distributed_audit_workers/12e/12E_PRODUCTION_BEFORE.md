# 12E production-before snapshot

This is a read-only snapshot taken before 12E testing.  It is a guardrail, not
permission to modify any of these resources.

- Production backend is PID `277145`, listening only on `127.0.0.1:8081`; its
  `/api/info` response was HTTP 200.
- Center UFW is active.  TCP/8443 is restricted to `176.12.77.31`; TCP/9443
  has no rule.  Neither 8443 nor 9443 had a listener.
- nginx is active and Caddy is inactive.  Neither is a 12E test component.
- Production polling Agent on `.31` is PID `1575036`; production Executor is
  PID `1384880`.  Their actual command/path evidence is authoritative; the
  guessed generic systemd aliases were not active and were not used to judge
  the production processes.
- No isolated 12E worker directory existed on `.31` at snapshot time.
- Claude, Codex and OpenRouter process-count observation was `0/0/0`.

## Cloudflared separation

Pre-existing production Cloudflared PID `1263127` targets only
`127.0.0.1:8081` and exposes metrics on `127.0.0.1:20251`.  It started before
12E and has no 8443/9443 target or listener.  It must not be changed and is
not a 12E path.  An additional unrelated process targets localhost:5050; it is
also outside 12E scope.

The 12E direct control path, when physically run, is only
`176.12.77.31 -> 176.12.77.128:8443`.  There is no Cloudflare tunnel, SSH
forwarding, reverse proxy or overlay VPN in that topology.

The complete machine-readable snapshot is
[`12E_PRODUCTION_BEFORE.json`](12E_PRODUCTION_BEFORE.json).
