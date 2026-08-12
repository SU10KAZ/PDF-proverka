# 12F.1A stable polling endpoint

`https://auditmanager.app` is the existing stable production endpoint:

- nginx server name `auditmanager.app`/`www.auditmanager.app`;
- public listeners TCP/443;
- proxy target `http://127.0.0.1:8081`;
- DNS A result `176.12.77.128`;
- valid Let's Encrypt certificate for both names, valid through
  `2026-10-24`;
- `GET /api/info` returned 200 from Center and from Worker `.31`.

Read-only `.31` proof at `2026-08-12T18:01:22+03:00` resolved the same Center
IP, verified TLS (`ssl_verify=0`) and received health 200. Active Agent config
was not changed and no worker/job endpoint was called.

The intended polling base path is `https://auditmanager.app/api/v1/worker` as
already specified by the architecture and worker docs. Current production code
does not yet register that router, so physical network/TLS reachability is PASS
while exact candidate protocol proof remains pending candidate construction.
The stale Quick Tunnel hostname is not a suitable endpoint and cloudflared is
not required for this path.
