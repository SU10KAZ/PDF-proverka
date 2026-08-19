# 12F.1A backend service staging plan

Current production is partially supervised, but not release-supervised. The
`coder` crontab runs `/home/coder/bin/webapp-watchdog.sh` every minute; it
starts `scripts/server/start_server_deploy.sh`, which ultimately launches
uvicorn from the mutable production checkout. It also monitors the independent
production cloudflared process. 12F.1A changed none of these files or jobs.

The Phase-B-ready design is a non-root systemd service under the approved
`coder` identity (or a dedicated non-root service identity if the operator
chooses one):

- `WorkingDirectory=/home/coder/auditmanager/current` where `current` is an
  atomic symlink to an immutable release;
- release-specific `.venv/bin/python -m uvicorn backend.app.main:app --host
  127.0.0.1 --port 8081`;
- secure `EnvironmentFile` reference outside releases;
- `UMask=0077`, `Restart=on-failure`, bounded restart rate;
- graceful `SIGTERM`, explicit `TimeoutStopSec`, no root requirement;
- persistent state and logs outside the release directory.

The minute cron watchdog would race a controlled switch and must be migrated
or made service-aware only in a separately authorized Phase B. It must not be
disabled while the current backend is still managed by it. No unit was
installed, enabled, started or restarted in 12F.1A.

Release-specific dependencies are mandatory; the current shared mutable
`/opt/py312` environment may be used only as a recorded reference, not mutated
to build the candidate and rollback simultaneously.
