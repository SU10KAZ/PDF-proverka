# 12F production AFTER snapshot

12F stopped fail-closed during read-only preflight. There was no production
deploy, cutover, job, provider call, rollback, service signal, network change,
or database mutation.

- production backend PID `1931160`: active on `127.0.0.1:8081`;
- polling Agent PID `1575036`: active and unchanged;
- Executor PID `1384880`: active and unchanged;
- target transport: `POLLING_UNCHANGED`;
- active Worker attempts: `0`;
- `:8443` listener: absent;
- `:9443` listener/rule: absent;
- UFW `.31 -> 8443`: source-scoped and unchanged;
- production cloudflared PID `1263127`: present, unchanged;
- Claude/Codex/OpenRouter inference by 12F: `0/0/0`.
