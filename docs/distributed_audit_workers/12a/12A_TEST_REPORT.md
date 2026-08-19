# 12A — test report

## Contract suite

Command:

```text
PYTHONPATH=/tmp/agent-stream-proto-tools-exact:. python3 -m pytest -q tests/test_agent_stream_protocol_v1.py
```

Result: **45 passed**. It covers required A–AO: compile/import/package/service/oneof, golden hello/job/heartbeat/events/cancel/result messages, exact enum parity, multi-provider/multi-slot, route identity, typed decline/cancel/result rejects, EventOutbox sequence/duplicate ACK, resume, connection epoch, version rejection, reserved fields, size/secret/RCE/data-plane guards, HTTP semantic adapters, descriptor snapshot and unchanged polling requirements.

Generated descriptor is byte-reproducible with exact pins and SHA-256 `b17f857c47e5ce904a6b6283d8b1e1c2d74b31ce4fbd6bc728823b58c0f9e324`.

## Existing runtime regression evidence

Run separately to avoid a known module-fixture teardown interaction in a combined pytest process:

- `tests/test_distributed_workers_agent.py`: **38 passed**.
- `tests/test_distributed_workers_center.py`: **31 passed**.
- `tests/test_worker_bootstrap_11k.py`: **31 passed**, one Python `crypt` deprecation warning.
- `tests/test_worker_bootstrap_11l.py`: **3 passed**.

Total reproducible passing cases recorded here: **148**.

`tests/test_distributed_workers_hardening.py::test_register_does_not_issue_token` blocked before its first assertion in the existing TestClient registration call, both alone and after the other modules, and was interrupted rather than reported as PASS. 12A changes no backend/runtime file and the center/agent regression suites plus explicit unchanged-runtime guard pass. This environment/test-harness hang is not hidden or reclassified as a protocol failure.

No socket, gateway, :8443, mTLS, production operation, audit or provider inference was invoked. Real Claude/Codex/OpenRouter calls: **0**.

## Immutable review tests

Candidate `4b641b303ef1dd5fa7008fda464dc2907aa1c481` exposed one security defense-in-depth gap and was rejected. After fix + regression test in `095353c9dd6a9422987a81e8428ab05bb36c36e8`, a new detached worktree repeated all six lenses and the 45-test suite: PASS. Details: `12A_ADVERSARIAL_REVIEW.md`.
