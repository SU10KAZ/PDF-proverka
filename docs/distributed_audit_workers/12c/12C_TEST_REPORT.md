# Test report and A–BM coverage

Final pre-candidate runs: 12C module **26 PASS** (including a real 60-second
idle stream); combined 12A/12B/11K/11L **126 PASS**; polling vertical slice
**3 PASS**. The final combined invocation was **129 PASS**. All gRPC socket
tests used isolated loopback ephemeral ports. Provider inference calls were zero.

| IDs | Evidence |
|---|---|
| A–D | config/default/single-owner tests; polling and gRPC vertical slices |
| E–G | durable epoch unit and restart E2Es |
| H–L | real Hello metadata/attempt/cursor tests plus 12A/12B negotiation suite |
| M–O | reconnect E2E, typed bounded backoff, and 60-second no-busy-loop test |
| P–Q | negotiated heartbeat, capability coalescing, Worker/Gateway metrics |
| R–U | 12A adapters, 12B offer tests, real local E2E, client dedup |
| V–Z | `worker.db` idempotency, no-slot test, HTTPS transfer test, real Executor E2E |
| AA–AE | network-loss replay E2E, negotiated batch clamp, cursor rewind/upper-bound fail-safe, 12A/12B gap tests |
| AF–AJ | existing progress adapter, real CancelCommand local-queue path, 12B cancel tests |
| AK–AR | shared result builder/uploader, ResultReady/Ack/reject 12B tests, local retention assertion |
| AS–AU | Agent restart, Gateway restart and higher epoch E2Es |
| AV–AY | multi-slot E2E, durable ownership and polling/gRPC parity artifact |
| AZ–BC | bounded critical queue, heartbeat/capability coalescing, durable outbox replay |
| BD–BH | crash matrix plus restart/replay/idempotent ResultAck tests |
| BI–BK | public insecure rejection, loopback allowance, polling suite/default |
| BL | 11K/11L 34 PASS and bootstrap transport model regression |
| BM | config gates false; approved fake pipeline only; inference counters 0/0/0 |

The immutable review artifact records the later candidate/fix-candidate runs.
