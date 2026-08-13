# 12F.1C delta review

The old and new authoritative snapshots differ at exactly seven paths. All
seven form one coherent deterministic stage-comparison feature slice: router
and store call sites, semantic-diff v6a1/v6a2 implementations, a CLI runner,
and matching tests. They are `PRESERVE`; `UNKNOWN = 0`.

The running production PID predates these bytes and therefore does not prove
their runtime behavior. The operator reported the feature work complete; the
bytes were stable for 20 minutes and then passed the full stage-comparison
suite (`1527 passed, 57 skipped`). They do not overlap the distributed-worker
candidate delta. `semantic_diff_v6a2.py` is retained exactly at SHA-256
`09f8e808…382fc`; it is deterministic, calls v6a1, performs fail-closed parity
checks and invokes no model provider.
