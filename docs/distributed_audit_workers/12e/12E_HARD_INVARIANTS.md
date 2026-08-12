# 12E hard invariants

Every chaos case is assessed against these non-negotiable rules. A scenario
with an unexplained violation is a 12E failure, not a candidate for a
workaround.

| ID | Invariant |
| --- | --- |
| I-01 | A job never silently disappears; a non-terminal attempt remains discoverable and reconcilable. |
| I-02 | One attempt has no more than one live Executor identity. |
| I-03 | A fake-provider logical action ledger stays idempotent across transport retry. |
| I-04 | Stream loss is not job failure and never kills an Executor merely because control is disconnected. |
| I-05 | Upload complete is distinct from validated ResultAck. |
| I-06 | No result is deleted before validated acknowledgement establishes retention. |
| I-07 | EventOutbox is durable until its contiguous acknowledgement cursor advances. |
| I-08 | Cancel is command-idempotent and affects only its verified attempt. |
| I-09 | Certificate authentication is not job authorization; identity, attempt and transfer checks remain separate. |
| I-10 | Polling and `grpc_stream` ownership cannot lease work for the same Worker simultaneously. |

The local C01/C02 process tests explicitly checked I-01, I-02, I-04, I-05,
I-06 and I-07. The physical phase must re-check the same invariants on the
direct `.31 → .128` route.
