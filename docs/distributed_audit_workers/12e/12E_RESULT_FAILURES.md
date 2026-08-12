# Result / acknowledgement failure evidence

Verdict: `PASS` for C18–C20 and duplicate ResultReady/ResultAck.

C18 leaves the attempt in durable `validating` when validation is interrupted;
repeat finalization resumes and only then produces accepted state. C19 closes
the stream after central acceptance but before ResultAck delivery; reconnect
replays the persisted ACK and exact retention deadline. C20 restarts from a
locally missing retention marker and central reconciliation restores accepted
state plus `retention_until` without a second import.

Duplicate ResultReady is one result identity/finalization. Duplicate ResultAck
is nonblocking and does not multiply or drift retention. Final Center evidence:
24 completed attempts, 24 validated storage records, 24 ResultAck timestamps,
zero completed-without-ACK and zero premature deletion.
