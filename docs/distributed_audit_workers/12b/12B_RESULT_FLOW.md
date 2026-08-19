# Result flow

The worker uploads package bytes through the unchanged HTTPS upload/finalize path. `ResultReady` carries only job/attempt identity, the HTTPS transfer descriptor, hashes, revision, routing reference, and summaries.

Gateway verifies that the existing upload session matches job, attempt, transfer id, and expected hash. It persists a notification identity but does not validate or publish the result itself. Until the authoritative attempt records completed or superseded-result acceptance with retention, no `ResultAck` is emitted.

After existing central validation persists success, the gateway derives `ResultAck` from the authoritative hash, validation timestamp, storage class, and retention deadline. A restart or lost ACK regenerates the same outcome. Persisted failure yields bounded `ResultRejected`. Duplicate ACK is safe.
