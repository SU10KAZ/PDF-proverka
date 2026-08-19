# Result flow

The existing Executor materializes the existing result archive first. The shared
HTTPS uploader creates/resumes a session, uploads chunks and asks Center to
validate stored bytes. The gRPC transport then emits ResultReady containing only
the opaque upload id and bounded hashes/metadata. Local retention remains
unconfirmed until correlated ResultAck. Loss/rejection keeps the archive in
`completed_locally`; a later pass reuses HTTPS upload idempotency and re-emits
ResultReady. ResultRejected never enables deletion.
