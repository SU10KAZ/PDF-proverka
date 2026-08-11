# HTTPS data-plane authorization

gRPC is control-plane only. Source and result descriptors are bound to `worker_id`, `job_id`, `attempt_id`, direction, transfer id, and expiry in `gateway_transfers`. A descriptor belonging to worker A cannot be used as worker B's control-plane proof.

For result notification, the gateway additionally requires the existing HTTPS upload session and exact expected hash. The established HTTPS authentication, range/chunk upload, finalize, validation, and retention code remains unchanged. No signed URL, token, package byte, credential, or document content is logged or placed in metrics.

This metadata binding is not a substitute for mTLS. Production transport security remains deliberately unavailable in 12B.
