# Data-plane authorization

`gateway_transfers` records opaque transfer ID, worker, job, attempt, direction
and expiry. Server-side `authorize_transfer` requires the complete tuple.
Credential-bearing long-lived URLs are not introduced. Existing bearer token
remains scoped to HTTPS package/runtime compatibility and is not the primary
AgentStream identity.
