# Rotation flow

The Worker persists a resumable rotation state, generates key B, renews, stages
certificate B, and calls `ValidateIdentity` using B before changing live files.
Only then are key/cert/bundle atomically replaced per file. `ActivateCertificate`
using B marks A `REPLACED`; a lost activation response leaves a deterministic
`activation_pending` state. Connection epoch remains independently monotonic.
