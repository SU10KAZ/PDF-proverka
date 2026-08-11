# 11K bootstrap architecture

`BootstrapManager` is the sole state machine used by the CLI and admin API. `SSHBootstrapRemote` is an executor, not a second workflow.

Flow: create persistent session → fingerprint enrollment → read-only preflight → deterministic bundle → double SHA/tree verification → release self-test → atomic switch → config/policy → namespaced user units → zero-inference provider checks → scoped registration/auto-approval/claim → services → heartbeat/revision/capability gate → protocol test job → READY.

SSH is admin plane only. Runtime remains worker-initiated outbound HTTPS for poll, packages, events and results. No inbound worker port or firewall change is introduced.

Operations share the same component: `install`, `update`, `repair`, `status`, `validate`, `rollback`, `uninstall`, `deregister`. Update/repair reuse the verified release contract; rollback is attempted automatically when a newly switched release fails later configuration.

Version contract: bootstrap `1.0.0`, worker package version from release, pipeline revision from manifest, provider policy schema `1`; all are visible in session/heartbeat metadata.
