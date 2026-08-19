# Rollback

Rollback changes only `<root>/current` to an existing verified release and restarts the two namespaced units. Automatic rollback is attempted only when this run switched a new release and has a known previous release. Session events record `rolling_back` and `rollback_complete`.

Before update, generated `worker.env` and the non-secret provider policy are snapshotted per release, including explicit "absent" markers for legacy installs. Rollback restores that matching snapshot before switching/restarting, so an old release is not run under the failed release's revision/policy configuration.

It does not delete data, provider auth, unrelated services or other roots. Uninstall is separately reversible: service/release/config material moves under a per-session backup; `data/` and provider auth remain by default. Uninstall revokes a known center identity; `deregister` also remains available as a separate operation.
