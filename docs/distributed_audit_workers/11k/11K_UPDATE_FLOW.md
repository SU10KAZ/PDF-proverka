# Update

The same manager builds/selects an approved bundle, verifies local/remote SHA, installs a new immutable release, recomputes tree hash, runs pre-switch self-test, switches `current` atomically, regenerates versioned nonsecret policy/config, restarts only namespaced units, waits heartbeat/revision and runs a protocol self-test job.

Existing `data/` and provider homes are outside releases. A later failure rolls back to `previous_release_id`; resume redeploys because rolled-back release markers are cleared.
