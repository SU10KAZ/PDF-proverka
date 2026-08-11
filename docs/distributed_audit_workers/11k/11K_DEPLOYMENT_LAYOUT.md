# Deployment layout

For configured root `<root>`:

```text
app/<release>/              immutable extracted release
current -> app/<release>    atomic symlink
venv/                       shared dependency environment
data/                       identity, DB, jobs, outbox, provider homes
config/worker.env           generated 0600, no provider secret
logs/                       unit logs
incoming/                   verified admin transfer staging
```

The archive is deterministic at gzip/tar level, manifest SHA is checked before and after transfer, and extracted files are recomputed into the manifest tree hash before self-test/switch. Admin transfer is SCP; audit source/result transport after install is HTTPS.

The release tree and `current` switch are atomic. The shared venv follows the already proven deployment contract and is updated before release self-test/switch; application data and provider homes are never inside either release or venv.
