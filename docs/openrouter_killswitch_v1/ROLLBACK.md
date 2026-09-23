# Independent code and configuration rollback

Immediate code rollback target recorded before this task:
`/home/coder/auditmanager/releases/ui-real-99d0741b-bridgev1-allowlist-r2`.
The old immutable release is retained byte-for-byte.

After checking idle jobs, queues and child inference processes as in the runbook,
use the guarded switch from the canonical checkout:

```bash
python3 scripts/deploy_center_release.py \
  --release ui-real-99d0741b-bridgev1-allowlist-r2 \
  --milestone openrouter-killswitch-v1-rollback
```

The current protected untracked files must be temporarily quarantined with verified
SHA256 copies and restored byte-identically in a `finally` block if the source guard
requires a clean tree. Never commit or inspect their semantic contents.

Configuration rollback is separate. The exact private backup path is recorded in
`OPENROUTER_CONFIG_RECEIPT.json`. While idle, atomically restore that backup to
`/home/coder/.config/auditmanager/backend.phaseb.env`, retain mode 0600, and restart
only `auditmanager-backend.service`. Verify health and the effective switch state.
Do not include backup contents or hashes of secrets in the handoff.

The previous release does not implement this new switch. Rolling code back removes
the gate even if the environment still contains `AUDIT_OPENROUTER_ENABLED=0`.
Therefore a rollback is **not** a way to keep OpenRouter disabled: keep inference
intake closed and use the prior authorized operational controls before resuming it.
Configuration rollback may also restore the previous enabled default. Perform
these two rollback actions deliberately and verify their independent effects.
