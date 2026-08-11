# Repair

Repair repeats evidence-producing idempotent steps: SSH identity, preflight, bundle/hash/tree, release self-test, policy/config permissions, namespaced units, provider metadata, registration identity, heartbeat/revision/capabilities and test job.

It never removes `data/jobs`, results, EventOutbox or provider credential homes. It does not touch unrelated units/firewall/sshd. Same-revision release directories are validated rather than overlaid.
