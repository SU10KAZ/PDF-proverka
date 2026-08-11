# SSH security

- Production options always include `-F /dev/null`, `BatchMode=yes`, `StrictHostKeyChecking=yes` and a per-session `UserKnownHostsFile`.
- Enrollment runs `ssh-keyscan`, computes OpenSSH SHA-256 fingerprints, and writes known_hosts atomically only if the operator-supplied out-of-band fingerprint matches.
- `ssh_auth_ref=agent` uses the existing agent. Other refs resolve through a central JSON secret-reference map; only key paths are resolved in memory and key files must be mode 0600.
- Password and private-key contents are not API/model fields, DB columns, worker config or job payload.
- Registration secret uses stdin; provider auth uses an attached remote TTY. Audit runtime never uses SSH.
- No `StrictHostKeyChecking=no`, sshd/firewall mutation, or inbound runtime listener.
