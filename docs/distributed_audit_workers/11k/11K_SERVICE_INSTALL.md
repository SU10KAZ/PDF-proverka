# Services

Bootstrap writes two user units: Agent (outbound HTTPS) and Executor (local jobs). Names include readable root basename plus SHA-256 of the full absolute install root, preventing same-basename collisions.

Units have no dependency on each other, use `KillMode=process`, restart policy, network ordering for Agent, explicit environment/current Python, append-only logs and basic systemd hardening. They are enabled for `default.target`; linger is verified in preflight and enabled automatically only through passwordless `sudo loginctl` when required.

READY requires both units active plus a center heartbeat. A physical reboot was not performed on `.31`; enablement/linger are the evidence level in 11K.
