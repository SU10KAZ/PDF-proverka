# 12D network before

Center is `176.12.77.128`; Worker is `176.12.77.31`. Before 12D, Center had
public listeners on 22/80/443 and no listener on 8443. nginx was active. No
Agent protocol listener was present on Worker. The existing Worker initiated
outbound HTTPS polling.

Runtime `ufw`/`nft` inventory requires interactive root authentication. The
world-readable persisted UFW policy was later inspected: it allows 22/80/443
but contains no 8443 rule. No firewall rule was changed. A pre-existing
unrelated `cloudflared` process was observed on Center; 12D does not use or
modify it, and physical peer evidence must point directly to `176.12.77.128:8443`.
