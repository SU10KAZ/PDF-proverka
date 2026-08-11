# 12D network before

Center is `176.12.77.128`; Worker is `176.12.77.31`. Before 12D, Center had
public listeners on 22/80/443 and no listener on 8443. nginx was active. No
Agent protocol listener was present on Worker. The existing Worker initiated
outbound HTTPS polling.

`ufw` and `nft` inventory require interactive root authentication on Center,
so rule contents could not be read. This is recorded as an evidence limitation;
no firewall rule was changed before or during local tests. A pre-existing
unrelated `cloudflared` process was observed on Center; 12D does not use or
modify it, and physical peer evidence must point directly to `176.12.77.128:8443`.
