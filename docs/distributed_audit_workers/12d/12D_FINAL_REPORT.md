# 12D final report

1. **12D overall:** FAIL acceptance gate; implementation/local security PASS,
   direct physical network gate failed at TCP.
2. **mTLS control channel:** PARTIAL — complete and locally proven, not physical.
3. **Certificate lifecycle:** PARTIAL — complete/local PASS; physical rotation
   and rejection not reached.
4. **Linux key storage:** PASS, including physical `.31` creation and modes.
5. **Windows DPAPI:** IMPLEMENTED_NOT_PHYSICALLY_PROVEN.
6. **Direct `.31 → Center`:** FAIL, `PORT_BLOCKED`.
7. **Tunnel required:** UNKNOWN. No tunnel was used or added.
8. **Production cutover:** NOT_DONE.
9. **Base commit:** `0ba50e7cfcc3b495148b40b731660d3c46b85aaa`.
10. **Reviewed implementation commit:**
    `f0ca3260213efdf9bf1b9605c35f3d27ec15c8ba`.
11. **CA architecture:** offline root → protected online intermediate → leaves.
12. **Gateway access to CA signing key:** NO.
13. **Worker private key origin:** Worker only, platform KeyStore.
14. **Private key sent to Center:** NO; only CSR crossed SSH/admin plane.
15. **Certificate identity:** `urn:auditmanager:worker:<worker_id>` URI SAN.
16. **worker_id ↔ certificate binding:** PASS_LOCAL and enrollment PASS.
17. **Missing client cert:** rejected at TLS.
18. **Wrong CA:** rejected locally; external physical not reached.
19. **Wrong worker identity:** rejected before domain dispatch.
20. **Expired/not-yet-valid cert:** rejected; active expiry closes boundedly.
21. **Renewal:** PASS_LOCAL, automatic Agent scheduler included.
22. **Key rotation:** PASS_LOCAL; PHYSICAL_NOT_TESTED.
23. **Revocation:** PASS_LOCAL; temporary unused physical cert revoked at cleanup.
24. **Active revoked stream:** closes boundedly in real-grpcio local test.
25. **Server certificate rotation:** PASS_LOCAL.
26. **CA trust rotation:** overlapping old+new bundle, reissue, then remove old.
27. **Linux mechanism:** dedicated owner, directory 0700, PKCS#8 file 0600,
    atomic replace/fsync and regular-file/owner/symlink validation.
28. **Windows status:** DPAPI machine scope, memory-only plaintext; not physical.
29. **Public bind used:** YES, temporary secure `0.0.0.0:8443`.
30. **Endpoint:** `176.12.77.128:8443`.
31. **Caddy involved:** NO; inactive and unchanged. nginx unchanged.
32. **Tunnel involved:** NO. Pre-existing unrelated cloudflared was untouched and
    was not in the 8443 path.
33. **SSH runtime involved:** NO; SSH only performed bootstrap/deploy/diagnostics.
34. **Worker inbound runtime port:** NO new listener.
35. **Real physical grpcio mTLS stream:** NO, TCP gate failed.
36. **Zero-inference E2E:** NOT_TESTED; inference count nevertheless 0.
37. **Secure reconnect:** NOT_TESTED_PHYSICAL; PASS_LOCAL.
38. **Physical certificate rotation:** NOT_TESTED.
39. **Physical revocation rejection:** NOT_TESTED.
40. **Source/result byte transport:** HTTPS by design; physical transfer not run.
41. **Control transport:** gRPC+mTLS implementation; original Worker remains polling.
42. **Cross-worker transfer:** PASS_LOCAL, tuple-bound authorization denies it.
43. **Default Worker transport:** polling.
44. **Automatic fallback:** NO.
45. **Production 8443 left running:** NO; temporary process stopped after failure.
46. **Firewall final:** unchanged; no 8443 rule, no temporary rule applied.
47. **Caddy final:** inactive/unchanged; nginx active/unchanged.
48. **Provider auth preserved:** YES; existing homes/config were not modified.
49. **Claude calls:** 0.
50. **Codex calls:** 0.
51. **OpenRouter calls:** 0.
52. **Secret leaks:** 0 detected; no tracked private-key artifact.
53. **Tests:** 183/183 immutable combined; 27/27 12D mTLS.
54. **Immutable review:** software PASS; direct-public lens FAIL_EXTERNAL.
55. **Report path:** this file.
56. **Proceed to 12E:** NO for physical reliability/cutover claims; first unblock
    source-scoped TCP/8443 and rerun 12D stages D–I.
57. **Cutover blockers:** authorized UFW change, direct TCP/TLS/mTLS/grpc proof,
    zero-inference E2E, reconnect, physical rotation/revocation, operator-approved
    service installation and final firewall policy.

## Required security verdicts

- A. MTLS CONTROL CHANNEL: **PARTIAL**
- B. CERTIFICATE LIFECYCLE: **PARTIAL**
- C. LINUX KEY STORAGE: **PASS**
- D. WINDOWS DPAPI: **IMPLEMENTED_NOT_PHYSICALLY_PROVEN**
- E. DIRECT `.31 → CENTER :8443`: **FAIL**
- F. TUNNEL REQUIRED: **UNKNOWN**
- G. PRODUCTION CUTOVER: **NOT_DONE**
