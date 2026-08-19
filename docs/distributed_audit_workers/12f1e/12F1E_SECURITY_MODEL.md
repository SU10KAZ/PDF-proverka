# 12F.1E security model

The authority to restore a historical Worker ID belongs to an authenticated
portal ADMIN, never to a machine. An authorization is a short-lived capability
bound to one immutable Worker/installation pair. Possessing it is insufficient
to select a different identity: the stored pair, request pair and token hash
must all match.

Security invariants:

- generic registration remains Center-ID-owned;
- an empty registry grants no special trust;
- authorization TTL is 30–3600 seconds, production default 300 seconds;
- authorization and runtime tokens have at least 256 bits of random source
  entropy and are stored only as SHA-256;
- validation uses constant-time digest comparison;
- status is typed (`PENDING`, `CONSUMED`, `EXPIRED`, `REVOKED`);
- public machine failures are bounded and non-oracular, while typed reasons are
  retained in the security event stream;
- the existing durable registration rate limiter is charged before token
  validation;
- Worker and instance conflicts fail closed; there is no reassignment;
- machine capacity hints cannot configure the restored identity; it starts at
  the conservative one-slot setting and remains human-drained by schema v12;
- the old runtime token is neither read nor sent by the re-enrollment command;
- no raw secret enters logs, events, metrics, URLs, argv, docs, or Center DB.

Adversarial answer: a compromised or newly installed Worker cannot claim
`wrk_19c87718` unless an ADMIN first created an unexpired authorization for the
exact installation and securely delivered its one-time token. Even then the
Worker cannot substitute either ID.
