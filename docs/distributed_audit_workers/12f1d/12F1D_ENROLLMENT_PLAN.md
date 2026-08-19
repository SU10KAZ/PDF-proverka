# 12F.1D enrollment plan and hard stop

## Decision

The exact production candidate cannot preserve `wrk_19c87718` through its
canonical registration API. Section 4 of the operator runbook therefore
requires a stop before issuing a production enrollment authorization or
changing Worker state.

This is not an inference from a test double. The files inspected are the exact
immutable candidate release `4767d0bf83fcb99ee69267d94324495b92954b41`:

- `RegisterRequest` contains `instance_id`, but no existing/requested
  `worker_id` field;
- `registration_service.register_worker()` looks up only `instance_id`; when
  no Center row exists it delegates to `repositories.create_worker()`;
- `repositories.create_worker()` unconditionally calls `new_id("wrk")`;
- the one-time registration-token table and issuance/consume contract bind the
  token only to `expected_instance_id`, not to an expected Worker ID;
- `BootstrapRequest.worker_id` is used by status/deregister discovery. The
  install registration step does not pass it into the registration domain;
- leaving the old token/state in place does not re-enroll anything: the Worker
  takes its existing-token branch, receives 401 from the empty Center and
  retains the old local identity.

The production registry is empty (`workers=0`, `worker_tokens=0`, bootstrap
sessions/tokens `0/0`). Therefore there is no existing Center row whose ID can
be reused by the current repeat-registration branch.

## Safe ways forward

One of two explicit operator decisions is required:

1. **Preserve the identity (preferred).** Implement and review a typed
   identity-preserving re-enrollment contract. A Center-created one-time
   authorization must bind both `expected_worker_id=wrk_19c87718` and
   `expected_instance_id=inst_boot_e129036dddf5c59049080ddd15624e72`.
   Consumption must atomically create the pending row with the authorized ID,
   never trust a Worker-supplied ID by itself, and reject conflicts, expiry,
   replay, wrong instance and wrong Worker. This needs a schema migration,
   domain/API changes, regression/negative tests, immutable review and a
   separately authorized production Center deployment/restart.
2. **Approve a new identity.** Explicitly authorize the canonical flow to
   replace the local Worker ID with a generated `wrk_*`. Only after that
   approval may the one-time instance-scoped enrollment proceed and atomically
   replace the local runtime credential. No old production jobs/relations
   currently exist in this new registry, but changing identity breaks the
   continuity requested by this runbook and is therefore not assumed.

Manual SQLite insertion, raw copying of the old token, importing a test
registry or broadening the reusable bootstrap-secret path are not acceptable
alternatives.

## Actions deliberately not taken

No production authorization was issued. No token was delivered. Worker config,
state and credentials were not edited. Agent and Executor were not restarted.
Center backend, scheduler and infrastructure were not changed. No provider was
called.
