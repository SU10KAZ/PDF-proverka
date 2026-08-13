# Future production plan — not executed

Prerequisite: operator explicitly accepts active base `f6ca2ca9`, confirms the
main worktree/writers are quiescent, reviews immutable patch `775f66b7`, and
authorizes deployment.

1. Read-only freeze and backup of production config and `workers.db` including
   WAL-consistent backup.
2. Deploy exact release `auditmanager-12f1e-775f66b7` and perform one
   controlled :8081 restart.
3. Verify health/core/distributed smoke; scheduler remains disabled.
4. ADMIN creates one exact authorization for the approved historical
   Worker/installation pair with default 300-second TTL.
5. Deliver the one-time token through the approved secure channel and run
   Worker `identity-reenroll`; do not pass the token in argv.
6. Confirm exact identity, new runtime credential, consumed authorization,
   secret-free events and polling ownership.
7. Update Worker Center URL to `https://auditmanager.app`, then controlled
   restart of polling Agent only if separately authorized. Executor remains
   untouched.
8. Confirm authenticated heartbeat and Center visibility; observe at least ten
   minutes. Scheduler stays disabled and no real audit/inference runs.
9. If committed response was lost, use existing ADMIN rotate-token recovery;
   never repeat enrollment with a changed request.

Immediate code rollback is `775f66b7 → f6ca2ca9`; deeper fallback remains
`46bcd527`. Do not delete/downgrade schema-v13 DB tables on application rollback.
