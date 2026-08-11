# Immutable security review

Candidate/final commit IDs and seven lens results are filled only after commits are created. Review policy: detached read-only worktree; any fix creates a new commit and invalidates the earlier review.

Lenses:

1. SSH credential handling.
2. Remote host verification.
3. Registration token lifecycle.
4. Provider secret handling.
5. Bootstrap logs/state.
6. Rollback/destructive operations.
7. Existing VPS isolation.

Current status: `PENDING IMMUTABLE CANDIDATE`.
