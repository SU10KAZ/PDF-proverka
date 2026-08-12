# 12F.1C source-writer attribution

Read-only process and `/proc/<pid>/cwd` inspection confirms an active Claude
coding agent in the mutable production checkout:

| Field | Evidence |
| --- | --- |
| Writer class | `PARALLEL_CODING_AGENT` |
| Tool | Claude |
| PID | `1630609` |
| Start | `2026-08-12 07:22:24 +03:00` |
| Working directory | `/home/coder/projects/PDF-proverka` |
| Git root | `/home/coder/projects/PDF-proverka` |
| Branch | `feature/block-vector-graphs` |
| Production checkout | YES |
| Isolated worktree | NO |
| Confidence | HIGH |

This is sufficient to fail the quiescence prerequisite. It is not sufficient
to claim that this exact process wrote every one of the seven files observed
after the 12F.1B snapshot. No per-file process-open/audit/inotify evidence was
available, so file-level attribution is explicitly **NO / not proven**.

Other active Claude sessions checked have cwd outside this production root.
Processes in the root also include the production backend, idle shells,
language tooling and orphaned multiprocessing workers. Their mere cwd does not
prove source writes; none is classified as a writer without evidence. The
mandatory final absence audit must be repeated after PID `1630609` is stopped
by the operator.

No process was killed, signalled, restarted or reconfigured.

## Architectural rule

`/home/coder/projects/PDF-proverka` is production/deployment source only and
must not be used as a mutable coding-agent worktree. Claude, Codex and other
autonomous development workflows must use
`/home/coder/projects/PDF-proverka/.claude/worktrees/<task>` or another separate
Git worktree following project convention.

A development preflight guard should fail closed before mutations when the Git
toplevel is the production root, while permitting read-only production
runtime access. The guard is not implemented now because source freeze has not
been achieved and this task forbids production-checkout mutation.
