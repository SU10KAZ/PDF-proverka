# Development worktree policy

`/home/coder/projects/PDF-proverka` is the production/deployment checkout. It
is not a mutable development worktree for Claude, Codex, editors, generators
or other autonomous coding workflows.

Before a development mutation, run:

```bash
python3 scripts/development_worktree_guard.py --intent mutate
```

The command exits `2` when the Git toplevel is the production root. Create and
use `/home/coder/projects/PDF-proverka/.claude/worktrees/<task>` (or another
project-approved linked Git worktree) and rerun the guard there. Read-only
inspection can be declared explicitly with `--intent read` and is never
blocked.

The production root can be overridden for a different deployment with
`AUDITMANAGER_PRODUCTION_GIT_ROOT`. The guard does not change permissions,
install hooks, signal services or affect the backend's ability to read its
release files.
