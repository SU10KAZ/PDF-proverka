# Agent development safety

This repository's primary checkout at `/home/coder/projects/PDF-proverka` is
production/deployment source and must remain read-only during coding work.

Before changing files, run:

```bash
python3 scripts/development_worktree_guard.py --intent mutate
```

If it blocks, preserve the current work and move development to
`.claude/worktrees/<task>` or another separate Git worktree. Do not reset,
clean, stash, commit or otherwise modify the production checkout to work
around the guard. Read-only inspection remains permitted.
