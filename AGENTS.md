# Agent development safety

Development happens directly in `/home/coder/projects/PDF-proverka` on `main`,
in small isolated commits. No per-task feature branches, no per-task Git
worktrees.

This checkout is also the deployment source, so keep it releasable:

- one logical change = one commit on `main`; stage only the files that belong
  to the task (never `git add -A`);
- do not leave long-lived uncommitted work in the tree — a dirty tree blocks
  `scripts/production_source_guard.py` and therefore blocks release builds;
- the live portal is not served from this tree (it runs from
  `/home/coder/auditmanager/current`), so editing files here never changes
  production by itself — only a new release does;
- before a release, the commit must be reachable from `origin/main`; that is
  still enforced by `scripts/production_source_guard.py` (see
  `docs/production_source_guard.md`).
