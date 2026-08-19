# 12F.1C source-writer attribution and final audit

The earlier parallel Claude session is confirmed as a production-root coding
agent with HIGH confidence: PID `1630609`, cwd and Git root
`/home/coder/projects/PDF-proverka`, branch `feature/block-vector-graphs`, not
an isolated worktree. This proves the earlier quiescence prerequisite was
violated. It does not prove that this PID wrote every one of the seven later
deltas; no per-file audit/inotify record exists, so that claim remains NO.

After the operator closed the coding session, the PID remained as a dormant
VS Code extension daemon in `ep_poll`. Read-only `/proc` checks found no open
repository files, and its `write_bytes` counter stayed exactly `823296` from
the precheck through the final audit. Other Claude processes had cwd in
Normirovanie or OSA projects and no open PDF-proverka files.

The decisive evidence is content-based: a 60-second precheck and the fresh
1200.106-second window both retained the same 1477-path manifest hash
`af0ccd9a6d3b41136d09b2fac324636f81db98a1c4f62d1bbb622e87d55a704a`.
The post-build check at 08:55 retained it again. Final active source writers:
`0`. No process was killed or signalled.

## Architectural rule

The production root is deployment source only. Coding agents must mutate a
separate Git worktree. Final baseline commit `46bcd527` contains a preflight
guard and tests: mutation intent from the production root exits `2`, while
read-only access and linked-worktree development remain allowed. Filesystem
permissions of the running production checkout were not changed.
