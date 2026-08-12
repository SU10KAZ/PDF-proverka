# 12F.1B captured baseline immutable review

- Commit: `8cf5c6738b37d27c54dfa63fd1b7b2e186078a34`
- Tree: `2a59ca933d0368628740783dc19a085e370fcc92`
- Release: `/tmp/12f1b-releases/baseline-8cf5c673`
- Manifest SHA-256: `1d133c00beb8c641d607905d84fb7905f2bff94ad903b5829e6d8b198e3e4cf0`
- Worktree clean; `git show --check`/`git diff --check` passed.
- Release files are read-only; no writable regular files were found.
- No live `.env`, DB, log, certificate private key or runtime state is tracked
  in the release.
- The release-specific dependency environment contains 128 frozen entries.
- Isolated polling boot and candidate-to-baseline state-preserving rollback
  passed.

Review verdict: `PASS_AS_IMMUTABLE_19:29_CAPTURE`.

It is not a current production restart target. The historical running PID is
not fully reproducible, and production source changed after this commit was
built. A later moving checkout cannot be silently folded into this review.
