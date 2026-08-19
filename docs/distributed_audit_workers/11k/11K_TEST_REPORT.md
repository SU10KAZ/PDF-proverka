# Test report

Pre-candidate results (2026-08-11):

- 11K/bootstrap + center/deploy/SSH/hardening: **243 passed**.
- Provider/OpenRouter/routing: **477 passed**.
- Isolated outbound-HTTPS/network routing: **32 passed**.
- Total disjoint focused selection: **752 passed**.
- The 11K-specific file itself: **26 passed** (A–BG grouped gates plus added adversarial regressions).
- Real runtime provider inference: Claude 0, Codex 0, OpenRouter 0.

The sandbox blocks asyncio self-pipe sockets; API/network suites were rerun outside that seccomp with only loopback/fake fixtures. No public network/provider call is part of those tests.

Immutable candidate `6431c004a61486b10687e1f8f593627b16120e04` was rechecked in its detached worktree: 26/26 11K tests and py_compile PASS. Deterministic builds from that commit produced identical artifacts:

- archive SHA-256: `9d31bccb4efb480d12e15f4c5ef50409e503b72e36e83c0072c7639873a0dd9c` (both builds);
- manifest SHA-256: `11417ce119baee89fa654fbfceff29779949cb92760b245b64b7052dce03fa6d` (both builds);
- tree hash: `sha256:743dfe7fa00e98b5c512e290105ac7e374dfc7166b29e3962828a840ed83f257`;
- release: `20260811T055027-743dfe7fa00e`;
- manifest revision/source commit: exact candidate hash.

An unfiltered repository-wide collection stops before execution on nine pre-existing geometry files because their ignored `experiments/.../*_DIVERSE_CORPUS.json` inputs are absent from this clean worktree. With those nine data-dependent files ignored, the first unrelated baseline failure is the absent ignored `norms/tools/venv/bin/python` used by `test_classic_codex_exec_runner.py`; this is the documented fresh-worktree setup gate, not an 11K regression. The focused 752-test selection covers every changed runtime contour and has no unexplained failure.
