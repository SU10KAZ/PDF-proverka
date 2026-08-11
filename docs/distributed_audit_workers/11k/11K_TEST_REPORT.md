# Test report

Pre-candidate results (2026-08-11):

- 11K/bootstrap + center/deploy/SSH/hardening: **243 passed**.
- Provider/OpenRouter/routing: **477 passed**.
- Isolated outbound-HTTPS/network routing: **32 passed**.
- Total disjoint focused selection: **752 passed**.
- The 11K-specific file itself: **26 passed** (A–BG grouped gates plus added adversarial regressions).
- Real runtime provider inference: Claude 0, Codex 0, OpenRouter 0.

The sandbox blocks asyncio self-pipe sockets; API/network suites were rerun outside that seccomp with only loopback/fake fixtures. No public network/provider call is part of those tests.

An unfiltered repository-wide collection stops before execution on nine pre-existing geometry files because their ignored `experiments/.../*_DIVERSE_CORPUS.json` inputs are absent from this clean worktree. With those nine data-dependent files ignored, the first unrelated baseline failure is the absent ignored `norms/tools/venv/bin/python` used by `test_classic_codex_exec_runner.py`; this is the documented fresh-worktree setup gate, not an 11K regression. The focused 752-test selection covers every changed runtime contour and has no unexplained failure.
