# 12F.1A operator change map

All 75 current dirty `SOURCE_CODE` paths were compared with production HEAD
and final 12E blobs.

- 71 form a coherent stage-comparison production adaptation and associated
  regressions and are classified `PRESERVE` for a future baseline review;
- 0 are proven superseded;
- 4 are `UNKNOWN` because they changed after the currently running backend
  started;
- 0 have actually been committed into a baseline.

The unresolved four are:

1. `backend/app/api/routers/stage_comparison.py`;
2. `backend/app/services/stage_comparison/store.py`;
3. `backend/app/services/stage_comparison/semantic_diff.py`;
4. `backend/tests/test_semantic_diff.py`.

The first two are loaded production modules whose current disk bytes differ
from the startup point. The latter two are a new semantic-diff pilot and test;
they are not proven to be part of PID `1968811`. Treating this in-progress
group as an approved hotfix, debug code, or current production logic would be
a product/operator decision, so automatic integration stopped.
