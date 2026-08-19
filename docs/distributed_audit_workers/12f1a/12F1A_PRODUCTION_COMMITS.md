# 12F.1A production-only commits

Exactly 11 commits are reachable from production HEAD and absent from final
12E. None is `UNKNOWN`; all are required inputs because the baseline must
inherit current production behavior rather than cherry-pick an older subset.

| Commit | Classification | Purpose | 12E path overlap |
| --- | --- | --- | --- |
| `f7873f41` | REQUIRED | processed/unprocessed project columns | no |
| `d4e2e804` | REQUIRED | project counter selection toggle | no |
| `aa964a7e` | REQUIRED | section selection toggle | no |
| `49c5e9d6` | REQUIRED | remove obsolete optimization subtitle | no |
| `a2744e11` | REQUIRED | remove local-LLM pipeline transport | yes |
| `1903b01d` | REQUIRED | remove local model-server control | yes |
| `c44af97f` | REQUIRED | remove Qwen from stage comparison | yes |
| `c3aeaf34` | REQUIRED | Opus-only stage-comparison UI | no |
| `ad34bafe` | REQUIRED | remove obsolete local-LLM config/docs/tests | yes |
| `a62856d2` | REQUIRED | compact/sorted project cards | no |
| `9168c393` | REQUIRED | Codex transient retry and durable Stage 02 cache | yes |

The future integration method is to inherit them through the approved
`PRODUCTION_BASELINE_COMMIT`; they must not be blindly cherry-picked on top of
12E. Candidate construction has not started because baseline equivalence
failed first.
