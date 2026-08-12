# 12E test report

Final relevant-suite result: `660 passed, 1 skipped`, counting each listed
file group once. Targeted reruns used during defect isolation overlap these
groups and are not added again.

| Non-overlapping suite group | Result |
| --- | --- |
| 12A protocol descriptors | 45 passed |
| 12B Gateway, including C28/C32 | 49 passed |
| 12C real Agent gRPC | 33 passed |
| 12D mTLS/security | 27 passed |
| New 12E reliability/process/harness | 15 passed |
| 11K bootstrap/repair | 31 passed |
| 11L bootstrap/repair | 3 passed |
| Worker Agent | 38 passed |
| Worker hardening | 70 passed |
| Center/polling E2E | 34 passed |
| Execution backend | 128 passed, 1 skipped |
| Executor/flag-off/review/step35/central-handoff | 187 passed |

The expanded group initially exposed two environment-only constraints. The
host's live swap correctly reduced production-style slots to zero, so the
process-lifecycle test now hides optional telemetry only inside its subprocess;
the production threshold is unchanged. Four route probes were run with the
project Python/FastAPI environment because the auxiliary gRPC venv contains
FastAPI 0.141, whose included-router representation differs from the project's
version. Results are reported as 183 passed in the gRPC environment plus those
four project-environment passes, i.e. 187 unique tests.

Real defects found, reproduced, fixed and regressed:

- stale gRPC request iterator consuming work after reconnect (`043a28f4`);
- source download restart from zero instead of durable range resume
  (`489d2448`);
- result acknowledgement/upload recovery hardening (`bd5ee589`);
- Center DB failure escaping Gateway fail-closed handling (`8e206a00`).

No test used production `:8081`, production DB, production Agent/Executor,
tunnel, real project or provider inference.
