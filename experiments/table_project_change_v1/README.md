# TABLE → ProjectChange V1

Offline experiment; no backend imports or production integration. Consumes
immutable Table V3 rows and emits the established ProjectChange schema from
`experiments/project_change_text_v1/contract.py` without altering it.

Requires Python 3.10+ and `jsonschema` (the existing TEXT experiment's local
dependency directory can be used). From the repository root:

```sh
PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PYTHONPATH python -m unittest experiments.table_project_change_v1.test_engine experiments.table_project_change_v1.test_source -v
PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PYTHONPATH python -m experiments.table_project_change_v1.run --output /home/coder/auditmanager/corpus-audits/20260913_table_project_change_v1
PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PYTHONPATH python -m experiments.table_project_change_v1.diagnose --output /home/coder/auditmanager/corpus-audits/20260913_table_project_change_v1
```

`run` pins code before source-only replay. It uses only the explicitly named
existing DEV manifest and source-only conservation manifest. It never scans
holdout/EVAL inputs. Reports distinguish constructed event truth, actual
source-only differences and human V3 boundary truth. Real event quality without
an event-labelled benchmark remains unknown, not zero.

`diagnose` completes the post-freeze accounting audit: raw unmatched TABLE row
observations are reported separately from comparable typed facts, and all
extracted input evidence is verified even if no ProjectChanges are emitted.
It never changes entity extraction/grouping or source truth. The actual source
replay has no entity matches; real compression is not evaluable. Constructed
controls establish grouping behavior only. Final verdict: C, TABLE evidence
available to this narrow consumer is too weak for real event extraction.

Identity excludes model/row/page. A replacement absorbs explicitly selected
characteristics in the same mode/basis, with quantity/material/mode/requirements
independent. REVIEW boundaries remain REVIEW. Unknown or incomplete scopes
never establish equipment absence. The narrow parser exposes unsupported rows
as gaps and never supplements TABLE with TEXT/GRAPHIC inference.

`TABLE_PROJECT_CHANGES.json` is an array of unchanged public ProjectChange
objects. `TABLE_DIFFS.json` holds internal typed observations, evidence,
unresolved cases and one primary event owner per fact. Linking fields are
prepared; cross-route fusion is not implemented.
