# Autonomous ProjectChange research

Offline program state, frozen candidates, audit images and validation manifests:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_autonomous_master/`.
Start with `00_STATE/MASTER_STATE.json`. No production consumer imports this package.

The first cycle separates dimension-checked property addresses from engineering
event ownership. Models are state, not identity; a replacement owns its changed
characteristics while count/requirements retain independent event ownership.
All old resolver/scope validation is explicitly DEV-known. Source-audit labels
from this agent are not independent human truth.

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.project_change_master.test_state
python -m experiments.project_change_master.run state_run
python -m experiments.project_change_master.run freeze
```

Existing results and candidate manifests cannot be silently overwritten. A new
development iteration archives prior artifacts; validation never patches frozen
code. The code contains no model/provider call, production write, push or deploy.
