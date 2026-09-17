# Pair B: one AI mapper + change miner experiment

Artifacts are external to the deployment checkout:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/pair_b_ai_mapping_change_miner_v1/`.

The independent script admits only DEV pair 8 through the frozen corpus guard,
extracts mechanical page data from the admitted v002 files, and excludes OLD p4
and NEW p8 as required by the split. This historically DEV-known pair is not
a historically blind sample. Production and previous pipelines are unchanged.

Each inference is a tools-disabled, fresh `gpt-6-astra` / `xhigh` Codex request
through the existing ChatGPT login. Bubblewrap exposes only that request's
inputs and authentication. No OpenRouter/Claude calls, retries, semantic repair,
or result editing are implemented. The user requested these inference passes.

Execution is deliberately staged:

```sh
python -m experiments.pair_b_ai_mapping_change_miner_v1.run prepare
python -m experiments.pair_b_ai_mapping_change_miner_v1.run map
python -m experiments.pair_b_ai_mapping_change_miner_v1.run mine
```

Never rerun a model command after failure. An existing call directory refuses
another invocation. Mapping must account for every accessible page before it
can be frozen. Mining makes one call per frozen group, with full native/OCR
text, extracted Markdown tables, and all group page rasters. A model-asserted
change is a prediction, not an audited true change. No semantic deduplication
or fixing follows the model output.

Only a successful `CHANGE_MINER_FREEZE.json` unlocks PROVEN10/F13 evaluation.
Evaluation rules: STRONG requires correct subject/correspondence, both states,
direction and sufficient same-version evidence; PARTIAL requires a meaningful
but incomplete recovered part; otherwise MISSED. Source-first false-positive
audit must cover every change or at least all confidence >= 0.85. It reports
correct / partial / false / insufficient to judge; citation existence alone
is not correctness. No model self-grading request is part of this script.

The report helper can render a stopped run with explicit NOT_RUN placeholders;
these are not valid freezes and contain no invented zero metrics.

```sh
python -m unittest experiments.pair_b_ai_mapping_change_miner_v1.test_contracts
```
