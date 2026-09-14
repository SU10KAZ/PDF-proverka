# Codex inference over object-272 frozen DEV packages

Research-only route. Never deploys, never opens VALIDATION/FINAL_HOLDOUT and never
calls OpenRouter. Initial audit and full input receipts:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/semantic_codex_v1/audit/`.

The immutable package files and source rasters are reused. The installed Codex
configuration is explicitly pinned to gpt-6-astra / xhigh / priority through the
existing ChatGPT login. Each request starts a fresh ephemeral CLI process inside
an OS filesystem allowlist. Corpus and repository are not mounted. Source text
is untrusted data. Model tools, plugins, MCP and web search are disabled.

The provider retains raw CLI JSONL, raw final text, normalized JSON, schema
validation, input hashes and usage for every attempt. Successful requests are
reused only under identical request/configuration hashes. Authorization denial
stops the queue; no automatic provider/model fallback or quota reset exists.
Max concurrency is two. SIGINT/SIGTERM stops new work and lets in-flight work save.

```bash
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:.
python -m unittest experiments.project_change_semantic_codex_272.test_provider
python -m experiments.project_change_semantic_codex_272.run --name codex_smoke_v2 --smoke
# Repeat exactly and record RESUME_CHECK.json to verify no additional inference.
python -m experiments.project_change_semantic_codex_272.freeze --name codex_v1 --smoke codex_smoke_v2
# Only after successful smoke and configuration freeze:
python -m experiments.project_change_semantic_codex_272.run --name codex_v1
python -m experiments.project_change_semantic_codex_272.downstream prepare-closure --name codex_v1
python -m experiments.project_change_semantic_codex_272.run --name codex_v1_closure --closure-packets codex_v1_closure_packets
python -m experiments.project_change_semantic_codex_272.downstream ownership --name codex_v1
python -m experiments.project_change_semantic_codex_272.downstream report --name codex_v1
```

No smoke output contributes to the clean 230-packet candidate. Completed primary
proposal/verification is not ProjectChange quality: closure, ownership resolution,
source audit, duplicate/materiality checks and the >=95% DEV precision gate must
complete before freeze for regression validation. Zero accepted means undefined
precision. Same-model review is not independent expertise.

CLI automation and account telemetry were checked against the installed CLI and
[official non-interactive documentation](https://learn.chatgpt.com/docs/non-interactive-mode)
and [app-server documentation](https://learn.chatgpt.com/docs/app-server).
