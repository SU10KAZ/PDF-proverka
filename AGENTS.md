# OpenRouter: explicit permission before every request

User rule, 2026-09-14: before EVERY request to OpenRouter, ask the user and
wait for an explicit affirmative answer for that particular request.
State its purpose, model/endpoint, data to be sent and estimated maximum cost
(or that it is a free read-only request). This includes inference, retries,
verification calls, quota/balance checks and calls made through scripts, SDKs,
tools, background jobs or delegated agents.

Silence, elapsed time, autonomy instructions, a previous request's approval,
available credentials, enabled paid-API settings and restored quota do not
authorize a new request. Do not reinterpret LOCAL AI as external-API consent.
Do not launch a batch or auto-resume that can make further unapproved requests.
Do not disable or bypass a permission gate. Prepare and inspect everything
locally first; without the user's answer, stop before contacting OpenRouter.
The request to establish this rule is not permission to contact OpenRouter.

# Agent development safety

Development happens directly in `/home/coder/projects/PDF-proverka` on `main`,
in small isolated commits. No per-task feature branches, no per-task Git
worktrees.

This checkout is also the deployment source, so keep it releasable:

- one logical change = one commit on `main`; stage only the files that belong
  to the task (never `git add -A`);
- do not leave long-lived uncommitted work in the tree — a dirty tree blocks
  `scripts/production_source_guard.py` and therefore blocks release builds;
- the live portal is not served from this tree (it runs from
  `/home/coder/auditmanager/current`), so editing files here never changes
  production by itself — only a new release does;
- before a release, the commit must be reachable from `origin/main`; that is
  still enforced by `scripts/production_source_guard.py` (see
  `docs/production_source_guard.md`).

# ProjectChange research corpus (2026-09-14 scope correction)

For ProjectChange development, truth, tuning, validation, metrics and architecture
decisions, use only Садовническая 76 / Балчуг Эстейт, object 272, OLD=stage_1,
NEW=stage_2, established logical v002 baseline. Other projects are archived
research only, including foreign documents accidentally stored under object 272.
Cross-project generalization and availability of other projects are not gates.

Use the frozen whole-cipher split and access guard in
`experiments/project_change_272/`; artifacts live at
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/`.
Keep prior exposure honest: historical DEV-known cases are never newly blind.
No validation/final reserve evidence may inform tuning before candidate freeze.
The final product is ProjectChange; TEXT/TABLE/GRAPHIC are evidence routes.
