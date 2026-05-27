# Data Backfill Decision

**Дата:** 2026-05-20

Решение по существующим production проектам с v1 schema.

---

## Решение: NO automatic backfill

Существующие `01_text_analysis.json` / `03_findings.json` **не пересоздаются
автоматически** после Phase 0/1 deploy.

---

## Обоснование

1. **Cost.** Re-run LLM (Opus + Sonnet) для каждого legacy project = significant
   subscription budget. На 100 проектах = ~200 messages → ~1 hour subscription
   time. Не критично, но не free.
2. **v1 readers продолжают работать.** Schema migration plan (см.
   `migration_plan.md`) гарантирует forward compatibility — Frontend / Excel /
   cross-queries обрабатывают v1 проекты как сейчас.
3. **Phase 0 dedup — no-op на A0 baseline outputs.** Доказано:
   ([FINAL_SUMMARY §3](../../algorithm_research/reports/FINAL_SUMMARY.md)).
   Backfill дал бы 0 эффекта.
4. **Phase 1 completeness lens** — disabled by default для всех doc_types
   кроме audit_comparison. На full_rd legacy projects backfill вообще не
   применим (route disabled).
5. **Engineer ownership.** Existing audits — это deliverables к конкретным
   клиентам. Менять их без явного re-audit — нарушение audit trail.

---

## Re-audit on demand

Existing **version_service** уже умеет:
- Стартовать new audit run на existing project.
- Хранить предыдущие версии артефактов.
- Switch между версиями в UI.

Если engineer хочет получить Phase 1 версию старого проекта:
1. `POST /api/audit/<project_id>/retry/text_analysis` (или новый endpoint
   `/migrate-to-v2` если будет добавлен).
2. Pipeline пересчитывает Phase 1.
3. UI показывает обе версии; engineer выбирает.

Это **на каждый проект отдельно**, не batch.

---

## Reports / Excel / cross-queries — обработка mixed versions

| Component | Behaviour |
|---|---|
| Excel export per project | Использует local `03_findings.json` — v1 или v2 transparent |
| Aggregate Excel report (cross-project) | Iterate projects; v1 проекты — нет `is_beyond_gt_useful` колонки, пишем `-` или skip |
| `/api/optimization/summary/all` | Schema-agnostic aggregation; новые fields opt-in |
| Cross-project search | Search всех findings; ignore `problem_class` если absent |

Эти components уже schema-loose (см. CLAUDE.md — frontend толерантен к legacy
formats). Verify через smoke test.

---

## Specifically NO backfill для:

- ❌ Automated cron job который пробегает по всем проектам и пересчитывает.
- ❌ Database/file migration script который добавляет default `schema_version: 1`
  во все existing files.
- ❌ Forced re-audit при first read любого legacy `03_findings.json`.

---

## Specifically YES для on-demand:

- ✅ Engineer запрашивает re-audit через UI (existing flow).
- ✅ Smoke test pipeline на legacy project (one-off).
- ✅ Manual `migrate-on-demand` если когда-нибудь будет нужно.

---

## Когда YES автоматическому backfill (future scenario)

Если future PR добавит:
- mandatory v2-only фичу (e.g. AI search on `problem_class`),
- которую impossible выдать на v1 data,
- и >50% existing projects блокируются от этой фичи,

то можно рассмотреть batch backfill. На текущий момент **это not the case**.

---

## Mixed-version metrics impact

Cross-project metrics dashboards могут показывать "v1 / v2" split:
- "47 projects analyzed (12 with Phase 1)".
- "Phase 1 finding distribution: ..."
- "Legacy (v1) finding distribution: ..."

Это полезно для tracking adoption + сравнения metric drift.

Implementation: добавить group_by `meta.schema_version` в существующие aggregate
endpoints. Это **отдельный PR**, не блокер для Phase 1 merge.

---

## Snapshot чек-лист — что в каком состоянии остаётся

Перед Phase 0/1 merge:
- `projects/*/_output/03_findings.json` — все v1 (implicit, no `schema_version`).
- `projects/*/_output/01_text_analysis.json` — все v1.
- 03_findings_review.json — v1 (не trogается на Phase 0/1).

После Phase 0 merge (flag enabled на canary):
- New runs: v2 с `meta.dedup_report` (но `same_class_drops=0` на A0 baseline).
- Legacy: v1, без изменений.

После Phase 1 merge (flag enabled на canary):
- New runs (audit_comparison только) — v2 с `meta.completeness_applied=true`.
- New runs (other doc_types) — v2 с `meta.completeness_applied=false` или
  отсутствует.
- Legacy: v1, без изменений.

---

## Что инженер видит

Никаких видимых для пользователя изменений в legacy проектах — UI render'ит как
было.

Новые проекты после Phase 1 deploy:
- Если document_type = audit_comparison → видит Phase 1-усиленный output.
- Если document_type = full_rd → видит как было (lens disabled).

Engineer может через UI явно запросить re-audit, чтобы получить Phase 1 версию
старого проекта.

---

## Summary

| Действие | Полит |
|---|---|
| Auto backfill | NO |
| On-demand re-audit | YES (через version_service) |
| Mixed-version reads | TOLERATED |
| Future automatic backfill | NOT PLANNED (revisit only if blocker фича появится) |
