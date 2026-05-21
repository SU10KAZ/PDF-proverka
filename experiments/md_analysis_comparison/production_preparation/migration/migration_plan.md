# Schema Migration Plan v1 → v2

**Дата:** 2026-05-20

Phase 0 / Phase 1 расширяют schema `01_text_analysis.json` и `03_findings.json`.
Все новые поля **optional**; legacy readers должны их игнорировать.

---

## Schema versions

| Version | Period | Producers | Readers |
|---|---|---|---|
| v1 (implicit, `schema_version` absent) | до Phase 0 | current pipeline | frontend, Excel export, version_service, cross-project queries |
| v2 (`schema_version=2`) | Phase 0 / Phase 1 | new pipeline | same readers, after schema-v2-aware update |

**Coexistence period:** indefinite. Старые `03_findings.json` остаются как
есть; новые пишутся в v2. Readers must handle both.

---

## Новые поля v2 (additive only)

### В `01_text_analysis.json` / `03_findings.json` top-level

```jsonc
{
  "stage": "01_text_analysis",
  "schema_version": 2,                     // NEW
  "document_type": "audit_comparison",     // NEW
  "project_id": "...",
  "text_source": "md",
  "timestamp": "...",
  "project_params": { ... },                // existing
  "normative_refs_found": [...],            // existing
  "text_findings": [...],                   // existing (расширены)
  "meta": {                                  // NEW
    "schema_version": 2,
    "document_type": "audit_comparison",
    "document_type_confidence": 0.87,
    "completeness_applied": true,
    "completeness_status": "ok",
    "completeness_findings_in": 3,
    "completeness_findings_kept": 2,
    "dedup_report": {
      "total_in": 18,
      "total_out": 14,
      "clusters": 14,
      "same_class_drops": 4,
      "same_class_drops_by_key": { "...": 4 },
      "critical_collapsed_count": 0,
      "methods_seen": ["current_method", "completeness"]
    }
  }
}
```

### В каждой finding

```jsonc
{
  "id": "T-001",
  "category": "Критическое",                  // existing
  "severity": "КРИТИЧЕСКОЕ",                  // existing
  "problem_class": "cable_undersized",        // NEW (используется dedup)
  "affected_system": "Кабель ввода ВРУ-1",    // NEW (используется dedup)
  "severity_reasoning": "Iдоп 220 А ...",     // NEW (KILL-LIST guard)
  "confidence": 0.95,                          // existing
  "is_beyond_gt_useful": false,                // NEW
  "interface_type": "electric_power",          // NEW (для cross-discipline дедупа)
  "discipline_pair": null,                     // NEW (для cross-discipline дедупа)
  "internal_duplicate_of": null,               // NEW (после dедупа)
  "is_canonical": true,                        // NEW
  "class_key": "cable_undersized|кабель...|none|none",  // NEW
  "duplicate_count_in_cluster": 0,             // NEW
  "source_agents": ["current_method"],         // NEW
  ...
}
```

---

## Forward compatibility (legacy readers must tolerate)

| Reader | Required behaviour |
|---|---|
| Frontend (`frontend/static/js/app.js`) | Ignore unknown fields. Display `is_beyond_gt_useful` if present (future enhancement). |
| Excel export (`backend/app/pipeline/stages/report/generate_excel_report.py`) | Ignore new finding fields; render existing fields as today. New columns optional in future PR. |
| version_service | Treat v1/v2 transparently; no schema-aware logic. |
| `findings_service._enrich_sheet_page` | No change; works on existing `evidence`/`related_block_ids`. |
| Cross-project queries (`/api/optimization/summary/all`) | No schema dependency on new fields. |
| Excel column ordering | Add new columns at the end; preserve existing column order для legacy projects. |

Frontend specifically — verify before Phase 1 merge:
- `app.js` parsing: грепнуть на known field references; убедиться что нет
  `forIn(data)` strict checks.
- (Spot check: see CLAUDE.md notes — frontend уже толерантен к legacy formats.)

---

## Backward compatibility (v2 readers must default missing fields)

| Field | Default if missing |
|---|---|
| `schema_version` | 1 |
| `document_type` | `full_rd` (preserves status quo for legacy) |
| `meta` | `{}` |
| `meta.dedup_report` | None / not applied |
| `meta.completeness_applied` | False |
| finding.`problem_class` | `""` (dedup fall back to category+signature) |
| finding.`affected_system` | `""` |
| finding.`is_beyond_gt_useful` | False |
| finding.`interface_type` | None |
| finding.`discipline_pair` | None |
| finding.`is_canonical` | True (legacy data — все canonical) |
| finding.`internal_duplicate_of` | None |
| finding.`class_key` | computed lazily on demand |
| finding.`duplicate_count_in_cluster` | 0 |
| finding.`source_agents` | `[]` or `["current_method"]` (legacy implicit source) |
| finding.`severity_reasoning` | `""` |

Где это реализуется:
- Dedup utilities (`class_dedup.derive_class_key`) уже падают graceful на
  missing `problem_class` (см. `production_preparation/dedup/class_dedup.py:138`).
- Frontend / Excel: missing fields render as empty.
- Schema validation (если включена) — все новые поля `additionalProperties=true`
  и `required=false`.

---

## NO data backfill required

Решение: **не backfill'ить** legacy `01_text_analysis.json` / `03_findings.json`.

Reasons:
1. Backfill потребует re-run LLM (costly).
2. v1 readers продолжают работать.
3. Re-audit on demand уже доступен через `version_service` — users могут
   обновить проект на v2 schema при необходимости.

---

## Migration sequence

```
Step 1: Deploy schema-v2-aware readers (default-tolerant)
  ├─ frontend update: handle new fields (1 PR, can be no-op if defaults OK)
  ├─ Excel: handle new fields (1 PR, can be no-op)
  └─ version_service: confirm transparency (no PR needed)

Step 2: Deploy Phase 0 (producers add `meta.dedup_report`)
  └─ `STAGE01_DEDUP_ENABLED=false` default → flag-flip to enable

Step 3: Deploy Phase 1 (producers add `document_type`, `completeness_applied`)
  └─ `STAGE01_COMPLETENESS_LENS_ENABLED=false` default → progressive enable

Step 4: Keep v1 fallback path for ≥ 30 days
  └─ Если regression observed → rollback path documented

Step 5 (after 30 days):
  └─ schema_version=1 default in readers (assume v1 unless v2 explicitly tagged)
```

Минимум 30 дней coexistence — это safety net.

---

## Mixed-version queries

Cross-project endpoints (`/api/optimization/summary/all`) могут получить mix
v1 + v2 проектов. Должны:
- Filter findings independent of schema_version.
- Aggregate metrics independently.
- НЕ assume `problem_class` available (fall back на category).

Если new feature (e.g. dashboard for `problem_class` distribution) — это
addressed only для projects с v2 data. Show "N projects не имеют v2 data;
re-audit для full statistics" hint.

---

## Schema validation (опционально)

Если backend использует JSON-schema validation:
- `backend/app/schemas/text_analysis.json` обновляется в Phase 1 (~+25 LOC,
  см. `files_to_modify.md`).
- Все новые поля `additionalProperties=true`, `required=false`.
- Существующие validation checks не ломаются.

Если schema validation не используется в runtime (только в IDE/tests) —
update оптимальный, но не блокирующий.

---

## Frontend visibility plan (отдельный PR)

Это вне scope Phase 1, но planning'ить заранее:

| Поле | UX |
|---|---|
| `is_beyond_gt_useful` | Show badge "доп. инженерное замечание" |
| `meta.document_type` | Show in project header: "Документ: audit_comparison" |
| `meta.dedup_report.same_class_drops` | Show в pipeline log: "Свернуто N дубликатов" |
| `meta.completeness_applied` | Show "Phase 1: completeness lens применён" в audit log |
| `source_agents` | Show в finding card: "Источник: completeness lens" (если != current_method) |
| `class_key` | Internal-only (debug overlay, не для main UI) |
| `duplicate_count_in_cluster` | Если > 0 — show "(N дубликатов свёрнуто)" |

Эти UX-enhancements не блокируют Phase 1 deploy; они полезны для
engineer-experience позже.

---

## Validation чек-лист

Перед merge'ем schema-v2 changes:

- [ ] frontend parsing test: подсунуть v2 `03_findings.json` → render без
      errors.
- [ ] Excel generation: подсунуть v2 → file генерируется, опускает unknown
      fields gracefully.
- [ ] version_service: switch between v1 and v2 versions same project → OK.
- [ ] Cross-project query: mix v1+v2 dataset → aggregate works.
- [ ] schema_version=1 explicit на старом проекте читается ОК.
- [ ] schema_version=2 explicit на новом проекте читается ОК.
- [ ] Missing schema_version (implicit v1) на legacy проекте читается ОК.

После 7/7 PASS — schema migration considered ready.
