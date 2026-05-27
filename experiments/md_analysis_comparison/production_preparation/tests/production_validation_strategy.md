# Production Validation Strategy — Shadow + Canary + A/B

**Дата:** 2026-05-20

Post-merge validation для Phase 1. Три уровня progressively более expose'нутые.

---

## Stage 1: Shadow mode

### Что
Phase 1 lens **запускается**, но её output **НЕ surfacing** в production UI.
Только пишется в logs / отдельный staging-файл.

### Цель
Убедиться что:
1. Lens не падает на real-world MD-документах (production scale).
2. Output content acceptable engineer'ам (qualitative review).
3. Cost / latency в рамках expected.

### Конфигурация
```bash
STAGE01_COMPLETENESS_LENS_ENABLED=true
STAGE01_COMPLETENESS_BY_DOC_TYPE=audit_comparison
STAGE01_SHADOW_MODE=true   # NEW flag — реквестит lens output в staging
```

`STAGE01_SHADOW_MODE` — новый предлагаемый env. Implementation:
- `completeness_lens.py` пишет в `_output/01_text_analysis_completeness_shadow.json`
  вместо merge.
- `findings_service` НЕ объединяет shadow output в финальный
  `01_text_analysis.json`.
- UI не видит shadow output.

### Selection
- N = 10 projects с `document_type=audit_comparison` (обычно cross-discipline
  audits).
- Selection: prefer ongoing audits (где engineer всё равно делает review).
- Не делать shadow на критичных production deliverables (выкатывать для
  internal sample').

### Feedback loop
Engineer review каждого shadow output:
```jsonl
# tests/canary/feedback.jsonl
{"project_id": "EOM/...", "shadow_run_id": "abc123",
 "finding_idx": 0, "verdict": "correct|fp|wrong_severity|beyond_useful|missed",
 "notes": "..."}
```

`tests/canary/build_feedback_report.py` агрегирует.

### Success criteria
- ≥ 80% findings одобрены (correct OR beyond_useful).
- 0 missed_critical regressions (vs engineer-expected list).
- Pipeline 0 crashes.
- Cost per project < 2× baseline (within budget).

Длительность: **2 недели** или N=10 projects (whichever first).

---

## Stage 2: Canary (visible)

### Что
Phase 1 lens output **surfacing** в UI, но только для N pilot users (или
N pilot project_ids).

### Конфигурация
```bash
STAGE01_COMPLETENESS_LENS_ENABLED=true
STAGE01_COMPLETENESS_BY_DOC_TYPE=audit_comparison
STAGE01_CANARY_PROJECT_IDS=<csv>   # NEW flag, optional whitelist
```

Если `STAGE01_CANARY_PROJECT_IDS` пустой → lens применяется на ВСЕХ
audit_comparison проектах. Если непустой → только на listed.

UI показывает Phase 1 finding'и с явным "experimental" badge (per finding или
per project'у).

### Selection
- 10 проектов реальных audits.
- Engineer informed заранее: "your audit will include experimental findings".
- Engineer может opt-out (revert через version_service).

### Feedback
Per finding:
- thumbs up / down.
- optional comment.

Per project:
- "would you keep Phase 1 enabled for similar projects?" (yes/no).

### Success criteria
- ≥ 70% findings get thumbs-up.
- ≥ 70% projects: engineer says "keep enabled".
- ≥ 90% completion rate (audits didn't break).

Длительность: **3-4 недели**.

---

## Stage 3: A/B

### Что
Same project_id, два audit'а:
- **Version A:** pre-Phase-1 (current_method only).
- **Version B:** Phase 1 (current_method + completeness lens).

Engineer reviews both, picks one.

### Конфигурация
Через существующий version_service. Pipeline запускается дважды с разными env
contexts.

### Selection
- 20 проектов / users.
- Random assignment: A first or B first (controlling for order bias).

### Feedback
- Forced choice: A vs B.
- Optional: "what would you change?" free-text.

### Success criteria
- ≥ 60% engineers prefer Version B.
- Statistical significance: binomial test, p < 0.05.

Длительность: **4 недели**.

### Decision points
- ≥ 60% B preference → enable Phase 1 for default audit_comparison.
- 40-60% — extend A/B with more projects; investigate qualitative feedback.
- < 40% — pause Phase 1; investigate root cause; either tune prompts further
  or accept "Phase 1 not net positive" conclusion.

---

## Graduation criteria (canary → full opt-in)

После 3-х stage'ей:

| Cohort | Default state |
|---|---|
| audit_comparison | Phase 1 enabled by default (after Stage 3 PASS) |
| specification_only | Phase 1 enabled by default after separate shadow pass (3-5 projects) |
| tz_vs_rd | Phase 1 enabled by default after separate shadow pass (3-5 projects) |
| full_rd | **HOLD** until remediation completed (cap reduction + further validation) |

Каждый doc_type — независимый graduation. Включение через
`STAGE01_COMPLETENESS_BY_DOC_TYPE` env var.

---

## Engineer feedback form (structured)

```yaml
# tests/canary/finding_feedback.schema.yaml
project_id: string
finding_id: string
verdict:
  enum: [correct, fp_real, wrong_severity, beyond_useful, missed, duplicate]
fields:
  norm_ref_acceptable: bool
  evidence_quote_acceptable: bool
  severity_should_be: enum[КРИТ, ЭКОН, ЭКСПЛ, РЕКОМ, ПРОВ, REMOVE]
free_text: string
```

Структурированный feedback → automated reports → next prompt iteration data.

---

## Roll-forward and roll-back signals

### Roll forward (move to next stage)
- Shadow → Canary: shadow success criteria met за N projects.
- Canary → A/B: canary success met.
- A/B → Production: A/B success met.

### Roll back (regress to previous stage)
- Shadow: 0 projects пройдут success criteria за месяц → pause, investigate.
- Canary: engineer thumbs-down > 50% → pause + analyze; либо rollback prompt
  changes.
- A/B: B preference < 40% → pause, обсудить с teamlead.

---

## Cost projection

- Shadow stage: 10 projects × 1 Sonnet call = 10 messages. ~30 min wall-clock.
  Budget: ~0.2% subscription.
- Canary stage: 10 projects × 1 Sonnet call = 10 messages. Same.
- A/B stage: 20 projects × 2 runs (A + B). Run B costs ~Sonnet leg extra.
  Total ~20 extra Sonnet messages. ~60 min wall-clock.

Cumulative: ~50 extra Sonnet messages over 8-10 weeks. Easily within
subscription budget.

---

## Что делать при критическом сбое

| Сбой | Action |
|---|---|
| Pipeline crash в shadow | rollback ENV: `STAGE01_COMPLETENESS_LENS_ENABLED=false`. Investigate. |
| Engineer reports data corruption | rollback + audit `version_service` для affected projects. |
| Massive FP explosion (engineer count > 50% thumbs-down) | rollback `STAGE01_COMPLETENESS_BY_DOC_TYPE=""`. Investigate prompt regression. |
| Cost spike (subscription quota threat) | rollback + add cost guard в `completeness_lens.py`. |

См. `integration_plan/rollback_steps.md` для конкретных команд.

---

## После Production deploy

Continuous monitoring:
- daily / weekly aggregate engineer feedback (через UI thumbs).
- monthly metric report (Phase 1 enabled vs disabled cohorts).
- quarterly review: tune `STAGE01_COMPLETENESS_BY_DOC_TYPE` based on data.

Никаких "set and forget" — Phase 1 — это living system, требует periodic
tune'инга.
