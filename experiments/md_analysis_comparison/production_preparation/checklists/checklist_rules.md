# Checklist Rules — shared semantics for completeness lens

**Status:** production-preparation. Files in this directory are loaded by the
completeness lens runner via simple file read. Keep them human-parseable —
no YAML/JSON frontmatter, no exotic markdown features.

## 1. How a checklist is loaded

The completeness lens runner does the following at lens-prompt assembly time:

1. Resolve discipline code for the MD (one of `AR / KJ / KM / EOM / OV / VK /
   SS / MULTI`). If discipline cannot be narrowed, fall back to `MULTI`.
2. Read `production_preparation/checklists/<DISCIPLINE>.md` as text.
3. Filter items whose `applies=` gate matches the resolved `document_type`
   (`full_rd` / `audit_comparison` / `tz_vs_rd` / `specification_only`).
4. Splice the filtered items into the completeness lens prompt, preserving
   tier headers (Mandatory / Recommended / Conditional / Anti-patterns).
5. Run Sonnet completeness lens; for each emitted finding, runner enriches
   `problem_class` from the checklist item that fired.

This is **read-only** for the runner. Editing the checklist text is the only
way to change lens behaviour at runtime.

## 2. The four tiers

| Tier | Section header in file | Lens behaviour when item absent |
|---|---|---|
| Mandatory | `## Mandatory required sections / artifacts`, `## Mandatory required calculations`, `## Mandatory required specifications / schedules`, `## Mandatory required diagrams / schemes`, `## Mandatory required parameters (per ...)`, `## Mandatory coordination requirements (cross-discipline)`, `## Mandatory testing / commissioning requirements` | Emit a finding. Severity per the table in §3. |
| Recommended | `## Recommended items` | Emit only if obvious from MD that the item is absent. Severity = РЕКОМЕНДАТЕЛЬНОЕ. |
| Conditional | `## Conditional items (by object type / document_type)` | Apply only when the applicability gate inside the item text matches. If unsure → do not emit. |
| Anti-pattern | `## Anti-patterns — DO NOT flag these as findings` | Suppress: if the candidate finding matches an anti-pattern, drop it. |

Section headers above are matched verbatim — do not rename them when editing.

## 3. Severity mapping

| Symptom (from research v1/v2 completeness prompt) | Severity |
|---|---|
| `missing_mandatory_section` declared as absent by the MD itself (e.g. "раздел отсутствует") | КРИТИЧЕСКОЕ |
| `missing_mandatory_section` silently absent (no mention but expected) | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ |
| `missing_mandatory_schedule` (cable journal, spec, ведомость) | ЭКСПЛУАТАЦИОННОЕ |
| `missing_mandatory_parameter` (specific param missing on element) | ЭКСПЛУАТАЦИОННОЕ; escalate to КРИТИЧЕСКОЕ only if construction is blocked without it |
| `missing_diagram` (схема, узел, аксонометрия) | ЭКСПЛУАТАЦИОННОЕ |
| `missing_calculation_basis` (расчёт отсутствует или необоснован) | КРИТИЧЕСКОЕ for I-ПС / structural / load calc; otherwise ЭКСПЛУАТАЦИОННОЕ |
| `missing_norm_reference` (ссылка на норму отсутствует) | РЕКОМЕНДАТЕЛЬНОЕ |
| `incomplete_specification` (спецификация частично заполнена) | РЕКОМЕНДАТЕЛЬНОЕ unless required by norm |
| `stub_section` (раздел есть, но содержимое — заглушка) | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ |

Items in the checklist files declare the *baseline* severity inside the
`[..., severity=..., ...]` tag. The runner may downgrade based on declared-absence
heuristics (e.g. silently absent → ПРОВЕРИТЬ_ПО_СМЕЖНЫМ instead of КРИТИЧЕСКОЕ),
but must not upgrade past the declared level.

## 4. `applies=` gate semantics

Each item carries an `applies=` token that lists one or more `document_type`s
the item is valid for. Tokens are joined with `|`.

| Token | Semantics |
|---|---|
| `full_rd` | Полная рабочая документация — все mandatory sections / calculations / diagrams применимы. |
| `audit_comparison` | Сравнительный аудит (старый аудит vs ответ проектировщика). Применять только пункты, эксплицитно входящие в scope сравнения. Никаких phantom-секций. |
| `tz_vs_rd` | Сравнение ТЗ и РД. Mandatory items только если ТЗ затрагивает этот пункт. Расхождения параметров ловятся как `tz_rd_mismatch_<parameter>`. |
| `specification_only` | Только спецификация / параметрический документ. Mandatory schedule + Mandatory parameters per element — да. Sections / diagrams / calculations — нет. |

Если у item не указан `applies=`, runner трактует его как `applies=full_rd`
(консервативный default — не флагать вне full_rd).

## 5. `problem_class=` requirement

Every checklist item MUST declare a `problem_class` slug. The vocabulary is
fixed (see `dedup/problem_class_rules.md`):

- `missing_mandatory_section`
- `missing_mandatory_schedule`
- `missing_mandatory_parameter`
- `missing_diagram`
- `missing_calculation_basis`
- `missing_norm_reference`
- `incomplete_specification`
- `stub_section`

Slugs are case-sensitive. Adding a new slug requires updating
`dedup/problem_class_rules.md` first (the canonicalisation table is the source
of truth — checklist files must align).

## 6. Per-discipline caps

To avoid checklist bloat / over-aggressive flagging:

- Mandatory items per file: aim for **8-16**. KJ and EOM are at the upper end
  because they cover wider scope; AR / SS may be lower.
- Total items per file (Mandatory + Recommended + Conditional): **≤ 25**.
  Anti-patterns are not counted in this cap.
- Anti-patterns per file: **≥ 6** (per task spec; one of the categories from
  `checklist_quality_report.md`).
- Each item is one bullet line of the micro-format:
  ```
  - [problem_class=..., severity=..., applies=...]
    <Russian text> (<norm citation>).
  ```
  Multi-line wrap is allowed inside a single bullet; do not split into
  sub-bullets.

## 7. Maintenance policy

- **Owners:** инженер-эксперт по дисциплине (см. `disciplines/_registry.json`)
  + maintainer of `production_preparation/`.
- **Adding an item:** PR must include (a) the new item, (b) at least one
  example case in `examples/checklist_examples.md` that demonstrates the
  intended trigger, (c) at least one case from the research dataset that
  shows the item would not over-fire.
- **Deprecating an item:** delete the bullet. The runner is stateless: next
  lens call will not emit it. Do NOT leave commented-out items — they bloat
  the prompt token budget.
- **Renaming a `problem_class`:** update `dedup/problem_class_rules.md`
  canonicalisation table FIRST, then sweep checklist files. Sequence
  matters — dedup will silently drop findings with unknown classes.
- **Versioning:** the file's content hash is captured in lens telemetry
  (`completeness_checklist_hash`) so per-run regressions can be attributed
  to a specific edit.

## 8. Out-of-scope for checklist files

- Vendor-specific guidance (e.g. "use Schneider Electric for ВРУ").
- Norm-status verification (handled by 4-tier norms verification in
  `norms_db.json`).
- Per-finding evidence (handled by lens runner from the MD itself).
- Severity inflation (КРИТИЧЕСКОЕ for cosmetic items) — runner has a
  guardrail that re-checks anti-pattern hits.

## 9. Relation to research checklists

The files in `algorithm_research/prompt_optimization/checklists/` are the
*starting point*. They use a table-based tier-letter format (M / R / O).
The production version here:

- expands each tier into Mandatory / Recommended / Conditional / Anti-pattern
  sections;
- adds explicit `problem_class` slugs, `severity`, and `applies=` tokens;
- adds production-realistic coverage (testing / commissioning, cross-discipline,
  conditional items by object type);
- aligns with `document_type` routing (Phase 1 rollout).

When in doubt, the *production* file (this directory) wins over the research
file.
