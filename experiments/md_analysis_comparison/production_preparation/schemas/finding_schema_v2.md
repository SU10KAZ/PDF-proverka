# Finding Schema — v2

**Status:** production-preparation (NOT in production yet)
**Production v1 schema:** `backend/app/schemas/text_analysis.json`
**Anchor research:** `algorithm_research/reports/phase0_phase1_validation_report.md` §1.1

v2 keeps every production v1 field and **adds** classification / evidence /
context fields needed by Phase 0 dedup (`class_dedup`, `fuzzy_dedup`) and
Phase 1 completeness routing.

Backwards compatibility: every new field is **optional**. A v1 finding
loaded as v2 just has its new fields default to `null` / `false` / empty.
A v2 finding written back to disk and read by v1 code passes through as
long as v1 code uses additive merging (it does — see
`backend/app/services/findings/findings_service.py`).

Schema is described in JSON-Schema style here (markdown + embedded JSON
snippets). No `*.schema.json` file is shipped; production gets that when
this proposal is accepted.

---

## Top-level finding object

```json
{
  "id": "T-001",
  "problem_class": "outdated_norm_reference",
  "affected_system": "ВРУ-1",
  "interface_type": "electrical_supply",
  "discipline_pair": "EOM,OV",
  "category": "Норматив",
  "severity": "КРИТИЧЕСКОЕ",
  "discipline": "ЭОМ",
  "cross_discipline_with": ["ОВ"],

  "problem": "Применена отменённая редакция СП 31-110-2003",
  "description": "В пояснительной записке использована редакция от 2003 г. ...",
  "recommendation": "Перейти на СП 256.1325800.2016",
  "risk": "Несоответствие обязательным требованиям ПП РФ №815",

  "severity_reasoning": "Прямое нарушение обязательного СП",

  "norm": "СП 256.1325800.2016 (ред. 29.01.2024), п. 7.1.1",
  "norm_quote": "...пункт цитируется дословно...",
  "norm_confidence": 0.92,
  "confidence": 0.88,

  "evidence_quote": "В соответствии с СП 31-110-2003 принять...",
  "md_excerpt": "...исходный фрагмент MD...",
  "source": "MD (строка 142)",

  "is_beyond_gt_useful": false,
  "internal_duplicate_of": null,
  "is_canonical": true,
  "class_key": "outdated_norm_reference|вру 1|electrical_supply|eom,ov",
  "duplicate_count_in_cluster": 0,
  "source_agents": ["current_method", "completeness"]
}
```

---

## Field groups

### 1. Identity

| Field | Type | Required | Semantics |
|---|---|---|---|
| `id` | string | YES | Production format `^T-\d{3}$` for text findings; merged `F-\d{3}` after Stage 03. Unchanged from v1. |

### 2. Classification (NEW in v2)

| Field | Type | Required | Semantics |
|---|---|---|---|
| `problem_class` | string | optional | Slug from the canonical list (see `dedup/problem_class_rules.md`). Drives class_dedup. |
| `affected_system` | string | optional | The component / equipment unit / system the finding applies to (e.g. `"ВРУ-1"`, `"ДГУ"`). Drives class_dedup. |
| `interface_type` | string \| null | optional | When the finding is at an interface between disciplines (e.g. `electrical_supply`, `heat_load`, `cable_passage`). Default `null`. |
| `discipline_pair` | string \| null | optional | Alphabetised CSV like `"EOM,OV"` for cross-discipline findings. Default `null`. |
| `category` | string | required | Production v1 field. Free-text category. Kept for backward compat. |
| `severity` | enum | required | One of: `КРИТИЧЕСКОЕ`, `ЭКОНОМИЧЕСКОЕ`, `ЭКСПЛУАТАЦИОННОЕ`, `ПРОВЕРИТЬ ПО СМЕЖНЫМ`, `РЕКОМЕНДАТЕЛЬНОЕ`. (Underscore variant `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ` is also accepted by dedup.) Unchanged from v1. |

**Validation:**
- `problem_class` SHOULD be one of the slugs in `problem_class_rules.md`. If
  it isn't, dedup falls back to a category-based composite key — still safe.
- `severity_reasoning` (see Engineering) is REQUIRED for КРИТИЧЕСКОЕ; ≤ 120 chars.
- `discipline_pair` MUST be alphabetised CSV when present (e.g. `"EOM,OV"`,
  not `"OV,EOM"`).

### 3. Evidence

| Field | Type | Required | Semantics |
|---|---|---|---|
| `evidence_quote` | string | optional | NEW in v2. Verbatim quote from the MD that supports the finding. Used by fuzzy_dedup signature. |
| `md_excerpt` | string | optional | NEW in v2. Slightly longer surrounding context (a few lines from MD). |
| `source` | string | required | Production v1 field. e.g. `"MD (строка 142)"`, `"PDF (стр. 5)"`. |
| `norm` | string | optional | Production v1 field. Default `""`. |
| `norm_quote` | string \| null | optional | Production v1 field. Default `null`. |
| `norm_confidence` | number | optional | NEW in v2. `0.0–1.0`. Confidence of the norm citation. |
| `confidence` | number | optional | NEW in v2. `0.0–1.0`. Overall confidence in the finding. Used as the second sort key in `_canonical_score` (after severity). |

**Validation:**
- `confidence`, `norm_confidence` in `[0.0, 1.0]`.
- `evidence_quote` length recommended ≤ 500 chars (longer is allowed but
  fuzzy_dedup only uses first 120 of it for its signature).

### 4. Engineering (production v1 + v2 additions)

| Field | Type | Required | Semantics |
|---|---|---|---|
| `problem` | string | optional in v1, semi-required in v2 | One-line problem statement. v1 calls this `finding`; v2 prefers `problem` but `finding` is accepted. |
| `description` | string | optional | NEW in v2. Multi-line explanation. |
| `recommendation` | string | optional | NEW in v2. What the auditor proposes. |
| `risk` | string | optional | NEW in v2. The consequence of leaving the issue unaddressed. |
| `severity_reasoning` | string | REQUIRED if `severity == КРИТИЧЕСКОЕ`, ≤ 120 chars | NEW in v2. One-sentence justification for the severity assignment. |

**Validation:**
- `severity_reasoning` REQUIRED when `severity == "КРИТИЧЕСКОЕ"`. Hard cap
  at 120 chars (enforced by post-LLM critic, not by the dedup modules).

### 5. Context

| Field | Type | Required | Semantics |
|---|---|---|---|
| `discipline` | string | optional | Which discipline the finding belongs to (e.g. `"ЭОМ"`, `"ОВ"`). |
| `cross_discipline_with` | array<string> | optional | Other disciplines this finding touches. May be `[]`. |
| `related_block_ids` | array<string> | optional | Production v1 field. Block IDs from `02_blocks_analysis.json`. Default `[]`. |

### 6. Flags (dedup / observability)

| Field | Type | Required | Semantics |
|---|---|---|---|
| `is_beyond_gt_useful` | bool | optional | NEW in v2. The LLM (or critic) marks a finding as engineering-useful but outside the ground-truth scope. Used in research metrics to distinguish noise from beyond-scope value. |
| `internal_duplicate_of` | string \| null | optional | NEW in v2. Set by `class_dedup.mark_duplicates` when a finding is a class-duplicate of another. Value is the id of the canonical. `null` for canonicals. |
| `is_canonical` | bool | optional | NEW in v2. `true` if this is the canonical of its cluster. Set by all dedup functions. |
| `class_key` | string | optional | NEW in v2. Computed class key string for observability. Set by all dedup functions. |
| `duplicate_count_in_cluster` | int | optional | NEW in v2. Number of duplicates that were collapsed into this canonical. `0` if no duplicates. |
| `source_agents` | array<string> | optional | NEW in v2. The set of agents/methods that produced findings collapsed into this canonical. e.g. `["current_method", "completeness"]`. |

**Validation:**
- `internal_duplicate_of` must reference an existing finding `id` in the
  same document, or be `null`.
- `is_canonical = true` implies `internal_duplicate_of = null` (and vice
  versa).

---

## Example: a КРИТИЧЕСКОЕ cross-discipline finding (v2 full shape)

```json
{
  "id": "T-007",
  "problem_class": "cross_discipline_mismatch",
  "affected_system": "ВРУ-1",
  "interface_type": "electrical_supply",
  "discipline_pair": "EOM,OV",
  "category": "Расхождение между разделами",
  "severity": "КРИТИЧЕСКОЕ",
  "discipline": "ЭОМ",
  "cross_discipline_with": ["ОВ"],
  "problem": "Суммарная мощность ЭОМ (380 кВт) не покрывает тепловую нагрузку ОВ (290 кВт + 100 кВт резерва)",
  "description": "В ЭОМ заявлено 380 кВт. ОВ требует 290 кВт постоянно + 100 кВт пускового. Дефицит 10 кВт без резервирования по 2-й категории.",
  "recommendation": "Увеличить расчётную мощность ВРУ-1 до 420 кВт или согласовать с ОВ снижение пускового резерва.",
  "risk": "При пуске тепловых установок возможна отсечка по току",
  "severity_reasoning": "Прямое нарушение ПУЭ-7 п.5.3.45 — недостаток мощности на пусковой режим",
  "norm": "ПУЭ-7, п. 5.3.45",
  "norm_quote": "Мощность ВРУ должна покрывать пусковой ток...",
  "norm_confidence": 0.85,
  "confidence": 0.92,
  "evidence_quote": "ВРУ-1: установленная мощность 380 кВт ... ОВ: тепловая нагрузка 290 кВт",
  "md_excerpt": "...исходный фрагмент таблицы нагрузок...",
  "source": "MD (строки 142, 287)",
  "is_beyond_gt_useful": false,
  "internal_duplicate_of": null,
  "is_canonical": true,
  "class_key": "cross_discipline_mismatch|вру 1|electrical_supply|eom,ov",
  "duplicate_count_in_cluster": 1,
  "source_agents": ["current_method", "completeness"],
  "related_block_ids": ["block_004_2"]
}
```

---

## Backwards-compatibility notes (per new field)

| New field | If absent on read | If absent on write |
|---|---|---|
| `problem_class` | Dedup falls back to category-based key (still safe). | Production stage keeps writing without it; v2 dedup degrades to v1 behaviour. |
| `affected_system` | Dedup falls back to evidence_quote signature. | Same — degrades gracefully. |
| `interface_type` | Treated as `None`; class_key has empty 3rd slot. | OK. |
| `discipline_pair` | Treated as `None`. | OK. |
| `severity_reasoning` | Production cannot enforce ≤120 char rule on КРИТ — log warning only. | OK. |
| `confidence` | Treated as `0.0` in canonical scoring (worst). | OK. |
| `evidence_quote` | fuzzy_dedup signature uses empty string in that slot. | OK. |
| `md_excerpt` | No effect on dedup. | OK. |
| `is_beyond_gt_useful` | Treated as `false`. | OK. |
| `internal_duplicate_of` | Treated as `null`. | OK. |
| `is_canonical` | Treated as `true` (no dedup ran). | OK. |
| `class_key` | Recomputed by dedup if needed. | OK. |
| `duplicate_count_in_cluster` | Treated as `0`. | OK. |
| `source_agents` | Treated as empty list. | OK. |

**Forward compatibility:** v2 readers should NOT reject unknown fields.
Production v1 already uses `additionalProperties: false` on
`text_analysis.json`, so a v2 schema migration step needs to either (a)
relax that to allow new fields, or (b) explicitly enumerate the v2 fields
in the v2 schema. Recommendation: (b), to keep validation strict.
