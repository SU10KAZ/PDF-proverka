# Speculative Findings — что Phase 1 prompt должен подавить

**Дата:** 2026-05-20

5 примеров спекулятивных findings + правило, которое их ловит.

Реальные примеры из A0 baseline ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md))
показывают, что speculative_noise класс уменьшается в Phase 1, но всё ещё есть
кейсы. Правило 4 ("Speculation rule") в
[stage01_production_prompt.md](../prompts/stage01_production_prompt.md)
явно их фильтрует.

---

## Spec #1 — "Уточнить по АР"

```json
{
  "id": "T-008",
  "severity": "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ",
  "problem_class": "needs_clarification",
  "affected_system": "Узел примыкания кровли",
  "problem": "Узел примыкания требует уточнения по АР",
  "evidence_quote": "Узел: см. АР",
  "confidence": 0.5
}
```

**Правило, ловящее это (правило 4):**
> Запрещены формулировки «уточнить X», «проверить Y», «требуется проверка»
> без конкретного дефекта.

**Что не так:** "требует уточнения" не указывает на **дефект**, только на
**отсутствие данных**. Это нормальная inter-discipline координация.

**Допустимая замена:**

Если в МДcurrent_method НЕ нашёл ничего конкретного — finding **не подаётся**.

Если есть конкретное расхождение (например, расчёт перемычки сделан для пролёта
900 мм, а в АР проём 1200 мм) — finding подаётся **с конкретикой**:

```json
{
  "problem_class": "calc_dimension_mismatch",
  "problem": "Расчёт перемычки выполнен для пролёта 900 мм; расчёт для пролёта 1200 мм (Д-2 по АР) отсутствует",
  "evidence_quote": "Перемычка Б.3: пролёт 900 мм",
  ...
}
```

---

## Spec #2 — "Проверить корректность расчёта"

```json
{
  "id": "T-010",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "verify_calculation",
  "affected_system": "Расчёт нагрузок",
  "problem": "Проверить корректность расчёта селективности",
  "evidence_quote": "Расчёт селективности приведён в таблице 5.",
  "confidence": 0.4
}
```

**Правило 4 (Speculation rule)** + правило 9 (`confidence < 0.6 — не сообщай`).

**Что не так:**
- "Проверить корректность" — нет конкретики, что именно неверно.
- confidence 0.4 — само по себе ниже порога.

**Допустимая замена:**

Сделать конкретный расчёт и flag'нуть конкретный дефект:

```json
{
  "problem_class": "selectivity_ratio_inadequate",
  "problem": "Отношение I_расц-вышестоящего/I_расц-нижестоящего = 40/40 = 1,0 — селективность не достигается",
  "severity_reasoning": "При ratio 1,0 оба автомата сработают одновременно — selectivity невозможна",
  "evidence_quote": "Декларация: '40/40 = 1,0 соответствует требованиям'",
  ...
}
```

---

## Spec #3 — "Возможно, требуется анализ"

```json
{
  "id": "T-002",
  "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
  "problem_class": "possible_issue",
  "affected_system": "Защитный слой бетона",
  "problem": "Возможно, защитный слой бетона требует дополнительного анализа",
  "evidence_quote": "Защитный слой: см. узел.",
  "confidence": 0.55
}
```

**Правило 4** + правило 9 (`< 0.6`).

**Что не так:**
- "Возможно" — speculation hedge.
- "Требует анализа" — нет конкретики.
- confidence ниже threshold.

**Допустимая замена:**

Либо подать конкретно (если защитный слой ниже норматива):
```json
{
  "problem_class": "concrete_cover_insufficient",
  "problem": "Защитный слой 20 мм меньше норматива (35 мм для класса XC3 по СП 28.13330.2017)",
  "evidence_quote": "Защитный слой: 20 мм. Класс XC3.",
  ...
}
```

Либо вообще НЕ подавать — если в МД действительно недостаточно данных.

---

## Spec #4 — "Не указан тип системы заземления"

```json
{
  "id": "T-006",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "missing_specification",
  "affected_system": "Система заземления",
  "problem": "Не указан тип системы заземления — необходимо уточнить",
  "evidence_quote": "Заземление: см. схему ЭОМ.",
  "confidence": 0.6
}
```

Это **borderline speculative**. Зависит от document_type:

- **`full_rd`:** В полном РД отсутствие типа заземления — реальный дефект.
  Finding подаётся **без** "необходимо уточнить" — просто факт отсутствия:
  ```json
  {
    "problem": "Тип системы заземления (TN-C-S / TT / TN-S) не указан в разделе",
    "evidence_quote": "Заземление: см. схему ЭОМ. [при отсутствии явной маркировки]",
    "severity": "КРИТИЧЕСКОЕ" or "ЭКСПЛУАТАЦИОННОЕ" (зависит от impact),
  }
  ```
- **`audit_comparison` / `specification_only`:** finding **не подаётся**.
  Это не предмет сравнения / спецификации (HARD RULE, document_type routing).

---

## Spec #5 — Multi-finding speculation lump

```json
{
  "id": "T-001",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "multiple_concerns",
  "affected_system": "Раздел в целом",
  "problem": "Состав раздела АПС неполный: отсутствуют планы этажей, функциональная схема, кабельный журнал",
  "evidence_quote": "[нет упоминания планов/схемы/журнала]",
  "confidence": 0.7
}
```

(Реальный пример из ss_02_fire_alarm A1-v2 — классифицирован как
speculative_noise в [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md).)

**Правило 4** + правило 2 (`affected_system` "Раздел в целом" — слишком
generic).

**Что не так:**
- Lump-finding охватывает 3 отдельных дефекта.
- `affected_system="Раздел в целом"` — нарушает Class rule (affected_system
  должен быть конкретным).
- На `audit_comparison` / `specification_only` — out-of-scope.

**Допустимая замена:**

Подавать 3 отдельных finding'a:

```json
[
  {
    "problem_class": "missing_mandatory_diagram",
    "affected_system": "Поэтажные планы расстановки извещателей",
    "problem": "Поэтажные планы расстановки извещателей отсутствуют",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    ...
  },
  {
    "problem_class": "missing_mandatory_diagram",
    "affected_system": "Функциональная схема АПС",
    "problem": "Функциональная схема АПС не приведена",
    ...
  },
  {
    "problem_class": "missing_mandatory_schedule",
    "affected_system": "Кабельный журнал АПС",
    "problem": "Кабельный журнал АПС отсутствует",
    ...
  }
]
```

Каждое — атомарное. Dедуп их не свернёт (разные `affected_system`).

---

## Правило-ловушка (suppress patterns)

`stage01_production_prompt.md` §"Запреты (KILL-LIST)":

1. "НЕ пиши «уточнить», «проверить», «верифицировать» без конкретного
   дефекта."

Patterns для регулярки (на стороне corrector, post-LLM):
- `r"\bуточнить\b"` без `:[А-Я]` следом (как `Уточнить: M_x=...`).
- `r"\bтребует.*\bпроверк\w+"`.
- `r"\bвозможно,? требует"`.
- `r"\bнеобходимо.*\bдополнительн"`.

Critic verdict для таких: `speculative` → corrector dropит или просит LLM
переформулировать с конкретикой.

---

## Где это видно в данных

[a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md) показывает
`speculative_noise` count per case:

| Case | speculative_noise |
|---|---|
| ss_02_fire_alarm | 5 |
| km_01_truss_design | 3 |
| eom_02_grounding | 3 |
| ov_02_smoke_protection | 1 |
| ar_02_facade_thermal | 1 |
| ... | ... |
| **TOTAL across 24 cases** | **27** |

Это уже **после** Phase 1 prompt'а — но KILL-LIST не идеально соблюдается.
Future iteration: усилить KILL-LIST + добавить regex-postcheck в corrector.

Если speculative_noise > 5 per case → flag в regression suite + investigate.

---

## Acceptance criteria

Для каждого finding в выходе Phase 1:
- НЕ содержит "уточнить", "проверить", "верифицировать", "возможно" в
  `problem` (если содержит — нужно конкретное обоснование в `description`).
- `confidence ≥ 0.6` (правило 9).
- `affected_system` конкретный (не "Раздел в целом" / "Документ").
- Если КРИТ — `severity_reasoning` ≤ 120 символов с конкретикой.

Engineer review canary outputs обязан проверить эти 4 пункта.
