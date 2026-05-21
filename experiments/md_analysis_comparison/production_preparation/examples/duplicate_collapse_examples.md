# Duplicate Collapse Examples

**Дата:** 2026-05-20

5 примеров дубликатов на входе → что dедуп оставляет на выходе и почему.
Дополняет [`dedup_examples.md`](dedup_examples.md) более realistic кейсами.

---

## Сценарий 1 — Те же findings, разный wording (current_method + completeness)

**Input:**

```json
[
  {
    "id": "OPUS-1",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "cable_undersized",
    "affected_system": "Кабель ввода ВРУ-1",
    "problem": "АВВГ 4×95 не проходит по нагреву",
    "evidence_quote": "АВВГ 4×95, Iдоп 220 А",
    "norm": "ПУЭ-7, п. 1.3.10",
    "norm_quote": null,
    "confidence": 0.90,
    "source_agent": "current_method"
  },
  {
    "id": "SONNET-1",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "cable_undersized",
    "affected_system": "Кабель ввода ВРУ-1",
    "problem": "Iдоп кабеля 220 А < расчётный 302 А",
    "evidence_quote": "Расчётный ток 302 А. Кабель АВВГ 4×95.",
    "norm": "ПУЭ-7, п. 1.3.10; СП 256.1325800.2016, п. 7.4.3",
    "norm_quote": "Длительно допустимые токовые нагрузки...",
    "confidence": 0.95,
    "source_agent": "completeness"
  }
]
```

**Анализ:**
- Same `class_key`: `cable_undersized|кабель ввода вру 1|none|none`.
- Обе КРИТ → срабатывает КРИТ-protect → НЕ сворачиваются.

**Output:** 2 findings, оба сохранены. `critical_collapsed_count = 1`.

**Почему сохранены:** rationale — два независимых KRT findings могут отражать
разные аспекты одной проблемы; safer сохранить оба и дать engineer review.

---

## Сценарий 2 — Real semantic duplicate (ЭКСПЛ, не КРИТ)

**Input:**

```json
[
  {
    "id": "OPUS-3",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem_class": "missing_specification_item",
    "affected_system": "Спецификация ЭОМ",
    "problem": "Не указана марка кабеля для линии W3",
    "evidence_quote": "W3: длина 35 м, сечение 4×10",
    "confidence": 0.70,
    "source_agent": "current_method"
  },
  {
    "id": "SONNET-3",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem_class": "missing_specification_item",
    "affected_system": "Спецификация ЭОМ",
    "problem": "Марка кабеля для W3 (ВВГнг/АВВГ?) не приведена",
    "evidence_quote": "W3: длина 35 м, сечение 4×10. Марка кабеля: —",
    "norm": "ГОСТ 21.613-2014",
    "confidence": 0.88,
    "source_agent": "completeness"
  }
]
```

**Анализ:**
- Same class_key.
- Не КРИТ.
- `canonical_score`:
  - OPUS-3: `(3, 0.70, 0, ...)`
  - SONNET-3: `(3, 0.88, 1, ...)` — norm filled, longer
- SONNET-3 wins.

**Output:**
```json
{
  "id": "SONNET-3",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "missing_specification_item",
  "affected_system": "Спецификация ЭОМ",
  "problem": "Марка кабеля для W3 (ВВГнг/АВВГ?) не приведена",
  "confidence": 0.88,
  "source_agents": ["current_method", "completeness"],
  "is_canonical": true,
  "class_key": "missing_specification_item|спецификация эом|none|none",
  "duplicate_count_in_cluster": 1
}
```

`source_agents` показывает, что обе линзы нашли — strong signal.

---

## Сценарий 3 — Cross-method priority override

**Input:**

```json
{
  "current_method": [
    {
      "id": "OPUS-7",
      "severity": "ЭКСПЛУАТАЦИОННОЕ",
      "problem_class": "norm_obsolete",
      "affected_system": "Раздел нормативная база",
      "problem": "Ссылка на отменённый ГОСТ 23166-99",
      "confidence": 0.92,
      "norm": "ГОСТ 23166-2021"
    }
  ],
  "completeness": [
    {
      "id": "SONNET-7",
      "severity": "ЭКСПЛУАТАЦИОННОЕ",
      "problem_class": "norm_obsolete",
      "affected_system": "Раздел нормативная база",
      "problem": "В нормативной базе указан ГОСТ 23166-99, действует ГОСТ 23166-2021",
      "confidence": 0.94,
      "norm": "ГОСТ 23166-2021",
      "norm_quote": "Окна и двери. Общие технические условия...",
    }
  ]
}
```

**Анализ (merge_across_methods с priority=[current_method, completeness]):**
- Same class_key.
- Не КРИТ.
- `_score()`:
  - OPUS-7: `(-0, 3, 0.92, 1, 36, 0)`
  - SONNET-7: `(-1, 3, 0.94, 1, 60, 0)`
- OPUS-7 wins на priority (priority_index=0 → `-0` > `-1`).

**Output:**
```json
{
  "id": "OPUS-7",
  "problem": "Ссылка на отменённый ГОСТ 23166-99",
  ...
  "source_agents": ["completeness", "current_method"],
  "duplicate_count_in_cluster": 1
}
```

**Note:** Priority overrides canonical_score. Это намеренно — current_method
is established production source.

Engineer может видеть, что completeness тоже это поймал (`source_agents`).

---

## Сценарий 4 — Fuzzy на legacy A0 data (нет problem_class)

**Input** (старый A0 baseline, нет structured fields):

```json
[
  {
    "id": "T-001",
    "category": "Эксплуатационное",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem": "СНиП 2.04.01-85 устарел — действует СП 30.13330.2020",
    "evidence_quote": "Расчёт по СНиП 2.04.01-85*",
    "confidence": 0.90
  },
  {
    "id": "T-005",
    "category": "Эксплуатационное",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem": "Применена отменённая редакция СНиП 2.04.01-85*",
    "evidence_quote": "СНиП 2.04.01-85*",
    "confidence": 0.85
  }
]
```

**class_dedup:**
- Нет `problem_class` → fallback: `category:short_sig(problem)`.
- T-001 sig: "снип 2 04 01 85 устарел — действует сп 30 13330 2020"
- T-005 sig: "применена отменённая редакция снип 2 04 01 85"
- → разные class_key → НЕ сворачиваются.

**fuzzy_dedup (threshold 0.7):**
- Signature T-001: `эксплуатационное снип 2 04 01 85 устарел — действует сп 30 13330 2020 расчёт по снип 2 04 01 85`
- Signature T-005: `эксплуатационное применена отменённая редакция снип 2 04 01 85 снип 2 04 01 85`
- SequenceMatcher.ratio() ≈ 0.71 (close call) → marginal match.

Если ratio >= 0.7 — collapse. T-001 (higher confidence) becomes canonical.

**Output:** 1 finding (T-001 канонический).

**Edge case:** Если threshold = 0.75 — НЕ collapse (ratio 0.71 < 0.75).
`STAGE01_DEDUP_FUZZY_THRESHOLD=0.7` — default; tighten до 0.75 если будут
false collapses.

---

## Сценарий 5 — Внутри-кластерные близнецы (3 КРИТ одного класса)

**Input:**

```json
[
  {
    "id": "A",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "norm_obsolete",
    "affected_system": "Раздел нормативная база",
    "problem": "СНиП 2.04.01-85 — обязателен по тексту, фактически отменён",
    "severity_reasoning": "Обязательная ссылка на отменённую — нарушение legal basis",
    "confidence": 0.95
  },
  {
    "id": "B",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "norm_obsolete",
    "affected_system": "Раздел нормативная база",
    "problem": "ГОСТ 23166-99 указан как действующий — фактически отменён",
    "severity_reasoning": "Обязательная ссылка на устаревшую редакцию",
    "confidence": 0.92
  },
  {
    "id": "C",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "norm_obsolete",
    "affected_system": "Раздел нормативная база",
    "problem": "СП 14.13330 указан без года — заменён несколько раз",
    "severity_reasoning": "Без редакции невозможно подтвердить применимость",
    "confidence": 0.85
  }
]
```

**Анализ:**
- Same class_key: `norm_obsolete|раздел нормативная база|none|none`.
- Все 3 — КРИТ → срабатывает КРИТ-protect.

**class_dedup поведение:**
- A — первый КРИТ, canonical своего кластера. class_key без суффикса.
- B — второй КРИТ, canonical своего кластера. class_key с суффиксом `#crit1`.
- C — третий КРИТ, canonical своего кластера. class_key с суффиксом `#crit2`.

`critical_collapsed_count = 2` (B и C добавили суффикс).

**Output:** 3 finding'a, все сохранены.

**Trade-off:** возможно, engineer хотел бы видеть это как ОДИН lump finding
("нормативная база содержит 3 устаревшие ссылки"). НО dедуп не делает
lump'инг — это работа prompt'а (не дублировать формулировки).

Если 3 КРИТ findings одного класса часто появляются в outputs — это signal,
что prompt должен учить LLM lump'ить заранее. Это **prompt fix**, не **dedup
fix**.

---

## Сводка частоты

Из 24-case A1-v2 sweep ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)):

| Pattern | Частота на кейс |
|---|---|
| duplicate_of_gt (semantic dup matches GT) | ~ 8 (среднее) |
| Same class_key, обе КРИТ (КРИТ-protect срабатывает) | ~1 (среднее) |
| Same class_key, не-КРИТ (collapse работает) | ~2 (среднее) |
| Fuzzy match > 0.7 (legacy / cross-method) | ~3 на multi-source data |

Phase 0 (class_dedup) сэкономит ~2 findings/case на A1-v2 output. Phase 0 +
fuzzy сэкономит ещё ~3 на legacy outputs.

---

## Acceptance criteria для dедупа

После dедупа в `03_findings.json` НЕ должно быть:
- Двух finding'ов с **тем же** `class_key` и **тем же** severity (если оба
  не-КРИТ).
- Двух finding'ов с identical `evidence_quote` (если class_dedup correctly
  выставил `problem_class`).

После Phase 0 deploy: smoke test проверяет это invariant. Если violation —
investigate prompt: возможно, LLM эмитит дубликаты с very different
`affected_system` strings, которые normalisation не сводит.
