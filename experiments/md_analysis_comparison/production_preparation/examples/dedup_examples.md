# Dedup Examples — 5 worked decisions

**Дата:** 2026-05-20

Конкретные dedup-решения от `class_dedup.collapse_to_canonical` и
`fuzzy_dedup.fuzzy_collapse`. Все примеры — на realistic Russian-language
findings.

---

## Пример 1 — class_dedup exact match (collapse)

**Input** (2 findings из current_method + completeness lens на одном проекте):

```json
[
  {
    "id": "T-003",
    "category": "Критическое",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "cable_undersized",
    "affected_system": "кабель ввода вру 1",
    "problem": "АВВГ 4×95 не проходит по нагреву",
    "evidence_quote": "Кабель ввода: АВВГ 4×95, Iдоп 220 А",
    "norm": "ПУЭ-7, п. 1.3.10",
    "confidence": 0.90,
    "source_agent": "current_method"
  },
  {
    "id": "T-007",
    "category": "Критическое",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "cable_undersized",
    "affected_system": "кабель ввода вру 1",
    "problem": "Iдоп кабеля 220 А < расчётный 302 А",
    "evidence_quote": "Расчётный ток 302 А. Кабель ввода АВВГ 4×95.",
    "norm": "ПУЭ-7, п. 1.3.10; СП 256.1325800.2016, п. 7.4.3",
    "norm_quote": "Длительно допустимые токовые нагрузки на провода и кабели приведены в таблицах...",
    "confidence": 0.95,
    "source_agent": "completeness"
  }
]
```

**Class key** обоих findings:
```
cable_undersized|кабель ввода вру 1|none|none
```

**Решение:** оба КРИТИЧЕСКОЕ → срабатывает КРИТ-protect → **оба сохранены**
как separate canonical. `class_key` второго получает suffix `#crit1`.
`critical_collapsed_count = 1`.

**Output:** 2 finding'a, оба остаются.

---

## Пример 2 — class_dedup non-critical collapse

**Input:**

```json
[
  {
    "id": "T-010",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem_class": "missing_specification_item",
    "affected_system": "спецификация раздела ЭОМ",
    "problem": "Отсутствует кабельный журнал",
    "confidence": 0.7,
    "source_agent": "current_method"
  },
  {
    "id": "T-011",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem_class": "missing_specification_item",
    "affected_system": "спецификация раздела ЭОМ",
    "problem": "Не приведён кабельный журнал отходящих линий",
    "norm": "ГОСТ 21.613-2014",
    "confidence": 0.85,
    "source_agent": "completeness"
  }
]
```

Class key обоих: `missing_specification_item|спецификация раздела эом|none|none`.

**canonical_score:**
- T-010: `(3, 0.7, 0, 30, 0)` (sev_weight=3 ЭКСПЛ, conf=0.7, norm_filled=0)
- T-011: `(3, 0.85, 1, 50, 0)` (norm_filled=1, conf=0.85, длиннее)

T-011 выигрывает.

**Решение:** T-011 — canonical; T-010 — duplicate. Output:
```json
{
  "id": "T-011",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "missing_specification_item",
  ...
  "source_agents": ["current_method", "completeness"],
  "is_canonical": true,
  "class_key": "missing_specification_item|спецификация раздела эом|none|none",
  "duplicate_count_in_cluster": 1
}
```

`same_class_drops = 1`.

---

## Пример 3 — fuzzy_dedup на baseline data (no problem_class)

**Input** (legacy A0 baseline — без `problem_class`):

```json
[
  {
    "id": "T-001",
    "category": "Эксплуатационное",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem": "Норма СНиП 2.04.01-85 устарела — заменена СП 30.13330.2020",
    "evidence_quote": "Расчёт по СНиП 2.04.01-85*",
    "confidence": 0.9
  },
  {
    "id": "T-008",
    "category": "Эксплуатационное",
    "severity": "ЭКСПЛУАТАЦИОННОЕ",
    "problem": "Применён отменённый СНиП 2.04.01-85*",
    "evidence_quote": "Применён СНиП 2.04.01-85*",
    "confidence": 0.92
  }
]
```

class_dedup: оба finding'a имеют разные `class_key`'ы (потому что fallback
`category + signature(problem)` даёт разные signature'ы).

Поэтому **class_dedup не сворачивает**. Запускаем fuzzy:

**Signature** для каждого:
- T-001: `эксплуатационное норма снип 2 04 01 85 устарела заменена сп 30 13330 2020 расчёт по снип 2 04 01 85`
- T-008: `эксплуатационное применён отменённый снип 2 04 01 85 применён снип 2 04 01 85`

SequenceMatcher.ratio() = ~0.72 — выше threshold 0.7 → fuzzy match.

**canonical_score:**
- T-001: (3, 0.9, 0, ...)
- T-008: (3, 0.92, 0, чуть короче problem)

T-008 побеждает по confidence.

**Решение:** T-008 — canonical, T-001 — drop. Output: 1 finding.

---

## Пример 4 — КРИТ vs не-КРИТ same key (КРИТ-protect)

**Input:**

```json
[
  {
    "id": "T-002",
    "severity": "КРИТИЧЕСКОЕ",
    "problem_class": "norm_obsolete",
    "affected_system": "раздел нормативная база",
    "problem": "ПУЭ-7 указан как обязательный — добровольный согласно ПП РФ №815",
    "severity_reasoning": "Обязательная ссылка на добровольную норму искажает обязательность",
    "confidence": 0.95
  },
  {
    "id": "T-005",
    "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
    "problem_class": "norm_obsolete",
    "affected_system": "раздел нормативная база",
    "problem": "Ссылка на устаревший ГОСТ 23166-99 — действует 2021",
    "confidence": 0.85
  }
]
```

Class key обоих: `norm_obsolete|раздел нормативная база|none|none`.

**Что произошло:** _split_critical_protected разделил members:
- critical = [T-002]
- non_critical = [T-005]

КРИТ-protect: T-002 остаётся как canonical своего кластера.
T-005 — отдельный канонический (single-member кластер с non-critical).

**Output:** 2 finding'a, оба остаются. **Никаких schлопываний.**

Это иллюстрирует, что КРИТ-protect срабатывает не только когда обе finding'a
КРИТ, но и когда mix.

`critical_collapsed_count = 0` (потому что был только 1 КРИТ; suffix#crit
добавляется только при > 1 КРИТ в кластере).

---

## Пример 5 — Cross-method merge с priority

**Сценарий:** `merge_across_methods(method_to_findings, priority=["current_method",
"completeness"])`.

**Input:**

```python
{
    "current_method": [
        {
            "id": "OPUS-1",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "problem_class": "missing_diagram",
            "affected_system": "однолинейная схема вру",
            "problem": "Отсутствует однолинейная схема ВРУ-1",
            "confidence": 0.80
        }
    ],
    "completeness": [
        {
            "id": "SONNET-1",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "problem_class": "missing_diagram",
            "affected_system": "однолинейная схема вру",
            "problem": "В составе раздела ЭОМ нет однолинейной схемы ВРУ-1",
            "norm": "ГОСТ 21.613-2014",
            "norm_quote": "В состав рабочей документации входит однолинейная схема...",
            "confidence": 0.92
        }
    ]
}
```

Class key обоих: `missing_diagram|однолинейная схема вру|none|none`.

_score():
- OPUS-1: `(-0, 3, 0.80, 0, 30, 0)` (priority_index 0 → -0)
- SONNET-1: `(-1, 3, 0.92, 1, 60, 0)` (priority_index 1 → -1)

Кортеж сравнивается по позициям. OPUS-1 имеет `-0` > SONNET-1 `-1` — **OPUS-1
canonical** (priority wins over higher canonical_score).

**Output:**
```json
{
  "id": "OPUS-1",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "missing_diagram",
  "affected_system": "однолинейная схема вру",
  "problem": "Отсутствует однолинейная схема ВРУ-1",
  "confidence": 0.80,
  "source_agents": ["completeness", "current_method"],
  "is_canonical": true,
  "class_key": "missing_diagram|однолинейная схема вру|none|none",
  "duplicate_count_in_cluster": 1
}
```

Note: priority overrides score. Это намеренно — current_method is the established
production source; completeness — auxiliary.

---

## Common patterns summary

| Pattern | Поведение |
|---|---|
| Same class key, обе КРИТ | Обе сохранены (КРИТ-protect) |
| Same class key, КРИТ + не-КРИТ | Обе сохранены (КРИТ-protect отделяет КРИТ) |
| Same class key, обе не-КРИТ | Свёрнуто в одну (higher canonical_score) |
| Different class key, fuzzy match > 0.7 | Свёрнуто в одну (если обе не-КРИТ) |
| Different class key, fuzzy match < 0.7 | Обе сохранены |
| Cross-method, priority differs | Priority wins (current_method beats completeness on tie) |

---

## Что DedupReport отдаёт

```json
{
  "total_in": 8,
  "total_out": 5,
  "clusters": 5,
  "same_class_drops": 3,
  "same_class_drops_by_key": {
    "missing_specification_item|спецификация эом|none|none": 2,
    "norm_obsolete|раздел нормативная база|none|none": 1
  },
  "critical_collapsed_count": 0,
  "methods_seen": ["current_method", "completeness"]
}
```

Этот блок попадает в `meta.dedup_report` финального `03_findings.json`. Frontend
может отображать "Свёрнуто N дубликатов" в audit summary.
