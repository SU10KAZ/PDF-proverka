# Bad Findings — 5 violations + fixes

**Дата:** 2026-05-20

5 finding'ов, нарушающих правила
[stage01_production_prompt.md](../prompts/stage01_production_prompt.md).
Для каждого — конкретное нарушение + правило + fix.

---

## Bad #1 — No evidence_quote

```json
{
  "id": "T-003",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "missing_calculation",
  "affected_system": "Расчёт теплопотерь",
  "problem": "Расчёт теплопотерь не приведён",
  "evidence_quote": "",            // ← ПУСТО
  "confidence": 0.7
}
```

**Нарушение:** Правило 1 (Evidence rule) — `evidence_quote` ОБЯЗАН содержать
verbatim-подстроку из MD.

**Что делает corrector:** drops finding.

**Fix:** Если LLM не может процитировать MD — finding **не подаётся**. Это
KILL-LIST в prompt'е. Если действительно нет расчёта в MD, дать цитату
отсутствия:

```json
{
  ...
  "evidence_quote": "В разделе 'Расчёты' приведены только результаты — методика и исходные данные не показаны.",
  "problem": "Расчёт теплопотерь представлен без методики и исходных данных",
  ...
}
```

Цитата существующего фрагмента, обосновывающая отсутствие детального расчёта.

---

## Bad #2 — Выдуманный пункт нормы

```json
{
  "id": "T-007",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "incorrect_design",
  "affected_system": "Заземление контурного типа",
  "problem": "Заглубление контура < 0,7 м",
  "norm": "ПУЭ-7, п. 1.7.121",     // ← Этот пункт не существует
  "norm_quote": "Заглубление искусственного заземлителя должно быть не менее 0,7 м.",
  "norm_confidence": 0.95,         // ← Высокая, но это галлюцинация
  "confidence": 0.9
}
```

**Нарушение:** Правило 5 (Norm rule) — "Не выдумывай номера пунктов".
Реальный пункт ПУЭ-7 1.7.111 (заглубление < 0,5 м), но не 1.7.121.

**Что делает corrector:** Уровень 4 verification (см.
[docs/norms_verification.md](../../../docs/norms_verification.md))
поймает invalid quote → `paragraph_verified=false` → норма помечена.

**Fix:**
```json
{
  ...
  "norm": "ПУЭ-7 (раздел 1.7 — заземление)",
  "norm_quote": null,
  "norm_confidence": 0.0,
  ...
}
```

Если LLM не помнит точный пункт — оставить `null` / `0.0`. На этапе 04
верификации Python проверит через `norms_paragraphs.json` или WebSearch.

---

## Bad #3 — Severity inflation (РЕКОМ → КРИТ)

```json
{
  "id": "T-002",
  "severity": "КРИТИЧЕСКОЕ",          // ← Inflation
  "severity_reasoning": "Опечатка нужна для нормоконтроля",
  "problem_class": "typo",
  "affected_system": "Спецификация",
  "problem": "Опечатка: 'С2000-2' с латинской 'C'",
  "evidence_quote": "С2000-2 (24 шт.)",
  "confidence": 0.95
}
```

**Нарушение:** Правило 6.calibration — `КРИТИЧЕСКОЕ` означает "здание
непостроимо или прямая угроза жизни". Опечатка в обозначении — РЕКОМЕНДАТЕЛЬНОЕ.

**Что делает critic:** verdict = `wrong_severity` → downgrade.

**Fix:**
```json
{
  ...
  "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
  "severity_reasoning": null,
  ...
}
```

---

## Bad #4 — `КРИТИЧЕСКОЕ` без severity_reasoning

```json
{
  "id": "T-001",
  "severity": "КРИТИЧЕСКОЕ",
  "severity_reasoning": null,      // ← KILL-LIST violation
  "problem_class": "structural_failure",
  "affected_system": "Перекрытие",
  "problem": "Расчёт перекрытия некорректен",
  ...
}
```

**Нарушение:** KILL-LIST в prompt'е: "НЕ ставь КРИТИЧЕСКОЕ без
`severity_reasoning` ≤ 120 символов и конкретного дефекта".

**Что делает critic:** verdict = `weak_evidence` → или удалить или добавить
reasoning.

**Fix:**
```json
{
  ...
  "severity": "КРИТИЧЕСКОЕ",
  "severity_reasoning": "M_x=65 кН·м не учтён в расчёте на продавливание — фактическая нагрузка > 1,2×N_доп",
  ...
}
```

Если автор LLM не может объяснить почему именно КРИТ ≤ 120 chars — нужно либо
конкретизировать (предпочтительно), либо downgrade severity.

---

## Bad #5 — Out-of-scope finding на specification_only

`document_type = specification_only`. Spec'ификация остекления.

```json
{
  "id": "T-005",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "missing_section",
  "affected_system": "Пояснительная записка",
  "problem": "Отсутствует пояснительная записка раздела АР",
  "norm": "ГОСТ Р 21.1101-2013, п. 4.2",
  "evidence_quote": "[нет упоминания ПЗ во всём документе]",
  "confidence": 0.7
}
```

**Нарушение:**
- HARD RULE в prompt'е (Маршрутизация по document_type): "Если
  `document_type != full_rd`, никогда не пиши замечания вида: «отсутствует полный
  комплект РД», «не представлена пояснительная записка», ...".
- KILL-LIST явный.

**Что делает critic:** verdict = `out_of_scope` → drop.

**Fix:**
- Don't emit this finding at all.
- Если LLM очень хочет flag'нуть — пометить `is_beyond_gt_useful=true` И
  переформулировать в scope:
  ```json
  {
    "problem": "Поз. Б5: маркировка СПД (двухкамерный) противоречит формуле 4-16-4 (однокамерный)",
    "evidence_quote": "Б5: СПД, 4-16-4",
    "is_beyond_gt_useful": false
  }
  ```

Это уже легитимное spec-finding.

---

## Сводка по правилам

| Правило | Bad-ситуация | Что corrector делает |
|---|---|---|
| Evidence rule (1) | Пустой `evidence_quote` | drop finding |
| Class rule (2) | Нет `problem_class` / `affected_system` | drop finding (или backfill через category) |
| Dup rule (3) | Same `(problem_class, affected_system)` | collapse через class_dedup |
| Speculation rule (4) | "Уточнить", "проверить" без конкретики | drop finding |
| Norm rule (5) | Выдуманный пункт нормы | norm_verified=false → flag |
| Severity calibration (6) | wrong severity | critic verdict `wrong_severity` |
| Beyond-GT useful (7) | engineering advice без флага | critic verdict `missing_flag` |
| Cap (8) | > 14 findings | drop lowest-confidence |
| Confidence (9) | confidence < 0.6 | drop finding |
| Категории (10) | Неточное написание | normalise в одном из 5 значений |

---

## Что critic возвращает (verdicts)

[critic_corrector.md "5 проверок critic"](../../../docs/critic_corrector.md):

| Verdict | Описание |
|---|---|
| `pass` | OK |
| `no_evidence` | Пустой / отсутствует evidence_quote |
| `phantom_block` | block_id ссылается на несуществующий блок |
| `weak_evidence` | evidence-цитата не подтверждает finding |
| `page_mismatch` | sheet/page не сходится с document_graph |
| `contradicts_text` | finding противоречит MD-тексту |
| `wrong_severity` | severity inflation / deflation |
| `out_of_scope` | document_type не позволяет такой finding |
| `missing_norm` | norm указан, но не верифицирован |
| `speculative` | "Уточнить" / "проверить" без конкретики |
| `duplicate_of_canonical` | dедуп должен был свернуть |
| `missing_flag` | engineering advice без `is_beyond_gt_useful=true` |

12 verdicts — extended set из
[final_verdict.md "Extended critic verdicts"](../../algorithm_research/reports/final_verdict.md).

---

## Why this matters

A0 baseline на pre-Phase-1 кейсах:
- ~30% findings были "wrong_severity" (per [a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)).
- ~15% — "speculative" формулировки.
- ~10% — out-of-scope на non-full_rd документах.

Phase 1 prompt + critic + corrector adressируют эти 3 категории напрямую.
