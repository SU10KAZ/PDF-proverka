# Before / After: A0 vs A1-v2 — конкретные кейсы

**Дата:** 2026-05-20

Три кейса, на которых видно, что Phase 1 добавляет, удаляет и меняет в текущем
поведении. Источники: реальные A0/A1-v2 outputs из
[`experiments/.../algorithm_research/results/`](../../algorithm_research/results/)
+ FP-аудит ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)).

---

## Кейс 1 — `cross_01_eom_ov_loads` (audit_comparison)

Сравнение ЭОМ vs ОВ по нагрузкам ВРУ.

### A0 (current_method only) — 8 findings

```json
{
  "id": "T-001",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem": "Расхождение учтённой в ЭОМ и заявленной ОВ нагрузки",
  "description": "ЭОМ учёл 12,0 кВт по системе вентиляции, ОВ заявляет 18,5 кВт.",
  "evidence_quote": "ЭОМ: 12,0 кВт; ОВ: 18,5 кВт",
  "norm": "СП 256.1325800.2016, п. 7.4",
  "confidence": 0.92,
  "recommendation": "Привести нагрузку в ЭОМ к 18,5 кВт."
}
```

(всего 8 finding'ов, без полей `problem_class`, `affected_system`,
`severity_reasoning`, `is_beyond_gt_useful`)

**Проблема:** A0 не поймал findings про "не учтены тепловые завесы" и про
"пусковой режим автомата" (см. cross_01 GT). missed_critical = 2.

### A1-v2 (current_method + Sonnet completeness lens) — 13 findings

```json
{
  "id": "T-001",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "load_mismatch",
  "affected_system": "ЩВ-ОВ ввод",
  "severity_reasoning": "Расчётная нагрузка занижена 12,0 vs 18,5 кВт — переразмер ввода",
  "problem": "Расхождение учтённой в ЭОМ и заявленной ОВ нагрузки: 12,0 кВт vs 18,5 кВт",
  "description": "...",
  "evidence_quote": "ЭОМ: 12,0 кВт; ОВ: 18,5 кВт",
  "norm": "СП 256.1325800.2016, п. 7.4",
  "norm_quote": "Расчётный ток ВРУ принимается с учётом коэффициентов спроса...",
  "confidence": 0.95,
  "is_beyond_gt_useful": false,
  "source_agents": ["current_method", "completeness"],
  "is_canonical": true,
  "class_key": "load_mismatch|щв-ов ввод|electric_power|EOM,OV"
}
```

**Новые findings, не пойманные A0:**
```json
{
  "id": "T-005",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "thermal_curtain_load_missing",
  "affected_system": "ЩВ-ОВ ввод",
  "severity_reasoning": "Тепловые завесы 3×4 кВт = 12 кВт не учтены в расчёте",
  "problem": "В ЭОМ не учтены тепловые завесы П2, В2",
  "evidence_quote": "Тепловые завесы: П2 (4 кВт), В2 (4 кВт), 3 шт.",
  "is_beyond_gt_useful": false,
  "source_agents": ["completeness"]
}
```

```json
{
  "id": "T-008",
  "category": "Эксплуатационное",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "breaker_curve_inappropriate",
  "affected_system": "Автомат ввода ЩВ-ОВ",
  "severity_reasoning": "C-кривая на грани ложных срабатываний при I_пуск=6,5×I_ном",
  "problem": "Характеристика C для прямого пуска двигателей с I_пуск/I_ном = 6,5–7,0 — на границе ложных срабатываний",
  "is_beyond_gt_useful": true,
  "source_agents": ["completeness"]
}
```

**Сравнение метрик** ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)):

| Метрика | A0 | A1-v2 |
|---|---|---|
| total findings | 8 | 13 |
| matched_gt | 4 | 5 |
| missed_critical | 2 | 1 |
| FP (raw) | 4 | 6 |
| FP с учётом beyond_useful | 4 | 4 (2 finding'a помечены `is_beyond_gt_useful=true`) |

---

## Кейс 2 — `eom_03_low_voltage_selectivity` (full_rd)

Расчёт селективности автоматов ВРУ.

### A0 — 8 findings, добротные base findings + 0 phantoms

(не приводим полный JSON ради краткости; ключевое: A0 поймал основные дефекты
расчёта селективности.)

### A1-v2 — 15 findings; +7 от Sonnet completeness lens

Из этих 7:
- 1 правильно (duplicate of GT) — `evidence_quote` другая, но смысл тот же.
- 2 `is_beyond_gt_useful=true` (токи КЗ на промежуточных уровнях).
- 12 wrong_severity (FP-аудит классифицировал так).

Это и есть pattern из [FINAL_SUMMARY §4](../../algorithm_research/reports/FINAL_SUMMARY.md):
- A1-v2 не теряет critical (`missed_critical` не растёт).
- A1-v2 добавляет шум на full_rd (wrong_severity findings).

**Поэтому full_rd — НЕ default route для Phase 1**. См.
`integration_plan/phase1_integration.md` §"Opt-in routing".

---

## Кейс 3 — `ar_03_balcony_glazing` (specification_only)

Спецификация остекления балконов.

### A0 — 0 baselines на specification_only (новый кейс, A0 не запускался).

### A1-v2 — 22 findings

Сильное:
- ловит реальные дефекты спецификации (Б4 без профиля, Б5 СПД vs формула
  однокамерного).
- corrector корректно фильтрует «отсутствует пояснительная записка»
  (specification_only KILL-LIST).

Проблемное:
- 1/15 phantom finding от Opus current_method, который НЕ знает про
  document_type (см. [FINAL_SUMMARY §4.3](../../algorithm_research/reports/FINAL_SUMMARY.md)).
- 3 wrong_severity (`Холодное остекление Б3 — требует проверки по АГР` —
  это РЕКОМ, не ЭКСПЛ).

### Что меняется по сравнению с before

| Поле | A0 | A1-v2 |
|---|---|---|
| `problem_class` | absent | `vendor_marking_wrong`, `incomplete_specification`, ... |
| `affected_system` | absent | `Позиция Б5`, `Позиция Б4`, ... |
| `severity_reasoning` | absent | "Маркировка СПД противоречит формуле — фактически другой стеклопакет" |
| `is_beyond_gt_useful` | absent | true на 4 findings ([a1v2_fp_audit.md](../../algorithm_research/reports/a1v2_fp_audit.md)) |
| `source_agents` | absent | `["current_method"]` / `["completeness"]` / `["current_method", "completeness"]` |
| `meta.document_type` | absent | `"specification_only"` |
| `meta.completeness_applied` | absent | `true` |
| `meta.dedup_report.same_class_drops` | absent | 2 (пара findings про Б4 и Б5 свёрнуты) |

---

## Summary

Phase 1 добавляет:
1. Структурные поля (`problem_class`, `affected_system`, `class_key`) → enable
   dedup.
2. Калибровку severity (`severity_reasoning`) → reviewer может понять почему
   КРИТ.
3. Beyond-GT useful flag → engineer может фильтровать "только нарушения нормы".
4. Source agents → видит, какая лента нашла finding.
5. Document type metadata → видит scope.

Phase 1 НЕ удаляет existing findings.

Phase 1 на A1-v2 **уменьшает** missed_critical в 2 раза per-case
([FINAL_SUMMARY §4 "Critical recall"](../../algorithm_research/reports/FINAL_SUMMARY.md)).
