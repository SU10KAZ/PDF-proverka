# Good Findings — 5 ideal-form examples

**Дата:** 2026-05-20

5 finding'ов, полностью соответствующих
[stage01_production_prompt.md](../prompts/stage01_production_prompt.md):
- `evidence_quote` — verbatim из MD.
- `problem_class` + `affected_system` — для dедупа.
- `severity_reasoning` ≤ 120 символов для КРИТ.
- Норма с реальной редакцией.
- `confidence` adequate.

---

## Finding 1 — Кабель не проходит по нагреву

```json
{
  "id": "T-001",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "cable_undersized",
  "affected_system": "Кабель ввода ВРУ-1",
  "severity_reasoning": "Iдоп 220 А < Iрасч 302 А — тепловой пробой при нагрузке",
  "problem": "Iдоп кабеля АВВГ 4×95 (220 А) меньше расчётного тока ввода (302 А)",
  "description": "Расчётный ток ввода 302 А (раздел 4). В спецификации (раздел 6) указан АВВГ 4×95, длительно допустимый ток которого по таблице 1.3.10 ПУЭ-7 — 220 А. Перегрузка ~37%.",
  "norm": "ПУЭ-7, п. 1.3.10; СП 256.1325800.2016, п. 7.4.3",
  "norm_quote": "Длительно допустимые токовые нагрузки на провода и кабели приведены в таблицах 1.3.4–1.3.31 настоящих Правил.",
  "norm_confidence": 0.9,
  "recommendation": "Заменить на АВВГ 4×185 (Iдоп ≥ 320 А) или ВВГнг 4×150 (Iдоп ≥ 305 А).",
  "risk": "Тепловой пробой изоляции, возгорание ВРУ.",
  "evidence_quote": "Кабель ввода: АВВГ 4×95, Iдоп 220 А. Расчётный ток 302 А.",
  "md_excerpt": "Раздел 4. Кабельные сети. Кабель ввода: АВВГ 4×95, Iдоп 220 А. Расчётный ток 302 А, коэффициент использования 0,9.",
  "source": "MD стр. 12 / Раздел 4",
  "discipline": "EOM",
  "cross_discipline_with": [],
  "confidence": 0.95,
  "related_block_ids": [],
  "is_beyond_gt_useful": false,
  "source_agents": ["current_method"]
}
```

**Почему ideal:**
- Прямое нарушение нормы (220 < 302 А) — `КРИТИЧЕСКОЕ` обоснованно.
- `severity_reasoning` ≤ 120 символов, конкретный риск.
- evidence_quote — verbatim из MD.
- Норма с пунктом + цитата.
- Конкретная recommendation.

---

## Finding 2 — Несоответствие между разделами (cross_01)

```json
{
  "id": "T-005",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "load_mismatch",
  "affected_system": "ЩВ-ОВ ввод (электрические нагрузки)",
  "severity_reasoning": "Расхождение 12,0 vs 18,5 кВт — ввод 32 А может не запитать систему",
  "problem": "Расхождение учтённой в ЭОМ и заявленной ОВ нагрузки: 12,0 кВт vs 18,5 кВт",
  "description": "В ЭОМ для ЩВ-ОВ учтено 12,0 кВт (раздел 4, таблица 4.2). В ОВ заявленный расход — 18,5 кВт (раздел 3, табл. 3.1). Расхождение 6,5 кВт — тепловые завесы П2, В2 (3 шт. × 4 кВт = 12 кВт) не учтены в ЭОМ.",
  "norm": "СП 256.1325800.2016, п. 7.4",
  "norm_quote": "Расчётный ток ВРУ принимается с учётом коэффициентов спроса и расчётных мощностей электроприёмников.",
  "norm_confidence": 0.85,
  "recommendation": "Привести нагрузку в ЭОМ к 18,5 кВт с учётом тепловых завес. Перепроверить автомат ввода.",
  "evidence_quote": "ЭОМ: 12,0 кВт; ОВ заявляет 18,5 кВт. Тепловые завесы: П2, В2 (4 кВт каждая, 3 шт.)",
  "md_excerpt": "Раздел 3.1. Расчёт нагрузок ОВ: 18,5 кВт. [...] Раздел 4. ЭОМ. Нагрузки ЩВ-ОВ: 12,0 кВт.",
  "discipline": "EOM",
  "cross_discipline_with": ["OV"],
  "interface_type": "electric_power",
  "discipline_pair": "EOM,OV",
  "confidence": 0.95,
  "is_beyond_gt_useful": false,
  "source_agents": ["current_method", "completeness"]
}
```

**Почему ideal:**
- Cross-discipline данные (`discipline_pair`, `interface_type`).
- Конкретные цифры в evidence.
- Recommendation указывает на проверку downstream effects.
- `source_agents` показывает, что обе линзы поймали.

---

## Finding 3 — Устаревший норматив без affecting значений (ЭКСПЛУАТАЦИОННОЕ)

```json
{
  "id": "T-008",
  "category": "Эксплуатационное",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem_class": "norm_obsolete_reference",
  "affected_system": "Раздел нормативная база",
  "problem": "Применена ссылка на отменённый СНиП 2.04.01-85*",
  "description": "В разделе нормативная база (стр. 4) указан СНиП 2.04.01-85* как актуальный. Документ отменён, заменён на СП 30.13330.2020. Значения расходов в расчёте корректны, но правовая основа РД невалидна.",
  "norm": "СП 30.13330.2020, ПП РФ №815 от 28.05.2022",
  "norm_quote": null,
  "norm_confidence": 0.7,
  "recommendation": "Заменить ссылку на актуальный СП 30.13330.2020 (ред. 2022).",
  "evidence_quote": "Расчёт водопотребления по СНиП 2.04.01-85*.",
  "md_excerpt": "Нормативная база: СНиП 2.04.01-85*, ГОСТ 8732-78.",
  "discipline": "VK",
  "confidence": 0.92,
  "is_beyond_gt_useful": false,
  "source_agents": ["current_method"]
}
```

**Почему ideal:**
- НЕ КРИТ (правильная калибровка — значения корректны, проблема legal).
- `norm_quote=null` потому что LLM не помнит точно.
- `norm_confidence=0.7` — реалистично.
- Recommendation конкретная.

---

## Finding 4 — Beyond-GT useful (engineering advice)

```json
{
  "id": "T-012",
  "category": "Рекомендательное",
  "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
  "problem_class": "breaker_curve_inappropriate",
  "affected_system": "Автомат ввода ЩВ-ОВ",
  "severity_reasoning": null,
  "problem": "Характеристика C автомата при прямом пуске двигателей с I_пуск/I_ном = 6,5–7,0",
  "description": "Автомат C-кривая стандартный, но для прямого пуска двигателей с высоким пусковым током (6,5–7,0 × I_ном) D-кривая предпочтительнее — снижает риск ложных срабатываний при старте.",
  "norm": null,
  "norm_quote": null,
  "norm_confidence": 0.0,
  "recommendation": "Рассмотреть замену на D-кривую для линий с прямым пуском АД.",
  "evidence_quote": "Автомат ввода: C32А. Подключены вентустановки прямого пуска.",
  "md_excerpt": "Защита ЩВ-ОВ: автомат C32А, 1Р+N. Электроприводы: прямой пуск.",
  "discipline": "EOM",
  "confidence": 0.85,
  "is_beyond_gt_useful": true,
  "source_agents": ["completeness"]
}
```

**Почему ideal:**
- `is_beyond_gt_useful=true` — engineer'у есть, что узнать, но норма не нарушена.
- `severity_reasoning=null` потому что РЕКОМ (не требуется для не-КРИТ).
- `norm=null`, `norm_confidence=0.0` потому что не привязано к норме (это
  engineering practice).
- Recommendation — "рассмотреть", не "обязан".

---

## Finding 5 — TZ vs RD (specification_only НЕ применим)

Для document_type=tz_vs_rd:

```json
{
  "id": "T-001",
  "category": "Критическое",
  "severity": "КРИТИЧЕСКОЕ",
  "problem_class": "tz_violation",
  "affected_system": "Система отопления",
  "severity_reasoning": "ТЗ п.4.2.1 требует двухтрубной — РД принимает однотрубную",
  "problem": "Принята однотрубная вертикальная система отопления — прямое нарушение ТЗ п.4.2.1",
  "description": "ТЗ п.4.2.1: 'Система отопления — двухтрубная горизонтальная с поквартирной разводкой'. РД (раздел 3.2): принята однотрубная вертикальная. Согласование отступления от ТЗ не приложено.",
  "norm": "ТЗ п.4.2.1 (внутренний документ)",
  "norm_quote": "Система отопления — двухтрубная горизонтальная с поквартирной разводкой.",
  "norm_confidence": 1.0,
  "recommendation": "Либо вернуться к двухтрубной системе, либо приложить согласованное отступление от ТЗ.",
  "evidence_quote": "ТЗ п.4.2.1: двухтрубная. РД п.3.2: однотрубная вертикальная.",
  "md_excerpt": "Сравнение: ТЗ — двухтрубная горизонтальная (п.4.2.1); РД — однотрубная вертикальная (раздел 3.2).",
  "discipline": "OV",
  "interface_type": "design_requirement",
  "discipline_pair": null,
  "confidence": 0.98,
  "is_beyond_gt_useful": false,
  "source_agents": ["current_method"]
}
```

**Почему ideal:**
- ТЗ как `norm` — корректно для tz_vs_rd.
- `norm_quote` точно цитирует ТЗ.
- severity КРИТ обоснован (прямое нарушение договора).
- Recommendation предлагает оба пути выхода (вернуться или согласовать).

---

## Сводка чек-листа для review

При engineer review canary outputs — проверять:

- [ ] `evidence_quote` — verbatim, не reword'нутый?
- [ ] `problem_class` + `affected_system` заполнены?
- [ ] Если КРИТ — есть `severity_reasoning` ≤ 120 chars?
- [ ] Норма + редакция явные?
- [ ] `norm_quote` — точная цитата ИЛИ `null`?
- [ ] Recommendation конкретная (не "уточнить" / "проверить")?
- [ ] Severity калибровка адекватна (см. severity_calibration_examples.md)?
- [ ] Если `is_beyond_gt_useful=true` — flag оправдан?

Если все 8 boxes ticked — finding good.
