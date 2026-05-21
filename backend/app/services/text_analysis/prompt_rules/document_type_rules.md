# Document type rules — completeness lens prompt block

**Назначение:** запретить требование тех артефактов, которые по
определению document_type не должны быть в текущем документе.

Четыре возможных document_type (см. `document_type_detector.py`):

- `full_rd` — полная РД-документация дисциплины.
- `audit_comparison` — пофрагментное сравнение двух разделов.
- `tz_vs_rd` — ТЗ-требования против РД-решений.
- `specification_only` — спецификация / ведомость / отдельная выгрузка.

## Правила

### document_type = `audit_comparison`

- **НЕ требуй** mandatory разделы РД (пояснительная записка, расчёты,
  спецификации, схемы, кабельные журналы).
- Допустимы missing findings ТОЛЬКО на формальные атрибуты сравнительного
  отчёта: штамп, шифр сравниваемого документа, стадия, дата, сводная
  ведомость замечаний с статусом.
- Все дисциплинарные missing-категории → **drop**.

### document_type = `tz_vs_rd`

- Mandatory: каждое требование ТЗ → ссылка на пункт РД; явные отклонения
  оформлены согласованием.
- **НЕ требуй** mandatory разделы из дисциплинарных чек-листов.
- Допустим cross-mapping ТЗ↔РД.

### document_type = `specification_only`

- Mandatory ТОЛЬКО спецификационные атрибуты:
  позиция, наименование, тип, количество, единицы, ключевые параметры
  дисциплины (сечение/марка кабеля, P_уст, Q, H, ...).
- **НЕ требуй** ПЗ, расчётов, схем, координации, аксонометрий,
  планов этажей.
- Если item имеет `applicable_document_types = [full_rd]` (без
  spec_only) → **drop**.

### document_type = `full_rd`

- Применять дисциплинарный чек-лист в полном объёме,
  С УЧЁТОМ stage gate и object_signal gates.

## Запреты на cross-mismatch

- «Отсутствует кабельный журнал» в `audit_comparison` → drop.
- «Отсутствует расчёт инсоляции» в `specification_only` → drop.
- «Отсутствует ПЗ» в `specification_only` → drop.
- «Отсутствует электробаланс» в `specification_only` → drop.

## Применимость

Этот блок применяется ВСЕГДА, до всех остальных gate'ов.

## Source

`experiments/md_analysis_comparison/normative_checklist_research/final_report.md`,
вопрос 7 + 8; `recommendations/prompt_rules_update.md` §2.
