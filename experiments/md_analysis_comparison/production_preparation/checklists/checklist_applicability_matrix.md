# Checklist Applicability Matrix — discipline × document_type

How to read this file: for each (discipline, document_type) cell, the matrix
says whether the discipline checklist is fully, partially, or not applicable.
The completeness lens runner uses this matrix as the second filter on top of
the per-item `applies=` gate.

If both a per-item `applies=` and this matrix conflict, the matrix wins
(`scope-limited` and `n/a` cells block all items regardless of `applies=`).

## Matrix

| Discipline | full_rd | audit_comparison | tz_vs_rd | specification_only |
|---|---|---|---|---|
| AR  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| KJ  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| KM  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| EOM | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| OV  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| VK  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| SS  | applicable | scope-limited (only items in comparison) | partial (parameter-level only) | partial (parameter-level only) |
| MULTI | applicable | applicable | applicable | partial (parameter-level only) |

Legend:

- **applicable** — все tier'ы checklist'а применимы (Mandatory / Recommended /
  Conditional). Anti-patterns всегда применяются.
- **partial (parameter-level only)** — применимы только Mandatory required
  parameters (per element / pump / cable / unit) и spec-level
  `incomplete_specification`. Sections / diagrams / calculations / coordination
  / testing — **не флагать**.
- **scope-limited (only items in comparison)** — применимы только пункты,
  которые явно входят в scope сравнения (упомянуты в исходном аудите ИЛИ в
  ответе проектировщика). Phantom-секции (unmentioned sections) — не флагать.
- **n/a** — checklist не применяется; lens должен skip-нуть дисциплину.

## Notes per discipline

### AR
- `audit_comparison`: scope-limited — типично audit_comparison для AR
  касается замечаний по эвакуации / пожарной безопасности / отделке.
  Не флагать "отсутствует пояснительная записка", если она вне scope.
- `tz_vs_rd`: partial — AR в ТЗ обычно фиксирует параметры (площади, ТЭП,
  материалы фасада). Sections как таковые в ТЗ не требуются.
- `specification_only`: partial — типично "Спецификация заполнения оконных
  и дверных проёмов" или "Ведомость отделки". Sections и расчёты не нужны.

### KJ
- `audit_comparison`: scope-limited — типично замечания по расчётам или
  армированию конкретных элементов.
- `tz_vs_rd`: partial — ТЗ задаёт классы бетона / арматуры, нагрузки.
  Полная расчётная схема не сравнивается.
- `specification_only`: partial — типично спецификация арматуры или
  ведомость закладных.

### KM
- `audit_comparison`: scope-limited — типично замечания по конкретным узлам
  / сварным швам / устойчивости.
- `tz_vs_rd`: partial — ТЗ задаёт классы стали и нагрузки. Узлы /
  спецификации проверяются на уровне параметров.
- `specification_only`: partial — типично ведомость отправочных марок или
  спецификация металлопроката.

### EOM
- `audit_comparison`: scope-limited — типично замечания по схеме, кабельному
  журналу или категории надёжности. Не флагать "отсутствует однолинейная
  схема", если она вне scope.
- `tz_vs_rd`: partial — ТЗ обычно фиксирует категорию надёжности, мощность
  ввода, систему заземления. Перечень оборудования сравнивается на уровне
  параметров.
- `specification_only`: partial — типично спецификация электрооборудования
  или кабельный журнал.

### OV
- `audit_comparison`: scope-limited — типично замечания по противодымной
  вентиляции или координации с ЭОМ.
- `tz_vs_rd`: partial — ТЗ обычно задаёт расчётные температуры, кратности,
  тип системы. Аксонометрии не сравниваются.
- `specification_only`: partial — типично спецификация вентоборудования
  или ведомость воздуховодов.

### VK
- `audit_comparison`: scope-limited — типично замечания по узлам ИТП,
  внутреннему пожарному водопроводу, рециркуляции ГВС.
- `tz_vs_rd`: partial — ТЗ задаёт расходы, тип счётчиков, схему
  водоснабжения. Точный гидравлический расчёт не сравнивается.
- `specification_only`: partial — типично спецификация сантехоборудования.

### SS
- `audit_comparison`: scope-limited — типично замечания по АПС, СОУЭ,
  координации с ОВ.
- `tz_vs_rd`: partial — ТЗ задаёт состав подсистем (СКС, АПС, СКУД),
  типы оборудования. Структурные схемы не сравниваются.
- `specification_only`: partial — типично спецификация СС-оборудования или
  кабельный журнал.

### MULTI
- `audit_comparison` и `tz_vs_rd`: applicable — это основной режим работы
  MULTI checklist'а. Sections "Cross-section consistency" применяются
  полностью; параметрические сверки тоже.
- `full_rd`: applicable, но MULTI обычно играет роль fallback при
  невозможности сузить дисциплину.
- `specification_only`: partial — применяются только пункты
  "Mandatory parameter consistency" и "Recommended → spec соответствует ТЗ".

## Applicable checklist categories per (discipline, document_type)

Для каждой ячейки матрицы перечислены категории, которые имеют смысл флагать.
Эти категории соответствуют section-header'ам в checklist-файлах.

### full_rd (все 8 дисциплин)
- Mandatory required sections / artifacts
- Mandatory required calculations
- Mandatory required specifications / schedules
- Mandatory required diagrams / schemes
- Mandatory required parameters (per element)
- Mandatory coordination requirements (cross-discipline)
- Mandatory testing / commissioning requirements
- Recommended items
- Conditional items (по применимости)

### audit_comparison
- (AR / KJ / KM / EOM / OV / VK / SS) — scope-limited:
  только пункты, явно упомянутые в исходном аудите.
- MULTI — applicable полностью:
  - Mandatory required sections / artifacts (для audit-comparison
    специфические пункты — статус замечаний)
  - Mandatory cross-section consistency
  - Mandatory parameter consistency

### tz_vs_rd
- (AR / KJ / KM / EOM / OV / VK / SS) — partial (parameter-level):
  - Mandatory required parameters (per element)
  - `incomplete_specification`
  - Сравнение значений → `tz_rd_mismatch_<parameter>`.
- MULTI — applicable полностью:
  - Mandatory required sections / artifacts (TZ-specific: лист изменений)
  - Mandatory parameter consistency (ключевая категория для tz_vs_rd)
  - Mandatory cross-section consistency

### specification_only
- (AR / KJ / KM / EOM / OV / VK / SS / MULTI) — partial:
  - Mandatory required specifications / schedules
  - Mandatory required parameters (per element)
  - `incomplete_specification`
  - **НЕ применять:** sections, diagrams, calculations, coordination,
    testing/commissioning. Эти tier'ы заведомо out-of-scope для
    specification-only документа.

## Edge cases

- **Unknown discipline:** lens fallback → MULTI; matrix row MULTI применяется.
- **Discipline detection low-confidence:** runner логирует
  `discipline_confidence < 0.7` и fallback'ает в MULTI.
- **Multiple document_types in one MD:** не поддерживается. Detection
  возвращает один `document_type`; при неоднозначности — MULTI.
- **Discipline = ITP / GP / POS / PS / TX / PT / AI** (есть в registry, но не
  в production checklist set): lens skip-ает completeness — для этих
  разделов checklist'а пока нет. Это запланировано в Phase 2.
