# Human Review Orchestrator

Human Review Orchestrator — детерминированный read model между атомарным
результатом сравнения и вопросами инженеру. Он не меняет FAST detection,
finding IDs, evidence или решения инженера. Его задача — сохранить полный
аудит и не превращать каждую техническую запись backend в отдельную кнопку.

## Контракт

`human_review_plan.json` содержит:

- `summary` — отдельно Stage-7 targets, actionable atoms, groups, standalone
  questions, metadata, requirements и missing evidence;
- `groups[]` — `HumanReviewGroup` с вопросом, допустимыми ответами, общей
  причиной, evidence и политикой materialization;
- `standalone_questions[]` — только вопросы с конкретным инженерным выбором;
- `informational[]`, `metadata_changes[]`, `text_requirement_changes[]` и
  `missing_evidence[]` — видимые записи без обязательной кнопки;
- `atomic_target_mapping[]` — ровно одна классификация каждого Stage-7 target
  и каждого report-only атома;
- `review_item_classification[]` — трассировка старой строки Preliminary Report
  в новую категорию.

Подтверждённый FAST finding имеет категорию `PROVEN_CHANGE`: он остаётся
доступен для финального Engineer Approval и per-atom override, но не считается
нерешённым вопросом. Human Review Orchestrator не выполняет автоматическое
approval.

## Классификация текста

Штамп и технические примечания разделяются не одним списком слов. Классификатор
использует source block, тип fragment и координаты области. Текст в title block
становится `DOCUMENT_METADATA_CHANGE`; императивный текст в note block —
кандидатом `TEXT_REQUIREMENT_CHANGE`.

`TEXT_REQUIREMENT_ADDED` разрешён только через bounded absence:

1. выбрана точная страница другой редакции;
2. у страницы есть полный, не усечённый searchable text layer;
3. recognition coverage достаточен;
4. нет exact и normalized match;
5. нет сильного semantic candidate.

Если не хватает coverage/page scope и нет конкретного кандидата, результат —
`MISSING_EVIDENCE` без кнопки решения. Если exact/normalized или сильный
semantic candidate всё же найден, отсутствие также не доказано, но появляется
содержательный standalone-вопрос `TEXT_REQUIREMENT_EQUIVALENCE`: инженер
выбирает, является ли кандидат тем же требованием. Короткие электрические
обозначения на схеме (например, подпись PE-шины) учитываются как кандидаты
только вместе со structured note-block context.

## Группы и materialization

Группа создаётся только для общей причины, где один ответ применим ко всем
members. Для несовпадающего словаря режимов один document-level вопрос хранит
все затронутые объекты и атомарные facets. Ответ материализуется в отдельную
atomic resolution для каждого target. Per-atom human override всегда имеет
приоритет.

Похожие независимые изменения не группируются. В частности, одинаковая
формулировка или соседство строк не объединяют кабели, мощности и токи, если
инженер может принять по ним разные решения.

## Provenance ownership

AI identity resolution получает `MATERIALIZED_FINDING` только если оно создало
finding, повысило точный REVIEW finding, удалило точный review target как
NO_CHANGE или разрешило Document Inconsistency. Повторно найденный FAST finding
записывает AI relation только в `supporting_resolution` и получает
`IDENTITY_CONFIRMED_NO_DECISION_EFFECT`; его producer и статус
`Найдено автоматически` не меняются.
