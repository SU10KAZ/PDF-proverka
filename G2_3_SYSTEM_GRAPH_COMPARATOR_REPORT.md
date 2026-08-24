# G2.3 — SYSTEM_GRAPH Comparator

## Краткие ответы

1. **Получилось ли сравнивать без координат?** Да. Matching использует
   `canonical_identity`, функциональную роль, labels, relations и устойчивые
   attributes. `bbox` не участвует в identity: его вес равен `0.0`; отдельный
   negative test меняет все bbox и получает `NO_CHANGE`.
2. **Какие изменения ГРЩ найдены?** Секционный аппарат изменил функциональный
   подтип `CIRCUIT_BREAKER → SWITCH_DISCONNECTOR`; число отходящих аппаратов
   изменилось `30 → 27`. Backbone остался сохранён.
3. **Что признано детализацией?** Оба пути источника
   `UPSTREAM_TP_CONNECTION → TRANSFORMER_EXPLICIT` (`ТП1/ТП2 → Т1/Т2`) дали
   `DETAIL_LEVEL_INCREASED`, а не появление новых источников или аппаратов.
4. **Что признано изменением?** Подтверждённая смена типа секционного аппарата
   дала `NODE_TYPE_CHANGED`; изменение размера повторяющейся группы отходящих —
   один `GROUP_COUNT_CHANGED`.
5. **Что осталось uncertain?** Различие числа распознанных резервов имеет
   confidence `0.35`; индивидуальное соответствие части отходящих ветвей —
   `0.49`. Оба результата оставлены `UNCERTAIN_STRUCTURAL_CHANGE`.
6. **Есть ли ложные удаления/добавления?** Нет: `NODE_REMOVED=0`,
   `NODE_ADDED=0`. Перестановка колонок и слабые identities не превращаются в
   массовый removed+added.
7. **Можно ли переходить к GraphicChangeLedger?** Да. Каждый change уже имеет
   стабильный `change_id`, минимальный `type`, `level`, `subject`, `summary`,
   числовой `confidence`, `left_nodes`, `right_nodes` и grounded `evidence`.
   Интеграция с ledger в G2.3 намеренно не выполнялась.

## Реализация

- `graph_identity_matcher.py` — отдельный слой identity matching. Приоритеты
  зафиксированы в результате; geometry не участвует в matching (`weight=0.0`) и
  остаётся только частью исходного evidence.
- `system_graph_comparator.py` — три уровня сравнения, detail pass,
  one-to-many representation и контракт результата
  `system-graph-comparison.v1`.
- Comparator принимает только два JSON-compatible `SYSTEM_GRAPH`; PDF, Vision,
  extractor и профиль в нём отсутствуют.
- Production-правила не содержат обозначений или block ids обязательной пары.

## Level A — system backbone

Сравниваются количество источников, вводов и секций, число путей
`SOURCE → INPUT_DEVICE → BUS_SECTION`, функциональные сигнатуры основных путей и
пары секций, связанные `SECTION_DEVICE`. Для обязательной пары результат:

`BACKBONE_PRESERVED`

Две секции, два ввода, два источника и одна межсекционная связь сохранены.

## Level B — functional groups

`METERING_GROUP`, `COMPENSATION_GROUP` и агрегированная `SERVICE_GROUP`
присутствуют в тех же функциональных секциях. Смена labels или количества
распознанных member tokens не считается сменой функции.

Резервные ветви распознаны слева с низким confidence, а справа явных резервов
нет. Из-за неполной identity coverage это различие не повышено до доказанного
`FUNCTIONAL_GROUP_CHANGED` и оставлено uncertain.

## Level C — individual nodes and connections

Высокоуверенное canonical matching секционной связи позволило сравнить
`attrs.type_candidate`: `QF3` и `QS1` — тот же функциональный tie, но разные типы
коммутационного аппарата. Это один `NODE_TYPE_CHANGED`.

Отходящие анализируются как повторяющаяся функциональная группа. Общий count
изменился с 30 до 27, поэтому создан один `GROUP_COUNT_CHANGED`. Частично
неразрешённые identities сохранены в evidence одного uncertain change; comparator
не утверждает индивидуальные удаления или добавления.

## Detail-level pass

После основного matching comparator обходит `FEEDS`-подграфы между matched
`SOURCE` и `BUS_SECTION`. Более явное представление источника или дополнительные
промежуточные узлы помечаются `DETAIL_LEVEL_INCREASED` и поглощаются detail match,
поэтому не попадают в `NODE_ADDED`.

## Evidence и provenance

Каждое изменение содержит ids и confidence участвующих nodes/edges, а также их
`source_tokens`. Результат явно фиксирует:

- `input_kind = ready_system_graph_json`;
- `bbox_identity = false`;
- `geometry_identity_weight = 0.0`;
- `manual_cases = false`;
- `pdf_opened = false`;
- отсутствие Stage Comparison и GraphicChangeLedger integration.

## Тесты

- Comparator/negative/real GRSh: `10 passed`.
- G2.2 profile/source-kind regressions: `23 passed`.
- Classic Vectograf/singleline/common evidence: `57 passed, 23 skipped`;
  skips — отсутствующие локальные PDF-корпусы, как в G2.2.
- Stage Comparison: `300 passed`.

Покрыты identical graph с другими bbox, renamed functional labels, detail
expansion, node added, node removed, changed edge, uncertain identity, реальный
ГРЩ и обязательный evidence contract.

## Артефакты

- `experiments/g2_system_graph_comparator/comparator.py`
- `experiments/g2_system_graph_comparator/comparison_result.json`
- `experiments/g2_system_graph_comparator/comparison_report.md`
- `experiments/g2_system_graph_comparator/tests/test_comparator.py`

UI, Stage Comparison integration, GraphicChangeLedger, Vision, новые профили и
extractor в рамках G2.3 не реализовывались.
