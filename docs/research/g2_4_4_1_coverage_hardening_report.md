# G2.4.4.1 — Coverage Hardening

## Краткие ответы

1. **Закрыт ли FALSE CHECKED?** Да. `CONNECTION` и `STRUCTURE` теперь получают
   `CHECKED` только при high-match самой сущности и всех её внешних соседей на
   соответствующей стороне. На обязательных counterexamples и полном real IOS
   invariant нарушений после hardening: **0**.
2. **Закрыта ли order/hash dependency?** Да для semantic coverage state. Несколько
   релевантных block pairs больше не перезаписывают друг друга; они дают
   fail-closed `CHECK_BLOCKED`. Перестановка pairs и изменение provenance/hash не
   меняют ни одного subject state. Trace digests при изменении provenance ожидаемо
   меняются.
3. **Что теперь означает `CHECKED` для CONNECTION?** Сущность полностью high-match,
   не ambiguous, и каждый node на другом конце каждой её внешней связи также
   high-match. Частичного `CHECKED` нет.
4. **Что теперь означает `CHECKED` для STRUCTURE?** Comparator имел надёжную identity
   для самой сущности и всех внешних nodes, от которых зависит её локальная
   структура; quality gate и scope не blocked.
5. **Что теперь означает `QUANTITY CHECKED`?** Только наблюдение доказанного repeated
   group могло бы его разрешить. Текущий subject contract содержит отдельные
   entities, а не repeated-group subject, поэтому для всех текущих individual
   entities результат — `NOT_APPLICABLE`, а `QUANTITY CHECKED = 0`.
6. **Можно ли scope-level CHECKED ошибочно использовать как subject evidence?** Нет.
   Scope processing вынесен в технический `scope_processing` со states
   `SCOPE_*`; semantic `coverage(...)` требует непустой `subject_id`.
7. **Стабильны ли entity IDs при перестановке?** Да. После однократного перехода на
   `graph-entities.v2` ID зависит от стабильного block/page/profile/entity context и
   member node ids, но не от whole-graph digest, graph array index или edge order.
   Graph digest остаётся в source/content signature.
8. **Есть ли P0?** Известных P0 в coverage semantics после тестов нет.
9. **Есть ли P1?** Известных P1 в coverage semantics нет. Production grouping для
   multi-page cases намеренно остаётся консервативным: не доказанная общая группа
   не объединяется и fail-closed становится unresolved.
10. **Можно ли переходить к G2.4.5?** Да. Coverage safety blockers закрыты. G2.4.5
    должен использовать только subject query и подавать полный набор block pairs в
    deterministic grouping producer.

## Что изменено

### Neighbour-aware coverage

`graph-entities.v2` добавляет к каждой сущности детерминированный список
`external_connections`:

- `edge_id`, `edge_type`;
- направление `INCOMING | OUTGOING`;
- `neighbour_node_id` на внешнем конце связи.

Для `TYPE` достаточна надёжная identity самой сущности. Для `CONNECTION` и
`STRUCTURE` дополнительно требуется, чтобы множество всех внешних neighbours было
подмножеством comparator high matches и не пересекалось с ambiguous ids. Иначе:

`NOT_CHECKED / NEIGHBOUR_IDENTITY_UNRESOLVED`.

Это та же граница наблюдаемости, которую использует comparator: связь нельзя
проверить без identity обоих концов. Comparator conclusions, matching policy,
Stage 5.3, GraphicChangeLedger и MODE_1 diff не менялись.

### Несколько block pairs

Удалён `pair_by_block = {...}` с поведением last-wins. Релевантность теперь
определяется одновременно по side, `block_id` и `page_index`. Все подходящие пары
собираются до принятия решения:

- одна MODE_2 pair — обычная evidence evaluation;
- только MODE_1 — `NOT_APPLICABLE`;
- две или более пары, если хотя бы одна MODE_2, —
  `CHECK_BLOCKED / MULTIPLE_RELEVANT_BLOCK_PAIRS`;
- blocked scope — `CHECK_BLOCKED` для каждого наблюдаемого subject внутри него.

Таким образом pair order, `block_pair_ref` и SHA больше не выбирают outcome.

Permutation test строит два artifacts из одного semantic input, переставляет
pairs и меняет provenance salts. Полный multiset
`(subject kind, subject id, dimension, side, state)` совпадает.

### Quantity и scope processing

Policy обновлена до `graphic-coverage-policy-v2`:

- entity-observable: `STRUCTURE`, `CONNECTION`, `TYPE`;
- `QUANTITY` существует в возможностях MODE_2 comparator только как
  repeated-group observation;
- individual `GRAPH_ENTITY` и `TEXT_ENTITY` не являются такой группой и получают
  `NOT_APPLICABLE`.

`graphic-coverage.v2` больше не содержит semantic records с subject kind `SCOPE`.
Запуск/блокировка route сохраняются отдельно в `scope_processing`. Guard в
`coverage(...)` отклоняет `subject_id=None`, поэтому будущий Synthesizer не сможет
принять факт запуска MODE_2 за доказательство отсутствия subject change.

### Graphic scope grouping

Аудит исходного дерева подтвердил: до этого единственным фактическим producer был
CLI `run_g2_4_4_scope_side_coverage.py`, вручную создававший ровно `1 group × 1
pair`; production application grouping отсутствовал. Поэтому предыдущий real IOS
не доказывал ambiguity behavior.

Добавлен минимальный `produce_graphic_scope_groups(...)`. Его contract:

- вход — полный плоский набор готовых Ledger/comparison pairs;
- grouping key — явная пара canonical LEFT/RIGHT page indexes;
- несколько blocks на одной паре страниц образуют child pairs одной группы;
- порядок входа, geometry, filename и fuzzy matching не используются;
- разные page pairs не объединяются без доказательства; multi-page ambiguity
  остаётся fail-closed.

CLI теперь использует этот producer и сохраняет рядом исходный
`comparison_result.json`. Real IOS по-прежнему содержит только `1 × 1`, поэтому в
этом отчёте ambiguity на real data не заявляется; она проверена synthetic tests.

### Page identity, stable IDs и stale

- Единственная функция TEXT-конверсии находится в `page_identity.py`:
  `text_pdf_page_1based_to_canonical_index`. Scope join и side context используют
  её; matrix `1↔0`, `2↔1`, `3↔2` зелёная.
- `graph-entities.v2`/`system-graph-entity-adapter-v2` исключают whole-graph digest,
  graph index и edge list из entity identity. На переставленных real IOS nodes и
  edges стабильны все **108/108** LEFT+RIGHT entity ids и все semantic coverage
  states; source signatures при этом меняются, как и должны.
- `saved_coverage_bundle_is_stale(...)` сверяет сохранённые coverage, TEXT entities,
  side graph entities, side links и scope join без несохранённых raw graphs или
  raw comparison input. Для перегенерированных IOS и AR: `stale=False`; подмена
  сохранённой dependency даёт `stale=True`.

## Real IOS/ГРЩ: до и после

Таблица считает только semantic subject records; старые 40 scope records вынесены
из сравнения, потому что в v2 они являются техническим `scope_processing`.

| Dimension | До CHECKED | До NOT_CHECKED | До CHECK_BLOCKED | До N/A | После CHECKED | После NOT_CHECKED | После CHECK_BLOCKED | После N/A |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| CONNECTION | 28 | 329 | 0 | 0 | 24 | 333 | 0 | 0 |
| STRUCTURE | 28 | 329 | 0 | 0 | 24 | 333 | 0 | 0 |
| TYPE | 28 | 329 | 0 | 0 | 28 | 329 | 0 | 0 |
| QUANTITY | 28 | 329 | 0 | 0 | 0 | 0 | 0 | 357 |

Полный manifest после hardening: 2856 semantic records — `CHECKED=76`,
`NOT_CHECKED=995`, `CHECK_BLOCKED=0`, `NOT_APPLICABLE=1785`. Нулевой real
`CHECK_BLOCKED` означает, что единственная реальная MODE_2 pair прошла quality gate;
blocked propagation проверена отдельными negative tests.

### SECTION_1 / SECTION_2

Для каждой из двух секций и каждой стороны результат одинаков:

| Subject | Side | CONNECTION | STRUCTURE | TYPE | QUANTITY |
|---|---|---|---|---|---|
| SECTION_1 | LEFT/RIGHT | NOT_CHECKED | NOT_CHECKED | CHECKED | NOT_APPLICABLE |
| SECTION_2 | LEFT/RIGHT | NOT_CHECKED | NOT_CHECKED | CHECKED | NOT_APPLICABLE |

У всех восьми SECTION connection/structure records reason:
`NEIGHBOUR_IDENTITY_UNRESOLVED`. До hardening эти восемь records были false
`CHECKED`; после — 0. Дополнительный invariant прошёл по всем оставшимся real
`CHECKED` graph records: каждый member и каждый external neighbour входит в
high-match set, violations = 0.

## Real AR без SYSTEM_GRAPH

AR после hardening содержит 1632 semantic TEXT subject records:

| Dimension | CHECKED | NOT_CHECKED | CHECK_BLOCKED | N/A |
|---|---:|---:|---:|---:|
| CONNECTION | 0 | 204 | 0 | 0 |
| STRUCTURE | 0 | 204 | 0 | 0 |
| TYPE | 0 | 204 | 0 | 0 |
| QUANTITY | 0 | 0 | 0 | 204 |

Итоговое `CHECKED=0` сохранено. Восемь scope groups имеют технический
`SCOPE_NOT_PROCESSED` для наблюдаемых dimensions, а не semantic scope evidence.

## Negative tests

Зафиксированы все обязательные cases:

1. matched entity + unresolved neighbour → CONNECTION `NOT_CHECKED`;
2. matched SECTION + ambiguous outgoing nodes → STRUCTURE `NOT_CHECKED`;
3. один `block_id` в двух good pairs → `CHECK_BLOCKED`, порядок/hash не влияют;
4. good + blocked pair → blocked propagation, last-wins отсутствует;
5. MODE_1 + MODE_2 с одним block → order-independent `CHECK_BLOCKED`;
6. individual subject QUANTITY → `NOT_APPLICABLE`;
7. blocked scope не выдаёт ни одного observable subject `CHECKED`;
8. scope-only semantic query отклоняется guard;
9. page matrix `1↔0`, `2↔1`, `3↔2`;
10. node/edge permutation сохраняет entity ids и semantic coverage.

Дополнительно проверены deterministic grouping producer, saved-bundle stale и
полный real IOS no-false-checked invariant.

## Артефакты и тесты

Перегенерированы:

- `experiments/g2_4_4_scope_side_coverage/ios/`;
- `experiments/g2_4_4_scope_side_coverage/ar/`.

Контракты текущего выхода:

- `graph-entities.v2`, `side-graph-entities.v2`;
- `graphic-coverage.v2`, `graphic-coverage-policy-v2`;
- единая page convention `pdf-page-1based-to-index-0based-v2`.

Проверки:

- focused producers/context + coverage/scope: **44 passed**;
- Stage Comparison + G1/G2 + Entity Bridge + producers/context + coverage/scope:
  **406 passed**;
- JSON parse/compile checks: passed;
- saved IOS/AR bundle stale checks: both `False`.

Push и release не выполнялись. G2.4.5 не реализовывался.
