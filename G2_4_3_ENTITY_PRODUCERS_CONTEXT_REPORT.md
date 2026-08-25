# G2.4.3 — Entity Producers & Context

## Короткий ответ

1. **Стабильный TEXT entity artifact появился?** Да. Stage 5.3 теперь
   детерминированно выпускает отдельный `text_entities.json` по контракту
   `text-entities.v1`.
2. **Стабильный GRAPH entity artifact появился?** Да. Один или несколько готовых
   SYSTEM_GRAPH преобразуются в `graph_entities.json` по контракту
   `graph-entities.v1`.
3. **Удалось ли снять дубли graph nodes?** Частично и только доказуемые. На
   реальном правом графе 73 nodes стали 52 entities: 21 пара представлений
   объединена по явному `TERMINATES_AT`, одинаковой canonical identity и одной
   секции. По одному label ничего не объединяется.
4. **Что произошло с ВРУ-А?** Две пары `OUTGOING_DEVICE + LOAD` внутри BUS1 и
   BUS2 стали двумя graph entities. Межсекционное объединение запрещено, поэтому
   один TEXT `ВРУ-А` всё ещё имеет два кандидата и остаётся UNKNOWN.
5. **Что произошло с ЩР-6?** TEXT entity создан, но кандидата в проверенном
   SYSTEM_GRAPH нет. Он остаётся unresolved без синтетического UNKNOWN pair.
6. **Что произошло с QF1?** Stage 5.3 ИОС не выпускает QF1, поэтому в основном
   real artifact он NOT_EVALUATED. Отдельная проверка против реального графа:
   без context — UNKNOWN; с `section=BUS1` — единственный SAME_ENTITY/HIGH.
7. **Сколько HIGH / POSSIBLE / UNKNOWN?** ИОС: 10 candidate links → 1 HIGH,
   0 POSSIBLE, 9 UNKNOWN. АР без SYSTEM_GRAPH: 0 evaluated links, 27 TEXT
   entities остаются unresolved/not evaluated.
8. **Есть ли false entity links?** В проверенном наборе false HIGH = 0.
   Единственный real HIGH — буквальный уникальный `ВРУ-1 ↔ ВРУ1`; `ВРУ-А`,
   повторяющиеся ВРУ и локальные designations не были автоматически выбраны.
9. **Можно ли теперь делать Unified Change Synthesizer?** Можно проектировать
   следующий слой поверх стабильных artifacts и entity links. Автоматический
   synthesizer в G2.4.3 не реализован; UNKNOWN/POSSIBLE по-прежнему не дают права
   объединять изменения.

## Реализованный production flow

```text
Stage 5.3 high_level_project_changes.json
  -> text_entity_producer
  -> text_entities.json

SYSTEM_GRAPH[1..N]
  -> graph_entity_adapter
  -> graph_entities.json

text_entities.json + graph_entities.json
  -> существующие deterministic rules Entity Bridge
  -> entity_links.json (entity-bridge.v2)
```

Низкоуровневый G2.4.2 API `build_entity_links(text_list, graphic_list)` и его
`entity-bridge.v1` сохранены. Production API
`build_entity_links_from_artifacts(...)` принимает только проверенные
TEXT_ENTITIES/GRAPH_ENTITIES, сохраняет обе source signatures и использует тот
же normalizer и те же правила сопоставления.

CLI `scripts/run_g2_4_3_entity_producers.py` строит три artifacts только из
готовых JSON. PDF extraction, OCR, Vision, LLM, embeddings, fuzzy matching и
геометрическая identity не запускаются.

## TEXT entity producer

Producer читает только Stage 5.3 `details` и опциональный evidence-id keyed
index. Используются:

- явные structured `entities` / `designations` / `subjects`;
- строгие designation patterns для ВРУ/VRU, ЩР/SHR, ГРЩ/MSB и локальных
  QF/QS/FU/KM/KA/KT/SA/SB/HL/XT;
- явные номера помещений после слова «помещение»;
- уже существующие evidence IDs, fragment IDs, sheet groups, pages и change IDs;
- только явно приложенные `system`, `parent_group`, `section`, `room` context.

Слова `проект`, `этаж`, `лист`, `система` сами по себе отбрасываются. Произвольные
существительные из prose не превращаются в entities. Одинаковая canonical
identity с одинаковым context группируется; разные section/parent/system дают
разные entities.

`txt_ent_*` зависит от pair/scope, canonical identity, entity type/context,
source evidence IDs, Stage 5.3 schema/source signature и потому повторяется на
том же входе.

## GRAPH entity adapter

Для каждого node context выводится только из SYSTEM_GRAPH:

- `BELONGS_TO_SECTION` и `FEEDS` задают parent/section;
- `TERMINATES_AT` может передать доказанный section terminal representation;
- upstream `FEEDS` формирует `source_path`;
- graph envelope даёт discipline, block/page, profile и extractor versions.

Merge разрешён только для единственной пары `OUTGOING_DEVICE + LOAD`, когда:

- между nodes есть прямой `TERMINATES_AT`;
- canonical identity совпадает;
- обе стороны принадлежат одной и той же непустой section;
- у каждой стороны ровно одна такая representation pair;
- extraction conflicts отсутствуют.

Равенство label, bbox или близость сами по себе не участвуют. Одинаковые QF1 в
разных sections, одинаковые labels без relation и разные неразрешённые roles
остаются разными entities.

Каждый `gfx_ent_*` хранит node IDs, все incident edge IDs, source tokens,
block/page/bbox locators, graph digest, profile/extractor versions и aggregation
rule. Невалидный SYSTEM_GRAPH даёт ноль entities и явную запись в quality
report; HIGH из него невозможен.

## Source signatures и stale

TEXT source signature включает полный digest Stage 5.3 artifact, его собственную
source signature, schema/version, digest/version опционального evidence index,
producer version и общий normalizer version.

GRAPH source signature включает digest каждого SYSTEM_GRAPH, graph order/scope,
profile version, extractor version, adapter version и общий normalizer version.

`entity-bridge.v2` сохраняет обе signatures. Проверки `is_stale` и
`entity_links_are_stale` не позволяют молча использовать links после изменения
Stage 5.3, SYSTEM_GRAPH, profile/extractor, producer/adapter или normalizer.

## Реальные метрики

### ИОС + правый GRSh SYSTEM_GRAPH

| Этап | Метрика | Результат |
|---|---|---:|
| TEXT | source evidence | 96 |
| TEXT | строгие source candidates | 46 |
| TEXT | produced entities | 19 |
| TEXT | duplicate mentions collapsed | 27 |
| GRAPH | source nodes | 73 |
| GRAPH | produced entities | 52 |
| GRAPH | representation duplicates collapsed | 21 |
| BRIDGE | candidate links | 10 |
| BRIDGE | SAME_ENTITY / HIGH | 1 |
| BRIDGE | POSSIBLE_ENTITY | 0 |
| BRIDGE | UNKNOWN | 9 |
| BRIDGE | false HIGH | 0 |

`ВРУ-А`: 4 source nodes (`OUT+LOAD` в BUS1 и `OUT+LOAD` в BUS2) → 2 graph
entities → 2 UNKNOWN links из-за one-to-many cardinality.

`ЩР-6`: 1 TEXT entity → 0 graph candidates → unresolved.

`QF1`: отдельный probe с готовым TEXT evidence против того же real graph даёт
UNKNOWN без parent/section и HIGH при `section=BUS1`.

### АР без SYSTEM_GRAPH

| Этап | Метрика | Результат |
|---|---|---:|
| TEXT | source evidence | 261 |
| TEXT | строгие source candidates | 30 |
| TEXT | produced entities | 27 |
| TEXT | rooms | 26 |
| GRAPH | source nodes / entities | 0 / 0 |
| BRIDGE | evaluated links | 0 |
| BRIDGE | unresolved TEXT entities | 27 |

Дополнительная 27-я TEXT entity — буквальное обозначение `ВРУ1` в существующем
Stage 5.3 evidence; оно не создаёт graphic match. Никакая AR-графика не
имитировалась.

## Artifacts и код

- `text_entity_producer.py`, `text_entities.schema.json`;
- `graph_entity_adapter.py`, `graph_entities.schema.json`;
- `entity_bridge.py`, additive `entity_links_v2.schema.json`;
- runtime paths `text_entities.json`, `graph_entities.json`, `entity_links.json`;
- real reference outputs в `experiments/g2_4_3_entity_producers/{ios,ar}`;
- CLI `scripts/run_g2_4_3_entity_producers.py`;
- tests `test_stage_comparison_entity_producers_context.py`.

Stage 5.3 persistence атомарно добавляет `text_entities.json`. GET только читает
имеющийся artifact и не запускает producer. Если artifact отсутствует, старый
comparison продолжает открываться с `not_started`. Source Stage 5.3,
project summary, text final comparison, GraphicChangeLedger и SYSTEM_GRAPH не
изменяются.

## Проверка и ограничения

- producer/context + Entity Bridge + store focused set: 41 passed;
- Stage Comparison + G1/G2 ledger/profile/comparator regression: 388 passed;
- frontend Stage 5.3 regression: 10 passed;
- Python compileall: passed;
- `git diff --check`: clean.

Ограничения намеренные:

- label-only и cross-section graph dedup отсутствуют;
- локальный QF/QS без parent/system/section не получает HIGH;
- сущности из свободных noun phrases не извлекаются;
- отсутствие graph entity означает unresolved/not evaluated, а не различие;
- Unified Project Change, UI-объединение, новые profiles и AI/fuzzy matching не
  реализованы.
