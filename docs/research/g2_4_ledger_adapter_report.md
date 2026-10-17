# G2.4 — GraphicChangeLedger MODE_2 adapter

## 1. Как подключён MODE_2

Добавлен отдельный production-адаптер
`backend/app/services/stage_comparison/graphic_comparison/graphic_change_ledger_adapter.py`:

```text
SYSTEM_GRAPH comparison result
        +
left/right SYSTEM_GRAPH
        ↓
versioned MODE_2 adapter
        ↓
GraphicChangeLedger v2
```

Адаптер принимает уже готовые решения comparator и не запускает сопоставление,
не классифицирует различия и не открывает PDF. Перед преобразованием он повторно
проверяет контракт comparison result, оба SYSTEM_GRAPH и соответствие пары
графов ссылкам результата. Некорректный или несогласованный вход отклоняется.

Comparator, matcher, comparison policy и G1 MODE_1 extraction/diff не менялись.

## 2. Изменения Ledger schema

Старый контракт `graphic-change-ledger.v1` и константа `SCHEMA_VERSION` сохранены
для существующего MODE_1 producer. Для структурного результата добавлена отдельная
schema `graphic-change-ledger.v2` и dual-version runtime validator.

V2 требует:

- `route: MODE_2_REQUIRED` и `mode: MODE_2`;
- один исходный SYSTEM_GRAPH с каждой стороны;
- только структурные change types;
- `mode`, `summary`, `raw_confidence`, `mapped_confidence` и `structural` у каждого
  изменения;
- непустые graph evidence и address hints для каждой задействованной стороны;
- уникальные `change_id` и отсутствие дублирующих/конфликтующих структурных
  утверждений.

Поддержаны типы:

- `SYSTEM_BACKBONE_CHANGED`;
- `FUNCTIONAL_GROUP_CHANGED`;
- `NODE_ADDED`;
- `NODE_REMOVED`;
- `NODE_TYPE_CHANGED`;
- `CONNECTION_CHANGED`;
- `GROUP_COUNT_CHANGED`;
- `DETAIL_LEVEL_INCREASED`;
- `UNCERTAIN_STRUCTURAL_CHANGE`.

Старые MODE_1 типы остаются в v1 без принудительной миграции или перезаписи
сохранённых ledgers. `schema_path()` без аргумента по-прежнему возвращает v1;
для v2 используется явный version argument.

## 3. Mapping

Каждый comparator change преобразуется один-к-одному. `change_id`, `type`,
`summary`, `subject`, node ids и relation сохраняются. Adapter не создаёт новый
change и не отбрасывает существующий.

| Comparator change | Ledger structural level | Дополнительный контракт |
|---|---|---|
| `SYSTEM_BACKBONE_CHANGED` | `SYSTEM` | relation из comparator reason |
| `FUNCTIONAL_GROUP_CHANGED` | `GROUP` | relation из comparator reason |
| `GROUP_COUNT_CHANGED` | `GROUP` | разные integer `left_count/right_count` |
| `NODE_ADDED/REMOVED/TYPE_CHANGED` | `NODE` | строгая допустимость left/right nodes |
| `CONNECTION_CHANGED` | `EDGE` | как минимум одна edge reference |
| `DETAIL_LEVEL_INCREASED` | `SYSTEM/GROUP/NODE` по source level | `equivalence: representation_expansion` |
| `UNCERTAIN_STRUCTURAL_CHANGE` | по source level | неопределённость не повышается до change |

Исходный comparator level `A/B/C` отдельно сохраняется как `source_level`.

Числовая уверенность преобразуется только через
`system-graph-ledger-confidence-v1`:

- `HIGH`: `raw_confidence >= 0.85`;
- `MEDIUM`: `0.60 <= raw_confidence < 0.85`;
- `LOW`: `raw_confidence < 0.60`.

В change одновременно остаются numeric `raw_confidence`, canonical Ledger
`confidence` и явный `mapped_confidence`; validator проверяет их согласованность
с versioned policy.

## 4. Provenance и evidence

Comparator evidence используется только как трассируемая ссылка на node/edge ids
и как relation/reason. Адреса, bbox, source tokens и confidence заново берутся из
переданных SYSTEM_GRAPH:

- `source_graph`: side, schema, profile, block и page;
- `node_ids` и `edge_ids`;
- агрегированные `source_tokens`;
- минимальная confidence задействованных graph items;
- исходный `graph_provenance`;
- полное grounding каждого node/edge с его bbox и extraction evidence.

`left_region/right_region`, `comparison_scope` и `address_hints` строятся из
`SYSTEM_GRAPH.block` и bbox задействованных graph items. Значения block/page/bbox,
source tokens и confidence из comparator grounding для адресации не используются.
Проверка отдельно фиксирует это подменой этих полей в comparator evidence.

## 5. Backward compatibility и fail-closed validation

V1 validator сохраняет прежние MODE_1 правила. G1 router продолжает выпускать
`graphic-change-ledger.v1`; его extraction, registration, diff и confidence logic
не затронуты.

V2 validator отклоняет:

- неизвестные mode/type/поля;
- пустой evidence или address mapping;
- numeric confidence вне `[0, 1]`, неизвестный enum или policy mismatch;
- неизвестные graph references;
- duplicate `change_id` или дублирующее structural claim;
- structural level/payload, не соответствующий change type;
- конфликт `NODE_ADDED` с type/detail claim на том же right node;
- конфликт `NODE_REMOVED` с type/detail claim на том же left node.

Version bump реализован через новый v2 writer и dual reader. In-place migration
для старых ledgers не требуется: v1 остаётся поддерживаемым форматом.

## 6. Реальный GRSh

Проверен текущий G2.3.1 artifact
`experiments/g2_system_graph_comparator/comparison_result.json` вместе с
production SYSTEM_GRAPH из `experiments/g2_dense_sectioned_board`.

Ledger v2 содержит:

| Результат | Количество | Confidence |
|---|---:|---|
| `BACKBONE_PRESERVED` в structural diagnostics | 1 | source status |
| `DETAIL_LEVEL_INCREASED` | 2 | `0.94 → HIGH` |
| `GROUP_COUNT_CHANGED` (`30 → 27`) | 1 | `0.867 → HIGH` |
| `NODE_TYPE_CHANGED` (`QF3 → QS1`) | 1 | `0.92 → HIGH` |
| `UNCERTAIN_STRUCTURAL_CHANGE` | 2 | `0.35/0.49 → LOW` |
| `NODE_ADDED` | 0 | — |
| `NODE_REMOVED` | 0 | — |

Обе детализации имеют
`structural.equivalence: representation_expansion`. Для type change реальные
graph source tokens содержат `QF3` слева и `QS1` справа. Для group count relation
сохраняет `left_count: 30` и `right_count: 27`.

## 7. Проверки и готовность к UI

Выполнены:

- G2.4 adapter/contract/negative/real-GRSh: `15 passed`;
- полный Stage Comparison suite, включая неизменённые 300 прежних тестов:
  `315 passed`;
- G2.3.1 comparator suite: `16 passed`;
- dense sectioned board profile: `9 passed`;
- `git diff --check`: без ошибок.

К отдельному UI этапу переходить можно: MODE_2 имеет versioned, валидируемый и
трассируемый Ledger payload. В рамках G2.4 UI, persistence endpoint и автоматический
вызов адаптера из Stage Comparison намеренно не реализованы.
