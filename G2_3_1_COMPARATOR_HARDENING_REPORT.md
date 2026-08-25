# G2.3.1 — SYSTEM_GRAPH Comparator Hardening

## Краткий результат

1. **Какие P0 исправлены?** Закрыты все четыре класса из аудита:
   order-dependent matching, canonical override через parent relation, ложные
   ADD/REMOVE при rename, небезопасное поглощение functional node detail pass и
   certain changes на invalid/low-quality graphs.
2. **Как изменился matcher?** Greedy matching заменён deterministic global
   assignment. Решение пары теперь имеет `HIGH_MATCH`, `MEDIUM_MATCH` или
   `LOW_MATCH`, а HIGH требует threshold, двухсторонний margin и отсутствие
   identity conflict. Только HIGH участвует в structural comparison.
3. **Как работает fail-close?** Перед сравнением проверяются contract validity,
   identity coverage, средняя node/edge confidence и evidence completeness. При
   блокировке возвращается только `UNCERTAIN_STRUCTURAL_CHANGE`; certain types
   запрещены и дополнительно контролируются result validator.
4. **Как защищён detail pass?** Detail требует сохранённых source/section/input
   boundaries, тех же boundary relations, разрешённого representation transition
   и отсутствия нового functional node. Небезопасный intermediate не consumed и
   остаётся для `NODE_ADDED`, `FUNCTIONAL_GROUP_CHANGED` или uncertainty.
5. **Какие тесты добавлены?** Добавлены permutation, duplicate branches,
   canonical terminal protection, renamed unique function, unsafe detail node,
   invalid graph и zero identity coverage scenarios.
6. **Изменился ли результат ГРЩ?** Семантика не изменилась: backbone preserved,
   два detail changes, `QF3 → QS1` type change, `30 → 27` group-count change,
   две uncertainty и ноль ADD/REMOVE. Match count стал 16 HIGH pairs вместо 50
   смешанных pairs; 46 left nodes честно отражены как ambiguous.
7. **Можно ли переходить к G2.4?** Да, можно начинать отдельный G2.4 adapter для
   текущей domain policy. Прямого подключения ещё нет: G2.4 должен расширить
   Ledger schema/mode/types и выполнить mapping confidence/evidence/regions.
   Полная междисциплинарная schema остаётся отдельной задачей.

## Изменённая архитектура

```text
SYSTEM_GRAPH LEFT/RIGHT
        ↓
comparison_quality precheck
        ↓
deterministic global matcher
        ↓
generic comparator orchestration
        + comparison_policy
        ↓
validated structural changes
```

Добавлен `system_graph_comparison_policy.py`. В нём находятся:

- node/edge roles текущего `dense_sectioned_board`;
- functional group и repeated group semantics;
- разрешённые source representation transitions;
- разрешённые representation-only intermediate nodes;
- matching, quality и certain-change thresholds.

`compare_system_graphs(left, right, comparison_policy=...)` и
`match_graph_nodes(left, right, comparison_policy=...)` теперь получают policy
явно. По умолчанию используется versioned
`dense-sectioned-board-comparison-v1`. Comparator/matcher core больше не содержит
literal checks для `BUS_SECTION`, `OUTGOING_DEVICE`, `FEEDS`,
`TRANSFORMER_EXPLICIT` и остальных ЭОМ vocabulary: они сосредоточены в policy.

Это минимальное разделение CORE/DOMAIN POLICY, разрешённое scope G2.3.1. Полный
generic `SYSTEM_GRAPH` refactor и новые disciplines не выполнялись.

## P0-1 — deterministic global matching

### Global assignment

Canonical и functional/composite stages используют прямоугольный Hungarian
assignment с dummy unmatched columns. Rows/columns сортируются по node id,
поэтому порядок nodes/edges во входных JSON не влияет на результат.

Canonical stage выполняется первым и глобально. Functional stage работает только
на оставшихся nodes. Это сохраняет заданный приоритет canonical identity и
исключает локальный greedy выбор «первого подходящего» кандидата.

### Match decision policy

Каждый выбранный candidate получает:

- score и evidence confidence;
- margin относительно второго кандидата слева;
- margin относительно второго кандидата справа;
- identity conflict flag;
- итоговое решение.

Правила default policy:

| Decision | Условие | Использование |
|---|---|---|
| `HIGH_MATCH` | confidence ≥ 0.68, оба margin ≥ 0.05, нет conflict | Structural comparison |
| `MEDIUM_MATCH` | confidence ≥ 0.38, но HIGH не доказан | Только ambiguity/uncertainty |
| `LOW_MATCH` | Ниже medium threshold | Не используется |

Result сохраняет `medium_matches`, `ambiguous`, `ambiguous_left_ids`,
`ambiguous_right_ids` и `relation_conflicts`. `NODE_TYPE_CHANGED` и connection
comparison используют только HIGH matches.

### Canonical identity не переопределяется

Parent-relation alignment больше не удаляет уже найденные matches. Он может
сопоставить только два всё ещё unmatched terminals и только при HIGH parent
match. Если у terminals есть непересекающиеся canonical identities, создаётся
relation conflict, а автоматическая перепривязка запрещена.

Audit-counterexample с двумя одинаковыми parents и terminals `LOAD#A/LOAD#B`
теперь сохраняет:

```text
LOAD#A → LOAD#A
LOAD#B → LOAD#B
NODE_TYPE_CHANGED = 0
```

Перестановка parents остаётся ambiguity, а не структурным изменением.

## P0-2 — functional rename

Добавлен unique functional identity fallback. Он применяется, когда с обеих
сторон ровно по одному объекту данной роли и совпадают:

- node type;
- functional role и section/parent group;
- relation signature;
- stable non-geometric attributes.

Label и canonical designation могут различаться. Fallback получает собственный
grounded signal `unique_functional_identity`; новый change type не создавался.
Это `NO_STRUCTURAL_CHANGE` на уровне comparator.

Проверенный counterexample:

```text
PUMP-A / P-1 → PUMP-001 / Насос
status = NO_CHANGE
NODE_REMOVED = 0
NODE_ADDED = 0
```

Если functional role не уникальна или margin недостаточен, пара не становится
HIGH и остаётся uncertainty.

## P0-3 — hardened detail pass

`DETAIL_LEVEL_INCREASED` теперь создаётся только при выполнении всех условий:

1. source pair уже является HIGH match;
2. source и final section boundary сопоставлены;
3. input boundary сопоставлена;
4. boundary path использует ожидаемую relation policy;
5. representation transition явно разрешён policy либо есть разрешённое
   representation-only expansion;
6. каждый новый intermediate node разрешён policy и не является functional
   anchor/group.

Default policy разрешает переход
`UPSTREAM_TP_CONNECTION → TRANSFORMER_EXPLICIT` и representation node
`SERVICE_GROUP(subclass=BUSWAY)`. Она не разрешает произвольный новый service,
protection или aggregate functional node.

Rejected candidate записывается в `matching.detail_rejections` с причинами:
boundary state, relation state и `unsafe_right_nodes`. Узлы rejected path не
попадают в `consumed_right`.

Audit-counterexample `PROTECTIVE-STAGE#1` внутри source path теперь даёт
`NODE_ADDED`, а `DETAIL_LEVEL_INCREASED` отсутствует.

## P0-4 — graph quality fail-close

До matcher/structural passes строится `comparison_quality`:

```json
{
  "left_graph_valid": true,
  "right_graph_valid": true,
  "left_identity_coverage": 0.867,
  "right_identity_coverage": 0.926,
  "matched_nodes": 16,
  "ambiguous_nodes": 46,
  "blocked_changes_reason": [],
  "certain_changes_allowed": true
}
```

Дополнительно сохраняются node/edge confidence statistics, evidence completeness
и полный public policy contract.

Default global gates:

- оба graph contracts valid;
- identity coverage каждой стороны ≥ 0.5;
- средняя node confidence каждой стороны ≥ 0.5;
- средняя edge confidence каждой стороны ≥ 0.5;
- evidence и source tokens присутствуют у каждого node/edge.

Если хотя бы один gate не пройден:

- matcher не запускается на contract-invalid input;
- обычные backbone/functional/detail/node/connection passes не публикуют
  conclusions;
- result содержит `UNCERTAIN_BACKBONE`, `FUNCTIONS_UNCERTAIN` и единственный
  grounded `UNCERTAIN_STRUCTURAL_CHANGE` quality gate;
- `certain_changes_allowed=false`;
- result validator отклонит любой non-uncertain change при непустом
  `blocked_changes_reason`.

На valid graphs остаются локальные gates: low-confidence functional/reserve/group
count differences превращаются в `UNCERTAIN_STRUCTURAL_CHANGE`, а не certain
type.

Проверено:

```text
invalid graph       → UNCERTAIN, certain changes = 0
identity coverage 0 → UNCERTAIN, certain changes = 0
```

## Новые regression tests

Suite расширен с 10 до 16 тестов:

1. nodes и edges в другом array order → `NO_CHANGE`;
2. duplicate parent branches не перекрещивают canonical terminals;
3. duplicate branches не создают `NODE_TYPE_CHANGED`;
4. renamed unique function не создаёт ADD/REMOVE;
5. новый functional node в source path не становится detail;
6. invalid graph даёт только uncertainty;
7. identity coverage `0.0` даёт только uncertainty и заполненный quality report.

Один тест одновременно закрывает duplicate matching и canonical terminal
protection, поэтому файловый suite содержит шесть новых test functions и семь
обязательных assertions/scenarios.

Существующие tests bbox independence, renamed functional label, allowed detail,
node/edge changes, weak identity и реальный ГРЩ сохранены.

## Реальный ГРЩ после hardening

Обязательная пара G2.2 даёт:

```text
status                    CHANGED
backbone                  BACKBONE_PRESERVED
functional groups         FUNCTIONS_UNCERTAIN
HIGH matched pairs        16
ambiguous left/right      46 / 44
quality blocked reasons   []
result validation         valid
```

Changes:

| Level | Type | Count | Результат |
|---|---|---:|---|
| A | `DETAIL_LEVEL_INCREASED` | 2 | ТП1/ТП2 → Т1/Т2 |
| B | `UNCERTAIN_STRUCTURAL_CHANGE` | 1 | reserve 2 → 0, confidence 0.35 |
| C | `GROUP_COUNT_CHANGED` | 1 | repeated group 30 → 27 |
| C | `NODE_TYPE_CHANGED` | 1 | QF3 breaker → QS1 disconnector |
| C | `UNCERTAIN_STRUCTURAL_CHANGE` | 1 | unresolved repeated correspondence |

`METERING_GROUP`, `COMPENSATION_GROUP`, `SERVICE_GROUP` preserved. Ложных
`NODE_ADDED/NODE_REMOVED` нет. Changes по типам и смыслу полностью совпадают с
G2.3; изменился только уровень строгости matching diagnostics.

Обновлены воспроизводимые artifacts:

- `experiments/g2_system_graph_comparator/comparison_result.json`;
- `experiments/g2_system_graph_comparator/comparison_report.md`.

## Проверки

- G2.3/G2.3.1 comparator suite: `16 passed`.
- G2.2 dense profile, grounding и source-kind: `22 passed`.
- Classic Vectograf, evidence и singleline: `95 passed, 23 skipped`.
- Stage Comparison: `300 passed`.
- Python compilation новых/изменённых модулей: passed.
- JSON result validation: valid.

Skips classic suite связаны с отсутствующими локальными PDF fixtures и не
появились из-за hardening.

## Границы и готовность к G2.4

Не выполнялись:

- GraphicChangeLedger/Stage Comparison integration;
- UI, Vision, новые extractor/profile/discipline;
- изменение G2.1, G2.2, classic Vectograf или Graphic G1 Mode 1;
- полный generic SYSTEM_GRAPH schema refactor.

G2.3.1 закрывает обнаруженные P0 и готовит versioned policy и quality contract
для следующего адаптера. Переход к разработке G2.4 допустим. При этом G2.4 обязан
явно реализовать несовместимости с действующим Ledger: `MODE_2`, structural
change types, numeric confidence mapping, evidence array, regions/address hints,
per-change provenance и structural payload.
