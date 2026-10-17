# G2.3 AUDIT — SYSTEM_GRAPH Comparator Review

## Короткие ответы

1. **Можно ли сейчас подключать G2.3 к GraphicChangeLedger?** Нет. Реальный
   сценарий ГРЩ работает, но независимые контрпримеры выявили ложные
   `NODE_TYPE_CHANGED`, ложную пару `NODE_REMOVED + NODE_ADDED`, fail-open на
   недостоверных графах и слишком широкую классификацию детализации. Кроме того,
   текущий Ledger не принимает контракт comparator напрямую.
2. **Есть ли P0?** Да. Найдены четыре воспроизводимых класса опасных неверных
   выводов; они перечислены ниже.
3. **Есть ли P1?** Да. Comparator и `SYSTEM_GRAPH` фактически привязаны к
   электрической онтологии, comparator переносит block metadata, текущий
   evidence/result contract недостаточно строг, а стык с Ledger требует явного
   адаптера и расширения Ledger schema.
4. **Нужно ли менять архитектуру?** Нужно переработать внутренние границы, но не
   менять сам подход `SYSTEM_GRAPH → structural comparison`. Нужны generic core,
   отдельная domain policy и отдельный Ledger adapter.
5. **Нужно ли менять SYSTEM_GRAPH schema?** Да, если требование
   discipline-neutral остаётся обязательным. Текущие закрытые enums описывают
   ЭОМ и отклоняют естественные типы ВК/ОВ. Для локальной эксплуатации только на
   `dense_sectioned_board` это можно было бы отложить, но не для общего Mode 2.
6. **Нужно ли менять CHANGE_TYPES?** Девяти structural types достаточно как
   верхнего словаря G2.3; добавлять новые типы до G2.4 не требуется. Но нужно
   формализовать их взаимоисключение/иерархию, а `GraphicChangeLedger.CHANGE_TYPES`
   обязательно расширить structural types и `MODE_2`.
7. **Что исправить до G2.4?** Fail-close по graph validity/quality; устранить
   order-dependent matching и запретить parent relation перекрывать сильную
   canonical identity; сузить detail pass до доказанной representation
   equivalence; отделить электрическую policy и block metadata; утвердить строгий
   comparison/evidence contract и преобразование в Ledger; добавить найденные
   контрпримеры в production tests.

## Вердикт

**C — нужна переработка до интеграции.**

Подход с функциональным графом правильный, и обязательный пример ГРЩ
воспроизводится. Полной смены подхода не нужно. Однако найденные P0 означают, что
интеграция текущего результата в общий Ledger будет публиковать некоторые
неверные structural changes как определённые.

## Объём и база аудита

- Проверен код G2.3 из commit `f5253767`. Текущий `HEAD` содержит после него
  только постороннее изменение shell-скрипта; файлы comparator не менялись.
- Аудит выполнялся по production-модулям matcher/comparator, действующему
  `SYSTEM_GRAPH` validator, действующему G1 `GraphicChangeLedger` contract,
  тестам G2.3 и реальным G2.2 left/right JSON.
- Production code не изменялся. G2.4, UI, профили и Ledger integration не
  реализовывались.

## Сводка находок

| Severity | Количество | Блокирует G2.4 | Смысл |
|---|---:|---|---|
| P0 | 4 | Да | Воспроизводимые неверные structural conclusions |
| P1 | 5 | Да | Границы, schema, Ledger contract и validation |
| P2 | 4 | Нет после P0/P1 | Confidence, evidence size, scaling, JSON purity |
| P3 | 3 | Нет | Тестовая матрица и документация policy |

## P0 — опасные неверные выводы

### P0-1. Порядок дубликатов меняет matching и создаёт ложный NODE_TYPE_CHANGED

Canonical pass является greedy. При равных score порядок кандидатов наследует
порядок nodes во входном массиве. Затем `_align_terminals_from_matched_parents`
безусловно удаляет уже найденные terminal matches и заново привязывает terminals
через matched parent. Он не проверяет, что новая пара противоречит сильным
canonical identities терминалов.

Код:

- `graph_identity_matcher.py:247-325` — принудительная повторная привязка;
- `graph_identity_matcher.py:336-357` — greedy canonical matching;
- сортировка использует только `(confidence, score)` и не имеет устойчивого
  semantic tie-breaker.

Независимый сценарий:

- два одинаково обозначенных `OUTGOING_DEVICE`;
- terminals имеют устойчивые identities `LOAD#A` и `LOAD#B` и типы `MOTOR` и
  `PANEL`;
- справа изменён только порядок двух parent nodes в массиве.

Получено:

```text
LL1 → RL2, MOTOR → PANEL
LL2 → RL1, PANEL → MOTOR
changes = [NODE_TYPE_CHANGED, NODE_TYPE_CHANGED]
```

Фактических изменений нет. Это одновременно page/list-order dependency,
нарушение приоритета canonical identity и ложный результат для Ledger.

### P0-2. Переименование уникальной функции может дать REMOVE + ADD

`LOAD`, `OUTGOING_DEVICE` и `UNKNOWN_NODE` полностью исключены из unique
functional-role pass (`graph_identity_matcher.py:20-22, 364-386`). Если у
единственного `LOAD` сохраняются тип, секция и роль, но меняются label и
canonical designation, composite score недостаточен. Оба узла затем считаются
strong unmatched и публикуются как removal/addition
(`system_graph_comparator.py:844-909`).

Независимый сценарий:

```text
LEFT:  LOAD, section=SECTION#1, canonical=PUMP-A,   label=P-1
RIGHT: LOAD, section=SECTION#1, canonical=PUMP-001, label=Насос
```

Получено:

```text
status = CHANGED
changes = [NODE_ADDED, NODE_REMOVED]
```

Тест renamed labels покрывает только `METERING_GROUP`, который заранее
исключается из individual unmatched logic. Поэтому заявленное общее negative
свойство тестом не доказано.

### P0-3. Detail pass может скрыть появление функционального узла

Detail pass классифицирует любой дополнительный unmatched intermediate node на
кратчайшем `SOURCE → BUS_SECTION` пути как detail expansion. Семантика нового
узла не проверяется; после этого node попадает в `consumed_right` и не может дать
`NODE_ADDED` (`system_graph_comparator.py:580-647, 848-880`).

Независимый валидный сценарий добавил в путь strongly identified
`SERVICE_GROUP`, `canonical_identity=PROTECTIVE-STAGE#1`, при неизменном
`source_representation`.

Получено:

```text
changes = [DETAIL_LEVEL_INCREASED]
right_nodes = [SOURCE, PROTECTIVE-STAGE#1, INPUT_DEVICE, BUS_SECTION]
```

Никакого `NODE_ADDED` или `FUNCTIONAL_GROUP_CHANGED` нет. Comparator не способен
отличить «раскрыли прежнее представление» от «в цепь добавили новую функцию»,
если profile не выразил это другим hardcoded типом. Перед интеграцией detail
equivalence должна быть доказана role/representation contract, а не одной
разницей длины пути.

### P0-4. Invalid/low-quality input не закрывает публикацию certain changes

Только Level A переводится в `UNCERTAIN_BACKBONE` при invalid/low-confidence
графе. Остальные passes продолжают работу. `GROUP_COUNT_CHANGED` создаётся при
любом различии counts, без quality threshold (`system_graph_comparator.py:803-826`),
а любой тип, кроме `UNCERTAIN_STRUCTURAL_CHANGE`, делает общий status `CHANGED`
(`system_graph_comparator.py:1034-1035`).

Воспроизведено два случая:

1. Два contract-valid графа, `identity_coverage=0.0`, confidence повторяющихся
   узлов `0.1`, counts `3 → 4`:

   ```text
   status = CHANGED
   GROUP_COUNT_CHANGED confidence = 0.0
   ```

2. Граф с `evidence=[]` у source, то есть input validation invalid:

   ```text
   left_graph.validation.valid = false
   result.validation.valid = true
   status = CHANGED
   DETAIL_LEVEL_INCREASED опубликован как certain
   ```

Result validator не связывает допустимость результата с validity входных
графов. Для production нужен единый fail-closed gate: invalid graph не может
порождать certain change; low-quality scope должен давать uncertain/unsupported.

## P1 — архитектурные проблемы

### P1-1. Comparator не discipline-neutral

В production comparator зашиты:

- `METERING_GROUP`, `COMPENSATION_GROUP`, `SERVICE_GROUP`;
- `SOURCE`, `INPUT_DEVICE`, `BUS_SECTION`, `SECTION_DEVICE`,
  `OUTGOING_DEVICE`, `LOAD`;
- `FEEDS`, `TIES_SECTIONS`, `TERMINATES_AT`;
- representations `UPSTREAM_TP_CONNECTION` и `TRANSFORMER_EXPLICIT`;
- глобальный специальный алгоритм для outgoing devices.

См. `system_graph_comparator.py:44-52, 213-342, 412-455, 556-647, 803-845` и
`graph_identity_matcher.py:122-146`.

Ручного знания конкретных `QF3`, `QS1`, `ТП1/ТП2`, block ids или ГРЩ нет. Но
наличие `TRANSFORMER_EXPLICIT` и электрической topology означает, что утверждение
о discipline-neutral comparator неверно. Правильная граница — generic matching
engine плюс передаваемая domain comparison policy.

### P1-2. SYSTEM_GRAPH v1 не является общим контрактом ЭОМ/ВК/ОВ

`system_graph.py:14-38` задаёт закрытые электрические `NODE_TYPES` и
`EDGE_TYPES`. Валидатор отклонил audit node `PIPE_SECTION` с
`node_type_invalid`. Естественные `PIPE`, `VALVE`, `PUMP`, `DUCT`, `AHU`,
`FAN`, `DAMPER` и соответствующие relations выразить без потери смысла нельзя.

Дополнительно validator:

- не требует `canonical_identity`, functional role, `labels`, `attrs` или
  representation semantics, хотя comparator от них зависит;
- требует `block/profile/bbox` даже для чистого functional graph;
- проверяет наличие evidence list/tokens, но не их внутренний provenance
  contract.

Нужна эволюция schema: generic role/category, domain subtype/namespace,
group/scope identity, representation level/equivalence и расширяемые relation
roles. Электрический vocabulary должен стать policy/extension, а не единственным
допустимым enum.

### P1-3. Нарушена граница block metadata

Comparator не открывает PDF, не строит граф и не читает `discipline`. Это
правильно. Однако `_grounding` и `_graph_ref` явно читают и возвращают
`block_id`, `page_index`, `profile_id` (`system_graph_comparator.py:102-137,
943-951`). По условиям аудита comparator не должен знать `block_id`.

Block/page/bbox нужны Ledger для адресации, но владеть ими должен G2.4 adapter.
Comparator достаточно вернуть node/edge locators и input graph digest. Adapter,
имея оба исходных графа, восстановит regions и comparison scope.

### P1-4. Текущий результат нельзя напрямую положить в G1 Ledger

Действующий `graphic-change-ledger.v1` принимает:

- `mode ∈ {null, MODE_1}`, но не `MODE_2`;
- четыре graphic types, но не девять structural types;
- confidence enum `HIGH/MEDIUM/LOW`, тогда как comparator возвращает float;
- evidence как non-empty array, comparator возвращает object `{left,right,reason}`;
- обязательные `left_region`, `right_region`, `address_hints`, per-change
  `provenance`, которых в comparator change нет;
- change object с `additionalProperties=false`, поэтому `level`, `subject`,
  `summary` нельзя просто добавить без schema extension.

Независимая валидация дала:

```text
mode=MODE_2  → LedgerValidationError: invalid mode
mode=null    → missing left_region, right_region, address_hints, provenance
```

Следовательно, недостаточно только добавить `mode=MODE_2`. Нужны schema
extension и явный adapter с утверждённой mapping policy. В частности,
`summary/level/subject` нужен structural subobject либо нормализованное поле
Ledger; numeric confidence требует фиксированных thresholds; provenance нельзя
восстановить из одной декларации `vision_used=false`.

### P1-5. Comparison/evidence contract валидируется слишком поверхностно

`validate_comparison_result` проверяет наличие полей, диапазон confidence и
наличие keys `left/right` в evidence. Он не проверяет:

- что input graph validation valid или результат обязан быть uncertain;
- допустимые levels и непустые subject/summary;
- соответствие `left_nodes/right_nodes` grounded evidence;
- наличие и валидность source tokens/edge evidence;
- наличие input provenance, graph hashes и extractor/profile versions;
- согласованность общего status с changes;
- обоснование и агрегацию confidence.

В реальном результате top-level confidence заметно расходится с minimum
grounding confidence:

| Change | Result confidence | Left/Right grounding minimum |
|---|---:|---:|
| `DETAIL_LEVEL_INCREASED` | 0.940 | 0.750 / 0.750 |
| `GROUP_COUNT_CHANGED` | 0.867 | 0.350 / 0.350 |
| unresolved correspondence | 0.490 | 0.300 / 0.350 |

Разные confidence могут быть методически оправданы, но contract не объясняет,
что именно измеряет каждое число и как оно преобразуется в Ledger confidence.
Top-level provenance содержит версии comparator/matcher, но не переносит
`profile_version`, `vector_evidence.extraction_version` или hashes входных
графов. Для трассируемого Ledger этого недостаточно.

## P2 — качество и производительность

### P2-1. Greedy matching не решает глобальную задачу соответствия

Все passes выбирают пары greedy. Нет mutual-best/margin rule, bipartite
assignment или semantic tie-break. Даже после устранения P0-1 останется риск
локально оптимального, но глобально неверного matching на повторяющихся узлах.

### P2-2. Реализация имеет высокую асимптотику на больших графах

Pair scoring многократно строит node index и сканирует edges внутри двойного
цикла candidates. Фактическая сложность ближе к
`O(V_left × V_right × (V + E))`, причём несколько passes повторяют scoring. Для
ГРЩ с 82/73 nodes это незаметно; scaling tests отсутствуют.

### P2-3. Evidence для агрегатов слишком широк

Реальный `GROUP_COUNT_CHANGED` включает 79 left и 73 right source tokens и все
30/27 outgoing nodes. Это трассируемо, но плохо локализуется и раздует общий
Ledger. Нужны group evidence summary, count provenance и ограниченный набор
representative/member locators, без потери доступа к полному sidecar.

### P2-4. Runtime result не полностью JSON-native

`section_ties` возвращаются как tuples. JSON serialization преобразует их в
arrays, поэтому сохранённый artifact воспроизводится после JSON round-trip, но
не равен runtime result напрямую. Это небольшой дефект contract purity, не
изменяющий смысл.

## P3 — улучшения

1. Документировать направленность `DETAIL_LEVEL_INCREASED` и policy для случая
   detailed LEFT → coarse RIGHT.
2. Зафиксировать scope `GROUP_COUNT_CHANGED`: сейчас counts считаются глобально
   по всем `OUTGOING_DEVICE`, а не по секции/group identity; перераспределение
   между секциями может быть потеряно.
3. Добавить versioned JSON Schema для comparison result и golden compatibility
   test с будущим Ledger adapter.

## Identity matching audit

### Приоритеты

Заявленный порядок отражён в policy output, но реализация не полностью ему
соответствует:

- canonical identity действительно идёт первым и имеет weight `0.62`;
- `node_type` получает `0.16` до functional role (`0.10`), хотя отдельного
  приоритета node type в заданном списке нет;
- labels имеют малый weight `0.06` и сами по себе не могут создать strong match;
- relations и stable attributes имеют `0.04` и `0.02`;
- bbox/geometry в score не используются, weight `0.0`.

Скрытой координатной или bbox dependency не найдено. `column`, geometry keys,
`x/y` исключены из stable attributes. Однако подтверждена зависимость от
порядка nodes при равных semantic score, описанная в P0-1.

Label overfitting в score нет; наоборот, обнаружен противоположный риск —
functional identity недостаточно сильна при полном переименовании отдельных
узлов.

## Detail-level audit

Happy path работает:

- matched source;
- coarse left path;
- expanded right path;
- intermediate right nodes поглощаются;
- получается `DETAIL_LEVEL_INCREASED`, а не `NODE_ADDED`.

Обязательные `ТП1/ТП2 → Т1/Т2` классифицированы правильно. Но это не generic
one-to-many matcher: pass реализован только для `SOURCE`, только по `FEEDS`,
только до `BUS_SECTION`, с hardcoded source representation rank и выбором
кратчайшего пути. One-to-many не является общей cardinality моделью matching.

Главный blocker — отсутствие проверки semantic equivalence расширенного
подграфа. До G2.4 требуется whitelist/contract допустимых representation nodes
или доказательство сохранения boundary relations и functional roles; простое
увеличение числа unmatched intermediate nodes недостаточно.

## Change types audit

Девять structural types достаточны для текущего уровня абстракции:

- system-level: `SYSTEM_BACKBONE_CHANGED`;
- group-level: `FUNCTIONAL_GROUP_CHANGED`, `GROUP_COUNT_CHANGED`;
- node/edge-level: `NODE_ADDED`, `NODE_REMOVED`, `NODE_TYPE_CHANGED`,
  `CONNECTION_CHANGED`;
- representation/uncertainty: `DETAIL_LEVEL_INCREASED`,
  `UNCERTAIN_STRUCTURAL_CHANGE`.

Проблема не в нехватке типов, а в отсутствии формализованных правил:

- detail должен быть взаимоисключающим с semantic add/remove только после
  доказанной equivalence;
- backbone change и его дочерние node/connection changes требуют hierarchy или
  dedup policy, иначе Ledger получит двойное описание;
- `FUNCTIONAL_GROUP_CHANGED` и `GROUP_COUNT_CHANGED` должны иметь явный group
  identity/scope;
- broad uncertain type допустим, если `subject/reason` остаются обязательными и
  machine-readable.

Comparator types можно сохранить. Меняется общий Ledger vocabulary и adapter,
а также правила эмиссии типов.

## Evidence contract audit

На обязательном реальном результате все шесть changes содержат:

- стабильный в пределах результата `change_id`;
- type, level, subject, summary, numeric confidence;
- left/right node ids;
- grounded nodes/edges и source tokens;
- machine-readable reason.

То есть конкретный вывод ГРЩ вручную восстановить можно. Особенно хорошо
grounded `QF3 → QS1`: обе стороны имеют по одному token и confidence `0.92`.

Недостатки перед Ledger:

- нет per-change provenance;
- отсутствуют graph hashes/input artifact versions;
- node evidence не содержит bbox/region, поэтому direct wrapping невозможен без
  повторного доступа к graphs;
- validator не гарантирует полноту grounded ids/tokens;
- confidence aggregation не типизирована;
- `change_id` не включает graph/scope identity, поэтому при объединении
  нескольких block comparisons возможны одинаковые ids для одинаковых local
  node ids/subjects.

## Проверка реального ГРЩ

Свежий production-вызов на обязательных G2.2 JSON дал:

```text
status: CHANGED
backbone: BACKBONE_PRESERVED
functional groups: FUNCTIONS_UNCERTAIN
matched pairs: 50
left match rate: 0.610
right match rate: 0.685
ambiguous left nodes: 11
```

Changes:

- 2 × `DETAIL_LEVEL_INCREASED` для двух source paths;
- 1 × `NODE_TYPE_CHANGED` для `CIRCUIT_BREAKER → SWITCH_DISCONNECTOR`;
- 1 × `GROUP_COUNT_CHANGED`, `30 → 27`;
- 2 × `UNCERTAIN_STRUCTURAL_CHANGE` для reserve и unresolved branches;
- `NODE_ADDED=0`, `NODE_REMOVED=0`.

`METERING_GROUP`, `COMPENSATION_GROUP`, `SERVICE_GROUP` присутствуют с обеих
сторон в `SECTION#1` и `SECTION#2` и отмечены preserved. Скрытых проверок
`QF3/QS1/ТП1/ТП2/ГРЩ` в production source нет.

Сохранённый `comparison_result.json` равен свежему результату после обычного
JSON round-trip; единственное runtime отличие — tuple/list для `section_ties`.

Вывод: обязательный пример правильный и воспроизводимый, но один успешный
profile-specific fixture не компенсирует P0 на допустимых соседних графах.

## Качество тестов

Текущий suite: `10 passed`. Он импортирует production matcher/comparator; восемь
сценариев используют synthetic fixtures, два — реальные G2.2 JSON.

| Test | Класс | Оценка |
|---|---|---|
| Identical graph, other bbox | STRONG | Прямо доказывает отсутствие bbox identity |
| Renamed labels | WEAK | Только managed `METERING_GROUP`; generic node не проверен |
| Source detail expansion | STRONG для happy path / WEAK для safety | Проверяет ожидаемую детализацию, но не semantic node inside path |
| New branch added | WEAK | Только группа размера `1 → 2`; production repeated scope не включается |
| Node removed | WEAK | Изолированный strong node без ambiguity/relations |
| Edge type changed | STRONG | Проверяет обе стороны edge evidence и symmetric relation semantics |
| Weak identity uncertain | STRONG | Проверяет suppression ADD/REMOVE при низкой уверенности |
| Real GRSh | STRONG regression | Реальные artifacts и полный ожидаемый набор типов |
| Every change grounded | WEAK/TAUTOLOGICAL | Проверяет shape собственного result validator, но не G1 Ledger contract |
| No manual cases | TAUTOLOGICAL/WEAK | Поиск нескольких строк не доказывает generalization; transformer vocabulary остаётся |

Не покрыты:

- duplicate identities и перестановка nodes;
- canonical match против parent-relation rematch;
- renamed unique functional node;
- invalid graph и quality fail-close;
- zero-confidence certain change;
- semantic addition inside detail path;
- direct Ledger contract/golden adapter;
- ВК/ОВ vocabulary validation;
- per-section group counts;
- scaling/performance.

## Стык с G1 и рекомендуемая граница G2.4

Правильный поток после исправлений:

```text
SYSTEM_GRAPH L/R
    ↓
generic matcher + domain comparison policy
    ↓
validated structural comparison result
    ↓
G2.4 adapter (владеет block/page/bbox и confidence mapping)
    ↓
GraphicChangeLedger mode=MODE_2
```

Минимальные обязанности G2.4 adapter:

1. Расширить Ledger `mode` и union change types.
2. Преобразовать numeric confidence по versioned policy.
3. Преобразовать `{left,right,reason}` evidence в Ledger evidence array.
4. Получить left/right regions из исходных graph nodes, не из comparator core.
5. Заполнить address hints и per-change provenance.
6. Сохранить `level/subject/summary` в разрешённом structural subobject.
7. Проверить uniqueness ids на уровне всего Ledger.

Но этот adapter нельзя подключать до закрытия P0: он лишь перенесёт ошибочные
выводы в общий журнал.

## Обязательный pre-G2.4 checklist

1. Fail-close: invalid graph → no certain changes; quality gates едины для всех
   levels/types.
2. Matcher: semantic deterministic assignment; strong canonical pair нельзя
   перекрывать parent relation; order-permutation tests обязательны.
3. Functional rename: unique role/margin logic без blanket exclusion типа.
4. Detail pass: representation equivalence contract, boundary preservation и
   запрет поглощать новый semantic node без доказательства.
5. Schema: generic core roles + domain extensions; comparator получает policy,
   а не hardcoded electrical vocabulary.
6. Boundary: убрать block/page/profile metadata из comparator decisions/output
   core; оставить locators/digests для adapter.
7. Result schema: строгая validation evidence, provenance, status, levels,
   confidence semantics и input validity.
8. Ledger contract test: реальный G2.3 result → MODE_2 adapter → действующий
   versioned Ledger validator без monkeypatch.
9. Добавить все четыре P0 counterexamples как production regression tests.

После выполнения checklist архитектура сможет перейти к G2.4 без смены базового
graph-comparison подхода.
