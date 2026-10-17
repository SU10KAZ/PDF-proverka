# G2.4.4 — Scope, Side & Coverage

## Краткие ответы

1. **Закрыта ли TEXT entity attribution?** Да. G2.4.3 уже даёт цепочку
   `entity → source_change_ids → evidence_ids → fragment_ids`; она проверена и не
   переделывалась.
2. **Разделены ли LEFT и RIGHT entity links?** Да. `side-entity-links.v1` содержит
   две обязательные именованные ветки `LEFT` и `RIGHT`; bridge запускается для них
   независимо.
3. **Есть ли проблема page 1-based/0-based?** Да. Production TEXT использует
   `pdf_page` 1-based, prepared graphic block и SYSTEM_GRAPH используют
   `page_index` 0-based.
4. **Как она исправлена?** Добавлена единственная явная конверсия
   `pdf_page_1based - 1 → canonical_page_index`; в scope artifact обе исходные
   системы координат названы явно.
5. **Получился ли стабильный TEXT↔GRAPHIC scope join?** Да. Join использует только
   comparison pair, Stage 5.3 sheet group, canonical pages и явно сгруппированные
   graphic block pairs. First-match отсутствует; ambiguous mapping закрывается как
   `UNRESOLVED_SCOPE`.
6. **Сколько real scopes resolved/unresolved?** ИОС/ГРЩ: 1 resolved, 4 unresolved,
   1 resolved child block scope. АР: 0 resolved, 8 unresolved.
7. **Появился ли graphic coverage manifest?** Да: `graphic-coverage.v1` и query
   `coverage(scope, subject, dimension, side?)`.
8. **Можно ли отличить «нет изменения» от «не проверялось»?** Да. Отсутствие change
   не определяет coverage: успешный MODE_2 даёт `CHECKED`, отсутствие SYSTEM_GRAPH —
   `NOT_CHECKED`, quality gate — `CHECK_BLOCKED`, ненаблюдаемая dimension —
   `NOT_APPLICABLE`.
9. **Готова ли инфраструктура для deterministic Unified Change Synthesizer?** Да,
   как входная инфраструктура: есть sides, scope, coverage, trace и stale checks.
   Сам Synthesizer, merge/conflict logic и unified conclusions не реализовывались.

## Проверка базы G2.4.3

Проверен production contract на commit
`69801e141e8233caba395c145ee8eacdb1fab948`.

- `text-entities.v1` уже хранит `source_change_ids`, `evidence_ids`, `fragment_ids`,
  sheet groups/pages и provenance исходного Stage 5.3 artifact.
- `graph-entities.v1` уже хранит `graph_scope`, `graph_node_ids`, block/page,
  locations, edge/source-token evidence и graph digest.
- Существующие producer contracts не изменены; новые контракты — additive wrappers.
- Stage 5.3 semantic logic, SYSTEM_GRAPH comparator и GraphicChangeLedger conclusions
  не менялись.

## Новые контракты

### Explicit side

- `side-graph-entities.v1`: обязательные именованные ветки `LEFT`/`RIGHT`, каждая
  содержит неизменённый `graph-entities.v1` artifact.
- `side-entity-links.v1`: два независимых `entity-bridge.v2` результата. Side не
  определяется по порядку массива, имени файла или block heuristic.
- `query_text_entity_side(...)`: отдельно возвращает evidence-backed
  `PRESENT | ABSENT | UNKNOWN` и `HIGH | MEDIUM | UNKNOWN | NOT_MATCHED` для LEFT
  или RIGHT.

Такой wrapper сохраняет G2.4.3 compatibility и устраняет ложный cardinality conflict:
один LEFT candidate и один RIGHT candidate — две независимые задачи. Два candidates
на одной стороне по-прежнему дают `UNKNOWN`.

### Scope join

`text-graphic-scope-join.v1` хранит:

- `sheet_group_id`, TEXT `pdf_pages_1based` и их
  `canonical_page_indexes_0based`;
- явно именованные LEFT/RIGHT block ids, исходные `page_index_0based` и canonical
  indexes;
- stable `scope_ref`, stable child block scope refs, ledger/comparison digests;
- `RESOLVED | UNRESOLVED_SCOPE` и обязательные `reason_codes`.

Одна explicit graphic scope group может содержать несколько block pairs — это
валидные child scopes одного sheet scope. Две разные graphic scope groups, одинаково
подходящие одному sheet group, не выбираются по порядку и дают
`multiple_graphic_scope_groups_on_sheet`.

Production page contracts проверены непосредственно:

- `sheet_matching.py` создаёт `pdf_page = zero_based_page + 1`, а
  `text_comparison.py` открывает `document[pdf_page - 1]`;
- graphic extraction принимает authoritative `page_index` и открывает
  `document[page_index]`; schema Ledger требует `page_index >= 0`.

Следовательно TEXT page 1 canonical равна index 0, а GRAPHIC `page_index=1` — PDF
page 2 и с TEXT page 1 не joins.

### TEXT coverage query

`query_text_scope(...)` — тонкий read-only adapter над Stage 5.3. Он отвечает:

- найден и проверен ли sheet group;
- есть ли `PAIR_REVIEW_REQUIRED`;
- есть ли `SOURCE_LINK_UNCERTAIN`/uncertain source status;
- почему получен `CHECKED`, `CHECK_BLOCKED` или `NOT_CHECKED`.

Новый большой TEXT coverage artifact не создавался.

### Graphic coverage

`graphic-coverage.v1` строится только из готовых Stage 5.3/TEXT_ENTITIES,
GRAPH_ENTITIES LEFT/RIGHT, side links, scope join, SYSTEM_GRAPH comparison и
GraphicChangeLedger. PDF extraction, OCR, Vision, LLM и новый comparator не
запускаются.

Versioned policy `graphic-coverage-policy-v1` задаёт:

| Route | Dimension | Результат при успешной проверке |
|---|---|---|
| MODE_2 | STRUCTURE, CONNECTION, TYPE, QUANTITY | `CHECKED` |
| MODE_2 | PARAMETER, METHOD, PRINCIPLE, SPACE | `NOT_APPLICABLE` |
| MODE_1 | все semantic dimensions выше | `NOT_APPLICABLE` |
| нет SYSTEM_GRAPH/scope | наблюдаемые MODE_2 dimensions | `NOT_CHECKED` |
| MODE_2 quality gate blocked | наблюдаемые dimensions | `CHECK_BLOCKED` |

Subject coverage консервативен. GRAPH_ENTITY считается checked только если все его
member node ids входят в существующий comparator `HIGH_MATCH`/accepted high-confidence
detail match и ни один member не ambiguous. TEXT_ENTITY дополнительно требует
`SAME_ENTITY/HIGH` link на конкретной стороне к такому GRAPH_ENTITY. Поэтому manifest
не объявляет checked все graph nodes только потому, что comparator был запущен.

Каждая запись имеет stable `coverage_id`, `scope_ref`, subject kind/id, dimension,
side, state, reason codes и refs на block scope, block pair, ledger/comparison,
graph nodes и entity links.

## Source signatures и stale

Scope signature зависит от:

- полного digest и signature Stage 5.3;
- `text-entities.v1` signature;
- LEFT и RIGHT `graph-entities.v1` signatures и side wrapper signature;
- ledger/comparison digests каждого explicit block pair;
- Entity Bridge version, side bridge version, scope join version и page convention.

Coverage signature дополнительно зависит от side links signature, scope signature,
coverage builder version и coverage policy version. Изменённый source возвращает
stale; одинаковые inputs создают byte-equivalent artifacts и те же IDs.

## Real check — ИОС / ГРЩ

Inputs:

- Stage 5.3 pair `p26c08b83a6`;
- real LEFT/RIGHT dense-sectioned-board SYSTEM_GRAPH;
- существующий `system-graph-comparison.v1` и полученный штатным adapter
  `graphic-change-ledger.v2`.

Entity results:

- TEXT: 19 entities;
- LEFT graph: 82 source nodes → 56 entities, 26 representation duplicates removed;
- RIGHT graph: 73 source nodes → 52 entities, 21 representation duplicates removed;
- 27 canonical identities присутствуют с обеих сторон; 13 только LEFT, 10 только
  RIGHT. Это диагностическое множество имён, не новая change classification.
- обе стороны содержат, например, `SECTION_1`, `SECTION_2`, `INPUT_BUS_1/2`,
  `VRU_1/2/3/4`, `VRU_A`; LEFT-only examples: `QF_4`, `QF_5`, `УЗИП_1/2`;
  RIGHT-only examples: `FU_1/2`, `ДР_1/2`, `ХП`, `ЭБ_ГВС`.
- representation duplicates сохраняют node provenance: пары OUTGOING_DEVICE+LOAD
  объединяются только по явному `TERMINATES_AT` rule, но одинаковые сущности разных
  sections/feeders не сливаются.

Side bridge:

| Side | HIGH TEXT entities | UNKNOWN candidate links | unresolved TEXT entities |
|---|---:|---:|---:|
| LEFT | 0 | 18 | 19 |
| RIGHT | 1 (`VRU_1`) | 9 | 18 |

`VRU_A`, `VRU_2/3/4` остаются ambiguous из-за нескольких same-side candidates.
Они не выбираются по первой позиции. Нулевой LEFT HIGH — честный результат текущих
real identities, а не отсутствие LEFT branch.

Scope:

- 5 Stage 5.3 sheet groups;
- 1 graphic scope group;
- 1 resolved sheet scope (`link_3b1c7c47e1ab`) с LEFT PDF page 1/index 0 и
  RIGHT PDF pages 1,3/indexes 0,2; block pair использует index 0 на обеих сторонах;
- 4 unresolved TEXT sheet scopes; first-match не применялся.

Coverage:

- 2,896 records: 116 `CHECKED`, 1,332 `NOT_CHECKED`, 1,448 `NOT_APPLICABLE`,
  0 `CHECK_BLOCKED`;
- resolved sheet scope `CHECKED` по STRUCTURE/CONNECTION/TYPE/QUANTITY;
- по каждой из этих dimensions subject-level checked получили 14 LEFT и 14 RIGHT
  GRAPH_ENTITIES; остальные 42 LEFT и 38 RIGHT graph entities в resolved scope —
  `NOT_CHECKED` из-за отсутствия полного high identity match или ambiguity;
- TEXT subject coverage не завышена: единственный RIGHT HIGH cross-modal link ведёт
  к representation entity, не все member nodes которой high-matched comparator,
  поэтому итоговый TEXT subject остаётся `NOT_CHECKED`;
- PARAMETER/METHOD/PRINCIPLE/SPACE — `NOT_APPLICABLE`, а не графическое доказательство.

## Real check — АР

Inputs: real Stage 5.3 pair `p570d156f57`, без SYSTEM_GRAPH.

- 27 TEXT entities, из них 26 rooms;
- LEFT GRAPH_ENTITIES: 0; RIGHT GRAPH_ENTITIES: 0; synthetic entities не создавались;
- LEFT/RIGHT HIGH links: 0;
- 8 TEXT sheet scopes: 0 resolved, 8 unresolved;
- 1,696 coverage records: 0 `CHECKED`, 848 `NOT_CHECKED`, 848
  `NOT_APPLICABLE`;
- все scope records по STRUCTURE/CONNECTION/TYPE/QUANTITY — `NOT_CHECKED` с причиной
  `no_system_graph_for_sheet`, а не `CHECKED/NO_CHANGE`.

## Negative tests

Покрыты обязательные случаи A–K:

- TEXT page 1 не matches GRAPHIC `page_index=1`;
- явная conversion 1-based → 0-based;
- один sheet → несколько explicit block child scopes;
- block на другой странице не joins;
- multiple plausible graphic scope groups → `UNRESOLVED_SCOPE`;
- HIGH LEFT + HIGH RIGHT не образуют cardinality conflict;
- 1 TEXT → 2 candidates на одной стороне → `UNKNOWN`;
- no SYSTEM_GRAPH → `NOT_CHECKED`;
- low identity coverage → `CHECK_BLOCKED` с исходными comparator reason codes;
- unsupported dimension и MODE_1 semantic claim → `NOT_APPLICABLE`;
- одинаковые inputs → идентичные artifacts/IDs, изменённый source → stale.

## Artifacts и regression

Real artifacts находятся в:

- `experiments/g2_4_4_scope_side_coverage/ios/`;
- `experiments/g2_4_4_scope_side_coverage/ar/`.

CLI: `scripts/run_g2_4_4_scope_side_coverage.py`.

Проверены G2.4.4 tests и совместимость Stage 5.3, G2 comparator/profile,
GraphicChangeLedger, Unified Evidence Contract, G2.4.2 bridge и G2.4.3 producers.
Исходные conclusions и ранее сохранённые artifacts не изменялись.

- обязательный regression-набор: **175 passed**;
- полный доступный `tests/` прогон: **6021 passed, 53 skipped**; в checkout также
  остались 14 несвязанных baseline failures и 33 errors в norms fallback/config/UI/
  routing suites;
- ещё 5 agent-gateway/PKI файлов не собираются в текущем окружении из-за отсутствия
  `grpc`/`google.protobuf`. Эти модули и их зависимости задачей не изменялись.

Unified Change Synthesizer, dimension merge, conflict resolution, combined confidence,
AI wording, UI и parent/child unified changes намеренно отсутствуют.
