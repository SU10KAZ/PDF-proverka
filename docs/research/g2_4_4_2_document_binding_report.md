# G2.4.4.2 — Document Provenance Binding

Привязка графических блоков к документам пары. Отчёт по разведке, реализации
и проверке. Дата: 2026-08-26. Базовый коммит: `61553e16`.

Задача закрывает расхождение **5.4.10** из `docs/research/g2_4_5_policy_corpus.md`:
«Нет доказательства, что SYSTEM_GRAPH-блоки принадлежат тем же PDF, что и
Stage 5.3-пара» — и отвечает на вопрос **Q8** того же отчёта вариантом (а)+(б):
дескриптор документа в артефактах G2.4.4 плюс детерминированный резолвер
`block_id → документ` по `blocks.json`.

---

## Часть 1. Разведка

### 1.1 Где физически живёт связь `block_id → документ`

Единственный источник — вне артефактов G2.4.x:

```
projects_v2/objects/<объект>/comparison/<stage_1|stage_2>/documents/<КОД>/versions/vNNN/02_work/blocks.json
```

Поля: `document_id`, `document_name`, `document_path`, массив `blocks[].block_id`.
Код документа = `document.json → document_code` (совпадает с именем папки).

Внутри артефактов G2.4.x кода документа не было **нигде**:

| Артефакт | Что есть о блоке | Кода документа |
|---|---|---|
| `scope_join.json → source_artifacts.graphic_scope_groups[].block_pairs[]` | `block_id`, `page_index_0based`, дайджесты | нет |
| `graphic_change_ledger.json → comparison_scope.*_blocks[].source.graph_provenance` | `block_id`, статистика вектор-слоя | нет |
| `left/right_system_graph.json → provenance.vector_evidence` | `block_id`, `page_index_source` | нет |

Легаси-артефакты `experiments/g2_vectograf_system_graph_research/artifacts/grsh_*_graph.json`
несут `block.pdf` с полным путём, но в производстве корпуса **не участвовали**:
сверка подписей показала, что артефакты ИОС собраны из
`experiments/g2_dense_sectioned_board/{left,right}_system_graph.json`
(`build_side_graph_entities(...)["source_signature"]` совпал байт-в-байт),
а там пути нет.

### 1.2 Чем идентифицируются документы пары

В Stage 5 и Stage 5.3 — **только `pair_id`**. Коды документов лежат отдельно, в
`comparison/sessions/<sid>/pairs/<pair_id>/pair.json`:
`left/right → document_code, pdf_path, filename, discipline, version_id`.
Этот файл в G2.4.4 раньше не подавался вообще.

### 1.3 Готовый общий идентификатор

Готового не было. `document_code` есть только на текстовой стороне; на
графической его приходится выводить из `blocks.json`. Введён как общий ключ.

### 1.4 Дополненные артефакты

- `scope_join.json` — новый блок `document_binding`, поле
  `source_artifacts.pair_documents`, опциональное `block_pairs[].{left,right}.document`;
- `graphic_coverage.json` — `source_artifacts.document_binding` и
  `source_artifacts.pair_documents`, reason-код `document_binding_mismatch`;
- схемы `comparison_scope.schema.json`, `graphic_coverage.schema.json`;
- CLI `scripts/run_g2_4_4_scope_side_coverage.py` — флаг `--pair`.

`graphic_change_ledger_v2.schema.json` **не трогался**: `additionalProperties: false`
и замороженный контракт G2.4.1.

### 1.5 Когда связь недоказуема

Да, такие случаи есть и они штатные:

1. графики нет вовсе (АР `p570d156f57`: `graphic_scope_groups: []`);
2. блок-пары поданы без дескрипторов (прежний режим CLI);
3. `blocks.json` документа недоступен или не содержит блока.

Все три дают `UNPROVEN`, ни один не даёт `MISMATCH`.

### 1.6 Побочная находка разведки: стороны корпуса были перепутаны

При резолве блоков выяснилось, что оба SYSTEM_GRAPH-блока ИОС принадлежали не
документам пары `p26c08b83a6`, а отдельным одностраничным выпискам, лежавшим в
**неверных** стадиях:

| Выписка | Лежала в | Физически |
|---|---|---|
| `Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия` | stage_1 (LEFT) | стр. 21 **правого** документа |
| `Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1` | stage_2 (RIGHT) | стр. 52 **левого** документа |

Доказано детерминированно: sha256 нормализованного текста страницы выписки
совпал ровно с одной страницей полного документа противоположной стороны.
Косвенно подтверждено записью о пилоте `p9692b6b5` («стр.52 OLD ↔ стр.21 NEW»).

26.08.2026 выписки перезагружены в правильные стадии по решению Андрея Ивановича.
Следствие: пилотный корпус `experiments/g2_4_4_scope_side_coverage/ios/`
построен на **инвертированных** сторонах — «добавлено» и «удалено» в нём
поменяны местами. Пересборка вынесена в отдельный шаг (путь Б), в этой задаче
намеренно не выполнялась.

---

## Часть 2. Реализация

### 2.1 Новый модуль `document_binding.py`

Чистые функции, без ввода-вывода:

| Функция | Назначение |
|---|---|
| `normalize_document_descriptor(value, where)` | валидация одного дескриптора |
| `normalize_pair_documents(value)` | валидация пары LEFT/RIGHT |
| `pair_documents_from_pair_artifact(pair, stage53)` | чтение `pair.json` со сверкой `pair.id == stage53.pair_id` |
| `document_descriptor_for_block(blocks_payload, block_id, ...)` | привязка блока к документу с проверкой наличия блока в индексе |
| `verify_document_binding(pair_documents, graphic_scope_groups)` | вердикт |
| `validate_document_binding(payload)` | валидация сохранённого блока |

**Дескриптор документа:**

```json
{"document_code": "…|null", "source_path": "…|null", "provenance": "ARTIFACT|CLI_ARGUMENT|ABSENT"}
```

`provenance` отвечает на требование «признак, откуда это взято»:
`ARTIFACT` — прочитано из артефакта, ключуемого самой парой (сильнейшее);
`CLI_ARGUMENT` — подано вызывающим извне; `ABSENT` — дескриптора нет.

### 2.2 Три состояния

```
DOCUMENT_BINDING_PROVEN     совпадение доказано данными
DOCUMENT_BINDING_MISMATCH   данные явно противоречат
DOCUMENT_BINDING_UNPROVEN   доказать нечем
```

Правило, разделяющее `MISMATCH` и `UNPROVEN`:

> `MISMATCH` выносится **только** когда код документа блока известен **и**
> отличается от кода документа пары на той же стороне. Любая другая неполнота —
> `UNPROVEN`.

Отсутствие доказательства не есть доказательство отсутствия.

Сравнение кодов — строгое равенство строк. Никакого fuzzy, никакого сходства
имён файлов, все списки сортируются, результат не зависит от порядка входа.

### 2.3 Вывод состояния наружу

- `scope_join.document_binding` — полный вердикт с посторонней разбивкой
  (`sides.LEFT/RIGHT`: состояние, ожидаемый и наблюдаемые коды, число
  непривязанных блок-пар);
- `graphic_coverage.source_artifacts.document_binding` — тот же блок,
  входит в `source_signature`;
- `reason_codes` записей покрытия получают `document_binding_mismatch`.

### 2.4 Fail-closed

При `MISMATCH` ни одна запись покрытия не может быть `CHECKED`. Реализовано
тройным контуром:

1. состояние области `CHECKED` понижается до `CHECK_BLOCKED` до построения записей
   (оно уже каскадно распространяется на субъекты существующей логикой);
2. страховочный пост-проход по всем записям;
3. инвариант в `validate_graphic_coverage`: `CHECKED` при `MISMATCH` → ошибка.

При `UNPROVEN` поведение **не меняется** — состояние просто фиксируется.

Существующие состояния `CHECKED / NOT_CHECKED / CHECK_BLOCKED / NOT_APPLICABLE`
не переопределялись, новых состояний покрытия не вводилось. Логика сравнения,
comparator, matcher и coverage-семантика не менялись.

### 2.5 Additive-гарантия

Блок-пара без дескрипторов не получает ключ `document` вовсе, поэтому её
`block_pair_ref`, `graphic_scope_group_id` и все `coverage_id` остаются
байт-идентичными. Обогащение одной пары никогда не перенумеровывает другие.

### 2.6 Версии схем

| Артефакт | Было | Стало |
|---|---|---|
| scope join | `text-graphic-scope-join.v1` / `explicit-page-base-scope-join-v1` | `.v2` / `-v2` |
| graphic coverage | `graphic-coverage.v2` / `graphic-coverage-builder-v2` | `.v3` / `-v3` |

Старые файлы новым кодом **не читаются** и отвергаются явной ошибкой, а не
принимаются молча:

```
ios/scope_join.json       → ScopeJoinValidationError: scope join: invalid envelope
ios/graphic_coverage.json → GraphicCoverageValidationError: unsupported contract
```

Сохранённый корпус в `experiments/g2_4_4_scope_side_coverage/{ios,ar}/`
пересобран новым кодом без дескрипторов (`UNPROVEN`), числа сохранены.

---

## Часть 3. Проверка

### 3.1 Числа на реальных данных

Пересчёт тем же входом, до и после изменений:

| Корпус | Состояние | До | После | |
|---|---|---:|---:|---|
| ИОС `p26c08b83a6` | CHECKED | 76 | 76 | без изменений |
| | NOT_CHECKED | 995 | 995 | без изменений |
| | CHECK_BLOCKED | 0 | 0 | без изменений |
| | NOT_APPLICABLE | 1785 | 1785 | без изменений |
| АР `p570d156f57` | CHECKED | 0 | 0 | без изменений |
| | NOT_CHECKED | 612 | 612 | без изменений |
| | CHECK_BLOCKED | 0 | 0 | без изменений |
| | NOT_APPLICABLE | 1020 | 1020 | без изменений |

Регрессии нет.

### 3.2 Поведение привязки на живых данных

| Сценарий | Вердикт | CHECKED | CHECK_BLOCKED |
|---|---|---:|---:|
| Документы пары = реальные владельцы блоков | `PROVEN` | 76 | 0 |
| Документы пары = реальная пара `p26c08b83a6` | `MISMATCH` | **0** | 468 |
| Дескрипторы не поданы | `UNPROVEN` | 76 | 0 |
| АР, графики нет | `UNPROVEN` | 0 | 0 |

Вторая строка — тот самый риск из постановки: подмена чертежа из другого
документа раньше проходила незамеченной, теперь ловится и обнуляет `CHECKED`.

### 3.3 Тесты

Новый файл `tests/test_stage_comparison_document_binding.py`, 14 тестов:

- `PROVEN` на реальном ИОС против документов-владельцев блоков;
- АР без графики → `UNPROVEN`, не `MISMATCH`, без падения;
- подстановка чужого документа → `MISMATCH` и ни одной записи `CHECKED`;
- отсутствие `document_code` → `UNPROVEN`, числа прежние;
- независимость вердикта от порядка входа (три перестановки, byte-identical);
- повторные сборки байт-идентичны;
- `MISMATCH` ≠ `UNPROVEN` как отдельное утверждение;
- отказы: блок вне индекса, `pair.json` от другой пары, неизвестные поля
  дескриптора, код без provenance;
- additive: необогащённая пара сохраняет `block_pair_ref`.

Обновлён один существующий тест — `test_g244_contract_schemas_are_versioned`
в `tests/test_stage_comparison_scope_side_coverage.py`: он прибивает номера
версий схем гвоздями, а версии подняты по требованию задачи.

### 3.4 Прогон

| Набор | До (`61553e16`) | После |
|---|---|---|
| `test_stage_comparison_scope_side_coverage.py` + `…_unified_entity_bridge.py` + `…_entity_producers_context.py` | 64 passed | 64 passed |
| `tests/test_stage_comparison_document_binding.py` (новый) | — | 14 passed |
| **Итого по затронутой области** | **64** | **78** |

Полный прогон `pytest tests backend/tests` (без пяти grpc-модулей, которые не
собираются в этом окружении): **7725 passed, 52 failed, 66 skipped, 33 errors**
за 5 мин 24 с. Ни одно падение не относится к изменённым модулям — проверено
фильтром по `scope|coverage|bridge|entity|binding|graphic`: совпадений ноль.
Единственный падающий файл в сводке — `tests/test_norms_status_index_fallback.py`,
он присутствует в `scripts/ci_known_failures.txt`.

Регресс-гейт сообщил о пяти «новых» падениях:
`test_agent_gateway_12b`, `test_agent_stream_protocol_v1`, `test_mtls_12d`,
`test_pki_recovery_12f`, `test_reliability_chaos_12e`. Все пять падают на
`ModuleNotFoundError: No module named 'grpc'` при импорте, ни один не
ссылается на изменённые модули. Это дрейф окружения: те же пять файлов
перечислены как несобираемые ещё в `G2_4_4_SCOPE_SIDE_COVERAGE_REPORT.md`.

### 3.5 Замороженный эталон

`docs/research/g2_4_5_policy_corpus.md` не изменялся.

---

## Часть 4. Что осталось недоказуемым

1. **Резолвер `block_id → документ` живёт в вызывающем коде, не в артефактах.**
   Дескриптор в артефакте доказывает принадлежность блока документу только в той
   мере, в какой ему доверяет тот, кто его записал. Проверка «блок реально есть
   в `blocks.json` этого документа» выполняется в `document_descriptor_for_block`,
   но результат этой проверки в артефакте фиксируется как утверждение, а не как
   воспроизводимое доказательство.

2. **Продовой обвязки у G2.4.4 по-прежнему нет** (расхождение 5.4.8). Ни
   `paths.py`, ни `store.py`, ни роутер не знают про эти артефакты; единственный
   производитель — CLI. В проде связка блоков с документами пары обеспечена
   структурно (`store.py:_prepared_block_records` читает
   `pair.<side>.pdf_path.parent/blocks.json` и падает `unknown_<side>_block_id`),
   то есть дыра была только на экспериментальном пути.

3. **Корпус ИОС остаётся снятым с перепутанных сторон.** Числа в этом отчёте и в
   `g2_4_5_policy_corpus.md` получены на инвертированном входе. Пересборка —
   следующий шаг.

4. **Версия документа не участвует в привязке.** Сверяется `document_code`, но не
   `version_id`. Две версии одного документа сейчас неразличимы для привязки.
