# G2.4.1 — Ledger contract audit

## Краткий вердикт

**B — нужны небольшие contract extensions.**

TEXT и GRAPHIC контракты совместимы как независимые источники доказательств, но
сейчас не существует безопасного слоя, который устанавливает, что два source
changes относятся к одному объекту и одному смыслу. Непосредственно добавлять
graphic IDs в Stage 5.3 нельзя: его final validator сверяет все `evidence_ids`
только с TEXT atomic evidence.

Минимальный hardening выполнен отдельным контрактом
`unified-change-evidence.v1`. Это только lossless interop-envelope; он не меняет
Stage 5.3, GraphicChangeLedger, MODE_1 или comparator и не принимает merge-решений.

## 1. Фактические source contracts

### TEXT — Stage 5.3

`high_level_project_changes.json` имеет `schema_version: "1.0"`. Каждый результат
содержит:

- `change_id`, `type`, `title`, `reason`, `status`;
- `confidence: high | medium`;
- `evidence_sources: ["TEXT"]`;
- `evidence_ids`, `sheet_groups`, `semantic_subject`;
- `details` с before/after, fragment IDs, pages, labels и anchors.

Артефакт верхнего уровня уже содержит `evidence_sources: ["TEXT"]` и capability
flag `graphic_evidence_supported_by_contract: true`. Однако это задел, а не
реальная mixed validation: `validate_final_artifact()` строит индекс только из
Stage 5 TEXT evidence и отклонит любой GraphicChangeLedger ID как
`artifact_unknown_evidence`.

Текущий `change_id` детерминирован по TEXT semantic group, TEXT type и TEXT
evidence IDs. Он не является cross-modal entity ID.

### GRAPHIC — GraphicChangeLedger

MODE_1 остаётся `graphic-change-ledger.v1`; MODE_2 использует
`graphic-change-ledger.v2`. Структурный change содержит:

- `change_id`, `mode`, `type`, `summary`;
- numeric `raw_confidence` и Ledger enum confidence;
- `structural.subject/relation`, left/right node и edge IDs;
- graph-derived regions, address hints, source tokens и grounding;
- graph/comparator/profile provenance.

Graphic `change_id` уникален внутри ledger и трассирует comparator decision, но не
совпадает по namespace или derivation с `hlc_*` ID Stage 5.3.

## 2. Ответы на главные вопросы

### Может ли один итоговый change содержать TEXT + GRAPHIC evidence?

Да, через новый внешний envelope. Source artifacts должны остаться неизменными.
Envelope хранит один будущий unified `change_id`, aggregate source
`TEXT | GRAPHIC | BOTH` и массив нормализованных evidence items.

Каждый item сохраняет:

```json
{
  "evidence_source": "TEXT | GRAPHIC",
  "source_artifact": {
    "kind": "...",
    "schema_version": "..."
  },
  "source_change_id": "исходный change_id",
  "provenance": {},
  "locations": [],
  "source_ids": [],
  "confidence": {
    "level": "HIGH | MEDIUM | LOW | UNKNOWN",
    "raw": null,
    "source_scale": "..."
  }
}
```

V1 envelope валидирует source/artifact correspondence, непустые locations,
source IDs, provenance, confidence и точное соответствие aggregate source
фактическому составу evidence.

### Можно ли сохранить один change_id?

Да, но это должен быть новый ID unified layer. Нельзя выбирать TEXT `hlc_*` или
GRAPHIC `chg_*` как общий ID: оба source ID content-derived по разным данным и
могут измениться независимо.

Правильная схема:

```text
unified change_id
  ├─ TEXT source_change_id
  └─ GRAPHIC source_change_id
```

Interop contract допускает эту форму, но намеренно не генерирует unified ID:
право создать его появится у будущего correlation/merge слоя после entity match.

### Можно ли понять что изменилось, почему и откуда доказательство?

В source contracts — да:

- что: TEXT `type/title`, GRAPHIC `type/summary/structural`;
- почему: TEXT `reason/details`, GRAPHIC `structural.relation`;
- откуда: TEXT pages/sheets/fragments, GRAPHIC blocks/regions/nodes/edges и
  extraction provenance.

Interop envelope без потерь связывает эти источники. Будущий unified project
change должен поверх него добавить canonical outcome/type/title/reason. В G2.4.1
это не реализовано, потому что выбор taxonomy и merge-смысла был явно оставлен на
следующий этап.

### Есть ли конфликты TEXT ↔ GRAPHIC?

Конфликт возможен, но **отсутствие подтверждения не является конфликтом**.
Противоречие существует только когда два валидных source claims относятся к
одной entity и одной оси изменения, но утверждают несовместимые outcomes.

## 3. Почему нельзя просто расширить Stage 5.3

Добавление `evidence_sources: ["TEXT", "GRAPHIC"]` синтаксически не сломает
permissive reader, но будет недостоверным без изменений validator:

1. Stage 5.3 source index знает только TEXT atomic evidence.
2. `details` имеют TEXT-specific форму.
3. TEXT `semantic_subject` (`principle`, `areas`, `parameters` и т.п.) не является
   graph entity identity.
4. Stage 5.3 confidence имеет ordinal lower-case scale без numeric calibration.
5. Его ID вычислен до появления graphic evidence.

Поэтому source artifact остаётся immutable/text-only, а объединение выполняется
additive layer поверх него. Это сохраняет существующие гарантии Stage 5.3.

## 4. Synthetic merge scenarios

### CASE 1 — резервирование + секционирование

TEXT `SYSTEM_OPERATION_CHANGED` и GRAPHIC `SYSTEM_BACKBONE_CHANGED` могут стать
одним material change, если entity bridge подтверждает одну систему и merge policy
устанавливает, что topology является графическим доказательством изменённого
принципа резервирования.

```text
unified change: upc_reservation
evidence: hlc_reservation + chg_sectioning
source: BOTH
```

Одной близости формулировок недостаточно: секционирование может измениться без
смены принципа резервирования.

### CASE 2 — новый объект

TEXT `SYSTEM_STRUCTURE_CHANGED`/`EQUIPMENT_OR_MATERIAL_CHANGED` и GRAPHIC
`NODE_ADDED` объединяются в один material change только при совпавшей entity,
локализации и роли. Тогда два source changes остаются отдельными evidence links
под одним unified ID.

Если GRAPHIC лишь молчит, TEXT claim не отклоняется автоматически. Если GRAPHIC
явно показывает высококачественный same-entity no-change, результат уходит в
review, а не выбирается голосованием.

### CASE 3 — детализация источника

TEXT `DETAIL_LEVEL_INCREASED` и GRAPHIC `DETAIL_LEVEL_INCREASED` с
`equivalence: representation_expansion` объединяются в один neutral/detail
result. Ни новые graph nodes, ни более длинное текстовое описание не становятся
новым объектом.

```text
unified change: upc_source_detail
outcome: DETAIL_ONLY
source: BOTH
```

### CASE 4 — оформление + no graphic change

TEXT formatting/service change вместе с отсутствием GRAPHIC change не публикуется
как project change. TEXT evidence остаётся в non-material/service audit trail;
unified project change не создаётся.

## 5. Предлагаемая taxonomy

Один плоский enum смешает значимость, масштаб и характер изменения. Для unified
layer предлагаются три ортогональные оси:

### Outcome

- `MATERIAL_CHANGE`;
- `DETAIL_ONLY`;
- `NO_PROJECT_CHANGE`;
- `REVIEW_REQUIRED`.

### Scope

- `PROJECT`;
- `SUBSYSTEM`;
- `COMPONENT`.

### Change family

- `DESIGN_PRINCIPLE`;
- `SYSTEM_OPERATION`;
- `SYSTEM_STRUCTURE`;
- `SPACE_PROGRAM`;
- `CALCULATION_APPROACH`;
- `PARAMETER_SET`;
- `EQUIPMENT_OR_MATERIAL`;
- `QUANTITY_OR_CAPACITY`;
- `DETAIL`.

Пример mapping:

| Source type | Unified family | Обычный scope |
|---|---|---|
| TEXT `DESIGN_PRINCIPLE_CHANGED` | `DESIGN_PRINCIPLE` | `PROJECT/SUBSYSTEM` |
| TEXT `SYSTEM_OPERATION_CHANGED` | `SYSTEM_OPERATION` | `SUBSYSTEM` |
| TEXT/GRAPHIC `DETAIL_LEVEL_INCREASED` | `DETAIL` | source-dependent |
| GRAPHIC `SYSTEM_BACKBONE_CHANGED` | `SYSTEM_STRUCTURE` | `SUBSYSTEM` |
| GRAPHIC `CONNECTION_CHANGED` | `SYSTEM_STRUCTURE` | `SUBSYSTEM/COMPONENT` |
| GRAPHIC `NODE_TYPE_CHANGED` | `EQUIPMENT_OR_MATERIAL` | `COMPONENT` |
| GRAPHIC `GROUP_COUNT_CHANGED` | `QUANTITY_OR_CAPACITY` | `SUBSYSTEM` |

Mapping является предложением и в коде G2.4.1 не реализован.

## 6. Conflict policy

### Основные правила

1. Сначала проверяется contract/quality каждого источника. Invalid evidence не
   участвует в merge.
2. Затем требуется entity match. Без него evidence остаются раздельными либо
   результат получает `REVIEW_REQUIRED`.
3. Silence/неприменимость modality не является отрицательным evidence.
4. Explicit agreement может corroborate claim.
5. Explicit contradiction нельзя разрешать одним confidence score или приоритетом
   всей modality; результат только `REVIEW_REQUIRED`.
6. `DETAIL_ONLY` никогда не повышается до material change из-за количества слов,
   узлов или evidence items.

### Частные случаи

| TEXT | GRAPHIC | Policy |
|---|---|---|
| добавлено оборудование | нет claim | TEXT single-source; не конфликт |
| изменён параметр | геометрия та же | совместимо: geometry не проверяет параметр |
| детализация | `DETAIL_LEVEL_INCREASED` | один neutral/detail result |
| детализация | certain `NODE_ADDED` той же entity | explicit conflict → review |
| material structure change | representation expansion той же entity | explicit conflict → review |
| нет TEXT mention | certain structural change | GRAPHIC single-source material candidate |
| оформление/service | no GRAPHIC change | no project change |

У TEXT выше компетентность для смысла, параметров и режима работы; у GRAPHIC — для
топологии, связей и визуально представленных объектов. Это локальная
source-competence policy, а не глобальный приоритет одного источника.

## 7. Confidence policy

Арифметическое среднее применять нельзя:

- TEXT `high/medium` и GRAPHIC numeric/enum не калиброваны на одной шкале;
- источники могут быть независимы по extraction, но коррелированы по исходному
  документу и entity resolution;
- высокий score не устраняет semantic contradiction.

Рекомендуемый v1 подход — ordinal evidence matrix с сохранением source scores:

```json
{
  "level": "HIGH",
  "basis": "CORROBORATED",
  "source_confidences": {
    "TEXT": {"level": "HIGH", "raw": null},
    "GRAPHIC": {"level": "HIGH", "raw": 0.85}
  },
  "entity_bridge": {"level": "HIGH"}
}
```

Правила:

- один валидный источник → его level + `SINGLE_SOURCE`, без штрафа за silence;
- `HIGH + HIGH`, agreement и HIGH entity match → `HIGH/CORROBORATED`;
- `HIGH + MEDIUM` → `HIGH` только при HIGH entity match, иначе `MEDIUM`;
- `MEDIUM + MEDIUM` → `MEDIUM`;
- LOW evidence не повышает более сильный источник;
- explicit conflict → `REVIEW_REQUIRED`, combined confidence `UNKNOWN`;
- entity ambiguity → не выше `MEDIUM` и `REVIEW_REQUIRED` для material claim.

Numeric combined score до cross-modal calibration не публиковать. Envelope
сохраняет `source_scale` и raw value, поэтому будущая versioned policy сможет быть
добавлена без потери исходных данных.

## 8. Provenance

Для ответа «почему система решила это» unified layer должен сохранить оба source
change целиком по ссылке и нормализовать адресацию:

### TEXT

- source high-level change ID и atomic evidence IDs;
- sheet groups, sheet labels, pages;
- left/right fragment IDs и anchors;
- before/after/reason;
- Stage 5.3 source signature, validator/prompt/model provenance.

### GRAPHIC

- Ledger change ID;
- block/page/region;
- graph node/edge IDs и source tokens;
- SYSTEM_GRAPH grounding evidence;
- graph profile, adapter, comparator и confidence-policy versions.

Новый envelope требует непустые `provenance`, `locations` и `source_ids` для
каждого source item и не копирует source claim в новую неоднозначную форму.

## 9. Entity bridge

Entity bridge **нужен**. TEXT `"ВРУ-А"`, graph canonical identity, label и node ID
не имеют общего гарантированного ключа. `semantic_subject` Stage 5.3 часто отражает
категорию (`principle`, `parameters`), а не объект.

Предлагаемый versioned bridge record:

```json
{
  "entity_ref_id": "...",
  "canonical_role": "...",
  "discipline": "...",
  "system": "...",
  "aliases": ["ВРУ-А", "Vru-A"],
  "text_refs": ["fragment-id"],
  "graphic_refs": ["node-id"],
  "locations": [],
  "match_method": "canonical_alias+role+scope",
  "confidence": 0.0,
  "status": "MATCHED | AMBIGUOUS | UNMATCHED"
}
```

Правила bridge:

- canonical aliases/designations + functional role + discipline/system scope;
- page/block proximity только supporting signal;
- bbox никогда не является identity;
- ambiguous match не объединяет changes автоматически;
- source entity refs остаются неизменными и трассируемыми.

Bridge в G2.4.1 не реализован.

## 10. Финальная архитектура

```text
Stage 5.3 high_level_project_changes.json (immutable TEXT)
                  ↓ text evidence normalizer
                  \
                   → unified-change-evidence.v1
                  /            ↓
GraphicChangeLedger v1/v2     entity bridge
        (immutable GRAPHIC)        ↓
                         correlation + conflict policy
                                   ↓
                    unified project change contract
                                   ↓
                                  UI
```

В G2.4.1 реализован только центральный interop-envelope и его validator/schema.
Text/graphic normalizers, entity bridge, correlation, taxonomy mapping, combined
confidence и unified project change producer остаются следующими отдельными
этапами.

## 11. Contract hardening и проверки

Добавлены:

- `unified_evidence_contract.py`;
- `unified_change_evidence.schema.json`;
- contract tests для old Stage 5.3, old Ledger v2, TEXT, GRAPHIC и BOTH;
- три mixed synthetic bundles: reservation/sectioning, added object и detail.

Source contracts не менялись. Проверки:

- G2.4.1 contract/synthetic compatibility: `11 passed`;
- весь Stage Comparison suite: `326 passed`;
- G2.3.1 comparator suite: `16 passed`;
- dense sectioned board profile: `9 passed`;
- объединённый regression gate: `351 passed`.

## 12. Готовность к UI

Source-specific UI может показывать TEXT и GRAPHIC результаты независимо уже
сейчас. UI, который обещает единый ответ «что реально изменилось между П и РД»,
пока начинать рано: для него обязательны entity bridge и deterministic
correlation/conflict policy.

Следующий безопасный этап — спроектировать unified project change contract и
entity bridge поверх добавленного evidence envelope, не встраивая graphic evidence
в Stage 5.3.
