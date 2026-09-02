# Selector Forensics — ИОС 2.1

Дата: 2026-09-02. Контур: read-only research. Production, Candidate Generator v4, engineer mappings и deploy не изменялись. Новый массовый experiment не запускался: сделано 14 узких replay-вызовов по одному и тому же ИОС 2.1 map-контексту, по два cold repeats на изменённый вариант. Для current/full/no-proximity повторно использованы шесть сохранённых вызовов repeat.

## Итог

Основная причина 17→27, 18→24 и 19→25 — не position bias. Это конфликт семантики задачи с evaluation label:

- сохранённые engineer RIGHT 7/8/9 являются авторитетными ссылками на физические страницы журнала изменений (`CHANGE_REGISTER`);
- предыдущий same-version forensic map прямо помечает их `functional_analogue: false`;
- AI не видит факт engineer acceptance и получает инструкцию выбирать `same engineering function, not same PDF page number`;
- функциональные графические аналоги 27/24/25 содержат совпадающие корпус, зоны, этажи, стояки и системы, тогда как 7/8/9 почти не имеют object/zone/topology.

Для LEFT 20 причина другая и структурная: текущий whole-map verifier запрещает пересечение `right_pages` между разными option IDs. Но RIGHT 26, 28 и 29 содержат несколько независимых function lineages, поэтому их повторное использование инженерно легитимно. Это `RIGHT_MAP_CONFLICT` по текущему sheet-level контракту, но ложный конфликт на function-level.

Рекомендация: **C — нужны оба изменения**. Нужны полный структурированный Function Passport и явное разделение `DOCUMENT_LINK`/`FUNCTIONAL_ANALOGUE`, а также hierarchical Function Lineage Matcher с проверкой занятости на уровне `(RIGHT sheet, function lineage)`, а не физической страницы целиком.

## 1. Сравнение паспортов

Ниже приведён компактный вид фактических v4 passports. Поля и расширенные выборки с source counts находятся в `critical_cases.json`.

### LEFT 17: engineer RIGHT 7, AI RIGHT 27

| Поле | LEFT 17 | Engineer RIGHT 7 | AI RIGHT 27 |
|---|---|---|---|
| function | WATER_SUPPLY, HOT_WATER, FIRE_WATER, RISER_DISTRIBUTION, METERING | WATER_SUPPLY, HOT_WATER, FIRE_WATER, извлечённые из change register | WATER_DRAINAGE, WATER_SUPPLY, HOT_WATER, FIRE_WATER, RISER_DISTRIBUTION, METERING |
| serviced_object | Корпус 2 | отсутствует | Корпус 2, секции 2.1/2.2 |
| corpus | Корпус 2 | отсутствует | Корпус 2 |
| zone | Корпус 2 | отсутствует | Корпус 2, секции 2.1/2.2 |
| floors | −9.600…+14.850, подземные уровни — 6 этаж | отсутствуют | −9.600…+14.850, этажи −2…6 |
| systems | В1, Т3, Т4; domestic + internal fire + hot-water circulation | текст реестра перечисляет В1/Т3/Т4, но без схемной локализации | ХВС/ГВС/противопожарные стояки, зональные обозначения |
| consumers | квартиры, МОП/служебные помещения, пожарные краны | одна строка общего описания систем | квартиры секций 2.1/2.2, МОП, встроенные помещения, fire branches |
| equipment_roles | счётчики, регуляторы, балансировочные/запорные клапаны, пожарные краны | отсутствуют | узлы учёта, компенсаторы, обратные клапаны, краны |
| upstream | автостоянка/технические уровни → стояки | отсутствует | подземные разводки → стояки до 6 этажа |
| downstream | квартиры и пожарные ответвления | отсутствует | секции 2.1/2.2 и этажные ветви |
| entities | Корпус 2, В1, Т3, Т4, ПК6/ПК8 и арматура | отсутствуют | Корпус 2, секции 2.1/2.2, квартиры, В11/Т3.1 и арматура |
| topology | вертикальная схема стояков | отсутствует | вертикальная схема стояков |
| stamp/title | graphic sheet 2, «Внутренние системы водоснабжения» | `CHANGE_REGISTER`, title «Лист», stamp Sheet 2 | `GRAPHIC_SHEET`, содержательный vision title корпуса 2 |
| neighbor context | [16,18] | [6,8] | [26,28] |
| TOC context | 5 refs, преимущественно sheet_number | 6 refs, преимущественно sheet_number | 6 refs, title_tokens |
| retrieval rank | — | 5; score 0.702672 | 2; score 0.938463 |
| retrieval channels | — | ENTITY, TITLE_STAMP, NEIGHBOR_TOC | FUNCTION, ENTITY, OBJECT_ZONE, TOPOLOGY |
| contradictions | — | нет | нет |

Почему AI предпочитает 27: это единственный из двух вариантов, который доказывает одну и ту же схемную функцию и корпус. RIGHT 7 доказывает документарную связь с номером листа 2, но такой relation type в selector contract отсутствует.

### LEFT 18: engineer RIGHT 8, AI RIGHT 24

| Поле | LEFT 18 | Engineer RIGHT 8 | AI RIGHT 24 |
|---|---|---|---|
| function | WATER_SUPPLY, HOT_WATER, RISER_DISTRIBUTION, METERING | WATER_SUPPLY, FIRE_WATER, PUMPING_PRESSURE, FIRE/DOMESTIC_PRESSURE_BOOST из change register | WATER_DRAINAGE, WATER_SUPPLY, FIRE_WATER, RISER_DISTRIBUTION, METERING |
| serviced_object | Корпус 3 | отсутствует | Корпуса 3 и 3.1 |
| corpus | Корпус 3 | отсутствует | Корпуса 3 и 3.1 |
| zone | Корпус 3 | отсутствует | Корпуса 3 и 3.1 |
| floors | −9.600…+14.850, 1–5 этажи | отсутствуют | −9.600…+11.850, 1–5 этажи |
| systems | Т1/Т3/Т4, ХВС/ГВС, стояки | change text о В2.1/В2.2 и насосных установках | ХВС/ГВС, улучшенная питьевая вода, противопожарные стояки |
| consumers | квартиры, коридоры, техпомещения | краткие строки реестра | квартиры/МОП корпусов 3/3.1, fire branches |
| equipment_roles | счётчики, фильтры, запорная/балансировочная арматура | насосные установки из перечня изменений | узлы учёта, счётчики, клапаны, регуляторы |
| upstream | подземные горизонтальные сети | отсутствует | подвал/автостоянка |
| downstream | стояки и квартирные ветви | одна pump-related строка | стояки корпусов 3/3.1 и квартирные узлы |
| entities | Корпус 3, Т1, Т3, Т4, этажи и арматура | отсутствуют | Корпуса 3/3.1, В11, Т3.1/Т4.1 и др. |
| topology | вертикальная схема стояков | отсутствует | вертикальная схема стояков |
| stamp/title | graphic sheet 3, «Внутренние системы водоснабжения» | `CHANGE_REGISTER`, «Лист», stamp Sheet 3 | `GRAPHIC_SHEET`, содержательный title корпусов 3/3.1 |
| neighbor context | [17,19] | [7,9] | [23,25] |
| TOC context | 5 sheet-number refs | 6 sheet-number refs | 5 title-token refs |
| retrieval rank | — | 2; score 0.899464 | 1; score 0.964312 |
| retrieval channels | — | TITLE_STAMP | FUNCTION, ENTITY, OBJECT_ZONE, TOPOLOGY |
| contradictions | — | нет | нет |

Почему AI предпочитает 24: exact корпус-3 riser topology и четыре общих function classes дают прямое функциональное доказательство. RIGHT 8 — документарная change-register запись другого уровня гранулярности.

### LEFT 19: engineer RIGHT 9, AI склоняется к RIGHT 25

| Поле | LEFT 19 | Engineer RIGHT 9 | AI RIGHT 25 |
|---|---|---|---|
| function | WATER_SUPPLY, HOT_WATER, RISER_DISTRIBUTION | WATER_SUPPLY, FIRE_WATER, PUMPING_PRESSURE, FIRE_PRESSURE_BOOST из change register | WATER_DRAINAGE, WATER_SUPPLY, HOT_WATER, FIRE_WATER, RISER_DISTRIBUTION, METERING, INCOMING_METERING |
| serviced_object | Корпус 4 | ошибочно извлечён Корпус 1 | Корпус 4 |
| corpus | Корпус 4 | Корпус 1 | Корпус 4 |
| zone | Корпус 4 | отсутствует | Корпус 4 |
| floors | −9.500…+51.000, 1–16 этажи | отсутствуют | −9.600…+51.000, 1–16 этажи |
| systems | domestic/hot water, стояки; old sheet также содержит fire context | change text о водоподготовке/пожарных насосах | domestic/hot/fire-related risers и узлы учёта |
| consumers | квартиры, коридоры, МОП, пожарные ответвления | отсутствуют | квартиры/МОП корпуса 4 |
| equipment_roles | счётчики, регуляторы, клапаны, пожарные краны | насосы из change register | счётчики, арматура, этажные узлы |
| upstream | ввод В1/ИТП/подземный уровень | отсутствует | автостоянка/технические уровни |
| downstream | корпус-4 стояки и квартирные ветви | отсутствует | корпус-4 стояки; fire-компонент конкурирует с RIGHT 30 |
| entities | Корпус 4, этажные отметки, линии | отсутствуют | Корпус 4, этажи, Т3.2/Т4.2/В1.* |
| topology | один combined vertical riser sheet | отсутствует | vertical riser sheet; отделённый компонент также представлен RIGHT 30 |
| stamp/title | graphic sheet 4, «Внутренние системы водоснабжения» | `CHANGE_REGISTER`, «Лист», stamp Sheet 4 | `GRAPHIC_SHEET`, содержательный title корпуса 4 |
| neighbor context | [18,20] | [8,10] | [24,26] |
| TOC context | 4 refs | 4 refs | 6 refs |
| retrieval rank | — | 4; score 0.717104 | 2; score 0.763906 |
| retrieval channels | — | TITLE_STAMP | FUNCTION, ENTITY, OBJECT_ZONE, TOPOLOGY, TITLE_STAMP |
| contradictions | — | `INCOMPATIBLE_CORPUS`: LEFT 4 vs RIGHT 1 | нет |

Почему AI не выбирает 9: это единственный critical engineer candidate с явной corpus contradiction. Нестабильность относится не к 9, а к корректной гранулярности функционального продолжения: current full context даёт RIGHT 25 в 4/6 вызовов и группу [25,30] в 2/6.

### LEFT 20 / old graphic sheet 5: target [26,28,29]

У этого case нет stable AI-selected candidate. В current full context локальный выбор был target [26,28,29] в 5/6 вызовов, но map choice сохранил его только в 2/6; в 4/6 был `NEED_MORE_EVIDENCE`. Поэтому в третьей колонке показаны observed AI outcomes, а не вымышленный стабильный паспорт.

| Поле | LEFT 20 | Engineer target RIGHT [26,28,29] | Observed AI outcome / competitor |
|---|---|---|---|
| function | WATER_SUPPLY, FIRE_WATER, PUMPING_PRESSURE, METERING, DOMESTIC_PRESSURE_BOOST, FIRE_PRESSURE_BOOST, INCOMING_METERING | RIGHT 26: domestic boost; 28: fire boost; 29: incoming/common metering, плюс сопутствующие sheet functions | current dominant local = target; structured ablations часто выбрали [25,26,28] |
| serviced_object | пусто в generated passport | секции корпуса 1, Корпус 1, помещение водомерного узла | competitor добавляет Корпус 4 с RIGHT 25, что не относится к old pump sheet |
| corpus | пусто | corpus-1 + complex-wide meter room | competitor смешивает corpus 4 и corpus 1 |
| zone | пусто | pump/riser zones корпуса 1 + meter room | та же ошибочная смесь |
| floors | не извлечены | технические/подземные уровни и meter-room отметка | RIGHT 25 вносит этажи корпуса 4 |
| systems | В1, АПТ hand-off, ВПВ, metering | domestic water/boost, fire water/boost, incoming metering | competitor покрывает классы численно, но подменяет metering page 29 на corpus-4 riser page 25 |
| consumers | domestic network, ВПВ, APT boundary, но в passport это одна шумная entity-строка | domestic users, fire headers, downstream networks | coverage count выглядит полным, functional role неверна |
| equipment_roles | общий meter ВСХнд-40, domestic/fire boosters, tanks, filters, valves | 26 pumps; 28 fire pumps; 29 ВСХНд-65 meter/bypass | RIGHT 25 в основном riser/meter nodes, не common incoming meter lineage |
| upstream | два ввода В1 2Ø150 | RIGHT 29 revised PE225 inputs; 26/28 downstream split | competitor не восстанавливает common incoming-meter component |
| downstream | domestic risers, fire risers, APT boundary | 26 domestic, 28 fire, 29 shared input/meter chain | map-stage конфликтует с LEFT 16 и 21 |
| entities | В1, насосные станции, водомерный узел, APT/ВПВ | pump equipment, fire lines, meter node | full lists очень шумны: target aggregate содержит 148 unique entities |
| topology | input → common meter → domestic/fire boost → consumers | разложено на three component sheets | [25,26,28] имеет высокий class coverage, но неверную component topology |
| stamp/title | graphic sheet 5; «Внутренние системы водоснабжения» | RIGHT 26 water/boost, 28 fire/boost, 29 meter node | current parser не извлёк graphic numbers из stamps этих pages; vision titles содержательны |
| neighbor context | [19,21] | 26→[25,27], 28→[27,29], 29→[28,30] | sequence proximity само по себе не доказывает lineage |
| TOC context | 4 sheet-number refs | 6 unique title refs aggregate | не исправляет group choice |
| retrieval rank | — | group shortlist rank 10; group_score 1.073225 | competitor [25,26,28] rank 1; group_score 0.832941 |
| retrieval channels | — | FUNCTION_GROUP_SHORTLIST | FUNCTION_GROUP_SHORTLIST |
| contradictions | — | нет explicit contradictions | нет explicit contradictions, несмотря на corpus-4 contamination |

Почему возникает ошибка: target имеет только `function_coverage_count=3/7` и rank 10, тогда как [25,26,28] получает 7/7 class coverage и rank 1. Это локальный паспортный дефект. Даже когда модель правильно выбирает target, глобальная page-exclusivity конфликтует с уже верными LEFT 16→[26,28] и LEFT 21→29.

## 2. Фактический model input

`prompt_inputs.jsonl` содержит два input records:

1. byte-exact deterministic regeneration текущего TEXT prompt; signature `7fd29a113237353ee27eb768fbf7e561b70e512adb0f823e60fb35bcc320de48` совпадает со всеми шестью сохранёнными TEXT calls;
2. byte-exact text regeneration VISION_FALLBACK prompt; signature и image manifest совпадают с шестью вызовами. Исходные ephemeral JPEG bytes repeat не сохранял, поэтому в JSONL сохранены exact text и manifest, а не бинарные изображения.

TEXT input содержит 427 466 символов:

| Секция | Количество / serialized chars |
|---|---:|
| tasks | 7 |
| page candidates | 70 |
| group candidates | 64 |
| LEFT / RIGHT sheet passports | 7 / 23 |
| LEFT / RIGHT function passports | 29 / 105 |
| contents_context | 7 283 chars |
| sheet_passports | 112 037 chars |
| function_passports | 114 294 chars |
| page_candidates | 108 227 chars |
| group_candidates | 72 291 chars |
| tasks | 10 660 chars |

Проверки input:

- Engineer candidates присутствуют. RIGHT 7 стоит 5-м для LEFT 17; RIGHT 8 — 2-м для LEFT 18; RIGHT 9 — 4-м для LEFT 19. Target [26,28,29] стоит 10-м среди group candidates LEFT 20.
- Кандидаты описаны одной схемой, но не одинаково информативно. `CHANGE_REGISTER` pages объективно не имеют object/zone/floors/entities/topology, а graphic pages имеют.
- `object_corpus` и `zone` не исчезают полностью: они находятся в `sheet_passports`. Но фактический `function_passports` показывает только `function_id`, refs, `function_class` и `fragment_text`.
- Богатые v4 поля `serviced_object`, `serviced_zone`, `systems`, `consumers`, `equipment_roles`, `upstream`, `downstream`, `neighboring_function_context` существуют, но удаляются проекцией до AI.
- Context дублируется: одни OCR/vision excerpts повторяются в sheet consumers/topology/title и в нескольких function `fragment_text`. 105 RIGHT function rows на 23 sheets многократно повторяют одно sheet evidence.
- Numeric rank и scores показаны явно; page proximity отсутствует и в payload, и в prompt. Поэтому вариант «без proximity» byte-identical текущему.
- Engineer acceptance намеренно не раскрывается. Модель не может отличить «authoritative documentary link» от обычного weak candidate.

## 3. Rank / order bias

Изменённые варианты имеют два cold repeats; current/no-proximity используют шесть сохранённых calls. Options, IDs и evidence не менялись.

| Variant | LEFT 17 | LEFT 18 | LEFT 19 | LEFT 20 local → map | Mean total tokens/call |
|---|---|---|---|---|---:|
| A current, n=6 | 27, 6/6 | 24, 6/6 | 25: 4/6; [25,30]: 2/6 | target: 5/6 → target 2/6, NME 4/6 | 164 178 |
| B reverse, n=2 | 27, 2/2 | 24, 2/2 | 25: 1/2; [25,30]: 1/2 | target 2/2 → target 1/2, NME 1/2 | 165 214 |
| C randomized, n=2 | 27, 2/2 | 24, 2/2 | [25,30], 2/2 | target 2/2 → target 1/2, NME 1/2 | 164 427 |
| D no numeric scores, n=2 | 27, 2/2 | 24, 2/2 | 25, 2/2 | target 2/2 → target 1/2, NME 1/2 | 158 821 |
| E no page proximity | identical A | identical A | identical A | identical A | 164 178 |

Вывод: order/score не заставляют модель выбирать 27/24 вместо 7/8. Они влияют на cardinality LEFT 19 и на то, какой конфликтующий map вариант будет снят для LEFT 20. Numeric score removal экономит около 3.3% input tokens, но не исправляет target behavior.

## 4. Context ablation

Structural option IDs, exact candidate sets и local→map procedure сохранены. «B only» означает только перечисленные evidence fields; contract/task/group structure остаётся обязательной частью input. Stability для новых вариантов — доля modal choice в двух calls.

| Context | LEFT 17 | LEFT 18 | LEFT 19 local → map | LEFT 20 local → map | Mean total tokens/call | Amortized total/task |
|---|---|---|---|---|---:|---:|
| A current full, n=6 | 27, 100% | 24, 100% | 25 67%; [25,30] 33% | target 83% → target 33%, NME 67% | 164 178 | 23 454 |
| B function/object/zone/systems/consumers/up/down, n=2 | 27, 100% | 24, 100% | 25, 100% | [25,26,28] 100% → NME 100% | 117 997 | 16 857 |
| C B + entities/topology, n=2 | 27, 100% | 24, 100% | 25 → 30, 100% | [25,26,28] → same, 100% | 124 925 | 17 846 |
| D B + neighbor/TOC, n=2 | 27, 100% | 24, 100% | 30, 100% | [25,26,28] → NME, 100% | 121 991 | 17 427 |
| E B + vision, n=2 | 27, 100% | 24, 100% | 25, 100% | NME/target 50/50 → NME 100% | 175 886 | 25 127 |

Минимальный стабильный контекст для обычных functional sheet choices 17–19 — B: он на 28.1% дешевле full context по total tokens/call и стабилен. Но он не выполняет заданный engineer target 7/8/9, потому что эти targets относятся к другому relation type. И ни один ablation не даёт stable target map для LEFT 20.

Entities/topology и neighbor/TOC не повышают correctness target group: они помогают модели перераспределять занятые pages и тем самым маскируют неверную глобальную constraint. Vision стоит на 49.1% дороже B и не проходит LEFT 20 map-stage.

## 5. FUNCTION_DISTRIBUTED conflict

RIGHT pages уже занимают:

- LEFT 16 → `fcand_40b2fb5e47ddc3fd7581` → [26,28], 6/6 saved TEXT calls. Причина: old corpus-1 sheet объединяет domestic/hot и fire; new version разделяет компоненты между 26 и 28.
- LEFT 21 → `vcand_4e814c898ed83527b6ce` → [29], 6/6. Причина: прямое продолжение детального common meter node.

Когда LEFT 20 выбрал [26,28,29] как map choice в saved calls 2B и 3B, verifier поставил `RIGHT_MAP_CONFLICT` одновременно LEFT 16, 20 и 21.

Это не просто неправильный порядок. Verifier проверяет все пары options после построения полного map, поэтому reverse/random order не меняет математическую несовместимость. Настоящая проблема — гранулярность capacity constraint:

- sheet-level: конфликт настоящий по текущему контракту;
- function-level: конфликт ложный, потому что 26 и 28 содержат и corpus-riser lineages, и pump lineages, а 29 продолжает metering overview LEFT 20 и metering detail LEFT 21.

Research simulation `FUNCTION LINEAGE FIRST → SHEET MAP WITHIN LINEAGE` принимает LEFT 20→[26,28,29] без удаления LEFT 16/21. Полная трасса находится в `group_conflict.json`. Результат не материализован.

## 6. Hierarchical matching hypothesis

Архитектура подробно описана в `hierarchical_matching.md`.

Короткий verdict:

- LEFT 20 conflict — conceptually устраняется;
- LEFT 19 split 25/30 — становится явным lineage decomposition вместо нестабильного sheet choice;
- LEFT 17/18 — functional lineages будут по-прежнему вести к 27/24, что технически согласуется с prompt;
- чтобы воспроизводить authoritative engineer links 7/8/9, нужен отдельный `DOCUMENT_LINK` objective или relation namespace. Hierarchy сама по себе не должна превращать change-register page в functional analogue.

## 7. Cost

Исходный repeat: 7 315 563 tokens, 36 calls по трём проектам и TEXT/VISION arms.

Новый forensic replay: 14 calls, 2 058 520 tokens total. Это не новый benchmark repeat: вызовы ограничены одним frozen pair и четырьмя critical cases, которые оценивались внутри неизменного seven-task map context.

Для ablation tokens/case приведены как amortized tokens на одну из семи selector tasks одного bundled map call. Это честное деление общей model telemetry; shared map context нельзя точно приписать одному case.

| Ablation | Mean input/call | Mean total/call | Amortized input/case | Amortized total/case | Decision stability |
|---|---:|---:|---:|---:|---|
| A full | 161 989 | 164 178 | 23 141 | 23 454 | 17/18 stable; 19 and 20 unstable |
| B function core | 116 042 | 117 997 | 16 577 | 16 857 | stable, but LEFT 20 wrong local group |
| C + entities/topology | 122 971 | 124 925 | 17 567 | 17 846 | stable, but global reassignment and wrong LEFT 20 group |
| D + neighbor/TOC | 119 934 | 121 991 | 17 133 | 17 427 | stable, but LEFT 19→30 and LEFT 20 unresolved |
| E + vision | 174 123 | 175 886 | 24 875 | 25 127 | LEFT 20 local unstable, map unresolved |

Практический minimum: B для ordinary within-lineage sheet selection. Для distributed cases minimum пока не найден: нужен не больший prompt, а другая function-level representation/constraint; после этого стоит отдельно проверить компактный B + structured equipment/component roles.

## 8. Root causes и решение

- **17→27:** engineer RIGHT 7 — authoritative change-register link, но не functional analogue. Selector не видит authority и следует functional objective; RIGHT 27 имеет exact corpus-2/function/topology evidence. Rank/order/score не являются причиной.
- **18→24:** тот же objective/label mismatch; RIGHT 24 имеет exact corpus-3 riser evidence, RIGHT 8 — change register без object/zone/topology. Evidence asymmetry сильнее небольшой rank difference.
- **LEFT 19:** RIGHT 9 — non-functional change-register link плюс ошибочно извлечённый corpus 1 и explicit contradiction. Реальная нестабильность — 25 против distributed [25,30], потому что old combined corpus-4 function разделена в новой версии.
- **Distributed group conflict:** LEFT 16 законно использует 26/28, LEFT 21 законно использует 29; current page-global exclusivity считает повторное function-level использование незаконным и снимает LEFT 20 target.

Финальная рекомендация: **C — оба изменения**.

1. Function Passport / contract: передавать структурированные serviced object, zone, systems, consumers, equipment roles, upstream/downstream и component coverage; убрать повторяющиеся raw excerpts; явно разделить `DOCUMENT_LINK` и `FUNCTIONAL_ANALOGUE`.
2. Architecture: сначала строить many-to-many Function Lineage Map, затем sheet map внутри lineage; конфликтовать только одинаковую function capacity, а не весь RIGHT sheet.

## Артефакты

- `critical_cases.json` — rich passport comparisons, actual projection audit и per-case root causes;
- `prompt_inputs.jsonl` — exact regenerated TEXT/VISION prompt text и manifests;
- `order_bias.json` — все responses, telemetry, verification и stability для order/score controls;
- `context_ablation.json` — все responses, telemetry, verification и stability для context variants;
- `group_conflict.json` — occupants, verifier errors и lineage-first simulation;
- `hierarchical_matching.md` — architecture assessment.
