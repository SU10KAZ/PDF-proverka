# Исследование VectorBlockDescription для будущего П ↔ РД comparison

## Короткий ответ

Для **векторных** блоков с корректно сопоставленными границами уже можно строить полезное data-first описание и сравнивать геометрию преимущественно без raster. На 20 реальных блоках / 10 ручных парах из СС, АР, ВК и ЭОМ верхнеуровневый deterministic verdict оказался корректным в 8 случаях и частично корректным в 2; полностью ошибочных решений не было.

Но универсальной заменой Vision подход пока не является. Половина блоков достигла явных caps плотности, в трёх парах ВК vector text оказался недекодируемым из-за embedded-font mapping, а неполное/разное кадрирование блока нельзя исправить одной bbox-нормализацией. На пяти парах сильная модель с raster была немного точнее и полнее (81/100 по исследовательской шкале против 79/100), тогда как vector+diff был существенно проверяемее.

Итоговая рекомендация — **B: подход работает только для некоторых типов блоков; нужна гибридная архитектура Vector + Vision**. Ничего из эксперимента в production не подключено.

## 1. Границы и метод исследования

Эксперимент изолирован в `experiments/stage_comparison_vector_blocks/`. Он не импортирует production comparison, не меняет sheet/block matcher и не касается Stage 3/4/5, text comparison, AI reviewer, `project_change_summary`, UI или `sheet_links`.

Вход каждой стороны пары задан вручную в [`artifacts/block_pairs.json`](artifacts/block_pairs.json): PDF, нулевая страница, block id и нормализованный bbox. Пары взяты из реальных соседних версий документов корпуса. Старые `PreparedDocument`, comparison blocks, ORB, affine pipeline, change regions, entities, Pipeline V2 и semantic diff не использовались.

Core extractor читает только:

- `fitz.Page.get_drawings()`;
- vector text spans с bbox, rotation, font и font size;
- bbox или polygon блока.

Raster crops созданы после извлечения только для человеческой проверки и отдельного, явно размеченного Vision-arm. OCR, Vision, embeddings и image similarity в `extractor.py`/`comparator.py` отсутствуют.

## 2. Что уже умеет текущий «вектограф»

Аудит охватил production-код, тесты, старые эксперименты и корпусные исследования, в частности [`docs/vectograf.md`](../../docs/vectograf.md), [`docs/profiled-vector-graphs.md`](../../docs/profiled-vector-graphs.md), [`singleline_graph_geometry.py`](../../backend/app/pipeline/stages/block_grounding/singleline_graph_geometry.py), [`vector_path_graph.py`](../../backend/app/pipeline/stages/block_grounding/vector_path_graph.py), [`singleline_graph_algorithm.md`](../../docs/singleline_graph_algorithm.md), [`ДИЗАЙН_обобщение_Вектографа.md`](../../docs/graphic_anchors/ДИЗАЙН_обобщение_Вектографа.md) и [`ЭМПИРИКА_полный_корпус.md`](../../docs/graphic_anchors/ЭМПИРИКА_полный_корпус.md).

Текущий вектограф — не один универсальный geometry extractor, а профильный каскад:

1. ЭОМ single-line слой находит реальную страницу, клипует слова по bbox/polygon, строит X-колонки фидеров и связывает панель, QF/QD, номинал/полюса, кабель, нагрузку, шины, вводы/АВР и счётчики.
2. Текстовые и геометрические факты проходят детерминированные проверки физики, coverage/conflict/readiness и honesty gate.
3. Для ВК, ОВ, АР, КЖ/КМ, ГП, ТХ, СС существуют отдельные профили и роутинг; результат — профильный graph + validation + Markdown/user text.
4. `vector_path_graph.py` даёт общий path-backbone: линии, часть `C`/`QU`, endpoint/T-connectivity, components и отдельные специальные связи. Но он не является полным универсальным описанием: line styles/fill почти не участвуют, X-crossings намеренно не соединяются, curves/ellipses и повторные мотивы покрыты частично.

Сильная сторона существующего решения — не количество regex, а дисциплина доказательств: строгий clip, координатная привязка, provenance, layered facts, validation/gates и fail-soft/fail-closed поведение. Обобщённый `graphic_primitives` слой подробно спроектирован в документах, но как единый production-модуль ещё не реализован.

## 3. Что переиспользовано, а что нельзя переносить напрямую

Переиспользованы как принципы:

- fail-closed bbox/polygon clip;
- сохранение raw coordinates вместе с block-relative coordinates;
- endpoint graph, T-junctions, connected components, contours и честное отношение к X-crossings;
- proximity anchors «text → nearest geometry» только как candidate, не как семантический факт;
- exact/normalized/structural signatures;
- provenance, caps, quality labels, ambiguities и Markdown только из фактических данных;
- разделение raw facts, grouped topology и компактной проекции.

Не перенесены:

- regex и физика конкретных дисциплин;
- понятия «щит», «фидер», «стояк», «комната», «дверь» и другие специальные entity classes;
- 20+ правил и validation конкретного ЭОМ/ВК профиля;
- production router/reference catalog/user prompt;
- поиск/сопоставление листа или блока.

Причина проста: универсальный слой должен одинаково описывать план, схему, узел и таблицу, не выдавая linework за объект конкретной дисциплины. Специализация полезна только после базового geometry graph.

## 4. Экспериментальный VectorBlockDescription

Контракт намеренно имеет версию `vector-block-research-v0.1`, а не объявлен production API. В одном JSON хранятся:

| Слой | Фактические данные |
|---|---|
| identity/source | block id, page, raw/normalized bbox, optional polygon, PDF hash, page size, использованные/исключённые sources |
| geometry | `line`, `polyline`, `rectangle`, `curve`, `circle`, `ellipse`, `filled_polygon`, generic `path`; raw и normalized segments/bbox, length, angle, stroke/fill/width/dashes/cap/join/opacity/layer, closed и segment count |
| text | точный cosmetic-normalized span, raw/normalized bbox, center, rotation, font size/font, candidate category |
| anchors | nearest-geometry candidate, distance и confidence |
| topology | nodes/edges counts, endpoints, branch points, T-junctions, unconnected X-crossings, components, closed/nested contours, degree histogram |
| patterns | path-shape fingerprints и instance bboxes; отдельные parallel-segment `hatch_like` candidates |
| values/labels | engineering-value/dimension candidates и residual labels без доменной интерпретации |
| signatures | exact raw vector, normalized geometry+text, coarse structural/topological |
| honesty | `GOOD`, `LIMITED`, `LIMITED_CAPPED`, `VECTOR_DATA_INSUFFICIENT`, caps и ambiguities |
| size | Level 0 raw, Level 1 normalized, Level 2 grouped/topology и Level 3 compact bytes/pretty lines/token estimate |

Нормализация ровно задана формулой `x_norm=(x-block_x0)/block_width`, `y_norm=(y-block_y0)/block_height`. Она не выполняет affine warp и не может «натянуть» изменившуюся стену или неверный bbox на другую версию.

PDF paths с множеством line commands сохраняются как один path, но comparator сопоставляет их сегменты независимо от порядка и упаковки. Это устраняет ложное различие между одинаковой графикой, записанной как `l`, `re` или составной path.

## 5. Общая геометрия и топология: что получилось

Extractor детерминированно восстанавливает все запрошенные базовые классы, включая sampled Bezier curves и circle/ellipse, если набор cubic commands согласуется с эллипсом. Filled paths сохраняют fill/stroke, parallel clusters помечаются как hatch-like candidates без заявления «это штриховка».

Topology строится в block-relative coordinates:

- endpoints склеиваются только в пределах tolerance;
- endpoint, лежащий на другом сегменте, создаёт T-junction;
- чистое X-пересечение записывается, но не становится связью;
- union-find даёт components; degree histogram — endpoints/branches;
- closed paths и bbox nesting дают contour candidates.

Это полезнее простого «428 линий», но ещё не даёт универсальный инженерный маршрут уровня «ввод → автомат → шина». Для него нужны symbol grouping и профильная семантика поверх базового графа. На плотных CAD-листах hatch, мебель и тонкие background-линии также перегружают node graph; поэтому caps явны и качество понижается до `LIMITED_CAPPED`.

## 6. Comparator и эксперимент tolerances

Comparator отдельно сравнивает:

1. exact signatures;
2. spatial coverage нормализованных сегментов в обоих направлениях;
3. primitive packing как diagnostic, но не как главный score;
4. topology counts;
5. exact text multiset и character-stream similarity;
6. repeated patterns;
7. подозрение на encoding rewrite (`l` стабилен, `re` меняется, endpoints стабильны).

Undecodable text определяется по control-character ratio. Если обе стороны ВК имеют сломанный embedded-font mapping, text diff сохраняется как evidence низкого качества, но не управляет статусом. OCR для «починки» не вызывается.

Реальные результаты tolerance sweep:

| Tolerance | Mean geometry coverage | Median | Пар с coverage ≥ 0.985 |
|---:|---:|---:|---:|
| 0.10% | 0.411 | 0.269 | 1 / 10 |
| 0.25% | 0.666 | 0.745 | 2 / 10 |
| 0.50% | 0.834 | 0.958 | 5 / 10 |
| 1.00% | 0.902 | 0.997 | 8 / 10 |

Вывод: одного глобального порога нет. 0.1–0.25% слишком чувствительны к PDF precision и bbox noise; 1% полезен для guard-railed near-identical, но сам по себе может скрыть реальный небольшой сдвиг. Для следующего этапа разумна лестница: 0.25% exact-ish → 0.5% normal near → 1% только при согласованных topology/text и стабильном bbox. Текущие thresholds — исследовательские и не являются production policy.

## 7. Реальный benchmark и визуальная верификация

Все 10 пар заданы явно; автоматический matcher не строился. Выборка покрывает простой и сложный блок, планы, схемы, узлы, таблицу+графику, text-heavy blocks, repeated symbols, changed text, added/expanded branches, перенос на другую страницу и небольшие bbox/scale shifts.

| Pair | Type | Vector quality | Comparator verdict | Human verdict | Correct? |
|---|---|---|---|---|---|
| ss_scheme_text_changed | СС схема, text-heavy, повторы | GOOD | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_SAME_VALUES_CHANGED | CORRECT |
| ss_plan_dense | сложный плотный план | LIMITED_CAPPED | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ss_simple_node | простой узел | GOOD | IDENTICAL | IDENTICAL | CORRECT |
| ss_table_graphic | таблица + узел камеры | GOOD | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan | архитектурный план, повторы | LIMITED_CAPPED | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_wall_sections | повторяющиеся сечения | LIMITED_CAPPED | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_plan | план ВК | GOOD | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_nodes | два узла/системы ВК, text-heavy | LIMITED_CAPPED | NEAR_IDENTICAL | STRUCTURE_SAME_VALUES_CHANGED | PARTIALLY_CORRECT |
| vk_node_plan | смешанный узел + план | LIMITED_CAPPED | NEAR_IDENTICAL | NEAR_IDENTICAL | PARTIALLY_CORRECT |
| eom_singleline_changed | ЭОМ single-line, таблица + графика | GOOD | STRUCTURE_CHANGED | STRUCTURE_CHANGED | CORRECT |

Статистика comparator: **1 IDENTICAL, 7 NEAR_IDENTICAL, 1 STRUCTURE_SAME_VALUES_CHANGED, 1 STRUCTURE_CHANGED, 0 INSUFFICIENT_VECTOR_DATA**. Визуальная оценка: **8 correct, 2 partial, 0 wrong**.

Два partial важны:

- `vk_nodes`: основные системы геометрически совпадают, но RIGHT включает notes и отметку `−0,034`; vector text недекодируем, поэтому comparator честно не подтвердил изменение.
- `vk_node_plan`: geometry verdict верен, но равенство подписей/значений доказать по сломанному vector text нельзя.

`VECTOR_DATA_INSUFFICIENT` реализован и покрыт тестом на блоке без полезной геометрии, но среди отобранных 20 реальных векторных блоков таких не было; поэтому его benchmark count равен нулю.

## 8. Планы, схемы и узлы

**Планы.** Лучше всего сравниваются при одинаковой семантической границе bbox. AR plan дал geometry 0.9999 и topology 0.9987; dense SS потребовал 1% из-за crop height/precision. Большие планы быстро достигают caps, а фон и hatch конкурируют с инженерными линиями.

**Схемы.** СС схема показала главную пользу: layout сохранился, а `2.1 → 1.1`, связанные camera ids и помещение изменились. Дополнительные `re` commands в одной версии оказались иной PDF-упаковкой; directional coverage и стабильные endpoints позволили не объявить ложную structural change. ЭОМ схема дала сильный реальный change: обобщённые первая/n-я цепи заменены четырьмя QD/Wh/QF ветвями.

**Узлы.** Простой узел получился exact. Сложные ВК узлы геометрически сравнимы, но зависят от текста и crop. Если font mapping сломан, vector-only может подтвердить linework, но не значения и notes.

**Таблица + графика.** Геометрия таблицы/узла стабильна, но clipping меняет разбиение spans. Exact span diff создаёт шум даже при визуально одинаковой строке; нужны span-to-line reconstruction и table-cell grouping до LLM.

## 9. Какие изменения ловятся хорошо и какие — плохо

Хорошо:

- exact identity;
- одинаковый block-relative linework при другом положении bbox на странице;
- добавленные/удалённые крупные segments и branches;
- стабильная topology/components;
- точные изменённые значения при хорошем vector text;
- повторный path motif count;
- различия PDF command order/path packaging без ложного изменения.

Плохо или пока неоднозначно:

- реальное изменение только stroke/fill/style: style сохраняется, но основной segment score пока почти не штрафует его;
- маленькие symbols среди десятков тысяч CAD primitives при comparator cap;
- текст, разбитый clip-границей на разные spans;
- outlined text, Type3/кастомные fonts и недекодируемые character maps;
- semantic grouping множества отдельных line drawings в один символ;
- X-crossing: без junction evidence невозможно универсально решить, связан он или нет;
- вложенность произвольных curves и filled shapes только по bbox — approximation;
- неверный pair или разные semantic crop extents: normalization не должна и не может это скрывать.

## 10. Размеры и многоуровневое представление

Метрики рассчитаны на 20 блоках. Bytes/token estimates используют compact JSON; lines показывают число строк pretty JSON.

| Level | Содержимое | Bytes min / median / mean / max | Estimated tokens median / max |
|---|---|---:|---:|
| 0 | raw primitives | 2,563 / 2,880,581 / 3,231,253 / 6,988,279 | 720,146 / 1,747,070 |
| 1 | normalized primitives | 2,262 / 2,433,851 / 3,077,049 / 6,842,270 | 608,464 / 1,710,568 |
| 2 | groups/topology/anchors/patterns | 8,600 / 58,818 / 75,918 / 170,410 | 14,705 / 42,513 |
| 3 | compact summary for AI | 1,516 / 8,133 / 8,953 / 19,078 | 1,946 / 4,680 |

Суммарно Level 0 занял 64,625,058 bytes, Level 2 — 1,518,355, Level 3 — 179,060. Median raw/compact reduction — **204×**, aggregate — **361×**. Это подтверждает многоуровневую гипотезу: comparator должен работать на Level 1–2, а LLM — только на существенно очищенном Level 3 + коротком diff.

Важно: Level 1 почти не меньше raw, потому что нормализованные координаты сами по себе не являются compression. Level 2 уже полезен для аудита, но median ~14.7k estimated tokens на блок всё ещё велик. Level 3 проходит требование «не 100k tokens», однако текущий prompt добавляет обе стороны и подробный diff, что снова раздувает контекст.

## 11. Производительность

Полный прогон на финальном коде:

- extraction 20 блоков: **123.61 s** (~6.18 s/block в среднем);
- comparison 10 пар: **37.93 s** (~3.79 s/pair);
- простые блоки: десятки/сотни миллисекунд;
- плотные АР/ВК страницы: десятки секунд.

Главный bottleneck — повторный `page.get_drawings()` всей тяжёлой CAD-страницы, а не bbox filtering. Второй — четыре tolerance runs на capped segment samples. Следующий дизайн обязан иметь page-level cache `PDF hash + page index → drawings/text`, а блоки должны только clip/filter уже разобранный page payload.

## 12. Vision против VectorDescription + LLM

На пяти одинаковых парах один и тот же `gpt-5.6-sol` получил либо 10 raster crops, либо только Level 3 + deterministic diff. Вызовы были изолированы в `/tmp`, read-only, без доступа к репозиторию. Exact prompts/outputs сохранены в [`artifacts/ai_experiment`](artifacts/ai_experiment/).

| Pair | Vision | Vector + diff | Лучше |
|---|---|---|---|
| ss_scheme_text_changed | точные label changes | те же changes + трассируемые counts | tie |
| ss_table_graphic | верно: крупных изменений нет | false positive «добавлена позиция 1» из span/crop noise | Vision |
| ar_plan | почти верно, но мелкий текст не читается | near identity с численными evidence/caps | Vector |
| vk_nodes | нашёл notes и `−0,034`, но завысил класс | доказал geometry, пропустил annotations | Vision |
| eom_singleline_changed | верно описал четыре ветви | столь же верно и проверяемо | tie |

Исследовательская оценка: Vision **81/100**, Vector+diff **79/100**. Vision выиграл accuracy/completeness, vector — verifiability. Transport payload vector-arm был примерно 145 KB против 3.10 MB PNG (21× меньше), но сам вызов сообщил **70,631 tokens** против **38,069** у Vision. Значит, текущий Level 3 + diff слишком многословен: hashes, длинные text lists и repeated-pattern diagnostics следует передавать on demand, а не всегда.

AI не нужен для low-level segment/text/topology diff: там deterministic output точнее и дешевле. AI полезен для:

- формулировки нескольких крупных изменений;
- объединения line/text/pattern evidence;
- осторожной инженерной интерпретации после gates;
- raster fallback, если vector evidence отсутствует или text undecodable.

## 13. Минимальный обязательный набор и что можно выбросить

Минимум для будущего data-first comparison:

1. page/block provenance, bbox/polygon и quality/cap flags;
2. normalized segment geometry + style summary;
3. endpoint/T topology, components, closed contours и явная политика X-crossings;
4. exact text с normalized bbox и text-layer quality;
5. text-to-geometry anchor candidates;
6. repeated motif/hatch candidates с counts и instance positions;
7. multi-level signatures;
8. deterministic diff с directional coverage и uncertainty.

Можно не передавать LLM постоянно:

- raw page coordinates после comparator;
- все segment endpoints;
- `matches_sample` и unmatched ids;
- cryptographic hashes без события mismatch;
- font/style details, если они не изменились;
- полные component memberships;
- все labels — достаточно changed/added/removed и небольшой context window.

Raw/normalized layers нужно сохранять как machine artifact или cache, а не помещать в prompt.

## 14. Предложение следующей архитектуры (не реализовано)

```text
page-level vector cache
        ↓
block clip + VectorBlockDescription L0/L1
        ↓
quality/text/cap gates ───────────────┐
        ↓                             │ insufficient / undecodable
structural signature candidate search│
        ↓                             ↓
manual/automatic candidate shortlist  Vision fallback
        ↓                             │
L1 segment + L2 topology/text comparator
        ↓
deterministic evidence ledger
        ↓
compact L3 only for changed evidence
        ↓
AI summary (optional) + raster cross-check on gated cases
```

Поиск соответствующего блока на другом листе можно проектировать отдельно: coarse structural signature → candidates → bbox/aspect/text-anchor sanity check → vector comparator → optional Vision tie-break → AI summary. Это не возврат старого matcher/affine architecture и не часть текущего кода.

## 15. Ответы на три главных вопроса

**Можно ли сравнивать произвольный блок преимущественно по данным?** Для векторных планов/схем/узлов с устойчивым text layer и одинаковыми semantic crop extents — да. Для произвольного PDF-блока вообще — пока нет: raster, broken fonts, caps и crop mismatch требуют gate/fallback.

**Какой минимум данных обязателен?** Normalized segments + style-change signal, generic topology/components, exact positioned text + quality, anchors, repeated patterns, provenance/caps и deterministic diff. Одних counts или одной подписи недостаточно.

**Как потом искать пару?** Structural signature только создаёт shortlist. Решение должно подтверждаться bbox/aspect/text-anchor sanity checks и vector comparator; AI не должен сам выбирать пару без детерминированных evidence.

## Финальная рекомендация

**B. Подход работает только для некоторых типов блоков. Нужна гибридная архитектура Vector + Vision.**

Vector должен быть основным проверяемым слоем там, где его quality gates пройдены. Vision нужен как контролируемый fallback/cross-check для raster blocks, сломанных fonts, style-only изменений, неодинаковых crop boundaries и случаев, где caps делают описание неполным. Переходить к production Stage Vector Comparison по этому эксперименту без дополнительного benchmark, page cache, style diff и gate calibration рано.
